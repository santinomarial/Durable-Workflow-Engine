"""Reproducible correctness-engine benchmarks with explicit environment metadata."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import time
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from engine.persistence import (
    create_pool,
    fire_due_timer,
    lease_task,
    process_activity_timeout,
    reclaim_expired_workflow_tasks,
    register_workflow_definition,
    send_signal,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry, ReplayStatus, replay_workflow
from engine.runtime.history import HistoryEvent
from engine.runtime.serialization import JSONValue, fingerprint
from engine.sdk import RetryPolicy, WorkflowContext, activity, workflow
from engine.sdk.context import ENTITY_NAMESPACE
from engine.workers import run_workflow_task

BENCHMARK_WORKFLOW_ID = UUID("079f51ca-7401-4823-8fd5-6cc47f9c0af3")


@activity(name="benchmark-identity")
async def benchmark_identity(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="benchmark-replay")
async def benchmark_replay(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    assert isinstance(value, dict)
    count = value["count"]
    assert isinstance(count, int)
    result: JSONValue = None
    for index in range(count):
        result = await ctx.activity(benchmark_identity, index)
    return result


@workflow(version=1, name="benchmark-start")
async def benchmark_start(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


@workflow(version=1, name="benchmark-timer")
async def benchmark_timer(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.sleep(timedelta(milliseconds=1))
    return value


@workflow(version=1, name="benchmark-activity-dispatch")
async def benchmark_activity_dispatch(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        benchmark_identity,
        value,
        retry=RetryPolicy(max_attempts=2, initial_interval=timedelta(0)),
    )


def percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction)))
    return ordered[index]


def distribution(samples: list[float]) -> dict[str, float]:
    return {
        "p50": percentile(samples, 0.50),
        "p95": percentile(samples, 0.95),
        "p99": percentile(samples, 0.99),
    }


def replay_history(event_count: int) -> tuple[HistoryEvent, ...]:
    if event_count < 1:
        raise ValueError("event_count must be positive")
    events = [HistoryEvent(1, "WorkflowExecutionStarted", {"input": None})]
    policy = RetryPolicy().to_json()
    command_count = (event_count - 1) // 2
    for command_id in range(command_count):
        command_input: dict[str, JSONValue] = {"args": [command_id], "kwargs": {}}
        identity: dict[str, JSONValue] = {
            "command_type": "activity",
            "activity_type": benchmark_identity.name,
            "input": command_input,
            "retry_policy": policy,
            "schedule_to_start_seconds": None,
            "start_to_close_seconds": None,
            "heartbeat_timeout_seconds": None,
        }
        entity_id = uuid5(
            ENTITY_NAMESPACE,
            f"{BENCHMARK_WORKFLOW_ID}:activity:{command_id}",
        )
        events.append(
            HistoryEvent(
                len(events) + 1,
                "ActivityScheduled",
                {"fingerprint": fingerprint(identity)},
                command_id=command_id,
                entity_id=entity_id,
            )
        )
        events.append(
            HistoryEvent(
                len(events) + 1,
                "ActivityCompleted",
                {"result": command_id, "attempt": 1},
                entity_id=entity_id,
            )
        )
    while len(events) < event_count:
        filler = len(events)
        events.append(
            HistoryEvent(
                len(events) + 1,
                "SignalReceived",
                {"name": "benchmark-filler", "payload": filler},
                external_id=f"filler-{filler}",
            )
        )
    return tuple(events)


async def benchmark_replays(profile: str) -> dict[str, Any]:
    sizes = [10, 1_000, 100_000]
    sample_count = 2 if profile == "quick" else 5
    results: dict[str, Any] = {}
    for event_count in sizes:
        history = replay_history(event_count)
        command_count = (event_count - 1) // 2
        samples = []
        warmup = await replay_workflow(
            benchmark_replay,
            workflow_id=BENCHMARK_WORKFLOW_ID,
            workflow_input={"count": command_count},
            history=history,
        )
        assert warmup.status is ReplayStatus.COMPLETED
        for _ in range(sample_count):
            started = time.perf_counter()
            replay = await replay_workflow(
                benchmark_replay,
                workflow_id=BENCHMARK_WORKFLOW_ID,
                workflow_input={"count": command_count},
                history=history,
            )
            elapsed = time.perf_counter() - started
            assert replay.status is ReplayStatus.COMPLETED
            samples.append(elapsed)
        median = statistics.median(samples)
        results[str(event_count)] = {
            "median_seconds": median,
            "events_per_second": event_count / median,
            "samples": sample_count,
            "warmup_samples": 1,
        }
    return results


async def benchmark_postgres(database_url: str, profile: str) -> dict[str, Any]:
    await migrate(database_url)
    pool = await create_pool(database_url, max_size=32)
    registry = DefinitionRegistry()
    registry.register_workflow(benchmark_timer)
    registry.register_workflow(benchmark_activity_dispatch)
    start_count = 100 if profile == "quick" else 1_000
    try:
        await register_workflow_definition(pool, benchmark_start)
        await register_workflow_definition(pool, benchmark_timer)
        await register_workflow_definition(pool, benchmark_activity_dispatch)

        began = time.perf_counter()
        await asyncio.gather(
            *(
                start_workflow(
                    pool,
                    workflow_type=benchmark_start.name,
                    definition_version=1,
                    workflow_input=index,
                    queue_name="benchmark-starts",
                )
                for index in range(start_count)
            )
        )
        start_elapsed = time.perf_counter() - began

        workflow_dispatch_samples = []
        for _ in range(start_count):
            began = time.perf_counter()
            assert await lease_task(pool, task_type="workflow", queue_name="benchmark-starts")
            workflow_dispatch_samples.append((time.perf_counter() - began) * 1_000)

        activity_count = 50 if profile == "quick" else 500
        for index in range(activity_count):
            await start_workflow(
                pool,
                workflow_type=benchmark_activity_dispatch.name,
                definition_version=1,
                workflow_input=index,
                queue_name="benchmark-activities",
            )
            assert await run_workflow_task(pool, registry, queue_name="benchmark-activities")
        activity_dispatch_samples = []
        for _ in range(activity_count):
            began = time.perf_counter()
            assert await lease_task(pool, task_type="activity", queue_name="benchmark-activities")
            activity_dispatch_samples.append((time.perf_counter() - began) * 1_000)

        recovery_execution = await start_workflow(
            pool,
            workflow_type=benchmark_start.name,
            definition_version=1,
            workflow_input=None,
            queue_name="benchmark-recovery",
        )
        recovery_lease = await lease_task(
            pool, task_type="workflow", queue_name="benchmark-recovery"
        )
        assert recovery_lease is not None
        async with pool.acquire() as connection:
            await connection.execute(
                "update tasks set lease_expires_at = now() - interval '1 second' where id = $1",
                recovery_lease.id,
            )
        began = time.perf_counter()
        assert await reclaim_expired_workflow_tasks(pool) >= 1
        replacement = await lease_task(pool, task_type="workflow", queue_name="benchmark-recovery")
        assert replacement is not None and replacement.workflow_id == recovery_execution.workflow_id
        recovery_ms = (time.perf_counter() - began) * 1_000

        activity_recovery_execution = await start_workflow(
            pool,
            workflow_type=benchmark_activity_dispatch.name,
            definition_version=1,
            workflow_input=None,
            queue_name="benchmark-activity-recovery",
        )
        assert await run_workflow_task(pool, registry, queue_name="benchmark-activity-recovery")
        abandoned_activity = await lease_task(
            pool,
            task_type="activity",
            queue_name="benchmark-activity-recovery",
        )
        assert abandoned_activity is not None
        async with pool.acquire() as connection:
            await connection.execute(
                "update tasks set lease_expires_at = now() - interval '1 second' where id = $1",
                abandoned_activity.id,
            )
        began = time.perf_counter()
        assert await process_activity_timeout(
            pool,
            queue_name="benchmark-activity-recovery",
            random_value=0,
        )
        replacement_activity = None
        for _ in range(100):
            replacement_activity = await lease_task(
                pool,
                task_type="activity",
                queue_name="benchmark-activity-recovery",
            )
            if replacement_activity is not None:
                break
            await asyncio.sleep(0.001)
        assert (
            replacement_activity is not None
            and replacement_activity.workflow_id == activity_recovery_execution.workflow_id
            and replacement_activity.attempt == 2
        )
        activity_recovery_ms = (time.perf_counter() - began) * 1_000

        timer_delays = []
        timer_count = 20 if profile == "quick" else 200
        for index in range(timer_count):
            timer_queue = f"benchmark-timer-{index}"
            execution = await start_workflow(
                pool,
                workflow_type=benchmark_timer.name,
                definition_version=1,
                workflow_input=index,
                queue_name=timer_queue,
            )
            assert await run_workflow_task(pool, registry, queue_name=timer_queue)
            fired = False
            for _ in range(100):
                if await fire_due_timer(pool, queue_name=timer_queue):
                    fired = True
                    break
                await asyncio.sleep(0.001)
            if not fired:
                async with pool.acquire() as connection:
                    timer_state = await connection.fetchrow(
                        """
                        select t.status, t.queue_name, t.visible_at, now() as checked_at,
                               e.status as workflow_status
                        from tasks t
                        join workflow_executions e on e.id = t.workflow_id
                        where t.workflow_id = $1 and t.task_type = 'timer'
                        """,
                        execution.workflow_id,
                    )
                raise RuntimeError(f"scheduled timer did not fire: {dict(timer_state or {})}")
            async with pool.acquire() as connection:
                delay = await connection.fetchval(
                    """
                    select extract(epoch from (h.created_at - t.visible_at)) * 1000
                    from history_events h join tasks t on t.entity_id = h.entity_id
                    where h.workflow_id = $1 and h.event_type = 'TimerFired'
                    """,
                    execution.workflow_id,
                )
            timer_delays.append(float(delay))

        append_sizes = [10, 1_000, 100_000]
        append_cost: dict[str, float] = {}
        for history_size in append_sizes:
            execution = await start_workflow(
                pool,
                workflow_type=benchmark_start.name,
                definition_version=1,
                workflow_input=None,
                queue_name="benchmark-append",
            )
            async with pool.acquire() as connection, connection.transaction():
                await connection.execute(
                    """
                    insert into history_events (
                      workflow_id, seq, event_type, external_id, attributes
                    )
                    select $1, value, 'SignalReceived', 'seed-' || value,
                           jsonb_build_object('name', 'seed', 'payload', value)
                    from generate_series(2, $2::integer) value
                    """,
                    execution.workflow_id,
                    history_size,
                )
                await connection.execute(
                    "update workflow_executions set next_seq = $2 where id = $1",
                    execution.workflow_id,
                    history_size + 1,
                )
            began = time.perf_counter()
            await send_signal(
                pool,
                workflow_id=execution.workflow_id,
                signal_id="measured-append",
                name="measured",
            )
            append_cost[str(history_size)] = (time.perf_counter() - began) * 1_000

        pending_depths = [100, 1_000] if profile == "quick" else [1_000, 10_000]
        depth_results: dict[str, dict[str, float]] = {}
        for depth in pending_depths:
            await asyncio.gather(
                *(
                    start_workflow(
                        pool,
                        workflow_type=benchmark_start.name,
                        definition_version=1,
                        workflow_input=index,
                        queue_name=f"benchmark-depth-{depth}",
                    )
                    for index in range(depth)
                )
            )
            samples = []
            for _ in range(min(depth, 100)):
                began = time.perf_counter()
                await lease_task(
                    pool,
                    task_type="workflow",
                    queue_name=f"benchmark-depth-{depth}",
                )
                samples.append((time.perf_counter() - began) * 1_000)
            depth_results[str(depth)] = distribution(samples)

        contention: dict[str, float] = {}
        for workers in (1, 4, 16):
            queue = f"benchmark-contention-{workers}"
            count = workers * 20
            await asyncio.gather(
                *(
                    start_workflow(
                        pool,
                        workflow_type=benchmark_start.name,
                        definition_version=1,
                        workflow_input=index,
                        queue_name=queue,
                    )
                    for index in range(count)
                )
            )
            began = time.perf_counter()
            leases = await asyncio.gather(
                *(lease_task(pool, task_type="workflow", queue_name=queue) for _ in range(count))
            )
            elapsed = time.perf_counter() - began
            assert all(lease is not None for lease in leases)
            contention[str(workers)] = count / elapsed

        async with pool.acquire() as connection:
            postgres_version = await connection.fetchval("show server_version")
            postgres_settings = {
                name: await connection.fetchval(f"show {name}")
                for name in (
                    "max_connections",
                    "shared_buffers",
                    "synchronous_commit",
                    "wal_level",
                )
            }
        return {
            "postgres_version": postgres_version,
            "postgres_settings": postgres_settings,
            "workflow_starts_per_second": start_count / start_elapsed,
            "workflow_dispatch_call_ms": distribution(workflow_dispatch_samples),
            "activity_dispatch_call_ms": distribution(activity_dispatch_samples),
            "timer_fire_delay_ms": distribution(timer_delays),
            "expired_workflow_lease_recovery_ms": recovery_ms,
            "expired_activity_lease_recovery_ms": activity_recovery_ms,
            "append_transition_ms_by_history_size": append_cost,
            "dispatch_ms_by_pending_depth": depth_results,
            "leases_per_second_by_concurrent_pollers": contention,
            "largest_pending_depth_tested": max(pending_depths),
            "first_failure_point": None,
        }
    finally:
        await pool.close()


async def run(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {
        "metadata": {
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "profile": args.profile,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
        },
        "replay": await benchmark_replays(args.profile),
    }
    if args.database_url:
        result["postgres"] = await benchmark_postgres(args.database_url, args.profile)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("quick", "full"), default="quick")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
