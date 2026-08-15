import os
from datetime import timedelta

import pytest

from engine.persistence import (
    StaleLeaseError,
    complete_activity,
    create_pool,
    get_execution,
    lease_task,
    process_activity_timeout,
    record_idempotent_effect,
    register_workflow_definition,
    send_signal,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.replay_check import replay_check
from engine.runtime.serialization import JSONValue
from engine.sdk import RetryPolicy, WorkflowContext, activity, current_activity_context, workflow
from engine.workers import run_activity_task, run_maintenance, run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")
WORKFLOW_COUNT = 6

pytestmark = [
    pytest.mark.chaos,
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for chaos tests",
    ),
]

chaos_pool = None


@activity(name="chaos-effect")
async def chaos_effect(value: JSONValue) -> JSONValue:
    assert chaos_pool is not None
    context = current_activity_context()
    inserted = await record_idempotent_effect(
        chaos_pool,
        idempotency_key=context.idempotency_key,
        payload=value,
    )
    return {"inserted": inserted, "value": value}


@activity(name="chaos-pure")
async def chaos_pure(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="chaos-workflow")
async def chaos_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    effect = await ctx.activity(
        chaos_effect,
        value,
        retry=RetryPolicy(max_attempts=3, initial_interval=timedelta(0)),
    )
    parallel = await ctx.gather(
        ctx.activity(chaos_pure, {"branch": "left", "value": value}),
        ctx.activity(chaos_pure, {"branch": "right", "value": value}),
    )
    await ctx.sleep(timedelta(0))
    signal = await ctx.wait_signal("continue")
    return {"effect": effect, "parallel": parallel, "signal": signal}


async def test_failure_harness_preserves_history_and_deduplicates_effects() -> None:
    global chaos_pool
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL, max_size=20)
    chaos_pool = pool
    registry = DefinitionRegistry()
    registry.register_workflow(chaos_workflow)
    registry.register_activity(chaos_effect)
    registry.register_activity(chaos_pure)
    queue_name = "chaos-queue"
    try:
        await register_workflow_definition(pool, chaos_workflow)
        executions = [
            await start_workflow(
                pool,
                workflow_type=chaos_workflow.name,
                definition_version=1,
                workflow_input={"index": index},
                queue_name=queue_name,
            )
            for index in range(WORKFLOW_COUNT)
        ]
        for execution in executions:
            assert await send_signal(
                pool,
                workflow_id=execution.workflow_id,
                signal_id=f"continue-{execution.workflow_id}",
                name="continue",
                payload={"offline": True},
            )
            assert not await send_signal(
                pool,
                workflow_id=execution.workflow_id,
                signal_id=f"continue-{execution.workflow_id}",
                name="continue",
                payload={"duplicate": True},
            )

        for _ in range(WORKFLOW_COUNT):
            assert await run_workflow_task(pool, registry, queue_name=queue_name)

        killed_attempts = []
        for _ in range(WORKFLOW_COUNT):
            task = await lease_task(pool, task_type="activity", queue_name=queue_name)
            assert task is not None and isinstance(task.input, dict)
            key = task.input["idempotency_key"]
            assert isinstance(key, str)
            await record_idempotent_effect(pool, idempotency_key=key, payload=task.input["input"])
            killed_attempts.append(task)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                update tasks set lease_expires_at = now() - interval '1 second'
                where queue_name = $1 and task_type = 'activity' and status = 'leased'
                """,
                queue_name,
            )
        for _ in range(WORKFLOW_COUNT):
            assert await process_activity_timeout(pool, queue_name=queue_name, random_value=0)
        with pytest.raises(StaleLeaseError):
            await complete_activity(pool, task=killed_attempts[0], result="stale")

        await pool.close()
        pool = await create_pool(DATABASE_URL, max_size=20)
        chaos_pool = pool

        for _ in range(500):
            running = 0
            async with pool.acquire() as connection:
                running = await connection.fetchval(
                    "select count(*) from workflow_executions where status = 'running'"
                )
            if running == 0:
                break
            progressed = False
            progressed |= await run_workflow_task(pool, registry, queue_name=queue_name)
            progressed |= await run_activity_task(pool, registry, queue_name=queue_name)
            progressed |= bool(await run_maintenance(pool, queue_name=queue_name))
            assert progressed
        else:
            pytest.fail("chaos workflows did not converge")

        async with pool.acquire() as connection:
            ledger_count = await connection.fetchval("select count(*) from effect_ledger")
            malformed_histories = await connection.fetchval(
                """
                select count(*) from (
                  select workflow_id, count(*) as events, max(seq) as max_seq
                  from history_events group by workflow_id
                  having count(*) <> max(seq)
                ) malformed
                """
            )
        assert ledger_count == WORKFLOW_COUNT
        assert malformed_histories == 0
        for execution in executions:
            summary = await get_execution(pool, execution.workflow_id)
            assert summary is not None and summary.status == "completed"
            report = await replay_check(
                pool,
                workflow_id=execution.workflow_id,
                definition=chaos_workflow,
            )
            assert report.compatible
    finally:
        chaos_pool = None
        await pool.close()
