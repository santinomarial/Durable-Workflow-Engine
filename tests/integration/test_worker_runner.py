import asyncio
import json
import os
from datetime import timedelta

import pytest

from engine.persistence import create_pool, register_workflow_definition, start_workflow
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, activity, workflow
from engine.workers import run_worker

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@activity(name="runner-echo")
async def runner_echo(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="runner-integration")
async def runner_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.sleep(timedelta(0))
    return await ctx.activity(runner_echo, value)


async def test_continuous_worker_drives_timer_and_activity_to_completion() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(runner_workflow)
    registry.register_activity(runner_echo)
    stop = asyncio.Event()
    worker: asyncio.Task[None] | None = None
    try:
        await register_workflow_definition(pool, runner_workflow)
        execution = await start_workflow(
            pool,
            workflow_type=runner_workflow.name,
            definition_version=1,
            workflow_input={"driven": True},
            queue_name="runner-integration",
        )
        worker = asyncio.create_task(
            run_worker(
                pool,
                registry,
                queue_name="runner-integration",
                idle_delay=0.001,
                heartbeat_interval=0.01,
                stop=stop,
            )
        )
        async with asyncio.timeout(3):
            while True:
                async with pool.acquire() as connection:
                    row = await connection.fetchrow(
                        "select status, result from workflow_executions where id = $1",
                        execution.workflow_id,
                    )
                if row is not None and row["status"] == "completed":
                    break
                await asyncio.sleep(0.005)
        stop.set()
        await worker

        assert json.loads(row["result"]) == {"driven": True}
        async with pool.acquire() as connection:
            event_types = await connection.fetch(
                "select event_type from history_events where workflow_id = $1 order by seq",
                execution.workflow_id,
            )
            heartbeat = await connection.fetchrow(
                """
                select queue_name, roles, stopped_at
                from worker_heartbeats order by started_at desc limit 1
                """
            )
        assert [event["event_type"] for event in event_types] == [
            "WorkflowExecutionStarted",
            "TimerStarted",
            "TimerFired",
            "ActivityScheduled",
            "ActivityStarted",
            "ActivityCompleted",
            "WorkflowExecutionCompleted",
        ]
        assert heartbeat is not None
        assert heartbeat["queue_name"] == "runner-integration"
        assert set(heartbeat["roles"]) == {"workflow", "activity", "maintenance"}
        assert heartbeat["stopped_at"] is not None
    finally:
        stop.set()
        if worker is not None and not worker.done():
            await worker
        await pool.close()
