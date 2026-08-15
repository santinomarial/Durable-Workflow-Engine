import json
import os
from datetime import timedelta

import pytest

from engine.persistence import (
    create_pool,
    fire_due_timer,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow
from engine.workers import run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@workflow(version=1, name="durable-timer-e2e")
async def durable_timer_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.sleep(timedelta(days=1))
    return value


async def test_timer_fires_after_workers_were_offline_past_deadline() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(durable_timer_workflow)

    pool = await create_pool(DATABASE_URL)
    await register_workflow_definition(pool, durable_timer_workflow)
    started = await start_workflow(
        pool,
        workflow_type=durable_timer_workflow.name,
        definition_version=1,
        workflow_input={"resumed": True},
        queue_name="durable-timer-queue",
    )
    assert await run_workflow_task(pool, registry, queue_name="durable-timer-queue")
    await pool.close()

    restarted_pool = await create_pool(DATABASE_URL)
    try:
        async with restarted_pool.acquire() as connection:
            deadline = await connection.fetchval(
                """
                select visible_at from tasks
                where workflow_id = $1 and task_type = 'timer'
                """,
                started.workflow_id,
            )
        assert await fire_due_timer(
            restarted_pool,
            queue_name="durable-timer-queue",
            clock_time=deadline + timedelta(seconds=1),
        )
        assert not await fire_due_timer(
            restarted_pool,
            queue_name="durable-timer-queue",
            clock_time=deadline + timedelta(seconds=1),
        )
        assert await run_workflow_task(restarted_pool, registry, queue_name="durable-timer-queue")

        async with restarted_pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, result from workflow_executions where id = $1",
                started.workflow_id,
            )
            event_types = await connection.fetch(
                "select event_type from history_events where workflow_id = $1 order by seq",
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "completed"
        assert json.loads(execution["result"]) == {"resumed": True}
        assert [row["event_type"] for row in event_types] == [
            "WorkflowExecutionStarted",
            "TimerStarted",
            "TimerFired",
            "WorkflowExecutionCompleted",
        ]
    finally:
        await restarted_pool.close()
