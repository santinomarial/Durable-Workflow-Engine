import json
import os

import pytest

from engine.persistence import (
    complete_activity,
    create_pool,
    lease_task,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, activity, workflow
from engine.workers import run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@activity(name="parallel-echo")
async def parallel_echo(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="parallel-gather-e2e")
async def parallel_gather(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del value
    return await ctx.gather(
        ctx.activity(parallel_echo, "source-first"),
        ctx.activity(parallel_echo, "source-second"),
    )


async def test_parallel_activities_complete_reverse_order_and_join_source_order() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(parallel_gather)
    registry.register_activity(parallel_echo)
    try:
        await register_workflow_definition(pool, parallel_gather)
        started = await start_workflow(
            pool,
            workflow_type=parallel_gather.name,
            definition_version=1,
            workflow_input=None,
            queue_name="parallel-gather-queue",
        )
        assert await run_workflow_task(pool, registry, queue_name="parallel-gather-queue")
        first = await lease_task(pool, task_type="activity", queue_name="parallel-gather-queue")
        second = await lease_task(pool, task_type="activity", queue_name="parallel-gather-queue")
        assert first is not None and second is not None
        tasks_by_command = {first.command_id: first, second.command_id: second}

        await complete_activity(pool, task=tasks_by_command[1], result="finished-second")
        await complete_activity(pool, task=tasks_by_command[0], result="finished-first")
        assert await run_workflow_task(pool, registry, queue_name="parallel-gather-queue")

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, result from workflow_executions where id = $1",
                started.workflow_id,
            )
            completion_entities = await connection.fetch(
                """
                select entity_id from history_events
                where workflow_id = $1 and event_type = 'ActivityCompleted' order by seq
                """,
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "completed"
        assert json.loads(execution["result"]) == ["finished-first", "finished-second"]
        assert [row["entity_id"] for row in completion_entities] == [
            tasks_by_command[1].entity_id,
            tasks_by_command[0].entity_id,
        ]
    finally:
        await pool.close()
