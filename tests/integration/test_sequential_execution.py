import json
import os

import pytest

from engine.persistence import create_pool, register_workflow_definition, start_workflow
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, activity, workflow
from engine.workers import run_activity_task, run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@activity(name="sequential-uppercase")
async def uppercase(value: JSONValue) -> JSONValue:
    assert isinstance(value, str)
    return value.upper()


@workflow(version=1, name="sequential-e2e")
async def sequential(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(uppercase, value)


async def test_workers_complete_a_sequential_activity_workflow() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(sequential)
    registry.register_activity(uppercase)
    try:
        await register_workflow_definition(pool, sequential)
        started = await start_workflow(
            pool,
            workflow_type=sequential.name,
            definition_version=sequential.version,
            workflow_input="durable",
            queue_name="sequential-e2e-queue",
        )

        assert await run_workflow_task(pool, registry, queue_name="sequential-e2e-queue")
        assert await run_activity_task(pool, registry, queue_name="sequential-e2e-queue")
        assert await run_workflow_task(pool, registry, queue_name="sequential-e2e-queue")
        assert not await run_workflow_task(pool, registry, queue_name="sequential-e2e-queue")

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, result from workflow_executions where id = $1",
                started.workflow_id,
            )
            history = await connection.fetch(
                """
                select seq, event_type from history_events
                where workflow_id = $1 order by seq
                """,
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "completed"
        assert json.loads(execution["result"]) == "DURABLE"
        assert [(row["seq"], row["event_type"]) for row in history] == [
            (1, "WorkflowExecutionStarted"),
            (2, "ActivityScheduled"),
            (3, "ActivityStarted"),
            (4, "ActivityCompleted"),
            (5, "WorkflowExecutionCompleted"),
        ]
    finally:
        await pool.close()
