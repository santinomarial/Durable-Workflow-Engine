import json
import os
from uuid import uuid4

import pytest

from engine.persistence import create_pool, register_workflow_definition, start_workflow
from engine.persistence.migrations import migrate
from engine.persistence.transitions import DefinitionNotRegisteredError
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@workflow(version=1, name="transition-test")
async def transition_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


async def test_start_workflow_commits_execution_history_and_task_atomically() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    workflow_id = uuid4()
    try:
        await register_workflow_definition(pool, transition_workflow)
        started = await start_workflow(
            pool,
            workflow_type=transition_workflow.name,
            definition_version=transition_workflow.version,
            workflow_input={"order_id": "order-123"},
            queue_name="orders",
            workflow_id=workflow_id,
        )
        assert started.workflow_id == workflow_id

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select * from workflow_executions where id = $1", workflow_id
            )
            history = await connection.fetch(
                "select * from history_events where workflow_id = $1 order by seq", workflow_id
            )
            tasks = await connection.fetch(
                "select * from tasks where workflow_id = $1 order by created_at", workflow_id
            )

        assert execution is not None
        assert execution["status"] == "running"
        assert execution["next_seq"] == 2
        assert json.loads(execution["input"]) == {"order_id": "order-123"}
        assert [(event["seq"], event["event_type"]) for event in history] == [
            (1, "WorkflowExecutionStarted")
        ]
        assert json.loads(history[0]["attributes"])["definition_version"] == 1
        assert len(tasks) == 1
        assert tasks[0]["task_type"] == "workflow"
        assert tasks[0]["queue_name"] == "orders"
        assert tasks[0]["status"] == "pending"
    finally:
        await pool.close()


async def test_start_workflow_rejects_unknown_definition_without_partial_rows() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    workflow_id = uuid4()
    try:
        with pytest.raises(DefinitionNotRegisteredError, match="is not registered"):
            await start_workflow(
                pool,
                workflow_type="missing-definition",
                definition_version=99,
                workflow_input=None,
                workflow_id=workflow_id,
            )

        async with pool.acquire() as connection:
            execution_count = await connection.fetchval(
                "select count(*) from workflow_executions where id = $1", workflow_id
            )
            history_count = await connection.fetchval(
                "select count(*) from history_events where workflow_id = $1", workflow_id
            )
            task_count = await connection.fetchval(
                "select count(*) from tasks where workflow_id = $1", workflow_id
            )
        assert (execution_count, history_count, task_count) == (0, 0, 0)
    finally:
        await pool.close()
