import json
import os

import pytest

from engine.persistence import create_pool, register_workflow_definition, start_workflow
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


@workflow(version=1, name="marker-integration")
async def marker_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del value
    return {
        "now": ctx.now().isoformat(),
        "random": ctx.random(),
        "uuid": str(ctx.uuid()),
    }


async def test_markers_commit_and_resume_until_terminal_result() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(marker_workflow)
    try:
        await register_workflow_definition(pool, marker_workflow)
        execution = await start_workflow(
            pool,
            workflow_type=marker_workflow.name,
            definition_version=1,
            workflow_input=None,
            queue_name="markers",
        )

        for _ in range(4):
            assert await run_workflow_task(pool, registry, queue_name="markers")

        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                "select status, result from workflow_executions where id = $1",
                execution.workflow_id,
            )
            events = await connection.fetch(
                """
                select event_type, command_id, attributes
                from history_events where workflow_id = $1 order by seq
                """,
                execution.workflow_id,
            )
        assert row is not None and row["status"] == "completed"
        result = json.loads(row["result"])
        assert 0 <= result["random"] < 1
        assert [event["event_type"] for event in events] == [
            "WorkflowExecutionStarted",
            "MarkerRecorded",
            "MarkerRecorded",
            "MarkerRecorded",
            "WorkflowExecutionCompleted",
        ]
        assert [event["command_id"] for event in events[1:4]] == [0, 1, 2]
        assert [json.loads(event["attributes"])["marker_type"] for event in events[1:4]] == [
            "now",
            "random",
            "uuid",
        ]
        assert json.loads(events[0]["attributes"])["started_at"] == result["now"]
    finally:
        await pool.close()
