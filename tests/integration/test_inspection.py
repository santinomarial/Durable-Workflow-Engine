import os

import pytest

from engine.persistence import (
    create_pool,
    get_execution,
    get_history,
    list_executions,
    register_workflow_definition,
    start_workflow,
    terminate_workflow,
)
from engine.persistence.migrations import migrate
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


@workflow(version=1, name="inspection-e2e")
async def inspection_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.wait_signal("never")
    return value


async def test_queries_and_termination_expose_consistent_terminal_state() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        await register_workflow_definition(pool, inspection_workflow)
        started = await start_workflow(
            pool,
            workflow_type=inspection_workflow.name,
            definition_version=1,
            workflow_input={"inspect": True},
            queue_name="inspection-queue",
        )
        assert await terminate_workflow(
            pool, workflow_id=started.workflow_id, reason="operator request"
        )
        assert not await terminate_workflow(
            pool, workflow_id=started.workflow_id, reason="duplicate"
        )

        execution = await get_execution(pool, started.workflow_id)
        history = await get_history(pool, started.workflow_id)
        terminated = await list_executions(pool, status="terminated")
        assert execution is not None
        assert execution.status == "terminated"
        assert execution.closed_at is not None
        assert execution.input == {"inspect": True}
        assert [event.event_type for event in history] == [
            "WorkflowExecutionStarted",
            "WorkflowExecutionTerminated",
        ]
        assert history[-1].attributes == {"reason": "operator request"}
        assert started.workflow_id in {item.id for item in terminated}
        async with pool.acquire() as connection:
            open_tasks = await connection.fetchval(
                """
                select count(*) from tasks
                where workflow_id = $1 and status in ('pending', 'leased')
                """,
                started.workflow_id,
            )
        assert open_tasks == 0
    finally:
        await pool.close()
