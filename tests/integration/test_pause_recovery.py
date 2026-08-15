import os

import pytest

from engine.persistence import (
    create_pool,
    get_execution,
    get_history,
    lease_task,
    list_dead_tasks,
    pause_workflow,
    register_workflow_definition,
    resume_workflow,
    retry_workflow,
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


@workflow(version=1, name="pause-recovery-e2e")
async def pause_recovery_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.wait_signal("continue")
    return value


async def test_pause_fences_dispatch_and_retry_creates_linked_execution() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        await register_workflow_definition(pool, pause_recovery_workflow)
        started = await start_workflow(
            pool,
            workflow_type=pause_recovery_workflow.name,
            definition_version=1,
            workflow_input={"order": 7},
            queue_name="pause-recovery-queue",
            search_attributes={"customer": "customer-7"},
        )

        assert await pause_workflow(
            pool, workflow_id=started.workflow_id, reason="operator investigation"
        )
        assert not await pause_workflow(pool, workflow_id=started.workflow_id)
        assert (
            await lease_task(pool, task_type="workflow", queue_name="pause-recovery-queue") is None
        )
        paused = await get_execution(pool, started.workflow_id)
        assert paused is not None
        assert paused.paused_at is not None
        assert paused.pause_reason == "operator investigation"

        assert await resume_workflow(pool, workflow_id=started.workflow_id, reason="resolved")
        assert not await resume_workflow(pool, workflow_id=started.workflow_id)
        leased = await lease_task(pool, task_type="workflow", queue_name="pause-recovery-queue")
        assert leased is not None
        history = await get_history(pool, started.workflow_id)
        assert [item.event_type for item in history] == [
            "WorkflowExecutionStarted",
            "WorkflowExecutionPaused",
            "WorkflowExecutionResumed",
        ]

        assert await terminate_workflow(
            pool, workflow_id=started.workflow_id, reason="restart with clean state"
        )
        dead = await list_dead_tasks(pool)
        assert any(item.workflow_id == started.workflow_id for item in dead)

        retried = await retry_workflow(pool, workflow_id=started.workflow_id)
        retry_execution = await get_execution(pool, retried.workflow_id)
        assert retry_execution is not None
        assert retry_execution.retry_of == started.workflow_id
        assert retry_execution.input == {"order": 7}
        assert retry_execution.search_attributes == {
            "customer": "customer-7",
            "dwe.retry_of": str(started.workflow_id),
        }
    finally:
        await pool.close()
