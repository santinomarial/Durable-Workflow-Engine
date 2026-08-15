import os

import pytest

from engine.persistence import (
    create_pool,
    get_execution,
    get_history,
    register_workflow_definition,
    start_workflow,
    terminate_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow
from engine.workers.workflow_worker import run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@workflow(version=1, name="child-e2e")
async def child_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"processed": value}


@workflow(version=1, name="parent-e2e")
async def parent_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    child_result = await ctx.child_workflow(child_workflow, value)
    return {"parent": child_result}


async def test_child_completion_wakes_parent_and_parent_close_propagates() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(parent_workflow)
    registry.register_workflow(child_workflow)
    queue = "child-workflow-e2e-queue"
    try:
        await register_workflow_definition(pool, parent_workflow)
        await register_workflow_definition(pool, child_workflow)
        started = await start_workflow(
            pool,
            workflow_type=parent_workflow.name,
            definition_version=1,
            workflow_input={"order": 17},
            queue_name=queue,
        )
        assert await run_workflow_task(pool, registry, queue_name=queue)
        parent_history = await get_history(pool, started.workflow_id)
        assert [item.event_type for item in parent_history] == [
            "WorkflowExecutionStarted",
            "ChildWorkflowStarted",
        ]
        child_id = parent_history[-1].entity_id
        assert child_id is not None
        child = await get_execution(pool, child_id)
        assert child is not None
        assert child.parent_workflow_id == started.workflow_id
        assert child.parent_command_id == 0

        assert await run_workflow_task(pool, registry, queue_name=queue)
        parent_history = await get_history(pool, started.workflow_id)
        assert parent_history[-1].event_type == "ChildWorkflowCompleted"
        assert await run_workflow_task(pool, registry, queue_name=queue)
        parent = await get_execution(pool, started.workflow_id)
        assert parent is not None
        assert parent.status == "completed"
        assert parent.result == {"parent": {"processed": {"order": 17}}}

        second = await start_workflow(
            pool,
            workflow_type=parent_workflow.name,
            definition_version=1,
            workflow_input={"order": 18},
            queue_name=queue,
        )
        assert await run_workflow_task(pool, registry, queue_name=queue)
        second_history = await get_history(pool, second.workflow_id)
        second_child_id = second_history[-1].entity_id
        assert second_child_id is not None
        assert await terminate_workflow(
            pool, workflow_id=second.workflow_id, reason="parent operator stop"
        )
        second_child = await get_execution(pool, second_child_id)
        assert second_child is not None
        assert second_child.status == "terminated"
        child_history = await get_history(pool, second_child_id)
        assert child_history[-1].attributes["cause"] == "parent_close_policy"
    finally:
        await pool.close()
