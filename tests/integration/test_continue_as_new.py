import os

import pytest

from engine.persistence import (
    create_pool,
    get_continuation_chain,
    get_execution,
    get_history,
    register_workflow_definition,
    start_workflow,
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


@workflow(version=2, name="continuation-e2e")
async def continuation_v2(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"completed_generation": value}


@workflow(version=1, name="continuation-e2e")
async def continuation_v1(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    assert isinstance(value, dict)
    ctx.continue_as_new(continuation_v2, {"generation": int(value["generation"]) + 1})
    raise AssertionError("continue_as_new must suspend replay")


async def test_continue_as_new_atomically_links_and_runs_a_fresh_history() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(continuation_v1)
    registry.register_workflow(continuation_v2)
    queue = "continue-as-new-e2e-queue"
    try:
        await register_workflow_definition(pool, continuation_v1)
        await register_workflow_definition(pool, continuation_v2)
        started = await start_workflow(
            pool,
            workflow_type=continuation_v1.name,
            definition_version=1,
            workflow_input={"generation": 0},
            queue_name=queue,
            search_attributes={"customer": "acme"},
        )

        assert await run_workflow_task(pool, registry, queue_name=queue)
        original = await get_execution(pool, started.workflow_id)
        assert original is not None
        assert original.status == "completed"
        assert original.continued_to is not None
        assert original.result == {"continued_to": str(original.continued_to)}
        assert [event.event_type for event in await get_history(pool, original.id)] == [
            "WorkflowExecutionStarted",
            "WorkflowExecutionContinuedAsNew",
            "WorkflowExecutionCompleted",
        ]

        continuation = await get_execution(pool, original.continued_to)
        assert continuation is not None
        assert continuation.definition_version == 2
        assert continuation.continued_from == original.id
        assert continuation.input == {"generation": 1}
        assert continuation.search_attributes == {
            "customer": "acme",
            "dwe.continued_from": str(original.id),
        }
        assert [event.event_type for event in await get_history(pool, continuation.id)] == [
            "WorkflowExecutionStarted"
        ]

        chain = await get_continuation_chain(pool, continuation.id)
        assert [execution.id for execution in chain] == [original.id, continuation.id]

        assert await run_workflow_task(pool, registry, queue_name=queue)
        completed = await get_execution(pool, continuation.id)
        assert completed is not None
        assert completed.status == "completed"
        assert completed.result == {"completed_generation": {"generation": 1}}
    finally:
        await pool.close()
