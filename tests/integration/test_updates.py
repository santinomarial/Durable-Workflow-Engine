import os

import pytest

from engine.client import DurableClient
from engine.persistence import create_pool, get_history, get_update, register_workflow_definition
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


@workflow(version=1, name="updates-e2e")
async def updates_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    update = await ctx.wait_update("change")
    accepted = isinstance(update.payload, dict) and isinstance(update.payload.get("amount"), int)
    ctx.resolve_update(
        update,
        {"accepted_amount": update.payload.get("amount")} if accepted else {"error": "amount"},
        accepted=accepted,
    )
    return {"initial": value, "change": update.payload}


async def test_typed_handle_update_result_and_snapshot_query() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(updates_workflow)
    queue = "updates-e2e-queue"
    try:
        await register_workflow_definition(pool, updates_workflow)
        client = DurableClient(pool)
        handle = await client.start(
            updates_workflow,
            {"amount": 1},
            queue_name=queue,
            search_attributes={"account": "account-7"},
        )
        assert await run_workflow_task(pool, registry, queue_name=queue)
        update_handle = await handle.update("change", {"amount": 8}, update_id="change-1")
        duplicate = await handle.update("change", {"amount": 999}, update_id="change-1")
        assert duplicate.update_id == update_handle.update_id
        assert await run_workflow_task(pool, registry, queue_name=queue)
        update = await get_update(pool, workflow_id=handle.id, update_id="change-1")
        assert update is not None
        assert update.status == "completed"
        assert update.result == {"accepted_amount": 8}
        assert await update_handle.result(timeout=1) == {"accepted_amount": 8}
        assert await run_workflow_task(pool, registry, queue_name=queue)
        assert await handle.result(timeout=1) == {
            "initial": {"amount": 1},
            "change": {"amount": 8},
        }
        event_types = await handle.query(
            lambda snapshot: [event.event_type for event in snapshot.history]
        )
        assert event_types == [
            "WorkflowExecutionStarted",
            "WorkflowUpdateReceived",
            "WorkflowUpdateResolved",
            "WorkflowExecutionCompleted",
        ]
        assert [item.event_type for item in await get_history(pool, handle.id)] == event_types

        closing = await client.start(updates_workflow, None, queue_name=queue)
        unresolved = await closing.update("change", {"amount": 3}, update_id="closing-1")
        assert await closing.terminate("operator stop")
        rejected = await unresolved.describe()
        assert rejected.status == "rejected"
        assert rejected.failure == {
            "type": "WorkflowClosed",
            "message": "workflow terminated",
        }
    finally:
        await pool.close()
