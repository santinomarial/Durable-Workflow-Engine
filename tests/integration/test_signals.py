import json
import os

import pytest

from engine.persistence import (
    create_pool,
    register_workflow_definition,
    send_signal,
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


@workflow(version=1, name="durable-signal-e2e")
async def durable_signal_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    signal = await ctx.wait_signal("approved")
    return {"input": value, "signal": signal}


async def test_signal_sent_while_workers_offline_is_deduplicated_and_consumed() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(durable_signal_workflow)
    pool = await create_pool(DATABASE_URL)
    await register_workflow_definition(pool, durable_signal_workflow)
    started = await start_workflow(
        pool,
        workflow_type=durable_signal_workflow.name,
        definition_version=1,
        workflow_input="request",
        queue_name="durable-signal-queue",
    )
    assert await run_workflow_task(pool, registry, queue_name="durable-signal-queue")
    await pool.close()

    restarted_pool = await create_pool(DATABASE_URL)
    try:
        assert await send_signal(
            restarted_pool,
            workflow_id=started.workflow_id,
            signal_id="signal-123",
            name="approved",
            payload={"by": "operator"},
        )
        assert not await send_signal(
            restarted_pool,
            workflow_id=started.workflow_id,
            signal_id="signal-123",
            name="approved",
            payload={"by": "duplicate"},
        )
        assert await run_workflow_task(restarted_pool, registry, queue_name="durable-signal-queue")

        async with restarted_pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, result from workflow_executions where id = $1",
                started.workflow_id,
            )
            signals = await connection.fetch(
                """
                select external_id, attributes from history_events
                where workflow_id = $1 and event_type = 'SignalReceived'
                """,
                started.workflow_id,
            )
            open_tasks = await connection.fetchval(
                """
                select count(*) from tasks
                where workflow_id = $1 and status in ('pending', 'leased')
                """,
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "completed"
        assert json.loads(execution["result"])["signal"] == {"by": "operator"}
        assert len(signals) == 1
        assert signals[0]["external_id"] == "signal-123"
        assert open_tasks == 0
    finally:
        await restarted_pool.close()
