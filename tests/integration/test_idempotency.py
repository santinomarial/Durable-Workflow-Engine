import json
import os
from datetime import timedelta

import pytest

from engine.persistence import (
    create_pool,
    record_idempotent_effect,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import RetryPolicy, WorkflowContext, activity, current_activity_context, workflow
from engine.workers import run_activity_task, run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]

effect_pool = None


@activity(name="idempotent-effect")
async def idempotent_effect(value: JSONValue) -> JSONValue:
    assert effect_pool is not None
    context = current_activity_context()
    inserted = await record_idempotent_effect(
        effect_pool,
        idempotency_key=context.idempotency_key,
        payload={"value": value},
    )
    if context.attempt == 1:
        raise RuntimeError("crashed after external effect")
    return {"inserted": inserted, "value": value}


@workflow(version=1, name="idempotency-e2e")
async def idempotency_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        idempotent_effect,
        value,
        retry=RetryPolicy(max_attempts=2, initial_interval=timedelta(0)),
    )


async def test_retried_activity_observes_one_cooperating_ledger_effect() -> None:
    global effect_pool
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    effect_pool = pool
    registry = DefinitionRegistry()
    registry.register_workflow(idempotency_workflow)
    registry.register_activity(idempotent_effect)
    try:
        await register_workflow_definition(pool, idempotency_workflow)
        started = await start_workflow(
            pool,
            workflow_type=idempotency_workflow.name,
            definition_version=1,
            workflow_input="charge",
            queue_name="idempotency-queue",
        )
        assert await run_workflow_task(pool, registry, queue_name="idempotency-queue")
        assert await run_activity_task(pool, registry, queue_name="idempotency-queue")
        assert await run_activity_task(pool, registry, queue_name="idempotency-queue")
        assert await run_workflow_task(pool, registry, queue_name="idempotency-queue")

        async with pool.acquire() as connection:
            ledger = await connection.fetch("select idempotency_key, payload from effect_ledger")
            result = await connection.fetchval(
                "select result from workflow_executions where id = $1", started.workflow_id
            )
        assert len(ledger) == 1
        assert json.loads(ledger[0]["payload"]) == {"value": "charge"}
        assert json.loads(result) == {"inserted": False, "value": "charge"}
    finally:
        effect_pool = None
        await pool.close()
