import json
import os
from datetime import timedelta

import pytest

from engine.persistence import (
    StaleLeaseError,
    complete_activity,
    create_pool,
    lease_task,
    process_activity_timeout,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.serialization import JSONValue
from engine.sdk import RetryPolicy, WorkflowContext, activity, workflow
from engine.workers import run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@activity(name="timeout-activity")
async def timeout_activity(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="deadline-timeout-e2e")
async def deadline_timeout_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        timeout_activity,
        value,
        retry=RetryPolicy(max_attempts=2, initial_interval=timedelta(0)),
        schedule_to_start=timedelta(seconds=30),
        start_to_close=timedelta(seconds=30),
    )


@workflow(version=1, name="heartbeat-timeout-e2e")
async def heartbeat_timeout_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        timeout_activity,
        value,
        heartbeat_timeout=timedelta(seconds=30),
    )


@workflow(version=1, name="lease-timeout-e2e")
async def lease_timeout_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(timeout_activity, value)


async def test_schedule_and_start_deadlines_retry_then_fail_and_fence_old_worker() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(deadline_timeout_workflow)
    registry.register_activity(timeout_activity)
    try:
        await register_workflow_definition(pool, deadline_timeout_workflow)
        started = await start_workflow(
            pool,
            workflow_type=deadline_timeout_workflow.name,
            definition_version=1,
            workflow_input="payload",
            queue_name="deadline-timeout-queue",
        )
        assert await run_workflow_task(pool, registry, queue_name="deadline-timeout-queue")
        async with pool.acquire() as connection:
            await connection.execute(
                """
                update tasks set schedule_to_start_deadline = now() - interval '1 second'
                where workflow_id = $1 and task_type = 'activity' and attempt = 1
                """,
                started.workflow_id,
            )
        assert await process_activity_timeout(
            pool, queue_name="deadline-timeout-queue", random_value=0
        )

        second = await lease_task(pool, task_type="activity", queue_name="deadline-timeout-queue")
        assert second is not None
        assert second.attempt == 2
        async with pool.acquire() as connection:
            await connection.execute(
                """
                update tasks
                set start_to_close_deadline = now() - interval '1 second',
                    lease_expires_at = now() + interval '30 seconds'
                where id = $1
                """,
                second.id,
            )
        assert await process_activity_timeout(
            pool, queue_name="deadline-timeout-queue", random_value=0
        )
        with pytest.raises(StaleLeaseError, match="does not hold the current lease"):
            await complete_activity(pool, task=second, result="too late")
        assert await run_workflow_task(pool, registry, queue_name="deadline-timeout-queue")

        async with pool.acquire() as connection:
            execution_status = await connection.fetchval(
                "select status from workflow_executions where id = $1", started.workflow_id
            )
            timeout_rows = await connection.fetch(
                """
                select attributes from history_events
                where workflow_id = $1 and event_type = 'ActivityTimedOut' order by seq
                """,
                started.workflow_id,
            )
        assert execution_status == "failed"
        timeout_attributes = [json.loads(row["attributes"]) for row in timeout_rows]
        assert [row["timeout_type"] for row in timeout_attributes] == [
            "schedule_to_start",
            "start_to_close",
        ]
        assert [row["final"] for row in timeout_attributes] == [False, True]
    finally:
        await pool.close()


@pytest.mark.parametrize(
    ("definition", "queue_name", "column_updates", "expected_type"),
    [
        (
            heartbeat_timeout_workflow,
            "heartbeat-timeout-queue",
            "heartbeat_at = now() - interval '31 seconds', "
            "lease_expires_at = now() + interval '30 seconds'",
            "heartbeat",
        ),
        (
            lease_timeout_workflow,
            "lease-expiry-timeout-queue",
            "lease_expires_at = now() - interval '1 second'",
            "lease_expired",
        ),
    ],
)
async def test_heartbeat_and_lease_expiry_are_recorded(
    definition: WorkflowDefinition,
    queue_name: str,
    column_updates: str,
    expected_type: str,
) -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(definition)
    registry.register_activity(timeout_activity)
    try:
        await register_workflow_definition(pool, definition)
        started = await start_workflow(
            pool,
            workflow_type=definition.name,
            definition_version=1,
            workflow_input="payload",
            queue_name=queue_name,
        )
        assert await run_workflow_task(pool, registry, queue_name=queue_name)
        leased = await lease_task(pool, task_type="activity", queue_name=queue_name)
        assert leased is not None
        async with pool.acquire() as connection:
            await connection.execute(f"update tasks set {column_updates} where id = $1", leased.id)
        assert await process_activity_timeout(pool, queue_name=queue_name, random_value=0)
        async with pool.acquire() as connection:
            attributes = await connection.fetchval(
                """
                select attributes from history_events
                where workflow_id = $1 and event_type = 'ActivityTimedOut'
                """,
                started.workflow_id,
            )
        assert json.loads(attributes)["timeout_type"] == expected_type
    finally:
        await pool.close()
