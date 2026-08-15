import asyncio
import json
import os
from datetime import timedelta
from uuid import uuid4

import pytest

from engine.persistence import (
    StaleLeaseError,
    create_pool,
    heartbeat_activity,
    lease_task,
    load_workflow_replay_state,
    reclaim_expired_workflow_tasks,
    register_workflow_definition,
    renew_lease,
    start_workflow,
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


@workflow(version=1, name="lease-test")
async def lease_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


async def test_workflow_task_lease_is_exclusive_and_does_not_append_history() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        await register_workflow_definition(pool, lease_workflow)
        started = await start_workflow(
            pool,
            workflow_type=lease_workflow.name,
            definition_version=lease_workflow.version,
            workflow_input="input",
            queue_name="lease-workflow-queue",
        )

        leased = await lease_task(
            pool,
            task_type="workflow",
            queue_name="lease-workflow-queue",
            lease_duration=timedelta(seconds=15),
        )
        duplicate = await lease_task(
            pool,
            task_type="workflow",
            queue_name="lease-workflow-queue",
        )

        assert leased is not None
        assert leased.workflow_id == started.workflow_id
        assert leased.task_type == "workflow"
        assert leased.lease_token is not None
        assert leased.lease_expires_at > leased.leased_at
        assert duplicate is None

        renewed_until = await renew_lease(
            pool,
            task_id=leased.id,
            lease_token=leased.lease_token,
            lease_duration=timedelta(seconds=30),
        )
        assert renewed_until > leased.lease_expires_at
        with pytest.raises(StaleLeaseError, match="does not hold a live lease"):
            await renew_lease(
                pool,
                task_id=leased.id,
                lease_token=uuid4(),
            )

        async with pool.acquire() as connection:
            event_types = await connection.fetch(
                """
                select event_type
                from history_events
                where workflow_id = $1
                order by seq
                """,
                started.workflow_id,
            )
        assert [row["event_type"] for row in event_types] == ["WorkflowExecutionStarted"]
    finally:
        await pool.close()


async def test_activity_lease_appends_started_event_in_same_transition() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    entity_id = uuid4()
    try:
        await register_workflow_definition(pool, lease_workflow)
        started = await start_workflow(
            pool,
            workflow_type=lease_workflow.name,
            definition_version=lease_workflow.version,
            workflow_input=None,
            queue_name="activity-setup-workflow-queue",
        )
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                insert into history_events (
                  workflow_id, seq, event_type, command_id, entity_id, attributes
                ) values ($1, 2, 'ActivityScheduled', 0, $2, $3::jsonb)
                """,
                started.workflow_id,
                entity_id,
                json.dumps({"activity_type": "lease-activity", "fingerprint": "test"}),
            )
            await connection.execute(
                "update workflow_executions set next_seq = 3 where id = $1",
                started.workflow_id,
            )
            await connection.execute(
                """
                insert into tasks (
                  id, workflow_id, task_type, queue_name, entity_id, command_id,
                  input, start_to_close_timeout, heartbeat_timeout
                ) values ($1, $2, 'activity', 'lease-activity-queue', $3, 0,
                          '{}'::jsonb, interval '20 seconds', interval '5 seconds')
                """,
                uuid4(),
                started.workflow_id,
                entity_id,
            )

        leased = await lease_task(
            pool,
            task_type="activity",
            queue_name="lease-activity-queue",
            lease_duration=timedelta(seconds=10),
        )

        assert leased is not None
        assert leased.entity_id == entity_id
        assert leased.start_to_close_deadline is not None
        assert leased.start_to_close_deadline > leased.lease_expires_at
        original_execution_deadline = leased.start_to_close_deadline
        heartbeat_expiry = await heartbeat_activity(
            pool,
            task_id=leased.id,
            lease_token=leased.lease_token,
            details={"processed": 12},
            lease_duration=timedelta(seconds=15),
        )
        assert heartbeat_expiry > leased.lease_expires_at
        async with pool.acquire() as connection:
            execution_seq = await connection.fetchval(
                "select next_seq from workflow_executions where id = $1", started.workflow_id
            )
            started_event = await connection.fetchrow(
                """
                select * from history_events
                where workflow_id = $1 and event_type = 'ActivityStarted'
                """,
                started.workflow_id,
            )
            heartbeat = await connection.fetchrow(
                """
                select heartbeat_at, heartbeat_details, start_to_close_deadline
                from tasks where id = $1
                """,
                leased.id,
            )
        assert execution_seq == 4
        assert started_event is not None
        assert started_event["seq"] == 3
        assert started_event["entity_id"] == entity_id
        assert json.loads(started_event["attributes"])["attempt"] == 1
        assert heartbeat is not None
        assert heartbeat["heartbeat_at"] is not None
        assert json.loads(heartbeat["heartbeat_details"]) == {"processed": 12}
        assert heartbeat["start_to_close_deadline"] == original_execution_deadline

        async with pool.acquire() as connection:
            await connection.execute(
                "update tasks set lease_expires_at = now() - interval '1 second' where id = $1",
                leased.id,
            )
        with pytest.raises(StaleLeaseError, match="does not hold a live lease"):
            await heartbeat_activity(
                pool,
                task_id=leased.id,
                lease_token=leased.lease_token,
                details={"too_late": True},
            )
    finally:
        await pool.close()


async def test_expired_workflow_lease_is_reclaimed_with_a_new_token() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        await register_workflow_definition(pool, lease_workflow)
        await start_workflow(
            pool,
            workflow_type=lease_workflow.name,
            definition_version=1,
            workflow_input=None,
            queue_name="reclaim-workflow-queue",
        )
        original = await lease_task(pool, task_type="workflow", queue_name="reclaim-workflow-queue")
        assert original is not None
        async with pool.acquire() as connection:
            await connection.execute(
                "update tasks set lease_expires_at = now() - interval '1 second' where id = $1",
                original.id,
            )

        assert await reclaim_expired_workflow_tasks(pool) == 1
        replacement = await lease_task(
            pool, task_type="workflow", queue_name="reclaim-workflow-queue"
        )
        assert replacement is not None
        assert replacement.id == original.id
        assert replacement.lease_token != original.lease_token
        with pytest.raises(StaleLeaseError, match="does not hold the current lease"):
            await load_workflow_replay_state(pool, original)
    finally:
        await pool.close()


async def test_concurrent_pollers_claim_distinct_workflow_tasks() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL, max_size=20)
    try:
        await register_workflow_definition(pool, lease_workflow)
        for index in range(12):
            await start_workflow(
                pool,
                workflow_type=lease_workflow.name,
                definition_version=1,
                workflow_input=index,
                queue_name="concurrent-lease-queue",
            )

        leases = await asyncio.gather(
            *(
                lease_task(pool, task_type="workflow", queue_name="concurrent-lease-queue")
                for _ in range(12)
            )
        )
        assert all(task is not None for task in leases)
        task_ids = {task.id for task in leases if task is not None}
        lease_tokens = {task.lease_token for task in leases if task is not None}
        assert len(task_ids) == 12
        assert len(lease_tokens) == 12
    finally:
        await pool.close()
