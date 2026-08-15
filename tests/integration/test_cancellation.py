import json
import os

import pytest

from engine.persistence import (
    ActivityCancellationRequested,
    StaleLeaseError,
    complete_activity,
    create_pool,
    heartbeat_activity,
    lease_task,
    register_workflow_definition,
    request_workflow_cancellation,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, activity, workflow
from engine.workers import run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@activity(name="cancellation-long-running")
async def long_running(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="cancellation-integration")
async def cancellable_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(long_running, value, heartbeat_timeout=None)


async def test_cancellation_is_recorded_exposed_and_fences_late_completion() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(cancellable_workflow)
    try:
        await register_workflow_definition(pool, cancellable_workflow)
        execution = await start_workflow(
            pool,
            workflow_type=cancellable_workflow.name,
            definition_version=1,
            workflow_input="work",
            queue_name="cancellation",
        )
        assert await run_workflow_task(pool, registry, queue_name="cancellation")
        activity_task = await lease_task(pool, task_type="activity", queue_name="cancellation")
        assert activity_task is not None

        assert await request_workflow_cancellation(
            pool,
            workflow_id=execution.workflow_id,
            reason="operator request",
        )
        assert not await request_workflow_cancellation(
            pool,
            workflow_id=execution.workflow_id,
            reason="duplicate",
        )
        with pytest.raises(ActivityCancellationRequested, match="operator request"):
            await heartbeat_activity(
                pool,
                task_id=activity_task.id,
                lease_token=activity_task.lease_token,
            )

        assert await run_workflow_task(pool, registry, queue_name="cancellation")
        with pytest.raises(StaleLeaseError, match="current lease"):
            await complete_activity(pool, task=activity_task, result="too late")

        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                select status, cancellation_requested_at, cancellation_reason
                from workflow_executions where id = $1
                """,
                execution.workflow_id,
            )
            events = await connection.fetch(
                """
                select event_type, attributes
                from history_events where workflow_id = $1 order by seq
                """,
                execution.workflow_id,
            )
            open_tasks = await connection.fetchval(
                """
                select count(*) from tasks
                where workflow_id = $1 and status in ('pending', 'leased')
                """,
                execution.workflow_id,
            )
        assert row is not None and row["status"] == "terminated"
        assert row["cancellation_requested_at"] is not None
        assert row["cancellation_reason"] == "operator request"
        assert [event["event_type"] for event in events] == [
            "WorkflowExecutionStarted",
            "ActivityScheduled",
            "ActivityStarted",
            "WorkflowCancellationRequested",
            "WorkflowExecutionTerminated",
        ]
        assert json.loads(events[-1]["attributes"]) == {
            "cause": "cancellation",
            "reason": "operator request",
        }
        assert open_tasks == 0
    finally:
        await pool.close()
