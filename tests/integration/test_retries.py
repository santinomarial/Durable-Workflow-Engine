import json
import os
from datetime import timedelta

import pytest

from engine.persistence import create_pool, register_workflow_definition, start_workflow
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import RetryPolicy, WorkflowContext, activity, workflow
from engine.workers import run_activity_task, run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]

attempts = 0


@activity(name="eventually-succeeds")
async def eventually_succeeds(value: JSONValue) -> JSONValue:
    global attempts
    attempts += 1
    if attempts < 3:
        raise RuntimeError(f"attempt {attempts} failed")
    return value


@workflow(version=1, name="retry-e2e")
async def retry_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        eventually_succeeds,
        value,
        retry=RetryPolicy(max_attempts=3, initial_interval=timedelta(0)),
    )


async def test_failed_attempts_retry_with_same_entity_then_complete() -> None:
    global attempts
    attempts = 0
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(retry_workflow)
    registry.register_activity(eventually_succeeds)
    try:
        await register_workflow_definition(pool, retry_workflow)
        started = await start_workflow(
            pool,
            workflow_type=retry_workflow.name,
            definition_version=1,
            workflow_input="success",
            queue_name="retry-e2e-queue",
        )
        assert await run_workflow_task(pool, registry, queue_name="retry-e2e-queue")
        assert await run_activity_task(pool, registry, queue_name="retry-e2e-queue")
        assert await run_activity_task(pool, registry, queue_name="retry-e2e-queue")
        assert await run_activity_task(pool, registry, queue_name="retry-e2e-queue")
        assert await run_workflow_task(pool, registry, queue_name="retry-e2e-queue")

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, result from workflow_executions where id = $1",
                started.workflow_id,
            )
            events = await connection.fetch(
                """
                select event_type, entity_id, attributes
                from history_events where workflow_id = $1 order by seq
                """,
                started.workflow_id,
            )
            activity_tasks = await connection.fetch(
                """
                select attempt, entity_id, status from tasks
                where workflow_id = $1 and task_type = 'activity' order by attempt
                """,
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "completed"
        assert json.loads(execution["result"]) == "success"
        assert [event["event_type"] for event in events] == [
            "WorkflowExecutionStarted",
            "ActivityScheduled",
            "ActivityStarted",
            "ActivityFailed",
            "ActivityStarted",
            "ActivityFailed",
            "ActivityStarted",
            "ActivityCompleted",
            "WorkflowExecutionCompleted",
        ]
        failures = [
            json.loads(event["attributes"])
            for event in events
            if event["event_type"] == "ActivityFailed"
        ]
        assert [failure["final"] for failure in failures] == [False, False]
        assert [task["attempt"] for task in activity_tasks] == [1, 2, 3]
        assert len({task["entity_id"] for task in activity_tasks}) == 1
        assert [task["status"] for task in activity_tasks] == [
            "completed",
            "completed",
            "completed",
        ]
    finally:
        await pool.close()


@activity(name="always-fails")
async def always_fails(value: JSONValue) -> JSONValue:
    raise ValueError(f"rejected {value}")


@workflow(version=1, name="final-failure-e2e")
async def final_failure_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        always_fails,
        value,
        retry=RetryPolicy(max_attempts=2, initial_interval=timedelta(0)),
    )


async def test_final_attempt_wakes_and_fails_workflow_with_dead_task() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(final_failure_workflow)
    registry.register_activity(always_fails)
    try:
        await register_workflow_definition(pool, final_failure_workflow)
        started = await start_workflow(
            pool,
            workflow_type=final_failure_workflow.name,
            definition_version=1,
            workflow_input="payload",
            queue_name="final-failure-e2e-queue",
        )
        assert await run_workflow_task(pool, registry, queue_name="final-failure-e2e-queue")
        assert await run_activity_task(pool, registry, queue_name="final-failure-e2e-queue")
        assert await run_activity_task(pool, registry, queue_name="final-failure-e2e-queue")
        assert await run_workflow_task(pool, registry, queue_name="final-failure-e2e-queue")
        assert not await run_activity_task(pool, registry, queue_name="final-failure-e2e-queue")

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, failure from workflow_executions where id = $1",
                started.workflow_id,
            )
            failures = await connection.fetch(
                """
                select attributes from history_events
                where workflow_id = $1 and event_type = 'ActivityFailed' order by seq
                """,
                started.workflow_id,
            )
            attempts_rows = await connection.fetch(
                """
                select attempt, status from tasks
                where workflow_id = $1 and task_type = 'activity' order by attempt
                """,
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "failed"
        workflow_failure = json.loads(execution["failure"])
        assert workflow_failure["type"] == "ActivityError"
        assert [json.loads(row["attributes"])["final"] for row in failures] == [False, True]
        assert [(row["attempt"], row["status"]) for row in attempts_rows] == [
            (1, "completed"),
            (2, "dead"),
        ]
    finally:
        await pool.close()
