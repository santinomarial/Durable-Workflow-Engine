import json
import os
from uuid import uuid4

import pytest

from engine.persistence import (
    commit_workflow_replay,
    create_pool,
    lease_task,
    load_workflow_replay_state,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.persistence.transitions import DefinitionNotRegisteredError, StaleLeaseError
from engine.runtime.replay import ReplayStatus, replay_workflow
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, activity, workflow

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@workflow(version=1, name="transition-test")
async def transition_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


@activity(name="persisted-echo")
async def persisted_echo(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="replay-transition-test")
async def replay_transition_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(persisted_echo, value)


async def test_start_workflow_commits_execution_history_and_task_atomically() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    workflow_id = uuid4()
    try:
        await register_workflow_definition(pool, transition_workflow)
        started = await start_workflow(
            pool,
            workflow_type=transition_workflow.name,
            definition_version=transition_workflow.version,
            workflow_input={"order_id": "order-123"},
            queue_name="orders",
            workflow_id=workflow_id,
        )
        assert started.workflow_id == workflow_id

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select * from workflow_executions where id = $1", workflow_id
            )
            history = await connection.fetch(
                "select * from history_events where workflow_id = $1 order by seq", workflow_id
            )
            tasks = await connection.fetch(
                "select * from tasks where workflow_id = $1 order by created_at", workflow_id
            )

        assert execution is not None
        assert execution["status"] == "running"
        assert execution["next_seq"] == 2
        assert json.loads(execution["input"]) == {"order_id": "order-123"}
        assert [(event["seq"], event["event_type"]) for event in history] == [
            (1, "WorkflowExecutionStarted")
        ]
        assert json.loads(history[0]["attributes"])["definition_version"] == 1
        assert len(tasks) == 1
        assert tasks[0]["task_type"] == "workflow"
        assert tasks[0]["queue_name"] == "orders"
        assert tasks[0]["status"] == "pending"
    finally:
        await pool.close()


async def test_start_workflow_rejects_unknown_definition_without_partial_rows() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    workflow_id = uuid4()
    try:
        with pytest.raises(DefinitionNotRegisteredError, match="is not registered"):
            await start_workflow(
                pool,
                workflow_type="missing-definition",
                definition_version=99,
                workflow_input=None,
                workflow_id=workflow_id,
            )

        async with pool.acquire() as connection:
            execution_count = await connection.fetchval(
                "select count(*) from workflow_executions where id = $1", workflow_id
            )
            history_count = await connection.fetchval(
                "select count(*) from history_events where workflow_id = $1", workflow_id
            )
            task_count = await connection.fetchval(
                "select count(*) from tasks where workflow_id = $1", workflow_id
            )
        assert (execution_count, history_count, task_count) == (0, 0, 0)
    finally:
        await pool.close()


async def test_workflow_replay_atomically_schedules_activity_and_fences_duplicate() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        await register_workflow_definition(pool, replay_transition_workflow)
        started = await start_workflow(
            pool,
            workflow_type=replay_transition_workflow.name,
            definition_version=1,
            workflow_input={"value": 42},
            queue_name="replay-transition-queue",
        )
        task = await lease_task(pool, task_type="workflow", queue_name="replay-transition-queue")
        assert task is not None
        state = await load_workflow_replay_state(pool, task)
        replay = await replay_workflow(
            replay_transition_workflow,
            workflow_id=state.workflow_id,
            workflow_input=state.workflow_input,
            history=state.history,
        )
        assert replay.status is ReplayStatus.COMMANDS

        await commit_workflow_replay(pool, task=task, replay=replay)
        with pytest.raises(StaleLeaseError, match="does not hold the current lease"):
            await commit_workflow_replay(pool, task=task, replay=replay)

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                select event_type, command_id, entity_id
                from history_events where workflow_id = $1 order by seq
                """,
                started.workflow_id,
            )
            activity_tasks = await connection.fetch(
                """
                select status, entity_id, command_id, input
                from tasks
                where workflow_id = $1 and task_type = 'activity'
                """,
                started.workflow_id,
            )
        assert [row["event_type"] for row in rows] == [
            "WorkflowExecutionStarted",
            "ActivityScheduled",
        ]
        assert len(activity_tasks) == 1
        assert activity_tasks[0]["status"] == "pending"
        assert activity_tasks[0]["entity_id"] == rows[1]["entity_id"]
        assert activity_tasks[0]["command_id"] == 0
        assert json.loads(activity_tasks[0]["input"])["idempotency_key"] == str(
            rows[1]["entity_id"]
        )
    finally:
        await pool.close()


async def test_workflow_replay_atomically_closes_execution() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        await register_workflow_definition(pool, transition_workflow)
        started = await start_workflow(
            pool,
            workflow_type=transition_workflow.name,
            definition_version=1,
            workflow_input={"done": True},
            queue_name="terminal-transition-queue",
        )
        task = await lease_task(pool, task_type="workflow", queue_name="terminal-transition-queue")
        assert task is not None
        state = await load_workflow_replay_state(pool, task)
        replay = await replay_workflow(
            transition_workflow,
            workflow_id=state.workflow_id,
            workflow_input=state.workflow_input,
            history=state.history,
        )
        assert replay.status is ReplayStatus.COMPLETED
        await commit_workflow_replay(pool, task=task, replay=replay)

        async with pool.acquire() as connection:
            execution = await connection.fetchrow(
                "select status, result, next_seq, closed_at from workflow_executions where id = $1",
                started.workflow_id,
            )
            event_types = await connection.fetch(
                "select event_type from history_events where workflow_id = $1 order by seq",
                started.workflow_id,
            )
        assert execution is not None
        assert execution["status"] == "completed"
        assert json.loads(execution["result"]) == {"done": True}
        assert execution["next_seq"] == 3
        assert execution["closed_at"] is not None
        assert [row["event_type"] for row in event_types] == [
            "WorkflowExecutionStarted",
            "WorkflowExecutionCompleted",
        ]
    finally:
        await pool.close()
