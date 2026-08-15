import os

import pytest

from engine.persistence import (
    create_pool,
    lease_task,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.replay import ReplayStatus
from engine.runtime.replay_check import replay_check
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


@activity(name="replay-check-activity")
async def checked_activity(value: JSONValue) -> JSONValue:
    return value


@workflow(version=1, name="replay-check-workflow")
async def checked_v1(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(checked_activity, value)


@workflow(version=2, name="replay-check-workflow")
async def incompatible_v2(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(checked_activity, {"changed": value})


@workflow(version=1, name="terminal-result-check")
async def terminal_result_v1(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"value": value}


@workflow(version=2, name="terminal-result-check")
async def changed_terminal_result_v2(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"changed": value}


async def test_replay_check_reports_first_divergence_without_mutation() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(checked_v1)
    registry.register_activity(checked_activity)
    try:
        await register_workflow_definition(pool, checked_v1)
        started = await start_workflow(
            pool,
            workflow_type=checked_v1.name,
            definition_version=1,
            workflow_input="original",
            queue_name="replay-check-queue",
        )
        assert await run_workflow_task(pool, registry, queue_name="replay-check-queue")
        async with pool.acquire() as connection:
            before = await connection.fetchval(
                """
                select md5(string_agg(row_to_json(snapshot)::text, ',' order by snapshot.seq))
                from (
                  select seq, event_type, command_id, entity_id, external_id, attributes
                  from history_events where workflow_id = $1 order by seq
                ) snapshot
                """,
                started.workflow_id,
            )

        compatible = await replay_check(
            pool, workflow_id=started.workflow_id, definition=checked_v1
        )
        incompatible = await replay_check(
            pool, workflow_id=started.workflow_id, definition=incompatible_v2
        )

        assert compatible.compatible is True
        assert compatible.replay_status is ReplayStatus.BLOCKED
        assert incompatible.compatible is False
        assert incompatible.divergence_command_id == 0
        async with pool.acquire() as connection:
            after = await connection.fetchval(
                """
                select md5(string_agg(row_to_json(snapshot)::text, ',' order by snapshot.seq))
                from (
                  select seq, event_type, command_id, entity_id, external_id, attributes
                  from history_events where workflow_id = $1 order by seq
                ) snapshot
                """,
                started.workflow_id,
            )
        assert after == before
        assert await lease_task(pool, task_type="activity", queue_name="replay-check-queue")
    finally:
        await pool.close()


async def test_replay_check_rejects_changed_terminal_result_without_commands() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    registry = DefinitionRegistry()
    registry.register_workflow(terminal_result_v1)
    try:
        await register_workflow_definition(pool, terminal_result_v1)
        started = await start_workflow(
            pool,
            workflow_type=terminal_result_v1.name,
            definition_version=1,
            workflow_input="original",
            queue_name="terminal-replay-check",
        )
        assert await run_workflow_task(pool, registry, queue_name="terminal-replay-check")

        compatible = await replay_check(
            pool, workflow_id=started.workflow_id, definition=terminal_result_v1
        )
        changed = await replay_check(
            pool,
            workflow_id=started.workflow_id,
            definition=changed_terminal_result_v2,
        )

        assert compatible.compatible is True
        assert changed.compatible is False
        assert changed.replay_status is ReplayStatus.COMPLETED
        assert changed.divergence_command_id is None
        assert changed.message == "replayed result differs from the stored terminal result"
    finally:
        await pool.close()
