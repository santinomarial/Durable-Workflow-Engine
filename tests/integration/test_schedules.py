import os
from datetime import UTC, datetime, timedelta

import pytest

from engine.persistence import (
    backfill_schedule,
    create_pool,
    create_schedule,
    get_execution,
    list_schedule_occurrences,
    materialize_due_schedule,
    register_workflow_definition,
    set_schedule_paused,
    terminate_workflow,
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


@workflow(version=1, name="durable-schedule-e2e")
async def scheduled_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.wait_signal("finish")
    return value


async def test_schedule_materialization_overlap_pause_and_backfill() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    base = datetime(2030, 1, 1, 0, 0, 30, tzinfo=UTC)
    try:
        await register_workflow_definition(pool, scheduled_workflow)
        schedule = await create_schedule(
            pool,
            name="five-minute-orders",
            workflow_type=scheduled_workflow.name,
            definition_version=1,
            workflow_input={"scheduled": True},
            queue_name="schedule-e2e-queue",
            search_attributes={"team": "orders"},
            cron_expression="*/5 * * * *",
            overlap_policy="skip",
            clock_time=base,
        )
        assert schedule.next_run_at == datetime(2030, 1, 1, 0, 5, tzinfo=UTC)

        assert await materialize_due_schedule(
            pool, clock_time=datetime(2030, 1, 1, 0, 5, tzinfo=UTC)
        )
        first = await list_schedule_occurrences(pool, schedule_id=schedule.id)
        assert len(first) == 1
        assert first[0].status == "started"
        assert first[0].workflow_id is not None
        execution = await get_execution(pool, first[0].workflow_id)
        assert execution is not None
        assert execution.schedule_id == schedule.id
        assert execution.search_attributes["dwe.schedule_name"] == "five-minute-orders"

        assert await materialize_due_schedule(
            pool, clock_time=datetime(2030, 1, 1, 0, 10, tzinfo=UTC)
        )
        occurrences = await list_schedule_occurrences(pool, schedule_id=schedule.id)
        assert [item.status for item in occurrences] == ["skipped", "started"]

        assert await set_schedule_paused(
            pool,
            schedule_id=schedule.id,
            paused=True,
            clock_time=datetime(2030, 1, 1, 0, 11, tzinfo=UTC),
        )
        assert not await materialize_due_schedule(
            pool, clock_time=datetime(2030, 1, 1, 0, 15, tzinfo=UTC)
        )

        buffered_schedule = await create_schedule(
            pool,
            name="buffered-orders",
            workflow_type=scheduled_workflow.name,
            definition_version=1,
            workflow_input={"buffered": True},
            queue_name="schedule-buffer-e2e-queue",
            cron_expression="*/5 * * * *",
            overlap_policy="buffer_one",
            clock_time=base,
        )
        assert await materialize_due_schedule(
            pool, clock_time=datetime(2030, 1, 1, 0, 5, tzinfo=UTC)
        )
        buffered_first = await list_schedule_occurrences(pool, schedule_id=buffered_schedule.id)
        assert buffered_first[0].workflow_id is not None
        assert await materialize_due_schedule(
            pool, clock_time=datetime(2030, 1, 1, 0, 10, tzinfo=UTC)
        )
        buffered_waiting = await list_schedule_occurrences(pool, schedule_id=buffered_schedule.id)
        assert [item.status for item in buffered_waiting] == ["buffered", "started"]
        await terminate_workflow(
            pool, workflow_id=buffered_first[0].workflow_id, reason="release buffer"
        )
        assert await materialize_due_schedule(
            pool, clock_time=datetime(2030, 1, 1, 0, 10, tzinfo=UTC)
        )
        buffered_started = await list_schedule_occurrences(pool, schedule_id=buffered_schedule.id)
        assert [item.status for item in buffered_started] == ["started", "started"]
        await set_schedule_paused(
            pool,
            schedule_id=buffered_schedule.id,
            paused=True,
            clock_time=datetime(2030, 1, 1, 0, 11, tzinfo=UTC),
        )

        backfilled = await backfill_schedule(
            pool,
            schedule_id=schedule.id,
            start_at=datetime(2029, 12, 31, 23, 50, tzinfo=UTC),
            end_at=datetime(2029, 12, 31, 23, 55, tzinfo=UTC),
        )
        assert backfilled == 2
        assert first[0].workflow_id is not None
        await terminate_workflow(pool, workflow_id=first[0].workflow_id, reason="test cleanup")
        assert not await set_schedule_paused(
            pool, schedule_id=schedule.id, paused=True, clock_time=base + timedelta(hours=1)
        )
    finally:
        await pool.close()
