"""Single-step lease recovery and timeout maintenance worker."""

from __future__ import annotations

from engine.persistence import (
    Pool,
    fire_due_timer,
    process_activity_timeout,
    reclaim_expired_workflow_tasks,
)


async def run_maintenance(
    pool: Pool,
    *,
    queue_name: str | None = None,
    workflow_reclaim_limit: int = 100,
) -> int:
    """Run one bounded maintenance pass and return the number of transitions."""
    reclaimed = await reclaim_expired_workflow_tasks(pool, limit=workflow_reclaim_limit)
    timed_out = await process_activity_timeout(pool, queue_name=queue_name)
    timer_fired = await fire_due_timer(pool, queue_name=queue_name)
    return reclaimed + int(timed_out) + int(timer_fired)
