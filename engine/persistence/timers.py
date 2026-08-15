"""Durable timer firing transitions."""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import uuid4

from engine.persistence.database import Pool
from engine.runtime.serialization import canonical_json


async def fire_due_timer(pool: Pool, *, queue_name: str | None = None) -> bool:
    """Atomically fire at most one due timer and wake its workflow."""
    async with pool.acquire() as connection, connection.transaction():
        timer = await connection.fetchrow(
            """
            select t.*
            from tasks t
            join workflow_executions e on e.id = t.workflow_id
            where t.task_type = 'timer'
              and t.status = 'pending'
              and t.visible_at <= now()
              and e.status = 'running'
              and ($1::text is null or t.queue_name = $1)
            order by t.visible_at, t.id
            for update of t skip locked
            limit 1
            """,
            queue_name,
        )
        if timer is None:
            return False
        execution = await connection.fetchrow(
            """
            select status, next_seq
            from workflow_executions where id = $1 for update
            """,
            timer["workflow_id"],
        )
        if execution is None or execution["status"] != "running":
            return False
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, entity_id, attributes
            ) values ($1, $2, 'TimerFired', $3, $4::jsonb)
            """,
            timer["workflow_id"],
            next_seq,
            timer["entity_id"],
            canonical_json({"scheduled_for": cast(datetime, timer["visible_at"]).isoformat()}),
        )
        await connection.execute(
            "update tasks set status = 'completed', completed_at = now() where id = $1",
            timer["id"],
        )
        await connection.execute(
            """
            insert into tasks (id, workflow_id, task_type, queue_name)
            values ($1, $2, 'workflow', $3)
            """,
            uuid4(),
            timer["workflow_id"],
            timer["queue_name"],
        )
        await connection.execute(
            "update workflow_executions set next_seq = $2 where id = $1",
            timer["workflow_id"],
            next_seq + 1,
        )
    return True
