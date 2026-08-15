"""Concurrent task leasing with PostgreSQL row locks and lease-token fencing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

from engine.persistence.database import Connection, Pool
from engine.runtime.serialization import JSONValue, canonical_json

TASK_TYPES = frozenset({"workflow", "activity", "timer"})


class StaleLeaseError(RuntimeError):
    """Raised when a task no longer owns a live lease token."""


@dataclass(frozen=True, slots=True)
class LeasedTask:
    id: UUID
    workflow_id: UUID
    task_type: str
    queue_name: str
    entity_id: UUID | None
    command_id: int | None
    attempt: int
    input: JSONValue
    lease_token: UUID
    leased_at: datetime
    lease_expires_at: datetime
    start_to_close_deadline: datetime | None


async def renew_lease(
    pool: Pool,
    *,
    task_id: UUID,
    lease_token: UUID,
    lease_duration: timedelta = timedelta(seconds=30),
) -> datetime:
    """Renew a live lease without allowing an expired owner to resurrect itself."""
    if lease_duration.total_seconds() <= 0:
        raise ValueError("lease_duration must be positive")
    async with pool.acquire() as connection:
        expires_at = await connection.fetchval(
            """
            update tasks
            set lease_expires_at = now() + $3::interval
            where id = $1
              and status = 'leased'
              and lease_token = $2
              and lease_expires_at > now()
            returning lease_expires_at
            """,
            task_id,
            lease_token,
            lease_duration,
        )
    if expires_at is None:
        raise StaleLeaseError(f"task {task_id} does not hold a live lease")
    return cast(datetime, expires_at)


async def heartbeat_activity(
    pool: Pool,
    *,
    task_id: UUID,
    lease_token: UUID,
    details: JSONValue = None,
    lease_duration: timedelta = timedelta(seconds=30),
) -> datetime:
    """Record activity progress and renew its lease, but never its execution timeout."""
    if lease_duration.total_seconds() <= 0:
        raise ValueError("lease_duration must be positive")
    encoded_details = canonical_json(details)
    async with pool.acquire() as connection:
        expires_at = await connection.fetchval(
            """
            update tasks
            set heartbeat_at = now(),
                heartbeat_details = $3::jsonb,
                lease_expires_at = now() + $4::interval
            where id = $1
              and task_type = 'activity'
              and status = 'leased'
              and lease_token = $2
              and lease_expires_at > now()
            returning lease_expires_at
            """,
            task_id,
            lease_token,
            encoded_details,
            lease_duration,
        )
    if expires_at is None:
        raise StaleLeaseError(f"activity task {task_id} does not hold a live lease")
    return cast(datetime, expires_at)


async def reclaim_expired_workflow_tasks(pool: Pool, *, limit: int = 100) -> int:
    """Return expired workflow tasks to pending without emitting history events."""
    if limit < 1:
        raise ValueError("limit must be at least 1")
    async with pool.acquire() as connection, connection.transaction():
        reclaimed = await connection.fetch(
            """
            with expired as (
              select t.id
              from tasks t
              join workflow_executions e on e.id = t.workflow_id
              where t.task_type = 'workflow'
                and t.status = 'leased'
                and t.lease_expires_at <= now()
                and e.status = 'running'
              order by t.lease_expires_at, t.id
              for update of t skip locked
              limit $1
            )
            update tasks t
            set status = 'pending',
                visible_at = now(),
                leased_at = null,
                lease_token = null,
                lease_expires_at = null,
                start_to_close_deadline = null,
                heartbeat_at = null,
                heartbeat_details = null
            from expired
            where t.id = expired.id
            returning t.id
            """,
            limit,
        )
    return len(reclaimed)


async def release_workflow_task(
    pool: Pool,
    *,
    task: LeasedTask,
    retry_delay: timedelta = timedelta(seconds=1),
) -> None:
    """Release a workflow lease after a worker-local routing failure."""
    if task.task_type != "workflow":
        raise ValueError("only workflow tasks can be released for version routing")
    if retry_delay.total_seconds() < 0:
        raise ValueError("retry_delay cannot be negative")
    async with pool.acquire() as connection:
        result = await connection.execute(
            """
            update tasks
            set status = 'pending',
                visible_at = now() + $3::interval,
                leased_at = null,
                lease_token = null,
                lease_expires_at = null
            where id = $1
              and status = 'leased'
              and lease_token = $2
              and lease_expires_at > now()
            """,
            task.id,
            task.lease_token,
            retry_delay,
        )
    if result != "UPDATE 1":
        raise StaleLeaseError(f"workflow task {task.id} does not hold a live lease")


def _decode_json(value: object) -> JSONValue:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"expected PostgreSQL JSON text, received {type(value).__name__}")
    return cast(JSONValue, json.loads(value))


async def _lease_candidate(
    connection: Connection,
    *,
    task_type: str,
    queue_name: str,
) -> dict[str, object] | None:
    row = await connection.fetchrow(
        """
        select t.*
        from tasks t
        join workflow_executions e on e.id = t.workflow_id
        where t.status = 'pending'
          and t.task_type = $1::task_type
          and t.queue_name = $2
          and t.visible_at <= now()
          and e.status = 'running'
        order by t.visible_at, t.created_at, t.id
        for update of t skip locked
        limit 1
        """,
        task_type,
        queue_name,
    )
    return dict(row) if row is not None else None


async def lease_task(
    pool: Pool,
    *,
    task_type: str,
    queue_name: str = "default",
    lease_duration: timedelta = timedelta(seconds=30),
) -> LeasedTask | None:
    """Lease one runnable task; concurrent pollers skip rows already being claimed."""
    if task_type not in TASK_TYPES:
        raise ValueError(f"unknown task type: {task_type!r}")
    if not queue_name:
        raise ValueError("queue_name cannot be empty")
    if lease_duration.total_seconds() <= 0:
        raise ValueError("lease_duration must be positive")

    lease_token = uuid4()
    async with pool.acquire() as connection, connection.transaction():
        candidate = await _lease_candidate(
            connection,
            task_type=task_type,
            queue_name=queue_name,
        )
        if candidate is None:
            return None

        execution_seq: int | None = None
        if task_type == "activity":
            execution = await connection.fetchrow(
                """
                select status, next_seq
                from workflow_executions
                where id = $1
                for update
                """,
                candidate["workflow_id"],
            )
            if execution is None or execution["status"] != "running":
                return None
            execution_seq = cast(int, execution["next_seq"])

        leased = await connection.fetchrow(
            """
            update tasks
            set status = 'leased',
                leased_at = now(),
                lease_token = $2,
                lease_expires_at = now() + $3::interval,
                start_to_close_deadline = case
                  when task_type = 'activity'
                    then now() + coalesce(start_to_close_timeout, $3::interval)
                  else null
                end,
                heartbeat_at = case
                  when task_type = 'activity' and heartbeat_timeout is not null then now()
                  else heartbeat_at
                end
            where id = $1 and status = 'pending'
            returning *
            """,
            candidate["id"],
            lease_token,
            lease_duration,
        )
        if leased is None:
            return None

        if task_type == "activity":
            assert execution_seq is not None
            attributes = canonical_json(
                {
                    "attempt": cast(int, leased["attempt"]),
                    "task_id": str(leased["id"]),
                }
            )
            await connection.execute(
                """
                insert into history_events (
                  workflow_id, seq, event_type, entity_id, attributes
                ) values ($1, $2, 'ActivityStarted', $3, $4::jsonb)
                """,
                leased["workflow_id"],
                execution_seq,
                leased["entity_id"],
                attributes,
            )
            await connection.execute(
                "update workflow_executions set next_seq = $2 where id = $1",
                leased["workflow_id"],
                execution_seq + 1,
            )

    return LeasedTask(
        id=cast(UUID, leased["id"]),
        workflow_id=cast(UUID, leased["workflow_id"]),
        task_type=str(leased["task_type"]),
        queue_name=cast(str, leased["queue_name"]),
        entity_id=cast(UUID | None, leased["entity_id"]),
        command_id=cast(int | None, leased["command_id"]),
        attempt=cast(int, leased["attempt"]),
        input=_decode_json(leased["input"]),
        lease_token=cast(UUID, leased["lease_token"]),
        leased_at=cast(datetime, leased["leased_at"]),
        lease_expires_at=cast(datetime, leased["lease_expires_at"]),
        start_to_close_deadline=cast(datetime | None, leased["start_to_close_deadline"]),
    )
