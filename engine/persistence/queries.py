"""Read-only execution and history inspection queries."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from engine.persistence.database import Pool
from engine.runtime.serialization import JSONValue


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
    id: UUID
    workflow_type: str
    definition_version: int
    queue_name: str
    status: str
    input: JSONValue
    result: JSONValue
    failure: JSONValue
    next_seq: int
    created_at: datetime
    closed_at: datetime | None
    cancellation_requested_at: datetime | None
    cancellation_reason: str | None
    search_attributes: dict[str, JSONValue]
    paused_at: datetime | None
    pause_reason: str | None
    retry_of: UUID | None
    schedule_id: UUID | None
    scheduled_at: datetime | None
    parent_workflow_id: UUID | None
    parent_command_id: int | None
    parent_close_policy: str | None


@dataclass(frozen=True, slots=True)
class ExecutionStats:
    total: int
    running: int
    completed: int
    failed: int
    terminated: int


@dataclass(frozen=True, slots=True)
class HistoryRecord:
    seq: int
    event_type: str
    command_id: int | None
    entity_id: UUID | None
    external_id: str | None
    attributes: JSONValue
    created_at: datetime


@dataclass(frozen=True, slots=True)
class HistoryPage:
    items: tuple[HistoryRecord, ...]
    next_after_seq: int | None


def _json(value: object) -> JSONValue:
    if value is None:
        return None
    return cast(JSONValue, json.loads(cast(str, value)))


def _execution(row: dict[str, object]) -> ExecutionSummary:
    return ExecutionSummary(
        id=cast(UUID, row["id"]),
        workflow_type=cast(str, row["workflow_type"]),
        definition_version=cast(int, row["definition_version"]),
        queue_name=cast(str, row["queue_name"]),
        status=str(row["status"]),
        input=_json(row["input"]),
        result=_json(row["result"]),
        failure=_json(row["failure"]),
        next_seq=cast(int, row["next_seq"]),
        created_at=cast(datetime, row["created_at"]),
        closed_at=cast(datetime | None, row["closed_at"]),
        cancellation_requested_at=cast(datetime | None, row["cancellation_requested_at"]),
        cancellation_reason=cast(str | None, row["cancellation_reason"]),
        search_attributes=cast(dict[str, JSONValue], _json(row["search_attributes"])),
        paused_at=cast(datetime | None, row["paused_at"]),
        pause_reason=cast(str | None, row["pause_reason"]),
        retry_of=cast(UUID | None, row["retry_of"]),
        schedule_id=cast(UUID | None, row["schedule_id"]),
        scheduled_at=cast(datetime | None, row["scheduled_at"]),
        parent_workflow_id=cast(UUID | None, row["parent_workflow_id"]),
        parent_command_id=cast(int | None, row["parent_command_id"]),
        parent_close_policy=cast(str | None, row["parent_close_policy"]),
    )


async def list_executions(
    pool: Pool,
    *,
    status: str | None = None,
    workflow_type: str | None = None,
    queue_name: str | None = None,
    query: str | None = None,
    search_attributes: dict[str, JSONValue] | None = None,
    limit: int = 100,
) -> tuple[ExecutionSummary, ...]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        encoded_attributes = json.dumps(search_attributes or {}, separators=(",", ":"))
        rows = await connection.fetch(
            """
            select * from workflow_executions
            where (
                $1::text is null
                or ($1 = 'attention' and status in ('failed', 'terminated'))
                or status::text = $1
              )
              and ($2::text is null or workflow_type = $2)
              and ($3::text is null or queue_name = $3)
              and (
                $4::text is null
                or id::text ilike '%' || $4 || '%'
                or workflow_type ilike '%' || $4 || '%'
                or queue_name ilike '%' || $4 || '%'
                or search_attributes::text ilike '%' || $4 || '%'
              )
              and search_attributes @> $5::jsonb
            order by created_at desc, id
            limit $6
            """,
            status,
            workflow_type,
            queue_name,
            query,
            encoded_attributes,
            limit,
        )
    return tuple(_execution(dict(row)) for row in rows)


async def get_execution(pool: Pool, workflow_id: UUID) -> ExecutionSummary | None:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "select * from workflow_executions where id = $1",
            workflow_id,
        )
    return _execution(dict(row)) if row is not None else None


async def get_execution_stats(pool: Pool) -> ExecutionStats:
    """Return exact status counts for the operations console."""
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            select
              count(*) as total,
              count(*) filter (where status = 'running') as running,
              count(*) filter (where status = 'completed') as completed,
              count(*) filter (where status = 'failed') as failed,
              count(*) filter (where status = 'terminated') as terminated
            from workflow_executions
            """
        )
    if row is None:
        raise RuntimeError("execution statistics query returned no row")
    return ExecutionStats(
        total=cast(int, row["total"]),
        running=cast(int, row["running"]),
        completed=cast(int, row["completed"]),
        failed=cast(int, row["failed"]),
        terminated=cast(int, row["terminated"]),
    )


async def get_history(pool: Pool, workflow_id: UUID) -> tuple[HistoryRecord, ...]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select seq, event_type, command_id, entity_id, external_id,
                   attributes, created_at
            from history_events where workflow_id = $1 order by seq
            """,
            workflow_id,
        )
    return tuple(
        HistoryRecord(
            seq=cast(int, row["seq"]),
            event_type=cast(str, row["event_type"]),
            command_id=cast(int | None, row["command_id"]),
            entity_id=cast(UUID | None, row["entity_id"]),
            external_id=cast(str | None, row["external_id"]),
            attributes=_json(row["attributes"]),
            created_at=cast(datetime, row["created_at"]),
        )
        for row in rows
    )


async def get_history_page(
    pool: Pool,
    workflow_id: UUID,
    *,
    after_seq: int = 0,
    limit: int = 500,
) -> HistoryPage:
    """Return a bounded forward page and an opaque monotonic continuation cursor."""
    if after_seq < 0:
        raise ValueError("after_seq cannot be negative")
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select seq, event_type, command_id, entity_id, external_id,
                   attributes, created_at
            from history_events
            where workflow_id = $1 and seq > $2
            order by seq limit $3
            """,
            workflow_id,
            after_seq,
            limit + 1,
        )
    has_more = len(rows) > limit
    selected = rows[:limit]
    items = tuple(
        HistoryRecord(
            seq=cast(int, row["seq"]),
            event_type=cast(str, row["event_type"]),
            command_id=cast(int | None, row["command_id"]),
            entity_id=cast(UUID | None, row["entity_id"]),
            external_id=cast(str | None, row["external_id"]),
            attributes=_json(row["attributes"]),
            created_at=cast(datetime, row["created_at"]),
        )
        for row in selected
    )
    return HistoryPage(
        items=items,
        next_after_seq=items[-1].seq if has_more and items else None,
    )


async def get_history_tail(
    pool: Pool,
    workflow_id: UUID,
    *,
    limit: int = 500,
) -> tuple[HistoryRecord, ...]:
    """Return the latest bounded history slice in chronological order."""
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select seq, event_type, command_id, entity_id, external_id,
                   attributes, created_at
            from history_events where workflow_id = $1
            order by seq desc limit $2
            """,
            workflow_id,
            limit,
        )
    return tuple(
        HistoryRecord(
            seq=cast(int, row["seq"]),
            event_type=cast(str, row["event_type"]),
            command_id=cast(int | None, row["command_id"]),
            entity_id=cast(UUID | None, row["entity_id"]),
            external_id=cast(str | None, row["external_id"]),
            attributes=_json(row["attributes"]),
            created_at=cast(datetime, row["created_at"]),
        )
        for row in reversed(rows)
    )
