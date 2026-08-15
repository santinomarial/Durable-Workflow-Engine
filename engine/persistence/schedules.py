"""Durable cron schedules and atomic occurrence materialization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

from engine.persistence.audit import AuditContext, record_api_audit
from engine.persistence.database import Connection, Pool
from engine.persistence.transitions import DefinitionNotRegisteredError, TransitionError
from engine.runtime.cron import next_cron_time, parse_cron
from engine.runtime.serialization import JSONValue, canonical_json, clone_json

type OverlapPolicy = Literal["allow", "skip", "buffer_one"]


@dataclass(frozen=True, slots=True)
class WorkflowSchedule:
    id: UUID
    name: str
    workflow_type: str
    definition_version: int
    input: JSONValue
    queue_name: str
    search_attributes: dict[str, JSONValue]
    cron_expression: str
    timezone: str
    overlap_policy: str
    next_run_at: datetime
    last_run_at: datetime | None
    paused_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ScheduleOccurrence:
    schedule_id: UUID
    scheduled_at: datetime
    status: str
    workflow_id: UUID | None
    reason: str | None
    created_at: datetime


def _json(value: object) -> JSONValue:
    if value is None:
        return None
    return cast(JSONValue, json.loads(cast(str, value)))


def _schedule(row: dict[str, object]) -> WorkflowSchedule:
    attributes = _json(row["search_attributes"])
    if not isinstance(attributes, dict):
        raise TypeError("stored schedule search attributes must be an object")
    return WorkflowSchedule(
        id=cast(UUID, row["id"]),
        name=cast(str, row["name"]),
        workflow_type=cast(str, row["workflow_type"]),
        definition_version=cast(int, row["definition_version"]),
        input=_json(row["input"]),
        queue_name=cast(str, row["queue_name"]),
        search_attributes=attributes,
        cron_expression=cast(str, row["cron_expression"]),
        timezone=cast(str, row["timezone"]),
        overlap_policy=str(row["overlap_policy"]),
        next_run_at=cast(datetime, row["next_run_at"]),
        last_run_at=cast(datetime | None, row["last_run_at"]),
        paused_at=cast(datetime | None, row["paused_at"]),
        created_at=cast(datetime, row["created_at"]),
        updated_at=cast(datetime, row["updated_at"]),
    )


async def create_schedule(
    pool: Pool,
    *,
    name: str,
    workflow_type: str,
    definition_version: int,
    workflow_input: JSONValue,
    cron_expression: str,
    timezone: str = "UTC",
    queue_name: str = "default",
    search_attributes: dict[str, JSONValue] | None = None,
    overlap_policy: OverlapPolicy = "skip",
    clock_time: datetime | None = None,
    audit: AuditContext | None = None,
) -> WorkflowSchedule:
    if not name or len(name) > 200:
        raise ValueError("schedule name must contain 1 to 200 characters")
    if not queue_name or len(queue_name) > 200:
        raise ValueError("queue_name must contain 1 to 200 characters")
    if definition_version < 1:
        raise ValueError("definition_version must be at least 1")
    parse_cron(cron_expression)
    now = clock_time or datetime.now(UTC)
    next_run_at = next_cron_time(cron_expression, after=now, timezone_name=timezone)
    detached_input = clone_json(workflow_input)
    detached_attributes = clone_json(search_attributes or {})
    if not isinstance(detached_attributes, dict):
        raise ValueError("search_attributes must be a JSON object")
    if len(canonical_json(detached_attributes).encode()) > 14_000:
        raise ValueError("schedule search_attributes cannot exceed 14 KB")
    schedule_id = uuid4()
    async with pool.acquire() as connection, connection.transaction():
        definition_exists = await connection.fetchval(
            """
            select exists (
              select 1 from workflow_definitions
              where workflow_type = $1 and version = $2
            )
            """,
            workflow_type,
            definition_version,
        )
        if definition_exists is not True:
            raise DefinitionNotRegisteredError(
                f"workflow {workflow_type!r} version {definition_version} is not registered"
            )
        row = await connection.fetchrow(
            """
            insert into workflow_schedules (
              id, name, workflow_type, definition_version, input, queue_name,
              search_attributes, cron_expression, timezone, overlap_policy,
              next_run_at, created_at, updated_at
            ) values (
              $1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8, $9,
              $10::schedule_overlap_policy, $11, $12, $12
            ) returning *
            """,
            schedule_id,
            name,
            workflow_type,
            definition_version,
            canonical_json(detached_input),
            queue_name,
            canonical_json(detached_attributes),
            cron_expression,
            timezone,
            overlap_policy,
            next_run_at,
            now,
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=None,
            accepted=True,
            details={"schedule_id": str(schedule_id), "name": name},
        )
    if row is None:
        raise RuntimeError("schedule insert returned no row")
    return _schedule(dict(row))


async def list_schedules(pool: Pool) -> tuple[WorkflowSchedule, ...]:
    async with pool.acquire() as connection:
        rows = await connection.fetch("select * from workflow_schedules order by name, id")
    return tuple(_schedule(dict(row)) for row in rows)


async def list_schedule_occurrences(
    pool: Pool, *, schedule_id: UUID, limit: int = 100
) -> tuple[ScheduleOccurrence, ...]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select schedule_id, scheduled_at, status, workflow_id, reason, created_at
            from schedule_occurrences where schedule_id = $1
            order by scheduled_at desc limit $2
            """,
            schedule_id,
            limit,
        )
    return tuple(
        ScheduleOccurrence(
            schedule_id=cast(UUID, row["schedule_id"]),
            scheduled_at=cast(datetime, row["scheduled_at"]),
            status=str(row["status"]),
            workflow_id=cast(UUID | None, row["workflow_id"]),
            reason=cast(str | None, row["reason"]),
            created_at=cast(datetime, row["created_at"]),
        )
        for row in rows
    )


async def set_schedule_paused(
    pool: Pool,
    *,
    schedule_id: UUID,
    paused: bool,
    clock_time: datetime | None = None,
    audit: AuditContext | None = None,
) -> bool:
    now = clock_time or datetime.now(UTC)
    async with pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow(
            "select * from workflow_schedules where id = $1 for update", schedule_id
        )
        if row is None:
            raise TransitionError(f"schedule {schedule_id} does not exist")
        currently_paused = row["paused_at"] is not None
        if currently_paused == paused:
            await record_api_audit(
                connection,
                audit,
                workflow_id=None,
                accepted=False,
                details={"schedule_id": str(schedule_id), "duplicate": True},
            )
            return False
        next_run_at = row["next_run_at"]
        if not paused:
            next_run_at = next_cron_time(
                cast(str, row["cron_expression"]),
                after=now,
                timezone_name=cast(str, row["timezone"]),
            )
        await connection.execute(
            """
            update workflow_schedules
            set paused_at = case when $2 then $3::timestamptz else null end,
                next_run_at = $4,
                updated_at = $3
            where id = $1
            """,
            schedule_id,
            paused,
            now,
            next_run_at,
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=None,
            accepted=True,
            details={"schedule_id": str(schedule_id), "paused": paused},
        )
    return True


async def _start_occurrence(
    connection: Connection,
    *,
    schedule: dict[str, object],
    scheduled_at: datetime,
    buffered: bool = False,
) -> UUID:
    workflow_id = uuid4()
    started_at = datetime.now(UTC)
    schedule_id = cast(UUID, schedule["id"])
    base_attributes = _json(schedule["search_attributes"])
    if not isinstance(base_attributes, dict):
        raise TypeError("stored schedule search attributes must be an object")
    attributes = {
        **base_attributes,
        "dwe.schedule_id": str(schedule_id),
        "dwe.schedule_name": cast(str, schedule["name"]),
        "dwe.scheduled_at": scheduled_at.isoformat(),
    }
    workflow_input = _json(schedule["input"])
    await connection.execute(
        """
        insert into workflow_executions (
          id, workflow_type, definition_version, input, next_seq, queue_name,
          created_at, search_attributes, schedule_id, scheduled_at
        ) values ($1, $2, $3, $4::jsonb, 2, $5, $6, $7::jsonb, $8, $9)
        """,
        workflow_id,
        schedule["workflow_type"],
        schedule["definition_version"],
        canonical_json(workflow_input),
        schedule["queue_name"],
        started_at,
        canonical_json(attributes),
        schedule_id,
        scheduled_at,
    )
    await connection.execute(
        """
        insert into history_events (workflow_id, seq, event_type, attributes)
        values ($1, 1, 'WorkflowExecutionStarted', $2::jsonb)
        """,
        workflow_id,
        canonical_json(
            {
                "workflow_type": cast(str, schedule["workflow_type"]),
                "definition_version": cast(int, schedule["definition_version"]),
                "input": workflow_input,
                "search_attributes": attributes,
                "started_at": started_at.isoformat(),
                "schedule_id": str(schedule_id),
                "scheduled_at": scheduled_at.isoformat(),
            }
        ),
    )
    await connection.execute(
        """
        insert into tasks (id, workflow_id, task_type, queue_name)
        values ($1, $2, 'workflow', $3)
        """,
        uuid4(),
        workflow_id,
        schedule["queue_name"],
    )
    if buffered:
        await connection.execute(
            """
            update schedule_occurrences
            set status = 'started', workflow_id = $3, reason = null
            where schedule_id = $1 and scheduled_at = $2 and status = 'buffered'
            """,
            schedule_id,
            scheduled_at,
            workflow_id,
        )
    else:
        await connection.execute(
            """
            insert into schedule_occurrences (
              schedule_id, scheduled_at, status, workflow_id
            ) values ($1, $2, 'started', $3)
            """,
            schedule_id,
            scheduled_at,
            workflow_id,
        )
    return workflow_id


async def materialize_due_schedule(pool: Pool, *, clock_time: datetime | None = None) -> bool:
    """Atomically materialize one due or buffered schedule occurrence."""
    now = clock_time or datetime.now(UTC)
    async with pool.acquire() as connection, connection.transaction():
        buffered = await connection.fetchrow(
            """
            select s.*, o.scheduled_at as buffered_at
            from schedule_occurrences o
            join workflow_schedules s on s.id = o.schedule_id
            where o.status = 'buffered' and s.paused_at is null
              and not exists (
                select 1 from workflow_executions e
                where e.schedule_id = s.id and e.status = 'running'
              )
            order by o.scheduled_at, s.id
            for update of s, o skip locked limit 1
            """
        )
        if buffered is not None:
            await _start_occurrence(
                connection,
                schedule=dict(buffered),
                scheduled_at=cast(datetime, buffered["buffered_at"]),
                buffered=True,
            )
            return True

        row = await connection.fetchrow(
            """
            select * from workflow_schedules
            where paused_at is null and next_run_at <= $1
            order by next_run_at, id
            for update skip locked limit 1
            """,
            now,
        )
        if row is None:
            return False
        schedule = dict(row)
        schedule_id = cast(UUID, row["id"])
        scheduled_at = cast(datetime, row["next_run_at"])
        next_run_at = next_cron_time(
            cast(str, row["cron_expression"]),
            after=scheduled_at,
            timezone_name=cast(str, row["timezone"]),
        )
        active = cast(
            bool,
            await connection.fetchval(
                """
                select exists (
                  select 1 from workflow_executions
                  where schedule_id = $1 and status = 'running'
                )
                """,
                schedule_id,
            ),
        )
        policy = str(row["overlap_policy"])
        if active and policy != "allow":
            status = "skipped"
            reason = "overlap policy skipped active occurrence"
            if policy == "buffer_one":
                already_buffered = cast(
                    bool,
                    await connection.fetchval(
                        """
                        select exists (
                          select 1 from schedule_occurrences
                          where schedule_id = $1 and status = 'buffered'
                        )
                        """,
                        schedule_id,
                    ),
                )
                if not already_buffered:
                    status = "buffered"
                    reason = "waiting for active execution"
            await connection.execute(
                """
                insert into schedule_occurrences (schedule_id, scheduled_at, status, reason)
                values ($1, $2, $3::schedule_occurrence_status, $4)
                """,
                schedule_id,
                scheduled_at,
                status,
                reason,
            )
        else:
            await _start_occurrence(connection, schedule=schedule, scheduled_at=scheduled_at)
        await connection.execute(
            """
            update workflow_schedules
            set next_run_at = $2, last_run_at = $3, updated_at = $1
            where id = $4
            """,
            now,
            next_run_at,
            scheduled_at,
            schedule_id,
        )
    return True


async def backfill_schedule(
    pool: Pool,
    *,
    schedule_id: UUID,
    start_at: datetime,
    end_at: datetime,
    limit: int = 100,
    audit: AuditContext | None = None,
) -> int:
    """Start bounded historical occurrences, preserving uniqueness by scheduled minute."""
    if start_at.tzinfo is None or end_at.tzinfo is None:
        raise ValueError("backfill timestamps must be timezone aware")
    if end_at < start_at:
        raise ValueError("backfill end_at must not precede start_at")
    if limit < 1 or limit > 100:
        raise ValueError("backfill limit must be between 1 and 100")
    async with pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow(
            "select * from workflow_schedules where id = $1 for update", schedule_id
        )
        if row is None:
            raise TransitionError(f"schedule {schedule_id} does not exist")
        occurrences: list[datetime] = []
        cursor = start_at - timedelta(minutes=1)
        while True:
            candidate = next_cron_time(
                cast(str, row["cron_expression"]),
                after=cursor,
                timezone_name=cast(str, row["timezone"]),
            )
            if candidate > end_at:
                break
            occurrences.append(candidate)
            if len(occurrences) > limit:
                raise ValueError(f"backfill exceeds the {limit}-occurrence limit")
            cursor = candidate
        started = 0
        for scheduled_at in occurrences:
            exists = await connection.fetchval(
                """
                select exists (
                  select 1 from schedule_occurrences
                  where schedule_id = $1 and scheduled_at = $2
                )
                """,
                schedule_id,
                scheduled_at,
            )
            if exists is True:
                continue
            await _start_occurrence(connection, schedule=dict(row), scheduled_at=scheduled_at)
            started += 1
        await record_api_audit(
            connection,
            audit,
            workflow_id=None,
            accepted=True,
            details={
                "schedule_id": str(schedule_id),
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "started": started,
            },
        )
    return started
