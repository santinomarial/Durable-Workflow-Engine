"""Operational queue and worker inspection queries."""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from engine.persistence.database import Pool
from engine.runtime.serialization import JSONValue


@dataclass(frozen=True, slots=True)
class WorkerHeartbeat:
    worker_id: UUID
    hostname: str
    process_id: int
    queue_name: str
    roles: tuple[str, ...]
    started_at: datetime
    last_seen_at: datetime
    stopped_at: datetime | None
    healthy: bool


@dataclass(frozen=True, slots=True)
class DeadTask:
    id: UUID
    workflow_id: UUID
    workflow_type: str
    task_type: str
    queue_name: str
    entity_id: UUID | None
    command_id: int | None
    attempt: int
    input: JSONValue
    outcome: JSONValue
    created_at: datetime
    completed_at: datetime


def _json(value: object) -> JSONValue:
    if value is None:
        return None
    return cast(JSONValue, json.loads(cast(str, value)))


async def list_dead_tasks(pool: Pool, *, limit: int = 100) -> tuple[DeadTask, ...]:
    """Return bounded dead-letter inspection records with their terminal outcome."""
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select t.id, t.workflow_id, e.workflow_type, t.task_type, t.queue_name,
                   t.entity_id, t.command_id, t.attempt, t.input, t.created_at,
                   t.completed_at, outcome.attributes as outcome
            from tasks t
            join workflow_executions e on e.id = t.workflow_id
            left join lateral (
              select h.attributes
              from history_events h
              where h.workflow_id = t.workflow_id
                and (t.entity_id is null or h.entity_id = t.entity_id)
                and h.event_type in (
                  'ActivityFailed', 'ActivityTimedOut', 'TimerCanceled',
                  'WorkflowExecutionFailed', 'WorkflowExecutionTerminated'
                )
              order by h.seq desc limit 1
            ) outcome on true
            where t.status = 'dead'
            order by t.completed_at desc, t.id
            limit $1
            """,
            limit,
        )
    return tuple(
        DeadTask(
            id=cast(UUID, row["id"]),
            workflow_id=cast(UUID, row["workflow_id"]),
            workflow_type=cast(str, row["workflow_type"]),
            task_type=str(row["task_type"]),
            queue_name=cast(str, row["queue_name"]),
            entity_id=cast(UUID | None, row["entity_id"]),
            command_id=cast(int | None, row["command_id"]),
            attempt=cast(int, row["attempt"]),
            input=_json(row["input"]),
            outcome=_json(row["outcome"]),
            created_at=cast(datetime, row["created_at"]),
            completed_at=cast(datetime, row["completed_at"]),
        )
        for row in rows
    )


async def heartbeat_worker(
    pool: Pool,
    *,
    worker_id: UUID,
    queue_name: str,
    roles: tuple[str, ...],
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            insert into worker_heartbeats (
              worker_id, hostname, process_id, queue_name, roles
            ) values ($1, $2, $3, $4, $5)
            on conflict (worker_id) do update
            set last_seen_at = now(), stopped_at = null
            """,
            worker_id,
            socket.gethostname(),
            os.getpid(),
            queue_name,
            list(roles),
        )


async def stop_worker(pool: Pool, *, worker_id: UUID) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            update worker_heartbeats set last_seen_at = now(), stopped_at = now()
            where worker_id = $1
            """,
            worker_id,
        )


async def list_worker_heartbeats(
    pool: Pool,
    *,
    stale_after_seconds: int = 30,
) -> tuple[WorkerHeartbeat, ...]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select worker_id, hostname, process_id, queue_name, roles,
                   started_at, last_seen_at, stopped_at,
                   stopped_at is null
                     and last_seen_at > now() - $1 * interval '1 second' as healthy
            from worker_heartbeats order by started_at desc
            """,
            stale_after_seconds,
        )
    return tuple(
        WorkerHeartbeat(
            worker_id=cast(UUID, row["worker_id"]),
            hostname=cast(str, row["hostname"]),
            process_id=cast(int, row["process_id"]),
            queue_name=cast(str, row["queue_name"]),
            roles=tuple(cast(list[str], row["roles"])),
            started_at=cast(datetime, row["started_at"]),
            last_seen_at=cast(datetime, row["last_seen_at"]),
            stopped_at=cast(datetime | None, row["stopped_at"]),
            healthy=cast(bool, row["healthy"]),
        )
        for row in rows
    )


async def get_operational_gauges(pool: Pool, *, stale_after_seconds: int = 30) -> dict[str, float]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            select
              (select count(*) from tasks where status = 'pending') as tasks_pending,
              (select count(*) from tasks where status = 'leased') as tasks_leased,
              (select count(*) from tasks where status = 'dead') as tasks_dead,
              (select count(*) from workflow_executions
                where status = 'running') as workflows_running,
              (select count(*) from workflow_executions
                where status = 'running' and paused_at is not null) as workflows_paused,
              (select count(*) from worker_heartbeats
                where stopped_at is null
                  and last_seen_at > now() - $1 * interval '1 second') as workers_healthy
            """,
            stale_after_seconds,
        )
    if row is None:
        raise RuntimeError("operational metrics query returned no row")
    return {
        "dwe_tasks_pending": float(cast(int, row["tasks_pending"])),
        "dwe_tasks_leased": float(cast(int, row["tasks_leased"])),
        "dwe_tasks_dead": float(cast(int, row["tasks_dead"])),
        "dwe_workflows_running": float(cast(int, row["workflows_running"])),
        "dwe_workflows_paused": float(cast(int, row["workflows_paused"])),
        "dwe_workers_healthy": float(cast(int, row["workers_healthy"])),
    }
