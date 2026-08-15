"""Operational queue and worker inspection queries."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from engine.persistence.database import Pool


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
        "dwe_workers_healthy": float(cast(int, row["workers_healthy"])),
    }
