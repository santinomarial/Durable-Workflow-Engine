"""Continuous worker loops built from the auditable single-step transitions."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from typing import Literal
from uuid import UUID, uuid4

from engine.observability import METRICS
from engine.persistence import Pool, heartbeat_worker, stop_worker
from engine.runtime import DefinitionRegistry
from engine.workers.activity_worker import run_activity_task
from engine.workers.maintenance_worker import run_maintenance
from engine.workers.workflow_worker import run_workflow_task

WorkerRole = Literal["workflow", "activity", "maintenance"]
Step = Callable[[], Awaitable[bool | int]]
ALL_ROLES: frozenset[WorkerRole] = frozenset({"workflow", "activity", "maintenance"})
LOGGER = logging.getLogger(__name__)


async def _idle(stop: asyncio.Event, delay: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay)


async def _run_loop(
    role: WorkerRole,
    step: Step,
    *,
    stop: asyncio.Event,
    idle_delay: float,
) -> None:
    while not stop.is_set():
        started_at = time.monotonic()
        try:
            progressed = bool(await step())
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("%s worker step failed; polling will continue", role)
            METRICS.increment("dwe_worker_step_errors_total", labels={"role": role})
            progressed = False
        finally:
            METRICS.observe(
                "dwe_worker_step_duration_seconds",
                time.monotonic() - started_at,
                labels={"role": role},
            )
        METRICS.increment(
            "dwe_worker_steps_total",
            labels={"role": role, "result": "progressed" if progressed else "idle"},
        )
        if not progressed:
            await _idle(stop, idle_delay)


async def _heartbeat_loop(
    pool: Pool,
    *,
    worker_id: UUID,
    queue_name: str,
    roles: tuple[WorkerRole, ...],
    stop: asyncio.Event,
    interval: float,
) -> None:
    while not stop.is_set():
        await heartbeat_worker(
            pool,
            worker_id=worker_id,
            queue_name=queue_name,
            roles=roles,
        )
        await _idle(stop, interval)


async def run_worker(
    pool: Pool,
    registry: DefinitionRegistry,
    *,
    queue_name: str = "default",
    roles: Iterable[WorkerRole] = ALL_ROLES,
    idle_delay: float = 0.05,
    stop: asyncio.Event | None = None,
    heartbeat_interval: float = 10,
) -> None:
    """Run selected worker roles concurrently until the stop event is set or canceled."""
    if idle_delay <= 0:
        raise ValueError("idle_delay must be positive")
    if heartbeat_interval <= 0:
        raise ValueError("heartbeat_interval must be positive")
    selected = frozenset(roles)
    if not selected or not selected <= ALL_ROLES:
        raise ValueError(f"roles must be a non-empty subset of {sorted(ALL_ROLES)}")
    stop_event = stop or asyncio.Event()
    worker_id = uuid4()
    ordered_roles = tuple(sorted(selected))

    async def workflow_step() -> bool:
        return await run_workflow_task(pool, registry, queue_name=queue_name)

    async def activity_step() -> bool:
        return await run_activity_task(pool, registry, queue_name=queue_name)

    async def maintenance_step() -> int:
        return await run_maintenance(pool, queue_name=queue_name)

    steps: dict[WorkerRole, Step] = {
        "workflow": workflow_step,
        "activity": activity_step,
        "maintenance": maintenance_step,
    }
    LOGGER.info(
        "worker service starting",
        extra={
            "event": "worker_start",
            "worker_id": str(worker_id),
            "queue": queue_name,
            "roles": ordered_roles,
        },
    )
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(
                _heartbeat_loop(
                    pool,
                    worker_id=worker_id,
                    queue_name=queue_name,
                    roles=ordered_roles,
                    stop=stop_event,
                    interval=heartbeat_interval,
                ),
                name="durable-engine-heartbeat",
            )
            for role in ordered_roles:
                group.create_task(
                    _run_loop(
                        role,
                        steps[role],
                        stop=stop_event,
                        idle_delay=idle_delay,
                    ),
                    name=f"durable-engine-{role}",
                )
    finally:
        with suppress(Exception):
            await stop_worker(pool, worker_id=worker_id)
        LOGGER.info(
            "worker service stopped",
            extra={"event": "worker_stop", "worker_id": str(worker_id)},
        )
