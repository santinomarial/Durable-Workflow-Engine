"""Atomic workflow state transitions.

All accepted history changes and their resulting tasks are committed together in
this module so the engine's central correctness boundary remains auditable.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

from engine.persistence.audit import AuditContext, record_api_audit
from engine.persistence.database import Connection, Pool
from engine.persistence.leasing import LeasedTask, StaleLeaseError
from engine.runtime.commands import (
    ContinueAsNew,
    RecordMarker,
    ResolveWorkflowUpdate,
    ScheduleActivity,
    ScheduleTimer,
    StartChildWorkflow,
)
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.history import HistoryEvent
from engine.runtime.replay import ReplayResult, ReplayStatus
from engine.runtime.serialization import JSONValue, canonical_json, clone_json


class TransitionError(RuntimeError):
    """Base class for rejected durable transitions."""


class DefinitionNotRegisteredError(TransitionError):
    """Raised when an execution references a definition absent from storage."""


class StoredDefinitionConflictError(TransitionError):
    """Raised when stored and worker definition hashes disagree."""


class TerminalWorkflowError(TransitionError):
    """Raised when a task attempts to transition a closed execution."""


@dataclass(frozen=True, slots=True)
class StartedWorkflow:
    workflow_id: UUID
    workflow_type: str
    definition_version: int


@dataclass(frozen=True, slots=True)
class WorkflowReplayState:
    workflow_id: UUID
    workflow_type: str
    definition_version: int
    workflow_input: JSONValue
    history: tuple[HistoryEvent, ...]


def _decode_json(value: object) -> JSONValue:
    if not isinstance(value, str):
        raise TypeError(f"expected PostgreSQL JSON text, received {type(value).__name__}")
    return cast(JSONValue, json.loads(value))


def history_event_from_row(row: dict[str, object]) -> HistoryEvent:
    attributes = _decode_json(row["attributes"])
    if not isinstance(attributes, dict):
        raise TypeError("history attributes must be a JSON object")
    return HistoryEvent(
        seq=cast(int, row["seq"]),
        event_type=cast(str, row["event_type"]),
        attributes=attributes,
        command_id=cast(int | None, row["command_id"]),
        entity_id=cast(UUID | None, row["entity_id"]),
        external_id=cast(str | None, row["external_id"]),
    )


async def register_workflow_definition(pool: Pool, definition: WorkflowDefinition) -> None:
    """Persist a definition identity without allowing a pinned version to change."""
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            """
            insert into workflow_definitions (workflow_type, version, code_hash)
            values ($1, $2, $3)
            on conflict (workflow_type, version) do nothing
            """,
            definition.name,
            definition.version,
            definition.code_hash,
        )
        stored_hash = await connection.fetchval(
            """
            select code_hash
            from workflow_definitions
            where workflow_type = $1 and version = $2
            """,
            definition.name,
            definition.version,
        )
        if stored_hash != definition.code_hash:
            raise StoredDefinitionConflictError(
                f"stored workflow {definition.name!r} version {definition.version} "
                "has a different code hash"
            )


async def start_workflow(
    pool: Pool,
    *,
    workflow_type: str,
    definition_version: int,
    workflow_input: JSONValue,
    queue_name: str = "default",
    workflow_id: UUID | None = None,
    search_attributes: dict[str, JSONValue] | None = None,
    retry_of: UUID | None = None,
    audit: AuditContext | None = None,
) -> StartedWorkflow:
    """Atomically create an execution, its first event, and its first task."""
    if not workflow_type:
        raise ValueError("workflow_type cannot be empty")
    if definition_version < 1:
        raise ValueError("definition_version must be at least 1")
    if not queue_name:
        raise ValueError("queue_name cannot be empty")

    execution_id = workflow_id or uuid4()
    started_at = datetime.now(UTC)
    detached_input = clone_json(workflow_input)
    detached_search_attributes = clone_json(search_attributes or {})
    if not isinstance(detached_search_attributes, dict):
        raise ValueError("search_attributes must be a JSON object")
    encoded_search_attributes = canonical_json(detached_search_attributes)
    if len(encoded_search_attributes.encode()) > 16_000:
        raise ValueError("search_attributes cannot exceed 16 KB")
    encoded_input = canonical_json(detached_input)
    started_attributes = canonical_json(
        {
            "workflow_type": workflow_type,
            "definition_version": definition_version,
            "input": detached_input,
            "search_attributes": detached_search_attributes,
            "started_at": started_at.isoformat(),
        }
    )
    task_id = uuid4()

    async with pool.acquire() as connection, connection.transaction():
        definition_exists = await connection.fetchval(
            """
            select exists (
              select 1
              from workflow_definitions
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

        await connection.execute(
            """
            insert into workflow_executions (
              id, workflow_type, definition_version, input, next_seq, queue_name, created_at,
              search_attributes, retry_of
            ) values ($1, $2, $3, $4::jsonb, 2, $5, $6, $7::jsonb, $8)
            """,
            execution_id,
            workflow_type,
            definition_version,
            encoded_input,
            queue_name,
            started_at,
            encoded_search_attributes,
            retry_of,
        )
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, attributes
            ) values ($1, 1, 'WorkflowExecutionStarted', $2::jsonb)
            """,
            execution_id,
            started_attributes,
        )
        await connection.execute(
            """
            insert into tasks (id, workflow_id, task_type, queue_name)
            values ($1, $2, 'workflow', $3)
            """,
            task_id,
            execution_id,
            queue_name,
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=execution_id,
            accepted=True,
            details={
                "workflow_type": workflow_type,
                "definition_version": definition_version,
                "queue_name": queue_name,
                "search_attributes": detached_search_attributes,
                "retry_of": str(retry_of) if retry_of is not None else None,
            },
        )

    return StartedWorkflow(execution_id, workflow_type, definition_version)


async def pause_workflow(
    pool: Pool,
    *,
    workflow_id: UUID,
    reason: str | None = None,
    audit: AuditContext | None = None,
) -> bool:
    """Freeze new task dispatch and deadlines after any in-flight transition finishes."""
    async with pool.acquire() as connection, connection.transaction():
        execution = await connection.fetchrow(
            "select status, next_seq, paused_at from workflow_executions where id = $1 for update",
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {workflow_id} is {execution['status']}")
        if execution["paused_at"] is not None:
            await record_api_audit(
                connection,
                audit,
                workflow_id=workflow_id,
                accepted=False,
                details={"reason": reason, "duplicate": True},
            )
            return False
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (workflow_id, seq, event_type, attributes)
            values ($1, $2, 'WorkflowExecutionPaused', $3::jsonb)
            """,
            workflow_id,
            next_seq,
            canonical_json({"reason": reason}),
        )
        await connection.execute(
            """
            update workflow_executions
            set paused_at = now(), pause_reason = $2, next_seq = $3
            where id = $1
            """,
            workflow_id,
            reason,
            next_seq + 1,
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=workflow_id,
            accepted=True,
            details={"reason": reason},
        )
    return True


async def resume_workflow(
    pool: Pool,
    *,
    workflow_id: UUID,
    reason: str | None = None,
    audit: AuditContext | None = None,
) -> bool:
    """Resume dispatch and shift pending timers/deadlines by the paused duration."""
    async with pool.acquire() as connection, connection.transaction():
        execution = await connection.fetchrow(
            "select status, next_seq, paused_at from workflow_executions where id = $1 for update",
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {workflow_id} is {execution['status']}")
        paused_at = cast(datetime | None, execution["paused_at"])
        if paused_at is None:
            await record_api_audit(
                connection,
                audit,
                workflow_id=workflow_id,
                accepted=False,
                details={"reason": reason, "duplicate": True},
            )
            return False
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            update tasks
            set visible_at = visible_at + (now() - $2::timestamptz),
                schedule_to_start_deadline = case
                  when schedule_to_start_deadline is null then null
                  else schedule_to_start_deadline + (now() - $2::timestamptz)
                end
            where workflow_id = $1 and status = 'pending'
            """,
            workflow_id,
            paused_at,
        )
        await connection.execute(
            """
            insert into history_events (workflow_id, seq, event_type, attributes)
            values ($1, $2, 'WorkflowExecutionResumed', $3::jsonb)
            """,
            workflow_id,
            next_seq,
            canonical_json({"reason": reason, "paused_at": paused_at.isoformat()}),
        )
        await connection.execute(
            """
            update workflow_executions
            set paused_at = null, pause_reason = null, next_seq = $2
            where id = $1
            """,
            workflow_id,
            next_seq + 1,
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=workflow_id,
            accepted=True,
            details={"reason": reason},
        )
    return True


async def retry_workflow(
    pool: Pool,
    *,
    workflow_id: UUID,
    audit: AuditContext | None = None,
) -> StartedWorkflow:
    """Start a new execution from a closed execution without mutating its history."""
    async with pool.acquire() as connection:
        original = await connection.fetchrow(
            """
            select workflow_type, definition_version, input, status, queue_name,
                   search_attributes
            from workflow_executions where id = $1
            """,
            workflow_id,
        )
    if original is None:
        raise TransitionError(f"workflow {workflow_id} does not exist")
    if original["status"] not in ("failed", "terminated"):
        raise TransitionError("only failed or terminated workflows can be retried")
    workflow_input = _decode_json(original["input"])
    search_attributes = _decode_json(original["search_attributes"])
    if not isinstance(search_attributes, dict):
        raise TypeError("stored search attributes must be a JSON object")
    retry_attributes = {**search_attributes, "dwe.retry_of": str(workflow_id)}
    return await start_workflow(
        pool,
        workflow_type=cast(str, original["workflow_type"]),
        definition_version=cast(int, original["definition_version"]),
        workflow_input=workflow_input,
        queue_name=cast(str, original["queue_name"]),
        search_attributes=retry_attributes,
        retry_of=workflow_id,
        audit=audit,
    )


async def update_search_attributes(
    pool: Pool,
    *,
    workflow_id: UUID,
    attributes: dict[str, JSONValue],
    unset: tuple[str, ...] = (),
    audit: AuditContext | None = None,
) -> dict[str, JSONValue]:
    """Atomically merge visibility metadata without changing replay history."""
    detached = clone_json(attributes)
    if not isinstance(detached, dict):
        raise ValueError("attributes must be a JSON object")
    if any(not key or len(key) > 200 for key in (*detached.keys(), *unset)):
        raise ValueError("search attribute keys must contain 1 to 200 characters")
    async with pool.acquire() as connection, connection.transaction():
        execution = await connection.fetchrow(
            "select search_attributes from workflow_executions where id = $1 for update",
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        current = _decode_json(execution["search_attributes"])
        if not isinstance(current, dict):
            raise TypeError("stored search attributes must be a JSON object")
        merged = {**current, **detached}
        for key in unset:
            merged.pop(key, None)
        if len(canonical_json(merged).encode()) > 16_000:
            raise ValueError("search_attributes cannot exceed 16 KB")
        await connection.execute(
            "update workflow_executions set search_attributes = $2::jsonb where id = $1",
            workflow_id,
            canonical_json(merged),
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=workflow_id,
            accepted=True,
            details={"set": detached, "unset": list(unset)},
        )
    return merged


async def _cancel_pending_timers(
    connection: Connection,
    *,
    workflow_id: UUID,
    next_seq: int,
    reason: str,
) -> int:
    timers = await connection.fetch(
        """
        select id, entity_id
        from tasks
        where workflow_id = $1 and task_type = 'timer' and status = 'pending'
        order by command_id, id
        """,
        workflow_id,
    )
    for timer in timers:
        canceled = await connection.execute(
            """
            update tasks set status = 'dead', completed_at = now()
            where id = $1 and status = 'pending'
            """,
            timer["id"],
        )
        if canceled != "UPDATE 1":
            continue
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, entity_id, attributes
            ) values ($1, $2, 'TimerCanceled', $3, $4::jsonb)
            """,
            workflow_id,
            next_seq,
            timer["entity_id"],
            canonical_json({"reason": reason}),
        )
        next_seq += 1
    return next_seq


async def terminate_workflow(
    pool: Pool,
    *,
    workflow_id: UUID,
    reason: str | None = None,
    audit: AuditContext | None = None,
) -> bool:
    """Atomically terminate a running workflow and invalidate all outstanding work."""
    async with pool.acquire() as connection, connection.transaction():
        execution = await connection.fetchrow(
            """
            select status, next_seq, parent_workflow_id
            from workflow_executions where id = $1 for update
            """,
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        if execution["status"] == "terminated":
            await record_api_audit(
                connection,
                audit,
                workflow_id=workflow_id,
                accepted=False,
                details={"reason": reason, "duplicate": True},
            )
            return False
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {workflow_id} is {execution['status']}")
        next_seq = cast(int, execution["next_seq"])
        await _terminate_descendants(connection, parent_workflow_id=workflow_id)
        await _reject_pending_updates(
            connection, workflow_id=workflow_id, reason="workflow terminated"
        )
        next_seq = await _cancel_pending_timers(
            connection,
            workflow_id=workflow_id,
            next_seq=next_seq,
            reason="workflow terminated",
        )
        await connection.execute(
            """
            insert into history_events (workflow_id, seq, event_type, attributes)
            values ($1, $2, 'WorkflowExecutionTerminated', $3::jsonb)
            """,
            workflow_id,
            next_seq,
            canonical_json({"reason": reason}),
        )
        await connection.execute(
            """
            update workflow_executions
            set status = 'terminated', next_seq = $2, closed_at = now()
            where id = $1
            """,
            workflow_id,
            next_seq + 1,
        )
        await connection.execute(
            """
            update tasks set status = 'dead', completed_at = now()
            where workflow_id = $1 and status in ('pending', 'leased')
            """,
            workflow_id,
        )
        await _notify_parent_of_child_terminal(
            connection,
            child_workflow_id=workflow_id,
            parent_workflow_id=cast(UUID | None, execution["parent_workflow_id"]),
            status="terminated",
            payload={"reason": reason},
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=workflow_id,
            accepted=True,
            details={"reason": reason},
        )
    return True


async def request_workflow_cancellation(
    pool: Pool,
    *,
    workflow_id: UUID,
    reason: str | None = None,
    audit: AuditContext | None = None,
) -> bool:
    """Record one cooperative cancellation request and wake workflow replay."""
    async with pool.acquire() as connection, connection.transaction():
        execution = await connection.fetchrow(
            """
            select status, next_seq, queue_name, cancellation_requested_at
            from workflow_executions where id = $1 for update
            """,
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {workflow_id} is {execution['status']}")
        if execution["cancellation_requested_at"] is not None:
            await record_api_audit(
                connection,
                audit,
                workflow_id=workflow_id,
                accepted=False,
                details={"reason": reason, "duplicate": True},
            )
            return False
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (workflow_id, seq, event_type, attributes)
            values ($1, $2, 'WorkflowCancellationRequested', $3::jsonb)
            """,
            workflow_id,
            next_seq,
            canonical_json({"reason": reason}),
        )
        await connection.execute(
            """
            update workflow_executions
            set cancellation_requested_at = now(), cancellation_reason = $2, next_seq = $3
            where id = $1
            """,
            workflow_id,
            reason,
            next_seq + 1,
        )
        await connection.execute(
            """
            update tasks set status = 'dead', completed_at = now()
            where workflow_id = $1
              and task_type = 'activity'
              and status = 'pending'
            """,
            workflow_id,
        )
        await connection.execute(
            """
            insert into tasks (id, workflow_id, task_type, queue_name)
            values ($1, $2, 'workflow', $3)
            """,
            uuid4(),
            workflow_id,
            execution["queue_name"],
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=workflow_id,
            accepted=True,
            details={"reason": reason},
        )
    return True


async def load_workflow_replay_state(pool: Pool, task: LeasedTask) -> WorkflowReplayState:
    """Load the pinned definition identity and ordered history for a leased task."""
    if task.task_type != "workflow":
        raise ValueError("replay state can only be loaded for workflow tasks")
    async with pool.acquire() as connection:
        current_lease = await connection.fetchval(
            """
            select exists (
              select 1 from tasks
              where id = $1 and status = 'leased' and lease_token = $2
                and lease_expires_at > now()
            )
            """,
            task.id,
            task.lease_token,
        )
        if current_lease is not True:
            raise StaleLeaseError(f"workflow task {task.id} does not hold the current lease")
        execution = await connection.fetchrow(
            """
            select workflow_type, definition_version, input, status
            from workflow_executions
            where id = $1
            """,
            task.workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {task.workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {task.workflow_id} is {execution['status']}")
        rows = await connection.fetch(
            """
            select seq, event_type, command_id, entity_id, external_id, attributes
            from history_events
            where workflow_id = $1
            order by seq
            """,
            task.workflow_id,
        )
    return WorkflowReplayState(
        workflow_id=task.workflow_id,
        workflow_type=cast(str, execution["workflow_type"]),
        definition_version=cast(int, execution["definition_version"]),
        workflow_input=_decode_json(execution["input"]),
        history=tuple(history_event_from_row(dict(row)) for row in rows),
    )


async def _append_activity_command(
    connection: Connection,
    *,
    workflow_id: UUID,
    seq: int,
    command: ScheduleActivity,
    queue_name: str,
) -> None:
    attributes = canonical_json(
        {
            "activity_type": command.activity_type,
            "fingerprint": command.fingerprint,
            "input": command.input,
            "retry_policy": command.retry_policy,
            "schedule_to_start_seconds": command.schedule_to_start_seconds,
            "start_to_close_seconds": command.start_to_close_seconds,
            "heartbeat_timeout_seconds": command.heartbeat_timeout_seconds,
            "idempotency_key": str(command.entity_id),
        }
    )
    task_input = canonical_json(
        {
            "activity_type": command.activity_type,
            "input": command.input,
            "idempotency_key": str(command.entity_id),
            "retry_policy": command.retry_policy,
        }
    )
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, command_id, entity_id, attributes
        ) values ($1, $2, 'ActivityScheduled', $3, $4, $5::jsonb)
        """,
        workflow_id,
        seq,
        command.command_id,
        command.entity_id,
        attributes,
    )
    await connection.execute(
        """
        insert into tasks (
          id, workflow_id, task_type, queue_name, entity_id, command_id,
          input, schedule_to_start_deadline, schedule_to_start_timeout,
          start_to_close_timeout, heartbeat_timeout
        ) values (
          $1, $2, 'activity', $3, $4, $5, $6::jsonb,
          case when $7::double precision is null then null
            else now() + $7 * interval '1 second' end,
          $7 * interval '1 second',
          $8::double precision * interval '1 second',
          $9::double precision * interval '1 second'
        )
        """,
        uuid4(),
        workflow_id,
        queue_name,
        command.entity_id,
        command.command_id,
        task_input,
        command.schedule_to_start_seconds,
        command.start_to_close_seconds,
        command.heartbeat_timeout_seconds,
    )


async def _append_timer_command(
    connection: Connection,
    *,
    workflow_id: UUID,
    seq: int,
    command: ScheduleTimer,
    queue_name: str,
) -> None:
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, command_id, entity_id, attributes
        ) values ($1, $2, 'TimerStarted', $3, $4, $5::jsonb)
        """,
        workflow_id,
        seq,
        command.command_id,
        command.entity_id,
        canonical_json(
            {
                "delay_seconds": command.delay_seconds,
                "fingerprint": command.fingerprint,
                "purpose": command.purpose,
                "signal_name": command.signal_name,
            }
        ),
    )
    await connection.execute(
        """
        insert into tasks (
          id, workflow_id, task_type, queue_name, entity_id, command_id,
          input, visible_at
        ) values (
          $1, $2, 'timer', $3, $4, $5, $6::jsonb,
          now() + $7::double precision * interval '1 second'
        )
        """,
        uuid4(),
        workflow_id,
        queue_name,
        command.entity_id,
        command.command_id,
        canonical_json(
            {
                "delay_seconds": command.delay_seconds,
                "purpose": command.purpose,
                "signal_name": command.signal_name,
            }
        ),
        command.delay_seconds,
    )


async def _append_marker_command(
    connection: Connection,
    *,
    workflow_id: UUID,
    seq: int,
    command: RecordMarker,
) -> None:
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, command_id, attributes
        ) values ($1, $2, 'MarkerRecorded', $3, $4::jsonb)
        """,
        workflow_id,
        seq,
        command.command_id,
        canonical_json(
            {
                "marker_type": command.marker_type,
                "value": command.value,
                "fingerprint": command.fingerprint,
            }
        ),
    )


async def _append_update_resolution(
    connection: Connection,
    *,
    workflow_id: UUID,
    seq: int,
    command: ResolveWorkflowUpdate,
) -> None:
    status = "completed" if command.accepted else "rejected"
    updated = await connection.fetchrow(
        """
        update workflow_updates
        set status = $3::workflow_update_status,
            result = case when $3 = 'completed' then $4::jsonb else null end,
            failure = case when $3 = 'rejected' then $4::jsonb else null end,
            completed_at = now()
        where workflow_id = $1 and update_id = $2 and status = 'pending'
        returning name
        """,
        workflow_id,
        command.update_id,
        status,
        canonical_json(command.outcome),
    )
    if updated is None:
        raise TransitionError(
            f"workflow update {command.update_id!r} is missing or already resolved"
        )
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, command_id, external_id, attributes
        ) values ($1, $2, 'WorkflowUpdateResolved', $3, $4, $5::jsonb)
        """,
        workflow_id,
        seq,
        command.command_id,
        f"update-result:{command.update_id}",
        canonical_json(
            {
                "update_id": command.update_id,
                "name": cast(str, updated["name"]),
                "accepted": command.accepted,
                "outcome": command.outcome,
                "fingerprint": command.fingerprint,
            }
        ),
    )


async def _reject_pending_updates(
    connection: Connection, *, workflow_id: UUID, reason: str
) -> None:
    await connection.execute(
        """
        update workflow_updates
        set status = 'rejected', failure = $2::jsonb, completed_at = now()
        where workflow_id = $1 and status = 'pending'
        """,
        workflow_id,
        canonical_json({"type": "WorkflowClosed", "message": reason}),
    )


async def _append_child_workflow_command(
    connection: Connection,
    *,
    workflow_id: UUID,
    seq: int,
    command: StartChildWorkflow,
    parent_queue_name: str,
) -> None:
    child_queue = command.queue_name or parent_queue_name
    definition_exists = await connection.fetchval(
        """
        select exists (
          select 1 from workflow_definitions
          where workflow_type = $1 and version = $2
        )
        """,
        command.workflow_type,
        command.definition_version,
    )
    if definition_exists is not True:
        raise DefinitionNotRegisteredError(
            f"child workflow {command.workflow_type!r} version "
            f"{command.definition_version} is not registered"
        )
    started_at = datetime.now(UTC)
    child_attributes: dict[str, JSONValue] = {
        "dwe.parent_workflow_id": str(workflow_id),
        "dwe.parent_command_id": command.command_id,
    }
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, command_id, entity_id, attributes
        ) values ($1, $2, 'ChildWorkflowStarted', $3, $4, $5::jsonb)
        """,
        workflow_id,
        seq,
        command.command_id,
        command.child_workflow_id,
        canonical_json(
            {
                "workflow_type": command.workflow_type,
                "definition_version": command.definition_version,
                "input": command.input,
                "queue_name": child_queue,
                "parent_close_policy": command.parent_close_policy,
                "fingerprint": command.fingerprint,
            }
        ),
    )
    await connection.execute(
        """
        insert into workflow_executions (
          id, workflow_type, definition_version, input, next_seq, queue_name,
          created_at, search_attributes, parent_workflow_id, parent_command_id,
          parent_close_policy
        ) values ($1, $2, $3, $4::jsonb, 2, $5, $6, $7::jsonb, $8, $9, $10)
        """,
        command.child_workflow_id,
        command.workflow_type,
        command.definition_version,
        canonical_json(command.input),
        child_queue,
        started_at,
        canonical_json(child_attributes),
        workflow_id,
        command.command_id,
        command.parent_close_policy,
    )
    await connection.execute(
        """
        insert into history_events (workflow_id, seq, event_type, attributes)
        values ($1, 1, 'WorkflowExecutionStarted', $2::jsonb)
        """,
        command.child_workflow_id,
        canonical_json(
            {
                "workflow_type": command.workflow_type,
                "definition_version": command.definition_version,
                "input": command.input,
                "search_attributes": child_attributes,
                "started_at": started_at.isoformat(),
                "parent_workflow_id": str(workflow_id),
                "parent_command_id": command.command_id,
            }
        ),
    )
    await connection.execute(
        """
        insert into tasks (id, workflow_id, task_type, queue_name)
        values ($1, $2, 'workflow', $3)
        """,
        uuid4(),
        command.child_workflow_id,
        child_queue,
    )


async def _notify_parent_of_child_terminal(
    connection: Connection,
    *,
    child_workflow_id: UUID,
    parent_workflow_id: UUID | None,
    status: str,
    payload: JSONValue,
) -> None:
    if parent_workflow_id is None:
        return
    parent = await connection.fetchrow(
        "select status, next_seq, queue_name from workflow_executions where id = $1 for update",
        parent_workflow_id,
    )
    if parent is None or parent["status"] != "running":
        return
    event_types = {
        "completed": "ChildWorkflowCompleted",
        "failed": "ChildWorkflowFailed",
        "terminated": "ChildWorkflowTerminated",
    }
    event_type = event_types[status]
    attribute_name = "result" if status == "completed" else "failure"
    next_seq = cast(int, parent["next_seq"])
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, entity_id, attributes
        ) values ($1, $2, $3, $4, $5::jsonb)
        """,
        parent_workflow_id,
        next_seq,
        event_type,
        child_workflow_id,
        canonical_json({attribute_name: clone_json(payload)}),
    )
    await connection.execute(
        "update workflow_executions set next_seq = $2 where id = $1",
        parent_workflow_id,
        next_seq + 1,
    )
    await connection.execute(
        """
        insert into tasks (id, workflow_id, task_type, queue_name)
        values ($1, $2, 'workflow', $3)
        """,
        uuid4(),
        parent_workflow_id,
        parent["queue_name"],
    )


async def _terminate_descendants(connection: Connection, *, parent_workflow_id: UUID) -> None:
    children = await connection.fetch(
        """
        select id, next_seq
        from workflow_executions
        where parent_workflow_id = $1 and parent_close_policy = 'terminate'
          and status = 'running'
        order by created_at, id
        for update
        """,
        parent_workflow_id,
    )
    for child in children:
        child_id = cast(UUID, child["id"])
        await _terminate_descendants(connection, parent_workflow_id=child_id)
        next_seq = cast(int, child["next_seq"])
        await connection.execute(
            """
            insert into history_events (workflow_id, seq, event_type, attributes)
            values ($1, $2, 'WorkflowExecutionTerminated', $3::jsonb)
            """,
            child_id,
            next_seq,
            canonical_json({"reason": "parent workflow closed", "cause": "parent_close_policy"}),
        )
        await connection.execute(
            """
            update workflow_executions
            set status = 'terminated', next_seq = $2, closed_at = now(),
                failure = $3::jsonb
            where id = $1
            """,
            child_id,
            next_seq + 1,
            canonical_json({"reason": "parent workflow closed"}),
        )
        await connection.execute(
            """
            update tasks set status = 'dead', completed_at = now()
            where workflow_id = $1 and status in ('pending', 'leased')
            """,
            child_id,
        )
        await _reject_pending_updates(
            connection, workflow_id=child_id, reason="parent workflow closed"
        )


async def _commit_continue_as_new(
    connection: Connection,
    *,
    workflow_id: UUID,
    current_task_id: UUID,
    seq: int,
    command: ContinueAsNew,
) -> None:
    current = await connection.fetchrow(
        """
        select workflow_type, queue_name, search_attributes, schedule_id, scheduled_at,
               parent_workflow_id
        from workflow_executions where id = $1
        """,
        workflow_id,
    )
    if current is None:
        raise TransitionError(f"workflow {workflow_id} does not exist")
    if current["workflow_type"] != command.workflow_type:
        raise TransitionError("continue-as-new must retain the workflow type")
    if current["parent_workflow_id"] is not None:
        raise TransitionError("continue-as-new is not supported inside child workflows")
    definition_exists = await connection.fetchval(
        """
        select exists (
          select 1 from workflow_definitions
          where workflow_type = $1 and version = $2
        )
        """,
        command.workflow_type,
        command.definition_version,
    )
    if definition_exists is not True:
        raise DefinitionNotRegisteredError(
            f"continuation workflow {command.workflow_type!r} version "
            f"{command.definition_version} is not registered"
        )
    attributes = _decode_json(current["search_attributes"])
    if not isinstance(attributes, dict):
        raise TypeError("stored search attributes must be a JSON object")
    continuation_attributes = {
        **attributes,
        "dwe.continued_from": str(workflow_id),
    }
    queue_name = command.queue_name or cast(str, current["queue_name"])
    started_at = datetime.now(UTC)
    result: dict[str, JSONValue] = {"continued_to": str(command.new_workflow_id)}
    await connection.execute(
        """
        insert into history_events (
          workflow_id, seq, event_type, command_id, entity_id, attributes
        ) values ($1, $2, 'WorkflowExecutionContinuedAsNew', $3, $4, $5::jsonb)
        """,
        workflow_id,
        seq,
        command.command_id,
        command.new_workflow_id,
        canonical_json(
            {
                "workflow_type": command.workflow_type,
                "definition_version": command.definition_version,
                "input": command.input,
                "queue_name": queue_name,
                "fingerprint": command.fingerprint,
            }
        ),
    )
    await connection.execute(
        """
        insert into history_events (workflow_id, seq, event_type, attributes)
        values ($1, $2, 'WorkflowExecutionCompleted', $3::jsonb)
        """,
        workflow_id,
        seq + 1,
        canonical_json({"result": result, "cause": "continue_as_new"}),
    )
    await connection.execute(
        """
        insert into workflow_executions (
          id, workflow_type, definition_version, input, next_seq, queue_name,
          created_at, search_attributes, continued_from, schedule_id, scheduled_at
        ) values ($1, $2, $3, $4::jsonb, 2, $5, $6, $7::jsonb, $8, $9, $10)
        """,
        command.new_workflow_id,
        command.workflow_type,
        command.definition_version,
        canonical_json(command.input),
        queue_name,
        started_at,
        canonical_json(continuation_attributes),
        workflow_id,
        current["schedule_id"],
        current["scheduled_at"],
    )
    await connection.execute(
        """
        insert into history_events (workflow_id, seq, event_type, attributes)
        values ($1, 1, 'WorkflowExecutionStarted', $2::jsonb)
        """,
        command.new_workflow_id,
        canonical_json(
            {
                "workflow_type": command.workflow_type,
                "definition_version": command.definition_version,
                "input": command.input,
                "search_attributes": continuation_attributes,
                "started_at": started_at.isoformat(),
                "continued_from": str(workflow_id),
            }
        ),
    )
    await connection.execute(
        """
        insert into tasks (id, workflow_id, task_type, queue_name)
        values ($1, $2, 'workflow', $3)
        """,
        uuid4(),
        command.new_workflow_id,
        queue_name,
    )
    await connection.execute(
        """
        update workflow_executions
        set status = 'completed', result = $2::jsonb, next_seq = $3,
            closed_at = now(), continued_to = $4
        where id = $1
        """,
        workflow_id,
        canonical_json(result),
        seq + 2,
        command.new_workflow_id,
    )
    await _reject_pending_updates(
        connection, workflow_id=workflow_id, reason="workflow continued as new"
    )
    await connection.execute(
        """
        update tasks set status = 'dead', completed_at = now()
        where workflow_id = $1 and id <> $2 and status in ('pending', 'leased')
        """,
        workflow_id,
        current_task_id,
    )


async def commit_workflow_replay(
    pool: Pool,
    *,
    task: LeasedTask,
    replay: ReplayResult,
) -> None:
    """Fence and atomically commit the durable consequences of workflow replay."""
    if task.task_type != "workflow":
        raise ValueError("only a workflow task can commit workflow replay")
    async with pool.acquire() as connection, connection.transaction():
        locked_task = await connection.fetchrow(
            """
            select workflow_id, queue_name
            from tasks
            where id = $1 and status = 'leased' and lease_token = $2
              and lease_expires_at > now()
            for update
            """,
            task.id,
            task.lease_token,
        )
        if locked_task is None:
            raise StaleLeaseError(f"workflow task {task.id} does not hold the current lease")
        execution = await connection.fetchrow(
            """
            select status, next_seq, cancellation_requested_at, cancellation_reason,
                   parent_workflow_id
            from workflow_executions
            where id = $1
            for update
            """,
            task.workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {task.workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {task.workflow_id} is {execution['status']}")

        next_seq = cast(int, execution["next_seq"])
        cancellation_requested = execution["cancellation_requested_at"] is not None
        if cancellation_requested and replay.status in (
            ReplayStatus.COMMANDS,
            ReplayStatus.BLOCKED,
        ):
            next_seq = await _cancel_pending_timers(
                connection,
                workflow_id=task.workflow_id,
                next_seq=next_seq,
                reason="workflow cancellation requested",
            )
            await _terminate_descendants(connection, parent_workflow_id=task.workflow_id)
            await _reject_pending_updates(
                connection, workflow_id=task.workflow_id, reason="workflow cancelled"
            )
            await connection.execute(
                """
                insert into history_events (workflow_id, seq, event_type, attributes)
                values ($1, $2, 'WorkflowExecutionTerminated', $3::jsonb)
                """,
                task.workflow_id,
                next_seq,
                canonical_json(
                    {
                        "reason": cast(str | None, execution["cancellation_reason"]),
                        "cause": "cancellation",
                    }
                ),
            )
            await connection.execute(
                """
                update workflow_executions
                set status = 'terminated', next_seq = $2, closed_at = now()
                where id = $1
                """,
                task.workflow_id,
                next_seq + 1,
            )
            await connection.execute(
                """
                update tasks set status = 'dead', completed_at = now()
                where workflow_id = $1 and id <> $2 and status in ('pending', 'leased')
                """,
                task.workflow_id,
                task.id,
            )
            await _notify_parent_of_child_terminal(
                connection,
                child_workflow_id=task.workflow_id,
                parent_workflow_id=cast(UUID | None, execution["parent_workflow_id"]),
                status="terminated",
                payload={"reason": cast(str | None, execution["cancellation_reason"])},
            )
        elif replay.status is ReplayStatus.COMMANDS:
            if not replay.commands:
                raise TransitionError("commands replay result contains no commands")
            continuations = tuple(
                command for command in replay.commands if isinstance(command, ContinueAsNew)
            )
            if continuations:
                if len(replay.commands) != 1:
                    raise TransitionError("continue-as-new must be the only emitted command")
                await _terminate_descendants(connection, parent_workflow_id=task.workflow_id)
                await _commit_continue_as_new(
                    connection,
                    workflow_id=task.workflow_id,
                    current_task_id=task.id,
                    seq=next_seq,
                    command=continuations[0],
                )
            else:
                marker_recorded = False
                for command in replay.commands:
                    if isinstance(command, ScheduleActivity):
                        await _append_activity_command(
                            connection,
                            workflow_id=task.workflow_id,
                            seq=next_seq,
                            command=command,
                            queue_name=cast(str, locked_task["queue_name"]),
                        )
                    elif isinstance(command, ScheduleTimer):
                        await _append_timer_command(
                            connection,
                            workflow_id=task.workflow_id,
                            seq=next_seq,
                            command=command,
                            queue_name=cast(str, locked_task["queue_name"]),
                        )
                    elif isinstance(command, RecordMarker):
                        await _append_marker_command(
                            connection,
                            workflow_id=task.workflow_id,
                            seq=next_seq,
                            command=command,
                        )
                        marker_recorded = True
                    elif isinstance(command, StartChildWorkflow):
                        await _append_child_workflow_command(
                            connection,
                            workflow_id=task.workflow_id,
                            seq=next_seq,
                            command=command,
                            parent_queue_name=cast(str, locked_task["queue_name"]),
                        )
                    elif isinstance(command, ResolveWorkflowUpdate):
                        await _append_update_resolution(
                            connection,
                            workflow_id=task.workflow_id,
                            seq=next_seq,
                            command=command,
                        )
                        marker_recorded = True
                    next_seq += 1
                if marker_recorded:
                    await connection.execute(
                        """
                        insert into tasks (id, workflow_id, task_type, queue_name)
                        values ($1, $2, 'workflow', $3)
                        """,
                        uuid4(),
                        task.workflow_id,
                        locked_task["queue_name"],
                    )
                await connection.execute(
                    "update workflow_executions set next_seq = $2 where id = $1",
                    task.workflow_id,
                    next_seq,
                )
        elif replay.status in (ReplayStatus.COMPLETED, ReplayStatus.FAILED):
            completed = replay.status is ReplayStatus.COMPLETED
            event_type = "WorkflowExecutionCompleted" if completed else "WorkflowExecutionFailed"
            payload = replay.result if completed else replay.failure
            await _terminate_descendants(connection, parent_workflow_id=task.workflow_id)
            await _reject_pending_updates(
                connection, workflow_id=task.workflow_id, reason="workflow closed"
            )
            next_seq = await _cancel_pending_timers(
                connection,
                workflow_id=task.workflow_id,
                next_seq=next_seq,
                reason="workflow closed",
            )
            await connection.execute(
                """
                insert into history_events (workflow_id, seq, event_type, attributes)
                values ($1, $2, $3, $4::jsonb)
                """,
                task.workflow_id,
                next_seq,
                event_type,
                canonical_json({"result" if completed else "failure": payload}),
            )
            await connection.execute(
                """
                update workflow_executions
                set status = $2::workflow_status,
                    result = case when $2 = 'completed' then $3::jsonb else null end,
                    failure = case when $2 = 'failed' then $3::jsonb else null end,
                    next_seq = $4,
                    closed_at = now()
                where id = $1
                """,
                task.workflow_id,
                "completed" if completed else "failed",
                canonical_json(payload),
                next_seq + 1,
            )
            await connection.execute(
                """
                update tasks
                set status = 'dead', completed_at = now()
                where workflow_id = $1
                  and id <> $2
                  and status in ('pending', 'leased')
                """,
                task.workflow_id,
                task.id,
            )
            await _notify_parent_of_child_terminal(
                connection,
                child_workflow_id=task.workflow_id,
                parent_workflow_id=cast(UUID | None, execution["parent_workflow_id"]),
                status="completed" if completed else "failed",
                payload=payload,
            )
        elif replay.status is not ReplayStatus.BLOCKED:
            raise TransitionError(f"unsupported replay status: {replay.status}")

        completed_task = await connection.execute(
            """
            update tasks
            set status = 'completed', completed_at = now()
            where id = $1 and status = 'leased' and lease_token = $2
            """,
            task.id,
            task.lease_token,
        )
        if completed_task != "UPDATE 1":
            raise StaleLeaseError(f"workflow task {task.id} lost its lease before commit")


async def complete_activity(
    pool: Pool,
    *,
    task: LeasedTask,
    result: JSONValue,
) -> None:
    """Fence an activity completion, append its result, and wake workflow replay."""
    if task.task_type != "activity" or task.entity_id is None:
        raise ValueError("only an activity task can complete an activity")
    async with pool.acquire() as connection, connection.transaction():
        locked_task = await connection.fetchrow(
            """
            select workflow_id, entity_id, attempt, queue_name
            from tasks
            where id = $1 and status = 'leased' and lease_token = $2
              and lease_expires_at > now()
              and (start_to_close_deadline is null or start_to_close_deadline > now())
              and (
                heartbeat_timeout is null
                or heartbeat_at + heartbeat_timeout > now()
              )
            for update
            """,
            task.id,
            task.lease_token,
        )
        if locked_task is None:
            raise StaleLeaseError(f"activity task {task.id} does not hold the current lease")
        execution = await connection.fetchrow(
            """
            select status, next_seq
            from workflow_executions
            where id = $1
            for update
            """,
            task.workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {task.workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {task.workflow_id} is {execution['status']}")

        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, entity_id, attributes
            ) values ($1, $2, 'ActivityCompleted', $3, $4::jsonb)
            """,
            task.workflow_id,
            next_seq,
            task.entity_id,
            canonical_json(
                {
                    "result": clone_json(result),
                    "attempt": cast(int, locked_task["attempt"]),
                }
            ),
        )
        completed_task = await connection.execute(
            """
            update tasks
            set status = 'completed', completed_at = now()
            where id = $1 and status = 'leased' and lease_token = $2
            """,
            task.id,
            task.lease_token,
        )
        if completed_task != "UPDATE 1":
            raise StaleLeaseError(f"activity task {task.id} lost its lease before commit")
        await connection.execute(
            """
            insert into tasks (id, workflow_id, task_type, queue_name)
            values ($1, $2, 'workflow', $3)
            """,
            uuid4(),
            task.workflow_id,
            locked_task["queue_name"],
        )
        await connection.execute(
            "update workflow_executions set next_seq = $2 where id = $1",
            task.workflow_id,
            next_seq + 1,
        )


def retry_delay(
    retry_policy: dict[str, JSONValue],
    *,
    failed_attempt: int,
    random_value: float,
) -> timedelta:
    """Calculate full-jitter backoff for the next attempt."""
    if not 0 <= random_value <= 1:
        raise ValueError("random_value must be between 0 and 1")
    initial = float(cast(int | float, retry_policy["initial_interval_seconds"]))
    coefficient = float(cast(int | float, retry_policy["backoff_coefficient"]))
    maximum_value = retry_policy.get("maximum_interval_seconds")
    maximum = float(cast(int | float, maximum_value)) if maximum_value is not None else None
    cap = initial * coefficient ** (failed_attempt - 1)
    if maximum is not None:
        cap = min(cap, maximum)
    return timedelta(seconds=cap * random_value)


async def fail_activity(
    pool: Pool,
    *,
    task: LeasedTask,
    failure: JSONValue,
    random_value: float | None = None,
) -> bool:
    """Record an attempt failure and create a retry, returning whether it is final."""
    if task.task_type != "activity" or task.entity_id is None or task.command_id is None:
        raise ValueError("only an activity task can fail an activity")
    if not isinstance(task.input, dict):
        raise TypeError("activity task input must be an object")
    policy_value = task.input.get("retry_policy")
    if not isinstance(policy_value, dict):
        raise TypeError("activity task lacks its recorded retry policy")
    policy = policy_value
    max_attempts_value = policy.get("max_attempts")
    if not isinstance(max_attempts_value, int):
        raise TypeError("activity retry policy has invalid max_attempts")
    is_final = task.attempt >= max_attempts_value
    # This is retry-delay jitter, not a security decision or identifier.
    selected_random = random.random() if random_value is None else random_value  # nosec B311
    delay = (
        timedelta(0)
        if is_final
        else retry_delay(
            policy,
            failed_attempt=task.attempt,
            random_value=selected_random,
        )
    )
    next_visible_at = datetime.now(UTC) + delay

    async with pool.acquire() as connection, connection.transaction():
        locked_task = await connection.fetchrow(
            """
            select attempt, queue_name, input, schedule_to_start_timeout,
                   start_to_close_timeout, heartbeat_timeout
            from tasks
            where id = $1 and status = 'leased' and lease_token = $2
              and lease_expires_at > now()
              and (start_to_close_deadline is null or start_to_close_deadline > now())
              and (
                heartbeat_timeout is null
                or heartbeat_at + heartbeat_timeout > now()
              )
            for update
            """,
            task.id,
            task.lease_token,
        )
        if locked_task is None:
            raise StaleLeaseError(f"activity task {task.id} does not hold the current lease")
        execution = await connection.fetchrow(
            """
            select status, next_seq from workflow_executions where id = $1 for update
            """,
            task.workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {task.workflow_id} does not exist")
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {task.workflow_id} is {execution['status']}")
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, entity_id, attributes
            ) values ($1, $2, 'ActivityFailed', $3, $4::jsonb)
            """,
            task.workflow_id,
            next_seq,
            task.entity_id,
            canonical_json(
                {
                    "attempt": task.attempt,
                    "failure": clone_json(failure),
                    "final": is_final,
                    "next_visible_at": next_visible_at.isoformat() if not is_final else None,
                }
            ),
        )
        await connection.execute(
            """
            update tasks
            set status = $3::task_status, completed_at = now()
            where id = $1 and status = 'leased' and lease_token = $2
            """,
            task.id,
            task.lease_token,
            "dead" if is_final else "completed",
        )
        if is_final:
            await connection.execute(
                """
                insert into tasks (id, workflow_id, task_type, queue_name)
                values ($1, $2, 'workflow', $3)
                """,
                uuid4(),
                task.workflow_id,
                locked_task["queue_name"],
            )
        else:
            await connection.execute(
                """
                insert into tasks (
                  id, workflow_id, task_type, queue_name, entity_id, command_id,
                  attempt, input, visible_at, schedule_to_start_deadline,
                  schedule_to_start_timeout, start_to_close_timeout, heartbeat_timeout
                ) values (
                  $1, $2, 'activity', $3, $4, $5, $6, $7::jsonb, $8,
                  case when $9::interval is null then null else $8::timestamptz + $9 end,
                  $9, $10, $11
                )
                """,
                uuid4(),
                task.workflow_id,
                locked_task["queue_name"],
                task.entity_id,
                task.command_id,
                task.attempt + 1,
                locked_task["input"],
                next_visible_at,
                locked_task["schedule_to_start_timeout"],
                locked_task["start_to_close_timeout"],
                locked_task["heartbeat_timeout"],
            )
        await connection.execute(
            "update workflow_executions set next_seq = $2 where id = $1",
            task.workflow_id,
            next_seq + 1,
        )
    return is_final
