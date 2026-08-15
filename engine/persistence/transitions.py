"""Atomic workflow state transitions.

All accepted history changes and their resulting tasks are committed together in
this module so the engine's central correctness boundary remains auditable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast
from uuid import UUID, uuid4

from engine.persistence.database import Connection, Pool
from engine.persistence.leasing import LeasedTask
from engine.runtime.commands import ScheduleActivity
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


class StaleLeaseError(TransitionError):
    """Raised when a task transition does not own the current lease token."""


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


def _history_event(row: dict[str, object]) -> HistoryEvent:
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
) -> StartedWorkflow:
    """Atomically create an execution, its first event, and its first task."""
    if not workflow_type:
        raise ValueError("workflow_type cannot be empty")
    if definition_version < 1:
        raise ValueError("definition_version must be at least 1")
    if not queue_name:
        raise ValueError("queue_name cannot be empty")

    execution_id = workflow_id or uuid4()
    detached_input = clone_json(workflow_input)
    encoded_input = canonical_json(detached_input)
    started_attributes = canonical_json(
        {
            "workflow_type": workflow_type,
            "definition_version": definition_version,
            "input": detached_input,
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
              id, workflow_type, definition_version, input, next_seq
            ) values ($1, $2, $3, $4::jsonb, 2)
            """,
            execution_id,
            workflow_type,
            definition_version,
            encoded_input,
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

    return StartedWorkflow(execution_id, workflow_type, definition_version)


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
        history=tuple(_history_event(dict(row)) for row in rows),
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
            "start_to_close_seconds": command.start_to_close_seconds,
            "idempotency_key": str(command.entity_id),
        }
    )
    task_input = canonical_json(
        {
            "activity_type": command.activity_type,
            "input": command.input,
            "idempotency_key": str(command.entity_id),
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
          input, start_to_close_timeout
        ) values ($1, $2, 'activity', $3, $4, $5, $6::jsonb,
                  $7::double precision * interval '1 second')
        """,
        uuid4(),
        workflow_id,
        queue_name,
        command.entity_id,
        command.command_id,
        task_input,
        command.start_to_close_seconds,
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
            for update
            """,
            task.id,
            task.lease_token,
        )
        if locked_task is None:
            raise StaleLeaseError(f"workflow task {task.id} does not hold the current lease")
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
        if replay.status is ReplayStatus.COMMANDS:
            if not replay.commands:
                raise TransitionError("commands replay result contains no commands")
            for command in replay.commands:
                await _append_activity_command(
                    connection,
                    workflow_id=task.workflow_id,
                    seq=next_seq,
                    command=command,
                    queue_name=cast(str, locked_task["queue_name"]),
                )
                next_seq += 1
            await connection.execute(
                "update workflow_executions set next_seq = $2 where id = $1",
                task.workflow_id,
                next_seq,
            )
        elif replay.status in (ReplayStatus.COMPLETED, ReplayStatus.FAILED):
            completed = replay.status is ReplayStatus.COMPLETED
            event_type = "WorkflowExecutionCompleted" if completed else "WorkflowExecutionFailed"
            payload = replay.result if completed else replay.failure
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
