"""Atomic workflow state transitions.

All accepted history changes and their resulting tasks are committed together in
this module so the engine's central correctness boundary remains auditable.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from engine.persistence.database import Pool
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.serialization import JSONValue, canonical_json, clone_json


class TransitionError(RuntimeError):
    """Base class for rejected durable transitions."""


class DefinitionNotRegisteredError(TransitionError):
    """Raised when an execution references a definition absent from storage."""


class StoredDefinitionConflictError(TransitionError):
    """Raised when stored and worker definition hashes disagree."""


@dataclass(frozen=True, slots=True)
class StartedWorkflow:
    workflow_id: UUID
    workflow_type: str
    definition_version: int


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
