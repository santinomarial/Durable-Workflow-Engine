"""Durable, deduplicated, result-bearing workflow updates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID, uuid4

from engine.persistence.audit import AuditContext, record_api_audit
from engine.persistence.database import Pool
from engine.persistence.transitions import TerminalWorkflowError, TransitionError
from engine.runtime.serialization import JSONValue, canonical_json, clone_json


@dataclass(frozen=True, slots=True)
class WorkflowUpdateRecord:
    workflow_id: UUID
    update_id: str
    name: str
    payload: JSONValue
    status: str
    result: JSONValue
    failure: JSONValue
    created_at: datetime
    completed_at: datetime | None


def _json(value: object) -> JSONValue:
    if value is None:
        return None
    return cast(JSONValue, json.loads(cast(str, value)))


def _record(row: dict[str, object]) -> WorkflowUpdateRecord:
    return WorkflowUpdateRecord(
        workflow_id=cast(UUID, row["workflow_id"]),
        update_id=cast(str, row["update_id"]),
        name=cast(str, row["name"]),
        payload=_json(row["payload"]),
        status=str(row["status"]),
        result=_json(row["result"]),
        failure=_json(row["failure"]),
        created_at=cast(datetime, row["created_at"]),
        completed_at=cast(datetime | None, row["completed_at"]),
    )


async def send_update(
    pool: Pool,
    *,
    workflow_id: UUID,
    update_id: str,
    name: str,
    payload: JSONValue = None,
    audit: AuditContext | None = None,
) -> tuple[WorkflowUpdateRecord, bool]:
    """Persist one update request and wake replay, returning record and novelty."""
    if not update_id or len(update_id) > 200:
        raise ValueError("update_id must contain 1 to 200 characters")
    if not name or len(name) > 200:
        raise ValueError("update name must contain 1 to 200 characters")
    detached = clone_json(payload)
    async with pool.acquire() as connection, connection.transaction():
        execution = await connection.fetchrow(
            """
            select status, next_seq, queue_name
            from workflow_executions where id = $1 for update
            """,
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        existing = await connection.fetchrow(
            """
            select * from workflow_updates where workflow_id = $1 and update_id = $2
            """,
            workflow_id,
            update_id,
        )
        if existing is not None:
            await record_api_audit(
                connection,
                audit,
                workflow_id=workflow_id,
                accepted=False,
                details={"update_id": update_id, "name": name, "duplicate": True},
            )
            return _record(dict(existing)), False
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {workflow_id} is {execution['status']}")
        row = await connection.fetchrow(
            """
            insert into workflow_updates (workflow_id, update_id, name, payload)
            values ($1, $2, $3, $4::jsonb) returning *
            """,
            workflow_id,
            update_id,
            name,
            canonical_json(detached),
        )
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, external_id, attributes
            ) values ($1, $2, 'WorkflowUpdateReceived', $3, $4::jsonb)
            """,
            workflow_id,
            next_seq,
            f"update:{update_id}",
            canonical_json({"update_id": update_id, "name": name, "payload": detached}),
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
        await connection.execute(
            "update workflow_executions set next_seq = $2 where id = $1",
            workflow_id,
            next_seq + 1,
        )
        await record_api_audit(
            connection,
            audit,
            workflow_id=workflow_id,
            accepted=True,
            details={"update_id": update_id, "name": name},
        )
    if row is None:
        raise RuntimeError("workflow update insert returned no row")
    return _record(dict(row)), True


async def get_update(
    pool: Pool, *, workflow_id: UUID, update_id: str
) -> WorkflowUpdateRecord | None:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "select * from workflow_updates where workflow_id = $1 and update_id = $2",
            workflow_id,
            update_id,
        )
    return _record(dict(row)) if row is not None else None


async def list_updates(
    pool: Pool, *, workflow_id: UUID, limit: int = 100
) -> tuple[WorkflowUpdateRecord, ...]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select * from workflow_updates where workflow_id = $1
            order by created_at desc, update_id limit $2
            """,
            workflow_id,
            limit,
        )
    return tuple(_record(dict(row)) for row in rows)
