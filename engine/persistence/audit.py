"""Immutable API control-plane audit records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from engine.persistence.database import Connection, Pool
from engine.runtime.serialization import JSONValue, canonical_json, clone_json


@dataclass(frozen=True, slots=True)
class AuditContext:
    request_id: UUID
    actor_key_id: str
    actor_role: str
    action: str


@dataclass(frozen=True, slots=True)
class AuditRecord:
    id: int
    occurred_at: datetime
    request_id: UUID
    actor_key_id: str
    actor_role: str
    action: str
    workflow_id: UUID | None
    accepted: bool
    details: JSONValue


async def record_api_audit(
    connection: Connection,
    context: AuditContext | None,
    *,
    workflow_id: UUID | None,
    accepted: bool,
    details: JSONValue = None,
) -> None:
    """Append an audit record inside the caller's state-transition transaction."""
    if context is None:
        return
    await connection.execute(
        """
        insert into api_audit_log (
          request_id, actor_key_id, actor_role, action, workflow_id, accepted, details
        ) values ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        context.request_id,
        context.actor_key_id,
        context.actor_role,
        context.action,
        workflow_id,
        accepted,
        canonical_json(clone_json(details)),
    )


async def list_api_audit(pool: Pool, *, limit: int = 100) -> tuple[AuditRecord, ...]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            select id, occurred_at, request_id, actor_key_id, actor_role, action,
                   workflow_id, accepted, details
            from api_audit_log order by id desc limit $1
            """,
            limit,
        )
    return tuple(
        AuditRecord(
            id=cast(int, row["id"]),
            occurred_at=cast(datetime, row["occurred_at"]),
            request_id=cast(UUID, row["request_id"]),
            actor_key_id=cast(str, row["actor_key_id"]),
            actor_role=cast(str, row["actor_role"]),
            action=cast(str, row["action"]),
            workflow_id=cast(UUID | None, row["workflow_id"]),
            accepted=cast(bool, row["accepted"]),
            details=cast(JSONValue, json.loads(cast(str, row["details"]))),
        )
        for row in rows
    )
