"""Durable external signal ingestion."""

from __future__ import annotations

from typing import cast
from uuid import UUID, uuid4

from engine.persistence.database import Pool
from engine.persistence.transitions import TerminalWorkflowError, TransitionError
from engine.runtime.serialization import JSONValue, canonical_json, clone_json


async def send_signal(
    pool: Pool,
    *,
    workflow_id: UUID,
    signal_id: str,
    name: str,
    payload: JSONValue = None,
) -> bool:
    """Append a caller-deduplicated signal and wake replay in one transaction."""
    if not signal_id:
        raise ValueError("signal_id cannot be empty")
    if not name:
        raise ValueError("signal name cannot be empty")
    detached_payload = clone_json(payload)
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
        duplicate = await connection.fetchval(
            """
            select exists (
              select 1 from history_events
              where workflow_id = $1 and external_id = $2
            )
            """,
            workflow_id,
            signal_id,
        )
        if duplicate is True:
            return False
        if execution["status"] != "running":
            raise TerminalWorkflowError(f"workflow {workflow_id} is {execution['status']}")

        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, external_id, attributes
            ) values ($1, $2, 'SignalReceived', $3, $4::jsonb)
            """,
            workflow_id,
            next_seq,
            signal_id,
            canonical_json({"name": name, "payload": detached_payload}),
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
    return True
