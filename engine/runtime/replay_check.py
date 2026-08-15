"""Non-mutating compatibility checks for persisted workflow histories."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from engine.persistence.database import Pool
from engine.persistence.transitions import TransitionError, history_event_from_row
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.replay import ReplayStatus, replay_workflow
from engine.runtime.serialization import JSONValue
from engine.sdk.context import NonDeterminismError


@dataclass(frozen=True, slots=True)
class ReplayCheckReport:
    workflow_id: UUID
    workflow_type: str
    stored_version: int
    against_version: int
    compatible: bool
    replay_status: ReplayStatus | None
    divergence_command_id: int | None = None
    message: str | None = None


async def replay_check(
    pool: Pool,
    *,
    workflow_id: UUID,
    definition: WorkflowDefinition,
) -> ReplayCheckReport:
    """Replay persisted history against a candidate definition without any writes."""
    async with pool.acquire() as connection:
        execution = await connection.fetchrow(
            """
            select workflow_type, definition_version, input, status, result, failure
            from workflow_executions where id = $1
            """,
            workflow_id,
        )
        if execution is None:
            raise TransitionError(f"workflow {workflow_id} does not exist")
        rows = await connection.fetch(
            """
            select seq, event_type, command_id, entity_id, external_id, attributes
            from history_events where workflow_id = $1 order by seq
            """,
            workflow_id,
        )
    workflow_type = cast(str, execution["workflow_type"])
    stored_version = cast(int, execution["definition_version"])
    if definition.name != workflow_type:
        return ReplayCheckReport(
            workflow_id,
            workflow_type,
            stored_version,
            definition.version,
            False,
            None,
            message=f"definition type {definition.name!r} does not match {workflow_type!r}",
        )
    workflow_input = cast(JSONValue, json.loads(cast(str, execution["input"])))
    history = tuple(history_event_from_row(dict(row)) for row in rows)
    try:
        replay = await replay_workflow(
            definition,
            workflow_id=workflow_id,
            workflow_input=workflow_input,
            history=history,
        )
    except NonDeterminismError as error:
        return ReplayCheckReport(
            workflow_id,
            workflow_type,
            stored_version,
            definition.version,
            False,
            None,
            divergence_command_id=error.command_id,
            message=error.detail,
        )
    stored_status = str(execution["status"])
    expected_status = {
        "completed": ReplayStatus.COMPLETED,
        "failed": ReplayStatus.FAILED,
    }.get(stored_status)
    if expected_status is not None and replay.status is not expected_status:
        return ReplayCheckReport(
            workflow_id,
            workflow_type,
            stored_version,
            definition.version,
            False,
            replay.status,
            message=(
                f"stored workflow is {stored_status}, but replay produced {replay.status.value}"
            ),
        )
    if stored_status == "completed":
        stored_result = cast(JSONValue, json.loads(cast(str, execution["result"])))
        if replay.result != stored_result:
            return ReplayCheckReport(
                workflow_id,
                workflow_type,
                stored_version,
                definition.version,
                False,
                replay.status,
                message="replayed result differs from the stored terminal result",
            )
    if stored_status == "failed":
        stored_failure = cast(JSONValue, json.loads(cast(str, execution["failure"])))
        if replay.failure != stored_failure:
            return ReplayCheckReport(
                workflow_id,
                workflow_type,
                stored_version,
                definition.version,
                False,
                replay.status,
                message="replayed failure differs from the stored terminal failure",
            )
    return ReplayCheckReport(
        workflow_id,
        workflow_type,
        stored_version,
        definition.version,
        True,
        replay.status,
    )
