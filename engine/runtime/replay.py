"""Replay a workflow definition against committed history."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from engine.runtime.commands import Command
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.history import HistoryEvent, HistoryIndex
from engine.runtime.serialization import JSONValue, clone_json
from engine.sdk.context import NonDeterminismError, WorkflowContext, _Blocked, _NewCommands


class ReplayStatus(StrEnum):
    COMMANDS = "commands"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ReplayResult:
    status: ReplayStatus
    commands: tuple[Command, ...] = ()
    result: JSONValue = None
    failure: JSONValue = None


async def replay_workflow(
    definition: WorkflowDefinition,
    *,
    workflow_id: UUID,
    workflow_input: JSONValue,
    history: tuple[HistoryEvent, ...],
) -> ReplayResult:
    index = HistoryIndex(history)
    context = WorkflowContext(workflow_id, index)
    try:
        result = await definition.function(context, clone_json(workflow_input))
    except _NewCommands as suspended:
        return ReplayResult(ReplayStatus.COMMANDS, commands=suspended.commands)
    except _Blocked:
        return ReplayResult(ReplayStatus.BLOCKED)
    except NonDeterminismError:
        raise
    except Exception as error:
        return ReplayResult(
            ReplayStatus.FAILED,
            failure={"type": type(error).__name__, "message": str(error)},
        )

    unvisited = index.first_unvisited_command(context.next_command_id)
    if unvisited is not None:
        assert unvisited.command_id is not None
        raise NonDeterminismError(
            unvisited.command_id,
            f"history contains unvisited {unvisited.event_type}",
        )
    return ReplayResult(ReplayStatus.COMPLETED, result=clone_json(result))
