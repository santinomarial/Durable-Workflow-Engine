from uuid import UUID

import pytest

from engine.runtime.commands import ScheduleActivity
from engine.runtime.history import HistoryEvent, InvalidHistoryError
from engine.runtime.replay import ReplayStatus, replay_workflow
from engine.runtime.serialization import JSONValue
from engine.sdk import NonDeterminismError, WorkflowContext, activity, workflow

WORKFLOW_ID = UUID("ff2e347b-6072-4fff-9a82-50f9bb46a27d")


@activity()
async def uppercase(value: JSONValue) -> JSONValue:
    assert isinstance(value, str)
    return value.upper()


@workflow(version=1)
async def sequential_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    first = await ctx.activity(uppercase, value)
    return await ctx.activity(uppercase, first)


@workflow(version=1)
async def immediate_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


def started() -> HistoryEvent:
    return HistoryEvent(1, "WorkflowExecutionStarted", {"input": "hello"})


def scheduled(seq: int, command: ScheduleActivity) -> HistoryEvent:
    return HistoryEvent(
        seq,
        "ActivityScheduled",
        {
            "activity_type": command.activity_type,
            "fingerprint": command.fingerprint,
            "input": command.input,
            "retry_policy": command.retry_policy,
            "start_to_close_seconds": command.start_to_close_seconds,
        },
        command_id=command.command_id,
        entity_id=command.entity_id,
    )


def completed(seq: int, command: ScheduleActivity, result: JSONValue) -> HistoryEvent:
    return HistoryEvent(
        seq,
        "ActivityCompleted",
        {"result": result, "attempt": 1},
        entity_id=command.entity_id,
    )


async def test_sequential_replay_schedules_and_resolves_each_command() -> None:
    first_replay = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )
    assert first_replay.status is ReplayStatus.COMMANDS
    first_command = first_replay.commands[0]
    assert first_command.command_id == 0
    assert first_command.input == {"args": ["hello"], "kwargs": {}}

    first_schedule = scheduled(2, first_command)
    blocked_replay = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(), first_schedule),
    )
    assert blocked_replay.status is ReplayStatus.BLOCKED

    first_completion = completed(3, first_command, "HELLO")
    second_replay = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(), first_schedule, first_completion),
    )
    assert second_replay.status is ReplayStatus.COMMANDS
    second_command = second_replay.commands[0]
    assert second_command.command_id == 1
    assert second_command.input == {"args": ["HELLO"], "kwargs": {}}

    final_replay = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(
            started(),
            first_schedule,
            first_completion,
            scheduled(4, second_command),
            completed(5, second_command, "HELLO"),
        ),
    )
    assert final_replay.status is ReplayStatus.COMPLETED
    assert final_replay.result == "HELLO"


async def test_replay_emits_stable_command_identity() -> None:
    left = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )
    right = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )

    assert left == right


async def test_replay_reports_changed_command_fingerprint() -> None:
    first = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )
    event = scheduled(2, first.commands[0])
    changed = HistoryEvent(
        event.seq,
        event.event_type,
        {**event.attributes, "fingerprint": "changed"},
        command_id=event.command_id,
        entity_id=event.entity_id,
    )

    with pytest.raises(NonDeterminismError, match="non-determinism at command 0"):
        await replay_workflow(
            sequential_workflow,
            workflow_id=WORKFLOW_ID,
            workflow_input="hello",
            history=(started(), changed),
        )


async def test_replay_rejects_unvisited_historical_command() -> None:
    first = await replay_workflow(
        sequential_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )

    with pytest.raises(NonDeterminismError, match="unvisited ActivityScheduled"):
        await replay_workflow(
            immediate_workflow,
            workflow_id=WORKFLOW_ID,
            workflow_input="hello",
            history=(started(), scheduled(2, first.commands[0])),
        )


async def test_replay_rejects_non_contiguous_history() -> None:
    with pytest.raises(InvalidHistoryError, match="expected 2, found 3"):
        await replay_workflow(
            sequential_workflow,
            workflow_id=WORKFLOW_ID,
            workflow_input="hello",
            history=(started(), HistoryEvent(3, "SignalReceived", {})),
        )


async def test_replay_rejects_terminal_event_without_schedule() -> None:
    with pytest.raises(InvalidHistoryError, match="has no prior activity schedule"):
        await replay_workflow(
            sequential_workflow,
            workflow_id=WORKFLOW_ID,
            workflow_input="hello",
            history=(
                started(),
                HistoryEvent(
                    2,
                    "ActivityCompleted",
                    {"result": "orphaned"},
                    entity_id=UUID("c53d44e5-8fc5-47a2-8884-a16ef30563f4"),
                ),
            ),
        )
