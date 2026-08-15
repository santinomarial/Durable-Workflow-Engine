import random
from datetime import timedelta
from uuid import UUID

import pytest

from engine.runtime.commands import RecordMarker, ScheduleActivity, ScheduleTimer
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
            "schedule_to_start_seconds": command.schedule_to_start_seconds,
            "start_to_close_seconds": command.start_to_close_seconds,
            "heartbeat_timeout_seconds": command.heartbeat_timeout_seconds,
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


@workflow(version=1, name="deterministic-values")
async def deterministic_values(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del value
    return {
        "now": ctx.now().isoformat(),
        "random": ctx.random(),
        "uuid": str(ctx.uuid()),
    }


def recorded_marker(seq: int, command: RecordMarker) -> HistoryEvent:
    return HistoryEvent(
        seq,
        "MarkerRecorded",
        {
            "marker_type": command.marker_type,
            "value": command.value,
            "fingerprint": command.fingerprint,
        },
        command_id=command.command_id,
    )


async def test_deterministic_values_are_recorded_and_replayed() -> None:
    history = (started(),)
    values: dict[str, JSONValue] = {}
    for marker_type in ("now", "random", "uuid"):
        left = await replay_workflow(
            deterministic_values,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=history,
        )
        right = await replay_workflow(
            deterministic_values,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=history,
        )
        assert left == right
        assert left.status is ReplayStatus.COMMANDS
        command = left.commands[0]
        assert isinstance(command, RecordMarker)
        assert command.marker_type == marker_type
        values[marker_type] = command.value
        history = (*history, recorded_marker(len(history) + 1, command))

    replay = await replay_workflow(
        deterministic_values,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=history,
    )
    assert replay.status is ReplayStatus.COMPLETED
    assert replay.result == values


async def test_deterministic_value_type_change_reports_first_divergence() -> None:
    replay = await replay_workflow(
        deterministic_values,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=(started(),),
    )
    now_marker = replay.commands[0]
    assert isinstance(now_marker, RecordMarker)
    changed = recorded_marker(2, now_marker)
    changed = HistoryEvent(
        changed.seq,
        changed.event_type,
        {**changed.attributes, "marker_type": "uuid"},
        command_id=changed.command_id,
    )

    with pytest.raises(NonDeterminismError, match="non-determinism at command 0"):
        await replay_workflow(
            deterministic_values,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=(started(), changed),
        )


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


@workflow(version=1)
async def timeout_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(
        uppercase,
        value,
        schedule_to_start=timedelta(seconds=5),
        start_to_close=timedelta(seconds=10),
        heartbeat_timeout=timedelta(seconds=2),
    )


async def test_activity_timeout_options_are_fingerprinted() -> None:
    replay = await replay_workflow(
        timeout_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )
    command = replay.commands[0]

    assert command.schedule_to_start_seconds == 5
    assert command.start_to_close_seconds == 10
    assert command.heartbeat_timeout_seconds == 2


@workflow(version=1)
async def invalid_timeout_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.activity(uppercase, value, start_to_close=timedelta(0))


async def test_activity_rejects_nonpositive_timeout() -> None:
    replay = await replay_workflow(
        invalid_timeout_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="hello",
        history=(started(),),
    )

    assert replay.status is ReplayStatus.FAILED
    assert replay.failure == {"type": "ValueError", "message": "start_to_close must be positive"}


@workflow(version=1)
async def timer_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.sleep(timedelta(hours=1))
    return value


async def test_timer_replay_schedules_blocks_and_resolves() -> None:
    initial = await replay_workflow(
        timer_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="awake",
        history=(started(),),
    )
    timer = initial.commands[0]
    assert isinstance(timer, ScheduleTimer)
    assert timer.delay_seconds == 3600
    timer_started = HistoryEvent(
        2,
        "TimerStarted",
        {
            "delay_seconds": timer.delay_seconds,
            "fingerprint": timer.fingerprint,
            "purpose": timer.purpose,
            "signal_name": timer.signal_name,
        },
        command_id=timer.command_id,
        entity_id=timer.entity_id,
    )
    blocked = await replay_workflow(
        timer_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="awake",
        history=(started(), timer_started),
    )
    assert blocked.status is ReplayStatus.BLOCKED

    completed_replay = await replay_workflow(
        timer_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input="awake",
        history=(
            started(),
            timer_started,
            HistoryEvent(3, "TimerFired", {}, entity_id=timer.entity_id),
        ),
    )
    assert completed_replay.status is ReplayStatus.COMPLETED
    assert completed_replay.result == "awake"


@workflow(version=1)
async def two_signal_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del value
    first = await ctx.wait_signal("item")
    second = await ctx.wait_signal("item")
    return [first, second]


async def test_signal_waits_consume_matching_events_once_in_history_order() -> None:
    history = (
        started(),
        HistoryEvent(
            2,
            "SignalReceived",
            {"name": "other", "payload": 0},
            external_id="other-1",
        ),
        HistoryEvent(
            3,
            "SignalReceived",
            {"name": "item", "payload": 1},
            external_id="item-1",
        ),
        HistoryEvent(
            4,
            "SignalReceived",
            {"name": "item", "payload": 2},
            external_id="item-2",
        ),
    )
    replay = await replay_workflow(
        two_signal_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=history,
    )

    assert replay.status is ReplayStatus.COMPLETED
    assert replay.result == [1, 2]


@workflow(version=1, name="cancellation-aware")
async def cancellation_aware(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    if ctx.cancellation_requested:
        return "cancelled"
    return value


async def test_workflow_context_exposes_recorded_cancellation() -> None:
    replay = await replay_workflow(
        cancellation_aware,
        workflow_id=WORKFLOW_ID,
        workflow_input="running",
        history=(
            started(),
            HistoryEvent(
                2,
                "WorkflowCancellationRequested",
                {"reason": "operator request"},
            ),
        ),
    )

    assert replay.status is ReplayStatus.COMPLETED
    assert replay.result == "cancelled"


@workflow(version=1)
async def gather_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del value
    return await ctx.gather(
        ctx.activity(uppercase, "first"),
        ctx.activity(uppercase, "second"),
    )


async def test_gather_schedules_all_children_and_joins_in_source_order() -> None:
    initial = await replay_workflow(
        gather_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=(started(),),
    )
    assert initial.status is ReplayStatus.COMMANDS
    assert [command.command_id for command in initial.commands] == [0, 1]
    assert all(isinstance(command, ScheduleActivity) for command in initial.commands)
    first, second = initial.commands
    assert isinstance(first, ScheduleActivity)
    assert isinstance(second, ScheduleActivity)

    schedules = (scheduled(2, first), scheduled(3, second))
    blocked = await replay_workflow(
        gather_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=(started(), *schedules),
    )
    assert blocked.status is ReplayStatus.BLOCKED

    joined = await replay_workflow(
        gather_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=(
            started(),
            *schedules,
            completed(4, second, "SECOND"),
            completed(5, first, "FIRST"),
        ),
    )
    assert joined.status is ReplayStatus.COMPLETED
    assert joined.result == ["FIRST", "SECOND"]


@workflow(version=1, name="fuzzed-interleavings")
async def fuzzed_interleavings(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del value
    activities = await ctx.gather(*(ctx.activity(uppercase, str(index)) for index in range(5)))
    first_signal = await ctx.wait_signal("item")
    second_signal = await ctx.wait_signal("item")
    return {"activities": activities, "signals": [first_signal, second_signal]}


async def test_replay_fuzzes_completion_and_signal_arrival_order() -> None:
    initial = await replay_workflow(
        fuzzed_interleavings,
        workflow_id=WORKFLOW_ID,
        workflow_input=None,
        history=(started(),),
    )
    commands = initial.commands
    assert len(commands) == 5
    assert all(isinstance(command, ScheduleActivity) for command in commands)
    activity_commands = [command for command in commands if isinstance(command, ScheduleActivity)]
    schedules = tuple(
        scheduled(index + 2, command) for index, command in enumerate(activity_commands)
    )

    for seed in range(50):
        interleaved: list[tuple[str, int | str]] = [
            *(("completion", index) for index in range(5)),
            ("item", "first"),
            ("item", "second"),
            ("noise", "left"),
            ("noise", "right"),
        ]
        random.Random(seed).shuffle(interleaved)
        events: list[HistoryEvent] = [started(), *schedules]
        expected_signals: list[str] = []
        for kind, value in interleaved:
            seq = len(events) + 1
            if kind == "completion":
                index = int(value)
                events.append(completed(seq, activity_commands[index], str(index)))
            else:
                payload = str(value)
                events.append(
                    HistoryEvent(
                        seq,
                        "SignalReceived",
                        {"name": kind, "payload": payload},
                        external_id=f"{seed}-{kind}-{payload}",
                    )
                )
                if kind == "item":
                    expected_signals.append(payload)

        replay = await replay_workflow(
            fuzzed_interleavings,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=tuple(events),
        )
        repeated = await replay_workflow(
            fuzzed_interleavings,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=tuple(events),
        )
        assert replay == repeated
        assert replay.result == {
            "activities": ["0", "1", "2", "3", "4"],
            "signals": expected_signals,
        }


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


async def test_replay_rejects_missing_start_and_events_after_terminal() -> None:
    with pytest.raises(InvalidHistoryError, match="must begin"):
        await replay_workflow(
            immediate_workflow,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=(),
        )
    with pytest.raises(InvalidHistoryError, match="appears after terminal"):
        await replay_workflow(
            immediate_workflow,
            workflow_id=WORKFLOW_ID,
            workflow_input=None,
            history=(
                started(),
                HistoryEvent(2, "WorkflowExecutionCompleted", {"result": None}),
                HistoryEvent(
                    3,
                    "SignalReceived",
                    {"name": "late", "payload": None},
                    external_id="late",
                ),
            ),
        )
