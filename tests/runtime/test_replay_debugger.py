from uuid import UUID

from engine.runtime.history import HistoryEvent
from engine.runtime.replay_debugger import build_replay_trace, compare_command_histories

ENTITY = UUID("a405a43c-dc36-44a4-b3ec-a7d73b0add8f")


def activity_history(fingerprint: str = "stable") -> tuple[HistoryEvent, ...]:
    return (
        HistoryEvent(1, "WorkflowExecutionStarted", {"workflow_type": "debug"}),
        HistoryEvent(
            2,
            "ActivityScheduled",
            {"activity_type": "charge", "fingerprint": fingerprint},
            command_id=0,
            entity_id=ENTITY,
        ),
        HistoryEvent(3, "ActivityCompleted", {"result": {"ok": True}}, entity_id=ENTITY),
        HistoryEvent(4, "SignalReceived", {"name": "ship"}, external_id="signal-1"),
        HistoryEvent(5, "WorkflowExecutionCompleted", {"result": "done"}),
    )


def test_build_replay_trace_exposes_causality_and_state_at_each_event() -> None:
    frames = build_replay_trace(activity_history())
    scheduled = frames[1]
    completed = frames[2]
    terminal = frames[-1]

    assert scheduled.category == "command"
    assert scheduled.snapshot.waiting_entities == 1
    assert scheduled.snapshot.active_entities[0].label == "charge"
    assert completed.caused_by_seq == scheduled.seq
    assert completed.snapshot.waiting_entities == 0
    assert completed.snapshot.succeeded_entities == 1
    assert terminal.snapshot.signals_received == 1
    assert terminal.snapshot.terminal_status == "completed"


def test_compare_command_histories_reports_exact_first_divergence() -> None:
    compatible = compare_command_histories(activity_history(), activity_history())
    assert compatible.compatible is True
    assert compatible.matched_commands == 1

    changed = compare_command_histories(activity_history(), activity_history("changed"))
    assert changed.compatible is False
    assert changed.matched_commands == 0
    assert changed.divergence is not None
    assert changed.divergence.command_index == 0
    assert changed.divergence.left_seq == 2
    assert changed.divergence.right_seq == 2
