"""Bounded, side-effect-free inspection of committed replay history."""

from __future__ import annotations

from dataclasses import dataclass

from engine.runtime.history import SCHEDULE_EVENT_TYPES, HistoryEvent


@dataclass(frozen=True, slots=True)
class ActiveEntity:
    entity_id: str
    kind: str
    label: str
    scheduled_seq: int
    status: str


@dataclass(frozen=True, slots=True)
class ReplaySnapshot:
    commands: int
    waiting_entities: int
    succeeded_entities: int
    failed_entities: int
    signals_received: int
    pending_updates: int
    terminal_status: str | None
    active_entities: tuple[ActiveEntity, ...]


@dataclass(frozen=True, slots=True)
class ReplayFrame:
    index: int
    seq: int
    event_type: str
    category: str
    summary: str
    command_id: int | None
    entity_id: str | None
    caused_by_seq: int | None
    snapshot: ReplaySnapshot


@dataclass(frozen=True, slots=True)
class TraceDivergence:
    command_index: int
    reason: str
    left_seq: int | None
    left_event_type: str | None
    left_fingerprint: str | None
    right_seq: int | None
    right_event_type: str | None
    right_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class HistoryComparison:
    compatible: bool
    matched_commands: int
    left_commands: int
    right_commands: int
    divergence: TraceDivergence | None


_SCHEDULE_KINDS = {
    "ActivityScheduled": "activity",
    "TimerStarted": "timer",
    "ChildWorkflowStarted": "child",
}
_TERMINAL_ENTITY_EVENTS = {
    "ActivityCompleted": "succeeded",
    "ActivityFailed": "failed",
    "ActivityTimedOut": "failed",
    "TimerFired": "succeeded",
    "TimerCanceled": "failed",
    "ChildWorkflowCompleted": "succeeded",
    "ChildWorkflowFailed": "failed",
    "ChildWorkflowTerminated": "failed",
}
_WORKFLOW_TERMINALS = {
    "WorkflowExecutionCompleted": "completed",
    "WorkflowExecutionFailed": "failed",
    "WorkflowExecutionTerminated": "terminated",
}


def _label(event: HistoryEvent) -> str:
    for key in ("activity_type", "workflow_type", "purpose", "marker_type", "name"):
        value = event.attributes.get(key)
        if isinstance(value, str) and value:
            return value
    return event.event_type


def _category(event_type: str) -> str:
    if event_type in SCHEDULE_EVENT_TYPES:
        return "command"
    if event_type in _WORKFLOW_TERMINALS:
        return "terminal"
    if event_type in _TERMINAL_ENTITY_EVENTS:
        return "outcome"
    if event_type in {
        "SignalReceived",
        "WorkflowUpdateReceived",
        "WorkflowCancellationRequested",
    }:
        return "external"
    if event_type in {"WorkflowExecutionPaused", "WorkflowExecutionResumed"}:
        return "control"
    return "lifecycle"


def _summary(event: HistoryEvent) -> str:
    label = _label(event)
    summaries = {
        "WorkflowExecutionStarted": f"Started {label}",
        "ActivityScheduled": f"Scheduled activity {label}",
        "ActivityCompleted": "Activity returned a durable result",
        "ActivityFailed": "Activity attempt failed",
        "ActivityTimedOut": "Activity attempt timed out",
        "TimerStarted": f"Started timer {label}",
        "TimerFired": "Timer deadline became durable",
        "TimerCanceled": "Timer was canceled",
        "SignalReceived": f"Received signal {label}",
        "ChildWorkflowStarted": f"Started child workflow {label}",
        "ChildWorkflowCompleted": "Child workflow completed",
        "ChildWorkflowFailed": "Child workflow failed",
        "ChildWorkflowTerminated": "Child workflow was terminated",
        "WorkflowUpdateReceived": f"Received update {label}",
        "WorkflowUpdateResolved": "Workflow update was resolved",
        "MarkerRecorded": f"Recorded deterministic {label} value",
        "WorkflowExecutionContinuedAsNew": f"Continued as {label}",
        "WorkflowExecutionCompleted": "Workflow completed",
        "WorkflowExecutionFailed": "Workflow failed",
        "WorkflowExecutionTerminated": "Workflow terminated",
        "WorkflowCancellationRequested": "Cancellation was requested",
        "WorkflowExecutionPaused": "Dispatch and deadlines were paused",
        "WorkflowExecutionResumed": "Dispatch and deadlines resumed",
    }
    return summaries.get(event.event_type, event.event_type)


def build_replay_trace(events: tuple[HistoryEvent, ...]) -> tuple[ReplayFrame, ...]:
    """Project committed history into debugger frames without executing workflow code."""
    entities: dict[str, ActiveEntity] = {}
    commands = succeeded = failed = signals = 0
    pending_updates: set[str] = set()
    terminal_status: str | None = None
    frames: list[ReplayFrame] = []

    for index, event in enumerate(events):
        caused_by_seq: int | None = None
        if event.event_type in SCHEDULE_EVENT_TYPES:
            commands += 1
        kind = _SCHEDULE_KINDS.get(event.event_type)
        if kind is not None and event.entity_id is not None:
            entity_id = str(event.entity_id)
            entities[entity_id] = ActiveEntity(
                entity_id=entity_id,
                kind=kind,
                label=_label(event),
                scheduled_seq=event.seq,
                status="waiting",
            )
        elif event.event_type in {
            "MarkerRecorded",
            "WorkflowUpdateResolved",
            "WorkflowExecutionContinuedAsNew",
        }:
            succeeded += 1

        terminal_entity_status = _TERMINAL_ENTITY_EVENTS.get(event.event_type)
        if terminal_entity_status is not None and event.entity_id is not None:
            entity_id = str(event.entity_id)
            entity = entities.get(entity_id)
            if entity is not None:
                caused_by_seq = entity.scheduled_seq
                final = event.event_type == "ActivityCompleted" or bool(
                    event.attributes.get("final", True)
                )
                if final:
                    entities.pop(entity_id)
                    if terminal_entity_status == "succeeded":
                        succeeded += 1
                    else:
                        failed += 1
                else:
                    entities[entity_id] = ActiveEntity(
                        entity.entity_id,
                        entity.kind,
                        entity.label,
                        entity.scheduled_seq,
                        "retrying",
                    )

        if event.event_type == "SignalReceived":
            signals += 1
        elif event.event_type == "WorkflowUpdateReceived":
            update_id = event.attributes.get("update_id")
            if isinstance(update_id, str):
                pending_updates.add(update_id)
        elif event.event_type == "WorkflowUpdateResolved":
            update_id = event.attributes.get("update_id")
            if isinstance(update_id, str):
                pending_updates.discard(update_id)
        if event.event_type in _WORKFLOW_TERMINALS:
            terminal_status = _WORKFLOW_TERMINALS[event.event_type]

        active = tuple(sorted(entities.values(), key=lambda item: item.scheduled_seq)[:20])
        frames.append(
            ReplayFrame(
                index=index,
                seq=event.seq,
                event_type=event.event_type,
                category=_category(event.event_type),
                summary=_summary(event),
                command_id=event.command_id,
                entity_id=str(event.entity_id) if event.entity_id is not None else None,
                caused_by_seq=caused_by_seq,
                snapshot=ReplaySnapshot(
                    commands=commands,
                    waiting_entities=len(entities),
                    succeeded_entities=succeeded,
                    failed_entities=failed,
                    signals_received=signals,
                    pending_updates=len(pending_updates),
                    terminal_status=terminal_status,
                    active_entities=active,
                ),
            )
        )
    return tuple(frames)


def _command_signature(event: HistoryEvent) -> tuple[str, int | None, str | None]:
    fingerprint = event.attributes.get("fingerprint")
    return (
        event.event_type,
        event.command_id,
        fingerprint if isinstance(fingerprint, str) else None,
    )


def compare_command_histories(
    left: tuple[HistoryEvent, ...], right: tuple[HistoryEvent, ...]
) -> HistoryComparison:
    """Find the first command-stream divergence between two committed runs."""
    left_commands = tuple(event for event in left if event.event_type in SCHEDULE_EVENT_TYPES)
    right_commands = tuple(event for event in right if event.event_type in SCHEDULE_EVENT_TYPES)
    matched = 0
    for matched, (left_event, right_event) in enumerate(
        zip(left_commands, right_commands, strict=False)
    ):
        if _command_signature(left_event) != _command_signature(right_event):
            return HistoryComparison(
                compatible=False,
                matched_commands=matched,
                left_commands=len(left_commands),
                right_commands=len(right_commands),
                divergence=TraceDivergence(
                    command_index=matched,
                    reason="command type, ordinal, or fingerprint differs",
                    left_seq=left_event.seq,
                    left_event_type=left_event.event_type,
                    left_fingerprint=_command_signature(left_event)[2],
                    right_seq=right_event.seq,
                    right_event_type=right_event.event_type,
                    right_fingerprint=_command_signature(right_event)[2],
                ),
            )
    common = min(len(left_commands), len(right_commands))
    if len(left_commands) != len(right_commands):
        next_left = left_commands[common] if common < len(left_commands) else None
        next_right = right_commands[common] if common < len(right_commands) else None
        return HistoryComparison(
            compatible=False,
            matched_commands=common,
            left_commands=len(left_commands),
            right_commands=len(right_commands),
            divergence=TraceDivergence(
                command_index=common,
                reason="one command stream ended before the other",
                left_seq=next_left.seq if next_left else None,
                left_event_type=next_left.event_type if next_left else None,
                left_fingerprint=_command_signature(next_left)[2] if next_left else None,
                right_seq=next_right.seq if next_right else None,
                right_event_type=next_right.event_type if next_right else None,
                right_fingerprint=_command_signature(next_right)[2] if next_right else None,
            ),
        )
    return HistoryComparison(
        compatible=True,
        matched_commands=common,
        left_commands=len(left_commands),
        right_commands=len(right_commands),
        divergence=None,
    )
