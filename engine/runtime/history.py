"""Typed workflow history events and replay indexes."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from engine.runtime.serialization import JSONValue

SCHEDULE_EVENT_TYPES = frozenset({"ActivityScheduled", "TimerStarted", "MarkerRecorded"})
ACTIVITY_TERMINAL_EVENT_TYPES = frozenset(
    {"ActivityCompleted", "ActivityFailed", "ActivityTimedOut"}
)
TIMER_TERMINAL_EVENT_TYPES = frozenset({"TimerFired", "TimerCanceled"})
WORKFLOW_TERMINAL_EVENT_TYPES = frozenset(
    {
        "WorkflowExecutionCompleted",
        "WorkflowExecutionFailed",
        "WorkflowExecutionTerminated",
    }
)


@dataclass(frozen=True, slots=True)
class HistoryEvent:
    seq: int
    event_type: str
    attributes: dict[str, JSONValue]
    command_id: int | None = None
    entity_id: UUID | None = None
    external_id: str | None = None


class InvalidHistoryError(RuntimeError):
    """Raised when committed history violates an engine invariant."""


class HistoryIndex:
    def __init__(self, events: tuple[HistoryEvent, ...]) -> None:
        if not events or events[0].event_type != "WorkflowExecutionStarted":
            raise InvalidHistoryError("history must begin with WorkflowExecutionStarted")
        self.events = events
        self.scheduled: dict[int, HistoryEvent] = {}
        self.scheduled_entities: dict[UUID, HistoryEvent] = {}
        self.activity_terminal: dict[UUID, HistoryEvent] = {}
        self.timer_terminal: dict[UUID, HistoryEvent] = {}
        self.signals: list[HistoryEvent] = []
        self.cancellation_requested: HistoryEvent | None = None
        self.workflow_terminal: HistoryEvent | None = None
        previous_seq = 0
        for event in events:
            if event.seq != previous_seq + 1:
                raise InvalidHistoryError(
                    f"history sequence must be contiguous: expected {previous_seq + 1}, "
                    f"found {event.seq}"
                )
            previous_seq = event.seq
            if self.workflow_terminal is not None:
                raise InvalidHistoryError(
                    f"event {event.event_type} appears after terminal "
                    f"{self.workflow_terminal.event_type}"
                )
            if event.event_type == "WorkflowExecutionStarted" and event.seq != 1:
                raise InvalidHistoryError("WorkflowExecutionStarted appears more than once")
            if event.event_type in SCHEDULE_EVENT_TYPES:
                if event.command_id is None:
                    raise InvalidHistoryError(
                        f"{event.event_type} at sequence {event.seq} lacks command identity"
                    )
                if event.event_type != "MarkerRecorded" and event.entity_id is None:
                    raise InvalidHistoryError(
                        f"{event.event_type} at sequence {event.seq} lacks entity identity"
                    )
                if event.command_id in self.scheduled:
                    raise InvalidHistoryError(f"command {event.command_id} was scheduled twice")
                self.scheduled[event.command_id] = event
                if event.entity_id is not None:
                    if event.entity_id in self.scheduled_entities:
                        raise InvalidHistoryError(f"entity {event.entity_id} was scheduled twice")
                    self.scheduled_entities[event.entity_id] = event
            if event.event_type in ACTIVITY_TERMINAL_EVENT_TYPES:
                if event.entity_id is None:
                    raise InvalidHistoryError(
                        f"{event.event_type} at sequence {event.seq} lacks entity identity"
                    )
                if event.entity_id in self.activity_terminal:
                    raise InvalidHistoryError(
                        f"entity {event.entity_id} has multiple terminal events"
                    )
                scheduled = self.scheduled_entities.get(event.entity_id)
                if scheduled is None or scheduled.event_type != "ActivityScheduled":
                    raise InvalidHistoryError(
                        f"{event.event_type} for entity {event.entity_id} "
                        "has no prior activity schedule"
                    )
                if event.event_type == "ActivityCompleted" or event.attributes.get("final", True):
                    self.activity_terminal[event.entity_id] = event
            if event.event_type in TIMER_TERMINAL_EVENT_TYPES:
                if event.entity_id is None:
                    raise InvalidHistoryError(
                        f"{event.event_type} at sequence {event.seq} lacks entity identity"
                    )
                scheduled = self.scheduled_entities.get(event.entity_id)
                if scheduled is None or scheduled.event_type != "TimerStarted":
                    raise InvalidHistoryError(
                        f"{event.event_type} for entity {event.entity_id} has no prior timer start"
                    )
                if event.entity_id in self.timer_terminal:
                    raise InvalidHistoryError(
                        f"timer {event.entity_id} has multiple terminal events"
                    )
                self.timer_terminal[event.entity_id] = event
            if event.event_type == "SignalReceived":
                name = event.attributes.get("name")
                if not isinstance(name, str) or not name:
                    raise InvalidHistoryError(
                        f"SignalReceived at sequence {event.seq} has no signal name"
                    )
                if event.external_id is None:
                    raise InvalidHistoryError(
                        f"SignalReceived at sequence {event.seq} has no external identity"
                    )
                self.signals.append(event)
            if event.event_type == "WorkflowCancellationRequested":
                if self.cancellation_requested is not None:
                    raise InvalidHistoryError("workflow cancellation was requested more than once")
                self.cancellation_requested = event
            if event.event_type in WORKFLOW_TERMINAL_EVENT_TYPES:
                self.workflow_terminal = event

    def first_unvisited_command(self, next_command_id: int) -> HistoryEvent | None:
        remaining = [
            event for command_id, event in self.scheduled.items() if command_id >= next_command_id
        ]
        return min(remaining, key=lambda event: event.command_id or 0, default=None)
