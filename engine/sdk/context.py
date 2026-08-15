"""Deterministic operations available to workflow code."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid5

from engine.runtime.commands import Command, ScheduleActivity, ScheduleTimer
from engine.runtime.definitions import ActivityDefinition
from engine.runtime.history import HistoryIndex
from engine.runtime.serialization import JSONValue, clone_json, fingerprint
from engine.sdk.policies import RetryPolicy

ENTITY_NAMESPACE = UUID("590db7c2-1131-489a-a2b5-a94c2e5bc424")


class NonDeterminismError(RuntimeError):
    """Raised when workflow code diverges from its committed command history."""

    def __init__(self, command_id: int, detail: str) -> None:
        self.command_id = command_id
        self.detail = detail
        super().__init__(f"non-determinism at command {command_id}: {detail}")


class ActivityError(RuntimeError):
    def __init__(self, activity_type: str, failure: JSONValue) -> None:
        self.activity_type = activity_type
        self.failure = failure
        super().__init__(f"activity {activity_type!r} failed: {failure!r}")


class _ReplaySuspended(BaseException):
    pass


class _NewCommand(_ReplaySuspended):
    def __init__(self, command: Command) -> None:
        self.command = command


class _Blocked(_ReplaySuspended):
    pass


class WorkflowContext:
    def __init__(self, workflow_id: UUID, history: HistoryIndex) -> None:
        self._workflow_id = workflow_id
        self._history = history
        self._next_command_id = 0
        self._consumed_signal_seqs: set[int] = set()

    @property
    def next_command_id(self) -> int:
        return self._next_command_id

    async def activity(
        self,
        definition: ActivityDefinition,
        *args: JSONValue,
        retry: RetryPolicy | None = None,
        schedule_to_start: timedelta | None = None,
        start_to_close: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        **kwargs: JSONValue,
    ) -> JSONValue:
        command_id = self._next_command_id
        self._next_command_id += 1
        policy = retry or RetryPolicy()
        command_input: dict[str, JSONValue] = {
            "args": clone_json(list(args)),
            "kwargs": clone_json(kwargs),
        }
        schedule_to_start_seconds = (
            schedule_to_start.total_seconds() if schedule_to_start is not None else None
        )
        start_to_close_seconds = (
            start_to_close.total_seconds() if start_to_close is not None else None
        )
        heartbeat_timeout_seconds = (
            heartbeat_timeout.total_seconds() if heartbeat_timeout is not None else None
        )
        for option_name, seconds in (
            ("schedule_to_start", schedule_to_start_seconds),
            ("start_to_close", start_to_close_seconds),
            ("heartbeat_timeout", heartbeat_timeout_seconds),
        ):
            if seconds is not None and seconds <= 0:
                raise ValueError(f"{option_name} must be positive")
        identity: dict[str, JSONValue] = {
            "command_type": "activity",
            "activity_type": definition.name,
            "input": command_input,
            "retry_policy": policy.to_json(),
            "schedule_to_start_seconds": schedule_to_start_seconds,
            "start_to_close_seconds": start_to_close_seconds,
            "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
        }
        command_fingerprint = fingerprint(identity)
        scheduled = self._history.scheduled.get(command_id)
        if scheduled is None:
            entity_id = uuid5(ENTITY_NAMESPACE, f"{self._workflow_id}:activity:{command_id}")
            raise _NewCommand(
                ScheduleActivity(
                    command_id=command_id,
                    entity_id=entity_id,
                    activity_type=definition.name,
                    input=command_input,
                    retry_policy=policy.to_json(),
                    schedule_to_start_seconds=schedule_to_start_seconds,
                    start_to_close_seconds=start_to_close_seconds,
                    heartbeat_timeout_seconds=heartbeat_timeout_seconds,
                    fingerprint=command_fingerprint,
                )
            )
        if scheduled.event_type != "ActivityScheduled":
            raise NonDeterminismError(
                command_id,
                f"expected activity, history contains {scheduled.event_type}",
            )
        recorded_fingerprint = scheduled.attributes.get("fingerprint")
        if recorded_fingerprint != command_fingerprint:
            raise NonDeterminismError(
                command_id,
                f"activity command changed (recorded {recorded_fingerprint!r}, "
                f"replayed {command_fingerprint!r})",
            )
        assert scheduled.entity_id is not None
        terminal = self._history.activity_terminal.get(scheduled.entity_id)
        if terminal is None:
            raise _Blocked
        if terminal.event_type == "ActivityCompleted":
            return clone_json(terminal.attributes.get("result"))
        raise ActivityError(definition.name, clone_json(terminal.attributes.get("failure")))

    async def sleep(self, duration: timedelta) -> None:
        command_id = self._next_command_id
        self._next_command_id += 1
        delay_seconds = duration.total_seconds()
        if delay_seconds < 0:
            raise ValueError("sleep duration cannot be negative")
        command_fingerprint = fingerprint({"command_type": "timer", "delay_seconds": delay_seconds})
        scheduled = self._history.scheduled.get(command_id)
        if scheduled is None:
            entity_id = uuid5(ENTITY_NAMESPACE, f"{self._workflow_id}:timer:{command_id}")
            raise _NewCommand(
                ScheduleTimer(
                    command_id=command_id,
                    entity_id=entity_id,
                    delay_seconds=delay_seconds,
                    fingerprint=command_fingerprint,
                )
            )
        if scheduled.event_type != "TimerStarted":
            raise NonDeterminismError(
                command_id,
                f"expected timer, history contains {scheduled.event_type}",
            )
        recorded_fingerprint = scheduled.attributes.get("fingerprint")
        if recorded_fingerprint != command_fingerprint:
            raise NonDeterminismError(
                command_id,
                f"timer command changed (recorded {recorded_fingerprint!r}, "
                f"replayed {command_fingerprint!r})",
            )
        assert scheduled.entity_id is not None
        terminal = self._history.timer_terminal.get(scheduled.entity_id)
        if terminal is None:
            raise _Blocked
        if terminal.event_type == "TimerCanceled":
            raise RuntimeError("timer was canceled")

    async def wait_signal(self, name: str) -> JSONValue:
        """Consume the earliest unconsumed matching signal from durable history."""
        if not name:
            raise ValueError("signal name cannot be empty")
        for event in self._history.signals:
            if event.seq in self._consumed_signal_seqs:
                continue
            if event.attributes.get("name") == name:
                self._consumed_signal_seqs.add(event.seq)
                return clone_json(event.attributes.get("payload"))
        raise _Blocked
