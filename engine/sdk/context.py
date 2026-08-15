"""Deterministic operations available to workflow code."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
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


class SignalTimeoutError(TimeoutError):
    """Raised when a durable signal wait's timer wins the recorded race."""

    def __init__(self, signal_name: str) -> None:
        self.signal_name = signal_name
        super().__init__(f"timed out waiting for signal {signal_name!r}")


class _ReplaySuspended(BaseException):
    pass


class _NewCommands(_ReplaySuspended):
    def __init__(self, commands: tuple[Command, ...]) -> None:
        self.commands = commands


class _Blocked(_ReplaySuspended):
    pass


@dataclass(slots=True)
class ActivityCall:
    context: WorkflowContext
    definition: ActivityDefinition
    args: tuple[JSONValue, ...]
    kwargs: dict[str, JSONValue]
    retry: RetryPolicy | None
    schedule_to_start: timedelta | None
    start_to_close: timedelta | None
    heartbeat_timeout: timedelta | None
    command_id: int | None = None

    def __await__(self) -> Generator[object, None, JSONValue]:
        async def resolve() -> JSONValue:
            return self.context._evaluate_activity(self)

        return resolve().__await__()


class WorkflowContext:
    def __init__(self, workflow_id: UUID, history: HistoryIndex) -> None:
        self._workflow_id = workflow_id
        self._history = history
        self._next_command_id = 0
        self._consumed_signal_seqs: set[int] = set()

    @property
    def next_command_id(self) -> int:
        return self._next_command_id

    def activity(
        self,
        definition: ActivityDefinition,
        *args: JSONValue,
        retry: RetryPolicy | None = None,
        schedule_to_start: timedelta | None = None,
        start_to_close: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        **kwargs: JSONValue,
    ) -> ActivityCall:
        return ActivityCall(
            context=self,
            definition=definition,
            args=args,
            kwargs=kwargs,
            retry=retry,
            schedule_to_start=schedule_to_start,
            start_to_close=start_to_close,
            heartbeat_timeout=heartbeat_timeout,
        )

    def _evaluate_activity(self, call: ActivityCall) -> JSONValue:
        if call.context is not self:
            raise ValueError("activity call belongs to a different workflow context")
        if call.command_id is None:
            call.command_id = self._next_command_id
            self._next_command_id += 1
        command_id = call.command_id
        policy = call.retry or RetryPolicy()
        command_input: dict[str, JSONValue] = {
            "args": clone_json(list(call.args)),
            "kwargs": clone_json(call.kwargs),
        }
        schedule_to_start_seconds = (
            call.schedule_to_start.total_seconds() if call.schedule_to_start is not None else None
        )
        start_to_close_seconds = (
            call.start_to_close.total_seconds() if call.start_to_close is not None else None
        )
        heartbeat_timeout_seconds = (
            call.heartbeat_timeout.total_seconds() if call.heartbeat_timeout is not None else None
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
            "activity_type": call.definition.name,
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
            raise _NewCommands(
                (
                    ScheduleActivity(
                        command_id=command_id,
                        entity_id=entity_id,
                        activity_type=call.definition.name,
                        input=command_input,
                        retry_policy=policy.to_json(),
                        schedule_to_start_seconds=schedule_to_start_seconds,
                        start_to_close_seconds=start_to_close_seconds,
                        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
                        fingerprint=command_fingerprint,
                    ),
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
        raise ActivityError(call.definition.name, clone_json(terminal.attributes.get("failure")))

    async def gather(self, *calls: ActivityCall) -> list[JSONValue]:
        """Schedule child activities together and return results in source order."""
        if not calls:
            return []
        results: list[JSONValue] = []
        missing: list[Command] = []
        blocked = False
        for call in calls:
            try:
                results.append(self._evaluate_activity(call))
            except _NewCommands as suspended:
                missing.extend(suspended.commands)
                results.append(None)
            except _Blocked:
                blocked = True
                results.append(None)
        if missing:
            raise _NewCommands(tuple(missing))
        if blocked:
            raise _Blocked
        return results

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
            raise _NewCommands(
                (
                    ScheduleTimer(
                        command_id=command_id,
                        entity_id=entity_id,
                        delay_seconds=delay_seconds,
                        purpose="sleep",
                        signal_name=None,
                        fingerprint=command_fingerprint,
                    ),
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

    async def wait_signal(self, name: str, *, timeout: timedelta | None = None) -> JSONValue:
        """Consume the earliest unconsumed matching signal from durable history."""
        if not name:
            raise ValueError("signal name cannot be empty")
        matching_signal = None
        for event in self._history.signals:
            if event.seq in self._consumed_signal_seqs:
                continue
            if event.attributes.get("name") == name:
                matching_signal = event
                break
        if timeout is None:
            if matching_signal is not None:
                self._consumed_signal_seqs.add(matching_signal.seq)
                return clone_json(matching_signal.attributes.get("payload"))
            raise _Blocked

        command_id = self._next_command_id
        self._next_command_id += 1
        delay_seconds = timeout.total_seconds()
        if delay_seconds < 0:
            raise ValueError("signal timeout cannot be negative")
        command_fingerprint = fingerprint(
            {
                "command_type": "signal_timeout",
                "signal_name": name,
                "delay_seconds": delay_seconds,
            }
        )
        scheduled = self._history.scheduled.get(command_id)
        if scheduled is None:
            entity_id = uuid5(
                ENTITY_NAMESPACE,
                f"{self._workflow_id}:signal-timeout:{command_id}",
            )
            raise _NewCommands(
                (
                    ScheduleTimer(
                        command_id=command_id,
                        entity_id=entity_id,
                        delay_seconds=delay_seconds,
                        purpose="signal_timeout",
                        signal_name=name,
                        fingerprint=command_fingerprint,
                    ),
                )
            )
        if scheduled.event_type != "TimerStarted":
            raise NonDeterminismError(
                command_id,
                f"expected signal timeout, history contains {scheduled.event_type}",
            )
        if scheduled.attributes.get("fingerprint") != command_fingerprint:
            raise NonDeterminismError(command_id, f"signal timeout for {name!r} changed")
        assert scheduled.entity_id is not None
        timer_terminal = self._history.timer_terminal.get(scheduled.entity_id)
        if matching_signal is not None and (
            timer_terminal is None or matching_signal.seq < timer_terminal.seq
        ):
            self._consumed_signal_seqs.add(matching_signal.seq)
            return clone_json(matching_signal.attributes.get("payload"))
        if timer_terminal is not None and timer_terminal.event_type == "TimerFired":
            raise SignalTimeoutError(name)
        raise _Blocked
