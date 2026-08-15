"""Execution-local metadata exposed to activity implementations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import UUID

from engine.runtime.serialization import JSONValue

type HeartbeatCallback = Callable[[JSONValue, timedelta], Awaitable[datetime]]


class ActivityCancellationRequested(RuntimeError):
    """Raised by an activity heartbeat after workflow cancellation is requested."""

    def __init__(self, task_id: UUID, reason: str | None) -> None:
        self.task_id = task_id
        self.reason = reason
        message = f"cancellation requested for activity task {task_id}"
        if reason:
            message += f": {reason}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ActivityExecutionContext:
    idempotency_key: str
    task_id: UUID
    attempt: int
    _heartbeat: HeartbeatCallback = field(repr=False)

    async def heartbeat(
        self,
        details: JSONValue = None,
        *,
        lease_duration: timedelta = timedelta(seconds=30),
    ) -> datetime:
        """Record progress; raises when the lease is stale or cancellation was requested."""
        return await self._heartbeat(details, lease_duration)


_CURRENT_ACTIVITY: ContextVar[ActivityExecutionContext | None] = ContextVar(
    "durable_engine_current_activity",
    default=None,
)


def current_activity_context() -> ActivityExecutionContext:
    context = _CURRENT_ACTIVITY.get()
    if context is None:
        raise RuntimeError("no activity is executing in this context")
    return context


def set_activity_context(
    context: ActivityExecutionContext,
) -> Token[ActivityExecutionContext | None]:
    return _CURRENT_ACTIVITY.set(context)


def reset_activity_context(token: Token[ActivityExecutionContext | None]) -> None:
    _CURRENT_ACTIVITY.reset(token)
