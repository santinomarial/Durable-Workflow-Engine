"""Execution-local metadata exposed to activity implementations."""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class ActivityExecutionContext:
    idempotency_key: str
    task_id: UUID
    attempt: int


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
