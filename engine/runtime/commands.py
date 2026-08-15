"""Deterministic commands emitted by workflow replay."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from engine.runtime.serialization import JSONValue


@dataclass(frozen=True, slots=True)
class ScheduleActivity:
    command_id: int
    entity_id: UUID
    activity_type: str
    input: dict[str, JSONValue]
    retry_policy: dict[str, JSONValue]
    start_to_close_seconds: float | None
    fingerprint: str
