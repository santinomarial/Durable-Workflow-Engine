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
    schedule_to_start_seconds: float | None
    start_to_close_seconds: float | None
    heartbeat_timeout_seconds: float | None
    fingerprint: str


@dataclass(frozen=True, slots=True)
class ScheduleTimer:
    command_id: int
    entity_id: UUID
    delay_seconds: float
    purpose: str
    signal_name: str | None
    fingerprint: str


@dataclass(frozen=True, slots=True)
class RecordMarker:
    command_id: int
    marker_type: str
    value: JSONValue
    fingerprint: str


@dataclass(frozen=True, slots=True)
class StartChildWorkflow:
    command_id: int
    child_workflow_id: UUID
    workflow_type: str
    definition_version: int
    input: JSONValue
    queue_name: str | None
    parent_close_policy: str
    fingerprint: str


type Command = RecordMarker | ScheduleActivity | ScheduleTimer | StartChildWorkflow
