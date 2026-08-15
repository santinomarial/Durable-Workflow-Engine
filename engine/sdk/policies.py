"""Activity execution policies recorded in workflow history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from engine.runtime.serialization import JSONValue


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    initial_interval: timedelta = timedelta(seconds=1)
    backoff_coefficient: float = 2.0
    maximum_interval: timedelta | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_interval.total_seconds() < 0:
            raise ValueError("initial_interval cannot be negative")
        if self.backoff_coefficient < 1:
            raise ValueError("backoff_coefficient must be at least 1")
        if self.maximum_interval is not None and self.maximum_interval.total_seconds() < 0:
            raise ValueError("maximum_interval cannot be negative")

    def to_json(self) -> dict[str, JSONValue]:
        return {
            "max_attempts": self.max_attempts,
            "initial_interval_seconds": self.initial_interval.total_seconds(),
            "backoff_coefficient": self.backoff_coefficient,
            "maximum_interval_seconds": (
                self.maximum_interval.total_seconds() if self.maximum_interval is not None else None
            ),
        }
