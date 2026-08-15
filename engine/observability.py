"""Dependency-free structured logging and Prometheus metrics."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
from collections import defaultdict
from datetime import UTC, datetime
from typing import ClassVar

LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)
METRIC_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:")


class JSONFormatter(logging.Formatter):
    """Render standard and contextual LogRecord fields as one JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in LOG_RECORD_FIELDS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True)


def configure_logging() -> None:
    """Configure root logging once from environment-safe settings."""
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise RuntimeError(f"invalid LOG_LEVEL: {level_name}")
    log_format = os.environ.get("DWE_LOG_FORMAT", "json").lower()
    if log_format not in {"json", "text"}:
        raise RuntimeError("DWE_LOG_FORMAT must be 'json' or 'text'")
    handler = logging.StreamHandler(sys.stdout)
    if log_format == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def _validate_metric_name(value: str) -> None:
    if not value or any(character not in METRIC_NAME_CHARS for character in value):
        raise ValueError(f"invalid metric name: {value!r}")


def _labels(values: tuple[tuple[str, str], ...]) -> str:
    if not values:
        return ""
    rendered = ",".join(
        f'{key}="{value.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
        for key, value in values
    )
    return "{" + rendered + "}"


class MetricsRegistry:
    """Small thread-safe registry for bounded-label counters and histograms."""

    HISTOGRAM_BUCKETS: ClassVar[tuple[float, ...]] = (
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1,
        2.5,
        5,
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._histograms: dict[
            tuple[str, tuple[tuple[str, str], ...]], tuple[int, float, list[int]]
        ] = {}

    def increment(
        self, name: str, *, labels: dict[str, str] | None = None, value: float = 1
    ) -> None:
        _validate_metric_name(name)
        if value < 0:
            raise ValueError("counter increments cannot be negative")
        key = (name, tuple(sorted((labels or {}).items())))
        with self._lock:
            self._counters[key] += value

    def observe(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None:
        _validate_metric_name(name)
        key = (name, tuple(sorted((labels or {}).items())))
        with self._lock:
            count, total, buckets = self._histograms.get(
                key, (0, 0.0, [0] * len(self.HISTOGRAM_BUCKETS))
            )
            for index, boundary in enumerate(self.HISTOGRAM_BUCKETS):
                if value <= boundary:
                    buckets[index] += 1
            self._histograms[key] = (count + 1, total + value, buckets)

    def render(self, *, gauges: dict[str, float] | None = None) -> str:
        lines: list[str] = []
        declared: set[str] = set()
        with self._lock:
            counters = tuple(sorted(self._counters.items()))
            histograms = tuple(sorted(self._histograms.items()))
        for (name, labels), value in counters:
            if name not in declared:
                lines.append(f"# TYPE {name} counter")
                declared.add(name)
            lines.append(f"{name}{_labels(labels)} {value:g}")
        for (name, labels), (count, total, buckets) in histograms:
            if name not in declared:
                lines.append(f"# TYPE {name} histogram")
                declared.add(name)
            for boundary, bucket_count in zip(self.HISTOGRAM_BUCKETS, buckets, strict=True):
                bucket_labels = tuple(sorted((*labels, ("le", f"{boundary:g}"))))
                lines.append(f"{name}_bucket{_labels(bucket_labels)} {bucket_count}")
            infinity_labels = tuple(sorted((*labels, ("le", "+Inf"))))
            lines.append(f"{name}_bucket{_labels(infinity_labels)} {count}")
            lines.append(f"{name}_sum{_labels(labels)} {total:g}")
            lines.append(f"{name}_count{_labels(labels)} {count}")
        for name, value in sorted((gauges or {}).items()):
            _validate_metric_name(name)
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value:g}")
        return "\n".join(lines) + "\n"


METRICS = MetricsRegistry()
