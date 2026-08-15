"""Dependency-free standard five-field cron parsing and timezone evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class CronExpressionError(ValueError):
    """Raised when a schedule expression or timezone is unsupported."""


@dataclass(frozen=True, slots=True)
class CronField:
    values: frozenset[int]
    wildcard: bool


@dataclass(frozen=True, slots=True)
class CronExpression:
    minute: CronField
    hour: CronField
    day_of_month: CronField
    month: CronField
    day_of_week: CronField


def _field(raw: str, minimum: int, maximum: int, *, sunday: bool = False) -> CronField:
    values: set[int] = set()
    wildcard = raw == "*"
    for item in raw.split(","):
        base, separator, step_raw = item.partition("/")
        try:
            step = int(step_raw) if separator else 1
        except ValueError as error:
            raise CronExpressionError(f"invalid cron step: {item!r}") from error
        if step < 1:
            raise CronExpressionError("cron steps must be positive")
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_raw, end_raw = base.split("-", 1)
            try:
                start, end = int(start_raw), int(end_raw)
            except ValueError as error:
                raise CronExpressionError(f"invalid cron range: {item!r}") from error
        else:
            try:
                start = end = int(base)
            except ValueError as error:
                raise CronExpressionError(f"invalid cron value: {item!r}") from error
        if start < minimum or end > maximum or start > end:
            raise CronExpressionError(f"cron value {item!r} is outside {minimum}..{maximum}")
        values.update(range(start, end + 1, step))
    if sunday and 7 in values:
        values.remove(7)
        values.add(0)
    return CronField(frozenset(values), wildcard)


def parse_cron(expression: str) -> CronExpression:
    fields = expression.split()
    if len(fields) != 5:
        raise CronExpressionError("cron expressions must contain five fields")
    return CronExpression(
        minute=_field(fields[0], 0, 59),
        hour=_field(fields[1], 0, 23),
        day_of_month=_field(fields[2], 1, 31),
        month=_field(fields[3], 1, 12),
        day_of_week=_field(fields[4], 0, 7, sunday=True),
    )


def _matches(parsed: CronExpression, local: datetime) -> bool:
    if local.minute not in parsed.minute.values or local.hour not in parsed.hour.values:
        return False
    if local.month not in parsed.month.values:
        return False
    day_of_month = local.day in parsed.day_of_month.values
    cron_weekday = (local.weekday() + 1) % 7
    day_of_week = cron_weekday in parsed.day_of_week.values
    if parsed.day_of_month.wildcard:
        day_matches = day_of_week
    elif parsed.day_of_week.wildcard:
        day_matches = day_of_month
    else:
        day_matches = day_of_month or day_of_week
    return day_matches


def next_cron_time(
    expression: str,
    *,
    after: datetime,
    timezone_name: str = "UTC",
) -> datetime:
    """Return the first matching minute strictly after an aware timestamp."""
    if after.tzinfo is None:
        raise CronExpressionError("cron evaluation requires a timezone-aware timestamp")
    parsed = parse_cron(expression)
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise CronExpressionError(f"unknown IANA timezone: {timezone_name!r}") from error
    candidate = after.astimezone(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 366 * 5):
        if _matches(parsed, candidate.astimezone(timezone)):
            return candidate
        candidate += timedelta(minutes=1)
    raise CronExpressionError("cron expression produced no occurrence within five years")
