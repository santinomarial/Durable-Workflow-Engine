from datetime import UTC, datetime

import pytest

from engine.runtime.cron import CronExpressionError, next_cron_time, parse_cron


def test_cron_supports_steps_ranges_lists_and_sunday_alias() -> None:
    parsed = parse_cron("*/15 9-17 * 1,6 1-5")

    assert parsed.minute.values == frozenset({0, 15, 30, 45})
    assert 9 in parsed.hour.values and 17 in parsed.hour.values
    assert parsed.month.values == frozenset({1, 6})
    assert next_cron_time(
        "0 9 * * 1-5",
        after=datetime(2026, 8, 14, 20, 0, tzinfo=UTC),
        timezone_name="America/New_York",
    ) == datetime(2026, 8, 17, 13, 0, tzinfo=UTC)
    assert next_cron_time(
        "0 0 * * 7",
        after=datetime(2026, 8, 15, 0, 0, tzinfo=UTC),
    ) == datetime(2026, 8, 16, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize("expression", ["* * *", "60 * * * *", "*/0 * * * *", "x * * * *"])
def test_cron_rejects_invalid_expressions(expression: str) -> None:
    with pytest.raises(CronExpressionError):
        parse_cron(expression)


def test_cron_rejects_unknown_timezone() -> None:
    with pytest.raises(CronExpressionError, match="unknown IANA timezone"):
        next_cron_time(
            "* * * * *",
            after=datetime(2026, 1, 1, tzinfo=UTC),
            timezone_name="Mars/Olympus_Mons",
        )
