"""Pure parsing and resolution of time points: instants, period literals, relative tokens."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from slayer.core.enums import TimeGranularity
import slayer.core.keys as keys
from slayer.core.time_points import (
    Instant, Period, ceil_to, floor_to, is_time_point, parse_temporal_value, resolve_time_point,
)

from tests._time_points_fixtures import NOW, ROLLOVER_NOW


def _period(text: str, now: datetime = NOW) -> tuple[datetime, datetime]:
    point = resolve_time_point(text, now=now)
    assert isinstance(point, Period), f"{text!r} → {point!r}"
    return point.start, point.next_start


D = datetime


class TestPeriodLiterals:
    @pytest.mark.parametrize(("text", "start", "end"), [
        ("2025", D(2025, 1, 1), D(2026, 1, 1)),
        ("2025-Q1", D(2025, 1, 1), D(2025, 4, 1)),
        ("2025-Q4", D(2025, 10, 1), D(2026, 1, 1)),
        ("2025-03", D(2025, 3, 1), D(2025, 4, 1)),
        ("2025-12", D(2025, 12, 1), D(2026, 1, 1)),
        ("2025-W05", D(2025, 1, 27), D(2025, 2, 3)),
        ("2026-W01", D(2025, 12, 29), D(2026, 1, 5)),
        ("2020-W53", D(2020, 12, 28), D(2021, 1, 4)),
        ("2025-03-15", D(2025, 3, 15), D(2025, 3, 16)),
        ("2024-02-29", D(2024, 2, 29), D(2024, 3, 1)),
        ("2024-12-31", D(2024, 12, 31), D(2025, 1, 1)),
    ])
    def test_half_open_range(self, text, start, end) -> None:
        assert _period(text) == (start, end)

    def test_independent_of_now(self) -> None:
        assert _period("2025-Q1", NOW) == _period("2025-Q1", ROLLOVER_NOW)

    @pytest.mark.parametrize("text", [
        "2021-W53", "2025-W00", "2025-W5", "2025-02-29", "2025-13", "2025-00",
        "2025-Q0", "2025-Q5", "2025-04-31", "25-01", "2025/01/01", "20250101",
    ])
    def test_non_existent_or_malformed_is_not_a_time_point(self, text) -> None:
        assert resolve_time_point(text, now=NOW) is None
        assert not is_time_point(text)


class TestInstants:
    @pytest.mark.parametrize(("text", "value"), [
        ("2024-12-31 12:00:00", D(2024, 12, 31, 12)),
        ("2024-06-01T10:00:00", D(2024, 6, 1, 10)),
        ("2024-06-01 10:00", D(2024, 6, 1, 10)),
        ("2024-06-01 10:00:00.250", D(2024, 6, 1, 10, 0, 0, 250000)),
        ("2024-06-01 00:00:00", D(2024, 6, 1)),
    ])
    def test_instant(self, text, value) -> None:
        point = resolve_time_point(text, now=NOW)
        assert isinstance(point, Instant), f"{text!r} → {point!r}"
        assert point.value == value

    @pytest.mark.parametrize("text", [
        "2024-06-01 10:00:00Z", "2024-06-01T10:00:00+02:00", "2024-06-01 10:00:00-05:30",
    ])
    def test_zone_offset_is_rejected(self, text) -> None:
        assert resolve_time_point(text, now=NOW) is None
        assert not is_time_point(text)

    def test_values_are_naive(self) -> None:
        point = resolve_time_point("2024-06-01 10:00:00", now=NOW)
        assert isinstance(point, Instant) and point.value.tzinfo is None

    def test_midnight_instant_is_not_a_day_period(self) -> None:
        assert isinstance(resolve_time_point("2024-06-01 00:00:00", now=NOW), Instant)
        assert isinstance(resolve_time_point("2024-06-01", now=NOW), Period)

    @pytest.mark.parametrize("text", [
        "2024-06-01 25:00:00", "2024-06-01 10:61", "2024-06-01 10", "2024-06-01X10:00",
        "2024-06-01 10:00:00.1234567",
    ])
    def test_malformed_instant(self, text) -> None:
        assert resolve_time_point(text, now=NOW) is None


class TestCalendarTokens:
    @pytest.mark.parametrize(("text", "start", "end"), [
        ("today", D(2026, 9, 29), D(2026, 9, 30)),
        ("yesterday", D(2026, 9, 28), D(2026, 9, 29)),
        ("tomorrow", D(2026, 9, 30), D(2026, 10, 1)),
        ("this month", D(2026, 9, 1), D(2026, 10, 1)),
        ("last month", D(2026, 8, 1), D(2026, 9, 1)),
        ("next month", D(2026, 10, 1), D(2026, 11, 1)),
        ("this quarter", D(2026, 7, 1), D(2026, 10, 1)),
        ("last quarter", D(2026, 4, 1), D(2026, 7, 1)),
        ("next quarter", D(2026, 10, 1), D(2027, 1, 1)),
        ("this year", D(2026, 1, 1), D(2027, 1, 1)),
        ("last year", D(2025, 1, 1), D(2026, 1, 1)),
        ("next year", D(2027, 1, 1), D(2028, 1, 1)),
        ("this week", D(2026, 9, 28), D(2026, 10, 5)),
        ("last week", D(2026, 9, 21), D(2026, 9, 28)),
        ("this week_sunday", D(2026, 9, 27), D(2026, 10, 4)),
        ("last week_sunday", D(2026, 9, 20), D(2026, 9, 27)),
        ("this day", D(2026, 9, 29), D(2026, 9, 30)),
        ("this hour", D(2026, 9, 29, 12), D(2026, 9, 29, 13)),
        ("last hour", D(2026, 9, 29, 11), D(2026, 9, 29, 12)),
        ("last minute", D(2026, 9, 29, 11, 59), D(2026, 9, 29, 12)),
        ("this second", D(2026, 9, 29, 12), D(2026, 9, 29, 12, 0, 1)),
    ])
    def test_calendar(self, text, start, end) -> None:
        assert _period(text) == (start, end)


class TestLastNextN:
    @pytest.mark.parametrize(("text", "start", "end"), [
        ("last 7 days", D(2026, 9, 22), D(2026, 9, 29)),
        ("last 1 day", D(2026, 9, 28), D(2026, 9, 29)),
        ("last 3 months", D(2026, 6, 1), D(2026, 9, 1)),
        ("last 2 quarters", D(2026, 1, 1), D(2026, 7, 1)),
        ("last 2 weeks", D(2026, 9, 14), D(2026, 9, 28)),
        ("last 2 years", D(2024, 1, 1), D(2026, 1, 1)),
        ("last 6 hours", D(2026, 9, 29, 6), D(2026, 9, 29, 12)),
        ("last 30 minutes", D(2026, 9, 29, 11, 30), D(2026, 9, 29, 12)),
        ("next 2 days", D(2026, 9, 30), D(2026, 10, 2)),
        ("next 3 months", D(2026, 10, 1), D(2027, 1, 1)),
        ("last 3 month", D(2026, 6, 1), D(2026, 9, 1)),
    ])
    def test_excludes_current_unit(self, text, start, end) -> None:
        assert _period(text) == (start, end)


class TestOffsetsAndToDate:
    @pytest.mark.parametrize(("text", "start", "end"), [
        ("3 months ago", D(2026, 6, 1), D(2026, 7, 1)),
        ("12 months ago", D(2025, 9, 1), D(2025, 10, 1)),
        ("1 week ago", D(2026, 9, 21), D(2026, 9, 28)),
        ("1 year ago", D(2025, 1, 1), D(2026, 1, 1)),
        ("2 hours ago", D(2026, 9, 29, 10), D(2026, 9, 29, 11)),
        ("2 days from now", D(2026, 10, 1), D(2026, 10, 2)),
        ("1 quarter from now", D(2026, 10, 1), D(2027, 1, 1)),
        ("year to date", D(2026, 1, 1), D(2026, 9, 30)),
        ("quarter to date", D(2026, 7, 1), D(2026, 9, 30)),
        ("month to date", D(2026, 9, 1), D(2026, 9, 30)),
        ("week to date", D(2026, 9, 28), D(2026, 9, 30)),
    ])
    def test_resolution(self, text, start, end) -> None:
        assert _period(text) == (start, end)


class TestRolloverAndSubSecond:
    @pytest.mark.parametrize(("text", "start", "end"), [
        ("last month", D(2026, 12, 1), D(2027, 1, 1)),
        ("last quarter", D(2026, 10, 1), D(2027, 1, 1)),
        ("yesterday", D(2026, 12, 31), D(2027, 1, 1)),
        ("last 1 second", D(2026, 12, 31, 23, 59, 59), D(2027, 1, 1)),
        ("this second", D(2027, 1, 1), D(2027, 1, 1, 0, 0, 1)),
        ("last year", D(2026, 1, 1), D(2027, 1, 1)),
        ("year to date", D(2027, 1, 1), D(2027, 1, 2)),
    ])
    def test_rollover(self, text, start, end) -> None:
        assert _period(text, ROLLOVER_NOW) == (start, end)

    def test_leap_day_month_arithmetic(self) -> None:
        assert _period("1 month ago", D(2024, 3, 31, 9)) == (D(2024, 2, 1), D(2024, 3, 1))
        assert _period("last 12 months", D(2024, 2, 29, 9)) == (D(2023, 2, 1), D(2024, 2, 1))


class TestNormalisation:
    @pytest.mark.parametrize("text", ["Last   Month", "LAST MONTH", "  last month  ", "last\tmonth"])
    def test_case_and_whitespace(self, text) -> None:
        assert _period(text) == _period("last month")

    def test_plural_is_optional(self) -> None:
        assert _period("last 1 days") == _period("last 1 day")
        assert _period("2 month ago") == _period("2 months ago")


class TestInvalidTokens:
    @pytest.mark.parametrize("text", [
        "last fortnight", "this decade", "last", "next", "last 0 days", "last -1 days",
        "last 1.5 days", "0 days ago", "hour to date", "day to date", "last month ago",
        "3 months", "in 3 months", "", "   ", "now", "last 7 dayz",
    ])
    def test_rejected(self, text) -> None:
        assert resolve_time_point(text, now=NOW) is None
        assert not is_time_point(text)

    @pytest.mark.parametrize("text", ["last month", "2025-Q1", "2025-03-01 10:00:00", "Year To Date"])
    def test_syntax_check_accepts(self, text) -> None:
        assert is_time_point(text)


class TestFloorCeil:
    @pytest.mark.parametrize(("gran", "floor"), [
        (TimeGranularity.SECOND, D(2026, 9, 29, 12, 34, 56)),
        (TimeGranularity.MINUTE, D(2026, 9, 29, 12, 34)),
        (TimeGranularity.HOUR, D(2026, 9, 29, 12)),
        (TimeGranularity.DAY, D(2026, 9, 29)),
        (TimeGranularity.WEEK, D(2026, 9, 28)),
        (TimeGranularity.WEEK_SUNDAY, D(2026, 9, 27)),
        (TimeGranularity.MONTH, D(2026, 9, 1)),
        (TimeGranularity.QUARTER, D(2026, 7, 1)),
        (TimeGranularity.YEAR, D(2026, 1, 1)),
    ])
    def test_floor(self, gran, floor) -> None:
        assert floor_to(D(2026, 9, 29, 12, 34, 56, 789000), gran) == floor

    @pytest.mark.parametrize(("value", "gran", "ceil"), [
        (D(2024, 3, 15), TimeGranularity.MONTH, D(2024, 4, 1)),
        (D(2024, 3, 1), TimeGranularity.MONTH, D(2024, 3, 1)),
        (D(2024, 3, 15, 0, 0, 1), TimeGranularity.DAY, D(2024, 3, 16)),
        (D(2024, 3, 16), TimeGranularity.DAY, D(2024, 3, 16)),
        (D(2024, 12, 31, 23), TimeGranularity.YEAR, D(2025, 1, 1)),
        (D(2026, 9, 29), TimeGranularity.WEEK, D(2026, 10, 5)),
        (D(2026, 9, 28), TimeGranularity.WEEK, D(2026, 9, 28)),
    ])
    def test_ceil(self, value, gran, ceil) -> None:
        assert ceil_to(value, gran) == ceil


class TestParseTemporalValue:
    @pytest.mark.parametrize(("text", "value"), [
        ("2024-01-31", date(2024, 1, 31)),
        ("2024-01-31 10:15", D(2024, 1, 31, 10, 15)),
        ("2024-01-31T10:15:00", D(2024, 1, 31, 10, 15)),
        ("2024-01-31 10:15:00.5", D(2024, 1, 31, 10, 15, 0, 500000)),
    ])
    def test_day_is_date_instant_is_datetime(self, text, value) -> None:
        got = parse_temporal_value(text)
        assert got == value and type(got) is type(value)

    @pytest.mark.parametrize("text", ["2024-02-30", "2025-Q1", "last month", "2024-01-31 10:15:00Z", "x"])
    def test_not_a_date_value(self, text) -> None:
        assert parse_temporal_value(text) is None

    def test_is_the_one_iso_parser(self) -> None:
        assert not hasattr(keys, "parse_iso_temporal")
