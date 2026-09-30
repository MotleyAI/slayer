"""Time points: instants, period literals and relative tokens, resolved on naive wall-clock ``datetime``s."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Literal

from pydantic import BaseModel

from slayer.core.enums import SUB_DAY_GRANULARITIES, TimeGranularity

# The accepted forms, quoted by every time-literal error.
TIME_POINT_FORMS = (
    "an instant 'YYYY-MM-DD HH:MM[:SS[.ffffff]]' (no zone offset), a period literal "
    "'YYYY', 'YYYY-Qn', 'YYYY-MM', 'YYYY-Www' or 'YYYY-MM-DD', or a relative token: "
    "'today', 'yesterday', 'tomorrow', 'this|last|next <unit>', 'last N <units>', "
    "'next N <units>', 'N <units> ago', 'N <units> from now', "
    "'week|month|quarter|year to date' (unit: second, minute, hour, day, week, "
    "week_sunday, month, quarter, year)"
)


class Instant(BaseModel, frozen=True):
    """A single moment."""

    value: datetime

    @property
    def sub_day(self) -> bool:
        return self.value != floor_to(self.value, TimeGranularity.DAY)


class Period(BaseModel, frozen=True):
    """The half-open interval ``[start, next_start)``; ``sub_day`` marks an hour/minute/second token."""

    start: datetime
    next_start: datetime
    sub_day: bool = False


TimePoint = Instant | Period


class _Relative(BaseModel, frozen=True):
    """A parsed relative token: ``offset`` whole units from the current one, spanning ``count`` units."""

    kind: Literal["span", "to_date"]
    unit: TimeGranularity
    offset: int = 0
    count: int = 1


_UNIT = r"(second|minute|hour|day|week_sunday|week|month|quarter|year)"
_N = r"([1-9]\d*)"
_INSTANT_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?")
_YEAR_RE = re.compile(r"(\d{4})")
_QUARTER_RE = re.compile(r"(\d{4})-Q(\d)", re.IGNORECASE)
_MONTH_RE = re.compile(r"(\d{4})-(\d{2})")
_WEEK_RE = re.compile(r"(\d{4})-W(\d{2})", re.IGNORECASE)
_DAY_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_CALENDAR_RE = re.compile(rf"(this|last|next) {_UNIT}")
_LAST_NEXT_N_RE = re.compile(rf"(last|next) {_N} {_UNIT}s?")
_AGO_RE = re.compile(rf"{_N} {_UNIT}s? (ago|from now)")
_TO_DATE_RE = re.compile(r"(week|month|quarter|year) to date")
_DAY_WORDS = {"today": 0, "yesterday": -1, "tomorrow": 1}
_STEP = {"this": 0, "last": -1, "next": 1}


def _day_period(day: date) -> Period:
    start = datetime(day.year, day.month, day.day)
    return Period(start=start, next_start=start + timedelta(days=1))


def _parse_literal(text: str) -> TimePoint | None:
    """A period literal or an instant; ``None`` when ``text`` is neither or names no real period."""
    try:
        if m := _INSTANT_RE.fullmatch(text):
            y, mo, d, h, mi, s, frac = m.groups()
            micro = int(frac.ljust(6, "0")) if frac else 0
            return Instant(value=datetime(int(y), int(mo), int(d), int(h), int(mi), int(s or 0), micro))
        if m := _DAY_RE.fullmatch(text):
            return _day_period(date(*map(int, m.groups())))
        if m := _WEEK_RE.fullmatch(text):
            start = date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
            return _span(datetime(start.year, start.month, start.day), TimeGranularity.WEEK, 1)
        if m := _MONTH_RE.fullmatch(text):
            return _span(datetime(int(m.group(1)), int(m.group(2)), 1), TimeGranularity.MONTH, 1)
        if m := _QUARTER_RE.fullmatch(text):
            quarter = int(m.group(2))
            if not 1 <= quarter <= 4:
                return None
            return _span(datetime(int(m.group(1)), 3 * quarter - 2, 1), TimeGranularity.QUARTER, 1)
        if m := _YEAR_RE.fullmatch(text):
            return _span(datetime(int(m.group(1)), 1, 1), TimeGranularity.YEAR, 1)
    except ValueError:
        return None
    return None


def _parse_relative(text: str) -> _Relative | None:
    token = " ".join(text.lower().split())
    if token in _DAY_WORDS:
        return _Relative(kind="span", unit=TimeGranularity.DAY, offset=_DAY_WORDS[token])
    if m := _CALENDAR_RE.fullmatch(token):
        return _Relative(kind="span", unit=TimeGranularity(m.group(2)), offset=_STEP[m.group(1)])
    if m := _LAST_NEXT_N_RE.fullmatch(token):
        n = int(m.group(2))
        offset = -n if m.group(1) == "last" else 1
        return _Relative(kind="span", unit=TimeGranularity(m.group(3)), offset=offset, count=n)
    if m := _AGO_RE.fullmatch(token):
        n = int(m.group(1))
        return _Relative(kind="span", unit=TimeGranularity(m.group(2)), offset=-n if m.group(3) == "ago" else n)
    if m := _TO_DATE_RE.fullmatch(token):
        return _Relative(kind="to_date", unit=TimeGranularity(m.group(1)))
    return None


def _parse(text: str) -> TimePoint | _Relative | None:
    return _parse_literal(text.strip()) or _parse_relative(text)


def is_time_point(text: str) -> bool:
    """Whether ``text`` is a time point by syntax (independent of the clock)."""
    return _parse(text) is not None


def is_relative_token(text: str) -> bool:
    """Whether ``text`` is a relative token (its meaning depends on the clock)."""
    return isinstance(_parse(text), _Relative)


def resolve_time_point(text: str, *, now: datetime) -> TimePoint | None:
    """``text`` as an ``Instant`` or a resolved ``Period`` against ``now``; ``None`` when not a time point."""
    parsed = _parse(text)
    if not isinstance(parsed, _Relative):
        return parsed
    if parsed.kind == "to_date":
        return Period(
            start=floor_to(now, parsed.unit),
            next_start=floor_to(now, TimeGranularity.DAY) + timedelta(days=1),
        )
    start = add_units(floor_to(now, parsed.unit), parsed.unit, parsed.offset)
    period = _span(start, parsed.unit, parsed.count)
    return period.model_copy(update={"sub_day": parsed.unit in SUB_DAY_GRANULARITIES})


def parse_temporal_value(text: str) -> date | datetime | None:
    """``YYYY-MM-DD`` as a ``date``, an instant as a ``datetime``; ``None`` otherwise."""
    parsed = _parse_literal(text)
    if isinstance(parsed, Instant):
        return parsed.value
    if isinstance(parsed, Period) and _DAY_RE.fullmatch(text):
        return parsed.start.date()
    return None


def _span(start: datetime, unit: TimeGranularity, count: int) -> Period:
    return Period(start=start, next_start=add_units(start, unit, count))


_FIXED_STEPS = {
    TimeGranularity.SECOND: timedelta(seconds=1),
    TimeGranularity.MINUTE: timedelta(minutes=1),
    TimeGranularity.HOUR: timedelta(hours=1),
    TimeGranularity.DAY: timedelta(days=1),
    TimeGranularity.WEEK: timedelta(weeks=1),
    TimeGranularity.WEEK_SUNDAY: timedelta(weeks=1),
}
_MONTH_STEPS = {TimeGranularity.MONTH: 1, TimeGranularity.QUARTER: 3, TimeGranularity.YEAR: 12}


def add_units(value: datetime, unit: TimeGranularity, n: int) -> datetime:
    """``value`` plus ``n`` units; month-based units expect a month-aligned ``value``."""
    if unit in _FIXED_STEPS:
        return value + _FIXED_STEPS[unit] * n
    months = value.year * 12 + value.month - 1 + _MONTH_STEPS[unit] * n
    return value.replace(year=months // 12, month=months % 12 + 1)


def floor_to(value: datetime, unit: TimeGranularity) -> datetime:
    """The start of the ``unit`` bucket containing ``value``."""
    if unit is TimeGranularity.SECOND:
        return value.replace(microsecond=0)
    if unit is TimeGranularity.MINUTE:
        return value.replace(second=0, microsecond=0)
    if unit is TimeGranularity.HOUR:
        return value.replace(minute=0, second=0, microsecond=0)
    day = unit.period_start(value.date())
    return datetime(day.year, day.month, day.day)


def ceil_to(value: datetime, unit: TimeGranularity) -> datetime:
    """``value`` when bucket-aligned, else the next ``unit`` bucket start."""
    floor = floor_to(value, unit)
    return floor if floor == value else add_units(floor, unit, 1)
