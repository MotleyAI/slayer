"""Time points: instants, period literals and relative tokens, resolved on naive wall-clock ``datetime``s."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from types import MappingProxyType
from typing import Literal, Mapping, Optional

from pydantic import BaseModel

from slayer.core.enums import GRANULARITY_NAMES, UNIT_MONTHS, UNIT_SECONDS, TimeGranularity
from slayer.core.granularity import CustomGranularity, Granularity, granularity_key, granularity_parts, is_sub_day

# A datasource's custom granularities by ``granularity_key``.
Units = Mapping[str, CustomGranularity]
_NO_UNITS: Units = MappingProxyType({})

# The accepted forms, quoted by every time-literal error.
TIME_POINT_FORMS = (
    "an instant 'YYYY-MM-DD HH:MM[:SS[.ffffff]]' (no zone offset), a period literal "
    "'YYYY', 'YYYY-Qn', 'YYYY-MM', 'YYYY-Www' or 'YYYY-MM-DD', or a relative token: "
    "'today', 'yesterday', 'tomorrow', 'this|last|next <unit>', 'last N <units>', "
    "'next N <units>', 'N <units> ago', 'N <units> from now', "
    "'week|month|quarter|year to date' (unit: second, minute, hour, day, week, "
    "week_sunday, month, quarter, year, or a custom granularity of the datasource)"
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
    unit: Granularity
    offset: int = 0
    count: int = 1


_UNIT = r"([a-z_]\w*)"
_N = r"([1-9]\d*)"
_INSTANT_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?")
_YEAR_RE = re.compile(r"(\d{4})")
_QUARTER_RE = re.compile(r"(\d{4})-Q(\d)", re.IGNORECASE)
_MONTH_RE = re.compile(r"(\d{4})-(\d{2})")
_WEEK_RE = re.compile(r"(\d{4})-W(\d{2})", re.IGNORECASE)
_DAY_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_CALENDAR_RE = re.compile(rf"(this|last|next) {_UNIT}")
_LAST_NEXT_N_RE = re.compile(rf"(last|next) {_N} {_UNIT}")
_AGO_RE = re.compile(rf"{_N} {_UNIT} (ago|from now)")
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


def _unit(word: str, *, units: Units, plural: bool) -> Optional[Granularity]:
    """The unit ``word`` names (``plural`` also tries it minus an ``s``), else ``None``."""
    for candidate in (word, word[:-1]) if plural and word.endswith("s") else (word,):
        if candidate in GRANULARITY_NAMES:
            return TimeGranularity(candidate)
        if granularity_key(candidate) in units:
            return units[granularity_key(candidate)]
    return None


def _parse_relative(text: str, *, units: Units) -> _Relative | None:
    token = " ".join(text.lower().split())
    if token in _DAY_WORDS:
        return _Relative(kind="span", unit=TimeGranularity.DAY, offset=_DAY_WORDS[token])
    if m := _TO_DATE_RE.fullmatch(token):
        return _Relative(kind="to_date", unit=TimeGranularity(m.group(1)))
    if m := _CALENDAR_RE.fullmatch(token):
        step, word, n, plural = _STEP[m.group(1)], m.group(2), 1, False
        offset = step
    elif m := _LAST_NEXT_N_RE.fullmatch(token):
        n, word, plural = int(m.group(2)), m.group(3), True
        offset = -n if m.group(1) == "last" else 1
    elif m := _AGO_RE.fullmatch(token):
        n, word, plural = int(m.group(1)), m.group(2), True
        offset, n = (-n if m.group(3) == "ago" else n), 1
    else:
        return None
    unit = _unit(word, units=units, plural=plural)
    if unit is None:
        return None
    return _Relative(kind="span", unit=unit, offset=offset, count=n)


def _parse(text: str, *, units: Units = _NO_UNITS) -> TimePoint | _Relative | None:
    return _parse_literal(text.strip()) or _parse_relative(text, units=units)


def is_time_point(text: str, *, units: Units = _NO_UNITS) -> bool:
    """Whether ``text`` is a time point by syntax (independent of the clock); a relative
    token's unit must be built-in or one of ``units``."""
    return _parse(text, units=units) is not None


def is_time_point_shape(text: str) -> bool:
    """``is_time_point`` with any identifier as a relative token's unit (checked at binding)."""
    token = " ".join(text.lower().split())
    return _parse(text) is not None or any(r.fullmatch(token) for r in (_CALENDAR_RE, _LAST_NEXT_N_RE, _AGO_RE))


def is_relative_token(text: str, *, units: Units = _NO_UNITS) -> bool:
    """Whether ``text`` is a relative token (its meaning depends on the clock); its unit is built-in or one of ``units``."""
    return isinstance(_parse(text, units=units), _Relative)


def resolve_time_point(text: str, *, now: datetime, units: Units = _NO_UNITS) -> TimePoint | None:
    """``text`` as an ``Instant`` or a resolved ``Period`` against ``now``; ``None`` when not a
    time point (a relative token's unit must be built-in or in ``units``)."""
    parsed = _parse(text, units=units)
    if not isinstance(parsed, _Relative):
        return parsed
    if parsed.kind == "to_date":
        return Period(
            start=floor_to(now, parsed.unit),
            next_start=floor_to(now, TimeGranularity.DAY) + timedelta(days=1),
        )
    start = add_units(floor_to(now, parsed.unit), parsed.unit, parsed.offset)
    period = _span(start, parsed.unit, parsed.count)
    return period.model_copy(update={"sub_day": is_sub_day(parsed.unit)})


def parse_temporal_value(text: str) -> date | datetime | None:
    """``YYYY-MM-DD`` as a ``date``, an instant as a ``datetime``; ``None`` otherwise."""
    parsed = _parse_literal(text)
    if isinstance(parsed, Instant):
        return parsed.value
    if isinstance(parsed, Period) and _DAY_RE.fullmatch(text):
        return parsed.start.date()
    return None


def _span(start: datetime, unit: Granularity, count: int) -> Period:
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


def _add_months(value: datetime, months: int) -> datetime:
    total = value.year * 12 + value.month - 1 + months
    return value.replace(year=total // 12, month=total % 12 + 1)


def add_units(value: datetime, unit: Granularity, n: int) -> datetime:
    """``value`` plus ``n`` units; month-based units expect a day of month of at most 28."""
    if isinstance(unit, CustomGranularity):
        base, multiple, _ = granularity_parts(unit)
        return add_units(value, base, n * multiple)
    if unit in _FIXED_STEPS:
        return value + _FIXED_STEPS[unit] * n
    return _add_months(value, _MONTH_STEPS[unit] * n)


def _floor_custom(value: datetime, unit: CustomGranularity) -> datetime:
    base, multiple, origin = granularity_parts(unit)
    if base in UNIT_MONTHS:
        step = UNIT_MONTHS[base] * multiple
        months = (value.year * 12 + value.month) - (origin.year * 12 + origin.month)
        if _add_months(origin, months) > value:
            months -= 1
        return _add_months(origin, months // step * step)
    step_seconds = timedelta(seconds=UNIT_SECONDS[base] * multiple)
    return origin + ((value - origin) // step_seconds) * step_seconds


def floor_to(value: datetime, unit: Granularity) -> datetime:
    """The start of the ``unit`` bucket containing ``value``."""
    if isinstance(unit, CustomGranularity):
        return _floor_custom(value, unit)
    if unit is TimeGranularity.SECOND:
        return value.replace(microsecond=0)
    if unit is TimeGranularity.MINUTE:
        return value.replace(second=0, microsecond=0)
    if unit is TimeGranularity.HOUR:
        return value.replace(minute=0, second=0, microsecond=0)
    day = unit.period_start(value.date())
    return datetime(day.year, day.month, day.day)


def ceil_to(value: datetime, unit: Granularity) -> datetime:
    """``value`` when bucket-aligned, else the next ``unit`` bucket start."""
    floor = floor_to(value, unit)
    return floor if floor == value else add_units(floor, unit, 1)
