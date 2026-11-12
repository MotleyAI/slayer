"""Buckets keep their logical type through the spine, stages, time_shift, windows and consecutive periods."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery

from tests._dev2018_fixtures import (
    IN_PROCESS,
    PX,
    as_temporal,
    bucket_type,
    lt_engine,
    oracle_bucket,
    physical_type_error,
)
from tests._time_points_fixtures import bucket_text

D = datetime


@pytest.fixture(params=IN_PROCESS)
async def engine(request):
    async with lt_engine(request.param) as eng:
        yield request.param, eng


def _num(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def _value(row: dict[str, Any], name: str) -> Any:
    [key] = [k for k in row if k == name or k.endswith(f".{name}")]
    return row[key]


# ---------------------------------------------------------------------------
# Time spine over a DATE axis
# ---------------------------------------------------------------------------

async def _spine(eng, *, model: str, granularity: str, date_range: list[str]) -> dict[str, float]:
    resp = await eng.execute(SlayerQuery.model_validate({
        "measures": [{"formula": f"count({model}.id)", "name": "n"}],
        "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": granularity,
                             "date_range": date_range}],
    }))
    return {bucket_text(r["time_spine.timestamp"]): float(_value(r, "n") or 0) for r in resp.data}


class TestSpineOverDateAxis:
    @pytest.mark.parametrize("model", ["px", "px_ts"])
    async def test_month(self, engine, model) -> None:
        _backend, eng = engine
        got = await _spine(eng, model=model, granularity="month", date_range=["2024-05-01", "2024-08-01"])
        assert got == {
            "2024-05-01 00:00:00": 3.0, "2024-06-01 00:00:00": 0.0,
            "2024-07-01 00:00:00": 1.0, "2024-08-01 00:00:00": 0.0,
        }

    @pytest.mark.parametrize("model", ["px", "px_ts"])
    async def test_hour(self, engine, model) -> None:
        _backend, eng = engine
        got = await _spine(eng, model=model, granularity="hour", date_range=["2024-05-15", "2024-05-16"])
        assert len(got) == 48
        assert {k: v for k, v in got.items() if v} == {"2024-05-15 00:00:00": 1.0, "2024-05-16 00:00:00": 1.0}

    @pytest.mark.parametrize("model", ["px", "px_ts"])
    async def test_sub_day_bound_restricts_the_axis_as_midnight(self, engine, model) -> None:
        _backend, eng = engine
        got = await _spine(eng, model=model, granularity="hour",
                           date_range=["2024-05-15 06:00:00", "2024-05-17 12:00:00"])
        assert {k: v for k, v in got.items() if v} == {"2024-05-16 00:00:00": 1.0, "2024-05-17 00:00:00": 1.0}


# ---------------------------------------------------------------------------
# Two stages: an inner stage projects buckets, the outer stage re-buckets, compares and shifts them.
# ---------------------------------------------------------------------------

INNER = {
    "name": "s1", "source_model": "bk",
    "dimensions": [
        {"expression": "date_trunc('month', d)", "name": "m_d"},
        {"expression": "date_trunc('hour', d)", "name": "h_d"},
        {"expression": "date_trunc('month', ts)", "name": "m_ts"},
        {"expression": "date_trunc('fiscal_year', d)", "name": "fy_d"},
    ],
    "measures": [{"formula": "count(*)", "name": "n"}],
}
OUTER = {
    "source_model": "s1",
    "dimensions": [
        "h_d",
        {"expression": "date_trunc('quarter', m_d)", "name": "q"},
        {"expression": "date_trunc('day', h_d)", "name": "dh"},
        {"expression": "date_add(h_d, 1, 'hour')", "name": "h1"},
        {"expression": "date_add(m_d, 1, 'month')", "name": "nm"},
        {"expression": "date_diff('day', m_d, h_d)", "name": "dd"},
        {"expression": "date_diff('month', fy_d, m_ts)", "name": "fm"},
    ],
    "filters": ["m_d <= m_ts"],
    "order": [{"column": "h_d", "direction": "desc"}],
}
OUTER_ROWS = [
    # h_d, q, dh, h1, nm, dd, fm
    (D(2025, 3, 31), date(2025, 1, 1), D(2025, 3, 31), D(2025, 3, 31, 1), date(2025, 4, 1), 30, 12),
    (D(2024, 12, 30), date(2024, 10, 1), D(2024, 12, 30), D(2024, 12, 30, 1), date(2025, 1, 1), 29, 8),
    (D(2024, 12, 29), date(2024, 10, 1), D(2024, 12, 29), D(2024, 12, 29, 1), date(2025, 1, 1), 28, 8),
    (D(2024, 6, 15), date(2024, 4, 1), D(2024, 6, 15), D(2024, 6, 15, 1), date(2024, 7, 1), 14, 2),
]
OUTER_TYPES = [DataType.TIMESTAMP, DataType.DATE, DataType.TIMESTAMP, DataType.TIMESTAMP, DataType.DATE]


class TestTwoStages:
    async def test_stage_columns_record_the_bucket_types(self, engine) -> None:
        _backend, eng = engine
        model = await eng.create_model_from_query(query=INNER, name="staged", save=False)
        types = {c.name: c.type for c in model.columns}
        assert {k: types[k] for k in ("m_d", "h_d", "m_ts", "fy_d")} == {
            "m_d": DataType.DATE, "h_d": DataType.TIMESTAMP, "m_ts": DataType.TIMESTAMP, "fy_d": DataType.DATE,
        }

    async def test_outer_stage_values_follow_the_types(self, engine) -> None:
        backend, eng = engine
        resp = await eng.execute([SlayerQuery.model_validate(INNER), SlayerQuery.model_validate(OUTER)])
        names = ["h_d", "q", "dh", "h1", "nm", "dd", "fm"]
        got = [tuple(_value(r, n) for n in names) for r in resp.data]
        assert [tuple(as_temporal(v) for v in row[:5]) + tuple(int(v) for v in row[5:]) for row in got] == OUTER_ROWS
        problems = [
            (n, v, why) for row in got for n, v, t in zip(names, row, OUTER_TYPES)
            if (why := physical_type_error(v, t, backend=backend)) is not None
        ]
        assert not problems, problems


# ---------------------------------------------------------------------------
# DATE and TIMESTAMP parity: ``px.ts`` is midnight of ``px.d``, so every series agrees; only the type differs.
# ---------------------------------------------------------------------------

def _m(formula: str, name: str) -> dict[str, str]:
    return {"formula": formula, "name": name}


async def _series(eng, *, column: str, granularity: str, measures: list[dict[str, str]],
                  date_range: Any = None, whole_periods_only: bool = False) -> tuple[dict[str, tuple], list[Any]]:
    model = "px" if column == "d" else "px_ts"
    td: dict[str, Any] = {"dimension": column, "granularity": granularity}
    if date_range is not None:
        td["date_range"] = date_range
    resp = await eng.execute(SlayerQuery.model_validate({
        "source_model": model, "measures": measures, "time_dimensions": [td],
        "whole_periods_only": whole_periods_only,
    }))
    key = f"{model}.{column}"
    series = {bucket_text(r[key]): tuple(_num(_value(r, m["name"])) for m in measures) for r in resp.data}
    return series, [r[key] for r in resp.data]


PARITY_CASES: list[tuple[str, str, list[dict[str, str]], Any, bool]] = [
    ("shift_month", "month", [_m("time_shift(sum(amount), -1)", "p"), _m("time_shift(sum(amount), 1)", "n")],
     ["2024-01-01", "2024-08-31"], False),
    ("shift_day", "day", [_m("time_shift(sum(amount), -1)", "p"), _m("time_shift(sum(amount), 1)", "n")],
     ["2024-05-14", "2024-05-18"], False),
    ("shift_month_end", "day", [_m("time_shift(sum(amount), -1, 'month')", "p"),
                                _m("time_shift(sum(amount), 1, 'month')", "n")], None, False),
    ("shift_sub_day", "hour", [_m("time_shift(sum(amount), -24, 'hour')", "p"),
                               _m("time_shift(sum(amount), 24, 'hour')", "n")], ["2024-05-15", "2024-05-17"], False),
    ("consecutive_day", "day", [_m("consecutive_periods(count(*) > 0)", "c")], ["2024-05-14", "2024-05-18"], False),
    ("consecutive_month", "month", [_m("consecutive_periods(count(*) > 0)", "c")], None, False),
    ("trailing_window", "month", [_m("sum(amount, window='2m')", "w")], ["2024-01-01", "2024-08-31"], False),
    ("whole_periods", "month", [_m("sum(amount)", "s")], ["2024-01-15", "2024-05-16"], True),
    ("sunday_weeks", "week_sunday", [_m("sum(amount)", "s"), _m("time_shift(sum(amount), -1)", "p")], None, False),
]


def _sums(unit: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for _i, day, _ts, amount in PX.rows:
        key = bucket_text(oracle_bucket(date.fromisoformat(day), unit))
        out[key] = out.get(key, 0.0) + amount
    return out


def _shifted(unit: str, step: timedelta) -> dict[str, tuple]:
    """``(sum one step back, sum one step ahead)`` per bucket with data."""
    sums = _sums(unit)

    def at(key: str, delta: timedelta) -> Optional[float]:
        return sums.get(bucket_text(datetime.fromisoformat(key) + delta))

    return {k: (at(k, -step), at(k, step)) for k in sums}


def _within(series: dict[str, tuple], start: str, end: str) -> dict[str, tuple]:
    return {k: v for k, v in series.items() if start <= k[:10] <= end}


_SUNDAY_SUMS = _sums("week_sunday")
# Independent expected series where the parity check alone could hide a shared error.
EXPECTED: dict[str, dict[str, tuple]] = {
    "shift_day": _within(_shifted("day", timedelta(days=1)), "2024-05-14", "2024-05-18"),
    "shift_sub_day": _within(_shifted("hour", timedelta(hours=24)), "2024-05-15", "2024-05-17"),
    "consecutive_day": {"2024-05-15 00:00:00": (1.0,), "2024-05-16 00:00:00": (2.0,), "2024-05-17 00:00:00": (3.0,)},
    "whole_periods": {f"2024-0{m}-01 00:00:00": (float(m),) for m in range(1, 5)},
    "sunday_weeks": {
        k: (v, _SUNDAY_SUMS.get(bucket_text(datetime.fromisoformat(k) - timedelta(days=7))))
        for k, v in _SUNDAY_SUMS.items()
    },
}


class TestDateTimestampParity:
    @pytest.mark.parametrize(("case", "granularity", "measures", "date_range", "whole"),
                             PARITY_CASES, ids=[c[0] for c in PARITY_CASES])
    async def test_series_agree_and_buckets_keep_their_types(self, engine, case, granularity, measures, date_range,
                                                             whole) -> None:
        backend, eng = engine
        on_date, date_buckets = await _series(eng, column="d", granularity=granularity, measures=measures,
                                              date_range=date_range, whole_periods_only=whole)
        on_ts, ts_buckets = await _series(eng, column="ts", granularity=granularity, measures=measures,
                                          date_range=date_range, whole_periods_only=whole)
        assert on_date == on_ts
        assert any(any(v is not None for v in row) for row in on_date.values())
        if case in EXPECTED:
            assert on_date == EXPECTED[case]
        for buckets, source in ((date_buckets, DataType.DATE), (ts_buckets, DataType.TIMESTAMP)):
            expected = bucket_type(source, granularity)
            problems = [(b, why) for b in buckets if (why := physical_type_error(b, expected, backend=backend))]
            assert not problems, problems


def _monthly_sums() -> dict[str, float]:
    out: dict[str, float] = {}
    for _i, day, _ts, amount in PX.rows:
        key = f"{day[:7]}-01 00:00:00"
        out[key] = out.get(key, 0.0) + amount
    return out


class TestShiftOracle:
    async def test_month_shift_reads_the_neighbouring_month(self, engine) -> None:
        _backend, eng = engine
        series, _ = await _series(eng, column="d", granularity="month", measures=PARITY_CASES[0][2],
                                  date_range=["2024-01-01", "2024-08-31"])
        sums = _monthly_sums()
        months = [f"2024-{m:02d}-01 00:00:00" for m in range(1, 9)]
        expected = {
            m: (sums.get(months[k - 1]) if k else None, sums.get(months[k + 1]) if k + 1 < len(months) else None)
            for k, m in enumerate(months) if m in sums
        }
        assert {m: v for m, v in series.items() if m in expected} == expected

    async def test_sunday_weeks_group_sunday_with_the_following_monday(self, engine) -> None:
        _backend, eng = engine
        series, _ = await _series(eng, column="d", granularity="week_sunday", measures=[_m("sum(amount)", "s")])
        assert series["2024-12-29 00:00:00"] == (9.0 + 10.0,)
        assert series["2025-01-05 00:00:00"] == (11.0 + 12.0,)
