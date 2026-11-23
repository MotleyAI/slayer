"""A DATE meeting a TIMESTAMP reads as its midnight instant; SQLite temporal comparisons ignore storage spelling."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery

from tests._dev2018_fixtures import (
    IN_PROCESS,
    MIXED_FILTER_CASES,
    SP_MIDNIGHT,
    SP_VALID,
    WITH_POSTGRES,
    as_temporal,
    by_id,
    conditional_value_problems,
    ids,
    lt_engine,
    mx_where,
    physical_type_error,
)

postgresql_proc = factories.postgresql_proc(port=None)

D = datetime


@pytest.fixture(params=WITH_POSTGRES)
async def any_engine(request):
    async with lt_engine(request.param, request=request) as eng:
        yield request.param, eng


@pytest.fixture(params=IN_PROCESS)
async def engine(request):
    async with lt_engine(request.param) as eng:
        yield request.param, eng


@pytest.fixture
async def sqlite_engine():
    async with lt_engine("sqlite") as eng:
        yield eng


class TestMixedComparisons:
    async def test_date_column_against_timestamp_column(self, any_engine) -> None:
        _backend, eng = any_engine
        assert await ids(eng, "mx.d = mx.ts") == {1}

    @pytest.mark.parametrize(("filt", "pred"), MIXED_FILTER_CASES, ids=[c[0] for c in MIXED_FILTER_CASES])
    async def test_filter(self, engine, filt, pred) -> None:
        _backend, eng = engine
        assert await ids(eng, filt) == mx_where(pred)

    @pytest.mark.parametrize(("filt", "expected"), [
        ("date_trunc('hour', mx.d) in ('2025-03-01 00:00:00', '2025-03-03 00:00:00')", {1, 2, 3, 6}),
        ("date_trunc('day', mx.ts) in ('2025-03-01', '2025-03-02')", {1, 2, 3, 4, 5}),
        ("date_trunc('month', mx.d) not in ('2025-02-01')", {1, 2, 3, 4, 6}),
    ])
    async def test_in(self, engine, filt, expected) -> None:
        _backend, eng = engine
        assert await ids(eng, filt) == expected


class TestNullif:
    async def test_date_first_keeps_date(self, engine) -> None:
        backend, eng = engine
        got = await by_id(eng, "nullif(mx.d, mx.ts)")
        assert {i: as_temporal(v) for i, v in got.items()} == {
            1: None, 2: date(2025, 3, 1), 3: date(2025, 3, 1), 4: date(2025, 3, 2), 5: None,
            6: date(2025, 3, 3), 7: None,
        }
        assert all(physical_type_error(v, DataType.DATE, backend=backend) is None for v in got.values()), got

    async def test_timestamp_first_keeps_timestamp(self, engine) -> None:
        backend, eng = engine
        got = await by_id(eng, "nullif(mx.ts, mx.d)")
        assert {i: as_temporal(v) for i, v in got.items()} == {
            1: None, 2: D(2025, 3, 1, 10), 3: D(2025, 3, 2), 4: D(2025, 3, 1, 23), 5: D(2025, 3, 1),
            6: None, 7: None,
        }
        assert all(physical_type_error(v, DataType.DATETIME, backend=backend) is None for v in got.values()), got


class TestTransforms:
    async def test_lag_of_a_date_meets_a_timestamp_at_midnight(self, any_engine) -> None:
        _backend, eng = any_engine
        resp = await eng.execute(SlayerQuery.model_validate({
            "population": "mx", "time_dimensions": [{"field": "mx.ts", "granularity": "day"}],
            "metrics": [{"metric": "iif(lag(max(mx.d)) = max(mx.ts), 1, 0)", "alias": "x"}],
        }))
        got = {_day(as_temporal(r["mx.ts"])): int(r["x"]) for r in resp.data if r["mx.ts"] is not None}
        # Day 03-02: lag(max(d)) is 2025-03-02 and max(ts) is 2025-03-02 00:00.
        assert got == {date(2025, 3, 1): 0, date(2025, 3, 2): 1}


def _day(value: date | None) -> date | None:
    return value.date() if isinstance(value, datetime) else value


class TestConditionalValues:
    async def test_date_arm_of_a_timestamp_conditional_is_midnight(self, engine) -> None:
        backend, eng = engine
        problems = await conditional_value_problems(eng, backend=backend)
        assert not problems, "\n".join(problems)


class TestSqliteStorageSpelling:
    @pytest.mark.parametrize(("filt", "expected"), [
        ("date_trunc('day', sp.ts) = sp.ts", SP_MIDNIGHT),
        ("sp.ts = sp.ts2", SP_VALID),
        ("sp.ts is not distinct from sp.ts2", SP_VALID),
        ("sp.ts is distinct from sp.ts2", {6}),
        ("sp.d = sp.ts", SP_MIDNIGHT),
        ("sp.d is not distinct from sp.ts", SP_MIDNIGHT),
        ("sp.ts >= sp.d", SP_VALID),
        ("date_trunc('hour', sp.ts) = date_trunc('hour', sp.ts2)", SP_VALID),
        ("date_trunc('day', sp.ts) = sp.d", SP_VALID),
        ("date_trunc('day', sp.ts) in ('2025-03-01')", SP_VALID),
        ("sp.ts < sp.ts2", set()),
        ("sp.ts = timestamp '2025-03-01 10:00:00.25'", {4}),
        ("sp.ts = timestamp '2025-03-01 10:00:00.250001'", set()),
        ("sp.d is not distinct from timestamp '2025-03-01 00:00:00'", set(range(1, 8))),
        ("sp.d is distinct from timestamp '2025-03-01 06:00:00'", set(range(1, 8))),
        # A member of the column's own type keeps the author's spelling, as a literal bound does.
        ("sp.ts in ('2025-03-01T10:00:00')", {5}),
        ("sp.ts not in ('2025-03-01T10:00:00')", {1, 2, 3, 4, 6, 7}),
    ])
    async def test_comparisons(self, sqlite_engine, filt, expected) -> None:
        assert await ids(sqlite_engine, filt, model="sp") == set(expected)

    @pytest.mark.parametrize(("unit", "expected"), [
        ("day", {i: "2025-03-01 00:00:00" for i in SP_VALID}),
        ("hour", {1: "2025-03-01 00:00:00", 2: "2025-03-01 00:00:00", 3: "2025-03-01 00:00:00",
                  4: "2025-03-01 10:00:00", 5: "2025-03-01 10:00:00", 7: "2025-03-01 10:00:00"}),
        ("month", {i: "2025-03-01 00:00:00" for i in SP_VALID}),
    ])
    async def test_buckets_are_canonical(self, sqlite_engine, unit, expected) -> None:
        got = await by_id(sqlite_engine, f"date_trunc('{unit}', sp.ts)", model="sp")
        assert got == {**expected, 6: None}

    @pytest.mark.parametrize(("unit", "expected"), [
        ("day", {"2025-03-01 00:00:00": 6}),
        ("hour", {"2025-03-01 00:00:00": 3, "2025-03-01 10:00:00": 3}),
    ])
    async def test_spine_join(self, sqlite_engine, unit, expected) -> None:
        resp = await sqlite_engine.execute(SlayerQuery.model_validate({
            "metrics": [{"metric": "count(sp.id)", "alias": "n"}],
            "time_dimensions": [{"field": "time_spine.timestamp", "granularity": unit,
                                 "date_range": ["2025-03-01", "2025-03-01"]}],
        }))
        got = {str(r["time_spine.timestamp"]).replace("T", " "): r["n"]
               for r in resp.data}
        assert {k: int(v) for k, v in got.items() if v} == expected

