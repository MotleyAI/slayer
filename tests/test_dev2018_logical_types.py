"""A bucket has one logical type on every dialect: executed values and physical types, typing, nested positions."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import TimeLiteralError
from slayer.core.granularity import CustomGranularity, Granularity
from slayer.core.keys import ColumnKey, ScalarCallKey, TimeTruncKey, ValueKey, temporal_type, value_type
from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import dimension_key_metadata
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1737_fixtures import customers_model, orders_model
from tests._dev2018_fixtures import (
    ALL_UNITS,
    BK,
    GRANULARITIES,
    IN_PROCESS,
    WITH_POSTGRES,
    as_temporal,
    bucket_rows,
    check_bucket_values,
    ids,
    lt_engine,
    physical_type_error,
)
from tests._time_points_fixtures import EV_TS, ev_dt, shipped_dt, tp_engine

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


@pytest.fixture(params=IN_PROCESS)
async def ev_engine(request):
    async with tp_engine(request.param) as eng:
        yield request.param, eng


# ---------------------------------------------------------------------------
# The bucket matrix: every unit, DATE and TIMESTAMP sources, NULL, both positions.
# ---------------------------------------------------------------------------

class TestBucketMatrix:
    @pytest.mark.parametrize(("column", "source"), [("d", DataType.DATE), ("ts", DataType.TIMESTAMP)])
    @pytest.mark.parametrize("position", ["time_dimension", "dimension"])
    async def test_value_and_physical_type(self, any_engine, column, source, position) -> None:
        backend, eng = any_engine
        problems: list[str] = []
        for unit in ALL_UNITS:
            rows = await bucket_rows(eng, column=column, unit=unit, position=position)
            assert len(rows) == len(BK.rows), (unit, rows)
            problems += check_bucket_values(rows, source=source, unit=unit, table=BK, column=column, backend=backend)
        assert not problems, "\n".join(problems)

    async def test_sunday_weeks(self, engine) -> None:
        backend, eng = engine
        for column, expected_type in (("d", DataType.DATE), ("ts", DataType.TIMESTAMP)):
            got = dict(await bucket_rows(eng, column=column, unit="week_sunday", position="dimension"))
            assert {i: None if v is None else str(as_temporal(v))[:10] for i, v in got.items()} == {
                1: "2024-06-09", 2: "2024-12-29", 3: "2024-12-29", 4: "2025-03-30", 5: "2025-03-30", 6: None,
            }
            assert [physical_type_error(v, expected_type, backend=backend) for v in got.values()] == [None] * 6

    async def test_bucket_equals_its_own_column_at_a_boundary(self, engine) -> None:
        _backend, eng = engine
        assert await ids(eng, "date_trunc('day', ts) = ts", model="bk") == {3}


# ---------------------------------------------------------------------------
# Buckets inside aggregates and date functions (the issue's examples), on DATE and TIMESTAMP.
# ---------------------------------------------------------------------------

ALL_EV = set(EV_TS)


def _customer(i: int) -> int:
    return 1 + i % 3


def _month(v: datetime) -> datetime:
    return v.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _months_between(start: datetime, end: Optional[datetime]) -> Optional[int]:
    return None if end is None else (end.year * 12 + end.month) - (start.year * 12 + start.month)


class TestNestedBuckets:
    @pytest.mark.parametrize("column", ["ts", "d"])
    async def test_max_of_a_bucket_against_a_time_point(self, ev_engine, column) -> None:
        _backend, eng = ev_engine
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["customer_id"],
            "filters": [f"max(date_trunc('month', {column})) >= 'last month'"],
            "measures": [{"formula": "count(*)", "name": "n"}],
        }))
        latest = {c: max(_month(ev_dt(i)) for i in ALL_EV if _customer(i) == c) for c in (1, 2, 3)}
        assert {int(r["ev.customer_id"]) for r in resp.data} == {c for c, m in latest.items() if m >= D(2026, 8, 1)}

    @pytest.mark.parametrize("column", ["ts", "d"])
    async def test_date_diff_from_a_bucket(self, ev_engine, column) -> None:
        _backend, eng = ev_engine
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": [
                "id",
                {"expression": f"date_diff('month', date_trunc('month', {column}), shipped_at)", "name": "mo"},
                {"expression": f"date_diff('hour', date_trunc('month', {column}), ts)", "name": "hr"},
                {"expression": f"date_diff('day', date_trunc('month', {column}), d)", "name": "dy"},
            ],
        }))
        got = {int(r["ev.id"]): (r["ev.mo"], r["ev.hr"], r["ev.dy"]) for r in resp.data}
        expected = {}
        for i in ALL_EV:
            start = _month(ev_dt(i))
            ts = ev_dt(i)
            expected[i] = (
                _months_between(start, shipped_dt(i)),
                int((ts.replace(minute=0, second=0) - start).total_seconds() // 3600),
                (ts.date() - start.date()).days,
            )
        assert {i: tuple(None if v is None else int(v) for v in row) for i, row in got.items()} == expected

    @pytest.mark.parametrize(("column", "expected_type"), [("ts", DataType.TIMESTAMP), ("d", DataType.DATE)])
    async def test_date_add_to_a_bucket(self, ev_engine, column, expected_type) -> None:
        backend, eng = ev_engine
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": [
                "id", {"expression": f"date_add(date_trunc('month', {column}), 1, 'month')", "name": "nxt"},
            ],
        }))
        for r in resp.data:
            month = _month(ev_dt(int(r["ev.id"])))
            nxt = month.replace(year=month.year + month.month // 12, month=month.month % 12 + 1)
            got = as_temporal(r["ev.nxt"])
            assert got == (nxt.date() if expected_type is DataType.DATE else nxt), r
            assert physical_type_error(r["ev.nxt"], expected_type, backend=backend) is None, r

    async def test_sub_day_point_against_a_day_bucket_of_a_date(self, ev_engine) -> None:
        _backend, eng = ev_engine
        query = SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["id"],
            "filters": ["date_add(date_trunc('month', d), 1, 'day') >= '2025-01-01 10:00:00'"],
        })
        with pytest.raises(TimeLiteralError) as ei:
            await eng.execute(query)
        assert "day" in str(ei.value).lower()


# ---------------------------------------------------------------------------
# Typing: the bucket rule in temporal_type, value_type and the query plan.
# ---------------------------------------------------------------------------

_CUSTOM = {g["name"]: CustomGranularity.model_validate(g) for g in GRANULARITIES}
_TYPES = {"d": DataType.DATE, "ts": DataType.TIMESTAMP}


def _column_type(key: ValueKey) -> Optional[DataType]:
    assert isinstance(key, ColumnKey), key
    return _TYPES[key.leaf]


def _unit(name: str) -> Granularity:
    return _CUSTOM[name] if name in _CUSTOM else TimeGranularity(name)


def _bucket(unit: str, column: str) -> TimeTruncKey:
    return TimeTruncKey(source=ColumnKey(leaf=column), granularity=_unit(unit))


class TestBucketTyping:
    @pytest.mark.parametrize(("unit", "column", "expected"), [
        ("hour", "d", DataType.TIMESTAMP),
        ("minute", "d", DataType.TIMESTAMP),
        ("second", "d", DataType.TIMESTAMP),
        ("quarter_hour", "d", DataType.TIMESTAMP),
        ("day", "d", DataType.DATE),
        ("week_sunday", "d", DataType.DATE),
        ("month", "d", DataType.DATE),
        ("year", "d", DataType.DATE),
        ("fiscal_year", "d", DataType.DATE),
        ("month", "ts", DataType.TIMESTAMP),
        ("day", "ts", DataType.TIMESTAMP),
        ("fiscal_year", "ts", DataType.TIMESTAMP),
    ])
    def test_bucket_type_rule(self, unit, column, expected) -> None:
        key = _bucket(unit, column)
        assert temporal_type(key, column_type=_column_type) is expected
        assert value_type(key, column_type=_column_type) is expected

    def test_nested_bucket_is_bucketed_by_its_own_type(self) -> None:
        inner = _bucket("hour", "d")
        assert temporal_type(TimeTruncKey(source=inner, granularity=TimeGranularity.MONTH),
                             column_type=_column_type) is DataType.TIMESTAMP

    @pytest.mark.parametrize(("name", "args", "expected"), [
        ("nullif", (ColumnKey(leaf="d"), ColumnKey(leaf="ts")), DataType.DATE),
        ("nullif", (_bucket("month", "d"), ColumnKey(leaf="ts")), DataType.DATE),
        ("nullif", (_bucket("hour", "d"), ColumnKey(leaf="d")), DataType.TIMESTAMP),
        ("coalesce", (_bucket("hour", "d"), ColumnKey(leaf="d")), DataType.TIMESTAMP),
        ("coalesce", (_bucket("month", "d"), ColumnKey(leaf="d")), DataType.DATE),
        ("greatest", (_bucket("month", "d"), ColumnKey(leaf="ts")), DataType.TIMESTAMP),
    ])
    def test_conditionals_over_buckets(self, name, args, expected) -> None:
        key = ScalarCallKey(name=name, args=args)
        assert temporal_type(key, column_type=_column_type) is expected
        assert value_type(key, column_type=_column_type) is expected


_ORDERS = orders_model()
_BUNDLE = ResolvedSourceBundle(dialect="postgres", source_model=_ORDERS, referenced_models=[customers_model()])


def _bind(text: str) -> ValueKey:
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=_ORDERS), bundle=_BUNDLE,
                     allow_measures=True).value_key


class TestPlanTypes:
    @pytest.mark.parametrize(("text", "expected"), [
        ("date_trunc('month', order_date)", DataType.DATE),
        ("date_trunc('hour', order_date)", DataType.TIMESTAMP),
        ("date_trunc('day', created_at)", DataType.TIMESTAMP),
        ("date_add(date_trunc('month', order_date), 1, 'month')", DataType.DATE),
        ("date_add(date_trunc('hour', order_date), 1, 'month')", DataType.TIMESTAMP),
    ])
    def test_dimension_metadata(self, text, expected) -> None:
        dim_type, _fmt, _desc = dimension_key_metadata(model=_ORDERS, key=_bind(text), bundle=_BUNDLE)
        assert dim_type == expected
