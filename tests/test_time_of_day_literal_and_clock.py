"""The ``TIME`` literal and ``current_time()`` (spec: queries/time-of-day, queries/expression-grammar)."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import TimeLiteralError
from slayer.core.models import DatasetField
from slayer.core.syntax import parse_expr
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2000_fixtures import PinnedClock, instant, naive
from tests._time_points_fixtures import PinnedClock as SteppingClock
from tests._time_of_day_fixtures import (
    BACKENDS, NY, bind, d, emit, filtered_ids, is_time, planned_type, q, shifts_dataset, shifts_engine, value_of,
)

NEW_YEAR = "2025-01-01 03:30:00"  # 22:30 the day before in New York
CLOCK_SPELLINGS = ["current_time()", "CURRENT_TIME", "CURRENT_TIME()", "Current_Time"]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with shifts_engine(request.param) as eng:
        yield eng


@pytest.fixture
def data_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await real(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def _seconds(value: time) -> float:
    return value.hour * 3600 + value.minute * 60 + value.second + value.microsecond / 1e6


def _apart(a: time, b: time) -> float:
    """Seconds between two times of day, around midnight."""
    gap = abs(_seconds(a) - _seconds(b))
    return min(gap, 86400 - gap)


class TestLiteral:
    def test_the_literal_parses(self) -> None:
        parse_expr("opened_at > TIME '10:00'")

    @pytest.mark.parametrize("text", ["TIME '10:30:00'", "time '10:30'", "TIME '10:30:00.123456'"])
    def test_planned_type(self, text: str) -> None:
        assert planned_type(text) is DataType.TIME

    @pytest.mark.parametrize(("text", "expected"), [
        ("TIME '10:30:00'", time(10, 30)),
        ("TIME '10:30'", time(10, 30)),
        ("TIME '10:30:00.123456'", time(10, 30, 0, 123456)),
        ("TIME '10:30:00.5'", time(10, 30, 0, 500000)),
        ("TIME '00:00:00'", time(0, 0)),
        ("TIME '23:59:59.999999'", time(23, 59, 59, 999999)),
        ("time '09:05:07'", time(9, 5, 7)),
    ])
    async def test_value(self, engine: SlayerQueryEngine, text: str, expected: time) -> None:
        got = await value_of(engine, text)
        assert is_time(got, expected), got

    @pytest.mark.parametrize("text", [
        "24:00:00", "10:30:00+02:00", "10:30:00Z", "2024-01-15", "2024-01-15 10:30:00", "10:60:00", "10:30:60",
        "10:30:00.1234567", "10", "noon",
    ])
    async def test_invalid_text_is_rejected_at_bind_time(self, engine: SlayerQueryEngine, data_calls: list[str],
                                                         text: str) -> None:
        with pytest.raises(TimeLiteralError) as info:
            await value_of(engine, f"TIME '{text}'")
        assert text in str(info.value), str(info.value)
        assert [s for s in data_calls if "slayer_rk_" not in s] == []

    async def test_in_a_filter_an_order_and_a_conditional(self, engine: SlayerQueryEngine) -> None:
        assert await filtered_ids(engine, "shifts.starts_at >= TIME '09:00:00'") == {1, 2, 3}
        assert await filtered_ids(engine, "shifts.starts_at > TIME '09:30'") == {3}
        resp = await engine.execute(q(
            dimensions=["shifts.id", d("CASE WHEN shifts.id = 1 THEN TIME '08:00' ELSE shifts.starts_at END", "t")],
            order=[{"field": "CASE WHEN shifts.id = 1 THEN TIME '08:00' ELSE shifts.starts_at END"},
                   {"field": "shifts.id"}]))
        assert [int(r["shifts.id"]) for r in resp.data] == [1, 2, 3]
        assert is_time(resp.data[0]["t"], time(8, 0)), resp.data

    async def test_as_an_aggregate_argument(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[{"metric": "max(greatest(shifts.starts_at, TIME '10:30'))", "alias": "v"},
                                               {"metric": "min(TIME '08:00')", "alias": "lo"}]))
        [row] = resp.data
        assert is_time(row["v"], time(17, 0)), row
        assert is_time(row["lo"], time(8, 0)), row

    def test_one_key_for_every_spelling_of_one_time(self) -> None:
        assert bind("TIME '10:30'") == bind("TIME '10:30:00'") == bind("time '10:30:00.000'")


class TestCurrentTime:
    @pytest.mark.parametrize("spelling", CLOCK_SPELLINGS)
    def test_planned_type(self, spelling: str) -> None:
        assert planned_type(spelling) is DataType.TIME

    def test_spellings_share_one_key(self) -> None:
        assert len({bind(s) for s in CLOCK_SPELLINGS}) == 1

    @pytest.mark.parametrize("spelling", CLOCK_SPELLINGS)
    async def test_reads_the_database_clock(self, engine: SlayerQueryEngine, spelling: str) -> None:
        resp = await engine.execute(q(dimensions=[d(spelling, "ct"), d("now()", "nw")]))
        [row] = resp.data
        assert type(row["ct"]) is time, row
        assert _apart(row["ct"], naive(row["nw"]).time()) <= 1, row

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_without_a_zone_it_is_not_the_engine_clock(self, backend: str) -> None:
        async with shifts_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as eng:
            resp = await eng.execute(q(dimensions=[d("current_time()", "ct")]), dry_run=True)
        assert "03:30" not in (resp.sql or ""), resp.sql
        assert "22:30" not in (resp.sql or ""), resp.sql

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_under_a_zone_it_is_the_time_of_day_of_now(self, backend: str) -> None:
        async with shifts_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as eng:
            resp = await eng.execute(q(time_zone=NY, dimensions=[d("current_time()", "ct"), d("now()", "nw")]))
        [row] = resp.data
        assert is_time(row["ct"], time(22, 30)), row
        assert naive(row["nw"]) == datetime(2024, 12, 31, 22, 30)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_under_a_zone_it_renders_as_a_literal(self, backend: str) -> None:
        async with shifts_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as eng:
            resp = await eng.execute(q(time_zone=NY, dimensions=[d("CURRENT_TIME", "ct")]), dry_run=True)
        sql = (resp.sql or "").upper()
        assert "22:30:00" in sql, sql
        assert "CURRENT_TIME" not in sql, sql
        assert "'NOW'" not in sql, sql

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_one_clock_reading_per_execution(self, backend: str) -> None:
        clock = SteppingClock(instant(NEW_YEAR), step=timedelta(hours=1))
        async with shifts_engine(backend, clock=clock) as eng:
            clock.calls = 0
            resp = await eng.execute(q(time_zone=NY, dimensions=[d("current_time()", "ct"), d("now()", "nw")]))
        assert clock.calls == 1
        [row] = resp.data
        assert is_time(row["ct"], time(22, 30)), row
        assert row["ct"] == naive(row["nw"]).time(), row

    @pytest.mark.parametrize("zone", [None, "UTC"])
    @pytest.mark.parametrize("spelling", ["current_time()", "CURRENT_TIME"])
    async def test_never_cached(self, data_calls: list[str], zone: str | None, spelling: str) -> None:
        extra: dict[str, Any] = {} if zone is None else {"time_zone": zone}
        query = q(dimensions=[d(spelling, "ct")], **extra)
        async with shifts_engine("sqlite", clock=PinnedClock(instant(NEW_YEAR))) as eng:
            await eng.execute(query, cache=True)
            await eng.execute(query, cache=True)
            assert eng.cache_size == 0
        assert len([s for s in data_calls if "slayer_rk_" not in s]) == 2

    @pytest.mark.parametrize("call", ["current_time(1)", "CURRENT_TIME(3)", "current_time('UTC')"])
    async def test_an_argument_is_rejected(self, engine: SlayerQueryEngine, call: str) -> None:
        with pytest.raises(ValueError, match=r"(?is)current_time.*argument"):
            await value_of(engine, call)

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite"])
    async def test_emits_in_a_filter(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["shifts.id"], filter=["shifts.starts_at <= CURRENT_TIME"]), dialect=dialect)
        assert "starts_at" in sql


class TestAFieldNamedCurrentTime:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_the_quoted_name_reads_the_field(self, backend: str) -> None:
        dataset = shifts_dataset(extra_fields=[
            DatasetField(name="current_time", expression="label", datatype=DataType.STRING)])
        async with shifts_engine(backend, dataset=dataset) as eng:
            resp = await eng.execute(q(dimensions=["shifts.id", d('shifts."current_time"', "f"), d("CURRENT_TIME", "c")]))
        assert {int(r["shifts.id"]): r["f"] for r in resp.data} == {1: "a", 2: "b", 3: "c"}
        assert all(type(r["c"]) is time for r in resp.data), resp.data
