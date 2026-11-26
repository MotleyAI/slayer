"""Under a query zone one engine-clock reading, taken in the zone, drives tokens, whole_periods_only, the spine and
now() / current_date() (spec deltas: queries/time-zones, queries/time-points, queries/date-functions)."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import date, datetime
from typing import Any

import pytest

from slayer.core.query import SlayerQuery
from slayer.database.client import SlayerSQLClient

from tests._dev2000_fixtures import (
    BACKENDS,
    NY,
    ROWS,
    PinnedClock,
    d,
    day,
    default_datasets,
    instant,
    local,
    m,
    naive,
    orders_dataset,
    orders_table,
    q,
    tz_engine,
)

NEW_YEAR = "2025-01-01 03:30:00"  # 2024-12-31 22:30 in New York


@pytest.fixture
def host_zone(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], None]]:
    """Sets the host's local zone (``TZ``) for the test."""

    def use(zone: str) -> None:
        monkeypatch.setenv("TZ", zone)
        time.tzset()

    yield use
    monkeypatch.undo()
    time.tzset()


@pytest.fixture
def data_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await real(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def _data(calls: list[str]) -> list[str]:
    return [s for s in calls if "slayer_rk_" not in s]


async def _ids(engine, *filters: str, **extra: Any) -> set[int]:
    resp = await engine.execute(q(dimensions=["orders.id"], filter=list(filters), **extra))
    return {int(r["orders.id"]) for r in resp.data}


@pytest.mark.parametrize("backend", BACKENDS)
class TestZonedClock:
    async def test_relative_tokens_read_the_clock_in_the_zone(self, backend) -> None:
        async with tz_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as engine:
            assert await _ids(engine, "orders.ordered_at = 'today'", time_zone=NY) == {1}

    async def test_a_naive_clock_reading_is_host_local(self, backend, host_zone) -> None:
        host_zone("Asia/Tokyo")
        async with tz_engine(backend, clock=PinnedClock(datetime(2025, 1, 1, 12, 30))) as engine:
            assert await _ids(engine, "orders.ordered_at = 'today'", time_zone=NY) == {1}

    async def test_now_current_date_and_tokens_agree(self, backend) -> None:
        async with tz_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as engine:
            resp = await engine.execute(q(time_zone=NY, dimensions=[
                "orders.id", d("current_date()", "cd"), d("now()", "nw")], filter=["orders.ordered_at < 'tomorrow'"]))
        assert {int(r["orders.id"]) for r in resp.data} == {1}
        [row] = resp.data
        assert day(row["cd"]) == date(2024, 12, 31)
        assert naive(row["nw"]) == datetime(2024, 12, 31, 22, 30)

    async def test_clock_functions_render_as_literals(self, backend) -> None:
        async with tz_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as engine:
            resp = await engine.execute(q(time_zone=NY, dimensions=[d("current_date()", "cd"), d("now()", "nw")]),
                                        dry_run=True)
        sql = (resp.sql or "").upper()
        assert "2024-12-31 22:30:00" in sql, sql
        assert "2024-12-31" in sql, sql
        assert "CURRENT_DATE" not in sql, sql
        assert "CURRENT_TIMESTAMP" not in sql, sql
        assert "'NOW'" not in sql, sql

    async def test_the_clock_is_read_once_for_every_stage(self, backend) -> None:
        clock = PinnedClock(instant(NEW_YEAR))
        stage = {"name": "s1", "population": "orders", "time_zone": "Asia/Tokyo",
                 "dimensions": ["orders.id", d("now()", "nw")], "filter": ["orders.ordered_at >= 'last month'"]}
        final = {"population": "s1", "time_zone": NY, "dimensions": ["s1.id", "s1.nw", d("now()", "outer")]}
        async with tz_engine(backend, clock=clock) as engine:
            clock.calls = 0
            resp = await engine.execute([stage, final])
        assert clock.calls == 1
        for row in resp.data:
            assert naive(row["s1.nw"]) == datetime(2025, 1, 1, 12, 30)  # no timestamp read: civil, so not reconverted
            assert naive(row["outer"]) == datetime(2024, 12, 31, 22, 30)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_whole_periods_only_and_the_spine_in_the_zone(backend) -> None:
    spine = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2024-11-01", "2025-04-30"]}
    query = {"time_dimensions": [spine], "metrics": [m("sum(orders.amount)", "a")], "whole_periods_only": True}
    async with tz_engine(backend, clock=PinnedClock(instant("2025-03-01 02:00:00"))) as zoned:
        got = await zoned.execute(q(time_zone=NY, **query))
    ny_values = {i: local(i, NY).strftime("%Y-%m-%d %H:%M:%S") for i in ROWS}
    async with tz_engine(backend, tables=[orders_table(ordered_at=ny_values)],
                         datasets=[orders_dataset(field_zones={})], ds_zone=None,
                         clock=PinnedClock(datetime(2025, 2, 28, 21, 0))) as reference:
        want = await reference.execute(q(**query))
    assert sorted((day(r["time_spine.timestamp"]), r["a"]) for r in got.data) == sorted(
        (day(r["time_spine.timestamp"]), r["a"]) for r in want.data), got.sql


@pytest.mark.parametrize("backend", BACKENDS)
class TestZoneLessClockUnchanged:
    async def test_tokens_read_the_host_wall_clock(self, backend, host_zone) -> None:
        host_zone("UTC")
        async with tz_engine(backend, clock=PinnedClock(instant(NEW_YEAR))) as engine:
            assert await _ids(engine, "orders.ordered_at = 'today'") == {1, 2}

    async def test_clock_functions_still_read_the_database_clock(self, backend) -> None:
        async with tz_engine(backend, datasets=default_datasets(), clock=PinnedClock(instant(NEW_YEAR))) as engine:
            resp = await engine.execute(q(dimensions=[d("now()", "nw")]), dry_run=True)
        assert "2025-01-01 03:30" not in (resp.sql or ""), resp.sql
        assert "2024-12-31 22:30" not in (resp.sql or ""), resp.sql


class TestCache:
    async def test_zoned_current_date_is_cached(self, data_calls) -> None:
        query = q(time_zone="UTC", metrics=[m("count(orders.*)", "n")],
                  filter=["orders.ordered_at >= date_add(current_date(), -30, 'day')"])
        async with tz_engine("sqlite", clock=PinnedClock(instant(NEW_YEAR))) as engine:
            first = await engine.execute(query, cache=True)
            second = await engine.execute(query, cache=True)
            assert engine.cache_size == 1
        assert len(_data(data_calls)) == 1
        assert first.data == second.data

    async def test_zoned_now_still_bypasses(self, data_calls) -> None:
        query = q(time_zone="UTC", dimensions=[d("now()", "nw")])
        async with tz_engine("sqlite", clock=PinnedClock(instant(NEW_YEAR))) as engine:
            await engine.execute(query, cache=True)
            await engine.execute(query, cache=True)
            assert engine.cache_size == 0
        assert len(_data(data_calls)) == 2

    async def test_zone_less_current_date_still_bypasses(self, data_calls) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [m("count(orders.*)", "n")],
                                            "filter": ["orders.ordered_at >= date_add(current_date(), -30, 'day')"]})
        async with tz_engine("sqlite", clock=PinnedClock(instant(NEW_YEAR))) as engine:
            await engine.execute(query, cache=True)
            await engine.execute(query, cache=True)
            assert engine.cache_size == 0
        assert len(_data(data_calls)) == 2
