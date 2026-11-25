"""Executed time-zone scenarios on SQLite and DuckDB (spec: queries/time-zones)."""

from __future__ import annotations

import warnings
from collections import Counter
from datetime import date, datetime, timedelta

import pytest

from slayer.core.query import SlayerQuery

from tests._dev2000_fixtures import (
    BACKENDS,
    DST_INSTANTS,
    HOURLY_COUNT,
    HOURLY_START,
    NY,
    ROWS,
    TOKYO,
    ZONES,
    amount,
    by_id,
    d,
    day,
    default_datasets,
    local,
    m,
    naive,
    orders_dataset,
    q,
    to_zone,
    tz_engine,
    warnings_of,
)

DAY_TD = {"field": "orders.ordered_at", "granularity": "day"}


def _day_sums(zone: str) -> dict[date, float]:
    out: Counter[date] = Counter()
    for i in ROWS:
        out[local(i, zone).date()] += amount(i)
    return dict(out)


@pytest.mark.parametrize("backend", BACKENDS)
class TestReadAsZoneWallClock:
    async def test_local_day_bucket_new_york(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(q(time_zone=NY, time_dimensions=[DAY_TD], metrics=[m("sum(orders.amount)", "s")]))
        got = {day(r["orders.ordered_at"]): float(r["s"]) for r in resp.data}
        assert got == {date(2024, 12, 31): 10.0, date(2025, 1, 1): 20.0, date(2025, 1, 31): 30.0,
                       date(2025, 3, 9): 40.0}

    @pytest.mark.parametrize("zone", ZONES)
    async def test_local_day_bucket_matches_oracle(self, backend, zone) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(q(time_zone=zone, time_dimensions=[DAY_TD], metrics=[m("sum(orders.amount)", "s")]))
        assert {day(r["orders.ordered_at"]): float(r["s"]) for r in resp.data} == _day_sums(zone)

    async def test_utc_near_midnight_lands_in_the_local_day(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(q(time_zone=NY, dimensions=["orders.id"], filter=["orders.ordered_at in '2024-12-31'"]))
        assert {int(r["orders.id"]) for r in resp.data} == {1}

    async def test_datetimetz_returned_as_local_wall_clock(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(q(time_zone=TOKYO, dimensions=["orders.id", "orders.paid_at"],
                                          filter=["orders.id = 3"]))
        assert naive(by_id(resp, "orders.paid_at")[3]) == datetime(2025, 2, 1, 8, 30)

    @pytest.mark.parametrize("zone", ZONES)
    async def test_naive_and_instant_fields_read_alike(self, backend, zone) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(q(time_zone=zone, dimensions=["orders.id", "orders.ordered_at", "orders.paid_at"]))
        for r in resp.data:
            i = int(r["orders.id"])
            assert naive(r["orders.ordered_at"]) == local(i, zone), resp.sql
            assert naive(r["orders.paid_at"]) == local(i, zone), resp.sql

    async def test_date_passes_through(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(q(time_zone=TOKYO, dimensions=["orders.order_day"], metrics=[m("count(orders.*)", "n")]))
        assert {day(r["orders.order_day"]): int(r["n"]) for r in resp.data} == {
            date(2025, 1, 1): 2, date(2025, 1, 31): 1, date(2025, 3, 9): 1}

    async def test_field_zone_wins_over_the_datasource_zone(self, backend) -> None:
        datasets = default_datasets(orders_dataset(field_zones={"ordered_at": TOKYO, "local_slot": "civil"}))
        async with tz_engine(backend, datasets=datasets) as engine:
            resp = await engine.execute(q(time_zone="UTC", dimensions=["orders.id", "orders.ordered_at"],
                                          filter=["orders.id = 1"]))
        assert naive(by_id(resp, "orders.ordered_at")[1]) == datetime(2024, 12, 31, 18, 30)

    async def test_civil_skips_the_datasource_zone(self, backend) -> None:
        async with tz_engine(backend) as engine:
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                resp = await engine.execute(q(time_zone=NY, dimensions=["orders.id", "orders.local_slot"]))
        assert {i: naive(v) for i, v in by_id(resp, "orders.local_slot").items()} == {
            i: datetime.fromisoformat(ROWS[i][2]) for i in ROWS}
        assert not warnings_of(resp)


@pytest.mark.parametrize("backend", BACKENDS)
class TestDst:
    async def test_a_skipped_time_takes_the_pre_transition_offset(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "population": "dst", "time_zone": "UTC", "dimensions": ["dst.id", "dst.local_at"],
            }))
        got = {int(r["dst.id"]): naive(r["dst.local_at"]) for r in resp.data}
        assert got[1] == datetime(2025, 3, 9, 7, 30)

    async def test_a_repeated_time_takes_the_later_occurrence(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "population": "dst", "time_zone": "UTC", "dimensions": ["dst.id", "dst.local_at"],
            }))
        got = {int(r["dst.id"]): naive(r["dst.local_at"]) for r in resp.data}
        assert got[2] == datetime(2024, 11, 3, 6, 30)

    async def test_a_25_hour_day(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "population": "hourly", "time_zone": NY, "metrics": [m("count(hourly.*)", "n")],
                "time_dimensions": [{"field": "hourly.ts", "granularity": "day"}],
            }))
        got = {day(r["hourly.ts"]): int(r["n"]) for r in resp.data}
        assert got[date(2024, 11, 3)] == 25
        assert sum(got.values()) == HOURLY_COUNT
        expected = Counter(to_zone(HOURLY_START + timedelta(hours=h), NY).date() for h in range(HOURLY_COUNT))
        assert got == dict(expected)

    async def test_instants_in_a_repeated_hour_read_alike(self, backend) -> None:
        async with tz_engine(backend) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "population": "dst", "time_zone": NY, "dimensions": ["dst.id", "dst.instant_at"],
            }))
        got = {int(r["dst.id"]): naive(r["dst.instant_at"]) for r in resp.data}
        assert got == {i: to_zone(datetime.fromisoformat(t), NY) for i, t in DST_INSTANTS.items()}
        assert set(got.values()) == {datetime(2024, 11, 3, 1, 30)}


async def test_zoned_cast_is_converted_at_the_cast() -> None:
    async with tz_engine("duckdb") as engine:
        resp = await engine.execute(q(time_zone=TOKYO, dimensions=[
            d("CAST('2025-01-01 00:00:00+00:00' AS TIMESTAMPTZ)", "v")]))
    assert naive(resp.data[0]["v"]) == datetime(2025, 1, 1, 9, 0)
