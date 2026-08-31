"""DEV-1825 — executed values (SQLite + DuckDB) for regroup shapes beyond the
DEV-1740 flagship: multi-output producers and NOT-filters on the computed dim.

Dataset and expectations are hand-computed in ``tests/_dev1740_fixtures.py``:
city sums Paris=7000 Berlin=9000 Lyon=1000 NYC=2000 NULL=6000; per-city
non-NULL amount counts Paris=2 Berlin=2 Lyon=2 NYC=2 NULL=1.
"""

from __future__ import annotations

import pytest

from slayer.core.query import (
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
)
from slayer.core.enums import TimeGranularity

from tests._dev1740_fixtures import make_exec_engine, month_key

BAND = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


class TestMultiOutputProducerValues:
    async def test_sum_and_count_in_one_band(self, exec_engine) -> None:
        # band = 1 iff city sum > 5000 AND city count > 1: Paris, Berlin.
        # NULL-city has sum 6000 but count 1 -> band 0.
        band = ("CASE WHEN amount:sum(partition_by=city) > 5000 "
                "AND amount:count(partition_by=city) > 1 THEN 1 ELSE 0 END")
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(16000.0),
                      ("EU", 0): pytest.approx(1000.0),
                      ("US", 0): pytest.approx(8000.0)}

    async def test_two_dimensions_sharing_one_producer(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[
                "region",
                {"expression": BAND, "name": "band_hi"},
                {"expression": "CASE WHEN amount:count(partition_by=city) > 1 "
                               "THEN 1 ELSE 0 END", "name": "band_n"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band_hi"]), int(r["orders.band_n"])):
              float(r["orders.s"]) for r in resp.data}
        assert by == {
            ("EU", 1, 1): pytest.approx(16000.0),   # Paris + Berlin
            ("EU", 0, 1): pytest.approx(1000.0),    # Lyon
            ("US", 0, 1): pytest.approx(2000.0),    # NYC
            ("US", 1, 0): pytest.approx(6000.0),    # NULL city
        }


class TestNotFilterOnComputedDim:
    async def test_not_band_keeps_only_band_zero_groups(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["not (band == 1)"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
              for r in resp.data}
        assert by == {("EU", 0): pytest.approx(1000.0),
                      ("US", 0): pytest.approx(2000.0)}


class TestDateRangeReachesProducer:
    async def test_date_range_bound_copies_into_partition_aggregate(self, exec_engine) -> None:
        # January-only. If the date bound does NOT reach the producer, Berlin's
        # FULL total (9000) bands to 1; with copying, Berlin's Jan-only total
        # (5000, not > 5000) bands to 0. Every Jan city total is <= 5000 -> all
        # band 0, distinguishing the two behaviours by VALUE.
        jan = [TimeDimension(
            dimension=ColumnRef(name="ordered_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2024-01-01", "2024-01-31"],
        )]
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            time_dimensions=jan,
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], month_key(r["orders.ordered_at"]),
               int(r["orders.band"])): float(r["orders.s"]) for r in resp.data}
        assert by == {
            ("EU", "2024-01", 0): pytest.approx(8400.0),  # Paris 3000 + Berlin 5000 + Lyon 400
            ("US", "2024-01", 0): pytest.approx(800.0),   # NYC
        }


class TestOrderByComputedDimDescending:
    async def test_order_by_band_desc(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": "band", "direction": "desc"}],
        ))
        bands = [int(r["orders.band"]) for r in resp.data]
        assert bands == sorted(bands, reverse=True)
