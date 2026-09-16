"""DEV-1865 — filter conjuncts stratify.

Stratum-0 (aggregate-free, all base-level) conjuncts define the population every
producer sees; an attached-value conjunct masks at its re-aggregation point
without feeding other producers; a measure conjunct prunes cells only. Run on
SQLite + DuckDB.
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import (
    BAND35,
    CITY_TOTAL,
    GRAND_TOTAL,
    REGION_TOTAL,
    ModelMeasure,
    month_td,
    q,
    rows_by,
)


def _bucket_key(row):
    """A (region, month) bucket identity — every column except the measure."""
    return tuple(sorted((k, v) for k, v in row.items() if k != "orders.w"))


class TestFieldMaskRestrictsPopulation:
    async def test_row_filter_constrains_every_aggregate(self, exec_engine) -> None:
        # status='ok' drops the one 'hold' row (South/CityC, amount 25); every
        # aggregate is then computed over the passing rows only.
        by = rows_by(await exec_engine.execute(q(
            dimensions=["region", "city"],
            filters=["status = 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        expected = {**CITY_TOTAL, ("South", "CityC"): 25.0}
        assert set(by) == set(expected)
        for key, row in by.items():
            assert float(row["orders.s"]) == pytest.approx(expected[key])

    async def test_stratum0_reaches_the_partition_producer(self, exec_engine) -> None:
        # The partitioned producer recomputes over the stratum-0 population:
        # South's region total drops from 50 to 25 (the hold row is gone).
        by = rows_by(await exec_engine.execute(q(
            dimensions=["region", "city"],
            filters=["status = 'ok'"],
            measures=[ModelMeasure(formula="amount:sum(partition_by=region)", name="part")],
        )), "orders.region", "orders.city")
        ok_region_total = {"North": 100.0, "South": 25.0, None: 60.0}
        for key, row in by.items():
            assert float(row["orders.part"]) == pytest.approx(ok_region_total[key[0]])

    async def test_stratum0_reaches_the_windowed_producer(self, exec_engine) -> None:
        # Same stratum-0 filter reaches a windowed producer: the (South, Mar)
        # bucket is the lone 'hold' order, so it drops when status='ok' applies.
        win = ModelMeasure(
            formula="amount:sum(window='90d', partition_by=region)", name="w",
        )
        unfiltered = {_bucket_key(r): r for r in (await exec_engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(), measures=[win],
        ))).data}
        filtered = {_bucket_key(r): r for r in (await exec_engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            filters=["status = 'ok'"], measures=[win],
        ))).data}
        for key, row in filtered.items():
            assert float(row["orders.w"]) <= float(unfiltered[key]["orders.w"]) + 1e-9
        # The producer's population shrank: a bucket dropped or a value fell.
        assert set(filtered) != set(unfiltered) or any(
            float(filtered[k]["orders.w"]) < float(unfiltered[k]["orders.w"]) - 1e-9
            for k in filtered
        )


    async def test_stratum0_reaches_the_cross_model_producer(self, exec_engine) -> None:
        # A host-local stratum-0 filter reaches a cross-model producer via the
        # established cross-root propagation (axiom D): the grand total counts
        # only customers with >=1 app order (c1+c3 = 150), not the full 350.
        total = ModelMeasure(formula="customers.spend:sum(partition_by=[])", name="total")
        unfiltered = (await exec_engine.execute(q(
            dimensions=["customers.tier"], measures=[total],
        ))).data
        filtered = (await exec_engine.execute(q(
            dimensions=["customers.tier"], filters=["channel = 'app'"], measures=[total],
        ))).data
        assert all(float(r["orders.total"]) == pytest.approx(350.0) for r in unfiltered)
        assert filtered
        assert all(float(r["orders.total"]) == pytest.approx(150.0) for r in filtered)


class TestFieldWinsTieBreak:
    async def test_plain_dimension_filter_is_field_and_reaches_producer(self, exec_engine) -> None:
        # A plain dimension is valid as a field; axiom B types it as field, so it
        # masks host rows and the partition producer sees the row-filtered
        # population — North drops CityB (40) and its NULL-city row, leaving 30.
        by = rows_by(await exec_engine.execute(q(
            dimensions=["region", "city"],
            filters=["city != 'CityB'"],
            measures=[ModelMeasure(formula="amount:sum(partition_by=region)", name="part")],
        )), "orders.region", "orders.city")
        assert float(by[("North", "CityA")]["orders.part"]) == pytest.approx(30.0)
        assert float(by[("South", "CityC")]["orders.part"]) == pytest.approx(50.0)


class TestAttachedValueDoesNotFeedProducers:
    async def test_grand_total_ignores_the_attached_mask(self, exec_engine) -> None:
        # Filtering on the computed dim's own aggregate masks the re-aggregation,
        # but the grand-total producer still sees the unmasked population.
        by = rows_by(await exec_engine.execute(q(
            dimensions=["region", "city", {"expression": BAND35, "name": "band"}],
            filters=["amount:sum(partition_by=city) > 45"],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="amount:sum(partition_by=[])", name="grand"),
            ],
        )), "orders.region", "orders.city")
        expected_s = {k: v for k, v in CITY_TOTAL.items() if v > 45}
        assert set(by) == set(expected_s)
        for key, row in by.items():
            assert float(row["orders.s"]) == pytest.approx(expected_s[key])
            # Grand total is over ALL rows, never the masked survivors.
            assert float(row["orders.grand"]) == pytest.approx(GRAND_TOTAL)


class TestMeasureMaskPrunesCellsOnly:
    async def test_surviving_cells_keep_their_values(self, exec_engine) -> None:
        by = rows_by(await exec_engine.execute(q(
            dimensions=["region"],
            filters=["amount:sum > 55"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region")
        expected = {k: v for k, v in REGION_TOTAL.items() if v > 55}
        assert {k[0] for k in by} == set(expected)
        for key, row in by.items():
            assert float(row["orders.s"]) == pytest.approx(expected[key[0]])


class TestHiddenPositionsAddNoColumns:
    async def test_filter_only_and_order_only_add_no_columns(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            filters=["*:count > 0"],
            order=[{"column": "amount:max", "direction": "desc"}],
        ))
        assert resp.columns == ["orders.region", "orders.s"]
        for row in resp.data:
            assert set(row) == {"orders.region", "orders.s"}
