"""DEV-1900 — interim population-filter guard.

Until association pushdown reaches the population itself (DEV-1909), a row-level
filter conjunct that reaches the population root only across a fanning hop, in a
query that keeps at least one aggregate inline over the population rows, fails
with a typed error instead of the silently multiplied total — structural and
derived crossings alike, in every mode. A to-one filter stays inline; a
dimension-only or producer-only query is unaffected.

Covers ``queries/semantics`` (Filters restrict by association or fail loudly).
"""

from __future__ import annotations

import pytest

from tests._dev1892_fixtures import assert_ref_free
from tests._dev1900_fixtures import (
    AMOUNT_SUM,
    ModelMeasure,
    TO_ONE_FILTER_AMOUNT,
    cust_q,
    make_exec_engine,
    orders_q,
)

MODES = ["broadcast", "associate", "error"]
SPEND_SUM_LOCAL = ModelMeasure(formula="spend:sum", name="w")   # local on customers
ORDERS_AMOUNT_PRODUCER = ModelMeasure(formula="orders.amount:sum", name="oa")  # cross-model producer

#: producer-only orders.amount:sum over ok orders = 82 (EXISTS pushdown, no guard).
PRODUCER_ONLY_AMOUNT = 82.0


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


class TestFanningPopulationFilterFailsClosed:
    @pytest.mark.parametrize("mode", MODES)
    async def test_structural_fanning_filter(self, engine, mode):
        """Rooted at customers, filtered on orders.status across the 1:N
        customers→orders hop, with the local spend:sum inline."""
        with pytest.raises(ValueError) as ei:
            await engine.execute(cust_q(
                measures=[SPEND_SUM_LOCAL], filters=["orders.status = 'ok'"],
                to_many_handling=mode))
        msg = str(ei.value)
        assert "status" in msg and "orders" in msg, msg
        assert_ref_free(msg)

    @pytest.mark.parametrize("mode", MODES)
    async def test_derived_fanning_filter(self, engine, mode):
        """Rooted at orders, filtered on the derived regions.bad_pop across the
        1:N regions→region_events hop, with the local amount:sum inline."""
        with pytest.raises(ValueError) as ei:
            await engine.execute(orders_q(
                measures=[AMOUNT_SUM], filters=["customers.regions.bad_pop > 0"],
                to_many_handling=mode))
        msg = str(ei.value)
        assert "region_events" in msg and "bad_pop" in msg, msg
        assert_ref_free(msg)


class TestToOneFilterStaysInline:
    async def test_to_one_filter_executes(self, engine):
        resp = await engine.execute(orders_q(
            measures=[AMOUNT_SUM], filters=["customers.tier = 'gold'"]))
        assert float(resp.data[0]["orders.amt"]) == pytest.approx(TO_ONE_FILTER_AMOUNT)


class TestUnaffectedShapes:
    async def test_dimension_only_query_unaffected(self, engine):
        resp = await engine.execute(cust_q(
            dimensions=["tier"], filters=["orders.status = 'ok'"]))
        assert {r["customers.tier"] for r in resp.data} == {"gold", "silver"}

    async def test_producer_only_query_unaffected(self, engine):
        resp = await engine.execute(cust_q(
            measures=[ORDERS_AMOUNT_PRODUCER], filters=["orders.status = 'ok'"]))
        assert float(resp.data[0]["customers.oa"]) == pytest.approx(PRODUCER_ONLY_AMOUNT)
