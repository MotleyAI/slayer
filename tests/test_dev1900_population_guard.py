"""DEV-1900 → DEV-1909 — the retired population-filter guard.

The DEV-1900 interim guard raised whenever a fanning population filter met an
aggregate inline over the population. DEV-1909 retires it: the fanning arm now
restricts the population by association (structural 420, derived 120), while the
unanalyzable arm still fails closed through the new checker. To-one filters,
dimension-only and producer-only queries stay unaffected. The comprehensive
suite is ``tests/test_dev1909_population_pushdown.py``; these are the retired
guard's own cases, re-pointed.

Covers ``queries/semantics`` (Filters restrict by association or fail loudly).
"""

from __future__ import annotations

import pytest

from tests._dev1892_fixtures import assert_ref_free
from tests._dev1900_fixtures import (
    AMOUNT_SUM,
    ModelMeasure,
    POP_FILTER_DERIVED_ASSOC,
    POP_FILTER_STRUCTURAL_ASSOC,
    TO_ONE_FILTER_AMOUNT,
    cust_q,
    make_exec_engine,
    orders_q,
    unparseable_derived_models,
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


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for e in make_exec_engine(request, models=unparseable_derived_models()):
        yield e


class TestFanningPopulationFilterRestrictsByAssociation:
    @pytest.mark.parametrize("mode", MODES)
    async def test_structural_fanning_filter(self, engine, mode):
        """Rooted at customers, filtered on orders.status across the 1:N
        customers→orders hop, with the local spend:sum: 420, each once."""
        resp = await engine.execute(cust_q(
            measures=[SPEND_SUM_LOCAL], filters=["orders.status = 'ok'"],
            to_many_handling=mode))
        assert float(resp.data[0]["customers.w"]) == pytest.approx(
            POP_FILTER_STRUCTURAL_ASSOC)

    @pytest.mark.parametrize("mode", MODES)
    async def test_derived_fanning_filter(self, engine, mode):
        """Rooted at orders, filtered on the derived regions.bad_pop across the
        1:N regions→region_events hop, with the local amount:sum: 120."""
        resp = await engine.execute(orders_q(
            measures=[AMOUNT_SUM], filters=["customers.regions.bad_pop > 0"],
            to_many_handling=mode))
        assert float(resp.data[0]["orders.amt"]) == pytest.approx(
            POP_FILTER_DERIVED_ASSOC)

    @pytest.mark.parametrize("mode", MODES)
    async def test_aggregate_only_in_filter_restricts(self, engine, mode):
        """The population aggregate lives only in a measure predicate
        (``spend:sum > 1``): the fanning row filter still restricts by
        association, so tiers gold and silver survive without fanning."""
        resp = await engine.execute(cust_q(
            dimensions=["tier"],
            filters=["orders.status = 'ok'", "spend:sum > 1"],
            to_many_handling=mode))
        assert {r["customers.tier"] for r in resp.data} == {"gold", "silver"}


class TestUnanalyzableStillFailsClosed:
    @pytest.mark.parametrize("mode", MODES)
    async def test_unanalyzable_filter_fails_closed(self, unparse_engine, mode):
        """A filter on a derived column no dialect can analyse fails closed with a
        typed error, never routed as if it crossed nothing."""
        q = orders_q(
            measures=[AMOUNT_SUM], filters=["customers.regions.unparseable > 0"],
            to_many_handling=mode)
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(q)
        msg = str(ei.value)
        assert "analyse" in msg, msg
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
