"""DEV-1910 — the association producer roots at the aggregate's home dataset,
so a home entity absent from the query population still counts in the cells its
own join path reaches. Executed oracles on SQLite + DuckDB.

Under ``to_many_handling: "associate"`` a cross-model aggregate over an
unattributable dimension aggregates over its own home rows, each once (Axioms 3,
4, 8). A dimension reached only back through the population root needs a
population row (the presence rule); a home-side dimension does not.

Spec: queries/attribution-modes — "Distinct-entity association semantics";
queries/cross-model-aggregates — "Producer filter routing".
"""

from __future__ import annotations

import pytest

from tests._dev1840_fixtures import dev1840_models
from tests._dev1841_fixtures import (
    ASSOC_BASIC_SPEND_BY_STATUS,
    assoc_q as assoc_status_q,
    dropped_filter_warnings,
    pushed_filter_infos,
)
from tests._dev1900_fixtures import make_exec_engine
from tests._dev1910_fixtures import (
    AMOUNT_BY_BAD_POP,
    AMOUNT_SUM,
    APP_SPEND_BY_BAD_POP,
    BAD_POP,
    CUST_NULL_CELL,
    DICE_SLICE_SOUTH,
    LOCAL_COUPLED_OK,
    LOCAL_DECOUPLED_OK,
    LOCAL_SPEND_BY_STATUS_NULLSEED,
    LOCAL_SPEND_SUM,
    MIXED_SOUTH_OK,
    MIXED_SOUTH_WITH_C7_BUG,
    PRESENCE_NULL_CELL,
    PRESENCE_NULL_CELL_C7_BUG,
    REGION_POP_BY_BAD_POP,
    REGION_POP_SUM,
    RENT_BY_STATUS,
    SPEND_BY_BAD_POP,
    SPEND_BY_STATUS_NULLSEED,
    SPEND_SUM,
    SPEND_WAVG_AMOUNT,
    SPEND_WAVG_AMOUNT_SOUTH,
    SPEND_WAVG_HOME,
    SPEND_WAVG_SOUTH,
    SPEND_WAVG_SOUTH_C7_DROPPED,
    STORE_RENT_SUM,
    associated_warnings,
    bad_pop_status_cells,
    bad_pop_vals,
    cust_q,
    make_null_status_engine,
    orders_q,
    status_vals,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def null_engine(request):
    async for e in make_null_status_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def weak_engine(request):
    async for e in make_exec_engine(request, models=dev1840_models(strong_plans=False)):
        yield e


class TestIssueBar:
    async def test_home_entity_absent_from_population_counts(self, engine):
        """South cross-model spend includes c7 (zero orders); the local amount
        cell is unchanged (orders home, c7 has none); the cells warn."""
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[SPEND_SUM, AMOUNT_SUM],
            to_many_handling="associate"))
        spend = bad_pop_vals(resp, "orders.csp")
        amount = bad_pop_vals(resp, "orders.amt")
        for cell, expected in SPEND_BY_BAD_POP.items():
            assert float(spend[cell]) == pytest.approx(expected), cell
        for cell, expected in AMOUNT_BY_BAD_POP.items():
            assert float(amount[cell]) == pytest.approx(expected), cell
        assert associated_warnings(resp)


class TestHostFilterHomeSideDimension:
    async def test_app_filter_couples_to_the_home_side_dimension(self, engine):
        """channel='app' restricts to app customers before the per-entity
        aggregation, by bad_pop: North 250, South 140; c7 (no app order) out."""
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[SPEND_SUM],
            filters=["channel = 'app'"], to_many_handling="associate"))
        spend = bad_pop_vals(resp, "orders.csp")
        for cell, expected in APP_SPEND_BY_BAD_POP.items():
            assert float(spend[cell]) == pytest.approx(expected), cell


class TestMixedHomeAndPopulationRootDimensions:
    async def test_c7_absent_from_every_status_cell(self, engine):
        """By (bad_pop, status): the only South cell is (230, ok) = c3+c5; c7 —
        no order, so no status — is in no cell (South total 140, not 195)."""
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP, "status"], measures=[SPEND_SUM],
            to_many_handling="associate"))
        cells = bad_pop_status_cells(resp, "orders.csp")
        south = {k: v for k, v in cells.items() if k[0] == 230.0}
        assert float(south[(230.0, "ok")]) == pytest.approx(MIXED_SOUTH_OK)
        assert sum(float(v) for v in south.values()) == pytest.approx(MIXED_SOUTH_OK)
        assert sum(float(v) for v in south.values()) != pytest.approx(
            MIXED_SOUTH_WITH_C7_BUG)


class TestPresenceGuardOnTheBackHop:
    async def test_null_status_cell_holds_only_the_owner(self, null_engine):
        """Orders-rooted: the NULL-status cell holds only c1 (owner); c7 (no
        order) is in no cell, never a manufactured NULL cell."""
        resp = await null_engine.execute(orders_q(
            dimensions=["status"], measures=[SPEND_SUM],
            to_many_handling="associate"))
        spend = status_vals(resp, "orders.csp")
        assert float(spend[None]) == pytest.approx(PRESENCE_NULL_CELL)
        assert float(spend[None]) != pytest.approx(PRESENCE_NULL_CELL_C7_BUG)
        for cell, expected in SPEND_BY_STATUS_NULLSEED.items():
            assert float(spend[cell]) == pytest.approx(expected), cell


class TestCustomersRootedTwinKeepsItsNullCell:
    async def test_orderless_entity_sits_in_the_null_cell(self, null_engine):
        """Customers-rooted (home == host): the LEFT JOIN keeps orderless c7 in
        the NULL cell alongside c1 — no presence guard when home == host."""
        resp = await null_engine.execute(cust_q(
            dimensions=["orders.status"], measures=[LOCAL_SPEND_SUM],
            to_many_handling="associate"))
        spend = status_vals(resp, "customers.sp", root="customers")
        assert float(spend[None]) == pytest.approx(CUST_NULL_CELL)
        for cell, expected in LOCAL_SPEND_BY_STATUS_NULLSEED.items():
            assert float(spend[cell]) == pytest.approx(expected), cell


class TestDiceSlice:
    async def test_slice_equals_filter_on_the_home_side_dimension(self, engine):
        """Law dice–slice (exact in associate): the 230 (South) cell of a
        bad_pop group-by equals filtering to bad_pop = 230 — 195 both ways."""
        sliced = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[SPEND_SUM],
            to_many_handling="associate"))
        slice_val = float(bad_pop_vals(sliced, "orders.csp")[230.0])
        diced = await engine.execute(orders_q(
            measures=[SPEND_SUM], filters=[f"{BAD_POP} = 230"],
            to_many_handling="associate"))
        dice_val = float(diced.data[0]["orders.csp"])
        assert slice_val == pytest.approx(DICE_SLICE_SOUTH)
        assert dice_val == pytest.approx(DICE_SLICE_SOUTH)
        assert slice_val == pytest.approx(dice_val)


class TestTwoHopHome:
    async def test_regions_homed_metric_counts_distinct_regions(self, engine):
        """A metric homed two hops away (regions) associates over distinct
        regions per bad_pop cell — one region per cell: 100 / 200."""
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[REGION_POP_SUM],
            to_many_handling="associate"))
        pop = bad_pop_vals(resp, "orders.rp")
        for cell, expected in REGION_POP_BY_BAD_POP.items():
            assert float(pop[cell]) == pytest.approx(expected), cell


class TestCompositeBackHop:
    async def test_stores_rent_by_status_over_composite_reverse_hop(self, engine):
        """A metric homed at stores (composite (co, no) key) associates over
        distinct stores per status cell across the composite reverse hop:
        ok 1100, new 800."""
        resp = await engine.execute(orders_q(
            dimensions=["status"], measures=[STORE_RENT_SUM],
            to_many_handling="associate"))
        rent = status_vals(resp, "orders.rent")
        for cell, expected in RENT_BY_STATUS.items():
            assert float(rent[cell]) == pytest.approx(expected), cell


class TestLocalSameBranchCoupling:
    async def test_filter_and_dimension_bind_to_the_same_related_row(self, engine):
        """Customers-rooted, channel='app' by orders.status: the filter and the
        dimension bind to ONE order — ok = c3+c5 = 140, never the decoupled app
        total 390."""
        resp = await engine.execute(cust_q(
            dimensions=["orders.status"], measures=[LOCAL_SPEND_SUM],
            filters=["orders.channel = 'app'"], to_many_handling="associate"))
        spend = status_vals(resp, "customers.sp", root="customers")
        assert float(spend["ok"]) == pytest.approx(LOCAL_COUPLED_OK)
        assert float(spend["ok"]) != pytest.approx(LOCAL_DECOUPLED_OK)


class TestParameterTyping:
    async def test_home_determined_weight_counts_the_orderless_customer(self, engine):
        """A weight the home determines (customers.spend) is picked once per
        distinct customer; South's weighted average includes c7."""
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[SPEND_WAVG_HOME],
            to_many_handling="associate"))
        wa = bad_pop_vals(resp, "orders.wa")
        assert float(wa[230.0]) == pytest.approx(SPEND_WAVG_SOUTH)
        assert float(wa[230.0]) != pytest.approx(SPEND_WAVG_SOUTH_C7_DROPPED)

    async def test_fanning_host_column_weight_is_picked_across_the_reverse_hop(self, engine):
        """A fanning host column weight (orders.amount) is picked once per
        customer across the hop; c7's NULL weight drops it, South = 75."""
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[SPEND_WAVG_AMOUNT],
            to_many_handling="associate"))
        wa = bad_pop_vals(resp, "orders.wa")
        assert float(wa[230.0]) == pytest.approx(SPEND_WAVG_AMOUNT_SOUTH)


class TestAssociationRestrictedConjunctEntry:
    async def test_inlined_conjunct_carries_the_semi_join_pushed_entry(self, weak_engine):
        """An unsafe-but-reachable conjunct inlined on the home-rooted joins is
        reported through the same informational entry as a semi-join-pushed
        conjunct, and the executed values are the restricted membership."""
        resp = await weak_engine.execute(assoc_status_q(
            dimensions=["status"], measures=[SPEND_SUM],
            filters=["customers.plans.level = 'basic'"]))
        spend = status_vals(resp, "orders.csp")
        for cell, expected in ASSOC_BASIC_SPEND_BY_STATUS.items():
            assert float(spend[cell]) == pytest.approx(expected), cell
        (info,) = pushed_filter_infos(resp)
        assert info.measure == "csp"
        assert "basic" in info.filter_text
        assert not dropped_filter_warnings(resp)
