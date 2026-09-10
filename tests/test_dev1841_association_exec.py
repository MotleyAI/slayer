"""DEV-1841 task 4.1 — distinct-entity association, executed values on SQLite +
DuckDB: cross-model, multi-hop, ``*:count``, median, measure-local filter,
mode-invariance of attributable slices, and cardinality neutrality.

Spec: openspec …/specs/queries/attribution-modes — "Distinct-entity association
semantics", "Association eligibility and input handling"; and
queries/semantics — "Associate mode keeps attributable dimensions exact".
"""

from __future__ import annotations

import pytest

from tests._dev1841_fixtures import (
    ASSOC_COUNT_BY_STATUS,
    ASSOC_GOLD_SPEND_BY_STATUS,
    ASSOC_MEDIAN_BY_STATUS,
    ASSOC_POP_BY_STATUS,
    ASSOC_SPEND_BY_STATUS,
    BCAST_SPEND_CROSS,
    SPEND_BY_TIER,
    ModelMeasure,
    SlayerQuery,
    assoc_q,
    associated_warnings,
    bcast_q,
    broadcast_warnings,
    dev1841_models,
    make_exec_engine,
    rows_by,
    status_key,
)

M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_ext(request):
    async for engine in make_exec_engine(request, models=dev1841_models()):
        yield request.param, engine


class TestCrossModelAssociation:
    async def test_per_cell_distinct_customers(self, exec_backend):
        """Scenario: cross-model metric attributes per cell by executed values."""
        _, engine = exec_backend
        resp = await engine.execute(
            assoc_q(dimensions=["status"], measures=[CM]))
        by = status_key(resp)
        assert set(by) == {("ok",), ("new",)}  # grain unchanged
        for status, spend in ASSOC_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.cm"]) == pytest.approx(spend)

    async def test_differs_from_broadcast_default(self, exec_backend):
        """The default broadcasts the same value to every cell; associate does
        not — the two modes are observably different on this shape."""
        _, engine = exec_backend
        resp = await engine.execute(bcast_q(dimensions=["status"], measures=[CM]))
        by = status_key(resp)
        for cell in by.values():
            assert float(cell["orders.cm"]) == pytest.approx(BCAST_SPEND_CROSS)

    async def test_star_count_counts_distinct_entities(self, exec_backend):
        """Scenario: star-count counts distinct associated entities."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.*:count", name="nc")]))
        by = status_key(resp)
        for status, n in ASSOC_COUNT_BY_STATUS.items():
            assert int(by[(status,)]["orders.nc"]) == n

    async def test_percentile_family_over_the_association(self, exec_backend):
        """Scenario: percentile attributes over the association — median over
        odd-sized populations is the exact middle element on both engines."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:median", name="med")]))
        by = status_key(resp)
        for status, med in ASSOC_MEDIAN_BY_STATUS.items():
            assert float(by[(status,)]["orders.med"]) == pytest.approx(med)

    async def test_multi_hop_association(self, exec_backend):
        """Scenario: multi-hop association attributes per cell — the metric root
        (regions) is two hops from the host."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.regions.pop:sum",
                                   name="pop")]))
        by = status_key(resp)
        for status, pop in ASSOC_POP_BY_STATUS.items():
            assert float(by[(status,)]["orders.pop"]) == pytest.approx(pop)

    #: The plain scalar family beyond sum/count/median — avg/min/max over each
    #: cell's distinct customer population.
    @pytest.mark.parametrize("agg,expected", [
        ("avg", {"ok": 84.0, "new": 290.0 / 3.0}),
        ("min", {"ok": 30.0, "new": 40.0}),
        ("max", {"ok": 150.0, "new": 150.0}),
    ])
    async def test_scalar_family_over_the_association(self, exec_backend, agg, expected):
        """Requirement: the full plain scalar family resolves over the distinct
        association, each input evaluated per entity."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=f"customers.spend:{agg}", name="v")]))
        by = status_key(resp)
        for status, val in expected.items():
            assert float(by[(status,)]["orders.v"]) == pytest.approx(val)


class TestMixedGrain:
    async def test_attributable_partitions_unattributable_associates(
        self, exec_backend,
    ):
        """Scenario: associate mode keeps attributable dimensions exact — in a
        query mixing a customer-determined dimension (tier) and an orders-level
        one (status), tier partitions exactly while status carries per-cell
        distinct values."""
        _, engine = exec_backend
        assoc = await engine.execute(assoc_q(
            dimensions=["customers.tier", "status"], measures=[CM]))
        by = rows_by(assoc, "orders.customers.tier", "orders.status")
        assert float(by[("gold", "ok")]["orders.cm"]) == pytest.approx(190.0)
        assert float(by[("gold", "new")]["orders.cm"]) == pytest.approx(100.0)
        assert float(by[("silver", "ok")]["orders.cm"]) == pytest.approx(230.0)
        assert float(by[("silver", "new")]["orders.cm"]) == pytest.approx(150.0)

    async def test_broadcast_repeats_across_the_unattributable_axis(
        self, exec_backend,
    ):
        """Scenario: attributable dimension partitions, unattributable
        broadcasts — under the default, each tier's value repeats across
        status."""
        _, engine = exec_backend
        resp = await engine.execute(bcast_q(
            dimensions=["customers.tier", "status"], measures=[CM]))
        by = rows_by(resp, "orders.customers.tier", "orders.status")
        assert float(by[("gold", "ok")]["orders.cm"]) == pytest.approx(
            float(by[("gold", "new")]["orders.cm"]))
        assert float(by[("silver", "ok")]["orders.cm"]) == pytest.approx(
            float(by[("silver", "new")]["orders.cm"]))


class TestMeasureLocalFilter:
    async def test_filter_restricts_the_association(self, exec_backend_ext):
        """Scenario: measure-local filter restricts the association — the gold
        filter on the aggregate's own column limits the associated entities;
        result cardinality is unchanged."""
        _, engine = exec_backend_ext
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.gold_spend:sum",
                                   name="gs")]))
        by = status_key(resp)
        assert set(by) == {("ok",), ("new",)}
        for status, spend in ASSOC_GOLD_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.gs"]) == pytest.approx(spend)


class TestModeInvarianceOfAttributable:
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    async def test_attributable_slice_is_mode_invariant(self, exec_backend, mode):
        """Scenario: fully attributable queries are mode-invariant — a
        customer-determined slice is identical across all three modes."""
        _, engine = exec_backend
        resp = await engine.execute(SlayerQuery(
            source_model="orders", dimensions=["customers.tier"],
            measures=[CM], to_many_handling=mode))
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in SPEND_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend)
        # Fully attributable: no mode-related warning in any mode.
        assert broadcast_warnings(resp) == []
        assert associated_warnings(resp) == []


class TestCardinalityNeutral:
    async def test_adding_associate_measure_keeps_rows_and_siblings(
        self, exec_backend,
    ):
        """Scenario: adding an associated measure is cardinality-neutral."""
        _, engine = exec_backend
        without = await engine.execute(
            assoc_q(dimensions=["status"], measures=[M]))
        with_cm = await engine.execute(
            assoc_q(dimensions=["status"], measures=[M, CM]))
        a = status_key(without)
        b = status_key(with_cm)
        assert set(a) == set(b)
        for key in a:
            assert a[key]["orders.m"] == b[key]["orders.m"]
