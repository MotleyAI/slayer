"""Distinct-entity association, executed values on SQLite + DuckDB: cross-model,
multi-hop, ``count(*)``, median, measure-local filter, mode-invariance of
attributable slices, and cardinality neutrality.

Spec: queries/attribution-modes — "Distinct-entity association over the virtual
model", "Association eligibility, input handling and row identity"; and
queries/semantics — "Associate mode keeps attributable dimensions exact".
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1841_fixtures import (
    ASSOC_COUNT_BY_STATUS,
    ASSOC_GOLD_SPEND_BY_STATUS,
    ASSOC_MEDIAN_BY_STATUS,
    ASSOC_POP_BY_STATUS,
    ASSOC_SPEND_BY_STATUS,
    BCAST_SPEND_CROSS,
    SPEND_BY_TIER,
    SlayerQuery,
    assoc_q,
    associated_warnings,
    bcast_q,
    broadcast_warnings,
    dev1841_datasets,
    make_exec_engine,
    rows_by,
    status_key,
)

M = QueryMetric(metric="sum(orders.amount)", alias="m")
CM = QueryMetric(metric="sum(customers.spend)", alias="cm")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_ext(request):
    async for engine in make_exec_engine(request, datasets=dev1841_datasets()):
        yield request.param, engine


class TestCrossModelAssociation:
    async def test_per_cell_distinct_customers(self, exec_backend):
        """Scenario: cross-model metric attributes per cell by executed values."""
        _, engine = exec_backend
        resp = await engine.execute(
            assoc_q(dimensions=["status"], metrics=[CM]))
        by = status_key(resp)
        assert set(by) == {("ok",), ("new",)}  # grain unchanged
        for status, spend in ASSOC_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["cm"]) == pytest.approx(spend)

    async def test_differs_from_broadcast_default(self, exec_backend):
        """The default broadcasts the same value to every cell; associate does
        not — the two modes are observably different on this shape."""
        _, engine = exec_backend
        resp = await engine.execute(bcast_q(dimensions=["status"], metrics=[CM]))
        by = status_key(resp)
        for cell in by.values():
            assert float(cell["cm"]) == pytest.approx(BCAST_SPEND_CROSS)

    async def test_star_count_counts_distinct_entities(self, exec_backend):
        """Scenario: star-count counts distinct associated entities."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="count(customers.*)", alias="nc")]))
        by = status_key(resp)
        for status, n in ASSOC_COUNT_BY_STATUS.items():
            assert int(by[(status,)]["nc"]) == n

    async def test_filtered_count_over_association_counts_matching_entities(
        self, exec_backend_ext,
    ):
        """A non-star ``count`` over a filtered association column counts
        only the entities whose masked value is non-NULL (COUNT(_v)), NOT every
        associated entity (COUNT(*)). ``gold_spend`` masks ``spend`` to gold tier,
        so ``count(gold_spend)`` = distinct GOLD customers/cell — a strict subset of
        ``count(customers.*)`` (ok 5 / new 3). The pre-fix COUNT(*) would return the
        full population."""
        _, engine = exec_backend_ext
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="count(customers.gold_spend)", alias="gc")]))
        by = status_key(resp)
        # gold (c1,c3,c6) with an ok order: c1,c3,c6 → 3; with a new order: c1 → 1.
        for status, n in {"ok": 3, "new": 1}.items():
            assert int(by[(status,)]["gc"]) == n

    async def test_percentile_family_over_the_association(self, exec_backend):
        """Scenario: percentile attributes over the association — median over
        odd-sized populations is the exact middle element on both engines."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="median(customers.spend)", alias="med")]))
        by = status_key(resp)
        for status, med in ASSOC_MEDIAN_BY_STATUS.items():
            assert float(by[(status,)]["med"]) == pytest.approx(med)

    async def test_multi_hop_association(self, exec_backend):
        """Scenario: multi-hop association attributes per cell — the metric root
        (regions) is two hops from the host."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="sum(customers.regions.pop)",
                                   alias="pop")]))
        by = status_key(resp)
        for status, pop in ASSOC_POP_BY_STATUS.items():
            assert float(by[(status,)]["pop"]) == pytest.approx(pop)

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
            metrics=[QueryMetric(metric=f"{agg}(customers.spend)", alias="v")]))
        by = status_key(resp)
        for status, val in expected.items():
            assert float(by[(status,)]["v"]) == pytest.approx(val)


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
            dimensions=["customers.tier", "status"], metrics=[CM]))
        by = rows_by(assoc, "customers.tier", "orders.status")
        assert float(by[("gold", "ok")]["cm"]) == pytest.approx(190.0)
        assert float(by[("gold", "new")]["cm"]) == pytest.approx(100.0)
        assert float(by[("silver", "ok")]["cm"]) == pytest.approx(230.0)
        assert float(by[("silver", "new")]["cm"]) == pytest.approx(150.0)

    async def test_broadcast_repeats_across_the_unattributable_axis(
        self, exec_backend,
    ):
        """Scenario: attributable dimension partitions, unattributable
        broadcasts — under the default, each tier's value repeats across
        status."""
        _, engine = exec_backend
        resp = await engine.execute(bcast_q(
            dimensions=["customers.tier", "status"], metrics=[CM]))
        by = rows_by(resp, "customers.tier", "orders.status")
        assert float(by[("gold", "ok")]["cm"]) == pytest.approx(
            float(by[("gold", "new")]["cm"]))
        assert float(by[("silver", "ok")]["cm"]) == pytest.approx(
            float(by[("silver", "new")]["cm"]))


class TestMeasureLocalFilter:
    async def test_filter_restricts_the_association(self, exec_backend_ext):
        """Scenario: measure-local filter restricts the association — the gold
        filter on the aggregate's own column limits the associated entities;
        result cardinality is unchanged."""
        _, engine = exec_backend_ext
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            metrics=[QueryMetric(metric="sum(customers.gold_spend)",
                                   alias="gs")]))
        by = status_key(resp)
        assert set(by) == {("ok",), ("new",)}
        for status, spend in ASSOC_GOLD_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["gs"]) == pytest.approx(spend)


class TestModeInvarianceOfAttributable:
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    async def test_attributable_slice_is_mode_invariant(self, exec_backend, mode):
        """Scenario: fully attributable queries are mode-invariant — a
        customer-determined slice is identical across all three modes."""
        _, engine = exec_backend
        resp = await engine.execute(SlayerQuery(
            population="orders", dimensions=["customers.tier"],
            metrics=[CM], to_many_handling=mode))
        by = rows_by(resp, "customers.tier")
        for tier, spend in SPEND_BY_TIER.items():
            assert float(by[(tier,)]["cm"]) == pytest.approx(spend)
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
            assoc_q(dimensions=["status"], metrics=[M]))
        with_cm = await engine.execute(
            assoc_q(dimensions=["status"], metrics=[M, CM]))
        a = status_key(without)
        b = status_key(with_cm)
        assert set(a) == set(b)
        for key in a:
            assert a[key]["m"] == b[key]["m"]
