"""DEV-1836 task 1.7 — producer filter inheritance (design D3), reproducing the
``classify_host_filter`` decision table as behavior.

Spec: openspec …/specs/queries/cross-model-aggregates — "Producer filter
inheritance". Safe ROW conjuncts apply inside the producer (re-rooted);
DEV-1840 upgrades the unsafe-but-reachable class from drop+warn to semi-join
pushdown (the two pins below are updated with consent); AGGREGATE-phase
routing is unchanged.
"""

from __future__ import annotations

import warnings as _warnings

import pytest

from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1836_fixtures import (
    AMOUNT_BY_TIER,
    ModelMeasure,
    SPEND_BY_TIER,
    dropped_filter_warnings,
    make_exec_engine,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")


class TestAttributableRowFilters:
    async def test_target_level_filter_restricts_the_metric(self, exec_backend):
        """A conjunct attributable from the root applies inside the producer
        AND to the result rows — no warning."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["status"], measures=[M, CM],
            filters=["customers.tier = 'gold'"],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.status")
        assert set(by) == {("ok",), ("new",)}
        for row in by.values():
            # Gold customers only: c1 + c3 spend.
            assert float(row["orders.cm"]) == pytest.approx(SPEND_BY_TIER["gold"])
        assert float(by[("ok",)]["orders.m"]) == pytest.approx(20.0)
        assert float(by[("new",)]["orders.m"]) == pytest.approx(20.0)
        assert dropped_filter_warnings(resp) == []
        dry = await engine.execute(query, dry_run=True)
        assert_scope_closed(dry.sql, dialect=dialect)


class TestUnsafeRowFilters:
    async def test_host_filter_pushes_down_by_semi_join(self, exec_backend):
        """DEV-1836 class-(c), upgraded by DEV-1840: the host-level conjunct
        crosses the declared 1:N edge, so it restricts the producer by
        semi-join — customers with at least one web order, spend counted
        once, silently. Old drop+warn gave gold the whole 160 slice."""
        _, engine = exec_backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(q(
                dimensions=["customers.tier"], measures=[M, CM],
                filters=["channel = 'web'"],
            ))
        by = rows_by(resp, "orders.customers.tier")
        # Spine: web orders → o1 (c1), o3 (c2), o6 (c4), o7 (orphan).
        assert set(by) == {("gold",), ("silver",), ("bronze",), (None,)}
        # Web customers: c1 (gold 100), c2 (silver 150), c4 (bronze 40);
        # c3 (gold, app-only) is out — NOT the unfiltered 160 gold slice.
        assert float(by[("gold",)]["orders.cm"]) == pytest.approx(100.0)
        assert float(by[("silver",)]["orders.cm"]) == pytest.approx(150.0)
        assert float(by[("bronze",)]["orders.cm"]) == pytest.approx(40.0)
        assert by[(None,)]["orders.cm"] is None
        assert float(by[("gold",)]["orders.m"]) == pytest.approx(10.0)
        assert dropped_filter_warnings(resp) == []
        hits = [c for c in caught
                if issubclass(c.category, UnreachableFilterDroppedWarning)]
        assert hits == []

    async def test_result_rows_still_honor_the_pushed_filter(self, exec_backend):
        """Pushed into the producer AND kept on the spine: the predicate
        still restricts the result rows."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M, CM],
            filters=["channel = 'app'"],
        ))
        by = rows_by(resp, "orders.status")
        # App orders: o2 (new), o4/o5 (ok).
        assert set(by) == {("ok",), ("new",)}
        assert float(by[("ok",)]["orders.m"]) == pytest.approx(10.0)
        assert float(by[("new",)]["orders.m"]) == pytest.approx(20.0)
        # App customers c1 + c3 — no longer the unfiltered SPEND_TOTAL.
        for row in resp.data:
            assert float(row["orders.cm"]) == pytest.approx(160.0)


class TestAggregatePhaseRouting:
    async def test_cross_model_aggregate_filter_routes_to_having(self, exec_backend):
        """AGGREGATE-phase routing unchanged: an on-target aggregate predicate
        filters the groups, not the producer's rows."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"], measures=[M, CM],
            filters=["customers.spend:sum > 100"],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        assert float(by[("gold",)]["orders.cm"]) == pytest.approx(160.0)
        assert float(by[("silver",)]["orders.cm"]) == pytest.approx(150.0)
        assert float(by[("gold",)]["orders.m"]) == pytest.approx(
            AMOUNT_BY_TIER["gold"],
        )
        assert float(by[("silver",)]["orders.m"]) == pytest.approx(
            AMOUNT_BY_TIER["silver"],
        )
        dry = await engine.execute(query, dry_run=True)
        assert_scope_closed(dry.sql, dialect=dialect)

    async def test_local_aggregate_filter_outer_where_unchanged(self, exec_backend):
        """Regression pin: local aggregate filters keep their outer routing."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M],
            filters=["amount:sum > 30"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("bronze",)}
        assert float(by[("gold",)]["orders.m"]) == pytest.approx(40.0)
        assert float(by[("bronze",)]["orders.m"]) == pytest.approx(40.0)
