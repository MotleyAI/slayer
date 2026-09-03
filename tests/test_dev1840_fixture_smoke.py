"""DEV-1840 task 1.1 — the fixture graph is healthy on today's engine.

Pins only shapes whose behavior DEV-1840 does not change: unfiltered
cross-model aggregates, a provably-safe inline filter, and the planner entry
point. Everything here must pass before AND after the implementation.
"""

from __future__ import annotations

import pytest

from slayer.engine.stage_planner import plan_query

from tests._dev1840_fixtures import (
    ModelMeasure,
    SPEND_ALL_BY_TIER,
    SPEND_BASIC_BY_TIER,
    ambiguity_models,
    bundle,
    make_exec_engine,
    q,
    rows_by,
    tq,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_amb(request):
    async for engine in make_exec_engine(request, models=ambiguity_models()):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")


class TestGraphExecutes:
    async def test_unfiltered_cross_model_by_tier(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(dimensions=["customers.tier"],
                                      measures=[M, CM]))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",), ("bronze",), (None,)}
        for tier, spend in SPEND_ALL_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend)
        assert by[(None,)]["orders.cm"] is None
        assert float(by[("gold",)]["orders.m"]) == pytest.approx(50.0)
        assert float(by[("silver",)]["orders.m"]) == pytest.approx(70.0)

    async def test_provably_safe_filter_inlines_today(self, exec_backend):
        """strong plans: the m:1 hop is proven, the filter applies inline."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.plans.level = 'basic'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier, spend in SPEND_BASIC_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend)
        assert resp.warnings in (None, [])

    async def test_ambiguity_graph_executes(self, exec_backend_amb):
        _, engine = exec_backend_amb
        resp = await engine.execute(
            tq(measures=[ModelMeasure(formula="agents.score:sum", name="sm")]),
        )
        assert float(resp.data[0]["tickets.sm"]) == pytest.approx(60.0)


def test_planner_entry_point_builds_the_producer() -> None:
    planned = plan_query(
        query=q(dimensions=["customers.tier"], measures=[CM],
                filters=["channel = 'app'"]),
        bundle=bundle(),
    )
    assert len(planned.regroup_attach_plans) == 1
    assert planned.regroup_attach_plans[0].producer_root_model == "customers"
