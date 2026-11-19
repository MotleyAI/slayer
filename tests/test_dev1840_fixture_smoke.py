"""The fixture graph is healthy on today's engine.

Pins only shapes whose behavior the implementation does not change: unfiltered
cross-dataset aggregates, a provably-safe inline filter, and the planner entry
point. Everything here must pass before AND after the implementation.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import AmbiguousRelationshipPathError
from slayer.engine.plan import plan_query

from slayer.core.query import QueryMetric

from tests._dev1840_fixtures import (
    SPEND_ALL_BY_TIER,
    SPEND_BASIC_BY_TIER,
    ambiguity_datasets,
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
    async for engine in make_exec_engine(request, datasets=ambiguity_datasets()):
        yield request.param, engine


M = QueryMetric(metric="sum(amount)", alias="m")
CM = QueryMetric(metric="sum(customers.spend)", alias="cm")


class TestGraphExecutes:
    async def test_unfiltered_cross_model_by_tier(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(dimensions=["customers.tier"],
                                      metrics=[M, CM]))
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
            dimensions=["customers.tier"], metrics=[CM],
            filter=["customers.plans.level = 'basic'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        for tier, spend in SPEND_BASIC_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend)
        assert resp.warnings in (None, [])

    async def test_ambiguity_graph_executes(self, exec_backend_amb):
        """The healthy leg (tickets→reviews) executes; the ambiguous pair
        fails closed (divergences.md class (d))."""
        _, engine = exec_backend_amb
        resp = await engine.execute(
            tq(metrics=[QueryMetric(metric="sum(reviews.stars)", alias="rv")]),
        )
        assert float(resp.data[0]["tickets.rv"]) == pytest.approx(11.0)
        ambiguous = tq(
            metrics=[QueryMetric(metric="sum(agents.score)", alias="sm")],
        )
        with pytest.raises(AmbiguousRelationshipPathError):
            await engine.execute(ambiguous)


def test_planner_entry_point_builds_the_producer() -> None:
    planned = plan_query(
        query=q(dimensions=["customers.tier"], metrics=[CM],
                filter=["channel = 'app'"]),
        bundle=bundle(),
    )
    assert len(planned.regroup_attach_plans) == 1
    assert planned.regroup_attach_plans[0].producer_root_dataset == "customers"
