"""DEV-1836 task 1.11 — the total-routing invariant (design D7, F9).

Spec: openspec …/specs/queries/cross-model-aggregates — "Every aggregate has
exactly one disposition"; …/specs/queries/computed-dimensions — "Grain
self-containment error surface", "Coexistence deferrals after cross-model
unification". A shape the planner cannot route fails with a clear planner
error — never a silently dropped or wrong value, never leaked internals.
"""

from __future__ import annotations

import pytest

import slayer.engine.regroup_planner as regroup_planner
import slayer.engine.stage_planner as stage_planner
from slayer.core.errors import SlayerError
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import SQLGenerator

from tests._dev1836_fixtures import (
    ModelMeasure,
    dev1836_models,
    make_exec_engine,
    q,
)

LOCAL_BAND = {
    "expression": "CASE WHEN amount:sum(partition_by=channel) > 30 THEN 1 ELSE 0 END",
    "name": "band",
}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _bundle() -> ResolvedSourceBundle:
    models = dev1836_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=list(models[1:]),
    )


class TestExplicitRejections:
    async def test_bare_aggregate_in_dimension_is_rejected(self, exec_backend):
        _, engine = exec_backend
        query = q(
            dimensions=[{"expression": "amount:sum", "name": "d"}],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        )
        with pytest.raises(Exception, match="partition_by") as ei:  # noqa: B017 — message is the pin
            await engine.execute(query)
        assert not isinstance(ei.value, AssertionError)
        assert "__regroup__" not in str(ei.value)

    async def test_aggregate_over_attached_value_is_rejected(self, exec_backend):
        """Still excluded (D4/F3): aggregating over an attached aggregate
        value is a clear not-yet-supported error, not an internal one."""
        _, engine = exec_backend
        query = q(
            dimensions=[LOCAL_BAND],
            measures=[ModelMeasure(formula="amount:sum(partition_by=band)",
                                   name="x")],
        )
        with pytest.raises(NotImplementedError) as ei:
            await engine.execute(query)
        message = str(ei.value)
        assert "band" in message
        assert "not yet" in message or "not supported" in message
        assert "__regroup__" not in message

    def test_migrated_cm_still_renders_in_a_cte_body(self):
        """A plain cross-model measure renders inside a CTE body today; the
        migration onto the primitive must not drop it into the DEV-1838 arm."""
        bundle = _bundle()
        planned = plan_query(query=q(
            dimensions=["customers.tier"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="customers.spend:sum", name="cm"),
            ],
        ), bundle=bundle)
        generator = SQLGenerator(dialect="postgres")
        sql = generator.generate_from_planned(planned, bundle=bundle,
                                              as_cte_body=True)
        assert sql
        assert "__regroup__" not in sql


class TestTotalRoutingInvariant:
    """F9/D7 — a simulated discovery gap surfaces as an explicit planner
    error, never as a silently dropped value."""

    def test_unrouted_aggregate_raises_explicit_planner_error(self, monkeypatch):
        """Blind the combined-producer discovery to every partitioned leaf: the
        post-discovery invariant must catch the now-undisposed aggregate."""
        def _blind(*args, **kwargs):
            return [], {}

        for mod in (regroup_planner, stage_planner):
            if hasattr(mod, "combined_partitioned_aggregates"):
                monkeypatch.setattr(
                    mod, "combined_partitioned_aggregates", _blind,
                )
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum(partition_by=channel)",
                                   name="pt")],
        )
        bundle = _bundle()
        # D7: a raise, not an assert — AssertionError must not be the vehicle.
        with pytest.raises(
            (SlayerError, ValueError, NotImplementedError, RuntimeError),
        ) as ei:
            plan_query(query=query, bundle=bundle)
        assert not isinstance(ei.value, AssertionError)
        message = str(ei.value).lower()
        assert "aggregat" in message or "pt" in message, message

    def test_blinded_cross_model_discovery_hits_the_dedicated_invariant(
        self, monkeypatch,
    ):
        """Task 7.1 — the D7 invariant itself, not a coincidental backstop.
        Blind the CROSS-MODEL discovery: the un-desugared cross-model aggregate
        must be caught by ``_assert_total_routing`` with the explicit
        no-disposition error, not fall through to the legacy dispatch."""
        monkeypatch.setattr(
            stage_planner, "_discover_cross_model_combined",
            lambda prebound: ([], {}, {}),
        )
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="cm")],
        )
        bundle = _bundle()
        with pytest.raises(ValueError, match="no routing disposition"):
            plan_query(query=query, bundle=bundle)

    @pytest.mark.parametrize("role_kwargs", [
        pytest.param(
            {"filters": ["customers.spend:sum > 100"]}, id="filter-only",
        ),
        pytest.param(
            {"order": [{"column": "customers.spend:sum", "direction": "desc"}]},
            id="order-only",
        ),
    ])
    def test_blinded_discovery_catches_filter_and_order_leaves(
        self, monkeypatch, role_kwargs,
    ):
        """The invariant walks filters and orders too — a hidden cross-model
        leaf in either role must not survive blinded discovery."""
        monkeypatch.setattr(
            stage_planner, "_discover_cross_model_combined",
            lambda prebound: ([], {}, {}),
        )
        query = q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            **role_kwargs,
        )
        bundle = _bundle()
        with pytest.raises(ValueError, match="no routing disposition"):
            plan_query(query=query, bundle=bundle)


class TestNoSilentDrops:
    async def test_every_requested_measure_lands_in_the_result(self, exec_backend):
        """A discovery gap must never surface as a missing column."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="customers.spend:sum", name="cm"),
                ModelMeasure(formula="customers.spend:sum / amount:sum",
                             name="ratio"),
            ],
        ))
        for row in resp.data:
            for col in ("orders.m", "orders.cm", "orders.ratio"):
                assert col in row, col
