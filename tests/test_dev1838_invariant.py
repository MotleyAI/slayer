"""DEV-1838 design D8 — no plan without a root survives planning.

The sweep pin proved the target-rooted legacy arms dead before task 2.5
deleted them (with the plan type gone, it degenerates to "no cross-model
aggregate plans at all" and stays green as the lasting pin). The transition
pin for the deleted ``assert_no_unrooted_cross_model_plans`` checker retired
in the same commit as the checker."""

from __future__ import annotations

import pytest

from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._dev1838_fixtures import (
    BAND,
    ColumnRef,
    ModelMeasure,
    OrderItem,
    dev1838_models,
    month_td,
    q,
)

M = ModelMeasure(formula="amount:sum", name="m")


def _bundle() -> ResolvedSourceBundle:
    models = dev1838_models()
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=list(models[1:]),
    )


def _walk_cmas(planned, out) -> None:
    # getattr-guarded: task 2.5 deleted the CMA field, so the sweep
    # degenerates to "no cross-model aggregate plans at all" and stays green.
    for plan in getattr(planned, "cross_model_aggregate_plans", None) or ():
        out.append(plan)
        if plan.rerooted_plan is not None:
            _walk_cmas(plan.rerooted_plan, out)
    for attach in planned.regroup_attach_plans:
        _walk_cmas(attach.producer_plan, out)


SHAPES = {
    "cross_model_sum": q(dimensions=["status"], measures=[
        M, ModelMeasure(formula="customers.spend:sum", name="cm")]),
    "cross_model_last": q(dimensions=["status"], measures=[
        ModelMeasure(formula="customers.spend:last", name="sl")]),
    "filtered_local": q(dimensions=["status"], measures=[
        M, ModelMeasure(formula="gold_amount:sum", name="g")]),
    "crossing_arg": q(dimensions=["status"], measures=[
        M, ModelMeasure(formula="amount:wscaled_sum", name="w")]),
    "host_grain_wrap": q(dimensions=["status"], measures=[M],
                         order=[OrderItem(
                             column=ColumnRef(name="tier", model="customers"),
                             direction="asc")]),
    "windowed": q(dimensions=["region"], time_dimensions=month_td(),
                  measures=[ModelMeasure(formula="amount:sum(window='90d')",
                                         name="w")]),
    "band_cross_model": q(dimensions=["status", BAND], measures=[
        M, ModelMeasure(formula="customers.spend:sum", name="cm")]),
}


class TestNoUnrootedCrossModelPlan:
    @pytest.mark.parametrize("shape", sorted(SHAPES))
    def test_every_surviving_plan_is_host_rooted(self, shape: str) -> None:
        planned = plan_query(query=SHAPES[shape], bundle=_bundle())
        cmas = []
        _walk_cmas(planned, cmas)
        unrooted = [p for p in cmas if p.cte_root_model is None]
        assert not unrooted, [
            (p.target_model, p.aggregate_slot_id) for p in unrooted
        ]
