"""DEV-1750 / DEV-1836 — where the cross-model work for a ``time_shift`` inner
aggregate lives, pinned at the planner level (Codex test-review F1).

A TARGET-GRAIN cross-model inner is migrated onto the regroup primitive by
DEV-1836 — it becomes a ``RegroupAttachPlan`` and owns NO
``CrossModelAggregatePlan`` — while a host-rooted inner (``amount:wscaled_sum``)
still isolates into a ``CrossModelAggregatePlan`` (``cte_root_model`` = the host
model). These tests pin that structural split, so a regression that reroutes
either shape is caught here rather than only by an executed-value diff.
"""

from __future__ import annotations

from slayer.core.keys import AggregateKey, ColumnKey, TransformKey
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._dev1750_fixtures import (
    ModelMeasure,
    SlayerQuery,
    customers_model,
    line_items_model,
    month_td,
    orders_model,
    regions_model,
)


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=orders_model(),
        referenced_models=[customers_model(), regions_model(), line_items_model()],
    )


def _plan(formula: str, *, name: str = "prev"):
    return plan_query(
        query=SlayerQuery(
            source_model="orders", time_dimensions=month_td(),
            measures=[ModelMeasure(formula=formula, name=name)],
        ),
        bundle=_bundle(),
    )


def _all_slots(planned):
    return [
        *planned.row_slots,
        *planned.aggregate_slots,
        *planned.combined_expression_slots,
    ]


def _time_shift_inner_slot_id(planned) -> str:
    """The slot id of the aggregate a ``time_shift`` layer wraps."""
    layer = next(
        layer for layer in planned.transform_layers if layer.op == "time_shift"
    )
    slots = _all_slots(planned)
    out_slot = next(s for s in slots if s.id in layer.slot_ids)
    assert isinstance(out_slot.key, TransformKey), out_slot.key
    inner_key = out_slot.key.input
    assert isinstance(inner_key, AggregateKey), inner_key
    inner_slot = next(s for s in slots if s.key == inner_key)
    return inner_slot.id


def _plan_owning(planned, slot_id: str):
    return next(
        (p for p in planned.cross_model_aggregate_plans
         if p.aggregate_slot_id == slot_id),
        None,
    )


class TestGuardOwnership:
    def test_b_host_rooted_inner_is_owned_by_a_host_rooted_plan(self) -> None:
        """(b) ``time_shift(amount:wscaled_sum)`` — the inner aggregate's plan is
        HOST-rooted (``cte_root_model`` == the host), the shape the lift renders."""
        planned = _plan("time_shift(amount:wscaled_sum, -1)")
        inner_sid = _time_shift_inner_slot_id(planned)
        plan = _plan_owning(planned, inner_sid)
        assert plan is not None, "shape (b) inner aggregate has no cross-model plan"
        assert plan.cte_root_model == "orders", plan.cte_root_model

    def test_c_target_grain_inner_is_a_regroup_attach(self) -> None:
        """(c) ``time_shift(customers.spend:sum)`` — DEV-1836 migrates the
        target-grain inner onto the regroup primitive: the time_shift wraps a
        regroup placeholder, the query owns a regroup attach, and no cross-model
        plan is produced."""
        planned = _plan("time_shift(customers.spend:sum, -1)")
        assert not planned.cross_model_aggregate_plans
        assert planned.regroup_attach_plans
        layer = next(
            layer for layer in planned.transform_layers if layer.op == "time_shift"
        )
        out_slot = next(s for s in _all_slots(planned) if s.id in layer.slot_ids)
        assert isinstance(out_slot.key, TransformKey), out_slot.key
        inner = out_slot.key.input
        assert isinstance(inner, ColumnKey), inner
        assert inner.leaf.startswith("__regroup__"), inner

    def test_a_local_inner_has_no_cross_model_plan(self) -> None:
        """(a) ``time_shift(amount:sum)`` beside a cross-model sibling — the shift's
        OWN inner aggregate is local, so it owns NO cross-model plan (the guard's
        ownership lookup misses it, and it renders)."""
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders", time_dimensions=month_td(),
                measures=[
                    ModelMeasure(formula="customers.spend:sum", name="cm"),
                    ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
                ],
            ),
            bundle=_bundle(),
        )
        inner_sid = _time_shift_inner_slot_id(planned)
        assert _plan_owning(planned, inner_sid) is None
        # The sibling DID isolate onto the primitive — the query really crosses.
        assert planned.regroup_attach_plans
