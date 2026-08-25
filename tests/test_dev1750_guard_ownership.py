"""DEV-1750 — the narrowed guard is a PLAN-OWNERSHIP decision, pinned at the
planner level (Codex test-review F1).

The guard must fire for a ``time_shift`` whose inner aggregate is a TARGET-GRAIN
cross-model aggregate — identified by its ``CrossModelAggregatePlan`` having
``cte_root_model is None`` — and must NOT fire for a host-rooted one
(``cte_root_model`` = the host model name). Asserting only the error message
would let an implementation classify by formula text or op name instead; these
tests pin the exact ownership property the decision must read, so changing only
``cte_root_model`` flips supported vs guarded.
"""

from __future__ import annotations

from slayer.core.keys import AggregateKey, TransformKey
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

    def test_c_target_grain_inner_is_owned_by_a_target_rooted_plan(self) -> None:
        """(c) ``time_shift(customers.spend:sum)`` — the inner aggregate's plan is
        TARGET-rooted (``cte_root_model is None``), the shape that stays guarded."""
        planned = _plan("time_shift(customers.spend:sum, -1)")
        inner_sid = _time_shift_inner_slot_id(planned)
        plan = _plan_owning(planned, inner_sid)
        assert plan is not None, "shape (c) inner aggregate has no cross-model plan"
        assert plan.cte_root_model is None, plan.cte_root_model

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
        # The sibling DID isolate — so the query really is on the cross-model path.
        assert planned.cross_model_aggregate_plans
