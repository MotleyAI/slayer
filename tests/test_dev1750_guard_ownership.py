"""DEV-1750 / DEV-1836 / DEV-1838 — where the work for a ``time_shift`` inner
aggregate lives, pinned at the planner level (Codex test-review F1).

A TARGET-GRAIN inner desugars onto a target-rooted regroup producer; a
host-rooted crossing-fragment inner (``amount:wscaled_sum``) onto a HOST-rooted
producer attach; a plain local inner stays inline. These tests pin that
structural split, so a regression that reroutes any shape is caught here
rather than only by an executed-value diff.
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


def _attach_answering(planned, key):
    """The regroup attach whose substitutions answer ``key``, if any."""
    return next(
        (a for a in planned.regroup_attach_plans
         if any(sub.original_key == key for sub in a.substitutions)),
        None,
    )


class TestGuardOwnership:
    def test_b_host_rooted_inner_is_owned_by_a_host_rooted_plan(self) -> None:
        """(b) ``time_shift(amount:wscaled_sum)`` — DEV-1838 D5 migrates the
        crossing-fragment inner onto the regroup primitive: the shift wraps a
        placeholder answered by a HOST-rooted producer attach."""
        planned = _plan("time_shift(amount:wscaled_sum, -1)")
        layer = next(
            layer for layer in planned.transform_layers if layer.op == "time_shift"
        )
        out_slot = next(s for s in _all_slots(planned) if s.id in layer.slot_ids)
        assert isinstance(out_slot.key, TransformKey), out_slot.key
        inner = out_slot.key.input
        assert isinstance(inner, ColumnKey), inner
        assert inner.leaf.startswith("__regroup__"), inner
        attach = next(
            a for a in planned.regroup_attach_plans
            if any(sub.placeholder == inner for sub in a.substitutions)
        )
        assert attach.producer_root_model is None  # rooted at the host

    def test_c_target_grain_inner_is_a_regroup_attach(self) -> None:
        """(c) ``time_shift(customers.spend:sum)`` — DEV-1836 migrates the
        target-grain inner onto the regroup primitive: the time_shift wraps a
        regroup placeholder, the query owns a regroup attach, and no cross-model
        plan is produced."""
        planned = _plan("time_shift(customers.spend:sum, -1)")
        assert planned.regroup_attach_plans
        layer = next(
            layer for layer in planned.transform_layers if layer.op == "time_shift"
        )
        out_slot = next(s for s in _all_slots(planned) if s.id in layer.slot_ids)
        assert isinstance(out_slot.key, TransformKey), out_slot.key
        inner = out_slot.key.input
        assert isinstance(inner, ColumnKey), inner
        assert inner.leaf.startswith("__regroup__"), inner

    def test_a_local_inner_stays_inline(self) -> None:
        """(a) ``time_shift(amount:sum)`` beside a cross-model sibling — the shift's
        OWN inner aggregate is local, so no producer attach answers it (it stays
        an inline ``_base`` aggregate, and it renders)."""
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
        inner_key = next(
            s.key for s in _all_slots(planned) if s.id == inner_sid
        )
        assert _attach_answering(planned, inner_key) is None
        # The sibling DID isolate onto the primitive — the query really crosses.
        assert planned.regroup_attach_plans
