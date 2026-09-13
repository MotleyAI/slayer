"""DEV-1838 — a transform-wrapped cross-model aggregate predicate lowers POST,
never to the combined outer WHERE (mask lowering, DEV-1865)."""

from __future__ import annotations

from decimal import Decimal

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    Phase,
    TransformKey,
)
from slayer.ir.planned import (
    MaskEntry,
    MaskTyping,
    PlannedQuery,
    RegroupAttachPlan,
    RegroupSubstitution,
    ValueSlot,
)
from slayer.sql.generator import _lower_positions

_SPEND = AggregateKey(source=ColumnKey(path=("customers",), leaf="spend"), agg="sum")
_GT0 = LiteralKey(value=Decimal(0))
_PLACEHOLDER = ColumnKey(path=(), leaf="__regroup__0__spend_sum")


def _plan_with_mask(mask_key, *, phase: Phase, extra_row_slots=()) -> PlannedQuery:
    slot = ValueSlot(
        id="s1", key=mask_key, declared_name="__slayer_mask_0",
        hidden=True, phase=phase,
    )
    producer = PlannedQuery(source_relation="customers")
    attach = RegroupAttachPlan(
        producer_plan=producer, alias_hint="spend_sum", attach_phase="combined",
        substitutions=[RegroupSubstitution(
            placeholder=_PLACEHOLDER, producer_slot_id="p1", original_key=_SPEND,
        )],
    )
    return PlannedQuery(
        source_relation="orders",
        row_slots=list(extra_row_slots),
        combined_expression_slots=[slot],
        regroup_attach_plans=[attach],
        masks=[MaskEntry(slot_id="s1", typing=MaskTyping.MEASURE, stratum=1)],
    )


def test_transform_wrapped_cross_model_predicate_is_not_combined() -> None:
    cj = ArithmeticKey(op=">", operands=(TransformKey(op="cumsum", input=_SPEND), _GT0))
    # POST-phase: the transform owns the predicate, so it lowers to the post
    # wrapper instead of the combined outer WHERE (regressed before the fix).
    lowered = _lower_positions(_plan_with_mask(cj, phase=Phase.POST))
    (entry,) = [e for e in lowered.filters if e.id == "s1"]
    assert entry.phase == Phase.POST
    assert lowered.outer_where_ids == []


def test_bare_cross_model_predicate_still_routes_combined() -> None:
    cj = ArithmeticKey(op=">", operands=(_PLACEHOLDER, _GT0))
    ph_slot = ValueSlot(
        id="s2", key=_PLACEHOLDER, declared_name="__regroup__0__spend_sum",
        hidden=True, phase=Phase.ROW,
    )
    lowered = _lower_positions(_plan_with_mask(
        cj, phase=Phase.AGGREGATE, extra_row_slots=[ph_slot],
    ))
    (entry,) = [e for e in lowered.filters if e.id == "s1"]
    assert entry.phase == Phase.AGGREGATE
    assert lowered.outer_where_ids == ["s1"]
