"""Stage partitioning of a ``PlannedQuery`` (DEV-1800 D3/D8).

The SQL generator reads a value's placement from the planner-assigned
``ValueSlot.stage`` / ``needs_column`` rather than re-deriving it from key shape.
These pure helpers turn those facts into the slot-id sets each render phase
consumes — the host ``_base`` column set, the combined-SELECT expressions, the
post step — so both render paths share one derivation instead of the several
that used to disagree.
"""

from __future__ import annotations

from typing import AbstractSet, List, Set

from slayer.core.keys import ArithmeticKey, ScalarCallKey
from slayer.ir.planned import PlannedQuery, StageKind, ValueSlot

__all__ = [
    "base_render_order",
    "combined_composite_slot_ids",
    "plan_slots",
]

_COMPOSITE_KINDS = (ArithmeticKey, ScalarCallKey)


def plan_slots(planned_query: PlannedQuery) -> List[ValueSlot]:
    """Every value slot of ``planned_query``, in plan order (row, aggregate,
    combined-expression)."""
    return [
        *planned_query.row_slots,
        *planned_query.aggregate_slots,
        *planned_query.combined_expression_slots,
    ]


def base_render_order(
    planned_query: PlannedQuery,
    *,
    isolated_slot_ids: AbstractSet[str] = frozenset(),
) -> List[str]:
    """Slot ids the host ``_base`` CTE projects: every value staged at BASE that a
    later stage needs as a column, projected slots first (in projection order),
    then hidden aux (in plan order). A value also joined at the combined SELECT
    (``isolated_slot_ids`` — a dual-role placeholder) is read from that producer
    CTE, not ``_base``, so it is excluded."""
    base_ids = {
        s.id
        for s in plan_slots(planned_query)
        if s.stage is not None
        and s.stage.kind is StageKind.BASE
        and s.needs_column
        and s.id not in isolated_slot_ids
    }
    order: List[str] = []
    # Projected base slots first, keeping projection multiplicity — a C13 slot with
    # two public names appears once per name so _base emits both aliases.
    for sid in planned_query.projection:
        if sid in base_ids:
            order.append(sid)
    # Then hidden aux base slots, once each, in plan order.
    proj_set = set(planned_query.projection)
    seen: Set[str] = set(order)
    for slot in plan_slots(planned_query):
        if slot.id in base_ids and slot.id not in proj_set and slot.id not in seen:
            order.append(slot.id)
            seen.add(slot.id)
    return order


def combined_composite_slot_ids(planned_query: PlannedQuery) -> Set[str]:
    """Composite (arithmetic / scalar-call) measure or order slots the planner
    staged at COMBINED — they render at the combined SELECT reading producer
    columns. A POST composite (one carrying a transform) renders at the post step
    instead and is not returned here; a computed dimension groups in ``_base``."""
    return {
        s.id
        for s in plan_slots(planned_query)
        if isinstance(s.key, _COMPOSITE_KINDS)
        and not s.is_dimension
        and s.stage is not None
        and s.stage.kind is StageKind.COMBINED
    }
