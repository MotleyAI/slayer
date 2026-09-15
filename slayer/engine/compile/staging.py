"""The one materialisation-staging pass (DEV-1800 D3).

Assigns every ``ValueSlot`` in a plan exactly one ``Stage`` (which relation in
the emitted pipeline it materialises in), a ``needs_column`` flag (must it be
projected as a column for a later stage / consumer), and — for a transform —
its ``series`` regime. Producer bodies are staged by their own
``compile_prebound`` call, so this pass never recurses into them; it reads the
attach plans only to classify placeholders and join-back keys.

The generator becomes a stage partitioner over these facts (D8): the classifiers
it used to re-derive placement from key shape retire.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from slayer.core.errors import MaterialisationStageError
from slayer.core.keys import (
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    AggregateKey,
    InKey,
    REGROUP_LEAF_PREFIX,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    is_boolean_shaped,
    window_kwarg_of,
)
from slayer.engine.compile.projection import _iter_slot_deps
from slayer.ir.planned import (
    MaskEntry,
    MaskTyping,
    OrderEntry,
    RegroupAttachPlan,
    SlotId,
    Stage,
    StageKind,
    ValueSlot,
)

__all__ = ["stage_slots"]

#: Composite / predicate kinds staged by their operands' stages.
_COMPOSITE_KINDS = (ArithmeticKey, ScalarCallKey, BetweenKey, InKey)


def _is_placeholder(key: ValueKey) -> bool:
    return isinstance(key, ColumnKey) and key.leaf.startswith(REGROUP_LEAF_PREFIX)


def _placeholder_phases(
    attach_plans: List[RegroupAttachPlan],
) -> Tuple[Set[ValueKey], Set[ValueKey]]:
    """(row-attached placeholder keys, combined-attached placeholder keys). A
    row-attached placeholder is joined inside ``_base`` (BASE) even when the same
    value is also combined-attached (dual role); _base is the earliest relation
    it materialises in."""
    row: Set[ValueKey] = set()
    combined: Set[ValueKey] = set()
    for attach in attach_plans:
        target = row if attach.attach_phase == "row" else combined
        for sub in attach.substitutions:
            target.add(sub.placeholder)
    return row, combined


def _placeholder_to_original(
    attach_plans: List[RegroupAttachPlan],
) -> Dict[ValueKey, ValueKey]:
    return {
        sub.placeholder: sub.original_key
        for attach in attach_plans
        for sub in attach.substitutions
    }


def _series_flags(node: ValueKey, *, to_original: Dict[ValueKey, ValueKey]) -> Tuple[bool, bool]:
    """(has_transform, has_cross_model_agg) over a composite; a placeholder
    resolves to its original aggregate (D6, the retired ``_classify_walk``)."""
    has_transform = False
    has_cross_model = False
    if isinstance(node, TransformKey):
        has_transform = True
    elif isinstance(node, AggregateKey):
        if getattr(node.source, "path", ()):
            has_cross_model = True
    elif _is_placeholder(node):
        original = to_original.get(node)
        if original is None:
            has_cross_model = True  # unknown placeholder — fail closed
        else:
            return _series_flags(original, to_original=to_original)
    elif not isinstance(node, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        for child in node.children():
            t, x = _series_flags(child, to_original=to_original)
            has_transform = has_transform or t
            has_cross_model = has_cross_model or x
    return has_transform, has_cross_model


def _series_mode(inner: ValueKey, *, to_original: Dict[ValueKey, ValueKey]) -> bool:
    """Whether a transform shifts its materialised series (True) or re-aggregates
    (False) — a nested transform, a predicate root, or a composite carrying a
    transform or cross-model aggregate leaf (D6)."""
    if isinstance(inner, TransformKey) or is_boolean_shaped(inner):
        return True
    if isinstance(inner, (ArithmeticKey, ScalarCallKey)):
        has_transform, has_cross_model = _series_flags(inner, to_original=to_original)
        return has_transform or has_cross_model
    return False


def stage_slots(
    *,
    row_slots: List[ValueSlot],
    aggregate_slots: List[ValueSlot],
    combined_expression_slots: List[ValueSlot],
    regroup_attach_plans: List[RegroupAttachPlan],
    masks: List[MaskEntry],
    order: List[OrderEntry],
    projection: List[SlotId],
    distinct_dimension_values: bool = True,
) -> Tuple[List[ValueSlot], List[ValueSlot], List[ValueSlot]]:
    """Return the three slot lists with ``stage`` / ``needs_column`` / ``series``
    assigned. Producer bodies are staged separately; this pass classifies only
    this plan's own slots."""
    all_slots = [*row_slots, *aggregate_slots, *combined_expression_slots]
    by_key: Dict[ValueKey, ValueSlot] = {s.key: s for s in all_slots}
    by_id: Dict[SlotId, ValueSlot] = {s.id: s for s in all_slots}
    row_ph, combined_ph = _placeholder_phases(regroup_attach_plans)
    to_original = _placeholder_to_original(regroup_attach_plans)

    stage_cache: Dict[SlotId, Stage] = {}
    in_progress: Set[SlotId] = set()

    def _deps(slot: ValueSlot) -> List[ValueSlot]:
        seen: Set[SlotId] = set()
        out: List[ValueSlot] = []
        for dep_key in _iter_slot_deps(slot.key):
            if dep_key == slot.key:
                continue
            dep = by_key.get(dep_key)
            if dep is not None and dep.id not in seen:
                seen.add(dep.id)
                out.append(dep)
        return out

    def _stage_of(slot: ValueSlot) -> Stage:
        cached = stage_cache.get(slot.id)
        if cached is not None:
            return cached
        if slot.id in in_progress:
            raise MaterialisationStageError(
                f"value {slot.id!r} participates in a materialisation-stage "
                f"dependency cycle.",
            )
        in_progress.add(slot.id)
        stage = _compute_stage(slot)
        in_progress.discard(slot.id)
        stage_cache[slot.id] = stage
        return stage

    def _compute_stage(slot: ValueSlot) -> Stage:
        key = slot.key
        if isinstance(key, AggregateKey):
            # A windowed aggregate lives in its own windowed CTE; a host-grain
            # wrap is answered by a combined attach (its value comes from a joined
            # producer CTE) — both PRODUCER. A host-grain aggregate that is a
            # producer body's own output is not substituted here, so it stays BASE.
            if window_kwarg_of(key) is not None or key in combined_ph:
                return Stage(kind=StageKind.PRODUCER)
            return Stage(kind=StageKind.BASE)
        if _is_placeholder(key):
            # A row-attached placeholder is joined inside _base (BASE), even when
            # the same value is also combined-attached (dual role): _base is the
            # earliest relation it materialises in. A combined-only placeholder is
            # joined at the combined SELECT (PRODUCER). An externally-provided
            # carrier column (answered by the enclosing producer, absent from
            # this plan's own attaches) is likewise a base column (BASE).
            if key in row_ph:
                return Stage(kind=StageKind.BASE)
            if key in combined_ph:
                return Stage(kind=StageKind.PRODUCER)
            return Stage(kind=StageKind.BASE)
        if isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            return Stage(kind=StageKind.BASE)
        if isinstance(key, TransformKey):
            max_chain = max(
                (
                    _stage_of(d).level
                    for d in _deps(slot)
                    if _stage_of(d).kind is StageKind.CHAIN
                ),
                default=0,
            )
            return Stage(kind=StageKind.CHAIN, level=1 + max_chain)
        if isinstance(key, _COMPOSITE_KINDS):
            if slot.is_dimension:
                return Stage(kind=StageKind.BASE)
            dep_kinds = {_stage_of(d).kind for d in _deps(slot)}
            if StageKind.CHAIN in dep_kinds:
                return Stage(kind=StageKind.POST)
            if dep_kinds & {StageKind.PRODUCER, StageKind.COMBINED}:
                return Stage(kind=StageKind.COMBINED)
            return Stage(kind=StageKind.BASE)
        raise MaterialisationStageError(
            f"value {slot.id!r} of kind {type(key).__name__} has no "
            f"materialisation-stage rule.",
        )

    stages = {s.id: _stage_of(s) for s in all_slots}

    needs_column = _compute_needs_column(
        all_slots=all_slots, by_key=by_key, by_id=by_id, stages=stages,
        masks=masks, order=order, projection=projection,
        attach_plans=regroup_attach_plans,
        distinct_dimension_values=distinct_dimension_values,
    )

    def _rebuild(slots: List[ValueSlot]) -> List[ValueSlot]:
        out: List[ValueSlot] = []
        for s in slots:
            series: Optional[bool] = (
                _series_mode(s.key.input, to_original=to_original)
                if isinstance(s.key, TransformKey) else None
            )
            out.append(s.model_copy(update={
                "stage": stages[s.id],
                "needs_column": s.id in needs_column,
                "series": series,
            }))
        return out

    return _rebuild(row_slots), _rebuild(aggregate_slots), _rebuild(combined_expression_slots)


def _compute_needs_column(
    *,
    all_slots: List[ValueSlot],
    by_key: Dict[ValueKey, ValueSlot],
    by_id: Dict[SlotId, ValueSlot],
    stages: Dict[SlotId, Stage],
    masks: List[MaskEntry],
    order: List[OrderEntry],
    projection: List[SlotId],
    attach_plans: List[RegroupAttachPlan],
    distinct_dimension_values: bool,
) -> Set[SlotId]:
    """Slot ids that must be projected as a column of their own relation (D4)."""
    needs: Set[SlotId] = set(projection)

    def _direct_deps(slot: ValueSlot) -> List[ValueSlot]:
        """Slots ``slot`` directly references: descend through un-interned
        composite / transform structure, stopping at any interned slot — a
        materialised value is referenced as a whole, not through its internals
        (F7: a computed dimension resolves by its grouped alias). An aggregate's
        internals are its own business (like ``_iter_slot_deps``), so its source
        columns are never consumer deps."""
        out: List[ValueSlot] = []
        seen: Set[SlotId] = set()

        def visit(key: ValueKey) -> None:
            dep = by_key.get(key)
            if dep is not None:
                if dep.id not in seen:
                    seen.add(dep.id)
                    out.append(dep)
                return
            if isinstance(key, (TransformKey, *_COMPOSITE_KINDS)):
                for child in key.children():
                    visit(child)

        if isinstance(slot.key, AggregateKey):
            return out
        for child in slot.key.children():
            visit(child)
        return out

    # A value read by a strictly-later stage must be a column of its own relation.
    for slot in all_slots:
        for dep in _direct_deps(slot):
            if stages[dep.id] < stages[slot.id]:
                needs.add(dep.id)

    # A measure-typed mask renders in HAVING, which reads its aggregates by base
    # alias, so those deps need columns. A field-typed mask renders inline in
    # WHERE — its column deps are referenced directly, never projected.
    for mask in masks:
        if mask.typing is not MaskTyping.MEASURE:
            continue
        slot = by_id.get(mask.slot_id)
        if slot is None:
            continue
        for dep in _direct_deps(slot):
            needs.add(dep.id)

    # An attach joins back on host-side grain keys, which must be columns of
    # _base. join_pairs is (host_key, producer_slot_id): the host key is in this
    # plan's coordinates; the slot id belongs to the producer plan, not here.
    for attach in attach_plans:
        for host_key, _producer_slot_id in attach.join_pairs:
            host_slot = by_key.get(host_key)
            if host_slot is not None:
                needs.add(host_slot.id)

    # Order targets in a grouped query resolve to a hidden column of the relation
    # their stage names (BASE / CHAIN / POST); a PRODUCER / COMBINED value renders
    # inline in the ORDER BY. A raw-rows query (distinct_dimension_values=False)
    # has no grouping, so its row-column order targets resolve inline via split
    # emission, never as a hidden column.
    if distinct_dimension_values:
        for entry in order:
            stage = stages.get(entry.slot_id)
            if stage is None:
                continue
            if stage.kind in (StageKind.BASE, StageKind.CHAIN, StageKind.POST):
                needs.add(entry.slot_id)

    return needs
