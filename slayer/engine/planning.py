"""Stage 7a.6 (DEV-1450) — ValueRegistry, TransformLowerer, ProjectionPlanner.

Three composable concerns:

* ``ValueRegistry`` interns ``ValueKey``s by structural identity. Two
  structurally-equal keys share one ``ValueSlot`` (P2). The same key
  declared with multiple ``name``s accumulates multiple
  ``public_aliases`` on a single slot (P4 / C13). Alias collisions
  with source columns / duplicate names are rejected per DEV-1443.

* ``desugar_change`` / ``desugar_change_pct`` lower sugar transforms
  into their underlying form. The inner operand keeps the same
  structural identity across all occurrences (DEV-1446) so the
  ValueRegistry interns it once. ``partition_by`` threads through to
  the underlying ``time_shift`` (C6).

* ``ProjectionPlanner`` allocates slots for declared measures and
  creates hidden slots for refs that appear ONLY in order/filter.
  Hidden slots are materialised but trimmed from the public projection.

Dormant in 7a — no engine wiring. Stage 7a.7's ``stage_planner.py``
composes these with the cross-model planner to build a
``PlannedQuery``.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.errors import (
    CanonicalAliasShadowsColumnError,
    DuplicateMeasureNameError,
    MeasureNameCollidesWithColumnError,
)
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    normalize_scalar,
)
from slayer.engine.binding import BoundExpr, BoundFilter
from slayer.engine.planned import SlotId, ValueSlot

__all__ = [
    "DeclaredMeasure",
    "OrderSpec",
    "ProjectionPlan",
    "ProjectionPlanner",
    "ValueRegistry",
    "desugar_change",
    "desugar_change_pct",
    "filter_referenced_slot_ids",
    "lower_sugar_transforms",
]


# ---------------------------------------------------------------------------
# ValueRegistry
# ---------------------------------------------------------------------------


class ValueRegistry:
    """Interns ``ValueKey``s by structural identity into ``ValueSlot``s.

    Constructor takes ``source_column_names``: the set of column names
    on the host model used for the alias-collision validations
    (``MeasureNameCollidesWithColumnError``,
    ``CanonicalAliasShadowsColumnError``). Pass an empty set when the
    host doesn't expose column names (or when the validations should
    skip — e.g., in unit tests for the registry in isolation).
    """

    def __init__(
        self,
        *,
        source_column_names: Optional[FrozenSet[str]] = None,
        host_model_name: str = "(host)",
    ) -> None:
        self._source_columns: FrozenSet[str] = (
            source_column_names or frozenset()
        )
        self._host_model_name = host_model_name
        self._slots: Dict[SlotId, ValueSlot] = {}
        self._by_key: Dict[ValueKey, SlotId] = {}
        self._declared_names: Dict[str, SlotId] = {}
        self._counter = 0

    def _next_id(self) -> SlotId:
        self._counter += 1
        return f"s{self._counter}"

    def intern(
        self,
        *,
        key: ValueKey,
        declared_name: str,
        phase: Phase,
        public_name: Optional[str] = None,
        canonical_alias: Optional[str] = None,
        hidden: bool = False,
        label: Optional[str] = None,
        type: Optional[DataType] = None,
        expression: Optional["BoundExpr"] = None,
    ) -> SlotId:
        # Alias-collision validations (P4 / DEV-1443).
        # Exemption: a dimension whose public name IS its own column
        # name (``ColumnKey(path=(), leaf=X)`` declared as ``X``) is the
        # column, not a rename of it — collision check skipped. Same
        # exemption for a local ``TimeTruncKey`` over that same column
        # since a time dimension on ``created_at`` projects the
        # (truncated) ``created_at`` column rather than introducing a
        # new alias.
        is_self_named_dimension = (
            isinstance(key, ColumnKey)
            and key.path == ()
            and public_name == key.leaf
        ) or (
            isinstance(key, TimeTruncKey)
            and key.column.path == ()
            and public_name == key.column.leaf
        )
        if (
            public_name is not None
            and public_name in self._source_columns
            and not is_self_named_dimension
        ):
            raise MeasureNameCollidesWithColumnError(
                name=public_name, model=self._host_model_name,
            )
        if (
            canonical_alias is not None
            and canonical_alias in self._source_columns
        ):
            raise CanonicalAliasShadowsColumnError(
                formula=declared_name,
                canonical=canonical_alias,
                model=self._host_model_name,
            )

        existing_sid = self._by_key.get(key)
        if existing_sid is not None:
            return self._merge_into_existing(
                existing_sid=existing_sid,
                public_name=public_name,
                declared_name=declared_name,
                hidden=hidden,
            )

        # Fresh slot. Check declared_name collision against a different key.
        if public_name is not None:
            owner = self._declared_names.get(public_name)
            if owner is not None:
                raise DuplicateMeasureNameError(
                    name=public_name,
                    occurrences=[
                        self._slots[owner].declared_name,
                        declared_name,
                    ],
                )

        sid = self._next_id()
        public_aliases = [public_name] if public_name is not None else []
        slot = ValueSlot(
            id=sid,
            key=key,
            declared_name=declared_name,
            public_name=public_name,
            public_aliases=public_aliases,
            hidden=hidden,
            phase=phase,
            label=label,
            type=type,
            expression=expression if expression is not None else BoundExpr(value_key=key),
        )
        self._slots[sid] = slot
        self._by_key[key] = sid
        if public_name is not None:
            self._declared_names[public_name] = sid
        return sid

    def _merge_into_existing(
        self,
        *,
        existing_sid: SlotId,
        public_name: Optional[str],
        declared_name: str,
        hidden: bool,
    ) -> SlotId:
        slot = self._slots[existing_sid]
        updates: Dict = {}
        if public_name is not None and public_name not in slot.public_aliases:
            owner = self._declared_names.get(public_name)
            if owner is not None and owner != existing_sid:
                raise DuplicateMeasureNameError(
                    name=public_name,
                    occurrences=[
                        self._slots[owner].declared_name,
                        declared_name,
                    ],
                )
            updates["public_aliases"] = list(slot.public_aliases) + [public_name]
            if slot.hidden:
                updates["hidden"] = False
                updates["public_name"] = public_name
            self._declared_names[public_name] = existing_sid
        elif not hidden and slot.hidden and public_name is None:
            # Re-intern as non-hidden — promote to public.
            updates["hidden"] = False
        if updates:
            new_slot = slot.model_copy(update=updates)
            self._slots[existing_sid] = new_slot
        return existing_sid

    def get(self, slot_id: SlotId) -> ValueSlot:
        return self._slots[slot_id]

    def find_by_key(self, key: ValueKey) -> Optional[SlotId]:
        return self._by_key.get(key)

    @property
    def slots(self) -> List[ValueSlot]:
        return list(self._slots.values())


# ---------------------------------------------------------------------------
# TransformLowerer
# ---------------------------------------------------------------------------


def desugar_change(key: TransformKey) -> ArithmeticKey:
    """``change(x)`` → ``x - time_shift(x, periods=-1, [partition_by=…])``.

    The inner ``x`` is identity-preserving — the ``ArithmeticKey`` and
    the ``TransformKey`` use the SAME ``ValueKey`` instance, so a
    downstream ValueRegistry interns it as one slot (DEV-1446).

    ``partition_by`` (the binder put it on ``key.partition_keys``)
    threads through to the underlying ``time_shift`` (C6). ``periods``
    is fixed at ``-1`` (one period back) because ``change`` has no
    user-tunable offset.
    """
    if key.op != "change":
        raise ValueError(
            f"desugar_change expected op='change', got {key.op!r}."
        )
    inner = key.input
    shifted = TransformKey(
        op="time_shift",
        input=inner,
        kwargs=(("periods", normalize_scalar(-1)),),
        partition_keys=key.partition_keys,
        time_key=key.time_key,
    )
    return ArithmeticKey(op="-", operands=(inner, shifted))


def lower_sugar_transforms(key: ValueKey) -> ValueKey:
    """Recursively lower ``change`` / ``change_pct`` TransformKeys to
    their desugared arithmetic form, preserving the inner aggregate's
    structural identity (DEV-1446). Other ValueKey shapes are walked
    but otherwise unchanged.

    The desugar functions preserve ``partition_keys`` / ``time_key`` on
    the resulting ``time_shift`` TransformKey (DEV-1450 C6), so
    ``change(amount:sum, partition_by=region)`` lowers to
    ``amount:sum - time_shift(amount:sum, partition_by=region)``.
    """
    if isinstance(key, TransformKey):
        new_input = lower_sugar_transforms(key.input)
        if new_input is not key.input:
            key = key.model_copy(update={"input": new_input})
        if key.op == "change":
            return desugar_change(key)
        if key.op == "change_pct":
            return desugar_change_pct(key)
        return key
    if isinstance(key, ArithmeticKey):
        new_ops = tuple(lower_sugar_transforms(op) for op in key.operands)
        if all(a is b for a, b in zip(new_ops, key.operands)):
            return key
        return ArithmeticKey(op=key.op, operands=new_ops)
    if isinstance(key, ScalarCallKey):
        new_args = tuple(
            lower_sugar_transforms(a) if isinstance(a, _SLOTTABLE_KIND + (ArithmeticKey, ScalarCallKey)) else a
            for a in key.args
        )
        if all(a is b for a, b in zip(new_args, key.args)):
            return key
        return ScalarCallKey(name=key.name, args=new_args)
    return key


def desugar_change_pct(key: TransformKey) -> ArithmeticKey:
    """``change_pct(x)`` → ``(x - time_shift(x, periods=-1)) /
    time_shift(x, periods=-1)``.

    Same identity-preservation as ``desugar_change``.
    """
    if key.op != "change_pct":
        raise ValueError(
            f"desugar_change_pct expected op='change_pct', got {key.op!r}."
        )
    inner = key.input
    shifted = TransformKey(
        op="time_shift",
        input=inner,
        kwargs=(("periods", normalize_scalar(-1)),),
        partition_keys=key.partition_keys,
        time_key=key.time_key,
    )
    numerator = ArithmeticKey(op="-", operands=(inner, shifted))
    return ArithmeticKey(op="/", operands=(numerator, shifted))


# ---------------------------------------------------------------------------
# ProjectionPlanner
# ---------------------------------------------------------------------------


class DeclaredMeasure(BaseModel):
    """One declared measure on a query.

    ``bound`` is the binder's output. ``declared_name`` is the canonical
    or user-supplied name. ``public_name`` is the user-facing alias —
    set when the user supplied an explicit ``name`` on the measure spec.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    bound: BoundExpr
    declared_name: str
    public_name: Optional[str] = None
    label: Optional[str] = None
    canonical_alias: Optional[str] = None


class OrderSpec(BaseModel):
    """One ORDER BY entry on a query."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    bound: BoundExpr
    direction: str = "asc"


class ProjectionPlan(BaseModel):
    """ProjectionPlanner output: registry + projection order + filters / order."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    registry: "ValueRegistry"
    public_projection: List[SlotId] = Field(default_factory=list)
    filters: List[BoundFilter] = Field(default_factory=list)
    order: List["OrderSpec"] = Field(default_factory=list)


_SLOTTABLE_KIND = (
    ColumnKey, ColumnSqlKey, AggregateKey, TransformKey, TimeTruncKey,
)


def _iter_slot_deps(key: ValueKey):
    """Yield only ``ValueKey``s that need a materialised slot.

    Skips composite-only nodes that the SQL generator inlines:
    ``ArithmeticKey`` (operators), ``ScalarCallKey`` (function calls
    inlined into SELECT / WHERE), ``LiteralKey``, ``StarKey``. Stops
    at ``AggregateKey`` (its inner ``source`` ColumnKey is materialised
    inside the aggregate, not as a separate slot). Recurses into
    ``TransformKey.input`` so a nested aggregate inside a transform
    gets its own hidden slot.

    ``TimeTruncKey`` is itself the materialised slot (the generator
    emits the DATE_TRUNC at SELECT time); the inner ColumnKey is not
    yielded as a separate dependency — adding a time dimension must
    not auto-add the raw column as an output (matches legacy).
    """
    if isinstance(key, AggregateKey):
        yield key
        return
    if isinstance(key, TransformKey):
        yield key
        yield from _iter_slot_deps(key.input)
        # Transform aux deps: partition_keys and time_key must be
        # materialised as their own slots so the SQL generator (slice
        # 7b.10 / 7b.11) can render PARTITION BY / ORDER BY against
        # named SELECT projections instead of re-walking the model
        # graph.
        for pk in key.partition_keys:
            yield from _iter_slot_deps(pk)
        if key.time_key is not None:
            yield from _iter_slot_deps(key.time_key)
        return
    if isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        yield key
        return
    if isinstance(key, ArithmeticKey):
        for op in key.operands:
            yield from _iter_slot_deps(op)
        return
    if isinstance(key, ScalarCallKey):
        for arg in key.args:
            if isinstance(arg, _SLOTTABLE_KIND + (ArithmeticKey, ScalarCallKey)):
                yield from _iter_slot_deps(arg)
        return
    # StarKey, LiteralKey — never slottable on their own.


class ProjectionPlanner:
    """Allocate slots for declared measures + hidden slots for refs only
    used in order/filter."""

    def plan(
        self,
        *,
        measures: List[DeclaredMeasure],
        filters: List[BoundFilter],
        order: List[OrderSpec],
        source_column_names: Optional[FrozenSet[str]] = None,
        host_model_name: str = "(host)",
    ) -> ProjectionPlan:
        registry = ValueRegistry(
            source_column_names=source_column_names,
            host_model_name=host_model_name,
        )
        public_projection: List[SlotId] = []
        for m in measures:
            sid = registry.intern(
                key=m.bound.value_key,
                declared_name=m.declared_name,
                public_name=m.public_name,
                canonical_alias=m.canonical_alias,
                phase=m.bound.phase,
                label=m.label,
            )
            public_projection.append(sid)
            # Materialise any auxiliary slot-worthy deps of the measure
            # as hidden slots (e.g. the inner AggregateKey of a transform,
            # the partition columns, the time_key column). These are
            # rendered by the generator into the inner SELECT but not
            # surfaced in the public projection.
            for dep in _iter_slot_deps(m.bound.value_key):
                if dep == m.bound.value_key:
                    continue
                if registry.find_by_key(dep) is None:
                    registry.intern(
                        key=dep,
                        declared_name=_canonical_name(dep),
                        hidden=True,
                        phase=dep.phase,
                    )

        # Filter and order share the same dependency-selection rule: walk
        # the bound expression, intern each slot-worthy key as a hidden
        # slot if not already present.
        for f in filters:
            for dep in _iter_slot_deps(f.value_key):
                if registry.find_by_key(dep) is None:
                    registry.intern(
                        key=dep,
                        declared_name=_canonical_name(dep),
                        hidden=True,
                        phase=dep.phase,
                    )

        for o in order:
            for dep in _iter_slot_deps(o.bound.value_key):
                if registry.find_by_key(dep) is None:
                    registry.intern(
                        key=dep,
                        declared_name=_canonical_name(dep),
                        hidden=True,
                        phase=dep.phase,
                    )

        return ProjectionPlan(
            registry=registry,
            public_projection=public_projection,
            filters=filters,
            order=order,
        )


def _canonical_name(key: ValueKey) -> str:
    """Best-effort canonical name for a hidden slot.

    Mirrors the public-alias canonical form used by the engine
    elsewhere: ``revenue:sum`` → ``revenue_sum``; ``*:count`` →
    ``_count``; ``customers.regions.name`` → flattened ``customers__regions__name``.
    """
    if isinstance(key, ColumnKey):
        return "__".join(key.path + (key.leaf,))
    if isinstance(key, ColumnSqlKey):
        prefix = "__".join(key.path) + "__" if key.path else ""
        return f"{prefix}{key.column_name}"
    if isinstance(key, TimeTruncKey):
        # Legacy alias contract: granularity is encoded in the SQL
        # DATE_TRUNC, not in the alias.
        return _canonical_name(key.column)
    if isinstance(key, AggregateKey):
        if isinstance(key.source, StarKey):
            return f"_{key.agg}"
        leaf = getattr(key.source, "leaf", None) or getattr(key.source, "column_name", None)
        if leaf is None:
            return f"_agg_{key.agg}"
        return f"{leaf}_{key.agg}"
    if isinstance(key, TransformKey):
        return f"_{key.op}_inner"
    if isinstance(key, ArithmeticKey):
        return f"_arith_{key.op}"
    if isinstance(key, ScalarCallKey):
        return f"_scalar_{key.name}"
    if isinstance(key, LiteralKey):
        return f"_lit_{key.value}"
    if isinstance(key, StarKey):
        return "_star"
    return "_hidden"


ProjectionPlan.model_rebuild()


# ---------------------------------------------------------------------------
# Stage 7b.5 — filter → slot id mapping for cross-model planner routing
# ---------------------------------------------------------------------------


def filter_referenced_slot_ids(
    bound_filter: "BoundFilter",
    registry: "ValueRegistry",
) -> "set":
    """Return the set of ``SlotId``s that ``bound_filter``'s predicate
    references through interned slots.

    Walks the predicate's ``ValueKey`` tree via ``_iter_slot_deps`` —
    yielding only slot-worthy keys (``ColumnKey`` / ``ColumnSqlKey`` /
    ``AggregateKey`` / ``TransformKey`` / ``TimeTruncKey``) and skipping
    composite-only nodes (``ArithmeticKey``, ``ScalarCallKey``,
    ``LiteralKey``, ``StarKey``). Each slot-worthy key is looked up in
    the registry; keys without an interned slot are silently skipped
    (filter literals, hidden registry misses).

    Codex HIGH #3/#4 for DEV-1450: this helper exists so the
    cross-model planner gets ``set[SlotId]`` instead of having to
    classify ``BoundFilter.referenced_keys`` (which are
    pre-interning ``ValueKey``s, not slot ids) or naively walking only
    the top-level key (which misses composite-predicate leaves).
    """
    result: set = set()
    for dep in _iter_slot_deps(bound_filter.value_key):
        sid = registry.find_by_key(dep)
        if sid is not None:
            result.add(sid)
    return result
