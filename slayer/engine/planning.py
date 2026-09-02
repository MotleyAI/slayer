"""ValueRegistry, TransformLowerer, and ProjectionPlanner for query planning."""

from __future__ import annotations

from typing import Callable, Dict, FrozenSet, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat
from slayer.core.errors import (
    CanonicalAliasShadowsColumnError,
    DuplicateMeasureNameError,
    MeasureNameCollidesWithColumnError,
)
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    Phase,
    REGROUP_LEAF_PREFIX,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    column_leaf,
    column_path,
    normalize_scalar,
)
from slayer.core.formula import RANK_FAMILY_TRANSFORMS
from slayer.sql.naming import canonical_aggregate_alias
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


# ValueRegistry


def _fill_missing_metadata(
    *,
    slot: ValueSlot,
    updates: Dict,
    label: Optional[str] = None,
    type: Optional[DataType] = None,
    format: Optional[NumberFormat] = None,
    description: Optional[str] = None,
) -> None:
    for field_name, new_value in (
        ("label", label),
        ("type", type),
        ("format", format),
        ("description", description),
    ):
        if getattr(slot, field_name) is None and new_value is not None:
            updates[field_name] = new_value


class ValueRegistry:
    """Interns ``ValueKey``s by structural identity into ``ValueSlot``s; ``source_column_names`` drives alias-collision checks."""

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
        # Every alias name already taken (public names + each slot's declared_name);
        # hidden slots are uniquified against it so no alias maps to two expressions.
        self._taken_names: set = set()

    def _next_id(self) -> SlotId:
        self._counter += 1
        return f"s{self._counter}"

    def reserve_public_names(self, names) -> None:
        """Claim user-declared names before any hidden slot is interned (order-independent uniquification)."""
        for name in names:
            if name:
                self._taken_names.add(name)

    def _unique_hidden_name(self, declared_name: str) -> str:
        """``declared_name`` suffixed ``_2``/``_3``/… if taken — structural hidden names collide by construction (``cumsum(a)+cumsum(b)`` both ``_cumsum_inner``)."""
        if declared_name not in self._taken_names:
            return declared_name
        n = 2
        while f"{declared_name}_{n}" in self._taken_names:
            n += 1
        return f"{declared_name}_{n}"

    def _validate_alias_collisions(
        self,
        *,
        key: ValueKey,
        declared_name: str,
        public_name: Optional[str],
        canonical_alias: Optional[str],
    ) -> None:
        """Alias-collision validations (P4): a public name or renamed canonical alias shadowing a source column raises; self-named dimensions are exempt."""
        is_self_named_dimension = (
            isinstance(key, ColumnKey)
            and key.path == ()
            and public_name == key.leaf
        ) or (
            isinstance(key, ColumnSqlKey)
            and key.path == ()
            and public_name == key.column_name
        ) or (
            isinstance(key, TimeTruncKey)
            and column_path(key.column) == ()
            and public_name == column_leaf(key.column)
        )
        # An unnamed ``*:<agg>`` (StarKey) is exempt: its canonical alias (``_count``)
        # is a structural marker, not a column ref. An explicit user name still raises.
        is_unnamed_star_agg = (
            isinstance(key, AggregateKey)
            and isinstance(getattr(key, "source", None), StarKey)
            and canonical_alias is None
        )
        # A pathed column ref only flattens to a ``__`` name at a stage boundary;
        # that collision is owned by the stage-schema guard, so exempt it here.
        is_pathed_projection = (
            isinstance(key, (ColumnKey, ColumnSqlKey)) and key.path != ()
        )
        if (
            public_name is not None
            and public_name in self._source_columns
            and not is_self_named_dimension
            and not is_unnamed_star_agg
            and not is_pathed_projection
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
        type_is_explicit: bool = False,
        expression: Optional["BoundExpr"] = None,
        format: Optional[NumberFormat] = None,
        description: Optional[str] = None,
        is_dimension: bool = False,
    ) -> SlotId:
        self._validate_alias_collisions(
            key=key,
            declared_name=declared_name,
            public_name=public_name,
            canonical_alias=canonical_alias,
        )

        existing_sid = self._by_key.get(key)
        if existing_sid is not None:
            return self._merge_into_existing(
                existing_sid=existing_sid,
                public_name=public_name,
                declared_name=declared_name,
                hidden=hidden,
                label=label,
                type=type,
                type_is_explicit=type_is_explicit,
                format=format,
                description=description,
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
        # Uniquify hidden names only; a public name is the user's contract and a
        # duplicate already raised above.
        if hidden:
            declared_name = self._unique_hidden_name(declared_name)
        self._taken_names.add(declared_name)
        if public_name is not None:
            self._taken_names.add(public_name)
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
            type_is_explicit=type_is_explicit,
            is_dimension=is_dimension,
            expression=expression if expression is not None else BoundExpr(value_key=key),
            format=format,
            description=description,
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
        label: Optional[str] = None,
        type: Optional[DataType] = None,
        type_is_explicit: bool = False,
        format: Optional[NumberFormat] = None,
        description: Optional[str] = None,
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
        # On hidden→public promotion, carry display metadata from the public
        # re-intern, filling missing fields only.
        _fill_missing_metadata(
            slot=slot,
            updates=updates,
            label=label,
            type=type,
            format=format,
            description=description,
        )
        if type_is_explicit and type is not None and slot.type in (None, type):
            updates["type_is_explicit"] = True
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


# TransformLowerer


def desugar_change(key: TransformKey) -> ArithmeticKey:
    """``change(x)`` → ``x - time_shift(x, periods=-1)``; inner ``x`` is identity-preserving so the registry interns it once."""
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
    """Recursively lower ``change``/``change_pct`` TransformKeys to desugared arithmetic, preserving the inner aggregate's identity."""
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
            lower_sugar_transforms(a)
            if isinstance(
                a, _SLOTTABLE_KIND + (ArithmeticKey, ScalarCallKey, BetweenKey),
            )
            else a
            for a in key.args
        )
        if all(a is b for a, b in zip(new_args, key.args)):
            return key
        return ScalarCallKey(name=key.name, args=new_args)
    if isinstance(key, BetweenKey):
        new_col = lower_sugar_transforms(key.column)
        new_low = lower_sugar_transforms(key.low)
        new_high = lower_sugar_transforms(key.high)
        if (
            new_col is key.column
            and new_low is key.low
            and new_high is key.high
        ):
            return key
        return BetweenKey(column=new_col, low=new_low, high=new_high)
    if isinstance(key, InKey):
        # InKey.values is literal-only; only the LHS column can host a transform.
        new_col = lower_sugar_transforms(key.column)
        if new_col is key.column:
            return key
        return InKey(column=new_col, values=key.values, negated=key.negated)
    return key


def rewrite_rank_partition_keys(  # NOSONAR(S3776) — sequential isinstance dispatch over the closed ValueKey union; each branch is the per-type identity-preserving rebuild contract, mirroring lower_sugar_transforms. Extracting per-type helpers would scatter the contract across the module.
    key: ValueKey, *, rewrite_fn: Callable[[TransformKey], FrozenSet],
) -> ValueKey:
    """Replace every rank-family ``TransformKey`` with an explicit ``partition_by`` via ``rewrite_fn``; identity-preserving, runs before interning."""
    def _rec(k: ValueKey) -> ValueKey:
        return rewrite_rank_partition_keys(key=k, rewrite_fn=rewrite_fn)

    if isinstance(key, TransformKey):
        new_input = _rec(key.input)
        new_pk = key.partition_keys
        if key.op in RANK_FAMILY_TRANSFORMS and key.partition_keys:
            new_pk = rewrite_fn(key)
        if new_input is key.input and new_pk == key.partition_keys:
            return key
        return key.model_copy(update={"input": new_input, "partition_keys": new_pk})
    if isinstance(key, AggregateKey):
        if key.partition_keys:
            new_pk = rewrite_fn(key)
            if new_pk != key.partition_keys:
                return key.model_copy(update={"partition_keys": new_pk})
        return key
    if isinstance(key, ArithmeticKey):
        new_ops = tuple(_rec(op) for op in key.operands)
        unchanged = all(a is b for a, b in zip(new_ops, key.operands))
        return key if unchanged else ArithmeticKey(op=key.op, operands=new_ops)
    if isinstance(key, ScalarCallKey):
        rewritable = _SLOTTABLE_KIND + (ArithmeticKey, ScalarCallKey, BetweenKey)
        new_args = tuple(
            _rec(a) if isinstance(a, rewritable) else a for a in key.args
        )
        unchanged = all(a is b for a, b in zip(new_args, key.args))
        return key if unchanged else ScalarCallKey(name=key.name, args=new_args)
    if isinstance(key, BetweenKey):
        new_col, new_low, new_high = _rec(key.column), _rec(key.low), _rec(key.high)
        unchanged = (
            new_col is key.column and new_low is key.low and new_high is key.high
        )
        return key if unchanged else BetweenKey(
            column=new_col, low=new_low, high=new_high,
        )
    if isinstance(key, InKey):
        new_col = _rec(key.column)
        return key if new_col is key.column else InKey(
            column=new_col, values=key.values, negated=key.negated,
        )
    return key


def desugar_change_pct(key: TransformKey) -> ArithmeticKey:
    """``change_pct(x)`` → ``(x - time_shift(x,-1)) / NULLIF(time_shift(x,-1), 0)``; NULLIF guards a zero prior value."""
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
    guarded_divisor = ScalarCallKey(
        name="nullif", args=(shifted, normalize_scalar(0)),
    )
    return ArithmeticKey(op="/", operands=(numerator, guarded_divisor))


# ProjectionPlanner


class DeclaredMeasure(BaseModel):
    """One declared measure; ``type`` follows the aggregation (count → INT, avg → DOUBLE, else source type)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    bound: BoundExpr
    declared_name: str
    public_name: Optional[str] = None
    label: Optional[str] = None
    canonical_alias: Optional[str] = None
    type: Optional[DataType] = None
    type_is_explicit: bool = False
    format: Optional[NumberFormat] = None
    description: Optional[str] = None
    # A computed dimension is a ROW-phase composite projected AND grouped; the
    # flag distinguishes it from a bare-measure expression.
    is_dimension: bool = False


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
    """Yield only ``ValueKey``s needing a materialised slot; TimeTruncKey is itself the slot (its inner column isn't yielded, so a time dimension doesn't auto-add the raw column)."""
    if isinstance(key, AggregateKey):
        yield key
        return
    if isinstance(key, TransformKey):
        yield key
        yield from _iter_slot_deps(key.input)
        # partition_keys / time_key get their own slots so the generator renders
        # PARTITION BY / ORDER BY against named projections.
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
            if isinstance(
                arg,
                _SLOTTABLE_KIND
                + (ArithmeticKey, ScalarCallKey, BetweenKey),
            ):
                yield from _iter_slot_deps(arg)
        return
    if isinstance(key, BetweenKey):
        # BetweenKey is inlined into WHERE; recurse so its ColumnKey surfaces as
        # a referenced slot.
        yield from _iter_slot_deps(key.column)
        yield from _iter_slot_deps(key.low)
        yield from _iter_slot_deps(key.high)
    if isinstance(key, InKey):
        # InKey is inlined into WHERE; surface its LHS column (RHS literals aren't slottable).
        yield from _iter_slot_deps(key.column)
    # StarKey, LiteralKey — never slottable on their own.


def _regroup_substituted_composite_phase(value_key: ValueKey) -> Optional[Phase]:
    """AGGREGATE phase for a composite whose every leaf is a regroup placeholder (reads only ``_cm_`` values, so it renders at the combined SELECT), else None."""
    if not isinstance(value_key, (ArithmeticKey, ScalarCallKey)):
        return None
    deps = list(_iter_slot_deps(value_key))
    if not deps:
        return None
    for dep in deps:
        if not (isinstance(dep, ColumnKey)
                and dep.leaf.startswith(REGROUP_LEAF_PREFIX)):
            return None
    return Phase.AGGREGATE


class ProjectionPlanner:
    """Allocate slots for declared measures + hidden slots for order/filter-only refs."""

    @staticmethod
    def _intern_hidden(registry: "ValueRegistry", key: ValueKey) -> None:
        """Intern ``key`` as a hidden slot unless it already has one (the shared hidden-slot rule)."""
        if registry.find_by_key(key) is None:
            registry.intern(
                key=key,
                declared_name=_canonical_name(key),
                hidden=True,
                phase=key.phase,
            )

    def plan(  # NOSONAR(S3776) — one sequential allocation pass (reserve names → intern measures → filter/order deps) sharing the same registry + hidden-slot rule; splitting the loops scatters that shared mutation invariant.
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
        # Claim every declared name before interning so hidden-name uniquification
        # is order-independent.
        registry.reserve_public_names(
            name
            for m in measures
            for name in (m.declared_name, m.public_name, m.canonical_alias)
        )
        public_projection: List[SlotId] = []
        for m in measures:
            # An all-placeholder regroup composite measure is AGGREGATE-phase, not
            # the ROW phase its leaves imply; a computed dimension is excluded.
            phase = m.bound.phase
            if not m.is_dimension:
                phase = (
                    _regroup_substituted_composite_phase(m.bound.value_key)
                    or phase
                )
            sid = registry.intern(
                key=m.bound.value_key,
                declared_name=m.declared_name,
                public_name=m.public_name,
                canonical_alias=m.canonical_alias,
                phase=phase,
                label=m.label,
                type=m.type,
                type_is_explicit=m.type_is_explicit,
                format=m.format,
                description=m.description,
                is_dimension=m.is_dimension,
            )
            public_projection.append(sid)
            # Materialise the measure's aux deps (inner aggregate, partition/time
            # columns) as hidden slots — rendered but not publicly projected.
            for dep in _iter_slot_deps(m.bound.value_key):
                if dep != m.bound.value_key:
                    self._intern_hidden(registry, dep)

        # Filter and order: intern each slot-worthy dep as a hidden slot.
        for f in filters:
            for dep in _iter_slot_deps(f.value_key):
                self._intern_hidden(registry, dep)

        for o in order:
            for dep in _iter_slot_deps(o.bound.value_key):
                self._intern_hidden(registry, dep)
            # _iter_slot_deps yields a composite's operands but not the composite
            # itself, so an ORDER BY on a composite had no slot and was silently
            # dropped. Intern the top-level key here (order only — a filter's
            # top-level composite renders inline into WHERE/HAVING).
            if isinstance(o.bound.value_key, (ArithmeticKey, ScalarCallKey)):
                self._intern_hidden(registry, o.bound.value_key)

        return ProjectionPlan(
            registry=registry,
            public_projection=public_projection,
            filters=filters,
            order=order,
        )


def _canonical_name(key: ValueKey) -> str:  # NOSONAR(S3776) — sequential isinstance dispatch over the closed ValueKey union; each branch is the per-type canonical-name contract. Extracting per-type helpers would scatter the contract.
    """Best-effort canonical name for a hidden slot (``revenue:sum`` → ``revenue_sum``, ``customers.regions.name`` → ``customers__regions__name``)."""
    if isinstance(key, ColumnKey):
        return "__".join(key.path + (key.leaf,))
    if isinstance(key, ColumnSqlKey):
        prefix = "__".join(key.path) + "__" if key.path else ""
        return f"{prefix}{key.column_name}"
    if isinstance(key, TimeTruncKey):
        # Granularity lives in the SQL DATE_TRUNC, not the alias.
        return _canonical_name(key.column)
    if isinstance(key, AggregateKey):
        # Include args/kwargs so parametric aggregates over one column
        # (``revenue:percentile(p=0.5)`` vs ``p=0.95``) get distinct names.
        alias = canonical_aggregate_alias(key, profile="declared_name")
        # declared_name profile always yields a name.
        assert alias is not None
        return alias
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
    if isinstance(key, BetweenKey):
        # Defensive: BetweenKey is always inlined into WHERE, never a public slot.
        return f"_between_{_canonical_name(key.column)}"
    if isinstance(key, InKey):
        # Defensive: InKey is always inlined into WHERE, never a public slot.
        return f"_in_{_canonical_name(key.column)}"
    return "_hidden"


ProjectionPlan.model_rebuild()


# Filter → slot id mapping for cross-model planner routing


def filter_referenced_slot_ids(
    bound_filter: "BoundFilter",
    registry: "ValueRegistry",
) -> "set":
    """``set[SlotId]`` that ``bound_filter``'s predicate references through interned slots (slot-worthy keys only; misses are skipped)."""
    result: set = set()
    for dep in _iter_slot_deps(bound_filter.value_key):
        sid = registry.find_by_key(dep)
        if sid is not None:
            result.add(sid)
    return result
