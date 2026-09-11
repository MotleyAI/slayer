"""The regroup primitive's structural core: an aggregation-based dimension groups
by a value that exists only after aggregating at a finer grain. Owns the pieces
shared by discovery and substitution, plus the position typing pass (every filter
conjunct / order target is a field or a measure); orchestration lives in
``stage_planner``."""

from __future__ import annotations

from typing import Dict, List, Mapping, NamedTuple, Optional, Tuple

from slayer.core.enums import DataType
from slayer.core.errors import PositionTypingError
from slayer.core.grain import Grain
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    substitute_value_keys,
)
from slayer.engine.binding import BoundFilter, walk_value_keys
from slayer.engine.planned import MaskTyping
from slayer.engine.ranked_planner import RANKED_AGGREGATIONS
from slayer.sql.naming import canonical_aggregate_alias

__all__ = [
    "REGROUP_LEAF_PREFIX",
    "ConjunctTyping",
    "RegroupPlaceholderRegistry",
    "dimension_partitioned_aggregates",
    "dimension_regroup_roots",
    "regroup_root_grain",
    "CombinedConsumers",
    "combined_consumer_aggregates",
    "is_local_combined_regroup_ref",
    "split_top_level_and",
    "substitute_in_bound_filter",
    "reserved_prefix_columns",
    "type_position_conjunct",
]

#: Reused from ``ranked_planner`` so the two stay in step.
_RANKED_AGGS = RANKED_AGGREGATIONS


def is_local_combined_regroup_ref(
    k: ValueKey, *, row_agg_set: frozenset = frozenset(),
) -> bool:
    """A LOCAL aggregate attached at the COMBINED SELECT (explicit ``partition_by=``
    or a bare windowed/first/last measure); ``row_agg_set`` aggregates excluded."""
    return (
        isinstance(k, AggregateKey)
        and not getattr(k.source, "path", ())
        and k not in row_agg_set
        and (
            k.partition_keys is not None
            or any(kw == "window" for kw, _ in k.kwargs)
            or k.agg in _RANKED_AGGS
        )
    )


def reserved_prefix_columns(model) -> list:
    """``model`` columns colliding with the reserved regroup placeholder prefix."""
    if model is None:
        return []
    return [c.name for c in getattr(model, "columns", []) or []
            if c.name.startswith(REGROUP_LEAF_PREFIX)]


class RegroupPlaceholderRegistry:
    """Mints a distinct reserved-leaf ``ColumnKey`` per structural aggregate, keyed
    by ``AggregateKey`` identity NOT canonical alias (which two distinct aggregates
    can share); a monotonic index guarantees distinct leaves."""

    def __init__(self) -> None:
        self._by_key: Dict[ValueKey, ColumnKey] = {}

    def placeholder_for(self, key: ValueKey) -> ColumnKey:
        existing = self._by_key.get(key)
        if existing is not None:
            return existing
        idx = len(self._by_key)
        seed = (
            (canonical_aggregate_alias(key, profile="stage_formula")
             if isinstance(key, AggregateKey) else None)
            or getattr(key, "agg", None)
            or getattr(key, "op", None)
            or "regroup"
        )
        placeholder = ColumnKey(path=(), leaf=f"{REGROUP_LEAF_PREFIX}{idx}__{seed}")
        self._by_key[key] = placeholder
        return placeholder


def dimension_partitioned_aggregates(declared_measures) -> List[AggregateKey]:
    """Partitioned ``AggregateKey``s inside computed-dimension measures, first-seen, deduped."""
    seen: set = set()
    out: List[AggregateKey] = []
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        for k in walk_value_keys(dm.bound.value_key):
            if (
                isinstance(k, AggregateKey)
                and k.partition_keys is not None
                and k not in seen
            ):
                seen.add(k)
                out.append(k)
    return out


def _grained_inner_aggregates(vk: ValueKey) -> List[AggregateKey]:
    return [
        k for k in walk_value_keys(vk)
        if isinstance(k, AggregateKey) and k.partition_keys is not None
    ]


def regroup_root_grain(root: ValueKey) -> Grain:
    """Producer grain of a row-attach root: a transform evaluates at the set-union
    of ALL inner aggregates' partition grains; a bare aggregate at its own grain."""
    if isinstance(root, TransformKey):
        grain = Grain.EMPTY
        for inner in _grained_inner_aggregates(root.input):
            grain = grain | (inner.partition_keys or frozenset())
        return grain
    return Grain.of(getattr(root, "partition_keys", None) or frozenset())


def dimension_regroup_roots(declared_measures) -> List[ValueKey]:  # NOSONAR(S3776) — one discovery walk; the transform-root and bare-aggregate arms share the seen/covered state, so splitting scatters it.
    """Row-attach producer ROOTS inside computed dimensions: a transform over an
    explicitly-grained aggregate (evaluated at the PRODUCER grain, so ``rank`` ranks
    partitions), or a bare partitioned aggregate not under such a transform."""
    seen: set = set()
    out: List[ValueKey] = []
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        all_keys = list(walk_value_keys(dm.bound.value_key))
        transform_roots = [
            k for k in all_keys
            if isinstance(k, TransformKey) and _grained_inner_aggregates(k.input)
        ]
        covered: set = set()
        for t in transform_roots:
            covered.update(walk_value_keys(t.input))
        for k in all_keys:
            if isinstance(k, TransformKey) and k in transform_roots:
                pass  # keep: transform root, emitted below
            elif (
                isinstance(k, AggregateKey)
                and k.partition_keys is not None
                and k not in covered
            ):
                pass  # keep: uncovered partitioned aggregate, emitted below
            else:
                continue
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


class CombinedConsumers(NamedTuple):
    """Aggregates consumed in a COMBINED position — a non-dimension measure, a
    composite operand, a transform input, a raw ORDER target, or a filter-only
    reference — bucketed by routing need. The partitioned buckets (``local`` and
    ``cross_model``) require query-dimension partition keys for the join-back; the
    cross-model bare bucket carries no such constraint. ``public_alias`` /
    ``declared_type`` map a directly-selected aggregate to its measure name / explicit type."""

    local_partitioned: List[AggregateKey]
    cross_model_partitioned: List[AggregateKey]
    cross_model_bare: List[AggregateKey]
    public_alias: Dict[AggregateKey, str]
    declared_type: Dict[AggregateKey, DataType]


def _combined_consumer_kind(k: ValueKey) -> Optional[str]:
    """Combined-consumer bucket for ``k``, or ``None`` (a local BARE aggregate routes
    via ``_bare_combined_roots``; a host-grain wrap roots at the host, not here)."""
    if not isinstance(k, AggregateKey):
        return None
    partitioned = k.partition_keys is not None
    if not getattr(k.source, "path", ()):
        return "local_partitioned" if partitioned else None
    if getattr(k, "grain", "target") == "host":
        return None
    return "cross_model_partitioned" if partitioned else "cross_model_bare"


def combined_consumer_aggregates(  # NOSONAR(S3776) — one cohesive discovery walk over measures + orders + filters; splitting scatters the shared seen-set / bucket / alias-map state.
    declared_measures, order_specs, *, row_agg_set: frozenset,
    bound_filters=(),
    measure_typed_filter_indices: frozenset = frozenset(),
    dim_keys: frozenset = frozenset(),
) -> CombinedConsumers:
    """One walk discovering every partitioned / cross-model ``AggregateKey`` destined
    for a COMBINED attach, reachable from a non-dimension measure, order spec, or
    filter. ``row_agg_set`` (partitioned aggregates already carrying a computed-dimension
    ROW role) is excluded from ORDER-name and field-typed filter references — but NOT
    from measure use or a measure-typed filter (its index in
    ``measure_typed_filter_indices``), which evaluate at query grain — so a row-scope
    reference stays row-routed while a genuine combined consumer is kept. Local and
    cross-model partitioned aggregates share these asymmetric exclusions; a cross-model
    bare aggregate is kept from any position."""
    buckets: Dict[str, List[AggregateKey]] = {
        "local_partitioned": [],
        "cross_model_partitioned": [],
        "cross_model_bare": [],
    }
    public_alias: Dict[AggregateKey, str] = {}
    declared_type: Dict[AggregateKey, DataType] = {}
    seen: set = set()

    def _add(k: ValueKey) -> None:
        if k in seen:
            return
        kind = _combined_consumer_kind(k)
        if kind is None:
            return
        seen.add(k)
        buckets[kind].append(k)

    def _walk_excluding_row(vk: ValueKey) -> None:
        # ORDER-by-name and a filter over the dim's own aggregate are row-scope refs.
        for k in walk_value_keys(vk):
            kind = _combined_consumer_kind(k)
            if kind in ("local_partitioned", "cross_model_partitioned") and k in row_agg_set:
                continue
            _add(k)

    def _walk_measure(k: ValueKey) -> None:
        # A partition-key subtree references the GROUPED dimension value; its
        # inner aggregates keep their ROW role (DEV-1847 shape B).
        _add(k)
        embedded_pks = frozenset(getattr(k, "partition_keys", None) or ())
        for c in k.children():
            if c not in embedded_pks:
                _walk_measure(c)

    for dm in declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        # A MEASURE keeps a dual-role partitioned aggregate (no row exclusion) so it is strict-checked.
        _walk_measure(vk)
        top = _combined_consumer_kind(vk)
        if top is not None and dm.public_name is not None:
            public_alias.setdefault(vk, dm.public_name)
        if (
            top in ("cross_model_partitioned", "cross_model_bare")
            and dm.type_is_explicit and dm.type is not None
        ):
            declared_type.setdefault(vk, dm.type)
    for sp in order_specs:
        top = sp.bound.value_key
        # A RAW partitioned aggregate as the order target is a combined consumer (rejected if keyless).
        if _combined_consumer_kind(top) in ("local_partitioned", "cross_model_partitioned"):
            _add(top)
            continue
        _walk_excluding_row(top)
    def _walk_grain_aware(k: ValueKey) -> None:
        # A subtree equal to a query dimension's bound key references the
        # GROUPED value — its inner aggregates keep their ROW role.
        if k in dim_keys:
            return
        _add(k)
        for c in k.children():
            _walk_grain_aware(c)

    for i, bf in enumerate(bound_filters or ()):
        if i in measure_typed_filter_indices:
            _walk_grain_aware(bf.value_key)
        else:
            _walk_excluding_row(bf.value_key)
    return CombinedConsumers(
        local_partitioned=buckets["local_partitioned"],
        cross_model_partitioned=buckets["cross_model_partitioned"],
        cross_model_bare=buckets["cross_model_bare"],
        public_alias=public_alias,
        declared_type=declared_type,
    )


def split_top_level_and(vk: ValueKey) -> List[ValueKey]:
    """Top-level AND conjuncts; only ``and`` splits (OR/comparisons stay whole)."""
    if isinstance(vk, ArithmeticKey) and vk.op == "and":
        out: List[ValueKey] = []
        for o in vk.operands:
            out.extend(split_top_level_and(o))
        return out
    return [vk]


class ConjunctTyping(NamedTuple):
    """One position expression's typing: field/measure + stratum (0 = base-row population)."""

    typing: MaskTyping
    stratum: int


def _field_blockers(cj: ValueKey, row_agg_set: frozenset) -> Tuple[List[ValueKey], bool]:
    """(aggregates/transforms blocking field typing, saw-attached-ref); a ``row_agg_set``
    aggregate resolves to its row-attached value and counts as aggregate-free."""
    blockers: List[ValueKey] = []
    attached = False

    def _walk(k: ValueKey) -> None:
        nonlocal attached
        if isinstance(k, (AggregateKey, TransformKey)):
            # A row-attach root (partitioned aggregate or transform root of a
            # computed dimension) resolves row-side; its subtree is its own scope.
            if k in row_agg_set:
                attached = True
            else:
                blockers.append(k)
            return
        for c in k.children():
            _walk(c)

    _walk(cj)
    return blockers, attached


def _measure_blockers(cj: ValueKey, dim_keys: frozenset) -> List[ValueKey]:
    """Row-level refs outside aggregate subtrees not available at query grain.
    A subtree equal to a query dimension's bound key IS the grouped value —
    available at query grain wholesale."""
    blockers: List[ValueKey] = []

    def _walk(k: ValueKey) -> None:
        if k in dim_keys or isinstance(k, AggregateKey):
            return
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            blockers.append(k)
            return
        if isinstance(k, TransformKey):
            _walk(k.input)  # partition/time keys are transform machinery, not refs
            return
        for c in k.children():
            _walk(c)

    _walk(cj)
    return blockers


def _key_display(k: ValueKey) -> str:
    if isinstance(k, AggregateKey):
        leaf = getattr(k.source, "leaf", None) or getattr(k.source, "column_name", None) or "*"
        path = getattr(k.source, "path", ())
        name = f"{'.'.join((*path, leaf))}:{k.agg}"
        return f"{name} (partition_by)" if k.partition_keys is not None else name
    if isinstance(k, TransformKey):
        return f"{k.op}(...)"
    if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        col = k.column if isinstance(k, TimeTruncKey) else k
        leaf = getattr(col, "leaf", None) or getattr(col, "column_name", "?")
        return ".".join((*col.path, leaf))
    return type(k).__name__


def type_position_conjunct(
    cj: ValueKey,
    *,
    dim_keys: frozenset,
    row_agg_set: frozenset = frozenset(),
    has_measure_position: bool = True,
    position: str = "filter",
) -> ConjunctTyping:
    """Type one conjunct as field (aggregate-free after resolution; attached refs count
    as row-level) else measure (every bare ref available at query grain), else raise
    :class:`PositionTypingError` naming both failures. Field wins a tie."""
    field_blockers, attached = _field_blockers(cj, row_agg_set)
    if not field_blockers:
        return ConjunctTyping(MaskTyping.FIELD, 1 if attached else 0)
    if has_measure_position:
        measure_blockers = _measure_blockers(cj, dim_keys)
        if not measure_blockers:
            return ConjunctTyping(MaskTyping.MEASURE, 1)
        raise PositionTypingError(
            f"This {position} expression is valid as neither a field nor a "
            f"measure. Field typing failed: it references "
            f"{', '.join(_key_display(k) for k in field_blockers)}, available "
            f"only after aggregation. Measure typing failed: it references "
            f"row-level {', '.join(_key_display(k) for k in measure_blockers)}, "
            f"not available at the query grain (not among the query "
            f"dimensions). Split the top-level AND conjuncts so each resolves "
            f"in one typing, or add the row-level reference to the query "
            f"dimensions."
        )
    raise PositionTypingError(
        f"This {position} expression references "
        f"{', '.join(_key_display(k) for k in field_blockers)}, so it is not a "
        f"field, and measure typing is unavailable because the query has no "
        f"measure position (distinct_dimension_values=False)."
    )


def substitute_in_bound_filter(
    bf: BoundFilter, mapping: Mapping[ValueKey, ValueKey],
) -> BoundFilter:
    """Substitute placeholders in a filter and RECOMPUTE its phase (may lower to ROW)."""
    new_vk = substitute_value_keys(key=bf.value_key, mapping=mapping)
    refs = tuple(walk_value_keys(new_vk))
    phase = max((k.phase for k in refs), default=new_vk.phase)
    return BoundFilter(value_key=new_vk, phase=phase, referenced_keys=refs)
