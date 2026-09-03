"""The regroup primitive's structural core: an aggregation-based dimension groups
by a value that exists only after aggregating at a finer grain. Owns the pieces
shared by discovery and substitution; orchestration lives in ``stage_planner``."""

from __future__ import annotations

from typing import Dict, List, Mapping, NamedTuple, Optional, Tuple

from slayer.core.enums import DataType
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    Phase,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    substitute_value_keys,
)
from slayer.engine.binding import BoundFilter, walk_value_keys
from slayer.engine.ranked_planner import RANKED_AGGREGATIONS
from slayer.sql.naming import canonical_aggregate_alias

__all__ = [
    "REGROUP_LEAF_PREFIX",
    "RegroupPlaceholderRegistry",
    "dimension_partitioned_aggregates",
    "dimension_regroup_roots",
    "regroup_root_grain",
    "CombinedConsumers",
    "combined_consumer_aggregates",
    "is_local_combined_regroup_ref",
    "split_top_level_and",
    "conjunct_scope",
    "classify_regroup_filter",
    "substitute_in_bound_filter",
    "reserved_prefix_columns",
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


def regroup_root_grain(root: ValueKey) -> frozenset:
    """Producer grain of a row-attach root: a transform evaluates at the set-union
    of ALL inner aggregates' partition grains; a bare aggregate at its own grain."""
    if isinstance(root, TransformKey):
        grain: set = set()
        for inner in _grained_inner_aggregates(root.input):
            grain |= (inner.partition_keys or frozenset())
        return frozenset(grain)
    return getattr(root, "partition_keys", None) or frozenset()


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
) -> CombinedConsumers:
    """One walk discovering every partitioned / cross-model ``AggregateKey`` destined
    for a COMBINED attach, reachable from a non-dimension measure, order spec, or
    filter. ``row_agg_set`` (partitioned aggregates already carrying a computed-dimension
    ROW role) is excluded from ORDER-name and filter references — but NOT from measure
    use — so a row-scope reference stays row-routed while a genuine combined consumer is
    kept. Local and cross-model partitioned aggregates share these asymmetric exclusions;
    a cross-model bare aggregate is kept from any position."""
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

    for dm in declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        # A MEASURE keeps a dual-role partitioned aggregate (no row exclusion) so it is strict-checked.
        for k in walk_value_keys(vk):
            _add(k)
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
    for bf in (bound_filters or ()):
        _walk_excluding_row(bf.value_key)
    return CombinedConsumers(
        local_partitioned=buckets["local_partitioned"],
        cross_model_partitioned=buckets["cross_model_partitioned"],
        cross_model_bare=buckets["cross_model_bare"],
        public_alias=public_alias,
        declared_type=declared_type,
    )


# Non-literal predicate-dependency leaves; a dim-aggregate is terminal but classified apart.
_REF_LEAVES = (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey, AggregateKey)


def _top_level_refs(  # NOSONAR(S3776) — one flat structural walk over the ValueKey union (arithmetic / scalar-call / between / in / transform / leaves); each arm is independently trivial and splitting would fragment a single recursive dispatch
    vk: ValueKey, dim_agg_set: frozenset,
) -> Tuple[List[AggregateKey], List[ValueKey]]:
    """Split ``vk``'s refs into (dim-aggregates, other non-literal refs), not descending into a dim-aggregate's subtree."""
    dim_hits: List[AggregateKey] = []
    other: List[ValueKey] = []

    def _walk(k: ValueKey) -> None:
        if isinstance(k, AggregateKey) and k in dim_agg_set:
            dim_hits.append(k)
            return
        if isinstance(k, _REF_LEAVES):
            other.append(k)
            return
        if isinstance(k, ArithmeticKey):
            for o in k.operands:
                _walk(o)
        elif isinstance(k, ScalarCallKey):
            for a in k.args:
                if isinstance(a, (ArithmeticKey, ScalarCallKey, BetweenKey, InKey,
                                  TransformKey, *_REF_LEAVES)):
                    _walk(a)
        elif isinstance(k, BetweenKey):
            _walk(k.column)
            _walk(k.low)
            _walk(k.high)
        elif isinstance(k, InKey):
            _walk(k.column)
        elif isinstance(k, TransformKey):
            _walk(k.input)
        # LiteralKey / unknown scalars: not a reference.

    _walk(vk)
    return dim_hits, other


def split_top_level_and(vk: ValueKey) -> List[ValueKey]:
    """Top-level AND conjuncts; only ``and`` splits (OR/comparisons stay whole)."""
    if isinstance(vk, ArithmeticKey) and vk.op == "and":
        out: List[ValueKey] = []
        for o in vk.operands:
            out.extend(split_top_level_and(o))
        return out
    return [vk]


def conjunct_scope(
    vk: ValueKey, *, dim_keys: frozenset, row_agg_set: frozenset = frozenset(),
) -> str:
    """Availability scope of a filter conjunct: ``"combined"`` (a LOCAL combined
    partitioned aggregate with every operand resolving post-attach → outer WHERE)
    or ``"other"``; raises when one shares a conjunct with a base-row-only operand."""
    _, refs = _top_level_refs(vk=vk, dim_agg_set=frozenset())
    partitioned = [
        k for k in refs
        if is_local_combined_regroup_ref(k=k, row_agg_set=row_agg_set)
    ]
    if not partitioned:
        return "other"
    base_row_only = [
        k for k in refs
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey))
        and k not in dim_keys
    ]
    if base_row_only:
        raise ValueError(
            "A single filter predicate mixes a partition_by aggregate (available "
            "only after aggregation and attachment) with a row-level reference "
            "(available only before aggregation); they have no common scope. "
            "Rewrite so each top-level AND conjunct's references resolve in one "
            "scope — an OR across the two cannot be split without changing meaning."
        )
    return "combined"


def classify_regroup_filter(bf: BoundFilter, dim_agg_set: frozenset) -> str:
    """``"row_inherit"`` (base-row: copied into producer AND kept) / ``"final_only"``
    (only dim-aggregates: kept in consumer) / ``"standard"`` (agg/post: untouched);
    raises on a predicate mixing a dim-aggregate with another non-literal ref."""
    dim_hits, other = _top_level_refs(vk=bf.value_key, dim_agg_set=dim_agg_set)
    if dim_hits and other:
        raise NotImplementedError(
            "A single filter that mixes a computed-dimension aggregate with "
            "another predicate cannot be routed across the regroup boundary "
            "(one is grouped by the synthesized stage, the other filters raw "
            "rows). Put them in separate filters (DEV-1825)."
        )
    if dim_hits:
        return "final_only"
    if bf.phase == Phase.ROW:
        return "row_inherit"
    return "standard"


def substitute_in_bound_filter(
    bf: BoundFilter, mapping: Mapping[ValueKey, ValueKey],
) -> BoundFilter:
    """Substitute placeholders in a filter and RECOMPUTE its phase (may lower to ROW)."""
    new_vk = substitute_value_keys(key=bf.value_key, mapping=mapping)
    refs = tuple(walk_value_keys(new_vk))
    phase = max((k.phase for k in refs), default=new_vk.phase)
    return BoundFilter(value_key=new_vk, phase=phase, referenced_keys=refs)
