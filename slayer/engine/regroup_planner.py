"""DEV-1825 — the regroup primitive's structural core.

An aggregation-based dimension (``CASE WHEN amount:sum(partition_by=city) >
5000 THEN 1 ELSE 0 END`` grouped by region) groups by a value that only exists
AFTER aggregating at a finer grain. The planner synthesizes a *producer* stage
computing that aggregate at its partition grain, attaches it to the raw rows on
that grain (null-safe), and substitutes each consumed partitioned aggregate —
by structural key identity, never text — with a reserved-leaf placeholder
column that resolves to the producer's output at render time.

This module owns the pieces that must stay identical between discovery and
substitution: the placeholder registry (injective by ``AggregateKey``
identity), the discovery walk, the filter classifier, and the phase-recomputing
substitution. The orchestration that plans producers and builds
``RegroupAttachPlan``s lives in ``stage_planner`` (where the scope / metadata
helpers are), consuming these.
"""

from __future__ import annotations

from typing import Dict, List, Mapping, Tuple

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
    "combined_partitioned_aggregates",
    "is_local_combined_regroup_ref",
    "split_top_level_and",
    "conjunct_scope",
    "classify_regroup_filter",
    "substitute_in_bound_filter",
    "reserved_prefix_columns",
]

#: The aggregations that rank (reused from ``ranked_planner`` so they stay in
#: step); a bare ``first``/``last`` attaches at the combined SELECT like a
#: windowed one.
_RANKED_AGGS = RANKED_AGGREGATIONS


def is_local_combined_regroup_ref(
    k: ValueKey, *, row_agg_set: frozenset = frozenset(),
) -> bool:
    """A LOCAL aggregate the regroup primitive attaches at the COMBINED SELECT:
    an explicit ``partition_by=`` (DEV-1829) or a bare windowed / ``first`` /
    ``last`` measure (DEV-1835 D1). A computed-dimension row-attach aggregate is
    excluded via ``row_agg_set`` — it resolves through the base scope, not the
    combined one."""
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
    """Any column names on ``model`` that collide with the reserved regroup
    placeholder prefix — rejected at plan time when a regroup is active so a
    real ``__regroup__*`` column can never shadow a placeholder at render."""
    if model is None:
        return []
    return [c.name for c in getattr(model, "columns", []) or []
            if c.name.startswith(REGROUP_LEAF_PREFIX)]


# --------------------------------------------------------------------------- #
# Placeholder minting — injective by structural AggregateKey identity.
# --------------------------------------------------------------------------- #
class RegroupPlaceholderRegistry:
    """Mints a distinct reserved-leaf ``ColumnKey`` per structural aggregate.

    Keyed by ``AggregateKey`` identity (hash/eq), NOT by ``canonical_aggregate_
    alias`` — that alias omits ``column_filter_key`` and its partition suffix is
    display-based, so two structurally distinct aggregates can share it. A
    per-registry monotonic index guarantees distinct leaves regardless
    (``__regroup__<n>__<canonical>``); the canonical text is a readable seed.
    """

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


# --------------------------------------------------------------------------- #
# Discovery.
# --------------------------------------------------------------------------- #
def dimension_partitioned_aggregates(declared_measures) -> List[AggregateKey]:
    """Every partitioned ``AggregateKey`` inside a computed-dimension measure,
    in first-seen order, deduped by structural identity."""
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
    """Partitioned ``AggregateKey``s reachable inside ``vk``."""
    return [
        k for k in walk_value_keys(vk)
        if isinstance(k, AggregateKey) and k.partition_keys is not None
    ]


def regroup_root_grain(root: ValueKey) -> frozenset:
    """The producer grain of a row-attach root (DEV-1839 D1): a transform over
    grained aggregates evaluates at the set-union of ALL its inner aggregates'
    partition grains (any nesting depth; keyless contributes ∅); a bare
    partitioned aggregate at its own grain. When all inner aggregates share one
    grain the union degenerates to that grain (DEV-1824 behaviour unchanged)."""
    if isinstance(root, TransformKey):
        grain: set = set()
        for inner in _grained_inner_aggregates(root.input):
            grain |= (inner.partition_keys or frozenset())
        return frozenset(grain)
    return getattr(root, "partition_keys", None) or frozenset()


def dimension_regroup_roots(declared_measures) -> List[ValueKey]:  # NOSONAR(S3776) — one discovery walk; the transform-root and bare-aggregate arms share the seen/covered state, so splitting scatters it.
    """Row-attach producer ROOTS inside computed dimensions (DEV-1824 D3/D4).

    A transform whose input is an explicitly-grained aggregate is a
    ``self_contained`` root evaluated at the PRODUCER grain (so ``rank(...)`` as a
    dimension ranks partitions, not rows); a bare partitioned aggregate not
    already inside such a transform is a root too. First-seen order, deduped by
    structural identity."""
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
                pass  # a transform root
            elif (
                isinstance(k, AggregateKey)
                and k.partition_keys is not None
                and k not in covered
            ):
                pass  # a bare partitioned aggregate not under a transform root
            else:
                continue
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


def combined_partitioned_aggregates(  # NOSONAR(S3776) — one cohesive discovery walk over measures + orders + filters; splitting scatters the shared seen-set / alias-map state.
    declared_measures, order_specs, *, row_agg_set: frozenset,
    bound_filters=(),
) -> Tuple[List[AggregateKey], Dict[AggregateKey, str]]:
    """Partitioned ``AggregateKey``s destined for a COMBINED attach (DEV-1829).

    Every LOCAL partitioned aggregate reachable from a NON-dimension measure, an
    order spec, or a query filter — a partitioned MEASURE, a composite / order
    leaf, or a filter reference (DEV-1824). In first-seen order, deduped by
    structural identity. A cross-model source (non-empty ``source.path``) is
    EXCLUDED (D2): those keep the DEV-1739 cross-model narrow path, deferred to
    DEV-1824.

    ``row_agg_set`` (the computed-dimension / row-attach aggregates) is treated
    asymmetrically (CR): a MEASURE use of such a key IS a genuine row+combined
    coexistence and is kept here so ``_plan_regroups`` rejects it (DEV-1824)
    rather than silently rewriting the measure to a row placeholder; but an
    ORDER BY over a computed dimension merely references that dimension's own
    row attach, so an order-spec key already in ``row_agg_set`` is excluded (it
    is not a combined attach and must not trip the coexistence guard).

    The second return is the public-alias map for producer naming (F1 / D4): a
    directly-named measure whose value_key IS the partitioned aggregate maps to
    its public name; a composite / order leaf is absent (rendered under the
    canonical alias)."""

    def _is_local_combined(k: ValueKey) -> bool:
        return (
            isinstance(k, AggregateKey)
            and k.partition_keys is not None
            and not getattr(k.source, "path", ())
        )

    seen: set = set()
    out: List[AggregateKey] = []
    public_alias_by_agg: Dict[AggregateKey, str] = {}
    for dm in declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        for k in walk_value_keys(vk):
            if _is_local_combined(k) and k not in seen:
                seen.add(k)
                out.append(k)
        if _is_local_combined(vk) and dm.public_name is not None:
            public_alias_by_agg.setdefault(vk, dm.public_name)
    for sp in order_specs:
        top = sp.bound.value_key
        # DEV-1824 D9 — ordering by the RAW partitioned aggregate (a top-level
        # AggregateKey) routes to a COMBINED attach even when the same key is a
        # computed-dimension (row) aggregate; ordering by the dimension NAME (a
        # composite value_key) keeps the row routing, so it stays excluded below.
        if _is_local_combined(top):
            if top not in seen:
                seen.add(top)
                out.append(top)
            continue
        for k in walk_value_keys(top):
            if _is_local_combined(k) and k not in seen and k not in row_agg_set:
                seen.add(k)
                out.append(k)
    # DEV-1824 — a partitioned aggregate referenced in a query filter needs its
    # own combined producer + placeholder; a filter over a computed dimension's
    # own aggregate (in row_agg_set) is a row-attach reference, excluded here.
    for bf in (bound_filters or ()):
        for k in walk_value_keys(bf.value_key):
            if _is_local_combined(k) and k not in seen and k not in row_agg_set:
                seen.add(k)
                out.append(k)
    return out, public_alias_by_agg


# --------------------------------------------------------------------------- #
# Filter classification — total over the PRE-substitution tree.
# --------------------------------------------------------------------------- #
# Reference leaves that count as a NON-LITERAL predicate dependency (a value the
# filter reads from a row or an aggregate). A dim-aggregate is terminal too but
# is classified separately.
_REF_LEAVES = (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey, AggregateKey)


def _top_level_refs(  # NOSONAR(S3776) — one flat structural walk over the ValueKey union (arithmetic / scalar-call / between / in / transform / leaves); each arm is independently trivial and splitting would fragment a single recursive dispatch
    vk: ValueKey, dim_agg_set: frozenset,
) -> Tuple[List[AggregateKey], List[ValueKey]]:
    """Split ``vk``'s references into (discovered dim-aggregates, other
    non-literal refs), WITHOUT descending into a dim-aggregate's own subtree —
    so ``band == 1`` reports only the dim-aggregate, never the ``city`` /
    ``amount`` inside it."""
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
    """Top-level AND conjuncts of a filter tree (DEV-1824 D7). A boolean AND
    binds to ``ArithmeticKey(op='and', …)``; OR / comparisons / everything else
    stay whole so only genuinely independent predicates are routed apart."""
    if isinstance(vk, ArithmeticKey) and vk.op == "and":
        out: List[ValueKey] = []
        for o in vk.operands:
            out.extend(split_top_level_and(o))
        return out
    return [vk]


def conjunct_scope(
    vk: ValueKey, *, dim_keys: frozenset, row_agg_set: frozenset = frozenset(),
) -> str:
    """Availability scope of a filter conjunct (DEV-1824 D7).

    ``"combined"`` — references a LOCAL COMBINED partitioned aggregate (available
    only after attachment) and every other operand also resolves there (a
    dimension, a plain aggregate, or a literal): routes to the outer WHERE.
    ``"other"`` — references no combined partitioned aggregate: normal phase
    routing (a computed-dimension aggregate in ``row_agg_set`` is a row attach,
    handled by ``classify_regroup_filter``, not routed here).
    Raises (split the filter) when a combined partitioned aggregate shares the
    conjunct with a base-row-only operand — a raw column that is not a query
    dimension, resolvable only before aggregation — so the two have no common
    scope.
    """
    _, refs = _top_level_refs(vk=vk, dim_agg_set=frozenset())
    partitioned = [
        k for k in refs
        if is_local_combined_regroup_ref(k, row_agg_set=row_agg_set)
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
    """One of ``"row_inherit"`` / ``"final_only"`` / ``"standard"``; raises on a
    single predicate that mixes a computed-dimension aggregate with another
    non-literal reference (unroutable — the two live in different stages).

    * ``"row_inherit"`` — a base-row (ROW-phase, no dim-aggregate) predicate:
      copied into the producer AND kept in the consumer (DEV-1739 rule).
    * ``"final_only"`` — references only computed-dimension aggregates (+
      literals): kept in the consumer, never pushed into a producer.
    * ``"standard"`` — an aggregate/post predicate with no dim-aggregate:
      existing routing, untouched.
    """
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
    """Substitute placeholders in a filter and RECOMPUTE its phase — the swap
    lowers an aggregate-phase ``band == 1`` predicate to ROW."""
    new_vk = substitute_value_keys(key=bf.value_key, mapping=mapping)
    refs = tuple(walk_value_keys(new_vk))
    phase = max((k.phase for k in refs), default=new_vk.phase)
    return BoundFilter(value_key=new_vk, phase=phase, referenced_keys=refs)
