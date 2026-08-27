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
from slayer.sql.naming import canonical_aggregate_alias

__all__ = [
    "REGROUP_LEAF_PREFIX",
    "RegroupPlaceholderRegistry",
    "dimension_partitioned_aggregates",
    "combined_partitioned_aggregates",
    "classify_regroup_filter",
    "substitute_in_bound_filter",
    "reserved_prefix_columns",
]


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
        self._by_key: Dict[AggregateKey, ColumnKey] = {}

    def placeholder_for(self, key: AggregateKey) -> ColumnKey:
        existing = self._by_key.get(key)
        if existing is not None:
            return existing
        idx = len(self._by_key)
        seed = canonical_aggregate_alias(key, profile="stage_formula") or key.agg
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


def combined_partitioned_aggregates(  # NOSONAR(S3776) — one cohesive discovery walk over measures + orders; splitting scatters the shared seen-set / alias-map state.
    declared_measures, order_specs, *, row_agg_set: frozenset,
) -> Tuple[List[AggregateKey], Dict[AggregateKey, str]]:
    """Partitioned ``AggregateKey``s destined for a COMBINED attach (DEV-1829).

    Every LOCAL partitioned aggregate reachable from a NON-dimension measure or
    an order spec — a partitioned MEASURE or a composite / order leaf. In
    first-seen order, deduped by structural identity. A cross-model source
    (non-empty ``source.path``) is EXCLUDED (D2): those keep the DEV-1739
    cross-model narrow path, deferred to DEV-1824.

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
        for k in walk_value_keys(sp.bound.value_key):
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


def _top_level_refs(  # NOSONAR(S3776) — a single recursive ValueKey walker; the per-node-kind branches are the irreducible shape of the union.
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
