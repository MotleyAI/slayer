"""Bound expression forms: the binder's output types, read by the renderer."""

from __future__ import annotations

from typing import Dict, List, NamedTuple, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    AggregateKey,
    Phase,
    TransformKey,
    ValueKey,
    grained_inner_aggregates,
    is_row_attach_root,
    walk_consumer_keys,
    walk_value_keys,
)


class BoundFilter(BaseModel):
    """A bound filter predicate: ``value_key`` (like ``BoundExpr``), ``phase``
    (max phase any referenced slot reaches), and ``referenced_keys`` (every
    ``ValueKey`` in the tree, for the cross-model planner's filter routing)."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey
    phase: Phase
    referenced_keys: Tuple[ValueKey, ...] = Field(default_factory=tuple)


class BoundExpr(BaseModel):
    """A bound expression — its leaves are resolved ``ValueKey``s. ``routed_dotted``
    is the full routed dotted path when the whole field is a short-form
    ``DottedRef`` that auto-routed (DEV-1856), else ``None`` — the naming layer
    surfaces a routed dimension under this full path, not the short form typed."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey
    routed_dotted: Optional[str] = None

    @property
    def phase(self) -> Phase:
        return self.value_key.phase


def bound_filter_from_key(vk: ValueKey) -> BoundFilter:
    """A ``BoundFilter`` over ``vk`` with phase and referenced keys recomputed."""
    refs = tuple(walk_value_keys(vk))
    phase = max((k.phase for k in refs), default=vk.phase)
    return BoundFilter(value_key=vk, phase=phase, referenced_keys=refs)


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
    preserve_native_type: bool = False
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


def dimension_partitioned_aggregates(declared_measures) -> List[AggregateKey]:
    """Partitioned ``AggregateKey``s inside computed-dimension measures, first-seen, deduped."""
    seen: set = set()
    out: List[AggregateKey] = []
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        # Opaque below a row-attach root's inputs — its inners belong to its own
        # producer, not a separate dimension attach (DEV-1859).
        for k in walk_consumer_keys(dm.bound.value_key):
            if (
                isinstance(k, AggregateKey)
                and k.partition_keys is not None
                and k not in seen
            ):
                seen.add(k)
                out.append(k)
    return out


def dimension_regroup_roots(declared_measures) -> List[ValueKey]:  # NOSONAR(S3776) — one discovery walk; the transform-root and bare-aggregate arms share the seen/covered state, so splitting scatters it.
    """Row-attach producer ROOTS inside computed dimensions: a transform over an
    explicitly-grained aggregate (evaluated at the PRODUCER grain, so ``rank`` ranks
    partitions), or a bare partitioned aggregate not under such a transform."""
    seen: set = set()
    out: List[ValueKey] = []
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        all_keys = list(walk_consumer_keys(dm.bound.value_key))
        transform_roots = [
            k for k in all_keys
            if isinstance(k, TransformKey) and grained_inner_aggregates(k.input)
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
    if k.locus == "host":
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
        # Opaque below a row-attach root's inputs (DEV-1859).
        for k in walk_consumer_keys(vk):
            kind = _combined_consumer_kind(k)
            if kind in ("local_partitioned", "cross_model_partitioned") and k in row_agg_set:
                continue
            _add(k)

    def _walk_measure(k: ValueKey) -> None:
        # A partition-key subtree references the GROUPED dimension value; its
        # inner aggregates keep their ROW role (DEV-1847 shape B).
        _add(k)
        # A row-attach root's inputs belong to its own attach, never a combined
        # consumer here (DEV-1859).
        if is_row_attach_root(k):
            return
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
        # Opaque below a row-attach root's inputs; partition keys stay visible.
        if is_row_attach_root(k):
            for pk in (k.partition_keys or ()):
                _walk_grain_aware(pk)
            return
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
