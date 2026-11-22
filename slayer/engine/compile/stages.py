"""The stage compiler: one elaborated stage or synthesized producer → ``PlannedQuery``.
Binding lives in ``bind_inputs``; typing and the checker in ``elaborate_env``."""

from __future__ import annotations

import functools
import hashlib
import itertools
from decimal import Decimal
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Hashable,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    cast,
)

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import AXIS_COLLAPSING_TRANSFORMS, DataType, RelationshipCardinality, JoinType
from slayer.core.errors import AmbiguousRelationshipPathError, UnresolvableDimensionRelationshipError
from slayer.core.keys import SLOT_COMPOSITE_KINDS, AggregateKey, AggregateSource, Grain, ArithmeticKey, FieldKey, FieldSqlKey, InKey, LiteralKey, Phase, PREDICATE_COMPARISON_OPS, StarKey, TimeTruncKey, TransformKey, ValueKey, field_leaf, effective_root_grain, constituent_grain, attached_parameter_grain, substitute_value_keys, substitute_consumer_keys, walk_value_keys, REGROUP_LEAF_PREFIX, is_kernel_requiring, is_ranked, restricts_own_rows, input_masked, split_top_level_and, window_kwarg_of, is_row_attach_root, attached_inputs, NULL_SAFE_COMPARISON_OPS, NON_IDENTITY_KEY_FIELDS, OPTIONAL_KEY_FIELDS, operand_aggregates, operand_constituents, parameter_row_leaves, source_anchor_path, source_row_leaves, shift_offset_of, value_type, VALUE_KEY_TYPES
from slayer.core.time_spine import is_spine
from slayer.core.models import DatasetField, EmptyValue, SlayerDataset, aggregation_definition, empty_value
from slayer.core.refs import EXPRESSION_SOURCE_KINDS, first_bucket_remedy, key_display, legacy_class_name
from slayer.engine.reference_closure import (
    aggregate_input_closure,
    field_default_key,
    first_unanalyzable_filter_field,
    first_unanalyzable_input_field,
    first_unanalyzable_source_row_leaf,
    fragment_closure,
    fragment_null_propagates,
    key_closure,
    source_row_leaf_closure,
)
from slayer.core.relationship_walker import (
    aggregation_owner,
    canonical_token,
    follow,
    relationship_key_fields,
    dataset_field_type,
    reverse_token,
)
from slayer.engine.cardinality.relationship_safety import (
    UNREACHABLE_NO_PATH,
    attributable_from_root,
    broadcast_reason,
    crossing_local_root_predicate,
    determination_broadcast_reason,
    grain_determines,
    grain_member_attributable,
    grain_witness,
    key_attributable_from_root,
    key_broadcast_reason,
    key_host_path,
    reroot_from_root,
    shared_relationship_key_reroot,
    undetermined_witness,
    safe_reachable,
)
from slayer.core.query import (
    SlayerQuery,
)
from slayer.sql.naming import (
    ROW_IDENTITY_FIELD, canonical_aggregate_alias, flat_name, identified_datasets, pick_slot_alias, slot_result_key,
)
from slayer.sql.dialects.base import LIST_POSITION
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration
from slayer.core.scope import DatasetScope, PassThrough, StageField, StageSchema, host_dataset_name
from slayer.engine.elaborate_env import (
    check_reserved_regroup_prefix,
    ConsumerPosition,
    row_refs_off_grain,
    is_dimension_transform_root,
    position_classes,
    check_stage_flatten_collision,
    check_cross_dataset_inputs_safe,
    check_partition_keys_attributable,
    check_cross_dataset_source_resolves,
    check_input_dependencies_analyzable,
    check_local_producer_inputs_safe,
    check_metrics_at_query_grain,
    check_order_target_has_slot,
    check_parameter_determined,
    check_filter_dependencies_analyzable,
    check_raw_rows_no_aggregate_slots,
    check_reaggregation_dims_attributable,
    check_reaggregation_no_window,
    check_reaggregation_outer_keys_determined,
    check_windowed_time_axis_attributable,
    check_window_duration,
    check_windowed_time_dimension,
)
from slayer.engine.elaborate import elaborate_synthesized
from slayer.ir.bound import BoundExpr, BoundFilter, DeclaredMetric, OrderSpec, bound_filter_from_key, row_restriction_from_key
from slayer.engine.compile.discovery import RootDisposition, TransformRouting, discover_roots, row_attach_inputs
from slayer.ir.elaborated import ConjunctTyping, ElaboratedProducer, ElaboratedQuery, ElaboratedStage
from slayer.ir.terms import Aggregate, AggregateHomes
from slayer.engine.population.anchors import anchor_route, find_anchor
from slayer.engine.population.filter_reachability import (
    key_has_host_local_ref,
)
from slayer.ir.planned import (
    AssociationProducerKernel,
    EmptyBaseGrainPlan,
    EntityDedup,
    FilterReachability,
    MaskEntry,
    MaskTyping,
    OrderEntry,
    PickedParam,
    PlannedDomain,
    PlannedQuery,
    RankedProducerKernel,
    RegroupAttachPlan,
    RegroupSubstitution,
    regroup_producer_identity,
    SemiJoinFilter,
    SemiJoinHop,
    SlotId,
    SrcFilterRewrite,
    TrailingWindowProducerKernel,
    TransformLayer,
    ValueSlot,
)
from slayer.engine.ranked_planner import (
    explicit_ranking_key,
    resolve_ranking_time_key,
)
from slayer.engine.compile.projection import (
    ProjectionPlanner,
    _canonical_name,
    _iter_slot_deps,
)
from slayer.engine.compile.spine import (
    check_axis_rebucket, check_off_spine_order_keys, host_mask, is_spine_bound, plan_spine, shifted_spine_bounds,
    spine_field,
)
from slayer.engine.compile.shift import carried_placeholders
from slayer.engine.cardinality.unrelated import crosses_product_hop
from slayer.engine.compile.staging import stage_slots
from slayer.engine.stage_edges import inherited_edges, pass_through
from slayer.engine.key_metadata import (
    SlotTyping,
    minted_placeholders,
    dimension_key_format_description,
    key_element_type,
    metric_key_format_description,
    metric_key_preserves_native_type,
)
from slayer.ir.prebound import (
    ModePolicy,
    PreboundQuery,
    StrictQueryCarrier,
    partition_declared_metrics,
    walk_key_path,
)
from slayer.engine.compile.regroup import (
    RegroupPlaceholderRegistry,
    reserved_prefix_fields,
    substitute_in_bound_filter,
)
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
)


__all__ = [
    "compile_stage",
    "compile_synthesized",
]


def _order_wrap_source(key: ValueKey, *, dim_keys: frozenset) -> Optional[Tuple[AggregateSource, tuple]]:
    """(source, relationship path) of the MIN/MAX wrap a grouped ORDER BY row value needs, else ``None``."""
    if isinstance(key, TimeTruncKey):
        # Not a legal aggregate source; wrap its untruncated source (DATE_TRUNC is monotonic).
        return _order_wrap_source(key.source, dim_keys=dim_keys)
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return key, key.path
    if not isinstance(key, EXPRESSION_SOURCE_KINDS) or any(
        isinstance(k, (AggregateKey, TransformKey))
        or (isinstance(k, FieldKey) and k.leaf.startswith(REGROUP_LEAF_PREFIX))
        for k in walk_value_keys(key)
    ):
        return None
    off_grain = row_refs_off_grain(key, dim_keys)
    if not off_grain:
        return None
    return key, next((p for p in map(source_anchor_path, off_grain) if p), ())


# Duration-windowed metrics (``window='90d'``).


def _windowed_agg_keys(vk: ValueKey) -> list:
    return [k for k in walk_value_keys(vk) if window_kwarg_of(k) is not None]


def _guard_windowed_metrics(
    *,
    metric_vks: list,
    filter_vks: list,
    order_vks: list,
    active_td_key,
) -> dict:
    """Validate windowed-metric shapes; return cleanly-SELECTED windowed AggregateKeys in declaration order (value = slot ``hidden`` flag)."""
    all_vks = [*metric_vks, *filter_vks, *order_vks]
    if not any(_windowed_agg_keys(vk) for vk in all_vks):
        return {}

    for vk in all_vks:
        for key in _windowed_agg_keys(vk):
            check_window_duration(window_val=window_kwarg_of(key))

    selected_windowed: dict = {}
    for vk in metric_vks:
        if window_kwarg_of(vk) is not None:
            selected_windowed.setdefault(vk, False)
    # Order-only windowed target: HIDDEN, after the metric loop so an also-declared key keeps hidden=False.
    for vk in order_vks:
        for key in _windowed_agg_keys(vk):
            selected_windowed.setdefault(key, True)

    check_windowed_time_dimension(resolved=active_td_key is not None, remedy=first_bucket_remedy(metric_vks))
    return selected_windowed


def _windowed_slot_id_set(
    *,
    selected_windowed: dict,
    registry,
    active_td_slot_id,
) -> set:
    windowed_slot_ids: set = set()
    if not selected_windowed:
        return windowed_slot_ids

    # Post-projection: the window TD must be a SELECTED query TD (interned as a row slot).
    check_windowed_time_dimension(resolved=active_td_slot_id is not None)

    for key in selected_windowed:
        sid = registry.find_by_key(key)
        # A missing slot is planner/projection drift; fail rather than degrade to a plain aggregate.
        assert sid is not None, (
            f"Windowed metric {key!r} was selected but has no projection "
            f"slot; planner/projection drift."
        )
        windowed_slot_ids.add(sid)
    return windowed_slot_ids


# Regroup desugar: synthesize a producer stage per partition set.


def _stable_text(obj: Any) -> str:
    """A process-independent spelling of a key (sets sorted, never hash order)."""
    if isinstance(obj, BaseModel):
        fields = ",".join(
            _stable_text(getattr(obj, f)) for f in type(obj).model_fields
            if f not in NON_IDENTITY_KEY_FIELDS and not (f in OPTIONAL_KEY_FIELDS and getattr(obj, f) is None)
        )
        return f"{legacy_class_name(type(obj))}({fields})"
    if isinstance(obj, (set, frozenset)):
        return "{" + ",".join(sorted(_stable_text(x) for x in obj)) + "}"
    if isinstance(obj, (list, tuple)):
        return "(" + ",".join(_stable_text(x) for x in obj) + ")"
    return repr(obj)


def _regroup_grain_name(pk: ValueKey) -> str:
    """A field's path/leaf spelling; any other key a key-derived identifier."""
    if isinstance(pk, TimeTruncKey):
        return f"{_regroup_grain_name(pk.source)}_{pk.granularity}"
    if isinstance(pk, (FieldKey, FieldSqlKey)):
        return "__".join([*pk.path, field_leaf(pk)])
    return f"grain_{hashlib.sha1(_stable_text(pk).encode()).hexdigest()[:8]}"


def _regroup_partition_order(pks: Grain) -> List[ValueKey]:
    return sorted(
        pks, key=lambda k: (isinstance(k, TimeTruncKey), _regroup_grain_name(k), repr(k)),
    )


def _regroup_producer_prebound(  # NOSONAR(S3776) — one producer-prebound assembly; the grain / aggregate / inherited-filter / order arms share the prebound under construction.
    *,
    pks: Grain,
    aggs: List[ValueKey],
    dataset: Optional[SlayerDataset],
    bundle: ResolvedSourceBundle,
    inherited: List[BoundFilter],
    n_date_range: int,
    partition_order: Callable[
        [Grain], List[ValueKey],
    ] = _regroup_partition_order,
    public_alias_by_agg: Optional[Mapping[ValueKey, str]] = None,
    explicit_types: Optional[Mapping[ValueKey, DataType]] = None,
    grain_name_by_key: Optional[Mapping[ValueKey, str]] = None,
    window_td_key: Optional[TimeTruncKey] = None,
    mode_policy: ModePolicy,
    ranking_candidates: Sequence[ValueKey],
) -> Tuple[PreboundQuery, List[ValueKey]]:
    """The producer's bind product: grain from partition keys, one metric per consumed aggregate, inherited base-row filters; returns prebound + ordered grain keys."""
    public_alias_by_agg = public_alias_by_agg or {}
    explicit_types = explicit_types or {}
    grain_name_by_key = grain_name_by_key or {}
    ordered = partition_order(pks)
    dims = [pk for pk in ordered if not isinstance(pk, TimeTruncKey)]
    tds = [pk for pk in ordered if isinstance(pk, TimeTruncKey)]
    if window_td_key is not None and window_td_key not in pks:
        tds = [*tds, window_td_key]
    grain_keys = [*dims, *tds]
    # Declared names are reserved first; a colliding synthesized name gets a suffix.
    used_names: set[str] = {n for pk in grain_keys if (n := grain_name_by_key.get(pk))}

    def _unique(name: str) -> str:
        candidate = name
        i = 2
        while candidate in used_names:
            candidate = f"{name}_{i}"
            i += 1
        used_names.add(candidate)
        return candidate

    typing = SlotTyping(scope=DatasetScope(population=dataset), bundle=bundle) if dataset is not None else None
    grain_dms: List[DeclaredMetric] = []
    for pk in grain_keys:
        d_type = typing.value_type(pk) if typing is not None else None
        if dataset is not None:
            d_fmt, d_desc = dimension_key_format_description(dataset=dataset, key=pk)
        else:
            d_fmt, d_desc = None, None
        # A combined attach names its grain by the consumer's dimension name.
        name = grain_name_by_key.get(pk) or _unique(_regroup_grain_name(pk))
        grain_dms.append(DeclaredMetric(
            bound=BoundExpr(value_key=pk),
            declared_name=name, public_name=name,
            datatype=d_type, format=d_fmt, description=d_desc,
            element_type=None if dataset is None else key_element_type(pk, scope=DatasetScope(population=dataset), bundle=bundle),
            # A grain key is a dimension the producer GROUPS BY; marking a computed one makes its inner aggregate a ROW attach.
            is_dimension=True,
        ))
    grain_names = [dm.public_name for dm in grain_dms]
    assert len(set(grain_names)) == len(grain_names), grain_names
    # Constituents fall back to a bare op/name (``sum(cumsum(a) - cumsum(b))``);
    # disambiguated like grain names — wiring is by key, labels need only be unique.
    agg_dms: List[DeclaredMetric] = []
    for agg in aggs:
        canonical = _unique(
            public_alias_by_agg.get(agg)
            or (canonical_aggregate_alias(agg, profile="stage_expression")
                if isinstance(agg, AggregateKey) else None)
            or getattr(agg, "agg", None)
            or getattr(agg, "op", None)
            or "regroup"
        )
        # A transform root has no dataset-metric metadata; a consumer's explicit type wins over the derived one.
        a_type = typing.value_type(agg) if typing is not None else None
        if dataset is not None and isinstance(agg, AggregateKey):
            a_fmt, a_desc = metric_key_format_description(dataset=dataset, key=agg)
        else:
            a_fmt, a_desc = None, None
        agg_dms.append(DeclaredMetric(
            bound=BoundExpr(value_key=agg),
            declared_name=canonical, public_name=canonical,
            datatype=explicit_types.get(agg, a_type), format=a_fmt, description=a_desc,
            element_type=None if dataset is None else key_element_type(agg, scope=DatasetScope(population=dataset), bundle=bundle),
            type_is_explicit=agg in explicit_types,
            preserve_native_type=(
                dataset is not None
                and agg not in explicit_types
                and metric_key_preserves_native_type(dataset=dataset, key=agg)
            ),
        ))
    prebound = PreboundQuery(
        declared_metrics=[*grain_dms, *agg_dms],
        bound_filters=list(inherited),
        bound_filter_texts=[None] * len(inherited),
        n_date_range=n_date_range,
        order_specs=[],
        main_time_key=window_td_key,
        n_dims=len(dims),
        n_time_dimensions=len(tds),
        distinct_dimension_values=True,
        mode_policy=mode_policy,
        ranking_candidates=tuple(ranking_candidates),
    )
    return prebound, [*dims, *tds]


def _population_filters(
    prebound: PreboundQuery, filter_typings: Sequence[ConjunctTyping],
) -> Iterator[Tuple[int, BoundFilter, ConjunctTyping]]:
    """``(index, filter, typing)`` per population filter; a producer's FILTER row restriction is not one."""
    for idx, (bf, ct) in enumerate(zip(prebound.bound_filters, filter_typings)):
        if not bf.row_restriction:
            yield idx, bf, ct


def _regroup_inherited_filters(
    *, prebound: PreboundQuery, filter_typings: Sequence[ConjunctTyping],
) -> Tuple[List[BoundFilter], int]:
    """Stratum-0 field masks define every producer's population; nothing else inherits."""
    date_bounds: List[BoundFilter] = []
    others: List[BoundFilter] = []
    for idx, bf, ct in _population_filters(prebound, filter_typings):
        if ct.typing != MaskTyping.FIELD or ct.stratum != 0:
            continue
        if idx < prebound.n_date_range:
            date_bounds.append(bf)
        else:
            others.append(bf)
    return [*date_bounds, *others], len(date_bounds)


def _regroup_answer_slot_id(
    *, value_slots: List[ValueSlot], key: ValueKey, fallback: Optional[SlotId],
) -> SlotId:
    found = next(
        (slot.id for slot in value_slots if slot.key == key), fallback,
    )
    assert found is not None, (
        f"Regroup producer plan is missing the answer slot for "
        f"{type(key).__name__}; synthesis and planning disagree on its grain."
    )
    return found


def _producer_grain_slot_ids(producer_plan) -> set:
    return set(producer_plan.grain or ())


def _assert_attach_covers_producer_grain(
    *, joined_slot_ids: set, producer_grain_slot_ids: set,
) -> None:
    """The attach MUST join on the producer's COMPLETE grouping grain (from the planned producer); a coarser join multiplies rows."""
    if joined_slot_ids != producer_grain_slot_ids:
        raise ValueError(
            "Regroup attach join keys do not match the producer's grouping grain; "
            "the join must cover the complete grain or it changes cardinality."
        )


def _scalar_free_fields(node: ValueKey, out: set) -> None:
    # Asymmetric on purpose: aggregate subtrees are bound, not free.
    if isinstance(node, FieldKey):
        out.add(node)
    elif isinstance(node, SLOT_COMPOSITE_KINDS):
        for child in node.children():
            _scalar_free_fields(node=child, out=out)
    elif isinstance(node, TransformKey):
        _scalar_free_fields(node=node.input, out=out)


def _prune_functionally_determined_grain(pks: Grain) -> Grain:
    """Drop computed-dimension grain keys functionally determined by the raw dimensions already in the grain (redundant to group by)."""
    raw = frozenset(k for k in pks if isinstance(k, FieldKey))
    kept = set(pks)
    for k in pks:
        if isinstance(k, FieldKey):
            continue
        aggs = [a for a in walk_value_keys(k) if isinstance(a, AggregateKey)]
        if not aggs or any(
            a.partition_keys is None or not (frozenset(a.partition_keys) <= raw)
            for a in aggs
        ):
            continue
        free: set = set()
        _scalar_free_fields(k, free)
        if free <= raw:
            kept.discard(k)
    return Grain.of(kept)


def _windowed_or_ranked_identity(agg: ValueKey):
    """A hashable, partition-free identity for a windowed / ranked aggregate (own producer each); ``None`` for a plain aggregate."""
    if not is_kernel_requiring(agg):
        return None
    return ("windowed" if window_kwarg_of(agg) is not None else "ranked", _partition_free_identity(agg))


def _partition_free_identity(agg: ValueKey) -> ValueKey:
    """``agg`` without its ``partition_by=`` grain."""
    if not isinstance(agg, AggregateKey):
        return agg
    return agg.model_copy(update={"partition_keys": None})


def _cross_dataset_input_paths(
    *, agg_rooted: AggregateKey, root_dataset: SlayerDataset, root_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[List[Tuple[str, ...]]]:
    """The dependency closure of the aggregate's inputs in the root's coordinates
    (source / args / kwargs / field filter / definition defaults, derived
    definitions expanded); ``None`` when a dependency cannot be analysed."""
    closure = aggregate_input_closure(
        key=agg_rooted, anchor_dataset=root_dataset, anchor_relation=root_name,
        bundle=bundle, include_source=True,
    )
    return None if closure is None else list(closure)


def _first_unsafe_input_hop(
    *, paths: List[Tuple[str, ...]], root_dataset: SlayerDataset, root_name: str,
    datasets_by_name: Dict[str, SlayerDataset],
) -> List[str]:
    for path in paths:
        if not safe_reachable(
            root=root_dataset, path=path, datasets_by_name=datasets_by_name,
        ):
            # First violation wins; the checker raises it.
            return [path[-1] if path else root_name]
    return []


def _first_unattributable_arg_leaf(
    *, agg: AggregateKey, target_path: Tuple[str, ...],
    root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    bundle: ResolvedSourceBundle, host_dataset: Optional[SlayerDataset] = None,
    host_name: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """``[(leaf, reason)]`` for the first argument field (HOST coordinates) not
    attributable from the root, judged on its dependency closure; an expression
    parameter is named by its parameter name. host_name lets an off-home input
    traverse a proven reverse hop."""
    for name, value, arg in _param_row_fields(agg):
        reason = _unattributable_reason(
            arg=arg, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
            host_dataset=host_dataset, host_name=host_name,
        )
        if reason is not None:
            leaf = field_leaf(arg)
            return [(leaf if arg is value or name is None else name, reason)]
    return []


def _param_row_fields(
    agg: AggregateKey,
) -> Iterator[Tuple[Optional[str], object, FieldKey | FieldSqlKey]]:
    """``(parameter name or None, parameter value, field row leaf)`` per argument."""
    for name, value in [*((None, a) for a in agg.args), *agg.kwargs]:
        for arg in parameter_row_leaves(value):
            if isinstance(arg, (FieldKey, FieldSqlKey)):
                yield name, value, arg


def _unattributable_reason(
    *, arg: ValueKey, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    host_dataset: Optional[SlayerDataset], host_name: Optional[str],
) -> Optional[str]:
    """Why ``arg`` is not attributable from the root, else ``None``."""
    if host_dataset is not None:
        if key_attributable_from_root(
            key=arg, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
            host_dataset=host_dataset, host_name=host_name,
        ):
            return None
        return key_broadcast_reason(
            key=arg, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
            host_dataset=host_dataset, host_name=host_name,
        )
    if attributable_from_root(
        host_path=key_host_path(arg), target_path=target_path,
        root_dataset=root_dataset, datasets_by_name=datasets_by_name, host_name=host_name,
    ):
        return None
    return broadcast_reason(
        host_path=key_host_path(arg), target_path=target_path,
        root_dataset=root_dataset, datasets_by_name=datasets_by_name, host_name=host_name,
    )


def _assert_cross_dataset_inputs_safe(
    *, agg: AggregateKey, agg_rooted: AggregateKey, root_dataset: SlayerDataset,
    root_name: str, target_path: Tuple[str, ...], bundle: ResolvedSourceBundle,
    datasets_by_name: Dict[str, SlayerDataset], host_name: Optional[str] = None,
    host_dataset: Optional[SlayerDataset] = None,
) -> None:
    """Resolve every cross-dataset input's attributability from its root; the checker
    raises on a fanning/unproven relationship or an unanalysable derived dependency. Attached
    inputs are opaque (Axiom 2.3): masked by a literal, judged by their own producer."""
    alias = canonical_aggregate_alias(agg, profile="stage_expression")
    attached = attached_inputs(agg_rooted)
    if attached:
        agg_rooted = substitute_value_keys(
            key=agg_rooted,
            mapping={a: LiteralKey(value=Decimal(1)) for a in attached},
        )
    paths = _cross_dataset_input_paths(
        agg_rooted=agg_rooted, root_dataset=root_dataset, root_name=root_name,
        bundle=bundle,
    )
    if paths is None:
        check_input_dependencies_analyzable(
            alias=alias,
            field=first_unanalyzable_input_field(
                key=agg_rooted, anchor_dataset=root_dataset, anchor_relation=root_name,
                bundle=bundle,
            ),
        )
        paths = []
    # An explicit argument's violation names the field and its hop, ahead of the
    # closure's hop-only message.
    unattributable_arg_leaves = _first_unattributable_arg_leaf(
        agg=agg, target_path=target_path, root_dataset=root_dataset,
        datasets_by_name=datasets_by_name, host_name=host_name,
        bundle=bundle, host_dataset=host_dataset,
    )
    unsafe_input_hops = [] if unattributable_arg_leaves else _first_unsafe_input_hop(
        paths=paths, root_dataset=root_dataset, root_name=root_name,
        datasets_by_name=datasets_by_name,
    )
    check_cross_dataset_inputs_safe(
        alias=alias,
        root_name=root_name,
        unsafe_input_hops=unsafe_input_hops,
        unattributable_arg_leaves=unattributable_arg_leaves,
    )


#: One pushed conjunct: (rewritten key, filter text, hop registry, per-hop
#: null-rejection). Grouping merges pushes into correlated-EXISTS semi-joins.
_Push = Tuple[
    ValueKey, Optional[str], Dict[Tuple[str, ...], SemiJoinHop],
    Dict[Tuple[str, ...], bool],
]


def _producer_row_conjuncts(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], host_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
) -> List[Tuple[ValueKey, Optional[str]]]:
    """The base ROW conjuncts a home-rooted producer disposes; spine bounds reach a home only through its axis."""
    spine = spine_field(host_dataset)
    reaches_spine = spine is not None and key_attributable_from_root(
        key=spine, target_path=target_path, root_dataset=root_dataset, datasets_by_name=datasets_by_name,
        bundle=bundle, host_dataset=host_dataset, host_name=host_dataset.name,
    )
    return [
        (cj, text)
        for bf, text in base_filters if bf.phase == Phase.ROW
        for cj in split_top_level_and(bf.value_key)
        if reaches_spine or not is_spine_bound(cj, spine=spine)
    ]


def _cross_dataset_inherited_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], host_name: str,
    host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Tuple[List[BoundFilter], List[SemiJoinFilter]]:
    """Split base ROW filters into conjuncts and dispose each two ways: fully
    attributable → inherits inline (re-rooted); else pushed as a correlated
    EXISTS semi-join (grouped by shared branch, D3/D4)."""
    inherited: List[BoundFilter] = []
    pushes: List[_Push] = []
    for cj, text in _producer_row_conjuncts(
        base_filters=base_filters, target_path=target_path, root_dataset=root_dataset,
        datasets_by_name=datasets_by_name, host_dataset=host_dataset, bundle=bundle,
    ):
        inherited_bf, pushed = _conjunct_disposition(
            cj, text=text, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_name,
            host_dataset=host_dataset, bundle=bundle,
        )
        if inherited_bf is not None:
            inherited.append(inherited_bf)
        else:
            assert pushed is not None
            pushes.append(pushed)
    return inherited, _semi_join_groups_from_pushes(pushes)


def _semi_join_groups_from_pushes(
    pushes: Sequence["_Push"],
) -> List[SemiJoinFilter]:
    """Group pushed conjuncts into correlated EXISTS semi-joins by union-find over
    their outer-attached hops (branch roots; a reduced push's parent may be a
    materialised outer alias, D6): a push spanning several branches merges
    them (a product), and two pushes sharing a branch land in one group. Group
    order and sibling-hop order are first-appearance (the union-find never
    reorders), so disjoint single-branch pushes stay separate EXISTS in today's
    order. Each hop's ``null_extended`` = declared LEFT ∧ the group's
    AND-ed predicate does not reject its null row (the OR of its conjuncts' per-hop
    rejections, D5)."""
    uf = _UnionFind()
    firsts_by_push = [_first_level_hops(nodes) for _k, _t, nodes, _r in pushes]
    for firsts in firsts_by_push:
        for f in firsts:
            uf.union(a=firsts[0], b=f)
    groups: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    for (key_rewritten, conj_text, nodes, rejects), firsts in zip(pushes, firsts_by_push):
        group = groups.setdefault(
            uf.find(firsts[0]),
            {"nodes": {}, "conjuncts": [], "texts": [], "rejects": {}})
        for path, hop in nodes.items():
            group["nodes"].setdefault(path, hop)
        for path, r in rejects.items():
            group["rejects"][path] = group["rejects"].get(path, False) or r
        group["conjuncts"].append(key_rewritten)
        group["texts"].append(conj_text)
    return [_semi_join_filter(g) for g in groups.values()]


class _UnionFind:
    """Path-compressing union-find over hop paths; a path registers on first sight."""

    def __init__(self) -> None:
        self._parent: Dict[Tuple[str, ...], Tuple[str, ...]] = {}

    def find(self, x: Tuple[str, ...]) -> Tuple[str, ...]:
        parent = self._parent
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(self, *, a: Tuple[str, ...], b: Tuple[str, ...]) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[ra] = rb


def _first_level_hops(nodes: Mapping[Tuple[str, ...], Any]) -> List[Tuple[str, ...]]:
    """The outer-attached hops of a push: those whose parent is not one of its nodes."""
    return [p for p in nodes if p[:-1] not in nodes]


def _semi_join_filter(g: Dict[str, Any]) -> SemiJoinFilter:
    return SemiJoinFilter(
        hops=[
            hop.model_copy(update={
                "null_extended": hop.declared_left
                and not g["rejects"].get(path, False),
            })
            for path, hop in sorted(g["nodes"].items(), key=lambda ph: len(ph[0]))
        ],
        conjuncts=g["conjuncts"],
        filter_texts=g["texts"],
    )


def _ranking_key_crossing(
    *, key: ValueKey, root_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
    alias: Optional[str],
) -> Optional[Tuple[str, str]]:
    """The ``(leaf, hop)`` a ranked aggregate's ordering key crosses unsafely from
    ``root_dataset``, judged by its dependency closure (engine P10); ``None`` when every
    path is provably to-one. An unanalysable definition fails closed through
    ``check_input_dependencies_analyzable`` naming the field (Axiom 2.8). An expression
    key is judged per leaf, naming the crossing one."""
    for leaf_key in source_row_leaves(key) or [key]:
        leaf = field_leaf(leaf_key) if isinstance(leaf_key, (FieldKey, FieldSqlKey)) else key_display(leaf_key)
        closure = key_closure(
            key=leaf_key, anchor_dataset=root_dataset, anchor_relation=root_dataset.name, bundle=bundle,
        )
        if closure is None:
            check_input_dependencies_analyzable(alias=alias, field=leaf)
            closure = ()
        for path in closure:
            if not safe_reachable(
                root=root_dataset, path=path, datasets_by_name=bundle.datasets_by_name,
            ):
                return leaf, path[-1]
    return None


def _assert_local_producer_inputs_safe(
    *,
    agg: AggregateKey,
    host_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
    datasets_by_name: Dict[str, SlayerDataset],
) -> None:
    """Resolve per-role crossing-input safety for a HOST-rooted producer answer; the checker raises on an unproven hop (host-grain wrap's SOURCE path exempt)."""
    def _safe(path: Tuple[str, ...]) -> bool:
        return safe_reachable(
            root=host_dataset, path=tuple(path), datasets_by_name=datasets_by_name,
        )

    alias = canonical_aggregate_alias(agg, profile="stage_expression")
    # Role: crossed ranking argument, explicitly named (a first/last ranking arg),
    # judged by its closure so a derived argument names itself (D1).
    ranked_crossings: List[Tuple[str, str]] = []
    ranking_arg = explicit_ranking_key(agg)
    if ranking_arg is not None:
        crossing = _ranking_key_crossing(
            key=ranking_arg, root_dataset=host_dataset, bundle=bundle, alias=alias,
        )
        if crossing is not None:
            ranked_crossings.append(crossing)

    # Crossed predicate + remaining crossed args; the SOURCE's own crossings are separate.
    gated_crossings: List[str] = []
    source_crossings: List[str] = []
    if not ranked_crossings:
        gated = aggregate_input_closure(
            key=agg, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
            bundle=bundle, include_source=False,
        )
        if gated is None:
            check_input_dependencies_analyzable(
                alias=alias,
                field=first_unanalyzable_input_field(
                    key=agg, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
                    bundle=bundle, include_source=False,
                ),
            )
            gated = []
        gated_crossings = [p[-1] for p in gated if p and not _safe(p)]
        source_crossings = _source_crossings(
            agg=agg, host_dataset=host_dataset, bundle=bundle, alias=alias, safe=_safe,
        )
    check_local_producer_inputs_safe(
        alias=alias,
        host=host_dataset.name,
        ranked_crossings=ranked_crossings,
        gated_crossings=gated_crossings,
        source_crossings=source_crossings,
    )


def _source_crossings(
    *,
    agg: AggregateKey,
    host_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    safe: Callable[[Tuple[str, ...]], bool],
) -> List[str]:
    """Unproven hops the expression source's own ROW leaves cross from the host (constituents opaque, Axiom 2.3; unanalysable fails closed, Axiom 2.8); a host-grain wrap is exempt."""
    if agg.locus == "host":
        return []
    src = source_row_leaf_closure(
        key=agg, anchor_dataset=host_dataset, anchor_relation=host_dataset.name, bundle=bundle,
    )
    if src is None:
        check_input_dependencies_analyzable(
            alias=alias,
            field=first_unanalyzable_source_row_leaf(
                key=agg, anchor_dataset=host_dataset, anchor_relation=host_dataset.name, bundle=bundle,
            ),
        )
        return []
    return [p[-1] for p in src if p and not safe(p)]


def _checked_ranking_time_key(
    *,
    row_keys: Sequence[ValueKey],
    agg_key: AggregateKey,
    root_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    target_rooted: bool,
) -> ValueKey:
    """Resolve a ranked producer's ordering key, then judge it by closure (D5): a
    crossing key raises through the same checker its explicit spelling uses — the
    cross-dataset attributability form when target-rooted, the local ranked form else."""
    resolved = resolve_ranking_time_key(
        key=agg_key, root_dataset=root_dataset, bundle=bundle, row_keys=row_keys,
    )
    crossing = _ranking_key_crossing(
        key=resolved, root_dataset=root_dataset, bundle=bundle, alias=alias,
    )
    if crossing is not None:
        leaf, hop = crossing
        if target_rooted:
            check_cross_dataset_inputs_safe(
                alias=alias, root_name=root_dataset.name, unsafe_input_hops=[],
                unattributable_arg_leaves=[(leaf, key_broadcast_reason(
                    key=resolved, target_path=(), root_dataset=root_dataset,
                    datasets_by_name=bundle.datasets_by_name, bundle=bundle,
                    host_dataset=root_dataset, host_name=root_dataset.name,
                ))],
            )
        else:
            check_local_producer_inputs_safe(
                alias=alias, host=root_dataset.name,
                ranked_crossings=[(leaf, hop)], gated_crossings=[],
            )
    return resolved


def _trailing_window_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    root_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    target_rooted: bool,
    ranking_candidates: Sequence[ValueKey],
) -> TrailingWindowProducerKernel:
    window_raw = window_kwarg_of(agg_key)
    bucket_sid = producer_plan.active_time_dimension_slot_id
    bucket_slot = next(
        (s for s in producer_plan.row_slots if s.id == bucket_sid), None,
    )
    assert (
        isinstance(window_raw, str) and bucket_sid is not None
        and bucket_slot is not None and isinstance(bucket_slot.key, TimeTruncKey)
    ), (
        "Windowed producer is missing its window duration or bucket slot; "
        "synthesis and planning disagree."
    )
    src_where_ids, src_rewrites = _plan_src_row_filters(
        producer_plan=producer_plan,
    )
    # first/last rank the interval rows by the same closure-judged key plain first/last would.
    ranking_time_key = (
        _checked_ranking_time_key(
            row_keys=ranking_candidates, agg_key=agg_key,
            root_dataset=root_dataset, bundle=bundle, alias=alias, target_rooted=target_rooted,
        )
        if is_ranked(agg_key) else None
    )
    # Reference-bearing parameters read per interval row, each key remapped through
    # the sub-plan's regroup substitutions so an attached-aggregate parameter reads
    # its row-attach field.
    picked_params = _picked_params(agg_key)
    param_subst = {
        sub.original_key: sub.placeholder
        for a in producer_plan.regroup_attach_plans
        for sub in a.substitutions
    }
    if param_subst:
        picked_params = [
            pp.model_copy(update={
                "key": substitute_value_keys(key=pp.key, mapping=param_subst),
            })
            for pp in picked_params
        ]
    return TrailingWindowProducerKernel(
        window_raw=window_raw,
        window_parts=parse_window_duration(window_raw),
        window_granularity=bucket_slot.key.granularity,
        bucket_slot_id=bucket_sid,
        src_where_filter_ids=src_where_ids,
        src_filter_rewrites=src_rewrites,
        ranking_time_key=ranking_time_key,
        picked_params=picked_params,
    )


def _ranked_kernel(
    *,
    agg_key: AggregateKey,
    root_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    target_rooted: bool,
    ranking_candidates: Sequence[ValueKey],
) -> RankedProducerKernel:
    agg = agg_key.agg
    assert agg in ("first", "last")
    return RankedProducerKernel(
        agg=agg,
        ranking_time_key=_checked_ranking_time_key(
            row_keys=ranking_candidates, agg_key=agg_key, root_dataset=root_dataset,
            bundle=bundle, alias=alias, target_rooted=target_rooted,
        ),
    )


class _KernelDecision(BaseModel):
    """The kernel a producer renders through: its reducer, whether it associates, and the answer it renders."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    reducer: Literal["plain", "trailing_window", "ranked"]
    associate: bool = False
    answer: AggregateKey


def _kernel_decision(
    *, answer: ValueKey, windowed: bool, associate: bool = False, eligible: bool = True,
) -> Optional[_KernelDecision]:
    """The one kernel choice of a producer answering ``answer``: reducer × associate; ``None`` for a plain grouping."""
    if not eligible or not isinstance(answer, AggregateKey):
        return None
    if windowed and window_kwarg_of(answer) is not None:
        reducer = "trailing_window"
    elif is_ranked(answer):
        reducer = "ranked"
    elif associate:
        reducer = "plain"
    else:
        return None
    return _KernelDecision(reducer=reducer, associate=associate, answer=answer)


def _kernel_answer(decision: Optional[_KernelDecision]) -> Optional[AggregateKey]:
    return None if decision is None else decision.answer


def _decided_kernel(
    decision: Optional[_KernelDecision], *, producer_plan: PlannedQuery,
    root_dataset: SlayerDataset, bundle: ResolvedSourceBundle, alias: Optional[str],
    target_rooted: bool, ranking_candidates: Sequence[ValueKey],
) -> Dict[str, Any]:
    """The ``kernel=`` attach kwarg of a trailing-window / ranked decision (its association dedup set by the caller)."""
    if decision is None:
        return {}
    assert decision.reducer != "plain", "an association kernel is built by its synthesiser"
    if decision.reducer == "trailing_window":
        return {"kernel": _trailing_window_kernel(
            producer_plan=producer_plan, agg_key=decision.answer, root_dataset=root_dataset, bundle=bundle,
            alias=alias, target_rooted=target_rooted, ranking_candidates=ranking_candidates,
        )}
    return {"kernel": _ranked_kernel(
        agg_key=decision.answer, root_dataset=root_dataset, bundle=bundle,
        alias=alias, target_rooted=target_rooted, ranking_candidates=ranking_candidates,
    )}


def _empty_value(key: ValueKey, *, bundle: ResolvedSourceBundle) -> EmptyValue:
    """An attached aggregate's empty value, its definition read on the owning dataset."""
    if not isinstance(key, AggregateKey):
        return None
    datasets = bundle.datasets_by_name
    owner = aggregation_owner(root=bundle.population, source=key.source, datasets_by_name=datasets)
    root = bundle.population
    element = None if root is None else value_type(
        key.source, field_type=dataset_field_type(dataset=root, datasets_by_name=datasets),
    )
    return empty_value(
        agg=key.agg, definition=aggregation_definition(owner=owner, agg=key.agg), element_type=element,
    )


def _synthesize_wrap_attach(
    *,
    wrap_key: AggregateKey,
    prebound: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    producer_source_dataset: Optional[str],
    row_attaches: Sequence[RegroupAttachPlan] = (),
    population: "Population",
) -> RegroupAttachPlan:
    """A host-grain ORDER-BY wrap as a HOST-rooted producer synthesized late: a combined attach at the full projected grain whose placeholder IS the wrap key. Inherits the population disposition (D1) like every other host-rooted producer."""
    producer_dataset = scope.population
    datasets_by_name = bundle.datasets_by_name
    if producer_dataset is not None:
        _assert_local_producer_inputs_safe(
            agg=wrap_key, host_dataset=producer_dataset, bundle=bundle,
            datasets_by_name=datasets_by_name,
        )
    dim_dms, td_dms, _ = partition_declared_metrics(
        declared_metrics=prebound.declared_metrics,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    undo_desugar = {
        sub.placeholder: sub.original_key
        for attach in row_attaches
        for sub in attach.substitutions
    }

    def _raw(key: ValueKey) -> ValueKey:
        return substitute_value_keys(key, undo_desugar) if undo_desugar else key

    projected = [_raw(dm.bound.value_key) for dm in (*dim_dms, *td_dms)]
    consumer_order = {k: i for i, k in enumerate(projected)}
    grain_name_by_key = {
        _raw(dm.bound.value_key): dm.declared_name
        for dm in (*dim_dms, *td_dms)
        if dm.declared_name is not None
    }
    inherited, n_inherited_date = _regroup_inherited_filters(
        prebound=prebound, filter_typings=filter_typings,
    )
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=Grain.of(projected), aggs=[wrap_key], dataset=producer_dataset,
        bundle=bundle, inherited=inherited, n_date_range=n_inherited_date,
        partition_order=lambda pks: sorted(
            pks, key=lambda k: consumer_order.get(k, len(consumer_order)),
        ),
        grain_name_by_key=grain_name_by_key,
        mode_policy=prebound.mode_policy,
        ranking_candidates=prebound.ranking_candidates,
    )
    producer_plan = compile_synthesized(
        prebound=producer_prebound,
        source_dataset=producer_source_dataset,
        bundle=bundle,
        scope=scope,
        stage_schemas=stage_schemas,
        producer_registry=producer_registry,
        population=population,
    )
    producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
    answer_slot = _regroup_answer_slot_id(
        value_slots=[
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        ],
        key=wrap_key,
        fallback=producer_answer_ids[0] if producer_answer_ids else None,
    )
    producer_grain_ids = list(producer_plan.projection)[: len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, pk in enumerate(ordered_pks):
        slot_id = next(
            (s.id for s in producer_plan.row_slots if s.key == pk), None,
        )
        if slot_id is None:
            slot_id = producer_grain_ids[i]
        join_pairs.append((pk, slot_id))
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    # A wrap's source is a plain field, so the alias always resolves.
    alias_hint = canonical_aggregate_alias(wrap_key, profile="stage_expression")
    assert alias_hint is not None
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=alias_hint,
        attach_phase="combined",
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=wrap_key, producer_slot_id=answer_slot,
            original_key=wrap_key, empty_value=_empty_value(wrap_key, bundle=bundle),
        )],
        partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
    )


#: Answer name of a composite shifted producer — offset-free, so offsets intern.
_SHIFTED_ANSWER = "shifted"


def _frame_free_filters(
    *, prebound: PreboundQuery, filter_typings: Sequence[ConjunctTyping],
    time_fields: AbstractSet[ValueKey],
) -> List[BoundFilter]:
    """Every field mask of the population (any stratum) minus every frame bound."""
    out: List[BoundFilter] = []
    for idx, bf, ct in _population_filters(prebound, filter_typings):
        if ct.typing != MaskTyping.FIELD or idx < prebound.n_date_range:
            continue
        residual = strip_frame_bounds(key=bf.value_key, time_fields=time_fields)
        if residual is None:
            continue
        out.append(bf if residual is bf.value_key else substitute_in_bound_filter(
            bf, {bf.value_key: residual},
        ))
    return out


def _plan_shifted_attaches(
    *,
    slots: Sequence[ValueSlot],
    attaches: Sequence[RegroupAttachPlan],
    prebound: PreboundQuery,
    rewritten: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_dataset: Optional[str],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    population: "Population",
    homes: AggregateHomes,
    candidates: AbstractSet[ValueKey],
) -> List[RegroupAttachPlan]:
    """One frame-free producer per non-series ``time_shift`` slot, each leaf at its own
    grain (carried or re-evaluated), looked up by the slot on the shifted bucket."""
    to_original = {
        sub.placeholder: sub.original_key for a in attaches for sub in a.substitutions
    }
    shift_slots = [
        s for s in slots
        if isinstance(s.key, TransformKey) and s.key.op == "time_shift"
        and _original_key(s.key, to_original=to_original) in candidates
    ]
    if not shift_slots:
        return []
    n_grain = prebound.n_dims + prebound.n_time_dimensions
    grain = [dm.bound.value_key for dm in prebound.declared_metrics[:n_grain]]
    host_grain = [dm.bound.value_key for dm in rewritten.declared_metrics[:n_grain]]
    dim_keys = grain[:prebound.n_dims]
    td_keys = grain[prebound.n_dims:]
    consumer_order = {k: i for i, k in enumerate(grain)}
    grain_name_by_key = {
        dm.bound.value_key: dm.declared_name
        for dm in prebound.declared_metrics[:n_grain] if dm.declared_name is not None
    }
    inherited = _frame_free_filters(
        prebound=prebound, filter_typings=filter_typings,
        time_fields=frozenset(k.source for k in td_keys if isinstance(k, TimeTruncKey)),
    )
    producer_dataset = scope.population
    kernel_root = producer_dataset or bundle.population
    # A bare answer is named like its combined producer: the metric's public name.
    public_alias = {
        dm.bound.value_key: dm.public_name
        for dm in reversed(prebound.declared_metrics[n_grain:]) if dm.public_name is not None
    }
    out: List[RegroupAttachPlan] = []
    for slot in shift_slots:
        key = slot.key
        assert isinstance(key, TransformKey) and isinstance(key.time_key, TimeTruncKey)
        carried = carried_placeholders(
            key.input, axis=key.time_key, to_original=to_original,
            dim_keys=dim_keys, td_keys=td_keys, active_bucket=prebound.main_time_key,
        )
        answer = substitute_value_keys(key.input, {
            ph: orig for ph, orig in to_original.items() if ph not in carried
        })
        carried_attaches = _attaches_carrying(attaches=attaches, carried=carried)
        pks, window_td, own_grain = _shifted_producer_grain(
            answer=answer, resolved=substitute_value_keys(key.input, to_original),
            axis=key.time_key, grain=grain, dim_keys=dim_keys,
            td_keys=td_keys, active_bucket=prebound.main_time_key,
        )
        aliases, alias_hint = _shifted_answer_aliases(answer=answer, public_alias=public_alias)
        periods, shift_unit = shift_offset_of(key)
        producer_prebound, ordered_pks = _regroup_producer_prebound(
            pks=pks, aggs=[answer], dataset=producer_dataset, bundle=bundle,
            inherited=[*inherited, *shifted_spine_bounds(
                prebound=prebound, spine=spine_field(producer_dataset), periods=periods,
                unit=shift_unit or key.time_key.granularity,
            )], n_date_range=0,
            partition_order=lambda pks: sorted(
                pks, key=lambda k: consumer_order.get(k, len(consumer_order)),
            ),
            public_alias_by_agg=aliases,
            grain_name_by_key=grain_name_by_key,
            window_td_key=window_td,
            mode_policy=prebound.mode_policy,
            ranking_candidates=prebound.ranking_candidates,
        )
        decision = _kernel_decision(
            answer=answer, windowed=window_td is not None,
            eligible=own_grain and kernel_root is not None and not (
                isinstance(answer, AggregateKey) and homes.target_rooted(answer)
            ),
        )
        producer_plan = compile_synthesized(
            prebound=producer_prebound,
            source_dataset=producer_source_dataset,
            bundle=bundle, scope=scope, stage_schemas=stage_schemas,
            producer_registry=producer_registry,
            population=population,
            carried_attaches=carried_attaches,
            reserved_placeholders=frozenset(to_original),
            kernel_answer=_kernel_answer(decision),
            domain_of=key,
        )
        answer_ids = list(producer_plan.projection)[len(ordered_pks):]
        answer_slot = _regroup_answer_slot_id(
            value_slots=[*producer_plan.aggregate_slots,
                         *producer_plan.combined_expression_slots],
            key=answer, fallback=answer_ids[0] if answer_ids else None,
        )
        join_pairs = _shifted_join_pairs(
            producer_plan=producer_plan, ordered_pks=ordered_pks,
            host_of=dict(zip(grain, host_grain)), answer_slot=answer_slot,
        )
        kernel_kwargs = {} if decision is None or kernel_root is None else _decided_kernel(
            decision, producer_plan=producer_plan, root_dataset=kernel_root, bundle=bundle,
            alias=canonical_aggregate_alias(decision.answer, profile="stage_expression"),
            target_rooted=False, ranking_candidates=producer_prebound.ranking_candidates,
        )
        out.append(_intern_producer(RegroupAttachPlan(
            producer_plan=producer_plan,
            alias_hint=alias_hint,
            attach_phase="shifted",
            shift_of=slot.id,
            answer_slot_id=answer_slot,
            join_pairs=join_pairs,
            partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
            **kernel_kwargs,
        ), producer_registry))
    return out


def _original_key(key: ValueKey, *, to_original: Mapping[ValueKey, ValueKey]) -> ValueKey:
    """``key`` with every attach placeholder undone, nested ones included."""
    while True:
        undone = substitute_value_keys(key, to_original)
        if undone == key:
            return key
        key = undone


def _shifted_answer_aliases(
    *, answer: ValueKey, public_alias: Dict[ValueKey, str],
) -> Tuple[Dict[ValueKey, str], str]:
    """(producer public aliases, attach alias hint) of a shifted answer."""
    if isinstance(answer, AggregateKey):
        return public_alias, canonical_aggregate_alias(answer, profile="stage_expression") \
            or _SHIFTED_ANSWER
    return {answer: _SHIFTED_ANSWER}, _SHIFTED_ANSWER


def _attaches_carrying(
    *, attaches: Sequence[RegroupAttachPlan], carried: Sequence[ValueKey],
) -> List[RegroupAttachPlan]:
    """The attaches (identity-deduplicated) producing a carried placeholder."""
    out: List[RegroupAttachPlan] = []
    for a in attaches:
        if any(sub.placeholder in carried for sub in a.substitutions) \
                and not any(a is c for c in out):
            out.append(a)
    return out


def _shifted_producer_grain(
    *, answer: ValueKey, resolved: ValueKey, axis: TimeTruncKey, grain: List[ValueKey],
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[TimeTruncKey],
) -> Tuple[Grain, Optional[TimeTruncKey], bool]:
    """(grain, window time key, own-grain?) of a shifted producer: the operand grain
    (Axiom 11.1) when it holds the axis, else the query grain."""
    # A bare aggregate keyed by the axis is built exactly like its combined
    # producer, so a frame-free one interns with the base's.
    if isinstance(answer, AggregateKey):
        leaf_grain, windowed = effective_root_grain(
            agg=answer, projected_dim_keys=dim_keys, projected_td_keys=td_keys,
            active_bucket=active_bucket,
        )
        if windowed or axis in leaf_grain:
            return (_prune_functionally_determined_grain(leaf_grain),
                    active_bucket if windowed else None, True)
    elif not source_row_leaves(resolved):
        union: Grain = Grain.of(())
        for agg in operand_aggregates(resolved):
            union = union | constituent_grain(
                c=agg, projected_dim_keys=dim_keys, projected_td_keys=td_keys,
                active_bucket=active_bucket,
            )
        if axis in union:
            return _prune_functionally_determined_grain(union), axis, False
    return Grain.of(grain), axis, False


def _shifted_join_pairs(
    *, producer_plan: PlannedQuery, ordered_pks: Sequence[ValueKey],
    host_of: Dict[ValueKey, ValueKey], answer_slot: SlotId,
) -> List[Tuple[ValueKey, SlotId]]:
    """Consumer-key → producer-slot pairs covering the shifted producer's grain."""
    grain_ids = list(producer_plan.projection)[:len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = [
        (host_of.get(pk, pk), next(
            (s.id for s in producer_plan.row_slots if s.key == pk), grain_ids[i],
        ))
        for i, pk in enumerate(ordered_pks)
    ]
    joined = {sid for _, sid in join_pairs}
    # A bare row-valued answer (a carried placeholder) is a row slot, not grain.
    _assert_attach_covers_producer_grain(
        joined_slot_ids=joined,
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan)
        - ({answer_slot} - joined),
    )
    return join_pairs


class _PushBlocked(Exception):
    """A conjunct outside semi-join pushdown scope; the message is the warning reason."""


def _owning_dataset(
    name: str, *, host_dataset: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[SlayerDataset]:
    if host_dataset is not None and name == host_dataset.name:
        return host_dataset
    return datasets_by_name.get(name)


def _derived_field_owner(
    col: FieldSqlKey, *, host_dataset: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Tuple[SlayerDataset, DatasetField]]:
    """The derived field a ``FieldSqlKey`` names, with its owning dataset."""
    owner = _owning_dataset(
        col.dataset, host_dataset=host_dataset, datasets_by_name=datasets_by_name,
    )
    field = None if owner is None else next(
        (c for c in owner.fields if c.name == col.field_name), None)
    return None if owner is None or field is None else (owner, field)


def _ref_sql_dependency_paths(
    col: ValueKey, *, host_dataset: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset], bundle: Optional[ResolvedSourceBundle],
    include_filter: bool = True,
) -> Tuple[Tuple[str, ...], ...]:
    """Owner-relative relationship paths a derived field's ``DatasetField.sql`` AND its
    ``DatasetField.filter`` cross (the dependency closure at the owner) — the
    semi-join push tree registers a hop for each; ``include_filter=False`` keeps
    the value definition's paths only. ``bundle.datasets_by_name`` is host-inclusive,
    so a dep pointing back at the host resolves without a hand-patched
    bundle."""
    if not isinstance(col, FieldSqlKey) or bundle is None:
        return ()
    found = _derived_field_owner(
        col, host_dataset=host_dataset, datasets_by_name=datasets_by_name,
    )
    if found is None:
        return ()
    owner, field = found
    paths: List[Tuple[str, ...]] = []
    for sql in (field.definition_sql, field.filter if include_filter else None):
        if not sql:
            continue
        frag = fragment_closure(
            sql=sql, dataset=owner, owner_path=(),
            anchor_relation=owner.name, bundle=bundle,
        )
        if frag is None:
            # None = no dialect could analyse the fragment (≠ () = analysed, local).
            # Fail closed: we can't determine the hops it crosses, so block the push.
            raise _PushBlocked(
                f"unanalyzable definition fragment on {owner.name}.{field.name} "
                f"— cannot determine the semi-join hops it crosses"
            )
        paths.extend(frag)
    return tuple(dict.fromkeys(paths))


def _path_edges_exist(
    dataset: SlayerDataset, path: Tuple[str, ...],
    datasets_by_name: Dict[str, SlayerDataset],
) -> bool:
    try:
        return follow(root=dataset, path=path, datasets_by_name=datasets_by_name) is not None
    except AmbiguousRelationshipPathError:
        return False


def _register_hop(
    nodes: Dict[Tuple[str, ...], SemiJoinHop], *, node_path: Tuple[str, ...],
    target_dataset: str, keys: List[Tuple[DatasetField, DatasetField]], declared_left: bool, lateral: Optional[str] = None,
) -> None:
    nodes.setdefault(node_path, SemiJoinHop(
        to=target_dataset,
        **SemiJoinHop.key_args(keys),
        node_path=node_path,
        declared_left=declared_left,
        lateral=lateral,
    ))


def _forward_hops(
    *, start_dataset: SlayerDataset, rel_path: Tuple[str, ...],
    base_node_path: Tuple[str, ...], datasets_by_name: Dict[str, SlayerDataset],
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> Tuple[str, ...]:
    """Register hops along ``rel_path`` through the shared walker (reverse hops
    and edge-name tokens included); returns the final node path. The node-path
    token is the edge's CANONICAL token (its ``name`` when named, else its
    ``target_dataset``) so two spellings of one physical edge share one node and
    one alias; the hop's ``target_dataset`` is the resolved dataset.
    An ambiguous hop raises (fail closed)."""
    datasets = {**datasets_by_name, start_dataset.name: datasets_by_name.get(start_dataset.name, start_dataset)}
    chain = follow(root=start_dataset, path=rel_path, datasets_by_name=datasets)
    if chain is None:
        i = next(i for i in range(len(rel_path))
                 if follow(root=start_dataset, path=rel_path[:i + 1], datasets_by_name=datasets) is None)
        via = follow(root=start_dataset, path=rel_path[:i], datasets_by_name=datasets) or []
        raise _PushBlocked(
            f"unreachable from the aggregate's root (no relationship edge from "
            f"{via[-1].to if via else start_dataset.name} to {rel_path[i]})"
        )
    node_path = base_node_path
    for edge in chain:
        own_row = not node_path and _is_parent_hop(edge)
        node_path = (*node_path, canonical_token(edge))
        if own_row:
            continue  # the root list child's parent row is already in the producer's FROM
        _register_hop(
            nodes, node_path=node_path, target_dataset=edge.to,
            keys=relationship_key_fields(edge=edge, source=datasets[edge.source_dataset], target=datasets[edge.to]),
            declared_left=edge.join_type == JoinType.LEFT,
            lateral=_forward_lateral(edge),
        )
    return node_path


def _is_parent_hop(edge) -> bool:
    """A list child's hop back to its parent."""
    return edge.lateral is not None and edge.cardinality is not RelationshipCardinality.ONE_TO_MANY


def _forward_lateral(edge) -> Optional[str]:
    """A list child hop's list field; a parent hop below the producer root cannot be correlated (fail closed)."""
    if edge.lateral is None:
        return None
    if _is_parent_hop(edge):
        raise _PushBlocked("a list child's hop back to its parent is its own row")
    return edge.lateral


def _reverse_hops(
    *, target_path: Tuple[str, ...],
    host_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> Tuple[str, ...]:
    """Register the reverse chain root → … → host by inverting the forward walk
    host → … → root along ``target_path`` — so an edge-name token correlates
    through the exact edge the aggregate's path selected, never a re-parsed
    dataset name. An ambiguous forward hop raises (fail closed in both modes);
    an unresolvable one blocks the push. Returns the host node's path."""
    fwd = follow(root=host_dataset, path=target_path, datasets_by_name=datasets_by_name)
    if fwd is None:
        raise _PushBlocked(
            f"unreachable from the aggregate's root (relationship path "
            f"{'.'.join(target_path)!r} does not resolve from "
            f"{host_dataset.name})"
        )
    node_path: Tuple[str, ...] = ()
    by_name = {**datasets_by_name, host_dataset.name: host_dataset}
    for edge in reversed(fwd):
        node_path = (*node_path, reverse_token(edge))
        keys = relationship_key_fields(edge=edge, source=by_name[edge.source_dataset], target=by_name[edge.to])
        # A list child's parent is its own row, already in the producer's FROM: no hop.
        if edge.lateral is not None:
            continue
        # A reverse hop is the population correlation, not a declared relationship:
        # never null-extended.
        _register_hop(
            nodes, node_path=node_path, target_dataset=edge.source_dataset,
            keys=[(tgt, src) for src, tgt in keys],
            declared_left=False,
        )
    return node_path


def _remap_ref_path(r: ValueKey, node_path: Tuple[str, ...]) -> ValueKey:
    return r.model_copy(update={"path": node_path})


def _resolve_ref_anchor(
    hp: Tuple[str, ...], *, tp: Tuple[str, ...], root_dataset: SlayerDataset,
    host_dataset: SlayerDataset, lookup: Dict[str, SlayerDataset],
    nodes: Dict[Tuple[str, ...], SemiJoinHop], host_name: Optional[str],
    host_node: Optional[Tuple[str, ...]],
) -> Tuple[SlayerDataset, Tuple[str, ...], Tuple[str, ...],
           Optional[Tuple[str, ...]]]:
    """Where one cross-path ref anchors in the correlation tree:
    ``(base dataset, base node, relative path, host node)``."""
    if hp[: len(tp)] == tp:
        return root_dataset, (), hp[len(tp):], host_node
    if host_name is None or (tp and host_name == tp[0]):
        raise _PushBlocked(UNREACHABLE_NO_PATH)
    if hp and _path_edges_exist(root_dataset, hp, lookup):
        return root_dataset, (), hp, host_node
    if host_node is None:
        host_node = _reverse_hops(
            target_path=tp,
            host_dataset=host_dataset, datasets_by_name=lookup, nodes=nodes,
        )
    shared = 0
    while (
        shared < len(hp) and shared < len(tp) - 1
        and hp[shared] == tp[shared]
    ):
        shared += 1
    if shared:
        # The ref rides the reverse path itself: bind to that chain
        # node (same related combination, D3) instead of re-walking.
        # ``host_node`` tokens are canonical (edge name when named), so resolve
        # the base dataset through the node registry, not a token→dataset lookup.
        base_node = host_node[: len(tp) - shared]
        base_dataset = (
            lookup[nodes[base_node].to] if base_node else root_dataset
        )
        return base_dataset, base_node, hp[shared:], host_node
    return host_dataset, host_node, hp, host_node


def _register_dep_hops(
    col: ValueKey, *, node_path: Tuple[str, ...], host_dataset: SlayerDataset,
    lookup: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> List[Tuple[str, ...]]:
    """Register the hops a ref's Mode-A dependencies cross; return the terminal
    node paths of the VALUE definition's dependencies only — a filtered field is
    ``CASE WHEN f THEN v END``, NULL when ``v`` is, never because ``f``'s
    dependencies are (D5)."""
    dep_rels = _ref_sql_dependency_paths(
        col, host_dataset=host_dataset, datasets_by_name=lookup, bundle=bundle,
    )
    if not dep_rels:
        return []
    # Non-empty deps imply a FieldSqlKey with a resolved owner.
    assert isinstance(col, FieldSqlKey)
    value_rels = set(_ref_sql_dependency_paths(
        col, host_dataset=host_dataset, datasets_by_name=lookup, bundle=bundle,
        include_filter=False,
    ))
    owner = _owning_dataset(
        col.dataset, host_dataset=host_dataset, datasets_by_name=lookup,
    )
    assert owner is not None
    terminals: List[Tuple[str, ...]] = []
    for dep_rel in dep_rels:
        terminal = _forward_hops(
            start_dataset=owner, rel_path=tuple(dep_rel),
            base_node_path=node_path, datasets_by_name=lookup, nodes=nodes,
        )
        if dep_rel in value_rels:
            terminals.append(terminal)
    return terminals


# --------------------------------------------------------------------------- #
# Null-rejection analysis. A conservative three-valued evaluation
# (values T/F/U/D) of a conjunct with hop ``h`` and its descendants NULL decides
# whether the conjunct rejects ``h``'s null-extended row (value FALSE/UNKNOWN);
# ``null_extended(h) = declared LEFT ∧ ¬rejects(h)``. DEPENDS never rejects, so a
# wrong answer can only choose LEFT (always correct), never INNER. The group's
# AND-ed predicate rejects h iff ANY of its conjuncts does, so the per-conjunct
# result combines by OR at grouping time.
_T, _F, _U, _D = "T", "F", "U", "D"
#: AND / OR folds: the first value present wins, the last is the identity.
_AND_ORDER = (_F, _U, _D, _T)
_OR_ORDER = (_T, _D, _U, _F)
_NOT_VALUE = {_T: _F, _F: _T, _U: _U, _D: _D}
_IS_NOT = "is not"

#: null-source paths per ref: nulling any hop on/above one nulls the ref.
_NullSources = Dict[ValueKey, "frozenset[Tuple[str, ...]]"]


def _is_prefix(prefix: Tuple[str, ...], path: Tuple[str, ...]) -> bool:
    return path[: len(prefix)] == prefix


#: Predicate-valued operators: as a value operand they are NULL iff UNKNOWN.
_PREDICATE_OPS = frozenset({"and", "or", "not"}) | PREDICATE_COMPARISON_OPS


def _operand_null(k: Any, *, h: Tuple[str, ...], srcs: _NullSources) -> bool:
    """Is value operand ``k`` NULL when hop ``h`` and its descendants are NULL?
    A leaf's null-source paths decide it; arithmetic over a null operand is null;
    a predicate is null iff UNKNOWN; a ScalarCall / Star is data-dependent."""
    if isinstance(k, TimeTruncKey):
        return _operand_null(k.source, h=h, srcs=srcs)
    if isinstance(k, (FieldKey, FieldSqlKey, StarKey)):
        return any(_is_prefix(h, p) for p in srcs.get(k, frozenset()))
    if isinstance(k, InKey) or (
        isinstance(k, ArithmeticKey) and k.op.lower() in _PREDICATE_OPS
    ):
        return _pred_value(k, h=h, srcs=srcs) == _U
    if isinstance(k, ArithmeticKey):
        return any(_operand_null(o, h=h, srcs=srcs) for o in k.operands)
    return False


def _unknown_if_null(
    operands: Iterable[Any], *, h: Tuple[str, ...], srcs: _NullSources,
) -> str:
    return _U if any(_operand_null(o, h=h, srcs=srcs) for o in operands) else _D


def _fold(*, vals: Iterable[str], order: Tuple[str, ...]) -> str:
    present = set(vals)
    return next((v for v in order if v in present), order[-1])


def _is_value(
    *, a: Any, b: Any, negated: bool, h: Tuple[str, ...], srcs: _NullSources,
) -> str:
    """``IS`` / ``IS NOT`` between a literal and a null-valued operand, either
    order (NULL IS NULL, NULL IS NOT TRUE → TRUE; the mirrors → FALSE); anything
    else is DEPENDS."""
    lit, val = (a, b) if isinstance(a, LiteralKey) else (b, a)
    if not isinstance(lit, LiteralKey) or not _operand_null(val, h=h, srcs=srcs):
        return _D
    return _T if (lit.value is None) != negated else _F


def _pred_value(cj: ValueKey, *, h: Tuple[str, ...], srcs: _NullSources) -> str:
    """Three-valued (T/F/U/D) value of predicate ``cj`` under ``h``-NULL."""
    if isinstance(cj, InKey):
        return _unknown_if_null((cj.field,), h=h, srcs=srcs)
    if not isinstance(cj, ArithmeticKey):
        return _D
    op = cj.op.lower()
    if op in ("and", "or"):
        vals = [_pred_value(o, h=h, srcs=srcs) for o in cj.operands]
        return _fold(vals=vals, order=_AND_ORDER if op == "and" else _OR_ORDER)
    if op == "not":
        return _NOT_VALUE[_pred_value(cj.operands[0], h=h, srcs=srcs)]
    if op in ("is", _IS_NOT):
        return _is_value(
            a=cj.operands[0], b=cj.operands[1], negated=op == _IS_NOT, h=h, srcs=srcs,
        )
    if op in NULL_SAFE_COMPARISON_OPS:
        return _D
    if op in PREDICATE_COMPARISON_OPS:
        return _unknown_if_null(cj.operands, h=h, srcs=srcs)
    return _D


def _ref_null_propagates(
    col: ValueKey, *, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> bool:
    """A structural ref is NULL under a NULL node; a derived ref only when its
    expanded definition propagates NULL (D5), else it is data-dependent."""
    if not isinstance(col, FieldSqlKey):
        return True
    found = _derived_field_owner(
        col, host_dataset=host_dataset, datasets_by_name=datasets_by_name,
    )
    return found is not None and fragment_null_propagates(
        field=found[1], dataset=found[0], anchor_relation=found[0].name,
        bundle=bundle,
    )


def _conjunct_rejects_by_hop(
    cj: ValueKey, *, hop_paths: Iterable[Tuple[str, ...]], srcs: _NullSources,
) -> Dict[Tuple[str, ...], bool]:
    """Per hop, whether ``cj`` rejects that hop's null-extended row (value F/U)."""
    return {
        hp: _pred_value(cj, h=hp, srcs=srcs) in (_F, _U) for hp in hop_paths
    }


def _conjunct_push_plan(
    cj: ValueKey, *, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    host_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    bundle: ResolvedSourceBundle, host_name: Optional[str],
) -> Tuple[ValueKey, Dict[Tuple[str, ...], SemiJoinHop], Dict[Tuple[str, ...], bool]]:
    """Resolve a not-fully-attributable conjunct into a correlation-tree plan:
    the conjunct rewritten into producer-root coordinates (ref paths = tree-node
    paths, root-local refs correlate as outer references), the hop registry, and
    per-hop null-rejection (D5). Raises :class:`_PushBlocked` when a reference has
    no resolvable relationship path."""
    tp = tuple(target_path)
    lookup = dict(datasets_by_name)
    lookup.setdefault(host_dataset.name, host_dataset)
    nodes: Dict[Tuple[str, ...], SemiJoinHop] = {}
    mapping: Dict[ValueKey, ValueKey] = {}
    srcs: Dict[ValueKey, "frozenset[Tuple[str, ...]]"] = {}
    host_node: Optional[Tuple[str, ...]] = None
    for r in walk_value_keys(cj):
        if not isinstance(r, (FieldKey, FieldSqlKey, StarKey)):
            continue
        hp = tuple(r.path)
        base_dataset, base_node, rel, host_node = _resolve_ref_anchor(
            hp, tp=tp, root_dataset=root_dataset, host_dataset=host_dataset,
            lookup=lookup, nodes=nodes, host_name=host_name,
            host_node=host_node,
        )
        node_path = _forward_hops(
            start_dataset=base_dataset, rel_path=rel, base_node_path=base_node,
            datasets_by_name=lookup, nodes=nodes,
        )
        dep_terminals = _register_dep_hops(
            r, node_path=node_path, host_dataset=host_dataset, lookup=lookup,
            bundle=bundle, nodes=nodes,
        )
        remapped = _remap_ref_path(r, node_path)
        # Null sources: a ref is null when its own node, an ancestor or a
        # dependency terminal is — unless data-dependent (a star, a
        # non-propagating derived definition).
        if isinstance(r, StarKey) or not _ref_null_propagates(
            r, host_dataset=host_dataset, datasets_by_name=lookup, bundle=bundle,
        ):
            srcs[remapped] = frozenset()
        else:
            srcs[remapped] = frozenset({node_path, *dep_terminals})
        if remapped != r:
            mapping[r] = remapped
    # Pushdown is total over the conjunct's boolean shape — no OR/NOT
    # or single-branch block. Multiple first-level hops are a product; grouping
    # (union-find) merges shared branches downstream.
    remapped_cj = substitute_value_keys(cj, mapping)
    rejects = _conjunct_rejects_by_hop(
        remapped_cj, hop_paths=nodes.keys(), srcs=srcs)
    return remapped_cj, nodes, rejects


def _conjunct_disposition(
    cj: ValueKey, *, text: Optional[str], target_path: Tuple[str, ...],
    root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    host_name: str, host_dataset: SlayerDataset, bundle: ResolvedSourceBundle,
) -> Tuple[Optional[BoundFilter], Optional[_Push]]:
    """Two-way ROW-conjunct disposition (D1): inline / semi-join pushed. A
    bound conjunct always resolves from the host, so a push-plan block is an
    invariant violation and propagates (fail closed), never a silent drop."""
    refs = [
        k for k in walk_value_keys(cj)
        if isinstance(k, (FieldKey, FieldSqlKey, TimeTruncKey, StarKey))
    ]

    def _attributable(r: ValueKey) -> bool:
        # Judge each ref on its dependency closure — a derived field
        # crossing a fanning hop is unattributable exactly as a structural one is.
        return key_attributable_from_root(
            key=r, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
            host_dataset=host_dataset, host_name=host_name,
        )

    if all(_attributable(r) for r in refs):
        rerooted = reroot_from_root(
            cj, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_name,
        )
        return bound_filter_from_key(rerooted), None
    display = text or _canonical_name(cj)
    # A conjunct whose dependency closure no dialect can analyse is unsafe,
    # never 'crosses nothing' — fail closed for host and producers alike.
    if key_closure(
        key=cj, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
        bundle=bundle,
    ) is None:
        check_filter_dependencies_analyzable(
            filter_text=display,
            field=first_unanalyzable_filter_field(
                key=cj, anchor_dataset=host_dataset,
                anchor_relation=host_dataset.name, bundle=bundle,
            ),
        )
    key_rewritten, nodes, rejects = _conjunct_push_plan(
        cj, target_path=target_path, root_dataset=root_dataset,
        host_dataset=host_dataset, datasets_by_name=datasets_by_name,
        bundle=bundle, host_name=host_name,
    )
    return None, (key_rewritten, display, nodes, rejects)


class _DisposedConjunct(BaseModel):
    """One disposed population ROW conjunct (D1) with its fanning paths for the
    per-consumer same-row rule (D2)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    conjunct: ValueKey
    inline_bf: BoundFilter
    text: Optional[str] = None
    is_date_range: bool
    disposition: Literal["inline", "semi_join"]
    fanning_paths: Tuple[Tuple[str, ...], ...] = ()
    push: Optional[_Push] = None


class PopulationFilters(BaseModel):
    """The query population's ROW-filter disposition, computed once at the host
    root (D1) and consumed by the host base query and every host-rooted regroup
    producer; each consumer derives a view for its own grain (D2)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    conjuncts: List[_DisposedConjunct] = Field(default_factory=list)

    def host_split(
        self, grain_paths: AbstractSet[Tuple[str, ...]],
    ) -> Tuple[List[SemiJoinFilter], List[ValueKey]]:
        """(semi-join groups, conjunct keys to drop from the masks). A semi-join
        conjunct whose fanning branches the grain all materialises binds inline
        to the grouped row (D2); one it materialises some of quantifies only the
        rest (D6); otherwise the push as planned."""
        pushes: List[_Push] = []
        pushed_keys: List[ValueKey] = []
        for dc in self.conjuncts:
            if dc.disposition != "semi_join":
                continue
            materialised = [
                fp for fp in dc.fanning_paths if _materialised(fp, grain_paths)
            ]
            if len(materialised) == len(dc.fanning_paths):
                continue
            assert dc.push is not None
            pushes.append(
                _reduce_push(dc.push, grain_paths=grain_paths) if materialised
                else dc.push
            )
            pushed_keys.append(dc.conjunct)
        return _semi_join_groups_from_pushes(pushes), pushed_keys

    @property
    def has_semi_joins(self) -> bool:
        return any(dc.disposition == "semi_join" for dc in self.conjuncts)


class InheritedPopulation(BaseModel):
    """The enclosing plan's row-filter disposition, applied on the producer's grain."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    filters: PopulationFilters


class NoInheritedPopulation(BaseModel):
    """The producer inherits no row population; ``reason`` says why."""

    model_config = ConfigDict(frozen=True)

    reason: str


Population = InheritedPopulation | NoInheritedPopulation


def _population_of(filters: Optional[PopulationFilters]) -> Population:
    if filters is None:
        return NoInheritedPopulation(reason="the host has no row population to dispose")
    return InheritedPopulation(filters=filters)


class ProducerContext(BaseModel):
    """What a synthesized producer inherits from its enclosing plan."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    enclosing_grain: Grain
    population: Population
    carried_attaches: Tuple[RegroupAttachPlan, ...] = ()
    reserved_placeholders: FrozenSet[ValueKey] = frozenset()
    #: The one kernel-requiring aggregate this producer renders inline (its kernel's answer).
    kernel_answer: Optional[AggregateKey] = None
    #: Placeholders it reads from an attach riding alongside it, each to its original key.
    placeholder_originals: Mapping[ValueKey, ValueKey] = Field(default_factory=dict)
    #: The transform whose domain this producer's rows are (semantics Axiom 11.6).
    domain_of: Optional[TransformKey] = None


def _materialised(
    path: Tuple[str, ...], grain_paths: AbstractSet[Tuple[str, ...]],
) -> bool:
    return any(len(path) <= len(gp) and gp[: len(path)] == path for gp in grain_paths)


def _reduce_push(
    push: _Push, *, grain_paths: AbstractSet[Tuple[str, ...]],
) -> _Push:
    """Drop the hops the consumer's grain materialises — their refs bind to the
    outer query's aliases — and quantify only the rest (D6)."""
    key, text, nodes, rejects = push
    kept = {p: h for p, h in nodes.items() if not _materialised(p, grain_paths)}
    if len(kept) == len(nodes):
        return push
    assert kept, f"population conjunct {text!r}: every hop materialised yet not inlined"
    return key, text, kept, {p: r for p, r in rejects.items() if p in kept}


def _dispose_one_conjunct(
    cj: ValueKey, *, text: Optional[str], is_date: bool,
    host_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    bundle: ResolvedSourceBundle,
) -> _DisposedConjunct:
    """Dispose one top-level population conjunct (D1) — inline or semi-join —
    recording its fanning paths for the per-consumer same-row rule (D2)."""
    inline_bf, pushed = _conjunct_disposition(
        cj, text=text, target_path=(), root_dataset=host_dataset,
        datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        host_dataset=host_dataset, bundle=bundle,
    )
    closure = key_closure(
        key=cj, anchor_dataset=host_dataset,
        anchor_relation=host_dataset.name, bundle=bundle,
    ) or ()
    fanning = tuple(
        _canonical_path(p, root=host_dataset, datasets_by_name=datasets_by_name)
        for p in closure
        if p and not safe_reachable(
            root=host_dataset, path=p, datasets_by_name=datasets_by_name,
        )
    )
    if inline_bf is not None:
        disposition: Literal["inline", "semi_join"] = "inline"
        push = None
    else:
        assert pushed is not None
        disposition, push = "semi_join", pushed
        inline_bf = bound_filter_from_key(cj)
    return _DisposedConjunct(
        conjunct=cj, inline_bf=inline_bf, text=text, is_date_range=is_date,
        disposition=disposition, fanning_paths=fanning, push=push,
    )


def dispose_population_filters(
    *, prebound: PreboundQuery, filter_typings: Sequence[ConjunctTyping],
    scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> Optional[PopulationFilters]:
    """Dispose the population's ROW-phase, FIELD-typed, stratum-0 filter conjuncts
    once at the host root (D1) — inline / semi-join / excluded — recording each
    conjunct's fanning paths for the per-consumer same-row rule (D2). Raises the
    analyzability error on a conjunct whose closure cannot be analysed."""
    host_dataset = scope.population
    if host_dataset is None:
        return None
    datasets_by_name = bundle.datasets_by_name
    texts = prebound.bound_filter_texts
    out: List[_DisposedConjunct] = []
    for i, bf, ct in _population_filters(prebound, filter_typings):
        if (bf.phase != Phase.ROW or ct.typing != MaskTyping.FIELD
                or ct.stratum != 0):
            continue
        text = texts[i] if i < len(texts) else None
        is_date = i < prebound.n_date_range
        for cj in split_top_level_and(bf.value_key):
            out.append(_dispose_one_conjunct(
                cj, text=text, is_date=is_date, host_dataset=host_dataset,
                datasets_by_name=datasets_by_name, bundle=bundle,
            ))
    return PopulationFilters(conjuncts=out)


def _drop_conjuncts(
    value_key: ValueKey, drop: AbstractSet[ValueKey],
) -> Optional[ValueKey]:
    """Rebuild a filter's key keeping only the top-level AND conjuncts not in
    ``drop`` (the ones pushed to a semi-join); ``None`` when all are dropped."""
    if not drop:
        return value_key
    kept = [cj for cj in split_top_level_and(value_key) if cj not in drop]
    if not kept:
        return None
    result = kept[0]
    for cj in kept[1:]:
        result = ArithmeticKey(op="and", operands=(result, cj))
    return result


def _canonical_path(
    path: Sequence[str], *, root: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset],
) -> Tuple[str, ...]:
    """A relationship path in canonical hop tokens (edge name, else target dataset — D3),
    so closure paths and push nodes compare regardless of spelling."""
    edges = follow(root=root, path=tuple(path), datasets_by_name=datasets_by_name)
    if edges is None:
        return tuple(path)
    return tuple(canonical_token(e) for e in edges)


def _grain_closure_paths(
    grain_keys: Sequence[ValueKey], *, host_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
) -> "set[Tuple[str, ...]]":
    """Every relationship path (canonical) the grain keys' dependency closures cross —
    the paths a consumer's grain materialises (D2)."""
    out: "set[Tuple[str, ...]]" = set()
    for k in grain_keys:
        closure = key_closure(
            key=k, anchor_dataset=host_dataset, anchor_relation=host_dataset.name,
            bundle=bundle,
        )
        for p in closure or ():
            if p:
                out.add(_canonical_path(
                    p, root=host_dataset, datasets_by_name=bundle.datasets_by_name))
    return out


class _ProducerSynthesisContext(BaseModel):
    """The per-plan inputs every cross-dataset producer synthesis shares."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    prebound: PreboundQuery
    bundle: ResolvedSourceBundle
    host_dataset: SlayerDataset
    datasets_by_name: Dict[str, SlayerDataset]
    projected_dim_keys: List[ValueKey]
    projected_td_keys: List[ValueKey]
    base_filters_with_text: List[Tuple[BoundFilter, Optional[str]]]
    scope: DatasetScope
    stage_schemas: Dict[str, StageSchema]
    # Every aggregate's home (Axiom 2), resolved in the elaborator.
    homes: AggregateHomes = Field(default_factory=AggregateHomes)
    # The population a windowed producer's endpoints are drawn from.
    population: Optional[Population] = None
    population_filters: List[BoundFilter] = []
    n_population_date: int = 0

    def home_of(self, agg: AggregateKey) -> Tuple[str, ...]:
        return self.homes.of(agg).path

    def dim_display(self, key: ValueKey) -> str:
        return _query_dim_display(self.prebound, key)

    def endpoints_source(self) -> "_EndpointsSource":
        return _EndpointsSource(
            host_dataset=self.host_dataset, bundle=self.bundle, scope=self.scope,
            stage_schemas=self.stage_schemas, population=self.population,
            population_filters=self.population_filters, n_population_date=self.n_population_date,
            mode_policy=self.prebound.mode_policy, ranking_candidates=self.prebound.ranking_candidates,
        )


def _query_dim_display(prebound: PreboundQuery, key: ValueKey) -> str:
    """A key's query name: its first explicitly declared dimension name, else its expression text."""
    for dm in prebound.declared_metrics[: prebound.n_dims + prebound.n_time_dimensions]:
        if dm.name_is_explicit and dm.bound.value_key == key:
            return dm.declared_name
    return key_display(key)


_KeyT = TypeVar("_KeyT", bound=ValueKey)


def _query_spelling(key: ValueKey, *, context: _ProducerSynthesisContext) -> str:
    """``key`` in canonical form, each reference spelled from its query anchor, a time bucket by its field."""
    if isinstance(key, TimeTruncKey):
        key = key.source
    datasets = {**context.datasets_by_name, context.host_dataset.name: context.host_dataset}
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in walk_value_keys(key):
        if isinstance(r, (FieldKey, FieldSqlKey, StarKey)) and r not in mapping:
            parts = context.prebound.reference_anchors.get(
                _canonical_path(r.path, root=context.host_dataset, datasets_by_name=datasets))
            if parts is not None:
                mapping[r] = r.model_copy(update={"path": parts})
    return key_display(substitute_value_keys(key, mapping) if mapping else key)


def _broadcast_replacement(
    agg: AggregateKey, attributable: Iterable[ValueKey], *, context: _ProducerSynthesisContext,
) -> Dict[str, Any]:
    """A broadcast attach's call and the ``partition_by=`` returning its values."""
    return {
        "broadcast_call": _query_spelling(agg, context=context),
        "broadcast_partition_by": list(dict.fromkeys(_query_spelling(g, context=context) for g in attributable)),
    }


class _UnattributableDim(NamedTuple):
    """A requested grain dimension not attributable from the aggregate's root: its host key, query name, broadcast
    reason and its key routed from the home."""

    key: ValueKey
    name: str
    reason: str
    routed: ValueKey


def _home_route(
    parts: Tuple[str, ...], *, root: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Tuple[str, ...]]:
    """The home's path to a reference spelled ``parts`` (anchor, canonical route): its own route to the anchor, then
    the route; ``None`` for the spine or an unrelated anchor."""
    anchor = find_anchor(parts[0], quoted=True, reader=root, datasets_by_name=datasets_by_name)
    if anchor is None or is_spine(anchor):
        return None
    reached = anchor_route(reader=root, anchor=anchor, datasets_by_name=datasets_by_name, rest=parts[1:])
    return None if reached.unrelated else (*reached.route, *parts[1:])


def _home_routing(
    keys: Iterable[ValueKey], *, anchors: Mapping[Tuple[str, ...], Tuple[str, ...]], host_dataset: SlayerDataset,
    target_path: Tuple[str, ...], root_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    source_anchor: Optional[str],
) -> Dict[ValueKey, ValueKey]:
    """Each leaf of ``keys`` the home reads by another route than the host's → its host coordinates through the home
    (queries/references: a reference is routed from the dataset reading it; one anchored at ``source_anchor`` is read
    back along the source's written path, which the host's reroot already is)."""
    mapping: Dict[ValueKey, ValueKey] = {}
    if not target_path or not anchors:
        return mapping
    for key in keys:
        for r in walk_value_keys(key):
            if not isinstance(r, (FieldKey, FieldSqlKey, StarKey)) or r in mapping:
                continue
            rerouted = _home_reroute(
                r, anchors=anchors, host_dataset=host_dataset, target_path=target_path, root_dataset=root_dataset,
                datasets_by_name=datasets_by_name, source_anchor=source_anchor,
            )
            if rerouted is not None:
                mapping[r] = rerouted
    return mapping


def _home_reroute(
    r: FieldKey | FieldSqlKey | StarKey, *, anchors: Mapping[Tuple[str, ...], Tuple[str, ...]],
    host_dataset: SlayerDataset, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], source_anchor: Optional[str],
) -> Optional[ValueKey]:
    """``r`` routed through the home, when the home reads it by another route than the host."""
    parts = anchors.get(_canonical_path(r.path, root=host_dataset, datasets_by_name=datasets_by_name))
    if parts is None or parts[0] == source_anchor:
        return None
    route = _home_route(parts, root=root_dataset, datasets_by_name=datasets_by_name)
    if route is None:
        return None
    via_host = reroot_from_root(r, target_path=target_path, root_dataset=root_dataset,
                                datasets_by_name=datasets_by_name, host_name=host_dataset.name)
    if _canonical_path(cast(FieldKey, via_host).path, root=root_dataset, datasets_by_name=datasets_by_name) == route:
        return None
    return r.model_copy(update={"path": (*target_path, *route)})


def _source_anchor(
    agg: AggregateKey, *, anchors: Mapping[Tuple[str, ...], Tuple[str, ...]], host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[str]:
    """The anchor ``agg``'s source was written from: the query reference spelling it, else (a saved measure composed
    at a frame) the one spelling its longest prefix — the frame's."""
    paths = [
        _canonical_path(r.path, root=host_dataset, datasets_by_name=datasets_by_name)
        for r in walk_value_keys(agg.source) if isinstance(r, (FieldKey, FieldSqlKey, StarKey))
    ]
    for path in paths:
        if (parts := anchors.get(path)) is not None:
            return parts[0]
    prefixes = [path[:n] for path in paths for n in range(len(path) - 1, -1, -1) if path[:n] in anchors]
    return anchors[max(prefixes, key=len)][0] if prefixes else None


def _synthesize_cross_dataset_producer(  # NOSONAR(S3776) — one cohesive target-rooted producer synthesis (root / safe-grain / broadcast / inputs / filter-inheritance / recursive plan / attach); the arms share the re-rooting coordinate state.
    *,
    agg: AggregateKey,
    placeholder: ValueKey,
    attach_phase: Literal["row", "combined"],
    public_alias: Optional[str],
    context: _ProducerSynthesisContext,
    declared_type: Optional[DataType] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> RegroupAttachPlan:
    """Build one target-rooted regroup producer for a cross-dataset aggregate: root at its source, compute at the fan-out-safe grain subset, broadcast the rest."""
    prebound, bundle = context.prebound, context.bundle
    host_dataset, datasets_by_name = context.host_dataset, context.datasets_by_name
    projected_dim_keys = context.projected_dim_keys
    projected_td_keys = context.projected_td_keys
    base_filters_with_text = context.base_filters_with_text
    stage_schemas = context.stage_schemas
    target_path = context.home_of(agg)
    root_dataset = walk_key_path(dataset=host_dataset, path=target_path, bundle=bundle)
    if root_dataset is None:  # pragma: no cover — bind resolved the path already
        check_cross_dataset_source_resolves(
            target_path=target_path, host_name=host_dataset.name,
        )
    root_dataset = bundle.factor_free(root_dataset)
    root_name = root_dataset.name
    alias = public_alias or canonical_aggregate_alias(agg, profile="stage_expression")
    assert alias is not None  # a projected cross-dataset aggregate always names one

    # Requested grain G: explicit partition_by (in canonical order, so a refusal names one key) else the query
    # dimensions.
    if agg.partition_keys is not None:
        requested = _regroup_partition_order(agg.partition_keys)
        explicit = True
    else:
        # A member the rest of the grain determines is redundant to group by: it neither associates nor broadcasts.
        kept = _prune_functionally_determined_grain(Grain.of([*projected_dim_keys, *projected_td_keys]))
        requested = [g for g in [*projected_dim_keys, *projected_td_keys] if g in kept]
        explicit = False

    policy = context.prebound.mode_policy
    # Host included so the reverse (fanning) hop resolves for the broadcast
    # reason; attributability/filter routing keep the host-free map unchanged.
    datasets_with_host = {**datasets_by_name, host_dataset.name: host_dataset}
    # The home reads its grain and the filters restricting it by its own routes (queries/references).
    route_home = functools.partial(
        _home_routing,
        anchors=prebound.reference_anchors, host_dataset=host_dataset, target_path=target_path,
        root_dataset=root_dataset, datasets_by_name=datasets_with_host,
        source_anchor=_source_anchor(agg, anchors=prebound.reference_anchors, host_dataset=host_dataset,
                                     datasets_by_name=datasets_with_host),
    )
    home_routing = route_home(
        [*requested, *(bf.value_key for bf, _t in base_filters_with_text),
         *((prebound.main_time_key,) if prebound.main_time_key is not None else ())],
    )

    def routed(key: _KeyT, routing: Dict[ValueKey, ValueKey] = home_routing) -> _KeyT:
        return substitute_value_keys(key, routing) if routing else key

    base_filters_with_text = [
        *((substitute_in_bound_filter(bf, home_routing) if home_routing else bf, t) for bf, t in base_filters_with_text),
        *_host_membership(
            host_dataset=host_dataset, target_path=target_path, datasets_by_name=datasets_with_host, bundle=bundle,
        ),
    ]
    home = root_dataset

    def home_determined(
        g: ValueKey, routing: Dict[ValueKey, ValueKey] = home_routing,
    ) -> Tuple[Optional[ValueKey], bool]:
        """``g`` in the home's coordinates when the home determines it (``None`` else), and whether by a shared key."""
        shared = shared_relationship_key_reroot(
            key=g, target_path=target_path, host_dataset=host_dataset,
            datasets_by_name=datasets_by_name,
        )
        if shared is not None:
            # The join-key identity needs no join in the producer.
            return shared, True
        g_home = routed(g, routing)
        if not grain_member_attributable(
            key=g_home, target_path=target_path, root_dataset=home,
            datasets_by_name=datasets_by_name, bundle=bundle, host_dataset=host_dataset,
            host_name=host_dataset.name,
        ):
            return None, False
        return reroot_from_root(
            g_home, target_path=target_path, root_dataset=home,
            datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        ), False

    # Safe grain S (attributable from R) vs unattributable.
    safe_pairs: List[Tuple[ValueKey, ValueKey]] = []  # (host_key, rerooted_key)
    unattributable: List[_UnattributableDim] = []
    for g in requested:
        rerooted, by_shared_key = home_determined(g)
        if rerooted is None:
            g_home = routed(g)
            unattributable.append(_UnattributableDim(
                key=g, name=context.dim_display(g), reason=key_broadcast_reason(
                    key=g_home, target_path=target_path, root_dataset=root_dataset,
                    datasets_by_name=datasets_with_host, bundle=bundle,
                    host_dataset=host_dataset, host_name=host_dataset.name,
                ), routed=g_home,
            ))
            continue
        if not by_shared_key:
            check_axis_rebucket(host_key=g, rerooted=rerooted, host=host_dataset, root=root_dataset, bundle=bundle)
        safe_pairs.append((g, rerooted))
    def candidate_determined(c: ValueKey) -> Optional[ValueKey]:
        """A default ranking-key candidate in the home's coordinates; ``None`` when the home does not determine it,
        an ambiguous route included."""
        try:
            routing = {**home_routing, **route_home([c])}
        except UnresolvableDimensionRelationshipError:
            return None
        return home_determined(c, routing)[0]

    # The query's dimensions the home determines, in its coordinates and order: its default ranking-key candidates.
    ranking_candidates = list(prebound.ranking_candidates) if not target_path else [
        rr for c in prebound.ranking_candidates if (rr := candidate_determined(c)) is not None
    ]

    # One rooting law (Axiom 2.5): re-anchor into the home's coordinates once, a
    # host-side leaf becoming a reverse-hop reference, so every attached input's
    # producer compiles at its own home whatever the mode.
    agg_rooted = reroot_from_root(
        key=agg, target_path=target_path, root_dataset=root_dataset,
        datasets_by_name=datasets_by_name, host_name=host_dataset.name,
    )
    product = crosses_product_hop(host=host_dataset, path=target_path, datasets_by_name=datasets_by_name)
    # Across a product hop every cell associates all home rows: the broadcast producer, warned as associated.
    product_associate = policy.associates and product
    rerooted_of = dict(safe_pairs)

    def _rerooted(g: ValueKey) -> ValueKey:
        return rerooted_of.get(g) or reroot_from_root(
            g, target_path=target_path, root_dataset=home,
            datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        )

    # A crossing implicit ranking key fails closed in every mode, ahead of any grain or axis decision.
    if is_ranked(agg_rooted) and explicit_ranking_key(agg_rooted) is None:
        _checked_ranking_time_key(
            row_keys=ranking_candidates, agg_key=agg_rooted, root_dataset=root_dataset,
            bundle=bundle.rerooted(root_dataset), alias=canonical_aggregate_alias(agg, profile="stage_expression"),
            target_rooted=bool(target_path),
        )
    active_td = prebound.main_time_key
    td_home = routed(active_td) if active_td is not None else None
    if window_kwarg_of(agg) is not None and active_td is not None and td_home is not None and not key_attributable_from_root(
        key=td_home, target_path=target_path, root_dataset=root_dataset, datasets_by_name=datasets_by_name,
        bundle=bundle, host_dataset=host_dataset, host_name=host_dataset.name,
    ):
        # A fanning window axis is judged before the grain: it associates, or it is refused.
        if not policy.associates or product:
            check_windowed_time_axis_attributable(
                alias=alias, root_name=root_name, active_td_name=context.dim_display(active_td),
                attributable=False,
            )
        if all(u.key != active_td for u in unattributable):
            unattributable.append(_UnattributableDim(
                key=active_td, name=context.dim_display(active_td), reason=key_broadcast_reason(
                    key=td_home, target_path=target_path, root_dataset=root_dataset,
                    datasets_by_name=datasets_with_host, bundle=bundle,
                    host_dataset=host_dataset, host_name=host_dataset.name,
                ), routed=td_home,
            ))
    # Association only when a DIMENSION is unattributable; attached inputs never need it.
    associate = policy.associates and bool(unattributable) and not product_associate
    entity: Optional[_AssociationEntity] = None
    if associate:
        entity = _association_entity(
            root_dataset=root_dataset, target_path=target_path, unattributable=unattributable,
            host_dataset=host_dataset, datasets_by_name=datasets_by_name, bundle=bundle,
        )
    window_td_key: Optional[TimeTruncKey] = None
    semi_joins: List[SemiJoinFilter] = []
    broadcast: List[Tuple[str, str]] = []
    picked_params: List[PickedParam] = []
    restricted_texts: List[str] = []
    entity_keys_root: List[ValueKey] = []
    associated_metric: Optional[str] = None
    associated_dimensions: List[str] = []

    if associate:
        # ASSOCIATION ARM (D2-4, 8): keep every unattributable dimension as a
        # rerooted grain member joined back on the host key, and dedup per home
        # entity via the association kernel — a home entity absent from the
        # population still counts in the cells its own path reaches.
        assert entity is not None
        agg_rooted = agg_rooted.model_copy(update={"locus": "host"})
        picked_params, entity_keys_root = _association_arm(
            agg=agg, agg_rooted=agg_rooted, alias=alias, entity=entity,
            target_path=target_path, host_dataset=host_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
        )
        # The unattributable dims join back on the host key exactly like safe_pairs.
        safe_pairs = [*safe_pairs, *entity.assoc_pairs]
        inherited, restricted_texts = _association_inline_filters(
            base_filters=base_filters_with_text, target_path=target_path,
            root_dataset=root_dataset, datasets_by_name=datasets_by_name,
            host_dataset=host_dataset, bundle=bundle,
        )
        associated_metric = None if explicit else alias
        associated_dimensions = (
            [] if explicit else [u.name for u in unattributable]
        )
        if window_kwarg_of(agg) is not None:
            if active_td is None:
                check_windowed_time_axis_attributable(
                    alias=alias, root_name=root_name, active_td_name=None, attributable=False,
                )
            assert active_td is not None and td_home is not None  # the checker raised otherwise
            window_td_key = cast(TimeTruncKey, dict(safe_pairs).get(active_td) or _rerooted(td_home))
    else:
        # BROADCAST / ERROR ARM: an unattributable explicit key is a hard error.
        if product_associate:
            associated_metric = None if explicit or not unattributable else alias
            associated_dimensions = [] if explicit else [u.name for u in unattributable]
        else:
            check_partition_keys_attributable(
                alias=alias, root_name=root_name, explicit=explicit,
                unattributable=[(u.name, u.reason) for u in unattributable],
            )
            broadcast = [(u.name, u.reason) for u in unattributable]
        if target_path != source_anchor_path(agg.source):
            # The source sits beyond the home: render inline as a host-locus
            # aggregate joining the to-one path from the home, never a
            # source-rooted producer.
            agg_rooted = agg_rooted.model_copy(update={"locus": "host"})
        _assert_cross_dataset_inputs_safe(
            agg=agg, agg_rooted=agg_rooted, root_dataset=root_dataset, root_name=root_name,
            target_path=target_path, bundle=bundle, datasets_by_name=datasets_by_name,
            host_name=host_dataset.name, host_dataset=host_dataset,
        )

        # A windowed cross-dataset aggregate folds the active TD into its grain as the bucket (must be attributable from the root).
        if window_kwarg_of(agg) is not None:
            active_td = prebound.main_time_key
            check_windowed_time_axis_attributable(
                alias=alias, root_name=root_name,
                active_td_name=(
                    None if active_td is None else context.dim_display(active_td)
                ),
                attributable=active_td is not None and key_attributable_from_root(
                    key=routed(active_td), target_path=target_path,
                    root_dataset=root_dataset, datasets_by_name=datasets_by_name,
                    bundle=bundle, host_dataset=host_dataset,
                    host_name=host_dataset.name,
                ),
            )
            assert active_td is not None  # the checker raised otherwise
            window_td_key = reroot_from_root(
                key=routed(active_td), target_path=target_path, root_dataset=root_dataset,
                datasets_by_name=datasets_by_name, host_name=host_dataset.name,
            )

        inherited, semi_joins = _cross_dataset_inherited_filters(
            base_filters=base_filters_with_text, target_path=target_path,
            root_dataset=root_dataset, datasets_by_name=datasets_by_name,
            host_name=host_dataset.name, host_dataset=host_dataset, bundle=bundle,
        )

    # SHARED TAIL: root at the home, compile the producer, attach on the host key.
    condition, unfiltered = _group_row_restriction([agg_rooted])
    if condition is not None:
        inherited = [*inherited, row_restriction_from_key(condition)]
        agg_rooted = unfiltered[agg_rooted]
    _check_attached_params_determined(
        agg=agg, alias=alias, target_path=target_path, root_dataset=root_dataset, context=context,
    )
    numbered: List[SlayerDataset] = []
    if associate and entity is not None:
        root_dataset, numbered = entity.root_dataset, entity.numbered
    root_bundle = bundle.rerooted(root_dataset, replacing=numbered)
    root_scope = DatasetScope(population=root_dataset)
    host_by_rerooted = {rr: hk for hk, rr in safe_pairs}
    if window_td_key is not None:
        # The bucket joins back on the consumer's own active TD.
        assert prebound.main_time_key is not None
        host_by_rerooted.setdefault(window_td_key, prebound.main_time_key)
    grain_keys = Grain.of(rr for _, rr in safe_pairs)
    # The producer metric keeps the CANONICAL alias (root fields could shadow the public name).
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=grain_keys, aggs=[agg_rooted], dataset=root_dataset, bundle=root_bundle,
        inherited=inherited, n_date_range=0, window_td_key=window_td_key,
        explicit_types=(
            {agg_rooted: declared_type} if declared_type is not None else None
        ),
        mode_policy=prebound.mode_policy, ranking_candidates=ranking_candidates,
    )
    decision = _kernel_decision(
        answer=agg_rooted, windowed=window_td_key is not None, associate=associate,
    )
    producer_plan = compile_synthesized(
        prebound=producer_prebound,
        source_dataset=root_name,
        bundle=root_bundle, scope=root_scope,
        stage_schemas=stage_schemas,
        producer_registry=producer_registry,
        population=NoInheritedPopulation(
            reason="target-rooted: re-roots and disposes the inherited filters itself",
        ),
        kernel_answer=_kernel_answer(decision),
    )
    if semi_joins:
        producer_plan = producer_plan.model_copy(
            update={"semi_join_filters": semi_joins},
        )
    # An attached parameter row-attached inside the producer: map each picked key
    # through the sub-plan's substitutions so the level-1 pick references the
    # producer's row-attach field, not the raw aggregate.
    if picked_params:
        _param_subst = {
            sub.original_key: sub.placeholder
            for a in producer_plan.regroup_attach_plans
            for sub in a.substitutions
        }
        if _param_subst:
            picked_params = [
                pp.model_copy(update={
                    "key": substitute_value_keys(key=pp.key, mapping=_param_subst),
                }) if pp.key is not None else pp
                for pp in picked_params
            ]
    producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
    answer_slot = _regroup_answer_slot_id(
        value_slots=[
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        ],
        key=agg_rooted,
        fallback=producer_answer_ids[0] if producer_answer_ids else None,
    )
    producer_grain_ids = list(producer_plan.projection)[: len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, rr in enumerate(ordered_pks):
        slot_id = next(
            (s.id for s in producer_plan.row_slots if s.key == rr), None,
        )
        if slot_id is None:
            slot_id = producer_grain_ids[i]
        join_pairs.append((host_by_rerooted[rr], slot_id))
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    cm_attach_kwargs: Dict[str, Any] = (
        {"kernel": AssociationProducerKernel(
            entity_keys=entity_keys_root, picked_params=picked_params,
        )}
        if decision is not None and decision.reducer == "plain"
        else _decided_kernel(
            decision, producer_plan=producer_plan, root_dataset=root_dataset,
            bundle=root_bundle,
            alias=canonical_aggregate_alias(agg, profile="stage_expression"),
            target_rooted=bool(target_path), ranking_candidates=producer_prebound.ranking_candidates,
        )
    )
    kernel = cm_attach_kwargs.get("kernel")
    if associate and isinstance(kernel, (RankedProducerKernel, TrailingWindowProducerKernel)):
        kernel = kernel.model_copy(update={
            "association": EntityDedup(entity_keys=entity_keys_root),
        })
        cm_attach_kwargs["kernel"] = kernel
    if isinstance(kernel, TrailingWindowProducerKernel):
        cm_attach_kwargs["kernel"] = kernel.model_copy(update={
            "endpoints": _window_endpoints(
                host_keys=[host_by_rerooted[rr] for rr in ordered_pks], source=context.endpoints_source(),
                producer_registry=producer_registry,
            ),
            "endpoint_keys": list(ordered_pks),
        })
    alias_hint = canonical_aggregate_alias(agg, profile="stage_expression")
    assert alias_hint is not None  # a cross-dataset source always has a leaf
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=alias_hint,
        attach_phase=attach_phase,
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=placeholder, producer_slot_id=answer_slot,
            original_key=agg, empty_value=_empty_value(agg, bundle=bundle),
        )],
        partition_display=[_regroup_grain_name(rr) for rr in ordered_pks],
        producer_root_dataset=root_name,
        semi_join_metric=alias,
        broadcast_metric=alias if broadcast else None,
        broadcast_dimensions=broadcast,
        **(_broadcast_replacement(agg, [g for g, _ in safe_pairs], context=context) if broadcast else {}),
        associated_metric=associated_metric,
        associated_dimensions=associated_dimensions,
        association_restricted_filter_texts=restricted_texts,
        **(_unrelated_attach_fields(agg=agg, alias=alias, dataset=target_path[0], context=context)
           if product else {}),
        **cm_attach_kwargs,
    )


def _unrelated_attach_fields(
    *, agg: AggregateKey, alias: str, dataset: str, context: _ProducerSynthesisContext,
) -> Dict[str, Any]:
    """The ``unrelated_dataset`` warning's fields: the public metrics reading ``agg``, else its alias."""
    metrics = [
        dm.public_name for dm in context.prebound.declared_metrics
        if dm.public_name and agg in walk_value_keys(dm.bound.value_key)
    ]
    return {
        "unrelated_dataset": dataset,
        "unrelated_population": context.host_dataset.spelling,
        "unrelated_metrics": metrics or [alias],
    }


class _EndpointsSource(BaseModel):
    """The host state a windowed producer's population cells are planned from."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host_dataset: SlayerDataset
    bundle: ResolvedSourceBundle
    scope: DatasetScope
    stage_schemas: Dict[str, StageSchema]
    population: Population | None
    population_filters: List[BoundFilter]
    n_population_date: int
    mode_policy: ModePolicy
    ranking_candidates: Tuple[ValueKey, ...]


def _window_endpoints(
    *, host_keys: List[ValueKey], source: _EndpointsSource,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
) -> Optional[PlannedQuery]:
    """The population's cells at a windowed producer's grain, in host coordinates, host keys' order."""
    if source.population is None:
        return None
    prebound, _ = _regroup_producer_prebound(
        pks=Grain.of(host_keys), aggs=[], dataset=source.host_dataset, bundle=source.bundle,
        inherited=source.population_filters, n_date_range=source.n_population_date,
        partition_order=lambda _pks: list(host_keys),
        mode_policy=source.mode_policy, ranking_candidates=source.ranking_candidates,
    )
    plan = compile_synthesized(
        prebound=prebound, source_dataset=source.host_dataset.name, bundle=source.bundle,
        scope=source.scope, stage_schemas=source.stage_schemas,
        producer_registry=producer_registry, population=source.population,
    )
    return plan.model_copy(update={"stage_bundle": source.bundle})


def _param_values(agg: AggregateKey) -> List[Tuple[str, ValueKey]]:
    """The reference-bearing parameters of ``agg`` (field, expression or attached
    value); literals and marker strings never lift."""
    return [(n, cast(ValueKey, v)) for n, v in agg.kwargs if isinstance(v, VALUE_KEY_TYPES)]


def _picked_params(agg: AggregateKey) -> List[PickedParam]:
    return [PickedParam(name=n, key=v) for n, v in _param_values(agg)]


def _param_is_determined(
    *, value: ValueKey, grain: Grain, host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    projected_dim_keys: Sequence[ValueKey] = (),
    projected_td_keys: Sequence[ValueKey] = (),
    active_bucket: Optional[ValueKey] = None,
) -> bool:
    """A parameter is legal iff the dataset grain determines it. An ungrained aggregate
    parameter types at the query grain, which the operand grain always refines; a
    grained transform parameter resolves to its result grain, every member of which
    must be determined."""
    if isinstance(value, AggregateKey) and value.partition_keys is None:
        return True
    members = [value]
    if isinstance(value, TransformKey):
        members = list(attached_parameter_grain(
            key=value, projected_dim_keys=list(projected_dim_keys),
            projected_td_keys=list(projected_td_keys), active_bucket=active_bucket,
        ) or ())
    return all(
        grain_determines(
            key=member, grain=grain, host_dataset=host_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
        )
        for member in members
    )


def _grain_display(grain: Grain, *, display: Callable[[ValueKey], str] = key_display) -> str:
    """A readable grain listing for a parameter-typing error."""
    names = [display(g) for g in _regroup_partition_order(grain)]
    return ", ".join(names) if names else "the grand total"


def _home_determines_grain_member(
    *, key: ValueKey, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    host_dataset: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    bundle: ResolvedSourceBundle,
) -> bool:
    """A grain member the home determines: every field it reads is attributable
    from it (as ``safe_pairs`` judges a dimension), aggregates by their grain."""
    return undetermined_witness(
        key=key, anchor_dataset=host_dataset, bundle=bundle,
        leaf_determined=lambda leaf: shared_relationship_key_reroot(
            key=leaf, target_path=target_path, host_dataset=host_dataset,
            datasets_by_name=datasets_by_name,
        ) is not None or grain_member_attributable(
            key=leaf, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle, host_dataset=host_dataset,
            host_name=host_dataset.name,
        ),
    ) is None


def _check_param_partition_keys(*, agg: AggregateKey, alias: str, context: "_ProducerSynthesisContext") -> None:
    """Outside association a parameter's own unattributable explicit key is refused by its producer's gate,
    ahead of the enclosing determination rule."""
    if context.prebound.mode_policy.associates:
        return
    host, bundle = context.host_dataset, context.bundle
    for _name, value in _param_values(agg):
        if not isinstance(value, AggregateKey) or value.partition_keys is None or value not in context.homes.by_key:
            continue
        target_path = context.home_of(value)
        home = walk_key_path(dataset=host, path=target_path, bundle=bundle)
        if home is None or crosses_product_hop(host=host, path=target_path, datasets_by_name=bundle.datasets_by_name):
            continue
        home = bundle.factor_free(home)
        check_partition_keys_attributable(
            alias=alias, root_name=home.name, explicit=True,
            unattributable=_unattributable_param_keys(
                value, partition_keys=value.partition_keys, target_path=target_path, home=home, context=context,
            ),
        )


def _unattributable_param_keys(
    value: AggregateKey, *, partition_keys: Grain, target_path: Tuple[str, ...], home: SlayerDataset,
    context: "_ProducerSynthesisContext",
) -> List[Tuple[str, str]]:
    """``(display, reason)`` for each of ``value``'s explicit partition keys its home cannot attribute."""
    host, bundle = context.host_dataset, context.bundle
    with_host = {**bundle.datasets_by_name, host.name: host}
    anchors = context.prebound.reference_anchors
    routing = _home_routing(
        list(partition_keys), anchors=anchors, host_dataset=host, target_path=target_path,
        root_dataset=home, datasets_by_name=with_host,
        source_anchor=_source_anchor(value, anchors=anchors, host_dataset=host, datasets_by_name=with_host),
    )
    unattributable = []
    for pk in _regroup_partition_order(partition_keys):
        pk_home = substitute_value_keys(pk, routing) if routing else pk
        if shared_relationship_key_reroot(
            key=pk, target_path=target_path, host_dataset=host, datasets_by_name=bundle.datasets_by_name,
        ) is None and not grain_member_attributable(
            key=pk_home, target_path=target_path, root_dataset=home, datasets_by_name=bundle.datasets_by_name,
            bundle=bundle, host_dataset=host, host_name=host.name,
        ):
            unattributable.append((context.dim_display(pk), key_broadcast_reason(
                key=pk_home, target_path=target_path, root_dataset=home, datasets_by_name=with_host,
                bundle=bundle, host_dataset=host, host_name=host.name,
            )))
    return unattributable


def _check_attached_params_determined(
    *, agg: AggregateKey, alias: str, target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    context: "_ProducerSynthesisContext",
) -> None:
    """One home-determination rule, every mode: the home determines every grain
    member of each attached parameter typed at its result grain (Axiom 2.3 / 11.4);
    an ungrained aggregate types at the query grain and is determined by
    construction. A grained transform resolves to its
    result grain first, so this predicate needs no transform arm."""
    _check_param_partition_keys(agg=agg, alias=alias, context=context)
    host_dataset, datasets_by_name, bundle = context.host_dataset, context.datasets_by_name, context.bundle
    projected_dim_keys, projected_td_keys = context.projected_dim_keys, context.projected_td_keys
    active_bucket = context.prebound.main_time_key
    primary_key = _primary_key_fields(root_dataset)
    grain_display = _grain_display(Grain.of(
        field_default_key(path=target_path, leaf=col, base=root_dataset)
        for col in primary_key
    )) if primary_key else f"{root_dataset.name} rows"
    for name, value in agg.kwargs:
        if not isinstance(value, (AggregateKey, TransformKey)):
            continue
        grain = attached_parameter_grain(
            key=value, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        if grain is None:  # ungrained aggregate — determined by construction
            continue
        check_parameter_determined(
            alias=alias, param_name=name, grain_display=grain_display,
            determined=all(
                _home_determines_grain_member(
                    key=member, target_path=target_path, root_dataset=root_dataset,
                    host_dataset=host_dataset, datasets_by_name=datasets_by_name,
                    bundle=bundle,
                )
                for member in grain
            ),
        )


def _check_inline_params_determined(
    *, dispositions: Sequence[RootDisposition], context: "_ProducerSynthesisContext",
) -> None:
    """The home-determination rule on the plain path: an inline root's home is the host."""
    host = context.bundle.factor_free(context.host_dataset)
    for d in dispositions:
        if d.routing != "inline" or not isinstance(d.root, AggregateKey):
            continue
        _check_attached_params_determined(
            agg=d.root, target_path=(), root_dataset=host, context=context,
            alias=(d.consumer_public_names[0] if d.consumer_public_names
                   else canonical_aggregate_alias(d.root, profile="stage_expression") or d.root.agg),
        )


def _primary_key_fields(dataset: SlayerDataset) -> List[str]:
    return [field.name for field in dataset.fields if field.primary_key]


class _AssociationEntity(NamedTuple):
    """The entities an association producer deduplicates: the home (carrying the row identity or list position
    its keys read), each unattributable dimension rerooted into it, the keys telling its rows apart in its
    coordinates, and the parent numbered for them."""

    root_dataset: SlayerDataset
    assoc_pairs: List[Tuple[ValueKey, ValueKey]]
    keys: List[ValueKey]
    numbered: List[SlayerDataset]

    def host_grain(self, target_path: Tuple[str, ...]) -> Grain:
        return Grain.of(_host_coordinates(k, target_path=target_path, root=self.root_dataset) for k in self.keys)

    def determines_param(
        self, *, value: ValueKey, target_path: Tuple[str, ...], host_dataset: SlayerDataset,
        datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
    ) -> bool:
        # For a local home the initial grain seed reads the host dataset directly.
        return _param_is_determined(
            value=value, grain=self.host_grain(target_path),
            host_dataset=host_dataset if target_path else self.root_dataset,
            datasets_by_name={
                **datasets_by_name, **{d.name: d for d in self.numbered}, self.root_dataset.name: self.root_dataset,
            },
            bundle=bundle,
        )


def _host_coordinates(key: ValueKey, *, target_path: Tuple[str, ...], root: SlayerDataset) -> ValueKey:
    """Root-coordinates ``key`` read from the host: a list child's back hop is the hop that reached it."""
    path = tuple(cast(FieldKey | FieldSqlKey, key).path)
    if root.list_of is not None and path[:1] == (root.list_of[1],):
        return key.model_copy(update={"path": (*target_path[:-1], *path[1:])})
    return key.model_copy(update={"path": (*target_path, *path)})


def _identity_keys(root: SlayerDataset, *, bundle: ResolvedSourceBundle) -> List[ValueKey]:
    """What tells ``root``'s rows apart, in its coordinates: its primary key, else its per-query row identity; a
    list child's, its parent row's (over the back hop) and the element's position (design D2/D3)."""
    if root.list_of is None:
        owner, path, position = root, (), []
    else:
        owner, path = bundle.datasets_by_name[root.list_of[0]], (root.list_of[1],)
        position = [FieldKey(leaf=LIST_POSITION)]
    # A standalone UNIQUE constraint can allow NULLs, which cannot identify individual rows.
    keys = [field_default_key(path=path, leaf=col, base=owner) for col in _primary_key_fields(owner)]
    return [*(keys or [FieldKey(path=path, leaf=ROW_IDENTITY_FIELD)]), *position]


def _association_entity(
    *, root_dataset: SlayerDataset, target_path: Tuple[str, ...],
    unattributable: List[_UnattributableDim], host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> _AssociationEntity:
    assoc_pairs = [
        (u.key, reroot_from_root(
            key=u.routed, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        ))
        for u in unattributable
    ]
    keys = _identity_keys(root_dataset, bundle=bundle)
    identified, numbered = identified_datasets(
        root=root_dataset, entity_keys=keys, datasets_by_name=bundle.datasets_by_name,
    )
    return _AssociationEntity(root_dataset=identified, assoc_pairs=assoc_pairs, keys=keys, numbered=numbered)


def _association_arm(
    *, agg: AggregateKey, agg_rooted: AggregateKey, alias: str,
    entity: _AssociationEntity, target_path: Tuple[str, ...], host_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle,
) -> Tuple[List[PickedParam], List[ValueKey]]:
    """The home-rooted association arm: mode-invariant
    input safety on the rerooted host-locus aggregate (compiled inline at its fanning
    grain, its level-1 dedup removing the fan-out); the kernel entity keys in ROOT
    coordinates and the parameters the entity grain picks, rerooted into the home."""
    root_dataset = entity.root_dataset
    _assert_cross_dataset_inputs_safe(
        agg=agg, agg_rooted=agg_rooted, root_dataset=root_dataset,
        root_name=root_dataset.name, target_path=target_path, bundle=bundle,
        datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        host_dataset=host_dataset,
    )
    # Type each field-valued / default parameter against the ENTITY grain in HOST
    # coordinates (the aggregation reads it once per associated entity); an attached
    # one is judged by the shared home-determination rule. Lift every parameter,
    # rerooted into the home so the level-1 pick reads it there; the kernel dedups
    # by the entity key in ROOT coordinates.
    entity_grain = entity.host_grain(target_path)
    entity_keys_root = list(entity.keys)
    picked_params: List[PickedParam] = []
    for name, value in _param_values(agg):
        # Attached kinds (aggregate- and transform-valued) are judged by the shared
        # home-determination tail (D5); only row-valued params are typed here.
        if not isinstance(value, (AggregateKey, TransformKey)):
            check_parameter_determined(
                alias=alias, param_name=name,
                grain_display=_grain_display(entity_grain),
                determined=entity.determines_param(
                    value=value, target_path=target_path, host_dataset=host_dataset,
                    datasets_by_name=datasets_by_name, bundle=bundle,
                ),
            )
        picked_params.append(PickedParam(name=name, key=reroot_from_root(
            key=value, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_dataset.name,
        )))
    return picked_params, entity_keys_root


def _association_inline_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_dataset: SlayerDataset,
    datasets_by_name: Dict[str, SlayerDataset], host_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
) -> Tuple[List[BoundFilter], List[str]]:
    """Route each ROW conjunct for a home-rooted association producer:
    attributable → inline re-rooted; else inline the re-rooted conjunct too
    (the per-entity dedup makes the fanning join harmless) with its text kept for
    the informational entry. No semi-join is emitted, so membership equals the
    semi-join semantics and a conjunct sharing a hop with an association
    dimension binds to the same related row."""
    inherited: List[BoundFilter] = []
    restricted_texts: List[str] = []
    for cj, text in _producer_row_conjuncts(
        base_filters=base_filters, target_path=target_path, root_dataset=root_dataset,
        datasets_by_name=datasets_by_name, host_dataset=host_dataset, bundle=bundle,
    ):
        inh, pushed = _conjunct_disposition(
            cj=cj, text=text, target_path=target_path, root_dataset=root_dataset,
            datasets_by_name=datasets_by_name, host_name=host_dataset.name,
            host_dataset=host_dataset, bundle=bundle,
        )
        if inh is not None:
            inherited.append(inh)
        else:
            assert pushed is not None
            inherited.append(bound_filter_from_key(reroot_from_root(
                key=cj, target_path=target_path, root_dataset=root_dataset,
                datasets_by_name=datasets_by_name, host_name=host_dataset.name,
            )))
            if pushed[1] is not None:
                restricted_texts.append(pushed[1])
    return inherited, restricted_texts


def _substitute_prebound(
    prebound: PreboundQuery, mapping: Mapping[ValueKey, ValueKey],
    *, law: Callable[[ConsumerPosition], Callable[..., ValueKey]] = lambda _p: substitute_value_keys,
    filter_positions: Optional[Sequence[ConsumerPosition]] = None,
) -> PreboundQuery:
    """Substitute value keys across a prebound's metrics / filters / orders; ``law``
    picks the traversal per consumer position (deep by default)."""
    dimension: ConsumerPosition = "dimension"
    metric: ConsumerPosition = "metric"
    positions: List[ConsumerPosition] = (
        list(filter_positions) if filter_positions is not None
        else ["field_filter" for _ in prebound.bound_filters]
    )
    return prebound.model_copy(update={
        "declared_metrics": [
            dm.model_copy(update={"bound": BoundExpr(
                value_key=law(dimension if dm.is_dimension else metric)(
                    key=dm.bound.value_key, mapping=mapping,
                ),
            )})
            for dm in prebound.declared_metrics
        ],
        "bound_filters": [
            substitute_in_bound_filter(bf, mapping, substitute=law(p))
            for bf, p in zip(prebound.bound_filters, positions, strict=True)
        ],
        "order_specs": [
            sp.model_copy(update={"bound": BoundExpr(
                value_key=law("order")(key=sp.bound.value_key, mapping=mapping),
            )})
            for sp in prebound.order_specs
        ],
    })


def _filter_position(ct: ConjunctTyping) -> ConsumerPosition:
    return "metric_filter" if ct.typing == MaskTyping.METRIC else "field_filter"


def _substitute_reaggregations(
    prebound: PreboundQuery, mapping: Mapping[ValueKey, ValueKey],
    *, filter_typings: Sequence[ConjunctTyping],
) -> PreboundQuery:
    """Substitute re-aggregation roots where discovery finds them — its per-position walk."""
    classes = position_classes(
        prebound.declared_metrics, n_grain=prebound.n_dims + prebound.n_time_dimensions,
    )
    return _substitute_prebound(
        prebound, mapping,
        law=lambda p: functools.partial(substitute_consumer_keys, opaque=classes.opaque(p)),
        filter_positions=[_filter_position(ct) for ct in filter_typings],
    )


def _constituent_alias(c: ValueKey) -> str:
    """A clean stage alias for a re-aggregation constituent: an aggregate's
    canonical alias (else its ``.agg``), a transform's ``.op`` (a transform has no
    canonical aggregate alias of its own)."""
    if isinstance(c, TransformKey):
        return c.op
    if isinstance(c, AggregateKey):
        return canonical_aggregate_alias(c, profile="stage_expression") or c.agg
    return "reagg"


def _synthesize_reaggregation_producer(  # NOSONAR(S3776) — one cohesive second-order synthesis (constituents → union grain → attributability/mode → carrier producer → outer producer → attach); the arms share the re-rooting state.
    *,
    root: AggregateKey,
    placeholder: ValueKey,
    attach_phase: Literal["row", "combined"],
    public_alias: Optional[str],
    context: _ProducerSynthesisContext,
    declared_type: Optional[DataType],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    registry: RegroupPlaceholderRegistry,
    inherited: List[BoundFilter],
    n_date_range: int,
    population: "Population",
    population_semi_join_metrics: Optional[List[str]] = None,
) -> RegroupAttachPlan:
    """Compile a re-aggregation as producer-over-producer: a carrier
    at the operand's union grain (the inner producers) and an outer aggregate
    grouping it by the attributable outer grain, reusing the association kernel."""
    prebound, bundle = context.prebound, context.bundle
    host_dataset, datasets_by_name = context.host_dataset, context.datasets_by_name
    scope, stage_schemas = context.scope, context.stage_schemas
    proj = [*context.projected_dim_keys, *context.projected_td_keys]

    # Constituents (aggregates and grained transforms) and their grains; a
    # constituent with no declared partition is typed at the query's dimensions,
    # a transform at the union of its inner aggregates' grains (Axiom 2.3). The
    # union grain is the carrier grain.
    constituents = operand_constituents(root.source)
    union_grain = Grain.EMPTY
    for c in constituents:
        union_grain = union_grain | constituent_grain(
            c=c, projected_dim_keys=context.projected_dim_keys,
            projected_td_keys=context.projected_td_keys,
            active_bucket=prebound.main_time_key,
        )

    # A clean, stable name for the re-aggregation (the nested source has no
    # canonical alias of its own; a transform constituent has no ``.agg``).
    inner_alias = _constituent_alias(constituents[0]) if constituents else "reagg"
    alias = (
        public_alias
        or canonical_aggregate_alias(root, profile="stage_expression")
        or f"{root.agg}_{inner_alias}"
    )

    # Checked before TD resolution — name the combination, not a misleading TD error.
    check_reaggregation_no_window(alias=alias, window_val=window_kwarg_of(root))
    # Requested outer grain: explicit partition_by= else the query dimensions.
    requested = list(root.partition_keys if root.partition_keys is not None else proj)

    # Type each outer parameter against the operand grain; a legal aggregate-valued
    # parameter rides the carrier as an extra constituent, a legal field parameter
    # is picked once per cell. An UNGRAINED aggregate parameter types at the OUTER
    # grain (decision 12) — normalise it there so the carrier broadcasts it per
    # outer cell; the original key stays the outer pipeline identity, aliased to the
    # normalised constituent's placeholder below.
    reagg_params = _param_values(root)
    param_constituent_of: Dict[ValueKey, ValueKey] = {}
    for _name, _value in reagg_params:
        check_parameter_determined(
            alias=alias, param_name=_name,
            grain_display=_grain_display(union_grain, display=context.dim_display),
            determined=_param_is_determined(
                value=_value, grain=union_grain, host_dataset=host_dataset,
                datasets_by_name=datasets_by_name, bundle=bundle,
                projected_dim_keys=context.projected_dim_keys,
                projected_td_keys=context.projected_td_keys,
                active_bucket=prebound.main_time_key,
            ),
        )
        if isinstance(_value, AggregateKey):
            ck = _value
            if ck.partition_keys is None:
                ck = ck.model_copy(update={"partition_keys": Grain.of(requested)})
            if ck not in constituents:
                constituents.append(ck)
            if ck != _value:
                param_constituent_of[_value] = ck
        elif isinstance(_value, TransformKey) and _value not in constituents:
            # A transform parameter rides the carrier as a constituent at its own
            # (explicit, post-D2) result grain — no ungrained normalisation (D4).
            constituents.append(_value)

    # Attributable iff the operand grain determines it (Axiom 7); the rest resolve
    # per the mode policy, exactly as for dataset-rooted aggregates.
    policy = prebound.mode_policy
    attributable: List[ValueKey] = []
    witness_of: Dict[ValueKey, ValueKey] = {}
    for g in requested:
        witness = grain_witness(
            key=g, grain=union_grain, host_dataset=host_dataset,
            datasets_by_name=datasets_by_name, bundle=bundle,
        )
        if witness is None:
            attributable.append(g)
        else:
            witness_of[g] = witness
    unattributable = list(witness_of)

    # An explicit outer key is judged here only — never against the query root.
    if root.partition_keys is not None:
        check_reaggregation_outer_keys_determined(
            alias=alias, policy=policy,
            undetermined_keys=[context.dim_display(g) for g in unattributable],
            grain_display=_grain_display(union_grain, display=context.dim_display),
        )
    associate_dims: List[ValueKey] = []
    broadcast_dims: List[Tuple[str, str]] = []
    if unattributable:
        check_reaggregation_dims_attributable(
            alias=alias, policy=policy,
            unattributable_names=[context.dim_display(g) for g in unattributable],
        )
        if policy.associates:
            associate_dims = unattributable
        else:
            broadcast_dims = [
                (context.dim_display(g), determination_broadcast_reason(
                    witness=witness_of[g], host_dataset=host_dataset,
                    datasets_by_name=datasets_by_name, bundle=bundle,
                ))
                for g in unattributable
            ]

    outer_grain = [*attributable, *associate_dims]
    # Degenerate: operand grain equals the outer grain — the identity, warned.
    degenerate = not broadcast_dims and Grain.of(union_grain) == Grain.of(outer_grain)

    # The carrier: one producer at the union grain carrying every constituent
    # (coarser constituents broadcast within it), row-attached to the population.
    constituent_placeholders: Dict[ValueKey, ValueKey] = {
        c: registry.placeholder_for(c) for c in constituents
    }
    # An ungrained parameter normalised to a distinct carrier constituent: the
    # outer aggregate still references the original key, so alias it to the same
    # placeholder (decision 12).
    for orig, ck in param_constituent_of.items():
        constituent_placeholders[orig] = constituent_placeholders[ck]
    # The public metric names this re-aggregation reports under (D6): a standalone
    # root its own alias; a mixed constituent the row-attach roots consuming it.
    if population_semi_join_metrics is not None:
        semi_join_names = list(population_semi_join_metrics)
    elif public_alias:
        semi_join_names = [public_alias]
    else:
        semi_join_names = []
    carrier_attach = _build_carrier_attach(
        union_grain=union_grain, constituents=constituents,
        constituent_placeholders=constituent_placeholders, host_dataset=host_dataset,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        inherited=inherited, n_date_range=n_date_range,
        producer_source_dataset=host_dataset.name, producer_registry=producer_registry,
        active_bucket=prebound.main_time_key,
        population=population,
        population_semi_join_metrics=semi_join_names,
        mode_policy=policy, ranking_candidates=prebound.ranking_candidates,
    )

    # The outer producer: OUTER_AGG over the constituent composite (placeholders),
    # grouped by the outer grain. Its body renders via the association kernel with
    # the carrier as the per-cell value; level 2 aggregates over the cells. An
    # attach-carrying grain key becomes an expression over carrier placeholders.
    # Substitute the WHOLE root (source AND params) so an aggregate-valued
    # parameter references its carrier placeholder like the source constituents do.
    original_by_pk: Dict[ValueKey, ValueKey] = {}
    for g in outer_grain:
        sub = substitute_value_keys(g, constituent_placeholders)
        original_by_pk[sub] = g
    # The settled outer grain rides as the explicit grain, so the outer compile
    # never re-judges it from the host (Axiom 9).
    outer_agg = substitute_value_keys(root, constituent_placeholders).model_copy(
        update={"partition_keys": Grain.of(original_by_pk)},
    )
    # Each legal parameter is picked once per cell: an aggregate-valued
    # one from its carrier placeholder, a field/expression one from the operand
    # scope; level 2 reads it as ``_base._p<i>``.
    reagg_picked_params = [
        PickedParam(
            name=_name,
            key=(constituent_placeholders[_value]
                 if isinstance(_value, (AggregateKey, TransformKey)) else _value),
        )
        for _name, _value in reagg_params
    ]
    outer_prebound, ordered_outer = _regroup_producer_prebound(
        pks=Grain.of(original_by_pk), aggs=[outer_agg], dataset=host_dataset, bundle=bundle,
        # Row filters define the population whose cells the outer aggregate
        # consumes — without them a NULL-masking composite (coalesce) would
        # fabricate cells for filtered-out entities.
        inherited=inherited, n_date_range=n_date_range,
        # A clean producer field name (the placeholder-sourced key would leak the
        # reserved __regroup__ prefix into the emitted alias).
        public_alias_by_agg={outer_agg: alias},
        explicit_types={outer_agg: declared_type} if declared_type is not None else None,
        mode_policy=policy, ranking_candidates=prebound.ranking_candidates,
    )
    outer_plan = compile_synthesized(
        prebound=outer_prebound,
        source_dataset=host_dataset.name,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        producer_registry=producer_registry,
        population=population,
        placeholder_originals={ph: c for c, ph in constituent_placeholders.items()},
    )
    # Keep any internal attach the outer plan desugared for an expression
    # grain key; the carrier rides alongside. Entity keys must render inside
    # the outer body, so they take the same internal desugar.
    internal_map: Dict[ValueKey, ValueKey] = {
        sub.original_key: sub.placeholder
        for a in outer_plan.regroup_attach_plans
        for sub in a.substitutions
    }
    entity_keys = [
        substitute_value_keys(k, internal_map)
        for k in _regroup_partition_order(union_grain)
    ]
    if internal_map:
        carrier_attach = carrier_attach.model_copy(update={"join_pairs": [
            (substitute_value_keys(hk, internal_map), sid)
            for hk, sid in carrier_attach.join_pairs
        ]})
    outer_plan = outer_plan.model_copy(update={
        "regroup_attach_plans": [*outer_plan.regroup_attach_plans, carrier_attach],
    })

    answer_slot = _regroup_answer_slot_id(
        value_slots=[*outer_plan.aggregate_slots, *outer_plan.combined_expression_slots],
        key=substitute_value_keys(outer_agg, internal_map), fallback=None,
    )
    grain_ids = list(outer_plan.projection)[: len(ordered_outer)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, g in enumerate(ordered_outer):
        slot_id = next(
            (s.id for s in outer_plan.row_slots if s.key == g), None,
        )
        # Join back on the ORIGINAL dimension key (the query slot's identity).
        join_pairs.append((
            original_by_pk.get(g, g),
            slot_id if slot_id is not None else grain_ids[i],
        ))
    _assert_attach_covers_producer_grain(
        joined_slot_ids={sid for _, sid in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(outer_plan),
    )
    degenerate_display = (
        [context.dim_display(g) for g in union_grain] if degenerate else []
    )
    return RegroupAttachPlan(
        producer_plan=outer_plan,
        alias_hint=alias,
        attach_phase=attach_phase,
        kernel=AssociationProducerKernel(
            entity_keys=entity_keys, null_safe=True,
            picked_params=reagg_picked_params,
        ),
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=placeholder, producer_slot_id=answer_slot,
            original_key=root, empty_value=_empty_value(root, bundle=bundle),
        )],
        partition_display=[
            _regroup_grain_name(original_by_pk.get(g, g)) for g in ordered_outer
        ],
        producer_root_dataset=host_dataset.name,
        broadcast_metric=alias if broadcast_dims else None,
        broadcast_dimensions=broadcast_dims,
        **(_broadcast_replacement(root, attributable, context=context) if broadcast_dims else {}),
        associated_metric=alias if associate_dims else None,
        associated_dimensions=[context.dim_display(g) for g in associate_dims],
        degenerate_metric=alias if degenerate else None,
        degenerate_operand_grain=degenerate_display,
        degenerate_outer_grain=(
            [context.dim_display(g) for g in outer_grain] if degenerate else []
        ),
        population_semi_join_metrics=(
            semi_join_names if outer_plan.semi_join_filters else []
        ),
    )


def _build_carrier_attach(
    *,  # NOSONAR(S107) — carrier plumbing: each parameter is a distinct compile input threaded from _plan_regroups; a one-use context type would only relocate them
    union_grain: Grain,
    constituents: List[ValueKey],
    constituent_placeholders: Dict[ValueKey, ValueKey],
    host_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle,
    scope: DatasetScope,
    stage_schemas: Dict[str, StageSchema],
    inherited: List[BoundFilter],
    n_date_range: int,
    producer_source_dataset: Optional[str],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    active_bucket: Optional[TimeTruncKey],
    population: "Population",
    mode_policy: ModePolicy,
    ranking_candidates: Sequence[ValueKey],
    population_semi_join_metrics: Optional[List[str]] = None,
) -> RegroupAttachPlan:
    """A row-attach producer at the union grain carrying every constituent (coarser
    ones broadcast within it) — the carrier / level-1 of the re-aggregation."""
    # A windowed inner carries the query's active bucket into its own grain
    # (Axiom 2.3); thread it as the carrier's main time dimension so the nested
    # producer for the windowed inner resolves it. The bucket is
    # already a union-grain key, so this only sets main_time_key.
    carrier_windowed = active_bucket is not None and any(
        window_kwarg_of(k) is not None
        for c in constituents for k in walk_value_keys(c)
    )
    carrier_prebound, ordered_pks = _regroup_producer_prebound(
        pks=union_grain, aggs=constituents, dataset=host_dataset, bundle=bundle,
        inherited=inherited, n_date_range=n_date_range,
        window_td_key=active_bucket if carrier_windowed else None,
        mode_policy=mode_policy, ranking_candidates=ranking_candidates,
    )
    carrier_plan = compile_synthesized(
        prebound=carrier_prebound,
        source_dataset=producer_source_dataset,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        producer_registry=producer_registry,
        population=population,
    )
    value_slots = [
        *carrier_plan.aggregate_slots, *carrier_plan.combined_expression_slots,
    ]
    answer_ids = list(carrier_plan.projection)[len(ordered_pks):]
    substitutions = [
        RegroupSubstitution(
            placeholder=constituent_placeholders[c],
            producer_slot_id=_regroup_answer_slot_id(
                value_slots=value_slots, key=c,
                fallback=answer_ids[i] if i < len(answer_ids) else None,
            ),
            original_key=c,
            empty_value=_empty_value(c, bundle=bundle),
        )
        for i, c in enumerate(constituents)
    ]
    grain_ids = list(carrier_plan.projection)[: len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, pk in enumerate(ordered_pks):
        slot_id = next((s.id for s in carrier_plan.row_slots if s.key == pk), None)
        join_pairs.append((pk, slot_id if slot_id is not None else grain_ids[i]))
    # A coarser constituent nests as a ROW attach whose placeholder lands in the
    # carrier's row slots; it is an answer field (grain-determined), not a grain key.
    answer_slot_ids = {sub.producer_slot_id for sub in substitutions}
    _assert_attach_covers_producer_grain(
        joined_slot_ids={sid for _, sid in join_pairs},
        producer_grain_slot_ids=(
            _producer_grain_slot_ids(carrier_plan) - answer_slot_ids
        ),
    )
    return RegroupAttachPlan(
        producer_plan=carrier_plan,
        alias_hint=(_constituent_alias(constituents[0]) if constituents else "carrier"),
        attach_phase="row",
        join_pairs=join_pairs,
        substitutions=substitutions,
        partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
        # Host-rooted like a local row attach (root None), so a structurally
        # identical standalone producer interns to one CTE.
        producer_root_dataset=None,
        # The carrier reads the base rows, so a population semi-join inherited here
        # reports under the enclosing metric names (D6).
        population_semi_join_metrics=(
            list(population_semi_join_metrics)
            if population_semi_join_metrics and carrier_plan.semi_join_filters
            else []
        ),
    )


def _assert_total_routing(prebound: PreboundQuery, *, homes: AggregateHomes) -> None:
    """Post-discovery total-routing invariant: every cross-dataset / partitioned aggregate leaf must be disposed; a survivor is an unrouted shape and raises."""
    roles: Tuple[Tuple[str, List[ValueKey]], ...] = (
        ("metric", [dm.bound.value_key for dm in prebound.declared_metrics]),
        ("filter", [bf.value_key for bf in prebound.bound_filters]),
        ("order", [sp.bound.value_key for sp in prebound.order_specs]),
    )
    for role, keys in roles:
        for vk in keys:
            for k in walk_value_keys(vk):
                if homes.target_rooted(k) or (
                    isinstance(k, AggregateKey) and k.partition_keys is not None
                ):
                    raise ValueError(
                        f"Aggregate {_canonical_name(k)!r} in a {role} received "
                        f"no routing disposition (inline, producer substitution, "
                        f"or explicit rejection) — the planner cannot compile "
                        f"this shape."
                    )


def _aggregate_terms_under_root(
    *, root: ValueKey, env: ElaboratedQuery, combined: set,
) -> Tuple[Dict[ValueKey, Aggregate], bool]:
    """(aggregate terms under ``root``, whether any key has a combined attach)."""
    terms: Dict[ValueKey, Aggregate] = {}
    has_combined = False
    for k in walk_value_keys(root):
        has_combined = has_combined or k in combined
        if k not in terms:
            term = env.terms.get(k)
            if isinstance(term, Aggregate):
                terms[k] = term
    return terms, has_combined


def _assert_broadcast_coherence(
    *, env: ElaboratedQuery, metric_roots: List[ValueKey],
    attach_plans: List[RegroupAttachPlan],
) -> None:
    """D5 coherence: a metric the compiler attaches at differing grains must carry the environment's Broadcast insertions for every non-union-grain term."""
    combined = {
        s.original_key
        for plan in attach_plans if plan.attach_phase == "combined"
        for s in plan.substitutions
    }
    if not combined:
        return
    for root, entry in zip(metric_roots, env.metrics):
        # Every aggregate term under the root (inline ones included): the
        # combine mixes grains iff their grains differ, and only a root with a
        # combined attach is a compiler-witnessed combine.
        terms, has_combined = _aggregate_terms_under_root(
            root=root, env=env, combined=combined,
        )
        if not has_combined or len({t.grain for t in terms.values()}) < 2:
            continue
        recorded = {b.source.recipe for b in entry.broadcasts}
        missing = [
            k for k, t in terms.items()
            if t.grain != entry.grain and k not in recorded
        ]
        assert not missing, (
            f"broadcast-coherence (D5): grain-differing combine lacks "
            f"Broadcast insertions for {sorted(str(k) for k in missing)}"
        )


def _intern_producer(
    attach: RegroupAttachPlan,
    registry: Optional[Dict[Hashable, PlannedQuery]],
) -> RegroupAttachPlan:
    if registry is None:
        return attach
    ident = regroup_producer_identity(attach)
    shared = registry.get(ident)
    if shared is None:
        registry[ident] = attach.producer_plan
        return attach
    if shared is attach.producer_plan:
        return attach
    return attach.model_copy(update={"producer_plan": shared})


class _RoutedRoots(BaseModel):
    """The discovered roots grouped by (phase, routing), in placeholder-mint order."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    row_local: List[ValueKey] = Field(default_factory=list)
    row_target: List[ValueKey] = Field(default_factory=list)
    combined_local: List[ValueKey] = Field(default_factory=list)
    combined_target: List[ValueKey] = Field(default_factory=list)
    #: Row-phase re-aggregations: a dimension's own, or a row-attach root's constituent.
    row_reagg: List[AggregateKey] = Field(default_factory=list)
    combined_reagg: List[AggregateKey] = Field(default_factory=list)
    inline_inputs: List[ValueKey] = Field(default_factory=list)
    public_alias: Dict[ValueKey, str] = Field(default_factory=dict)
    declared_type: Dict[ValueKey, DataType] = Field(default_factory=dict)
    constituent_consumers: Dict[ValueKey, List[str]] = Field(default_factory=dict)

    def add_constituent(self, root: AggregateKey, names: Sequence[str]) -> None:
        if root not in self.row_reagg:
            self.row_reagg.append(root)
        consumers = self.constituent_consumers.setdefault(root, [])
        consumers.extend(n for n in names if n not in consumers)

    def add_reaggregation(self, d: RootDisposition) -> None:
        """One attach per (root, phase); name and type are the root's, phase-free."""
        assert isinstance(d.root, AggregateKey)
        bucket = self.row_reagg if d.phase == "row" else self.combined_reagg
        if d.root not in bucket:
            bucket.append(d.root)
        if d.consumer_public_names:
            self.public_alias.setdefault(d.root, d.consumer_public_names[0])
        if d.declared_type is not None:
            self.declared_type.setdefault(d.root, d.declared_type)


def _combined_rank(k: ValueKey, *, target_rooted: bool) -> int:
    """Combined sub-order: partitioned before bare; cross-dataset before local."""
    partitioned = getattr(k, "partition_keys", None) is not None
    if not target_rooted:
        return 2 if partitioned else 3
    return 0 if partitioned else 1


def _group_routed_roots(  # NOSONAR(S3776) — one grouping pass over the dispositions; the bucket / nesting / inline-demotion arms share the bucket state.
    dispositions: Sequence[RootDisposition], *,
    reagg_mapping: Mapping[ValueKey, ValueKey],
    nests: Callable[[ValueKey, str], bool],
    homes: AggregateHomes,
) -> _RoutedRoots:
    """Group the dispositions per (phase, routing), keep only the roots ``nests``
    admits (an inline root's inputs and a re-aggregation always attach), and demote
    a row-attach root whose every producer disposition was dropped to inline,
    attaching its inputs."""
    out = _RoutedRoots()

    def _mapped(k: ValueKey, phase: str) -> ValueKey:
        if not reagg_mapping:
            return k
        opaque = is_dimension_transform_root if phase == "row" else None
        return substitute_consumer_keys(k, reagg_mapping, opaque=opaque)

    def _add(bucket: List[Any], k: ValueKey) -> None:
        if k not in bucket:
            bucket.append(k)

    inline_roots = [d.root for d in dispositions if d.routing == "inline"]
    inputs = {a for r in inline_roots for a in attached_inputs(r)}
    names_of: Dict[ValueKey, Tuple[str, ...]] = {}
    for d in dispositions:
        names_of.setdefault(d.root, d.consumer_public_names)
    combined: List[Tuple[ValueKey, RootDisposition]] = []
    for d in dispositions:
        if d.routing in ("shifted", "inline"):
            continue
        if d.routing == "reaggregation":
            out.add_reaggregation(d)
            continue
        if d.routing == "reaggregation_constituent" and isinstance(d.root, AggregateKey):
            out.add_constituent(d.root, d.consumer_public_names)
            continue
        is_input = d.phase == "row" and d.root in inputs
        k = d.root if is_input else _mapped(d.root, d.phase)
        if d.phase == "row":
            if is_input or nests(k, "row"):
                _add(out.row_target if d.routing == "target_rooted" else out.row_local, k)
            continue
        if d.consumer_public_names:
            out.public_alias.setdefault(k, d.consumer_public_names[0])
        if d.declared_type is not None:
            out.declared_type.setdefault(k, d.declared_type)
        combined.append((k, d))
    for k, d in sorted(combined, key=lambda kd: _combined_rank(kd[0], target_rooted=kd[1].routing == "target_rooted")):
        if d.routing == "target_rooted":
            _add(out.combined_target, k)
        elif nests(k, "combined"):
            _add(out.combined_local, k)
    producer_bound = {*out.row_local, *out.row_target, *out.combined_local, *out.combined_target}
    demoted = [
        d.root for d in dispositions
        if d.routing in ("local_producer", "target_rooted") and is_row_attach_root(d.root)
        and _mapped(d.root, d.phase) not in producer_bound and d.root not in inline_roots
    ]
    for root in dict.fromkeys([*inline_roots, *demoted]):
        for inp in (row_attach_inputs(root, names=names_of.get(root, ()), homes=homes)
                    if root in demoted else []):
            if inp.routing == "reaggregation_constituent" and isinstance(inp.root, AggregateKey):
                out.add_constituent(inp.root, inp.consumer_public_names)
            else:
                _add(out.row_target if inp.routing == "target_rooted" else out.row_local,
                     inp.root)
        for a in attached_inputs(root):
            _add(out.inline_inputs, a)
    return out


def _local_regroup_groups(
    roots: Sequence[ValueKey], *, projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey], active_bucket: Optional[ValueKey],
    crossing: Callable[[ValueKey], bool],
) -> List[Tuple[Grain, bool, List[ValueKey]]]:
    """``(grain, windowed, roots)`` per producer: one per grain and row restriction and, for a
    windowed / ranked / crossing-input root, per partition-free identity."""
    groups: Dict[Tuple, List[ValueKey]] = {}
    meta: Dict[Tuple, Tuple[Grain, bool]] = {}
    for agg in roots:
        grain, windowed = effective_root_grain(
            agg=agg, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        ident = _windowed_or_ranked_identity(agg)
        # A crossing-input root needs its OWN producer, else another aggregate's crossed relationships fan its rows.
        if ident is None and isinstance(agg, AggregateKey) and crossing(agg):
            ident = ("crossing", _partition_free_identity(agg))
        # A transform's rows are its own domain (Axiom 11.6).
        if isinstance(agg, TransformKey):
            ident = ("transform", agg)
        gkey = (grain, ident, agg.filter if restricts_own_rows(agg) else None)
        groups.setdefault(gkey, []).append(agg)
        meta[gkey] = (grain, windowed)
    return [(*meta[g], aggs) for g, aggs in groups.items()]


class _LocalRegroupContext(BaseModel):
    """The enclosing plan's state every host-rooted local regroup producer reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    prebound: PreboundQuery
    bundle: ResolvedSourceBundle
    scope: DatasetScope
    stage_schemas: Dict[str, StageSchema]
    producer_source_dataset: Optional[str]  # NOSONAR(S8396) — required-nullable: the one caller always decides
    producer_registry: Dict[Hashable, PlannedQuery]
    population: Population
    producer_dataset: Optional[SlayerDataset]  # NOSONAR(S8396) — required-nullable: the one caller always decides
    mapping: Dict[ValueKey, ValueKey]
    inherited: List[Any]
    n_inherited_date: int
    #: Each re-aggregation placeholder's original: a transform producer computes its own.
    reagg_originals: Dict[ValueKey, ValueKey] = Field(default_factory=dict)


def _group_row_restriction(
    aggs: Sequence[ValueKey],
) -> Tuple[Optional[ValueKey], Dict[ValueKey, AggregateKey]]:
    """A producer group's shared FILTER row restriction and each restricted root's unfiltered key."""
    restricted = [a for a in aggs if restricts_own_rows(a)]
    conditions = {a.filter for a in restricted}
    assert len(conditions) <= 1 and len(restricted) in (0, len(aggs)), conditions
    return next(iter(conditions), None), {a: a.model_copy(update={"filter": None}) for a in restricted}


def _canonical_producer_aggs(
    aggs: Sequence[ValueKey],
) -> Tuple[List[ValueKey], Dict[ValueKey, ValueKey]]:
    """One producer metric per partition-free identity: a bare and a
    ``partition_by=`` twin collapse to one field."""
    by_identity: Dict[Hashable, ValueKey] = {}
    canonical_of: Dict[ValueKey, ValueKey] = {}
    for agg in aggs:
        canonical_of[agg] = by_identity.setdefault(_partition_free_identity(agg), agg)
    return list(by_identity.values()), canonical_of


def _assert_local_regroup_safe(
    *, producer_aggs: Sequence[ValueKey], producer_dataset: SlayerDataset,
    bundle: ResolvedSourceBundle, active_td: Optional[ValueKey],
    alias_map: Dict[ValueKey, str], display: Callable[[ValueKey], str],
) -> None:
    """Per-role crossing-input safety for every answer, and a PRESENT windowed axis
    attributable from the producer root (decision 12); a missing axis is left to
    the time-resolution guard downstream."""
    for agg in producer_aggs:
        if isinstance(agg, AggregateKey):
            _assert_local_producer_inputs_safe(
                agg=agg, host_dataset=producer_dataset, bundle=bundle,
                datasets_by_name={m.name: m for m in bundle.referenced_datasets},
            )
    if active_td is None:
        return
    first = producer_aggs[0]
    check_windowed_time_axis_attributable(
        alias=alias_map.get(first) if isinstance(first, AggregateKey) else None,
        root_name=producer_dataset.name,
        active_td_name=display(active_td),
        attributable=key_attributable_from_root(
            key=active_td, target_path=(), root_dataset=producer_dataset,
            datasets_by_name=bundle.datasets_by_name, bundle=bundle,
            host_dataset=producer_dataset, host_name=producer_dataset.name,
        ),
    )


def _collapsed_axis(root: ValueKey, *, phase: str, ordered_pks: Sequence[ValueKey]) -> List[ValueKey]:
    """A measure-position collapsing transform's axis: its pick attaches on the grain without it (Axiom 11.3b)."""
    if phase != "combined" or not isinstance(root, TransformKey) or root.op not in AXIS_COLLAPSING_TRANSFORMS:
        return []
    axis = root.time_key
    return [axis] if axis is not None and axis in ordered_pks else []


def _local_regroup_join_pairs(
    *, producer_plan: PlannedQuery, ordered_pks: Sequence[ValueKey],
    mapping: Mapping[ValueKey, ValueKey], collapsed: Sequence[ValueKey] = (),
) -> List[Tuple[ValueKey, Any]]:
    """Each grain key joined to its producer slot — by structural identity, else by
    projection position; a host-side key embedding another attach's aggregate
    renders via that attach's placeholder. A ``collapsed`` key is not joined on."""
    join_pairs = [
        (substitute_value_keys(pk, mapping), _grain_slot(producer_plan, ordered_pks=ordered_pks, pk=pk))
        for pk in ordered_pks if pk not in collapsed
    ]
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs}
        | {_grain_slot(producer_plan, ordered_pks=ordered_pks, pk=k) for k in collapsed},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    return join_pairs


def _grain_slot(producer_plan: PlannedQuery, *, ordered_pks: Sequence[ValueKey], pk: ValueKey) -> SlotId:
    """``pk``'s producer slot: by structural identity, else by projection position."""
    found = next((s.id for s in producer_plan.row_slots if s.key == pk), None)
    return producer_plan.projection[list(ordered_pks).index(pk)] if found is None else found


def _local_regroup_kernel(
    decision: Optional[_KernelDecision], *, producer_plan: PlannedQuery,
    bundle: ResolvedSourceBundle, ranking_candidates: Sequence[ValueKey],
) -> Dict[str, Any]:
    """The kernel of a producer whose answer IS a windowed / ranked aggregate."""
    if decision is None:
        return {}
    root_dataset = producer_plan.render_source_dataset or bundle.population
    assert root_dataset is not None  # a local producer renders against its host
    return _decided_kernel(
        decision, producer_plan=producer_plan, root_dataset=root_dataset, bundle=bundle,
        alias=canonical_aggregate_alias(decision.answer, profile="stage_expression"),
        target_rooted=False, ranking_candidates=ranking_candidates,
    )


def _judged_as_masked(
    aggs: Sequence[ValueKey], *, condition: Optional[ValueKey],
) -> Dict[ValueKey, ValueKey]:
    """Each producer aggregate by the key its reads are judged as: under a row restriction, the built-in input mask."""
    if condition is None:
        return {a: a for a in aggs}
    return {(input_masked(a, condition=condition) if isinstance(a, AggregateKey) else a): a for a in aggs}


def _with_population_endpoints(
    attach_kwargs: Dict[str, Any], *, ctx: "_LocalRegroupContext", ordered_pks: List[ValueKey],
) -> Dict[str, Any]:
    """A row-restricted windowed producer's buckets: the population's cells, never its own restricted rows."""
    kernel = attach_kwargs.get("kernel")
    if not isinstance(kernel, TrailingWindowProducerKernel) or ctx.producer_dataset is None:
        return attach_kwargs
    source = _EndpointsSource(
        host_dataset=ctx.producer_dataset, bundle=ctx.bundle, scope=ctx.scope, stage_schemas=ctx.stage_schemas,
        population=ctx.population, population_filters=ctx.inherited,
        n_population_date=ctx.n_inherited_date, mode_policy=ctx.prebound.mode_policy,
        ranking_candidates=ctx.prebound.ranking_candidates,
    )
    return {**attach_kwargs, "kernel": kernel.model_copy(update={
        "endpoints": _window_endpoints(
            host_keys=list(ordered_pks), source=source, producer_registry=ctx.producer_registry,
        ),
        "endpoint_keys": list(ordered_pks),
    })}


def _anchor_witness(root: ValueKey, *, ctx: "_LocalRegroupContext") -> Optional[AggregateKey]:
    """An aggregate-free transform's domain is the dimension anchor's cells (Axiom 11.6): those of ``count(<anchor>.*)``
    when the anchor is not the host."""
    supply, host = ctx.bundle.row_supply, ctx.producer_dataset
    if (
        not isinstance(root, TransformKey) or any(isinstance(k, AggregateKey) for k in walk_value_keys(root))
        or supply is None or supply.anchor is None or host is None or supply.anchor == host.name
    ):
        return None
    anchor = ctx.bundle.datasets_by_name.get(supply.anchor)
    if anchor is None:
        return None
    route = anchor_route(reader=host, anchor=anchor, datasets_by_name=ctx.bundle.datasets_by_name)
    return None if route.unrelated else AggregateKey(source=StarKey(path=route.route), agg="count")


def _producer_window_td(
    prebound: PreboundQuery, *, windowed: bool, root: ValueKey, pks: Grain,
) -> Optional[TimeTruncKey]:
    """A windowed producer's active bucket; a transform producer keeps it too when its grain holds it."""
    td = prebound.main_time_key
    return td if windowed or (isinstance(root, TransformKey) and td in pks) else None


def _synthesize_local_regroup(
    *, ctx: "_LocalRegroupContext", phase: Literal["row", "combined"], pks: Grain,
    windowed: bool, aggs: List[ValueKey], order_fn: Callable[[Grain], List[ValueKey]],
    alias_map: Dict[ValueKey, str], grain_names: Dict[ValueKey, str],
) -> RegroupAttachPlan:
    """One host-rooted producer for a group of local roots at one grain."""
    prebound, bundle, scope = ctx.prebound, ctx.bundle, ctx.scope
    producer_dataset, mapping = ctx.producer_dataset, ctx.mapping
    inherited, n_inherited_date = ctx.inherited, ctx.n_inherited_date
    pks = _prune_functionally_determined_grain(pks)
    # A transform producer evaluates its operand whole, re-aggregations included.
    own = {a: substitute_value_keys(a, ctx.reagg_originals) if isinstance(a, TransformKey) else a for a in aggs}
    # A row-restricted group shares one condition: the producer aggregates the unfiltered keys over its rows.
    condition, unfiltered = _group_row_restriction(aggs)
    if condition is not None:
        inherited = [*inherited, row_restriction_from_key(condition)]
    unfiltered = {**{a: own[a] for a in aggs if own[a] != a}, **unfiltered}
    producer_alias = {unfiltered.get(k, k): v for k, v in alias_map.items()}
    producer_aggs, canonical_of = _canonical_producer_aggs([unfiltered.get(a, a) for a in aggs])
    canonical_of = {a: canonical_of[unfiltered.get(a, a)] for a in aggs}
    canon_index = {c: i for i, c in enumerate(producer_aggs)}
    if producer_dataset is not None:
        judged = _judged_as_masked(producer_aggs, condition=condition)
        _assert_local_regroup_safe(
            producer_aggs=list(judged), producer_dataset=producer_dataset, bundle=bundle,
            active_td=prebound.main_time_key if windowed else None,
            alias_map={j: producer_alias[a] for j, a in judged.items() if a in producer_alias},
            display=functools.partial(_query_dim_display, prebound),
        )
    witness = _anchor_witness(aggs[0], ctx=ctx)
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=pks, aggs=[*producer_aggs, *([witness] if witness is not None else [])], dataset=producer_dataset,
        bundle=bundle, inherited=inherited, n_date_range=n_inherited_date,
        partition_order=order_fn, public_alias_by_agg=producer_alias,
        explicit_types={
            unfiltered.get(dm.bound.value_key, dm.bound.value_key): dm.datatype
            for dm in prebound.declared_metrics
            if phase == "combined" and dm.type_is_explicit and dm.datatype is not None
        },
        grain_name_by_key=grain_names,
        window_td_key=_producer_window_td(prebound, windowed=windowed, root=aggs[0], pks=pks),
        mode_policy=prebound.mode_policy,
        ranking_candidates=prebound.ranking_candidates,
    )
    decision = _kernel_decision(answer=producer_aggs[0], windowed=windowed)
    answer = own[aggs[0]]
    producer_plan = compile_synthesized(
        prebound=producer_prebound,
        source_dataset=ctx.producer_source_dataset,
        bundle=bundle,
        scope=scope,
        stage_schemas=ctx.stage_schemas,
        producer_registry=ctx.producer_registry,
        population=ctx.population,
        kernel_answer=_kernel_answer(decision),
        domain_of=answer if isinstance(answer, TransformKey) else None,
    )
    # A union-grain producer MAY carry nested attaches at any depth; the
    # complete-grain assert is the admission rule.
    # A bare aggregate root resolves to an aggregate slot; a transform root to a combined-expression slot.
    producer_value_slots = [
        *producer_plan.aggregate_slots,
        *producer_plan.combined_expression_slots,
    ]
    # A union-grain producer desugars its inners to placeholders; fall back to projection position.
    producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
    substitutions = [
        RegroupSubstitution(
            placeholder=mapping[agg],
            producer_slot_id=_regroup_answer_slot_id(
                value_slots=producer_value_slots, key=canonical_of[agg],
                fallback=producer_answer_ids[canon_index[canonical_of[agg]]]
                if canon_index[canonical_of[agg]] < len(producer_answer_ids)
                else None,
            ),
            original_key=agg,
            empty_value=_empty_value(agg, bundle=bundle),
        )
        for agg in aggs
    ]
    axis = _collapsed_axis(aggs[0], phase=phase, ordered_pks=ordered_pks)
    join_pairs = _local_regroup_join_pairs(
        producer_plan=producer_plan, ordered_pks=ordered_pks, mapping=mapping, collapsed=axis,
    )
    attach_kwargs = _local_regroup_kernel(
        decision, producer_plan=producer_plan, bundle=bundle,
        ranking_candidates=producer_prebound.ranking_candidates,
    )
    if condition is not None:
        attach_kwargs = _with_population_endpoints(attach_kwargs, ctx=ctx, ordered_pks=ordered_pks)
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=(
            (canonical_aggregate_alias(aggs[0], profile="stage_expression")
             if isinstance(aggs[0], AggregateKey) else None)
            or getattr(aggs[0], "agg", None)
            or getattr(aggs[0], "op", None)
            or "regroup"
        ),
        attach_phase=phase,
        join_pairs=join_pairs,
        substitutions=substitutions,
        partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
        # A population semi-join inherited into this producer reports each of
        # the producer's own public metrics, not its stage alias; an aggregate
        # selected under two names warns only the first.
        population_semi_join_metrics=(
            [alias_map[a] for a in aggs
             if isinstance(a, AggregateKey) and a in alias_map]
            if producer_plan.semi_join_filters else []
        ),
        collapsed=[_grain_slot(producer_plan, ordered_pks=ordered_pks, pk=k) for k in axis],
        **attach_kwargs,
    )



def _rewrite_regrouped_prebound(
    prebound: PreboundQuery, *, mapping: Mapping[ValueKey, ValueKey],
    combined_mapping: Mapping[ValueKey, ValueKey],
) -> PreboundQuery:
    """Every root replaced by its placeholder: a computed dimension takes the full
    mapping, a metric the combined one (its inners desugar COMBINED) plus each
    dimension value's own substitution."""
    metric_dims = position_classes(
        prebound.declared_metrics, n_grain=prebound.n_dims + prebound.n_time_dimensions,
    ).dim_keys
    dim_values: Dict[ValueKey, ValueKey] = {}
    for d in metric_dims:
        sub = substitute_value_keys(d, mapping)
        if sub != d:
            dim_values[d] = sub
    metric_mapping = {**dim_values, **combined_mapping}
    return PreboundQuery(
        declared_metrics=[
            dm.model_copy(update={"bound": BoundExpr(
                value_key=substitute_value_keys(
                    dm.bound.value_key,
                    mapping if dm.is_dimension else metric_mapping,
                ),
            )})
            for dm in prebound.declared_metrics
        ],
        bound_filters=[
            substitute_in_bound_filter(bf, mapping) for bf in prebound.bound_filters
        ],
        bound_filter_texts=list(prebound.bound_filter_texts),
        n_date_range=prebound.n_date_range,
        order_specs=[
            OrderSpec(
                bound=BoundExpr(
                    value_key=substitute_value_keys(sp.bound.value_key, mapping),
                ),
                direction=sp.direction,
                nulls=sp.nulls,
            )
            for sp in prebound.order_specs
        ],
        main_time_key=prebound.main_time_key,
        n_dims=prebound.n_dims,
        n_time_dimensions=prebound.n_time_dimensions,
        limit=prebound.limit,
        offset=prebound.offset,
        distinct_dimension_values=prebound.distinct_dimension_values,
        mode_policy=prebound.mode_policy,
        ranking_candidates=prebound.ranking_candidates,
    )


def _plan_regroups(
    *,
    prebound: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_dataset: Optional[str],
    producer_registry: Dict[Hashable, PlannedQuery],
    dispositions: Sequence[RootDisposition],
    nests: Callable[[ValueKey, str], bool],
    homes: AggregateHomes,
    population: Population,
    reserved_placeholders: AbstractSet[ValueKey] = frozenset(),
) -> Tuple[PreboundQuery, List[RegroupAttachPlan]]:
    """Group the discovered roots and desugar them into producer stages +
    reserved-leaf placeholders (row attach at base FROM, combined at the combined
    SELECT); producers compiled meanwhile read each placeholder's type here."""
    registry = RegroupPlaceholderRegistry(
        reserved=reserved_placeholders, typing=SlotTyping(scope=scope, bundle=bundle),
    )
    with minted_placeholders(registry.types):
        return _desugar_regroups(
            registry=registry, prebound=prebound, filter_typings=filter_typings, scope=scope,
            bundle=bundle, stage_schemas=stage_schemas, producer_source_dataset=producer_source_dataset,
            producer_registry=producer_registry, dispositions=dispositions, nests=nests, homes=homes,
            population=population,
        )


def _desugar_regroups(  # NOSONAR(S3776) — the regroup desugar body, unchanged; run inside the minted-placeholder scope.
    *,
    registry: RegroupPlaceholderRegistry,
    prebound: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_dataset: Optional[str],
    producer_registry: Dict[Hashable, PlannedQuery],
    dispositions: Sequence[RootDisposition],
    nests: Callable[[ValueKey, str], bool],
    homes: AggregateHomes,
    population: Population,
) -> Tuple[PreboundQuery, List[RegroupAttachPlan]]:
    reagg_mapping: Dict[ValueKey, ValueKey] = {
        d.root: registry.placeholder_for(d.root)
        for d in dispositions if d.routing == "reaggregation"
    }
    if reagg_mapping:
        substituted = _substitute_reaggregations(
            prebound, reagg_mapping, filter_typings=filter_typings,
        )
        homes = _carried_homes(homes, before=prebound, after=substituted)
        prebound = substituted
    dim_dms, td_dms, _ = partition_declared_metrics(
        declared_metrics=prebound.declared_metrics,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    projected_dim_keys = [dm.bound.value_key for dm in dim_dms]
    projected_td_keys = [dm.bound.value_key for dm in td_dms]
    active_bucket = prebound.main_time_key
    _is_crossing_local_root = crossing_local_root_predicate(scope=scope, bundle=bundle, homes=homes)
    routed = _group_routed_roots(
        dispositions, reagg_mapping=reagg_mapping, nests=nests, homes=homes,
    )
    row_aggs, combined_aggs = routed.row_local, routed.combined_local
    cm_row, cm_combined = routed.row_target, routed.combined_target
    public_alias_by_agg = routed.public_alias
    cm_type = routed.declared_type
    mixed_inline_inner = routed.inline_inputs
    row_reaggs, combined_reaggs = routed.row_reagg, routed.combined_reagg
    if (
        not row_aggs and not combined_aggs and not cm_combined and not cm_row
        and not row_reaggs and not combined_reaggs
    ):
        return prebound, []
    # A real field sharing the reserved placeholder prefix would shadow a placeholder at render; reject while a regroup is active.
    producer_dataset = scope.population
    reserved = reserved_prefix_fields(producer_dataset)
    check_reserved_regroup_prefix(reserved)
    mapping: Dict[ValueKey, ValueKey] = {
        agg: registry.placeholder_for(agg)
        for agg in (
            *row_aggs, *combined_aggs, *cm_row, *cm_combined, *row_reaggs,
        )
    }

    inherited, n_inherited_date = _regroup_inherited_filters(
        prebound=prebound, filter_typings=filter_typings,
    )

    # A combined producer keeps the consumer's dimension order (row producers use the alphabetical default).
    consumer_order: Dict[ValueKey, int] = {
        dm.bound.value_key: idx for idx, dm in enumerate([*dim_dms, *td_dms])
    }
    # A combined producer names its grain by the consumer's dimension name (a month td is ``ordered_at``, not ``ordered_at_month``).
    grain_name_by_key: Dict[ValueKey, str] = {
        dm.bound.value_key: dm.declared_name
        for dm in [*dim_dms, *td_dms]
        if dm.declared_name is not None
    }

    def _combined_order(pks: Grain) -> List[ValueKey]:
        return sorted(pks, key=lambda k: consumer_order.get(k, len(consumer_order)))

    ctx = _LocalRegroupContext(
        prebound=prebound, bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        producer_source_dataset=producer_source_dataset, producer_registry=producer_registry,
        population=population, producer_dataset=producer_dataset, mapping=mapping,
        inherited=inherited, n_inherited_date=n_inherited_date,
        reagg_originals={ph: orig for orig, ph in reagg_mapping.items()},
    )
    phases: List[Tuple[
        Literal["row", "combined"], List[ValueKey], Callable[[Grain], List[ValueKey]],
        Dict[ValueKey, str], Dict[ValueKey, str],
    ]] = [
        ("row", row_aggs, _regroup_partition_order, {}, {}),
        ("combined", combined_aggs, _combined_order, public_alias_by_agg, grain_name_by_key),
    ]
    attaches: List[RegroupAttachPlan] = [
        _synthesize_local_regroup(
            ctx=ctx, phase=phase, pks=pks, windowed=windowed, aggs=aggs,
            order_fn=order_fn, alias_map=alias_map, grain_names=grain_names,
        )
        for phase, phase_aggs, order_fn, alias_map, grain_names in phases
        for pks, windowed, aggs in _local_regroup_groups(
            phase_aggs, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
            crossing=_is_crossing_local_root,
        )
    ]

    # One target-rooted producer per distinct cross-dataset aggregate; roles share one producer + placeholder.
    host_dataset_for_cm = scope.population
    assert host_dataset_for_cm is not None
    datasets_by_name_cm = bundle.datasets_by_name
    base_filters_with_text = [
        (bf, text) for bf, text in zip(
            prebound.bound_filters,
            prebound.bound_filter_texts
            + [None] * (len(prebound.bound_filters) - len(prebound.bound_filter_texts)),
        ) if not bf.row_restriction
    ]
    synthesis_context = _ProducerSynthesisContext(
        prebound=prebound, bundle=bundle, host_dataset=host_dataset_for_cm,
        datasets_by_name=datasets_by_name_cm,
        projected_dim_keys=projected_dim_keys,
        projected_td_keys=projected_td_keys,
        base_filters_with_text=base_filters_with_text, scope=scope,
        stage_schemas=stage_schemas, homes=homes,
        population=population, population_filters=inherited, n_population_date=n_inherited_date,
    )
    _check_inline_params_determined(dispositions=dispositions, context=synthesis_context)
    cm_phases: List[Tuple[Literal["row", "combined"], List[ValueKey]]] = [
        ("combined", cm_combined), ("row", cm_row),
    ]
    for phase, cm_aggs in cm_phases:
        for agg in cm_aggs:
            assert isinstance(agg, AggregateKey)  # target-rooted roots are aggregates
            attaches.append(_synthesize_cross_dataset_producer(
                agg=agg, placeholder=mapping[agg], attach_phase=phase,
                public_alias=public_alias_by_agg.get(agg),
                context=synthesis_context, declared_type=cm_type.get(agg),
                producer_registry=producer_registry,
            ))

    # One producer-over-producer per re-aggregation, attached once per phase.
    for root in combined_reaggs:
        attaches.append(_synthesize_reaggregation_producer(
            root=root, placeholder=reagg_mapping[root], attach_phase="combined",
            public_alias=public_alias_by_agg.get(root),
            context=synthesis_context,
            declared_type=cm_type.get(root),
            producer_registry=producer_registry, registry=registry,
            inherited=inherited, n_date_range=n_inherited_date,
            population=population,
        ))
    # A row-phase one is synthesized at its own declared grain (never widened to the
    # query dimensions) and broadcast per partition onto the base rows.
    for c in row_reaggs:
        c_grain = constituent_grain(
            c=c, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        attaches.append(_synthesize_reaggregation_producer(
            root=c, placeholder=mapping[c], attach_phase="row",
            public_alias=public_alias_by_agg.get(c),
            context=synthesis_context.model_copy(update={
                "projected_dim_keys": [
                    k for k in c_grain if not isinstance(k, TimeTruncKey)
                ],
                "projected_td_keys": [
                    k for k in c_grain if isinstance(k, TimeTruncKey)
                ],
            }),
            declared_type=cm_type.get(c),
            producer_registry=producer_registry, registry=registry,
            inherited=inherited, n_date_range=n_inherited_date,
            population=population,
            population_semi_join_metrics=routed.constituent_consumers.get(c),
        ))

    # The ROW substitution applies ONLY to computed DIMENSIONS; a non-dim metric
    # keeps query-grain (its inners desugar to COMBINED placeholders) — EXCEPT a
    # mixed-source metric's inline inner constituents, which row-attach and so
    # must reach the metric too.
    combined_mapping: Dict[ValueKey, ValueKey] = {
        agg: mapping[agg]
        for agg in (*combined_aggs, *cm_combined, *mixed_inline_inner)
    }
    rewritten = _rewrite_regrouped_prebound(
        prebound, mapping=mapping, combined_mapping=combined_mapping,
    )
    # Intern every producer: a structurally identical one becomes the same plan object.
    attaches = [_intern_producer(a, producer_registry) for a in attaches]
    return rewritten, attaches


def _has_inline_population_aggregate(prebound: PreboundQuery, *, homes: AggregateHomes) -> bool:
    """A plain host-rooted aggregate survives inline over the population rows —
    every producer-bound term is a placeholder by now, so any real ``AggregateKey``
    with a host-local source, no partition_by, no window and not first/last computes
    directly over the (row-filtered) population. Scans projected metrics, ordering
    keys AND filters — an aggregate in a HAVING/combined predicate is inline over
    the population too, so a fanning row filter multiplies it just the same."""
    keys = [dm.bound.value_key for dm in prebound.declared_metrics]
    keys += [sp.bound.value_key for sp in prebound.order_specs]
    keys += [bf.value_key for bf in prebound.bound_filters]
    return any(
        isinstance(a, AggregateKey) and not homes.target_rooted(a)
        and a.locus != "host" and a.partition_keys is None
        and not is_kernel_requiring(a)
        for vk in keys for a in walk_value_keys(vk)
    )


def compile_synthesized(
    prebound: PreboundQuery,
    *,
    source_dataset: Optional[str],
    bundle: ResolvedSourceBundle,
    scope: DatasetScope,
    stage_schemas: Dict[str, StageSchema],
    population: Population,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
    carried_attaches: Sequence[RegroupAttachPlan] = (),
    reserved_placeholders: AbstractSet[ValueKey] = frozenset(),
    kernel_answer: Optional[AggregateKey] = None,
    placeholder_originals: Optional[Mapping[ValueKey, ValueKey]] = None,
    domain_of: Optional[TransformKey] = None,
) -> PlannedQuery:
    """Elaborate a compiler-synthesized producer — its aggregates' homes relative to
    its OWN root (D3) — and compile it. ``carried_attaches`` are outer attaches whose
    placeholders it reads as is; ``reserved_placeholders`` are never minted again;
    ``kernel_answer`` is the answer its kernel renders."""
    env = elaborate_synthesized(
        prebound, bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        source_dataset=source_dataset,
    )
    context = ProducerContext(
        enclosing_grain=Grain.of(dm.bound.value_key for dm in env.prebound.grain_declared_metrics),
        population=population,
        carried_attaches=tuple(carried_attaches),
        reserved_placeholders=frozenset(reserved_placeholders),
        kernel_answer=kernel_answer,
        placeholder_originals=dict(placeholder_originals or {}),
        domain_of=domain_of,
    )
    routed = _route_producer(
        env, context=context,
        producer_registry={} if producer_registry is None else producer_registry,
    )
    return _emit_planned(routed)


def compile_stage(
    env: ElaboratedStage,
    *,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile one user-authored stage: the once-per-stage steps (population
    disposal, redundant-partition strip, total routing) run here only."""
    return _emit_planned(_route_top_level(
        env, producer_registry={} if producer_registry is None else producer_registry,
    ))


class _Routed(BaseModel):
    """A routed stage: the substituted prebound, its attaches and the emission inputs."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    query: SlayerQuery | StrictQueryCarrier
    env: ElaboratedStage | ElaboratedProducer
    typed_prebound: PreboundQuery
    prebound: PreboundQuery
    attaches: List[RegroupAttachPlan]
    population: Population
    producer_registry: Dict[Hashable, PlannedQuery]
    #: Non-series ``time_shift`` roots, each answered by a shifted producer.
    shift_candidates: FrozenSet[ValueKey]
    kernel_answer: Optional[AggregateKey] = None
    #: Every aggregate's home, each rebuilt aggregate keeping its original's.
    homes: AggregateHomes
    #: Placeholders read from an attach outside ``attaches``, each to its original key.
    placeholder_originals: Mapping[ValueKey, ValueKey] = Field(default_factory=dict)
    #: The transform whose domain this producer's rows are.
    domain_of: Optional[TransformKey] = None


def _shift_candidates(dispositions: Sequence[RootDisposition]) -> FrozenSet[ValueKey]:
    return frozenset(
        d.root for d in dispositions if d.routing == "shifted" and d.series is False
    )


def _route_top_level(
    env: ElaboratedStage, *, producer_registry: Dict[Hashable, PlannedQuery],
) -> _Routed:
    prebound, scope, bundle = env.prebound, env.scope, env.bundle
    filter_typings = list(env.filter_typings)
    population = _population_of(dispose_population_filters(
        prebound=prebound, filter_typings=filter_typings, scope=scope, bundle=bundle,
    ))
    stripped, homes = _strip_redundant_partitions(env)
    supply = bundle.row_supply
    dispositions = discover_roots(
        stripped, filter_typings=filter_typings, scope=scope, bundle=bundle, homes=homes,
        transforms=TransformRouting(
            route=spine_field(scope.population) is None,
            rows_are_host_cells=supply is None or not supply.branches,
            spine=spine_field(scope.population) is not None,
        ),
    )
    routed_prebound, attaches = _plan_regroups(
        prebound=stripped, filter_typings=filter_typings,
        scope=scope, bundle=bundle, stage_schemas=dict(env.stage_schemas),
        producer_source_dataset=_producer_source_dataset(env),
        producer_registry=producer_registry,
        dispositions=dispositions,
        nests=lambda _root, _phase: True,
        homes=homes,
        population=population,
    )
    check_metrics_at_query_grain(prebound)
    homes = _carried_homes(homes, before=stripped, after=routed_prebound)
    _assert_total_routing(routed_prebound, homes=homes)
    return _Routed(
        query=env.query, env=env, typed_prebound=prebound, prebound=routed_prebound,
        attaches=attaches, population=population, producer_registry=producer_registry,
        shift_candidates=_shift_candidates(dispositions), homes=homes,
    )


def _route_producer(
    env: ElaboratedProducer, *, context: ProducerContext,
    producer_registry: Dict[Hashable, PlannedQuery],
) -> _Routed:
    prebound, scope, bundle = env.prebound, env.scope, env.bundle
    dispositions = discover_roots(
        prebound, filter_typings=env.filter_typings, scope=scope, bundle=bundle, homes=env.homes,
        transforms=TransformRouting(answers=frozenset(
            dm.bound.value_key for dm in prebound.declared_metrics if not dm.is_dimension
        ), spine=spine_field(scope.population) is not None),
    )
    routed_prebound, attaches = _plan_regroups(
        prebound=prebound, filter_typings=list(env.filter_typings),
        scope=scope, bundle=bundle, stage_schemas=dict(env.stage_schemas),
        producer_source_dataset=_producer_source_dataset(env),
        producer_registry=producer_registry,
        dispositions=dispositions,
        nests=_producer_nesting_rule(prebound, context=context),
        homes=env.homes,
        population=context.population,
        reserved_placeholders=context.reserved_placeholders,
    )
    return _Routed(
        query=env.query, env=env, typed_prebound=prebound, prebound=routed_prebound,
        attaches=[*attaches, *context.carried_attaches], population=context.population,
        producer_registry=producer_registry,
        shift_candidates=_shift_candidates(dispositions),
        kernel_answer=context.kernel_answer,
        homes=_carried_homes(env.homes, before=prebound, after=routed_prebound),
        placeholder_originals=context.placeholder_originals,
        domain_of=context.domain_of,
    )


def _producer_source_dataset(env: ElaboratedStage | ElaboratedProducer) -> Optional[str]:
    if isinstance(env.query.population, str):
        return env.query.population
    if env.scope.population is not None:
        return env.scope.population.name
    return None


def _ordered_parts(key: ValueKey) -> Tuple[Tuple[ValueKey, ...], AbstractSet[ValueKey]]:
    """``key``'s positional children and its partition keys, a set never paired by position."""
    if isinstance(key, AggregateKey):
        # ``children()`` is the source / args / kwargs keys, then the partition keys, then the filter.
        keys, kids = frozenset(key.partition_keys or ()), key.children()
        tail = (kids[-1],) if key.filter is not None else ()
        return kids[:len(kids) - len(keys) - len(tail)] + tail, keys
    if isinstance(key, TransformKey):
        return tuple(c for c in (key.input, key.time_key) if c is not None), frozenset(key.partition_keys)
    return key.children(), frozenset()


def _rebuilt_aggregates(before: ValueKey, after: ValueKey, out: Dict[ValueKey, ValueKey]) -> None:
    """Map each aggregate of ``before`` a substitution rebuilt to its form in ``after`` (a replaced subtree ends the
    walk); a partition key pairs only as the one member replaced by the one new member."""
    if before == after or type(before) is not type(after):
        return
    if isinstance(before, AggregateKey):
        out[before] = after
    (b_ordered, b_keys), (a_ordered, a_keys) = _ordered_parts(before), _ordered_parts(after)
    for b, a in zip(b_ordered, a_ordered):
        _rebuilt_aggregates(b, a, out)
    gone, came = b_keys - a_keys, a_keys - b_keys
    if len(gone) == 1 and len(came) == 1:
        _rebuilt_aggregates(next(iter(gone)), next(iter(came)), out)


def _carried_homes(homes: AggregateHomes, *, before: PreboundQuery, after: PreboundQuery) -> AggregateHomes:
    """``homes`` with every aggregate the substitution ``before`` → ``after`` rebuilt keeping its home (D7)."""
    rebuilt: Dict[ValueKey, ValueKey] = {}
    pairs = [
        *zip((dm.bound.value_key for dm in before.declared_metrics), (dm.bound.value_key for dm in after.declared_metrics)),
        *zip((bf.value_key for bf in before.bound_filters), (bf.value_key for bf in after.bound_filters)),
        *zip((sp.bound.value_key for sp in before.order_specs), (sp.bound.value_key for sp in after.order_specs)),
    ]
    for b, a in pairs:
        _rebuilt_aggregates(b, a, rebuilt)
    return homes.carried(rebuilt) if rebuilt else homes


def _strip_redundant_partitions(env: ElaboratedStage) -> Tuple[PreboundQuery, AggregateHomes]:
    """A LOCAL row-attach root partitioned by exactly the query grain aggregates
    INLINE with its inputs row-attached; each stripped root keeps its home."""
    prebound, scope, bundle, homes = env.prebound, env.scope, env.bundle, env.homes
    query_grain = Grain.of(dm.bound.value_key for dm in prebound.grain_declared_metrics)
    roots = dict.fromkeys(d.root for d in discover_roots(
        prebound, filter_typings=env.filter_typings, scope=scope, bundle=bundle, homes=homes,
    ))
    strip: Dict[ValueKey, ValueKey] = {
        r: r.model_copy(update={"partition_keys": None})
        for r in roots
        if isinstance(r, AggregateKey) and is_row_attach_root(r)
        and not homes.target_rooted(r) and window_kwarg_of(r) is None
        and r.partition_keys is not None
        and Grain.of(r.partition_keys) == query_grain
    }
    if not strip:
        return prebound, homes
    stripped = _substitute_prebound(prebound, strip)
    return stripped, _carried_homes(homes, before=prebound, after=stripped)


def _producer_nesting_rule(
    prebound: PreboundQuery, *, context: ProducerContext,
) -> Callable[[ValueKey, str], bool]:
    """A row root always nests. A combined root at exactly the producer grain nests
    iff it is row-restricted, or kernel-requiring and not the producer's kernel answer; off the
    producer grain only the producer's own answers stay inline (their dropped
    members are the synthesizer's disposition)."""
    dim_dms, td_dms, _ = partition_declared_metrics(
        declared_metrics=prebound.declared_metrics,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    projected_dim_keys = [dm.bound.value_key for dm in dim_dms]
    projected_td_keys = [dm.bound.value_key for dm in td_dms]
    answers = {
        dm.bound.value_key for dm in prebound.declared_metrics if not dm.is_dimension
    }

    def nests(root: ValueKey, phase: str) -> bool:
        if phase == "row" or (isinstance(root, TransformKey) and root not in answers):
            return True
        grain = constituent_grain(
            c=root, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=prebound.main_time_key,
        )
        if not grain.is_subgrain_of(context.enclosing_grain):
            return root not in answers
        return grain != context.enclosing_grain or restricts_own_rows(root) or (
            is_kernel_requiring(root) and root != context.kernel_answer
        )

    return nests


def _emit_planned(routed: _Routed) -> PlannedQuery:  # NOSONAR(S3776) — projection and final plan assembly over one routed stage; the order-wrap / mask / reachability / shifted arms share the projection registry.
    """Project a routed stage and assemble its ``PlannedQuery``."""
    env = routed.env
    query = routed.query
    scope, bundle = env.scope, env.bundle
    stage_schemas = dict(env.stage_schemas)
    filter_typings = list(env.filter_typings)
    typed_prebound = routed.typed_prebound
    prebound = routed.prebound
    producer_registry = routed.producer_registry
    population = routed.population
    population_filters = (
        population.filters if isinstance(population, InheritedPopulation) else None
    )
    regroup_attach_plans: List[RegroupAttachPlan] = list(routed.attaches)
    _producer_source_dataset_name = _producer_source_dataset(env)
    # The generator renders FROM / joins against the binder's dataset.
    render_source_dataset = scope.population
    declared_metrics = list(prebound.declared_metrics)
    bound_filters = list(prebound.bound_filters)
    n_date_range = prebound.n_date_range
    order_specs = list(prebound.order_specs)
    active_td_key = prebound.main_time_key
    n_dims = prebound.n_dims
    n_tds = prebound.n_time_dimensions
    distinct_dimension_values = prebound.distinct_dimension_values
    # Pre-substitution metric roots, positionally aligned with env.metrics.
    _coh_metric_roots = [
        dm.bound.value_key
        for dm in typed_prebound.declared_metrics[n_dims + n_tds:]
    ]
    _assert_broadcast_coherence(
        env=env, metric_roots=_coh_metric_roots,
        attach_plans=regroup_attach_plans,
    )

    # An inferred stage's host is the planner's choice, never a namespace the query's aliases live in.
    source_col_names = frozenset() if bundle.row_supply is not None else _source_field_names(scope)
    host_name = host_dataset_name(scope)

    # Windowed-metric guards on the pre-projection trees; returns the cleanly-selected windowed AggregateKeys.
    selected_windowed = _guard_windowed_metrics(
        metric_vks=[dm.bound.value_key for dm in declared_metrics],
        filter_vks=[bf.value_key for bf in bound_filters],
        order_vks=[sp.bound.value_key for sp in order_specs],
        active_td_key=active_td_key,
    )

    projection = ProjectionPlanner().plan(
        metrics=declared_metrics,
        filters=bound_filters,
        order=order_specs,
        typing=SlotTyping(scope=scope, bundle=bundle, placeholders={
            **routed.placeholder_originals,
            **{s.placeholder: s.original_key for a in regroup_attach_plans for s in a.substitutions},
        }),
        source_field_names=source_col_names,
        host_dataset_name=host_name,
    )

    row_slots, agg_slots, combined_slots = _bucket_slots(
        projection.registry.slots,
    )
    grain: Optional[List[SlotId]] = (
        list(dict.fromkeys(projection.public_projection[:n_dims + n_tds]))
        if distinct_dimension_values else None
    )

    # Raw-rows mode: any aggregate-phase slot came from a filter or order item, which the flag forbids.
    if distinct_dimension_values is False and agg_slots:
        check_raw_rows_no_aggregate_slots(
            offender=_canonical_name(agg_slots[0].key),
        )

    # Detect the selected windowed slots (window TD = the resolved active TD).
    active_td_slot_id = (
        projection.registry.find_by_key(active_td_key)
        if active_td_key is not None
        else None
    )
    windowed_slot_ids = _windowed_slot_id_set(
        selected_windowed=selected_windowed,
        registry=projection.registry,
        active_td_slot_id=active_td_slot_id,
    )

    # Classify each ORDER BY target not a declared/public slot: it resolves like a filter ref (aggregate → hidden slot; grouped row field → hidden min/max wrap; transform/composite → hidden outer wrap).
    # ORDER BY targets rewritten to a hidden wrap, keyed by (key, DIRECTION) since ``a ASC, a DESC`` needs MIN(a) and MAX(a).
    order_key_remap: Dict[Tuple[ValueKey, str], ValueKey] = {}
    # Host-grain / crossing wraps synthesized as late producers, and the slots they answer.
    late_wrap_keys: set = set()
    late_attach_answered: set = set()
    # Minted after routing: a joined wrap is host-locus, a local one reads host rows only.
    order_wraps: set = set()
    host_dataset_for_wraps = scope.population
    grain_dim_keys = frozenset(dm.bound.value_key for dm in declared_metrics[:n_dims + n_tds])
    for spec in order_specs:
        okey = spec.bound.value_key
        osid = projection.registry.find_by_key(okey)
        if osid is not None and not projection.registry.get(osid).hidden:
            continue  # declared / projected output — orders on a real field
        if isinstance(okey, AggregateKey):
            continue  # hidden aggregate (local base or cross-dataset CTE)
        if isinstance(okey, FieldKey) and okey.leaf.startswith(REGROUP_LEAF_PREFIX):
            # A combined regroup placeholder resolves via its producer, not a hidden MIN/MAX.
            continue
        if grain is None:
            continue  # raw-rows query -> split emission, no wrap needed
        wrap = _order_wrap_source(okey, dim_keys=grain_dim_keys)
        if wrap is not None:
            src, path = wrap
            # ASC orders each group by its MIN, DESC by its MAX.
            wrap_key = AggregateKey(
                source=src,
                agg="min" if spec.direction == "asc" else "max",
                # A JOINED sort key is host-grain; a target-rooted CTE would degenerate to a scalar CROSS JOIN.
                locus="host" if path else "target",
            )
            if projection.registry.find_by_key(wrap_key) is None:
                projection.registry.intern(
                    key=wrap_key,
                    declared_name=_canonical_name(wrap_key),
                    hidden=True,
                    phase=wrap_key.phase,
                )
            order_key_remap[(okey, spec.direction)] = wrap_key
            order_wraps.add(wrap_key)
            # A JOINED wrap, or a local wrap whose source crosses a relationship, is a HOST-rooted producer synthesized late.
            _wrap_crosses = path or (
                host_dataset_for_wraps is not None
                and aggregate_input_closure(
                    key=wrap_key, anchor_dataset=host_dataset_for_wraps,
                    anchor_relation=host_dataset_for_wraps.name, bundle=bundle,
                )
            )
            if _wrap_crosses and wrap_key not in late_wrap_keys:
                late_wrap_keys.add(wrap_key)
                regroup_attach_plans.append(_intern_producer(
                    _synthesize_wrap_attach(
                        wrap_key=wrap_key, prebound=prebound,
                        filter_typings=filter_typings, scope=scope,
                        bundle=bundle,
                        stage_schemas=stage_schemas,
                        producer_registry=producer_registry,
                        producer_source_dataset=_producer_source_dataset_name,
                        row_attaches=[
                            a for a in regroup_attach_plans
                            if a.attach_phase == "row"
                        ],
                        population=population,
                    ),
                    producer_registry,
                ))
                wrap_sid = projection.registry.find_by_key(wrap_key)
                if wrap_sid is not None:
                    late_attach_answered.add(wrap_sid)
        # A transform / composite referenced only in ORDER BY materialises as a hidden slot at the outer wrap.

    # Re-bucket: hidden order-wrap slots interned above must reach the aggregate bucket.
    if order_key_remap:
        row_slots, agg_slots, combined_slots = _bucket_slots(
            projection.registry.slots,
        )

    # Population disposition consumer (D1/D2): this plan (host base or a
    # host-rooted producer) restricts by association whatever it does not
    # materialise on its own grain. Drop those conjuncts from the masks and carry
    # them as correlated EXISTS on this plan (total over the boolean
    # shape, never fails closed).
    pop_semi_join_groups: List[SemiJoinFilter] = []
    pushed_conjunct_keys: List[ValueKey] = []
    host_population_gated = False
    if population_filters is not None and render_source_dataset is not None:
        host_grain_paths = _grain_closure_paths(
            [dm.bound.value_key for dm in declared_metrics[:n_dims + n_tds]],
            host_dataset=render_source_dataset, bundle=bundle,
        )
        pop_semi_join_groups, pushed_conjunct_keys = (
            population_filters.host_split(host_grain_paths)
        )
        # Backstop (D6): with a plain aggregate inline over the population, a
        # fanning conjunct always takes the EXISTS — its fanning dimension is
        # routed to a producer, never materialised on the base grain.
        n_semi = sum(
            1 for dc in population_filters.conjuncts
            if dc.disposition == "semi_join"
        )
        assert not (
            _has_inline_population_aggregate(prebound, homes=routed.homes)
            and len(pushed_conjunct_keys) < n_semi
        ), "a spine-covered fanning mask coexists with an inline plain aggregate"
        host_population_gated = bool(pop_semi_join_groups)
    # An inferred spine stage with no other dimensions lists every bucket: its filters restrict the homes only.
    spine_unit = bundle.row_supply is not None and render_source_dataset is not None and is_spine(render_source_dataset)
    if spine_unit:
        pop_semi_join_groups, host_population_gated = [], False

    # Each filter conjunct compiles to a hidden whole-predicate slot; the mask entry
    # carries its typing and stratum. Interned after every other slot so no earlier
    # hidden name or slot id shifts; lowering to WHERE/HAVING/outer placements is
    # emission-side (sql.generator).
    masks: List[MaskEntry] = []
    mask_keys: List[ValueKey] = []
    pushed_set = set(pushed_conjunct_keys)
    # Date-range filters are the first ``n_date_range`` bound_filters; a semi-join
    # push can drop one, so count the survivors rather than trust the pre-drop
    # total — the prefix ``masks[:n_date_range_masks]`` must stay exactly the
    # surviving frame bounds (readers: sql.generator lowering, _plan_src_row_filters).
    surviving_date_range = 0
    spine = spine_field(render_source_dataset)
    spine_factor = (
        plan_spine(prebound=typed_prebound, host=render_source_dataset, spine=spine, bundle=bundle)
        if spine is not None and render_source_dataset is not None else None
    )
    if spine is None:
        check_off_spine_order_keys(typed_prebound)
    for i, (bf, ct) in enumerate(zip(bound_filters, filter_typings)):
        key = _drop_conjuncts(value_key=bf.value_key, drop=pushed_set)
        key = host_mask(key, spine=spine) if key is not None else None
        if spine_unit and ct.typing == MaskTyping.FIELD:
            key = None
        if key is None:
            continue  # every conjunct moved to the EXISTS, or bounds the spine series
        mask_sid = projection.registry.find_by_key(key)
        if mask_sid is None:
            mask_sid = projection.registry.intern(
                key=key,
                declared_name=f"__slayer_mask_{i}",
                hidden=True,
                phase=key.phase,
            )
        masks.append(MaskEntry(
            slot_id=mask_sid, typing=ct.typing, stratum=ct.stratum, row_restriction=bf.row_restriction,
        ))
        mask_keys.append(key)
        if i < n_date_range:
            surviving_date_range += 1
    if masks:
        row_slots, agg_slots, combined_slots = _bucket_slots(
            projection.registry.slots,
        )
    # Per-mask structural reachability summary, in this plan's coordinate system.
    reachability_anchor_dataset = render_source_dataset or bundle.population
    source_relation = host_name
    filter_reachability: List[FilterReachability] = []
    # One expansion cache for the whole plan (both visitors and every filter share it).
    reachability_cache: dict = {}
    for key, mask in zip(mask_keys, masks):
        filter_reachability.append(FilterReachability(
            filter_id=mask.slot_id,
            has_host_local_ref=key_has_host_local_ref(
                key=key,
                anchor_dataset=reachability_anchor_dataset,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
        ))
    # Backstop: raise on a cross-dataset slot that survived the desugar (else it fan-multiplies).
    for slot in agg_slots:
        if slot.id in late_attach_answered or slot.key in order_wraps:
            continue  # an order wrap reads host rows or is answered by a late host-grain producer
        key = slot.key
        assert not routed.homes.target_rooted(key), (
            f"Cross-dataset aggregate slot {slot.id!r} survived the regroup "
            f"desugar; every cross-dataset aggregate must "
            f"become a target-rooted producer."
        )

    order_entries = []
    for spec in order_specs:
        # A grouped row-field sort key was rewritten to a hidden wrap above; order on that slot.
        okey = order_key_remap.get(
            (spec.bound.value_key, spec.direction), spec.bound.value_key,
        )
        sid = projection.registry.find_by_key(okey)
        if sid is None:
            # An unslotted order target would be silently dropped; fail loudly instead.
            check_order_target_has_slot(
                type_name=type(spec.bound.value_key).__name__,
            )
        order_slot = projection.registry.get(sid)
        direction = spec.direction
        assert direction in ("asc", "desc")
        order_entries.append(OrderEntry(
            slot_id=sid,
            direction=direction,
            phase=order_slot.key.phase,
            nulls=spec.nulls,
        ))

    transform_layers = _emit_transform_layers(slots=projection.registry.slots)
    supply = bundle.row_supply
    field_relation = source_relation if supply is None else (supply.anchor or "")
    stage_schema = _emit_stage_schema(
        stage_name=query.name, source_relation=source_relation, field_relation=field_relation,
        projection=projection, grain=grain,
        root=render_source_dataset, datasets_by_name=bundle.datasets_by_name,
        originals={sub.placeholder: sub.original_key
                   for attach in regroup_attach_plans for sub in attach.substitutions},
        upstream=_upstream_respellings(scope),
        restricted=_stage_restricts(prebound=typed_prebound, population=render_source_dataset, bundle=bundle),
    )

    frame_bound_fields = _frame_bound_fields(grain=grain, registry=projection.registry)

    # A COMBINED regroup attach is an isolated aggregate (value in the producer CTE, never _base) → still an empty-base spine.
    regroup_combined_slot_ids: set = set()
    for attach in regroup_attach_plans:
        if attach.attach_phase != "combined":
            continue
        for sub in attach.substitutions:
            sid = projection.registry.find_by_key(sub.placeholder)
            if sid is not None:
                regroup_combined_slot_ids.add(sid)
    regroup_combined_slot_ids |= _isolated_composites(
        slots=[*agg_slots, *combined_slots], isolated={*windowed_slot_ids, *regroup_combined_slot_ids},
        registry=projection.registry,
    )
    empty_base_plan = _plan_empty_base_grain(
        projection=projection.public_projection,
        agg_slots=agg_slots,
        windowed_slot_ids=windowed_slot_ids,
        regroup_combined_slot_ids=regroup_combined_slot_ids,
        order_entries=order_entries,
        masks=masks,
        host_gated=host_population_gated or _restricted_dataset(render_source_dataset, bundle=bundle),
        dimensionless=not grain,
    )

    regroup_attach_plans = [*regroup_attach_plans, *_plan_shifted_attaches(
        slots=projection.registry.slots, attaches=regroup_attach_plans,
        prebound=typed_prebound, rewritten=prebound, filter_typings=filter_typings,
        scope=scope, bundle=bundle, stage_schemas=stage_schemas,
        producer_source_dataset=_producer_source_dataset_name,
        producer_registry=producer_registry, population=population,
        candidates=routed.shift_candidates, homes=routed.homes,
    )]
    # Assign every slot its materialisation stage / needs-field / series fact;
    # producer bodies were staged by their own compilation.
    row_slots, agg_slots, combined_slots = stage_slots(
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        combined_expression_slots=combined_slots,
        regroup_attach_plans=regroup_attach_plans,
        masks=masks,
        order=order_entries,
        projection=projection.public_projection,
        grain=grain,
    )

    if spine_factor is not None:
        # Over the spine every time_shift reads its shifted producer (its series evaluated over the moved bounds).
        combined_slots = [
            s.model_copy(update={"series": False})
            if isinstance(s.key, TransformKey) and s.key.op == "time_shift" else s for s in combined_slots
        ]
    _assert_no_inline_kernel_aggregates(
        slots=[*agg_slots, *combined_slots], kernel_answer=routed.kernel_answer,
        attaches=regroup_attach_plans,
    )
    planned = PlannedQuery(
        source_relation=source_relation,
        field_relation=field_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        regroup_attach_plans=regroup_attach_plans,
        combined_expression_slots=combined_slots,
        transform_layers=transform_layers,
        masks=masks,
        n_date_range_masks=surviving_date_range,
        projection=projection.public_projection,
        order=order_entries,
        limit=prebound.limit,
        offset=prebound.offset,
        stage_schema=stage_schema,
        active_time_dimension_slot_id=active_td_slot_id,
        render_source_dataset=render_source_dataset,
        grain=grain,
        frame_bound_fields=frame_bound_fields,
        filter_reachability=filter_reachability,
        empty_base_plan=empty_base_plan,
        semi_join_filters=pop_semi_join_groups,
        spine=spine_factor,
        mode_policy=prebound.mode_policy,
        domain=_plan_domain(
            attaches=regroup_attach_plans, agg_slots=agg_slots, grain=grain,
            # A shifted producer looks its operand up for every host cell too (the re-aggregation regime).
            host=routed.domain_of not in {dm.bound.value_key for dm in typed_prebound.declared_metrics},
        ) if routed.domain_of is not None else None,
    )
    return planned


def _plan_domain(
    *, attaches: Sequence[RegroupAttachPlan], agg_slots: Sequence[ValueSlot], grain: Optional[List[SlotId]],
    host: bool = False,
) -> Optional[PlannedDomain]:
    """A transform producer's rows when an attached constituent supplies cells: the union of each full-grain
    constituent's cells, the host's own when it computes an aggregate (Axiom 11.6); ``None`` when its rows are
    its host's cells."""
    supplying = [
        a.substitutions[0].placeholder for a in attaches
        if a.attach_phase == "combined" and a.substitutions and a.unrelated_dataset is None
        and not a.broadcast_dimensions and not a.collapsed and len(a.join_pairs) == len(grain or ())
    ]
    if not grain or not supplying:
        return None
    return PlannedDomain(host=host or bool(agg_slots), placeholders=supplying)




def _assert_no_inline_kernel_aggregates(
    *, slots: Sequence[ValueSlot], kernel_answer: Optional[AggregateKey],
    attaches: Sequence[RegroupAttachPlan],
) -> None:
    """A kernel-requiring aggregate renders only as its own producer's kernel answer (sql P10)."""
    to_original = {sub.placeholder: sub.original_key for a in attaches for sub in a.substitutions}
    for slot in slots:
        for k in walk_value_keys(slot.key):
            assert not is_kernel_requiring(k) or (
                _original_key(k, to_original=to_original) == kernel_answer
            ), (
                f"kernel-requiring aggregate {k!r} left inline in a producer "
                f"whose kernel answer is {kernel_answer!r}"
            )


def _isolated_composites(*, slots: list, isolated: AbstractSet[SlotId], registry) -> set[SlotId]:
    """Composites and transforms built only from isolated aggregates and literals: they too leave ``_base`` empty."""
    def composed(key: ValueKey) -> bool:
        if isinstance(key, LiteralKey) or registry.find_by_key(key) in isolated:
            return True
        if isinstance(key, TransformKey):
            return composed(key.input)
        return isinstance(key, SLOT_COMPOSITE_KINDS) and all(composed(c) for c in key.children())

    return {s.id for s in slots if isinstance(s.key, (*SLOT_COMPOSITE_KINDS, TransformKey)) and composed(s.key)}


def _host_membership(
    *, host_dataset: SlayerDataset, target_path: Tuple[str, ...], datasets_by_name: Dict[str, SlayerDataset],
    bundle: ResolvedSourceBundle,
) -> List[Tuple[BoundFilter, Optional[str]]]:
    """For a home beyond a restricted host, the row filter ``<host join column> IS NOT NULL`` on the
    home's first hop from the host: re-rooted, it keeps only home rows associated with a host row. An inferred
    stage hosted elsewhere than its dimension anchor has no population to restrict by (semantics 12.4)."""
    supply = bundle.row_supply
    if supply is not None and supply.anchor != host_dataset.name:
        return []
    if not target_path or not _restricted_dataset(host_dataset, bundle=bundle):
        return []
    chain = follow(root=host_dataset, path=target_path[:1], datasets_by_name=datasets_by_name)
    if not chain or not chain[0].column_pairs or chain[0].lateral is not None:
        return []  # a product hop associates every row; a list child reads its parent's dataset
    witness = FieldKey(leaf=chain[0].column_pairs[0][0])
    return [(bound_filter_from_key(ArithmeticKey(op=_IS_NOT, operands=(witness, LiteralKey(value=None)))), None)]


def _restricted_dataset(dataset: Optional[SlayerDataset], *, bundle: ResolvedSourceBundle) -> bool:
    """Whether reading ``dataset`` reads only some of its rows: it, or the parent a list child reads, has filters
    or is a stage whose rows restrict its population."""
    if dataset is None:
        return False
    if dataset.list_of is not None:
        parent = bundle.datasets_by_name.get(dataset.list_of[0])
        return parent is not None and (bool(parent.filters) or parent.restricts_rows)
    return bool(dataset.filters) or dataset.restricts_rows


def _plan_empty_base_grain(
    *,
    projection: List[SlotId],
    agg_slots: list,
    windowed_slot_ids: AbstractSet[SlotId],
    order_entries: list,
    masks: List[MaskEntry],
    regroup_combined_slot_ids: Optional[set] = None,
    host_gated: bool = False,
    dimensionless: bool = False,
) -> "EmptyBaseGrainPlan | None":
    """Decide the empty-base spine at plan time — the host base has nothing of its own exactly when every value asked for is an isolated aggregate. ``host_gated`` (D7) means a population semi-join or the host's own filters gate the spine even with no field mask; a ``dimensionless`` stage is never gated (one row, semantics Law 10)."""
    isolated = set(windowed_slot_ids)
    isolated |= (regroup_combined_slot_ids or set())
    if not projection or any(sid not in isolated for sid in projection):
        return None
    if any(slot.id not in isolated for slot in agg_slots):
        return None  # a host-local aggregate would give _base a field of its own
    if any(entry.slot_id not in isolated for entry in order_entries):
        return None
    if dimensionless:
        return EmptyBaseGrainPlan()
    # Field masks gate the host spine; metric masks resolve after attachment.
    host_filter_ids = [m.slot_id for m in masks if m.typing == MaskTyping.FIELD]
    return EmptyBaseGrainPlan(host_filter_ids=host_filter_ids, host_gated=host_gated)


def _frame_bound_fields(*, grain: Optional[List[SlotId]], registry) -> List[ValueKey]:
    """Raw fields of the grain's time dimensions — an explicit bound on one is a FRAME bound."""
    keys = (registry.get(sid).key for sid in grain or ())
    return list(dict.fromkeys(k.source for k in keys if isinstance(k, TimeTruncKey)))


def _plan_src_row_filters(
    *,
    producer_plan: PlannedQuery,
) -> "Tuple[List[str], List[SrcFilterRewrite]]":
    """Partition the producer's field masks for a windowed metric's ``_src`` scope into ``(where_filter_ids, src_filter_rewrites)`` by frame-bound membership (row restrictions exempt, date-range masks are frame bounds)."""
    time_cols = frozenset(producer_plan.frame_bound_fields)
    slots_by_id = {
        s.id: s
        for s in (
            *producer_plan.row_slots,
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        )
    }
    restricted = list(dict.fromkeys(m.slot_id for m in producer_plan.masks if m.row_restriction))
    where_ids: List[str] = list(restricted)
    rewrites: List[SrcFilterRewrite] = []
    date_ids = {
        m.slot_id
        for m in producer_plan.masks[:producer_plan.n_date_range_masks]
    }
    for m in producer_plan.masks:
        if m.typing != MaskTyping.FIELD or m.slot_id in date_ids or m.slot_id in restricted:
            continue
        key = slots_by_id[m.slot_id].key
        residual = strip_frame_bounds(key=key, time_fields=time_cols)
        if residual is None:
            continue  # wholly a frame bound
        where_ids.append(m.slot_id)
        if residual is not key:
            rewrites.append(SrcFilterRewrite(
                filter_id=m.slot_id,
                expression=BoundExpr(value_key=residual),
            ))
    return where_ids, rewrites


def _source_field_names(
    scope: DatasetScope,
) -> FrozenSet[str]:
    if scope.population is not None:
        return frozenset(c.name for c in scope.population.fields)
    return frozenset()


def _bucket_slots(slots: List[ValueSlot]):
    row: List[ValueSlot] = []
    agg: List[ValueSlot] = []
    combined: List[ValueSlot] = []
    for s in slots:
        if s.phase == Phase.ROW:
            row.append(s)
        elif s.phase == Phase.AGGREGATE:
            agg.append(s)
        else:
            combined.append(s)
    return row, agg, combined


def _value_anchor_path(key: ValueKey) -> Tuple[str, ...]:
    """The relationship path a path-derived output's auto-name is prefixed with."""
    if isinstance(key, TimeTruncKey):
        key = key.source
    if isinstance(key, (FieldKey, FieldSqlKey, StarKey)):
        return tuple(key.path)
    if isinstance(key, AggregateKey):
        return source_anchor_path(key.source)
    return ()


def _respellings(
    *, flat: str, key: ValueKey, root: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset], upstream: Mapping[str, Tuple[str, ...]],
) -> Tuple[str, ...]:
    """Every stale spelling of ``flat``: its path's named hops spelled by their
    target dataset (any subset), crossed with the leaf field's own respellings
    (auto-names are path- then leaf-prefixed, so re-deriving substitutes both)."""
    path = _value_anchor_path(key)
    if not path:
        return _leaf_variants(rest=flat, key=key, upstream=upstream)[1:]
    prefix = "__".join(path) + "__"
    if root is None or not flat.startswith(prefix):
        return ()
    try:
        chain = follow(root=root, path=path, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return ()
    if not chain:
        return ()
    named = [i for i, e in enumerate(chain) if e.name is not None]
    paths = [
        "__".join(chain[i].to if i in subset else tok
                  for i, tok in enumerate(path))
        for r in range(len(named) + 1)
        for subset in itertools.combinations(named, r)
    ]
    terminal = datasets_by_name.get(chain[-1].to)
    leaves = _leaf_variants(
        rest=flat[len(prefix):], key=key,
        upstream=_field_respellings(terminal.fields if terminal else []),
    )
    return tuple(p + "__" + leaf for p in paths for leaf in leaves)[1:]


def _field_respellings(fields) -> Dict[str, Tuple[str, ...]]:
    return {c.name: c.respellings for c in fields if c.respellings}


def _upstream_respellings(
    scope: DatasetScope,
) -> Dict[str, Tuple[str, ...]]:
    """Respellings of the fields a stage reads locally (upstream stage / query-backed)."""
    if scope.population is None:
        return {}
    return _field_respellings(scope.population.fields)


def _leaf_variants(
    *, rest: str, key: ValueKey, upstream: Mapping[str, Tuple[str, ...]],
) -> Tuple[str, ...]:
    """``rest`` first, then ``rest`` with its source field's name swapped for
    each of that field's respellings."""
    if isinstance(key, TimeTruncKey):
        key = key.source
    if isinstance(key, AggregateKey):
        key = key.source
    if not isinstance(key, (FieldKey, FieldSqlKey)):
        return (rest,)
    name = field_leaf(key)
    if not rest.startswith(name):
        return (rest,)
    tail = rest[len(name):]
    return (rest, *(r + tail for r in upstream.get(name, ())))


def _emit_stage_schema(
    *,
    stage_name: Optional[str],
    source_relation: str,
    field_relation: str,
    projection,
    grain: Optional[List[SlotId]],
    root: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset],
    originals: Mapping[ValueKey, ValueKey],
    upstream: Mapping[str, Tuple[str, ...]],
    restricted: bool = False,
) -> StageSchema:
    """Each grain slot's first public field names the schema grain; ``None`` in raw-row mode."""
    fields: List[StageField] = []
    alias_idx: Dict[str, int] = {}
    flat_by_sid: Dict[SlotId, str] = {}
    key_by_flat: Dict[str, str] = {}
    collision: Optional[Tuple[str, str, str]] = None
    for sid in projection.public_projection:
        slot = projection.registry.get(sid)
        if slot.hidden:
            continue
        alias = pick_slot_alias(slot=slot, alias_index=alias_idx)
        rk = slot_result_key(slot=slot, alias=alias, source_relation=source_relation)
        # Downstream bind + CTE column name are the ``__``-flattened form; public_alias keeps the dotted result-key form.
        flat = flat_name(rk, strip_relation=field_relation)
        # Two public fields flattening to one downstream name make the column ambiguous to a stage reading it by name.
        if flat in key_by_flat:
            check_stage_flatten_collision(flat_name=flat, collides=stage_name is not None or key_by_flat[flat] == rk,
                                          entries=(key_by_flat[flat], rk))
            collision = collision or (flat, key_by_flat[flat], rk)
            continue
        key_by_flat[flat] = rk
        fields.append(_stage_field(
            slot=slot, alias=rk.removeprefix(f"{field_relation}."), flat=flat,
            source=_source_field(key=slot.key, root=root, datasets_by_name=datasets_by_name),
            respellings=() if alias in slot.explicit_aliases else _respellings(
                flat=flat, key=originals.get(slot.key, slot.key), root=root,
                datasets_by_name=datasets_by_name, upstream=upstream,
            ),
            passes=pass_through(key=slot.key, root=root, datasets_by_name=datasets_by_name)
            if slot.phase == Phase.ROW else None,
        ))
        flat_by_sid.setdefault(sid, flat)
    grain_names = None if grain is None else [flat_by_sid[sid] for sid in grain if sid in flat_by_sid]
    return StageSchema(
        relation_name=stage_name or "(unnamed_stage)", fields=fields,
        grain=grain_names,
        default_time_dimension=_surviving_default(projection=projection, fields=fields, root=root),
        inherited_edges=inherited_edges(fields=fields, grain=grain_names, root=root, datasets_by_name=datasets_by_name),
        restricted=restricted,
        flatten_collision=collision,
    )


def _stage_restricts(
    *, prebound: PreboundQuery, population: Optional[SlayerDataset], bundle: ResolvedSourceBundle,
) -> bool:
    """Whether a stage's rows restrict its population: it filters (date bounds and metric filters included) or
    trims rows, or reads a restricted population."""
    trimmed = prebound.limit is not None or bool(prebound.offset)
    return bool(prebound.bound_filters) or trimmed or _restricted_dataset(population, bundle=bundle)


def _surviving_default(*, projection, fields: List[StageField], root: Optional[SlayerDataset]) -> Optional[str]:
    """The stage field carrying its source's default time field (raw or bucketed), if it survives."""
    default = root.effective_default_time_dimension if root is not None else None
    if default is None:
        return None
    wanted = FieldKey(path=(), leaf=default)
    visible = [projection.registry.get(sid) for sid in projection.public_projection]
    for slot, field in zip((s for s in visible if not s.hidden), fields):
        key = slot.key.source if isinstance(slot.key, TimeTruncKey) else slot.key
        if key == wanted:
            return field.name
    return None


def _source_field(
    *, key: ValueKey, root: Optional[SlayerDataset], datasets_by_name: Dict[str, SlayerDataset],
):
    """The field a row-level stage output reads (a dataset field or an upstream stage
    field), for the metadata it carries downstream; ``None`` for anything else."""
    if isinstance(key, TimeTruncKey):
        key = key.source
    if not isinstance(key, (FieldKey, FieldSqlKey)):
        return None
    leaf = field_leaf(key)
    if root is None:
        return None
    try:
        chain = follow(root=root, path=key.path, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return None
    if chain is None:
        return None
    dataset = datasets_by_name.get(chain[-1].to) if chain else root
    return dataset.get_field(leaf) if dataset is not None else None


def _stage_field(
    *, slot: ValueSlot, alias: str, flat: str, respellings: Tuple[str, ...], source=None,
    passes: Optional[PassThrough] = None,
) -> StageField:
    # A bucketed field (bucketed here, or passed through from upstream) carries its granularity
    # so a re-binding TimeDimension can type-check the re-bucket.
    row = source if slot.phase == Phase.ROW else None
    upstream_gran = (
        slot.key.granularity
        if isinstance(slot.key, TimeTruncKey) else getattr(row, "granularity", None)
    )
    return StageField(
        name=flat,
        sql_alias=flat,
        public_alias=alias,
        datatype=slot.type,
        element_type=slot.element_type or (row.element_type if row is not None and slot.type is DataType.ARRAY else None),
        granularity=upstream_gran,
        label=slot.label or (row.label if row is not None else None),
        hidden=False,
        format=slot.format or (row.format if row is not None else None),
        description=slot.description or (row.description if row is not None else None),
        respellings=respellings,
        pass_through=passes,
    )


def _transform_dep_graph(
    *, transform_slots: List[ValueSlot],
) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    """(in-degree, dependents) per slot id; an inner input's slot precedes its consumer."""
    slot_by_key = {s.key: s for s in transform_slots}
    in_degree = {s.id: 0 for s in transform_slots}
    deps_of: Dict[str, List[str]] = {s.id: [] for s in transform_slots}
    for s in transform_slots:
        for dep in _iter_slot_deps(s.key):
            if dep is s.key or not isinstance(dep, TransformKey):
                continue
            dep_slot = slot_by_key.get(dep)
            if dep_slot is None:
                continue
            deps_of[dep_slot.id].append(s.id)
            in_degree[s.id] += 1
    return in_degree, deps_of


def _toposort_slot_ids(
    *, transform_slots: List[ValueSlot], in_degree: Dict[str, int],
    deps_of: Dict[str, List[str]],
) -> List[str]:
    ready = [s.id for s in transform_slots if in_degree[s.id] == 0]
    ordered_ids: List[str] = []
    while ready:
        nxt = ready.pop(0)
        ordered_ids.append(nxt)
        for child in deps_of[nxt]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
    # A dependency cycle would strand slots here; the staging pass detects and
    # raises on one before this list is consumed, so no straggler fallback.
    return ordered_ids


def _emit_transform_layers(*, slots: List[ValueSlot]) -> List[TransformLayer]:
    """One TransformLayer per TransformKey slot, in dependency order (innermost first) so an inner window/self-join renders before the outer one consumes it."""
    transform_slots = [
        s for s in slots if isinstance(s.key, TransformKey)
    ]
    # Topological order: a slot whose TransformKey.input references another slot's key must come after it.
    in_degree, deps_of = _transform_dep_graph(transform_slots=transform_slots)
    ordered_ids = _toposort_slot_ids(
        transform_slots=transform_slots, in_degree=in_degree, deps_of=deps_of,
    )
    op_by_id = {
        s.id: s.key.op for s in slots if isinstance(s.key, TransformKey)
    }
    return [
        TransformLayer(op=op_by_id[sid], slot_ids=[sid])
        for sid in ordered_ids
    ]
