"""The stage compiler: one typed prebound → ``PlannedQuery`` (``compile_prebound``).
Binding lives in ``bind_inputs``; typing and the checker in ``elaborate_env``."""

from __future__ import annotations

from decimal import Decimal
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Hashable,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeGuard,
    Union,
)

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, RANKED_AGGREGATIONS, TimeGranularity
from slayer.core.errors import AmbiguousJoinPathError, UnreachableFilterDroppedWarning
from slayer.core.keys import AggregateKey, Grain, ArithmeticKey, BetweenKey, ColumnKey, ColumnSqlKey, InKey, LiteralKey, Phase, ScalarCallKey, StarKey, TimeTruncKey, TransformKey, ValueKey, column_leaf, regroup_root_grain, effective_root_grain, constituent_grain, reroot_value_key, substitute_value_keys, walk_value_keys, walk_consumer_keys, REGROUP_LEAF_PREFIX, is_cross_model_agg, is_local_partitioned_agg, split_top_level_and, window_kwarg_of, is_reaggregation_key, is_row_attach_root, attached_inputs, operand_aggregates, operand_constituents, source_anchor_path
from slayer.core.models import SlayerModel
from slayer.engine.reference_closure import (
    ParamSpec,
    aggregate_input_closure,
    first_unanalyzable_input_column,
    first_unanalyzable_source_row_leaf,
    fragment_closure,
    key_closure,
    resolve_aggregation_params,
    source_row_leaf_closure,
)
from slayer.core.join_walker import resolve_hop, walk
from slayer.engine.join_safety import (
    UNREACHABLE_NO_PATH,
    _back_path,
    attributable_from_root,
    broadcast_reason,
    crossing_local_root_predicate,
    grain_determines,
    grain_member_attributable,
    key_attributable_from_root,
    key_broadcast_reason,
    key_host_path,
    local_crossing_input_paths,
    reroot_from_root,
    shared_join_key_reroot,
    _unique_key_sets,
    safe_reachable,
)
from slayer.core.query import (
    SlayerQuery,
)
from slayer.sql.naming import canonical_aggregate_alias, flat_name
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration
from slayer.core.scope import ModelScope, StageColumn, StageSchema, host_model_name
from slayer.engine.elaborate_env import (
    check_reserved_regroup_prefix,
    check_stage_flatten_collision,
    validate_model_filter,
    check_association_root_unique_key,
    check_association_windowed_ranked,
    check_cross_model_inputs_safe,
    check_cross_model_partition_keys_attributable,
    check_cross_model_source_resolves,
    check_input_dependencies_analyzable,
    check_local_producer_inputs_safe,
    check_order_target_has_slot,
    check_attached_inputs_attributable,
    check_parameter_determined,
    check_population_filter_no_fanout,
    check_raw_rows_no_aggregate_slots,
    check_reaggregation_dims_attributable,
    check_reaggregation_no_window,
    check_reaggregation_partition_key_is_query_dim,
    check_windowed_cross_model_time_axis,
    check_windowed_key_supported,
    check_windowed_time_dimension,
)
from slayer.engine.elaborate import elaborate_query
from slayer.ir.bound import BoundExpr, BoundFilter, DeclaredMeasure, OrderSpec, bound_filter_from_key, combined_consumer_aggregates, dimension_partitioned_aggregates, dimension_regroup_roots
from slayer.ir.elaborated import ConjunctTyping, ElaboratedQuery
from slayer.ir.terms import Aggregate
from slayer.engine.filter_reachability import (
    compute_key_join_paths,
    key_has_host_local_ref,
)
from slayer.ir.planned import (
    AssociationProducerKernel,
    EmptyBaseGrainPlan,
    FilterReachability,
    MaskEntry,
    MaskTyping,
    ModeAFilter,
    OrderEntry,
    PickedParam,
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
    ordered_row_keys,
    resolve_ranking_time_key,
)
from slayer.engine.compile.projection import (
    ProjectionPlanner,
    _canonical_name,
    _iter_slot_deps,
)
from slayer.engine.compile.staging import stage_slots
from slayer.engine.key_metadata import (
    dimension_key_metadata,
    measure_key_format_description,
    measure_key_preserves_native_type,
    measure_key_type,
)
from slayer.ir.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    partition_declared_measures,
    position_typing_context,
    walk_key_path,
)
from slayer.engine.compile.regroup import (
    RegroupPlaceholderRegistry,
    reserved_prefix_columns,
    substitute_in_bound_filter,
)
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    source_name_if_sibling,
)


__all__ = [
    "compile_prebound",
]


def _row_key_path(key: ValueKey) -> tuple:
    if isinstance(key, TimeTruncKey):
        return _row_key_path(key.column)
    return tuple(getattr(key, "path", ()))


# Duration-windowed measures (``window='90d'``).


def _windowed_agg_keys(vk: ValueKey) -> list:
    return [k for k in walk_value_keys(vk) if window_kwarg_of(k) is not None]


def _guard_windowed_measures(
    *,
    measure_vks: list,
    filter_vks: list,
    order_vks: list,
    active_td_key,
) -> dict:
    """Validate windowed-measure shapes; return cleanly-SELECTED windowed AggregateKeys in declaration order (value = slot ``hidden`` flag)."""
    all_vks = [*measure_vks, *filter_vks, *order_vks]
    if not any(_windowed_agg_keys(vk) for vk in all_vks):
        return {}

    for vk in all_vks:
        for key in _windowed_agg_keys(vk):
            check_windowed_key_supported(key=key, window_val=window_kwarg_of(key))

    selected_windowed: dict = {}
    for vk in measure_vks:
        if window_kwarg_of(vk) is not None:
            selected_windowed.setdefault(vk, False)
    # Order-only windowed target: HIDDEN, after the measure loop so an also-declared key keeps hidden=False.
    for vk in order_vks:
        for key in _windowed_agg_keys(vk):
            selected_windowed.setdefault(key, True)

    check_windowed_time_dimension(resolved=active_td_key is not None)
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
            f"Windowed measure {key!r} was selected but has no projection "
            f"slot; planner/projection drift (DEV-1714)."
        )
        windowed_slot_ids.add(sid)
    return windowed_slot_ids


# Regroup desugar: synthesize a producer stage per partition set.


def _regroup_grain_name(pk: ValueKey) -> str:
    if isinstance(pk, TimeTruncKey):
        return f"{column_leaf(pk.column)}_{pk.granularity}"
    path = tuple(getattr(pk, "path", ()) or ())
    leaf = getattr(pk, "leaf", None) or getattr(pk, "column_name", None) or "grain"
    return "__".join([*path, leaf])


def _regroup_partition_order(pks: Grain) -> List[ValueKey]:
    return sorted(
        pks, key=lambda k: (isinstance(k, TimeTruncKey), _regroup_grain_name(k), repr(k)),
    )


def _regroup_producer_prebound(  # NOSONAR(S3776) — one producer-prebound assembly; the grain / aggregate / inherited-filter / order arms share the prebound under construction.
    *,
    pks: Grain,
    aggs: List[ValueKey],
    model: Optional[SlayerModel],
    bundle: ResolvedSourceBundle,
    inherited: List[BoundFilter],
    n_date_range: int,
    partition_order: Callable[
        [Grain], List[ValueKey],
    ] = _regroup_partition_order,
    public_alias_by_agg: Optional[Mapping[AggregateKey, str]] = None,
    explicit_types: Optional[Mapping[ValueKey, DataType]] = None,
    grain_name_by_key: Optional[Mapping[ValueKey, str]] = None,
    window_td_key: Optional[ValueKey] = None,
    to_many_handling: str = "broadcast",
) -> Tuple[PreboundQuery, List[ValueKey]]:
    """The producer's bind product: grain from partition keys, one measure per consumed aggregate, inherited base-row filters; returns prebound + ordered grain keys."""
    public_alias_by_agg = public_alias_by_agg or {}
    explicit_types = explicit_types or {}
    grain_name_by_key = grain_name_by_key or {}
    ordered = partition_order(pks)
    dims = [pk for pk in ordered if not isinstance(pk, TimeTruncKey)]
    tds = [pk for pk in ordered if isinstance(pk, TimeTruncKey)]
    if window_td_key is not None and window_td_key not in pks:
        tds = [*tds, window_td_key]
    grain_dms: List[DeclaredMeasure] = []
    for pk in [*dims, *tds]:
        if model is not None:
            d_type, d_fmt, d_desc = dimension_key_metadata(
                model=model, key=pk, bundle=bundle,
            )
        else:
            d_type, d_fmt, d_desc = None, None, None
        # A combined attach names its grain by the consumer's dimension name.
        name = grain_name_by_key.get(pk) or _regroup_grain_name(pk)
        grain_dms.append(DeclaredMeasure(
            bound=BoundExpr(value_key=pk),
            declared_name=name, public_name=name,
            type=d_type, format=d_fmt, description=d_desc,
            # A grain key is a dimension the producer GROUPS BY; marking a computed one makes its inner aggregate a ROW attach.
            is_dimension=True,
        ))
    agg_dms: List[DeclaredMeasure] = []
    for agg in aggs:
        canonical = (
            (public_alias_by_agg.get(agg) if isinstance(agg, AggregateKey) else None)
            or (canonical_aggregate_alias(agg, profile="stage_formula")
                if isinstance(agg, AggregateKey) else None)
            or getattr(agg, "agg", None)
            or getattr(agg, "op", None)
            or "regroup"
        )
        # A transform root has no model-measure metadata; a consumer's explicit type wins over the source column.
        if model is not None and isinstance(agg, AggregateKey):
            a_type = measure_key_type(model=model, key=agg)
            a_fmt, a_desc = measure_key_format_description(model=model, key=agg)
        else:
            a_type, a_fmt, a_desc = None, None, None
        agg_dms.append(DeclaredMeasure(
            bound=BoundExpr(value_key=agg),
            declared_name=canonical, public_name=canonical,
            type=explicit_types.get(agg, a_type), format=a_fmt, description=a_desc,
            type_is_explicit=agg in explicit_types,
            preserve_native_type=(
                model is not None
                and agg not in explicit_types
                and measure_key_preserves_native_type(model=model, key=agg)
            ),
        ))
    prebound = PreboundQuery(
        declared_measures=[*grain_dms, *agg_dms],
        bound_filters=list(inherited),
        bound_filter_texts=[None] * len(inherited),
        n_date_range=n_date_range,
        order_specs=[],
        main_time_key=window_td_key,
        n_dims=len(dims),
        n_time_dimensions=len(tds),
        distinct_dimension_values=True,
        to_many_handling=to_many_handling,
    )
    return prebound, [*dims, *tds]


def _regroup_inherited_filters(
    *, prebound: PreboundQuery, filter_typings: Sequence[ConjunctTyping],
) -> Tuple[List[BoundFilter], int]:
    """Stratum-0 field masks define every producer's population; nothing else inherits."""
    date_bounds: List[BoundFilter] = []
    others: List[BoundFilter] = []
    for idx, (bf, ct) in enumerate(zip(prebound.bound_filters, filter_typings)):
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
    projected = set(producer_plan.projection)
    return {slot.id for slot in producer_plan.row_slots if slot.id in projected}


def _assert_attach_covers_producer_grain(
    *, joined_slot_ids: set, producer_grain_slot_ids: set,
) -> None:
    """The attach MUST join on the producer's COMPLETE grouping grain (from the planned producer); a coarser join multiplies rows."""
    if joined_slot_ids != producer_grain_slot_ids:
        raise ValueError(
            "Regroup attach join keys do not match the producer's grouping grain; "
            "the join must cover the complete grain or it changes cardinality "
            "(DEV-1824)."
        )


# Bare windowed / first-last measures desugar as combined-attach roots.
def _is_bare_local_regroup_root(k: ValueKey) -> bool:
    return (
        isinstance(k, AggregateKey)
        and k.partition_keys is None
        and not source_anchor_path(k.source)
        and (window_kwarg_of(k) is not None or k.agg in RANKED_AGGREGATIONS)
    )


def _bare_combined_roots(  # NOSONAR(S3776) — straight-line discovery walk over projected slots + filters collecting bare regroup roots; each branch is independently simple
    prebound: PreboundQuery,
    *,
    extra_root: Optional[Callable[[ValueKey], bool]] = None,
) -> Tuple[List[AggregateKey], Dict[AggregateKey, str]]:
    """Bare windowed / first-last aggregates (plus keys ``extra_root`` admits) reachable from a non-dim measure/order/filter; first-seen, deduped, named measure maps to alias."""

    def _is_root(k: ValueKey) -> bool:
        return _is_bare_local_regroup_root(k) or (
            extra_root is not None and extra_root(k)
        )

    seen: set = set()
    out: List[AggregateKey] = []
    alias: Dict[AggregateKey, str] = {}
    # Opaque below a row-attach root's inputs — a windowed / first-last inner
    # belongs to that root's own attach, not a bare combined root (DEV-1859).
    for dm in prebound.declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        for k in walk_consumer_keys(vk):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
        if _is_root(vk) and dm.public_name is not None:
            alias.setdefault(vk, dm.public_name)
    for sp in prebound.order_specs:
        for k in walk_consumer_keys(sp.bound.value_key):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
    for bf in prebound.bound_filters:
        for k in walk_consumer_keys(bf.value_key):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
    return out, alias


def _scalar_free_columns(node: ValueKey, out: set) -> None:
    # Asymmetric on purpose: aggregate subtrees are bound, not free.
    if isinstance(node, ColumnKey):
        out.add(node)
    elif isinstance(node, ArithmeticKey):
        for op in node.operands:
            _scalar_free_columns(node=op, out=out)
    elif isinstance(node, ScalarCallKey):
        for arg in node.args:
            if isinstance(arg, (ColumnKey, ArithmeticKey, ScalarCallKey, TransformKey)):
                _scalar_free_columns(node=arg, out=out)
    elif isinstance(node, TransformKey):
        _scalar_free_columns(node=node.input, out=out)


def _prune_functionally_determined_grain(pks: Grain) -> Grain:
    """Drop computed-dimension grain keys functionally determined by the raw dimensions already in the grain (redundant to group by)."""
    raw = frozenset(k for k in pks if isinstance(k, ColumnKey))
    kept = set(pks)
    for k in pks:
        if isinstance(k, ColumnKey):
            continue
        aggs = [a for a in walk_value_keys(k) if isinstance(a, AggregateKey)]
        if not aggs or any(
            a.partition_keys is None or not (frozenset(a.partition_keys) <= raw)
            for a in aggs
        ):
            continue
        free: set = set()
        _scalar_free_columns(k, free)
        if free <= raw:
            kept.discard(k)
    return Grain.of(kept)


def _windowed_or_ranked_identity(agg: ValueKey):
    """A hashable, partition-free identity for a windowed / ranked aggregate (own producer each); ``None`` for a plain aggregate."""
    if not isinstance(agg, AggregateKey):
        return None
    windowed = window_kwarg_of(agg) is not None
    ranked = agg.agg in RANKED_AGGREGATIONS
    if not windowed and not ranked:
        return None
    return (
        "windowed" if windowed else "ranked",
        agg.source, agg.agg, tuple(agg.args), tuple(agg.kwargs),
    )


def _partition_free_identity(agg: ValueKey):  # NOSONAR(S8495) — distinct-shape identity tuples are intentional dict keys: a plain aggregate's 4-field identity and an "other" 2-tuple never collide (different lengths compare unequal)
    if not isinstance(agg, AggregateKey):
        return ("other", agg)
    return (agg.source, agg.agg, tuple(agg.args), tuple(agg.kwargs))


def _cross_model_input_paths(
    *, agg_rooted: AggregateKey, root_model: SlayerModel, root_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[List[Tuple[str, ...]]]:
    """The dependency closure of the aggregate's inputs in the root's coordinates
    (source / args / kwargs / column filter / definition defaults, derived
    definitions expanded); ``None`` when a dependency cannot be analysed."""
    closure = aggregate_input_closure(
        key=agg_rooted, anchor_model=root_model, anchor_relation=root_name,
        bundle=bundle, include_source=True,
    )
    return None if closure is None else list(closure)


def _first_unsafe_input_hop(
    *, paths: List[Tuple[str, ...]], root_model: SlayerModel, root_name: str,
    models_by_name: Dict[str, SlayerModel],
) -> List[str]:
    for path in paths:
        if not safe_reachable(
            root=root_model, path=path, models_by_name=models_by_name,
        ):
            # First violation wins; the checker raises it.
            return [path[-1] if path else root_name]
    return []


def _first_unattributable_arg_leaf(
    *, agg: AggregateKey, target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    bundle: ResolvedSourceBundle, host_model: Optional[SlayerModel] = None,
    host_name: Optional[str] = None,
) -> List[str]:
    # Positional args and column-valued kwargs in HOST coordinates (a ranking
    # first/last time key, a weight column); a fail-closed backstop under the
    # home rule, judged on each input's dependency closure (DEV-1900). host_name
    # lets an off-home input traverse a proven reverse hop, as the home rule judged.
    for arg in (*agg.args, *(v for _, v in agg.kwargs)):
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        attributable = (
            key_attributable_from_root(
                key=arg, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, bundle=bundle,
                host_model=host_model, host_name=host_name,
            )
            if host_model is not None
            else attributable_from_root(
                host_path=key_host_path(arg), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_name,
            )
        )
        if not attributable:
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            return [leaf]
    return []


def _first_unattributable_attached_leaf(
    *, agg: AggregateKey, target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: str, bundle: ResolvedSourceBundle, host_model: SlayerModel,
) -> List[Tuple[str, str, str]]:
    """(input alias, dotted leaf, reason) of the first row leaf inside an attached input the root cannot reach — judged on the leaf's dependency closure (DEV-1900)."""
    for inp in attached_inputs(agg):
        for leaf in walk_value_keys(inp):
            if not isinstance(leaf, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
                continue
            hp = key_host_path(leaf)
            if key_attributable_from_root(
                key=leaf, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, bundle=bundle,
                host_model=host_model, host_name=host_name,
            ):
                continue
            name = (
                getattr(leaf, "leaf", None)
                or getattr(leaf, "column_name", None)
                or getattr(getattr(leaf, "column", None), "leaf", None)
                or "input"
            )
            return [(
                _constituent_alias(inp) or "input",
                ".".join([*hp, name]),
                broadcast_reason(
                    host_path=hp, target_path=target_path, root_model=root_model,
                    models_by_name=models_by_name, host_name=host_name,
                ),
            )]
    return []


def _assert_cross_model_inputs_safe(
    *, agg: AggregateKey, agg_rooted: AggregateKey, root_model: SlayerModel,
    root_name: str, target_path: Tuple[str, ...], bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
    host_model: Optional[SlayerModel] = None,
) -> None:
    """Resolve every cross-model input's attributability from its root; the checker raises on a fanning/unproven join or an unanalysable derived dependency."""
    alias = canonical_aggregate_alias(agg, profile="stage_formula")
    paths = _cross_model_input_paths(
        agg_rooted=agg_rooted, root_model=root_model, root_name=root_name,
        bundle=bundle,
    )
    if paths is None:
        check_input_dependencies_analyzable(
            alias=alias,
            column=first_unanalyzable_input_column(
                key=agg_rooted, anchor_model=root_model, anchor_relation=root_name,
                bundle=bundle,
            ),
        )
        paths = []
    unsafe_input_hops = _first_unsafe_input_hop(
        paths=paths, root_model=root_model, root_name=root_name,
        models_by_name=models_by_name,
    )
    unattributable_arg_leaves = [] if unsafe_input_hops else (
        _first_unattributable_arg_leaf(
            agg=agg, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
            bundle=bundle, host_model=host_model,
        )
    )
    check_cross_model_inputs_safe(
        alias=alias,
        root_name=root_name,
        unsafe_input_hops=unsafe_input_hops,
        unattributable_arg_leaves=unattributable_arg_leaves,
    )


def _cross_model_inherited_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
    host_model: Optional[SlayerModel] = None,
    bundle: Optional[ResolvedSourceBundle] = None,
) -> Tuple[List[BoundFilter], List[SemiJoinFilter], List[UnreachableFilterDroppedWarning]]:
    """Split base ROW filters into conjuncts and dispose each three ways: fully
    attributable → inherits inline (re-rooted); reachable across an unproven hop →
    pushed as a correlated EXISTS semi-join (one group per first reverse hop, D3);
    else dropped and warned."""
    inherited: List[BoundFilter] = []
    dropped: List[UnreachableFilterDroppedWarning] = []
    groups: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Dict[str, Any]] = {}
    for bf, text in base_filters:
        if bf.phase != Phase.ROW:
            continue
        for cj in split_top_level_and(bf.value_key):
            inherited_bf, pushed, dropped_w = _conjunct_disposition(
                cj, text=text, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_name,
                host_model=host_model, bundle=bundle,
            )
            if inherited_bf is not None:
                inherited.append(inherited_bf)
            elif pushed is not None:
                key_rewritten, conj_text, nodes = pushed
                first = next(h for p, h in nodes.items() if len(p) == 1)
                group = groups.setdefault(
                    (first.target_model, first.join_pairs),
                    {"nodes": {}, "conjuncts": [], "texts": []},
                )
                for path, hop in nodes.items():
                    group["nodes"].setdefault(path, hop)
                group["conjuncts"].append(key_rewritten)
                group["texts"].append(conj_text)
            else:
                dropped.append(dropped_w)
    semi_joins = [
        SemiJoinFilter(
            hops=sorted(g["nodes"].values(), key=lambda h: len(h.node_path)),
            conjuncts=g["conjuncts"],
            filter_texts=g["texts"],
        )
        for g in groups.values()
    ]
    return inherited, semi_joins, dropped


def _assert_local_producer_inputs_safe(
    *,
    agg: AggregateKey,
    host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel],
) -> None:
    """Resolve per-role crossing-input safety for a HOST-rooted producer answer; the checker raises on an unproven hop (host-grain wrap's SOURCE path exempt)."""
    def _safe(path: Tuple[str, ...]) -> bool:
        return safe_reachable(
            root=host_model, path=tuple(path), models_by_name=models_by_name,
        )

    # Role: crossed argument, explicitly named (a first/last ranking arg).
    ranked_crossings = []
    for arg in agg.args:
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        path = key_host_path(arg)
        if path and not _safe(path):
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            ranked_crossings.append((leaf, path[-1]))
            break  # first violation wins; the checker raises it

    # Crossed predicate + remaining crossed args; the SOURCE's own crossings are separate.
    gated_crossings: List[str] = []
    source_crossings: List[str] = []
    alias = canonical_aggregate_alias(agg, profile="stage_formula")
    if not ranked_crossings:
        gated = local_crossing_input_paths(
            key=agg, bundle=bundle, host_model=host_model, include_source=False,
        )
        if gated is None:
            check_input_dependencies_analyzable(
                alias=alias,
                column=first_unanalyzable_input_column(
                    key=agg, anchor_model=host_model, anchor_relation=host_model.name,
                    bundle=bundle, include_source=False,
                ),
            )
            gated = []
        gated_crossings = [p[-1] for p in gated if p and not _safe(p)]
        source_crossings = _source_crossings(
            agg=agg, host_model=host_model, bundle=bundle, alias=alias, safe=_safe,
        )
    check_local_producer_inputs_safe(
        alias=alias,
        host=host_model.name,
        ranked_crossings=ranked_crossings,
        gated_crossings=gated_crossings,
        source_crossings=source_crossings,
    )


def _source_crossings(
    *,
    agg: AggregateKey,
    host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    safe: Callable[[Tuple[str, ...]], bool],
) -> List[str]:
    """Unproven hops the expression source's own ROW leaves cross from the host (constituents opaque, Axiom 2.3; unanalysable fails closed, Axiom 2.8); a host-grain wrap is exempt."""
    if agg.locus == "host":
        return []
    src = source_row_leaf_closure(
        key=agg, anchor_model=host_model, anchor_relation=host_model.name, bundle=bundle,
    )
    if src is None:
        check_input_dependencies_analyzable(
            alias=alias,
            column=first_unanalyzable_source_row_leaf(
                key=agg, anchor_model=host_model, anchor_relation=host_model.name, bundle=bundle,
            ),
        )
        return []
    return [p[-1] for p in src if p and not safe(p)]


def _trailing_window_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
) -> TrailingWindowProducerKernel:
    window_raw = window_kwarg_of(agg_key)
    bucket_sid = producer_plan.active_time_dimension_slot_id
    bucket_slot = next(
        (s for s in producer_plan.row_slots if s.id == bucket_sid), None,
    )
    assert window_raw is not None and bucket_slot is not None, (
        "Windowed producer is missing its window duration or bucket slot; "
        "synthesis and planning disagree (DEV-1838)."
    )
    src_where_ids, src_rewrites = _plan_src_row_filters(
        producer_plan=producer_plan,
    )
    return TrailingWindowProducerKernel(
        window_raw=window_raw,
        window_parts=parse_window_duration(window_raw),
        window_granularity=bucket_slot.key.granularity,
        bucket_slot_id=bucket_sid,
        src_where_filter_ids=src_where_ids,
        src_filter_rewrites=src_rewrites,
    )


def _ranked_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> RankedProducerKernel:
    return RankedProducerKernel(
        agg=agg_key.agg,
        ranking_time_key=resolve_ranking_time_key(
            key=agg_key,
            root_model=root_model,
            bundle=bundle,
            row_keys=ordered_row_keys(
                row_slots=producer_plan.row_slots,
                public_projection=producer_plan.projection,
            ),
        ),
    )


def _synthesize_wrap_attach(
    *,
    wrap_key: AggregateKey,
    prebound: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    producer_source_model: Optional[str],
    row_attaches: Sequence[RegroupAttachPlan] = (),
) -> RegroupAttachPlan:
    """A host-grain ORDER-BY wrap as a HOST-rooted producer synthesized late: a combined attach at the full projected grain whose placeholder IS the wrap key."""
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    models_by_name = bundle.models_by_name
    if producer_model is not None:
        _assert_local_producer_inputs_safe(
            agg=wrap_key, host_model=producer_model, bundle=bundle,
            models_by_name=models_by_name,
        )
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
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
        pks=Grain.of(projected), aggs=[wrap_key], model=producer_model,
        bundle=bundle, inherited=inherited, n_date_range=n_inherited_date,
        partition_order=lambda pks: sorted(
            pks, key=lambda k: consumer_order.get(k, len(consumer_order)),
        ),
        grain_name_by_key=grain_name_by_key,
        to_many_handling=prebound.to_many_handling,
    )
    producer_plan = compile_synthesized(
        producer_prebound,
        source_model=producer_source_model,
        bundle=bundle,
        scope=scope,
        stage_schemas=stage_schemas,
        # A computed-dimension grain member — or an attach-owning wrapped answer
        # (DEV-1859 decision 11) — nests its own producer inside the wrap.
        enable_producer_regroups=_answers_need_nested_regroups([wrap_key]) or any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(pk)
            for pk in projected
        ),
        producer_registry=producer_registry,
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
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=canonical_aggregate_alias(wrap_key, profile="stage_formula"),
        attach_phase="combined",
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=wrap_key, producer_slot_id=answer_slot,
            original_key=wrap_key,
        )],
        partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
    )


class _PushBlocked(Exception):
    """A conjunct outside semi-join pushdown scope; the message is the warning reason."""


def _owning_model(
    name: str, *, host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel],
) -> Optional[SlayerModel]:
    if host_model is not None and name == host_model.name:
        return host_model
    return models_by_name.get(name)


def _ref_sql_dependency_paths(
    col: ValueKey, *, host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel], bundle: Optional[ResolvedSourceBundle],
) -> Tuple[Tuple[str, ...], ...]:
    """Owner-relative join paths a derived column's ``Column.sql`` AND its
    ``Column.filter`` (DEV-1832) cross (the dependency closure at the owner) — the
    semi-join push tree registers a hop for each. ``bundle.models_by_name`` is
    host-inclusive (DEV-1900), so a dep pointing back at the host resolves without
    a hand-patched bundle."""
    if not isinstance(col, ColumnSqlKey) or bundle is None:
        return ()
    owner = _owning_model(
        col.model, host_model=host_model, models_by_name=models_by_name,
    )
    if owner is None:
        return ()
    column = next((c for c in owner.columns if c.name == col.column_name), None)
    if column is None:
        return ()
    paths: List[Tuple[str, ...]] = []
    for sql in (column.sql, column.filter):
        if not sql:
            continue
        frag = fragment_closure(
            sql=sql, model=owner, owner_path=(),
            anchor_relation=owner.name, bundle=bundle,
        )
        if frag:
            paths.extend(frag)
    return tuple(dict.fromkeys(paths))


def _path_edges_exist(
    model: SlayerModel, path: Tuple[str, ...],
    models_by_name: Dict[str, SlayerModel],
) -> bool:
    current = model
    for name in path:
        try:
            edge = resolve_hop(
                current=current, token=name, models_by_name=models_by_name,
            )
        except AmbiguousJoinPathError:
            return False
        nxt = models_by_name.get(edge.target_model) if edge is not None else None
        if edge is None or nxt is None:
            return False
        current = nxt
    return True


def _register_hop(
    nodes: Dict[Tuple[str, ...], SemiJoinHop], *, node_path: Tuple[str, ...],
    target_model: str, pairs: List[Tuple[str, str]],
) -> None:
    nodes.setdefault(node_path, SemiJoinHop(
        target_model=target_model,
        join_pairs=tuple((s, t) for s, t in pairs),
        node_path=node_path,
    ))


def _forward_hops(
    *, start_model: SlayerModel, rel_path: Tuple[str, ...],
    base_node_path: Tuple[str, ...], models_by_name: Dict[str, SlayerModel],
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> Tuple[str, ...]:
    """Register hops along ``rel_path`` through the shared walker (reverse hops
    and edge-name tokens included); returns the final node path. The node-path
    token stays as-typed for hop-alias identity while the hop's ``target_model``
    is the resolved model. An ambiguous hop raises (fail closed)."""
    current = start_model
    node_path = base_node_path
    for hop_name in rel_path:
        edge = resolve_hop(
            current=current, token=hop_name, models_by_name=models_by_name,
        )
        target = models_by_name.get(edge.target_model) if edge is not None else None
        if edge is None or target is None:
            raise _PushBlocked(
                f"unreachable from the aggregate's root (no join edge from "
                f"{current.name} to {hop_name})"
            )
        node_path = (*node_path, hop_name)
        _register_hop(
            nodes, node_path=node_path, target_model=edge.target_model,
            pairs=[(s, t) for s, t in edge.join_pairs],
        )
        current = target
    return node_path


def _reverse_hops(
    *, target_path: Tuple[str, ...],
    host_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> Tuple[str, ...]:
    """Register the reverse chain root → … → host by inverting the forward walk
    host → … → root along ``target_path`` — so an edge-name token correlates
    through the exact edge the aggregate's path selected, never a re-parsed
    model name. An ambiguous forward hop raises (fail closed in both modes);
    an unresolvable one blocks the push. Returns the host node's path."""
    fwd = walk(root=host_model, path=target_path, models_by_name=models_by_name)
    if fwd is None:
        raise _PushBlocked(
            f"unreachable from the aggregate's root (join path "
            f"{'.'.join(target_path)!r} does not resolve from "
            f"{host_model.name})"
        )
    node_path: Tuple[str, ...] = ()
    for edge in reversed(fwd):
        node_path = (*node_path, edge.source_model)
        _register_hop(
            nodes, node_path=node_path, target_model=edge.source_model,
            pairs=[(tgt, src) for src, tgt in edge.join_pairs],
        )
    return node_path


def _remap_ref_path(r: ValueKey, node_path: Tuple[str, ...]) -> ValueKey:
    if isinstance(r, TimeTruncKey):
        return r.model_copy(update={
            "column": r.column.model_copy(update={"path": node_path}),
        })
    return r.model_copy(update={"path": node_path})


def _child_keys(k: ValueKey) -> List[Any]:
    """The nested operand keys of a composite ``ValueKey`` node. Asymmetric on
    purpose: aggregates/transforms/truncs are scope boundaries here, not
    ``children()`` — the push-down classifies whole refs."""
    if isinstance(k, ArithmeticKey):
        return list(k.operands)
    if isinstance(k, ScalarCallKey):
        return list(k.args)
    if isinstance(k, BetweenKey):
        return [k.column, k.low, k.high]
    if isinstance(k, InKey):
        return [k.column]
    return []


def _reject_mixed_or_not(cj: ValueKey, cross_by_ref: Dict[ValueKey, bool]) -> None:
    """D2: an OR/NOT subtree mixing a root-anchored ref with a cross-path ref
    changes meaning under EXISTS — such a conjunct stays dropped."""

    def _walk(k) -> Tuple[bool, bool]:  # (has_local, has_cross)
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey)):
            cross = cross_by_ref.get(k, False)
            return (not cross, cross)
        has_local = has_cross = False
        for child in _child_keys(k):
            if isinstance(child, (Decimal, str, bool, int, float)) or child is None:
                continue
            local, cross = _walk(child)
            has_local, has_cross = has_local or local, has_cross or cross
        if (
            isinstance(k, ArithmeticKey)
            and k.op.lower() in ("or", "not")
            and has_local and has_cross
        ):
            raise _PushBlocked(
                "mixes a root-anchored predicate with a cross-path predicate "
                "under OR/NOT, which a semi-join cannot preserve"
            )
        return has_local, has_cross

    _walk(cj)


def _resolve_ref_anchor(
    hp: Tuple[str, ...], *, tp: Tuple[str, ...], root_model: SlayerModel,
    host_model: SlayerModel, lookup: Dict[str, SlayerModel],
    nodes: Dict[Tuple[str, ...], SemiJoinHop], host_name: Optional[str],
    host_node: Optional[Tuple[str, ...]],
) -> Tuple[SlayerModel, Tuple[str, ...], Tuple[str, ...],
           Optional[Tuple[str, ...]]]:
    """Where one cross-path ref anchors in the correlation tree:
    ``(base model, base node, relative path, host node)``."""
    if hp[: len(tp)] == tp:
        return root_model, (), hp[len(tp):], host_node
    if host_name is None or (tp and host_name == tp[0]):
        raise _PushBlocked(UNREACHABLE_NO_PATH)
    if hp and _path_edges_exist(root_model, hp, lookup):
        return root_model, (), hp, host_node
    if host_node is None:
        host_node = _reverse_hops(
            target_path=tp,
            host_model=host_model, models_by_name=lookup, nodes=nodes,
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
        # ``host_node`` carries walked MODEL names (reversed), so index it
        # rather than a token lookup — ``tp`` tokens may be edge names.
        return (
            lookup[host_node[len(tp) - shared - 1]],
            host_node[: len(tp) - shared],
            hp[shared:], host_node,
        )
    return host_model, host_node, hp, host_node


def _register_dep_hops(
    col: ValueKey, *, node_path: Tuple[str, ...], host_model: SlayerModel,
    lookup: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> bool:
    """Register the hops a ref's Mode-A dependencies cross; True when any."""
    dep_rels = _ref_sql_dependency_paths(
        col, host_model=host_model, models_by_name=lookup, bundle=bundle,
    )
    if not dep_rels:
        return False
    owner = _owning_model(
        col.model, host_model=host_model, models_by_name=lookup,
    )
    for dep_rel in dep_rels:
        _forward_hops(
            start_model=owner, rel_path=tuple(dep_rel),
            base_node_path=node_path, models_by_name=lookup, nodes=nodes,
        )
    return True


def _conjunct_push_plan(
    cj: ValueKey, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    host_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    bundle: ResolvedSourceBundle, host_name: Optional[str],
) -> Tuple[ValueKey, Dict[Tuple[str, ...], SemiJoinHop]]:
    """Resolve a not-fully-attributable conjunct into a correlation-tree plan:
    the conjunct rewritten into producer-root coordinates (ref paths = tree-node
    paths, root-local refs correlate as outer references) plus the hop registry.
    Raises :class:`_PushBlocked` when outside pushdown scope (D2/D4)."""
    tp = tuple(target_path)
    lookup = dict(models_by_name)
    lookup.setdefault(host_model.name, host_model)
    nodes: Dict[Tuple[str, ...], SemiJoinHop] = {}
    mapping: Dict[ValueKey, ValueKey] = {}
    cross_by_ref: Dict[ValueKey, bool] = {}
    host_node: Optional[Tuple[str, ...]] = None
    for r in walk_value_keys(cj):
        if not isinstance(r, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey)):
            continue
        col = r.column if isinstance(r, TimeTruncKey) else r
        hp = tuple(getattr(col, "path", ()) or ())
        base_model, base_node, rel, host_node = _resolve_ref_anchor(
            hp, tp=tp, root_model=root_model, host_model=host_model,
            lookup=lookup, nodes=nodes, host_name=host_name,
            host_node=host_node,
        )
        node_path = _forward_hops(
            start_model=base_model, rel_path=rel, base_node_path=base_node,
            models_by_name=lookup, nodes=nodes,
        )
        has_deps = _register_dep_hops(
            col, node_path=node_path, host_model=host_model, lookup=lookup,
            bundle=bundle, nodes=nodes,
        )
        cross_by_ref[r] = bool(node_path) or has_deps
        remapped = _remap_ref_path(r, node_path)
        if remapped != r:
            mapping[r] = remapped
    first_hops = [p for p in nodes if len(p) == 1]
    if len(first_hops) != 1:
        raise _PushBlocked(
            "its cross-path references span multiple join branches from "
            f"{root_model.name}, so no single semi-join tree covers them"
        )
    _reject_mixed_or_not(cj, cross_by_ref)
    return substitute_value_keys(cj, mapping), nodes


def _conjunct_disposition(
    cj: ValueKey, *, text: Optional[str], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str], host_model: Optional[SlayerModel] = None,
    bundle: Optional[ResolvedSourceBundle] = None,
) -> Tuple[
    Optional[BoundFilter],
    Optional[Tuple[ValueKey, Optional[str], Dict[Tuple[str, ...], SemiJoinHop]]],
    Optional[UnreachableFilterDroppedWarning],
]:
    """Three-way ROW-conjunct disposition (D1): inline / semi-join pushed / dropped."""
    refs = [
        k for k in walk_value_keys(cj)
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey))
    ]

    def _attributable(r: ValueKey) -> bool:
        # Judge each ref on its dependency closure (DEV-1900) — a derived column
        # crossing a fanning hop is unattributable exactly as a structural one is.
        if host_model is not None and bundle is not None:
            return key_attributable_from_root(
                key=r, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, bundle=bundle,
                host_model=host_model, host_name=host_name,
            )
        return attributable_from_root(
            host_path=key_host_path(r), target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_name=host_name,
        )

    unsafe = next((r for r in refs if not _attributable(r)), None)
    if unsafe is None:
        rerooted = (
            reroot_from_root(
                cj, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_name,
            )
            if host_name is not None
            else reroot_value_key(cj, target_path=target_path)
        )
        return bound_filter_from_key(rerooted), None, None
    display = text or _canonical_name(cj)
    if host_model is not None and bundle is not None:
        try:
            key_rewritten, nodes = _conjunct_push_plan(
                cj, target_path=target_path, root_model=root_model,
                host_model=host_model, models_by_name=models_by_name,
                bundle=bundle, host_name=host_name,
            )
            return None, (key_rewritten, display, nodes), None
        except _PushBlocked as exc:
            return None, None, UnreachableFilterDroppedWarning(
                filter_text=display, reason=str(exc),
            )
    reason = broadcast_reason(
        host_path=key_host_path(unsafe), target_path=target_path,
        root_model=root_model, models_by_name=models_by_name,
    )
    return None, None, UnreachableFilterDroppedWarning(
        filter_text=display, reason=reason,
    )


class _ProducerSynthesisContext(BaseModel):
    """The per-plan inputs every cross-model producer synthesis shares."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    prebound: PreboundQuery
    bundle: ResolvedSourceBundle
    host_model: SlayerModel
    models_by_name: Dict[str, SlayerModel]
    projected_dim_keys: List[ValueKey]
    projected_td_keys: List[ValueKey]
    base_filters_with_text: List[Tuple[BoundFilter, Optional[str]]]
    scope: Union[ModelScope, StageSchema]
    stage_schemas: Dict[str, StageSchema]
    # Home path per aggregate (Axiom 2), resolved in the elaborator and read
    # here; the source anchor is the fallback for keys with no term.
    home_paths: Dict[ValueKey, Tuple[str, ...]] = {}

    def home_of(self, agg: AggregateKey) -> Tuple[str, ...]:
        return self.home_paths.get(agg, source_anchor_path(agg.source))


class _UnattributableDim(NamedTuple):
    """A requested grain dimension not attributable from the aggregate's root:
    its display name, broadcast reason, and whether a join path reaches it
    (fanning) or none does (truly unreachable)."""

    key: ValueKey
    name: str
    reason: str
    reachable: bool


def _synthesize_cross_model_producer(  # NOSONAR(S3776) — one cohesive target-rooted producer synthesis (root / safe-grain / broadcast / inputs / filter-inheritance / recursive plan / attach); the arms share the re-rooting coordinate state.
    *,
    agg: AggregateKey,
    placeholder: ValueKey,
    attach_phase: str,
    public_alias: Optional[str],
    context: _ProducerSynthesisContext,
    declared_type: Optional[DataType] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> RegroupAttachPlan:
    """Build one target-rooted regroup producer for a cross-model aggregate: root at its source, compute at the fan-out-safe grain subset, broadcast the rest."""
    prebound, bundle = context.prebound, context.bundle
    host_model, models_by_name = context.host_model, context.models_by_name
    projected_dim_keys = context.projected_dim_keys
    projected_td_keys = context.projected_td_keys
    base_filters_with_text = context.base_filters_with_text
    scope, stage_schemas = context.scope, context.stage_schemas
    target_path = context.home_of(agg)
    root_model = walk_key_path(model=host_model, path=target_path, bundle=bundle)
    if root_model is None:  # pragma: no cover — bind resolved the path already
        check_cross_model_source_resolves(
            target_path=target_path, host_name=host_model.name,
        )
    root_name = root_model.name
    alias = public_alias or canonical_aggregate_alias(agg, profile="stage_formula")
    assert alias is not None  # a projected cross-model aggregate always names one

    # Requested grain G: explicit partition_by else the query dimensions.
    if agg.partition_keys is not None:
        requested = list(agg.partition_keys)
        explicit = True
    else:
        requested = [*projected_dim_keys, *projected_td_keys]
        explicit = False

    mode = context.prebound.to_many_handling
    # Host included so the reverse (fanning) hop resolves for the broadcast
    # reason; attributability/filter routing keep the host-free map unchanged.
    models_with_host = {**models_by_name, host_model.name: host_model}

    # Safe grain S (attributable from R) vs unattributable; each unattributable
    # dim carries whether it is reachable (fanning) or truly unreachable.
    safe_pairs: List[Tuple[ValueKey, ValueKey]] = []  # (host_key, rerooted_key)
    unattributable: List[_UnattributableDim] = []
    for g in requested:
        shared = shared_join_key_reroot(
            key=g, target_path=target_path, host_model=host_model,
            models_by_name=models_by_name,
        )
        if shared is not None:
            # The join-key identity needs no join in the producer.
            safe_pairs.append((g, shared))
        elif grain_member_attributable(
            key=g, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, bundle=bundle, host_model=host_model,
            host_name=host_model.name,
        ):
            safe_pairs.append((g, reroot_from_root(
                g, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            )))
        else:
            reason = key_broadcast_reason(
                key=g, target_path=target_path, root_model=root_model,
                models_by_name=models_with_host, bundle=bundle,
                host_model=host_model, host_name=host_model.name,
            )
            unattributable.append(_UnattributableDim(
                key=g, name=_regroup_grain_name(g), reason=reason,
                reachable=reason != UNREACHABLE_NO_PATH,
            ))

    # Arm-specific state shared into the common tail. Associate also when only an
    # attached INPUT (not a dimension) is unattributable from the home — its producer
    # then nests per home entity rather than the broadcast arm refusing it (DEV-1832).
    attached_leaf = _first_unattributable_attached_leaf(
        agg=agg, target_path=target_path, root_model=root_model,
        models_by_name=models_by_name, host_name=host_model.name,
        bundle=bundle, host_model=host_model,
    )
    associate = mode == "associate" and (bool(unattributable) or bool(attached_leaf))
    window_td_key: Optional[ValueKey] = None
    semi_joins: List[SemiJoinFilter] = []
    broadcast: List[Tuple[str, str]] = []
    picked_params: List[PickedParam] = []
    restricted_texts: List[str] = []
    present_keys: List[ValueKey] = []
    entity_keys_root: List[ValueKey] = []
    associated_measure: Optional[str] = None
    associated_dimensions: List[str] = []

    if associate:
        # ASSOCIATION ARM (D2-4, 8): root at the home, keep every unattributable
        # dimension as a rerooted grain member joined back on the host key, and
        # dedup per home entity via the association kernel — a home entity absent
        # from the population still counts in the cells its own path reaches.
        agg_rooted, picked_params, entity_keys_root, present_keys, assoc_pairs = (
            _association_arm(
                agg=agg, alias=alias, root_model=root_model,
                target_path=target_path, unattributable=unattributable,
                host_model=host_model, models_by_name=models_by_name,
                bundle=bundle,
            )
        )
        # The unattributable dims join back on the host key exactly like safe_pairs.
        safe_pairs = [*safe_pairs, *assoc_pairs]
        inherited, restricted_texts, dropped = _association_inline_filters(
            base_filters=base_filters_with_text, target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_model=host_model, bundle=bundle,
        )
        # No unattributable dimension (associate triggered by an attached input only)
        # means no association warning — nothing degraded per dimension.
        associated_measure = None if (explicit or not unattributable) else alias
        associated_dimensions = (
            [] if explicit else [u.name for u in unattributable]
        )
    else:
        # BROADCAST / ERROR ARM: an unattributable explicit key is a hard error.
        check_cross_model_partition_keys_attributable(
            alias=alias, root_name=root_name, explicit=explicit,
            unattributable=[(u.name, u.reason) for u in unattributable],
        )
        broadcast = [(u.name, u.reason) for u in unattributable]
        # Attached inputs nest as producers rooted here; error mode's dimension refusal wins.
        if mode != "error" or not unattributable:
            check_attached_inputs_attributable(
                alias=alias, root_name=root_name, mode=mode,
                unattributable=attached_leaf,
            )

        if target_path != source_anchor_path(agg.source):
            # The source sits beyond the home; re-anchor off-home inputs via the host
            # and render it inline as a host-locus aggregate joining the to-one path
            # from the home, never a source-rooted producer.
            agg_rooted = reroot_from_root(
                key=agg, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            ).model_copy(update={"locus": "host"})
        else:
            agg_rooted = reroot_value_key(key=agg, target_path=target_path)
        _assert_cross_model_inputs_safe(
            agg=agg, agg_rooted=agg_rooted, root_model=root_model, root_name=root_name,
            target_path=target_path, bundle=bundle, models_by_name=models_by_name,
            host_name=host_model.name, host_model=host_model,
        )

        # A windowed cross-model aggregate folds the active TD into its grain as the bucket (must be attributable from the root).
        if window_kwarg_of(agg) is not None:
            active_td = prebound.main_time_key
            check_windowed_cross_model_time_axis(
                alias=alias, root_name=root_name,
                active_td_name=(
                    None if active_td is None else _regroup_grain_name(active_td)
                ),
                attributable=active_td is not None and attributable_from_root(
                    host_path=key_host_path(active_td), target_path=target_path,
                    root_model=root_model, models_by_name=models_by_name,
                    host_name=host_model.name,
                ),
            )
            assert active_td is not None  # the checker raised otherwise
            window_td_key = reroot_from_root(
                key=active_td, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            )

        inherited, semi_joins, dropped = _cross_model_inherited_filters(
            base_filters=base_filters_with_text, target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_name=host_model.name, host_model=host_model, bundle=bundle,
        )

    # SHARED TAIL: root at the home, compile the producer, attach on the host key.
    assert isinstance(agg_rooted, AggregateKey)  # both arms reroot an aggregate
    root_bundle = bundle.rerooted(root_model)
    root_scope = (
        ModelScope(source_model=root_model)
        if isinstance(scope, ModelScope) else scope
    )
    host_by_rerooted = {rr: hk for hk, rr in safe_pairs}
    if window_td_key is not None:
        # The bucket joins back on the consumer's own active TD.
        host_by_rerooted.setdefault(window_td_key, prebound.main_time_key)
    grain_keys = Grain.of(rr for _, rr in safe_pairs)
    # The producer measure keeps the CANONICAL alias (root columns could shadow the public name).
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=grain_keys, aggs=[agg_rooted], model=root_model, bundle=root_bundle,
        inherited=inherited, n_date_range=0, window_td_key=window_td_key,
        explicit_types=(
            {agg_rooted: declared_type} if declared_type is not None else None
        ),
        to_many_handling=prebound.to_many_handling,
    )
    # A computed-dimension grain member, windowed producer, or attach-owning answer
    # (mixed source / attached parameter, DEV-1859) re-enables discovery so the
    # producer row-attaches those inputs.
    enable_nested = (
        window_td_key is not None
        or _answers_need_nested_regroups([agg_rooted])
        or any(
            isinstance(rr, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(rr)
            for rr in grain_keys
        )
    )
    producer_plan = compile_synthesized(
        producer_prebound,
        source_model=root_name,
        bundle=root_bundle, scope=root_scope,
        stage_schemas=stage_schemas,
        enable_producer_regroups=enable_nested,
        producer_registry=producer_registry,
    )
    if semi_joins:
        producer_plan = producer_plan.model_copy(
            update={"semi_join_filters": semi_joins},
        )
    # An attached parameter row-attached inside the producer: map each picked key
    # through the sub-plan's substitutions so the level-1 pick references the
    # producer's row-attach column, not the raw aggregate (DEV-1859 decision 13).
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
    cm_attach_kwargs: Dict[str, Any] = {}
    if associate:
        cm_attach_kwargs["kernel"] = AssociationProducerKernel(
            entity_keys=entity_keys_root, picked_params=picked_params,
            present_keys=present_keys,
        )
    elif window_td_key is not None:
        cm_attach_kwargs["kernel"] = _trailing_window_kernel(
            producer_plan=producer_plan, agg_key=agg_rooted,
        )
    elif isinstance(agg_rooted, AggregateKey) and agg_rooted.agg in RANKED_AGGREGATIONS:
        cm_attach_kwargs["kernel"] = _ranked_kernel(
            producer_plan=producer_plan, agg_key=agg_rooted,
            root_model=root_model, bundle=root_bundle,
        )
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=canonical_aggregate_alias(agg, profile="stage_formula"),
        attach_phase=attach_phase,
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=placeholder, producer_slot_id=answer_slot,
            original_key=agg,
        )],
        partition_display=[_regroup_grain_name(rr) for rr in ordered_pks],
        producer_root_model=root_name,
        dropped_filter_warnings=dropped,
        broadcast_measure=alias if broadcast else None,
        broadcast_dimensions=broadcast,
        associated_measure=associated_measure,
        associated_dimensions=associated_dimensions,
        association_restricted_filter_texts=restricted_texts,
        **cm_attach_kwargs,
    )


def _param_is_determined(
    *, spec: ParamSpec, grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> bool:
    """A parameter is legal iff the dataset grain determines it — the bound key,
    or (for an expression default) every column it references — judged on each
    reference's dependency closure (DEV-1900). An UNGRAINED aggregate parameter
    types at the query grain, which the operand grain always refines, so it is
    determined (DEV-1859 decision 12)."""
    if spec.key is not None:
        if isinstance(spec.key, AggregateKey) and spec.key.partition_keys is None:
            return True
        return grain_determines(
            key=spec.key, grain=grain, host_model=host_model,
            models_by_name=models_by_name, bundle=bundle,
        )
    return all(
        k is not None and grain_determines(
            key=k, grain=grain, host_model=host_model,
            models_by_name=models_by_name, bundle=bundle,
        )
        for k in spec.expr_refs
    )


def _grain_display(grain: Grain) -> str:
    """A readable grain listing for a parameter-typing error."""
    names = [_regroup_grain_name(g) for g in _regroup_partition_order(grain)]
    return ", ".join(names) if names else "the grand total"


def _association_arm(
    *, agg: AggregateKey, alias: str, root_model: SlayerModel,
    target_path: Tuple[str, ...], unattributable: List[_UnattributableDim],
    host_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    bundle: ResolvedSourceBundle,
) -> Tuple[
    ValueKey, List[PickedParam], List[ValueKey], List[ValueKey],
    List[Tuple[ValueKey, ValueKey]],
]:
    """The home-rooted association arm (DEV-1910 D2-3): eligibility + mode-invariant
    input safety; the rerooted host-locus aggregate (compiled inline at its fanning
    grain, its level-1 dedup removing the fan-out); the kernel entity keys in ROOT
    coordinates and the parameters the entity grain picks, rerooted into the home;
    the reverse-hop presence guard; and each unattributable dimension rerooted to
    join back on the host key exactly like ``safe_pairs``."""
    check_association_windowed_ranked(
        alias=alias,
        windowed_or_ranked=window_kwarg_of(agg) is not None or (
            isinstance(agg, AggregateKey) and agg.agg in RANKED_AGGREGATIONS
        ),
    )
    key_sets = _unique_key_sets(root_model)
    check_association_root_unique_key(
        alias=alias, root_name=root_model.name, has_unique_key=bool(key_sets),
    )
    # Input safety is mode-invariant — an input crossing an unproven/fanning hop is
    # not constant per home entity. An attached input row-attaches its own nested
    # producer (DEV-1859 decision 13), so strip it from the hop check while keeping
    # it in ``agg`` for parameter typing and the producer compile.
    _safety_rooted = agg
    if attached_inputs(agg):
        _safety_rooted = substitute_value_keys(
            key=agg,
            mapping={a: LiteralKey(value=Decimal(1)) for a in attached_inputs(agg)})
    agg_rooted = reroot_from_root(
        key=agg, target_path=target_path, root_model=root_model,
        models_by_name=models_by_name, host_name=host_model.name,
    ).model_copy(update={"locus": "host"})
    _assert_cross_model_inputs_safe(
        agg=agg, agg_rooted=reroot_from_root(
            key=_safety_rooted, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ).model_copy(update={"locus": "host"}),
        root_model=root_model, root_name=root_model.name, target_path=target_path,
        bundle=bundle, models_by_name=models_by_name, host_name=host_model.name,
        host_model=host_model,
    )
    # Type each GRAINED parameter against the ENTITY grain in HOST coordinates (the
    # aggregation reads it once per associated entity); an ungrained parameter types
    # at the query grain and is always determined (DEV-1859 decision 12). Lift the
    # legal ones, rerooted into the home so the level-1 pick reads them there; the
    # kernel dedups by the entity key in ROOT coordinates.
    host_entity_keys: List[ValueKey] = [
        ColumnKey(path=target_path, leaf=col) for col in key_sets[0]
    ]
    entity_keys_root: List[ValueKey] = [
        ColumnKey(path=(), leaf=col) for col in key_sets[0]
    ]
    # An expression source has no ``.path``; ``key_host_path`` would silently answer
    # the root, so the source anchor names where the definition is resolved (D1).
    source_path = source_anchor_path(agg.source)
    source_model = walk_key_path(
        model=host_model, path=source_path, bundle=bundle,
    ) or root_model
    rel_source = (
        source_path[len(target_path):]
        if source_path[: len(target_path)] == target_path else source_path
    )
    entity_grain = Grain.of(host_entity_keys)
    picked_params: List[PickedParam] = []
    for _ps in resolve_aggregation_params(
        agg=agg, owner_model=source_model, owner_path=source_path, bundle=bundle,
    ):
        check_parameter_determined(
            alias=alias, param_name=_ps.name,
            grain_display=_grain_display(entity_grain),
            determined=_param_is_determined(
                spec=_ps, grain=entity_grain, host_model=host_model,
                models_by_name=models_by_name, bundle=bundle,
            ),
        )
        picked_params.append(PickedParam(
            name=_ps.name,
            key=(reroot_from_root(
                key=_ps.key, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            ) if _ps.key is not None else None),
            sql=_ps.expr_sql, anchor_path=tuple(rel_source),
        ))
    assoc_pairs = [
        (u.key, reroot_from_root(
            key=u.key, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ))
        for u in unattributable
    ]
    present_keys = _association_present_keys(
        unattributable=unattributable, target_path=target_path,
        root_model=root_model, host_model=host_model, models_by_name=models_by_name,
        bundle=bundle,
    )
    return agg_rooted, picked_params, entity_keys_root, present_keys, assoc_pairs


def _association_inline_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Tuple[List[BoundFilter], List[str], List[UnreachableFilterDroppedWarning]]:
    """Route each ROW conjunct for a home-rooted association producer (DEV-1910
    D4): attributable → inline re-rooted; reachable-but-unsafe → inline the
    re-rooted conjunct too (the per-entity dedup makes the fanning join harmless)
    with its text kept for the informational entry; out of scope → dropped and
    warned. No semi-join is emitted, so membership equals the semi-join semantics
    and a conjunct sharing a hop with an association dimension binds to the same
    related row."""
    inherited: List[BoundFilter] = []
    restricted_texts: List[str] = []
    dropped: List[UnreachableFilterDroppedWarning] = []
    for bf, text in base_filters:
        if bf.phase != Phase.ROW:
            continue
        for cj in split_top_level_and(bf.value_key):
            inh, pushed, drop_w = _conjunct_disposition(
                cj=cj, text=text, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
                host_model=host_model, bundle=bundle,
            )
            if inh is not None:
                inherited.append(inh)
            elif pushed is not None:
                inherited.append(bound_filter_from_key(reroot_from_root(
                    key=cj, target_path=target_path, root_model=root_model,
                    models_by_name=models_by_name, host_name=host_model.name,
                )))
                if pushed[1] is not None:
                    restricted_texts.append(pushed[1])
            else:
                assert drop_w is not None  # the disposition's third arm
                dropped.append(drop_w)
    return inherited, restricted_texts, dropped


def _association_present_keys(
    *, unattributable: List[_UnattributableDim], target_path: Tuple[str, ...],
    root_model: SlayerModel, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> List[ValueKey]:
    """The reverse hop's host-side join columns in the home-rooted producer's
    coordinates (path = the reverse path), guarded NOT NULL in level 1 so a
    dimension the home reaches only back through the population root associates an
    entity only when a population row carries it (DEV-1910 D3). Empty when home ==
    host, or when no unattributable dimension reaches back through the reverse hop
    (a home-side dimension keeps its own NULL cell, as the population computes it)."""
    if not target_path:
        return []
    back = _back_path(
        host_name=host_model.name,
        target_path=target_path, models_by_name=models_by_name,
    )
    try:
        first_hop = resolve_hop(
            current=host_model, token=target_path[0], models_by_name=models_by_name,
        )
    except AmbiguousJoinPathError:
        first_hop = None
    if first_hop is None:
        return []

    def _reaches_back(u: _UnattributableDim) -> bool:
        # The dimension's own structural position (a base column like orders.status
        # reroots under the reverse path).
        if any(
            isinstance(r, (ColumnKey, ColumnSqlKey, TimeTruncKey))
            and key_host_path(r)[: len(back)] == back
            for r in walk_value_keys(reroot_from_root(
                key=u.key, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            ))
        ):
            return True
        # A derived column carries its dependencies in its SQL, not its structural
        # key: expand the full dependency set so a home-local derived dim whose
        # definition crosses back is guarded too. An unanalysable closure fails closed.
        closure = key_closure(
            key=u.key, anchor_model=host_model, anchor_relation=host_model.name,
            bundle=bundle,
        )
        if closure is None:
            return True
        return any(
            key_host_path(reroot_from_root(
                key=ColumnKey(path=p, leaf=""), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_model.name,
            ))[: len(back)] == back
            for p in closure
        )

    if not any(_reaches_back(u) for u in unattributable):
        return []
    return [ColumnKey(path=back, leaf=src) for src, _ in first_hop.join_pairs]


def _substitute_prebound(
    prebound: PreboundQuery, mapping: Mapping[ValueKey, ValueKey],
) -> PreboundQuery:
    """Substitute value keys across a prebound's measures / filters / orders."""
    return prebound.model_copy(update={
        "declared_measures": [
            dm.model_copy(update={"bound": BoundExpr(
                value_key=substitute_value_keys(dm.bound.value_key, mapping),
            )})
            for dm in prebound.declared_measures
        ],
        "bound_filters": [
            substitute_in_bound_filter(bf, mapping) for bf in prebound.bound_filters
        ],
        "order_specs": [
            sp.model_copy(update={"bound": BoundExpr(
                value_key=substitute_value_keys(sp.bound.value_key, mapping),
            )})
            for sp in prebound.order_specs
        ],
    })


def _answers_need_nested_regroups(answers: Iterable[ValueKey]) -> bool:
    """A producer re-runs regroup discovery when any answer is a transform or owns
    attached inputs — a re-aggregation constituent or a row-attached input
    (DEV-1859 decision 11). Each caller ORs its own grain-key / windowed clause."""
    return any(
        isinstance(a, TransformKey) or bool(attached_inputs(a)) for a in answers
    )


def _discover_roots(
    prebound: PreboundQuery, *, predicate: Callable[[ValueKey], TypeGuard[AggregateKey]],
) -> List[AggregateKey]:
    """Roots satisfying ``predicate`` reachable from any measure / order / filter,
    first-seen. Opaque below a matched root (its constituents belong to its own
    producer) and below any attach-owning root's inputs, still descending
    partition keys (walk_consumer_keys semantics, DEV-1859 decision 9)."""
    seen: set = set()
    out: List[AggregateKey] = []

    def _scan(vk: ValueKey) -> None:
        if predicate(vk):
            if vk not in seen:
                seen.add(vk)
                out.append(vk)
            return
        if isinstance(vk, AggregateKey) and attached_inputs(vk):
            for pk in (vk.partition_keys or ()):
                _scan(pk)
            return
        for c in vk.children():
            _scan(c)

    roots = [
        *(dm.bound.value_key for dm in prebound.declared_measures),
        *(sp.bound.value_key for sp in prebound.order_specs),
        *(bf.value_key for bf in prebound.bound_filters),
    ]
    for vk in roots:
        _scan(vk)
    return out


def _non_aggregate_leaf_check(
    key: ValueKey, *, ok: Callable[[ValueKey], bool],
) -> bool:
    """Every column-ish leaf OUTSIDE embedded aggregates satisfies ``ok``;
    unknown leaf kinds fail closed."""
    if isinstance(key, (AggregateKey, LiteralKey)):
        return True
    if isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        return ok(key)
    if isinstance(key, (ScalarCallKey, ArithmeticKey, InKey)):
        return all(_non_aggregate_leaf_check(c, ok=ok) for c in key.children())
    return False


def _grain_expression_determined(
    *, key: ValueKey, union_grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> bool:
    """An attach-carrying computed dimension is a function of the operand's
    union-grain cell when every embedded aggregate is grained by a subset of it
    and every leaf outside the aggregates is itself determined (DEV-1847)."""
    aggs = operand_aggregates(key)
    if not aggs or isinstance(key, AggregateKey):
        return False
    for a in aggs:
        if a.partition_keys is None or not all(
            pk in union_grain for pk in a.partition_keys
        ):
            return False
    return _non_aggregate_leaf_check(key, ok=lambda leaf: (
        leaf in union_grain or _reaggregation_determined(
            key=leaf, union_grain=union_grain, host_model=host_model,
            models_by_name=models_by_name, bundle=bundle,
        )
    ))


def _reaggregation_determined(
    *, key: ValueKey, union_grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> bool:
    """Is an outer dimension determined by the operand dataset's union grain?
    Delegates to the one determination rule: a grain member, or a
    column reached over provably to-one hops from a model the grain pins."""
    return grain_determines(
        key=key, grain=union_grain, host_model=host_model,
        models_by_name=models_by_name, bundle=bundle,
    )


def _constituent_alias(c: ValueKey) -> str:
    """A clean stage alias for a re-aggregation constituent: an aggregate's
    canonical alias (else its ``.agg``), a transform's ``.op`` (a transform has no
    canonical aggregate alias of its own)."""
    if isinstance(c, TransformKey):
        return c.op
    if isinstance(c, AggregateKey):
        return canonical_aggregate_alias(c, profile="stage_formula") or c.agg
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
) -> RegroupAttachPlan:
    """Compile a re-aggregation (DEV-1847) as producer-over-producer: a carrier
    at the operand's union grain (the inner producers) and an outer aggregate
    grouping it by the attributable outer grain, reusing the association kernel."""
    prebound, bundle = context.prebound, context.bundle
    host_model, models_by_name = context.host_model, context.models_by_name
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
            c, projected_dim_keys=context.projected_dim_keys,
            projected_td_keys=context.projected_td_keys,
            active_bucket=prebound.main_time_key,
        )

    # A clean, stable name for the re-aggregation (the nested source has no
    # canonical alias of its own; a transform constituent has no ``.agg``).
    inner_alias = _constituent_alias(constituents[0]) if constituents else "reagg"
    alias = (
        public_alias
        or canonical_aggregate_alias(root, profile="stage_formula")
        or f"{root.agg}_{inner_alias}"
    )

    # Checked before TD resolution — name the combination, not a misleading TD error.
    check_reaggregation_no_window(alias=alias, window_val=window_kwarg_of(root))
    # Requested outer grain: explicit partition_by= (combined-consumer rule: each
    # key must be a query dimension) else the query dimensions.
    if root.partition_keys is not None:
        requested = list(root.partition_keys)
        proj_set = set(proj)
        for g in requested:
            check_reaggregation_partition_key_is_query_dim(
                alias=alias,
                offending=None if g in proj_set else _regroup_grain_name(g),
            )
    else:
        requested = list(proj)

    # Type each outer parameter against the operand grain; a legal aggregate-valued
    # parameter rides the carrier as an extra constituent, a legal column parameter
    # is picked once per cell. An UNGRAINED aggregate parameter types at the OUTER
    # grain (decision 12) — normalise it there so the carrier broadcasts it per
    # outer cell; the original key stays the outer pipeline identity, aliased to the
    # normalised constituent's placeholder below.
    reagg_param_specs = resolve_aggregation_params(
        agg=root, owner_model=host_model, owner_path=(), bundle=bundle,
    )
    param_constituent_of: Dict[ValueKey, ValueKey] = {}
    for _ps in reagg_param_specs:
        check_parameter_determined(
            alias=alias, param_name=_ps.name, grain_display=_grain_display(union_grain),
            determined=_param_is_determined(
                spec=_ps, grain=union_grain, host_model=host_model,
                models_by_name=models_by_name, bundle=bundle,
            ),
        )
        if isinstance(_ps.key, AggregateKey):
            ck = _ps.key
            if ck.partition_keys is None:
                ck = ck.model_copy(update={"partition_keys": Grain.of(requested)})
            if ck not in constituents:
                constituents.append(ck)
            if ck != _ps.key:
                param_constituent_of[_ps.key] = ck

    # Attributability to the operand dataset: a grain member, or determined from
    # an entity-key grain field over to-one hops. Unattributable dims resolve per
    # to_many_handling, exactly as for model-rooted aggregates.
    mode = prebound.to_many_handling
    attributable: List[ValueKey] = []
    unattributable: List[_UnattributableDim] = []
    expression_determined: List[ValueKey] = []
    for g in requested:
        if g in union_grain or _reaggregation_determined(
            key=g, union_grain=union_grain, host_model=host_model,
            models_by_name=models_by_name, bundle=bundle,
        ):
            attributable.append(g)
        elif _grain_expression_determined(
            key=g, union_grain=union_grain, host_model=host_model,
            models_by_name=models_by_name, bundle=bundle,
        ):
            attributable.append(g)
            expression_determined.append(g)
        else:
            if key_host_path(g) and grain_member_attributable(
                key=g, target_path=(), root_model=host_model,
                models_by_name=models_by_name, bundle=bundle, host_model=host_model,
                host_name=host_model.name,
            ):
                # Reachable to-one but not SEEDED by the operand grain.
                reason = (
                    "not determined by the operand grain — add the join's "
                    "entity key to the inner partition_by="
                )
            else:
                reason = key_broadcast_reason(
                    key=g, target_path=(), root_model=host_model,
                    models_by_name=models_by_name, bundle=bundle,
                    host_model=host_model, host_name=host_model.name,
                )
            unattributable.append(_UnattributableDim(
                key=g, name=_regroup_grain_name(g), reason=reason,
                reachable=reason != UNREACHABLE_NO_PATH,
            ))

    associate_dims: List[_UnattributableDim] = []
    broadcast_dims: List[Tuple[str, str]] = []
    if unattributable:
        check_reaggregation_dims_attributable(
            alias=alias, mode=mode,
            unattributable_names=[u.name for u in unattributable],
        )
        if mode == "associate":
            associate_dims = unattributable
        else:
            broadcast_dims = [(u.name, u.reason) for u in unattributable]

    outer_grain = [*attributable, *[u.key for u in associate_dims]]
    # Degenerate: operand grain equals the outer grain — the identity, warned.
    degenerate = not broadcast_dims and Grain.of(union_grain) == Grain.of(outer_grain)

    # An expression-determined outer dimension consumes carrier cells; its
    # aggregates ride the carrier as extra constituents (grain ⊆ union grain
    # keeps it fixed). A dim already IN the grain stays the carrier's grain key.
    for g in expression_determined:
        for a in operand_aggregates(g):
            if a not in constituents:
                constituents.append(a)

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
    carrier_attach = _build_carrier_attach(
        union_grain=union_grain, constituents=constituents,
        constituent_placeholders=constituent_placeholders, host_model=host_model,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        inherited=inherited, n_date_range=n_date_range,
        producer_source_model=host_model.name, producer_registry=producer_registry,
        projected_dim_keys=context.projected_dim_keys,
        projected_td_keys=context.projected_td_keys,
        active_bucket=prebound.main_time_key,
    )

    # The outer producer: OUTER_AGG over the constituent composite (placeholders),
    # grouped by the outer grain. Its body renders via the association kernel with
    # the carrier as the per-cell value; level 2 aggregates over the cells. An
    # attach-carrying grain key becomes an expression over carrier placeholders.
    # Substitute the WHOLE root (source AND params) so an aggregate-valued
    # parameter references its carrier placeholder like the source constituents do.
    outer_agg = substitute_value_keys(root, constituent_placeholders)
    # Each legal parameter is picked once per cell: an aggregate-valued
    # one from its carrier placeholder, a column/expression one from the operand
    # scope; level 2 reads it as ``_base._p<i>``.
    reagg_picked_params = [
        PickedParam(
            name=_ps.name,
            key=(constituent_placeholders[_ps.key]
                 if isinstance(_ps.key, AggregateKey) else _ps.key),
            sql=_ps.expr_sql, anchor_path=(),
        )
        for _ps in reagg_param_specs
    ]
    original_by_pk: Dict[ValueKey, ValueKey] = {}
    for g in outer_grain:
        sub = substitute_value_keys(g, constituent_placeholders)
        original_by_pk[sub] = g
    outer_prebound, ordered_outer = _regroup_producer_prebound(
        pks=Grain.of(original_by_pk), aggs=[outer_agg], model=host_model, bundle=bundle,
        # Row filters define the population whose cells the outer aggregate
        # consumes — without them a NULL-masking composite (coalesce) would
        # fabricate cells for filtered-out entities.
        inherited=inherited, n_date_range=n_date_range,
        # A clean producer column name (the placeholder-sourced key would leak the
        # reserved __regroup__ prefix into the emitted alias).
        public_alias_by_agg={outer_agg: alias},
        explicit_types={outer_agg: declared_type} if declared_type is not None else None,
        to_many_handling=mode,
    )
    outer_plan = compile_synthesized(
        outer_prebound,
        source_model=host_model.name,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        # An expression grain key (in-grain computed dim) — or an attach-owning
        # outer answer (DEV-1859 decision 11) — desugars its own nested row
        # attach inside the outer producer.
        enable_producer_regroups=_answers_need_nested_regroups([outer_agg]) or any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(pk)
            for pk in ordered_outer
        ),
        producer_registry=producer_registry,
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

    answer_slot = outer_plan.aggregate_slots[0].id
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
        [_regroup_grain_name(g) for g in union_grain] if degenerate else []
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
            original_key=root,
        )],
        partition_display=[
            _regroup_grain_name(original_by_pk.get(g, g)) for g in ordered_outer
        ],
        producer_root_model=host_model.name,
        broadcast_measure=alias if broadcast_dims else None,
        broadcast_dimensions=broadcast_dims,
        associated_measure=alias if associate_dims else None,
        associated_dimensions=[u.name for u in associate_dims],
        degenerate_measure=alias if degenerate else None,
        degenerate_operand_grain=degenerate_display,
        degenerate_outer_grain=(
            [_regroup_grain_name(g) for g in outer_grain] if degenerate else []
        ),
    )


def _build_carrier_attach(
    *,  # NOSONAR(S107) — carrier plumbing: each parameter is a distinct compile input threaded from _plan_regroups; a one-use context type would only relocate them
    union_grain: Grain,
    constituents: List[ValueKey],
    constituent_placeholders: Dict[ValueKey, ValueKey],
    host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
    inherited: List[BoundFilter],
    n_date_range: int,
    producer_source_model: Optional[str],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> RegroupAttachPlan:
    """A row-attach producer at the union grain carrying every constituent (coarser
    ones broadcast within it) — the carrier / level-1 of the re-aggregation."""
    carrier_prebound, ordered_pks = _regroup_producer_prebound(
        pks=union_grain, aggs=constituents, model=host_model, bundle=bundle,
        inherited=inherited, n_date_range=n_date_range,
    )
    carrier_plan = compile_synthesized(
        carrier_prebound,
        source_model=producer_source_model,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        # Discovery must re-run inside the carrier for a coarser constituent
        # (nested broadcast), a constituent that is itself a re-aggregation or a
        # transform (Axiom 11), or an expression grain key needing its own nested
        # row attach. A constituent's grain is its result grain (Axiom 2.3), not a
        # raw partition_keys (a transform's is empty).
        enable_producer_regroups=any(
            constituent_grain(
                c, projected_dim_keys=projected_dim_keys,
                projected_td_keys=projected_td_keys, active_bucket=active_bucket,
            ) != union_grain
            for c in constituents
        ) or _answers_need_nested_regroups(constituents) or any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(pk)
            for pk in union_grain
        ),
        producer_registry=producer_registry,
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
        )
        for i, c in enumerate(constituents)
    ]
    grain_ids = list(carrier_plan.projection)[: len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, pk in enumerate(ordered_pks):
        slot_id = next((s.id for s in carrier_plan.row_slots if s.key == pk), None)
        join_pairs.append((pk, slot_id if slot_id is not None else grain_ids[i]))
    # A coarser constituent nests as a ROW attach whose placeholder lands in the
    # carrier's row slots; it is an answer column (grain-determined), not a grain key.
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
        producer_root_model=None,
    )


def _assert_total_routing(prebound: PreboundQuery) -> None:
    """Post-discovery total-routing invariant: every cross-model / partitioned aggregate leaf must be disposed; a survivor is an unrouted shape and raises."""
    roles: Tuple[Tuple[str, List[ValueKey]], ...] = (
        ("measure", [dm.bound.value_key for dm in prebound.declared_measures]),
        ("filter", [bf.value_key for bf in prebound.bound_filters]),
        ("order", [sp.bound.value_key for sp in prebound.order_specs]),
    )
    for role, keys in roles:
        for vk in keys:
            for k in walk_value_keys(vk):
                if is_cross_model_agg(k) or (
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
    *, env: ElaboratedQuery, measure_roots: List[ValueKey],
    attach_plans: List[RegroupAttachPlan],
) -> None:
    """D5 coherence: a measure the compiler attaches at differing grains must carry the environment's Broadcast insertions for every non-union-grain term."""
    combined = {
        s.original_key
        for plan in attach_plans if plan.attach_phase == "combined"
        for s in plan.substitutions
    }
    if not combined:
        return
    for root, entry in zip(measure_roots, env.measures):
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


def _plan_regroups(  # NOSONAR(S3776) — one cohesive desugar: discover row (computed-dim) + combined (measure/order) partitioned aggregates, synthesize one producer per (partition set, phase), and rewrite the prebound to placeholders. The two phases share the registry / inherited-filter / substitution state; splitting scatters it.
    *,
    prebound: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_model: Optional[str],
    in_producer: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
    local_discovery: bool = True,
    home_paths: Optional[Dict[ValueKey, Tuple[str, ...]]] = None,
) -> Optional[Tuple[PreboundQuery, List[RegroupAttachPlan]]]:
    """Discover partitioned aggregates and desugar into producer stages + reserved-leaf placeholders (row attach at base FROM, combined at the combined SELECT)."""
    # DEV-1847: re-aggregation roots — an aggregate whose operand resolves to
    # attached values. Pre-substitute each with a placeholder so the normal
    # discovery below treats it opaquely (its constituents belong to the carrier,
    # not a main-query attach); the producer-over-producer is synthesized later.
    registry = RegroupPlaceholderRegistry()
    # A LOCAL row-attach root whose partition_by is exactly the query grain is a
    # redundant partition (== the GROUP BY): strip it so the root aggregates
    # INLINE with its inputs row-attached, not through a redundant combined
    # producer (DEV-1859 decision 10). Only the top consumer level needs this —
    # a producer sub-plan renders partition_by==grain inline.
    if local_discovery and not in_producer:
        _dim_dms, _td_dms, _ = partition_declared_measures(
            declared_measures=prebound.declared_measures,
            n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
        )
        _query_grain = Grain.of(dm.bound.value_key for dm in (*_dim_dms, *_td_dms))
        _strip: Dict[ValueKey, ValueKey] = {
            r: r.model_copy(update={"partition_keys": None})
            for r in _discover_roots(prebound, predicate=is_row_attach_root)
            if not is_cross_model_agg(r) and window_kwarg_of(r) is None
            and r.partition_keys is not None
            and Grain.of(r.partition_keys) == _query_grain
        }
        if _strip:
            prebound = _substitute_prebound(prebound, _strip)
    reagg_roots = (
        _discover_roots(prebound, predicate=is_reaggregation_key)
        if local_discovery else []
    )
    reagg_mapping: Dict[ValueKey, ValueKey] = {
        root: registry.placeholder_for(root) for root in reagg_roots
    }
    reagg_public_alias: Dict[ValueKey, str] = {}
    reagg_declared_type: Dict[ValueKey, DataType] = {}
    reagg_phase: Dict[ValueKey, Literal["row", "combined"]] = {}
    if reagg_mapping:
        for dm in prebound.declared_measures:
            vk = dm.bound.value_key
            if vk in reagg_mapping:
                if dm.public_name:
                    reagg_public_alias.setdefault(vk, dm.public_name)
                if dm.type_is_explicit and dm.type is not None:
                    reagg_declared_type.setdefault(vk, dm.type)
                reagg_phase[vk] = "row" if dm.is_dimension else "combined"
        # A root reached from inside a computed dimension is a ROW attach (the
        # dimension's expression consumes the value at row level).
        for dm in prebound.declared_measures:
            if dm.is_dimension:
                for k in walk_value_keys(dm.bound.value_key):
                    if k in reagg_mapping:
                        reagg_phase[k] = "row"
        for root in reagg_roots:
            reagg_phase.setdefault(root, "combined")
        prebound = _substitute_prebound(prebound, reagg_mapping)
    # Row-attach roots: a partitioned aggregate or a transform over one; row_inner_aggs are bare aggregates inside dimensions.
    if local_discovery:
        row_aggs = dimension_regroup_roots(prebound.declared_measures)
        row_inner_aggs = dimension_partitioned_aggregates(
            prebound.declared_measures,
        )
    else:
        row_aggs, row_inner_aggs = [], []
    # One unified combined-consumer walk (local + cross-model); ``row_agg_set`` is empty
    # in a producer sub-plan, matching the pre-unification cross-model discovery.
    # A measure-typed filter conjunct consumes at query grain like a declared
    # measure (its row-attached refs need a combined twin); a field-typed one
    # row-routes its attached refs.
    consumers = combined_consumer_aggregates(
        declared_measures=prebound.declared_measures,
        order_specs=prebound.order_specs,
        row_agg_set=frozenset(row_inner_aggs),
        bound_filters=prebound.bound_filters,
        measure_typed_filter_indices=frozenset(
            i for i, ct in enumerate(filter_typings)
            if ct.typing == MaskTyping.MEASURE
        ),
        dim_keys=position_typing_context(prebound)[0],
    )
    combined_aggs = list(consumers.local_partitioned) if local_discovery else []
    public_alias_by_agg: Dict[AggregateKey, str] = dict(consumers.public_alias)
    # Bare windowed / first-last measures join the COMBINED roots at the full projected grain.
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    projected_dim_keys = [dm.bound.value_key for dm in dim_dms]
    projected_td_keys = [dm.bound.value_key for dm in td_dms]
    active_bucket = prebound.main_time_key

    # A LOCAL aggregate whose inputs cross a join desugars onto a HOST-rooted producer.
    _is_crossing_local_root = crossing_local_root_predicate(
        scope=scope, bundle=bundle,
    )

    if local_discovery:
        bare_combined, bare_alias = _bare_combined_roots(
            prebound, extra_root=_is_crossing_local_root,
        )
        for agg in bare_combined:
            if agg not in combined_aggs:
                combined_aggs.append(agg)
        for agg, name in bare_alias.items():
            public_alias_by_agg.setdefault(agg, name)

    def _root_grain(agg: ValueKey) -> Grain:
        return constituent_grain(
            agg, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )

    # Inside a union-grain producer, a root at EXACTLY the producer's grain compiles inline; only STRICT-subset grains nest (windowed transform inner excepted).
    if in_producer:
        own_grain = Grain.of([*projected_dim_keys, *projected_td_keys])
        windowed_transform_inputs = {
            k
            for dm in prebound.declared_measures
            for tk in walk_value_keys(dm.bound.value_key)
            if isinstance(tk, TransformKey)
            for k in walk_value_keys(tk.input)
            if window_kwarg_of(k) is not None
        }
        combined_aggs = [
            k for k in combined_aggs
            if _root_grain(k) != own_grain or k in windowed_transform_inputs
        ]
        row_aggs = [k for k in row_aggs if regroup_root_grain(k) != own_grain]
    # Cross-model aggregates become target-rooted producers; a cross-model root inside a computed dimension is a ROW-phase producer.
    cm_row = [k for k in row_aggs if is_cross_model_agg(k)]
    row_aggs = [k for k in row_aggs if not is_cross_model_agg(k)]
    cm_combined = [
        *consumers.cross_model_partitioned, *consumers.cross_model_bare,
    ]
    # DEV-1841: a LOCAL aggregate whose query grain includes a dimension not
    # attributable from the host root routes through the same producer synthesis
    # (broadcast / associate / error), never the naive fanned inline GROUP BY.
    host_for_local = scope.source_model if isinstance(scope, ModelScope) else None
    if host_for_local is not None:
        lb_models = bundle.models_by_name
        lb_grain = [*projected_dim_keys, *projected_td_keys]

        def _local_broadcasts(k: ValueKey) -> bool:
            if (
                not isinstance(k, AggregateKey) or is_cross_model_agg(k)
                or k.locus == "host"
                or k.partition_keys is not None or _is_crossing_local_root(k)
                # first/last and windowed aggregates have deliberate per-group
                # semantics over a fan-out grain (DEV-1748) and their own producer
                # path — only the additive/counting family fans destructively.
                or k.agg in RANKED_AGGREGATIONS or window_kwarg_of(k) is not None
            ):
                return False
            return any(
                not grain_member_attributable(
                    key=g, target_path=(), root_model=host_for_local,
                    models_by_name=lb_models, bundle=bundle,
                    host_model=host_for_local, host_name=host_for_local.name,
                )
                for g in lb_grain
            )

        seen_lb: set = set()
        local_broadcast: List[ValueKey] = []

        def _scan_lb(vk: ValueKey) -> None:
            # Opaque below a row-attach root's inputs (DEV-1859).
            for k in walk_consumer_keys(vk):
                if k not in seen_lb and _local_broadcasts(k):
                    seen_lb.add(k)
                    local_broadcast.append(k)

        for dm in prebound.declared_measures:
            if dm.is_dimension:
                continue
            vk = dm.bound.value_key
            if dm.public_name and _local_broadcasts(vk):
                public_alias_by_agg.setdefault(vk, dm.public_name)
            _scan_lb(vk)
        for sp in prebound.order_specs:
            _scan_lb(sp.bound.value_key)
        for bf in prebound.bound_filters:
            _scan_lb(bf.value_key)
        cm_combined = [*cm_combined, *local_broadcast]
    # DEV-1859: a row-attach root (mixed source and/or attached parameter)
    # broadcasts each attached input onto its rows. A root aggregating INLINE at
    # this grain row-attaches its inputs (local → row_aggs, cross-model → cm_row)
    # and stays an ordinary aggregate over the placeholder-rewritten
    # source/parameter; a root routed through its own producer (partitioned /
    # windowed / cross-model / broadcast) row-attaches them via that sub-plan's
    # recursion. Discovery opacity keeps these inputs out of the combined sets,
    # so no subtraction is needed.
    mixed_inline_inner: List[ValueKey] = []
    row_attach_roots = (
        _discover_roots(prebound, predicate=is_row_attach_root)
        if local_discovery else []
    )
    if row_attach_roots:
        producer_bound = {*combined_aggs, *row_aggs, *cm_combined, *cm_row}
        seen_inner: set = set()
        for root in row_attach_roots:
            if root in producer_bound:  # own-producer: its sub-plan row-attaches
                continue
            for a in attached_inputs(root):  # inline: broadcast each onto rows
                if a in seen_inner:
                    continue
                seen_inner.add(a)
                mixed_inline_inner.append(a)
                target = cm_row if is_cross_model_agg(a) else row_aggs
                if a not in target:
                    target.append(a)
    cm_type = dict(consumers.declared_type)
    if (
        not row_aggs and not combined_aggs and not cm_combined and not cm_row
        and not reagg_roots
    ):
        return None
    # A real column sharing the reserved placeholder prefix would shadow a placeholder at render; reject while a regroup is active.
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    reserved = reserved_prefix_columns(
        producer_model if isinstance(scope, ModelScope) else scope
    )
    check_reserved_regroup_prefix(reserved)
    mapping: Dict[ValueKey, ValueKey] = {
        agg: registry.placeholder_for(agg)
        for agg in (*row_aggs, *combined_aggs, *cm_row, *cm_combined)
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

    attaches: List[RegroupAttachPlan] = []
    for phase, phase_aggs, order_fn, alias_map, grain_names in (
        ("row", row_aggs, _regroup_partition_order, {}, {}),
        ("combined", combined_aggs, _combined_order, public_alias_by_agg,
         grain_name_by_key),
    ):
        if not phase_aggs:
            continue
        # Group roots by producer grain and (for windowed / ranked) partition-free identity, so each gets its own producer.
        groups: Dict[Tuple, List[ValueKey]] = {}
        group_meta: Dict[Tuple, Tuple[Grain, bool]] = {}
        for agg in phase_aggs:
            grain, windowed = effective_root_grain(
                agg, projected_dim_keys=projected_dim_keys,
                projected_td_keys=projected_td_keys, active_bucket=active_bucket,
            )
            ident = _windowed_or_ranked_identity(agg)
            # A crossing-input root needs its OWN producer, else another aggregate's crossed joins fan its rows.
            if ident is None and _is_crossing_local_root(agg):
                ident = ("crossing", agg.source, agg.agg, tuple(agg.args),
                         tuple(agg.kwargs))
            gkey = (grain, ident)
            groups.setdefault(gkey, []).append(agg)
            group_meta[gkey] = (grain, windowed)
        for gkey, aggs in groups.items():
            pks, windowed = group_meta[gkey]
            pks = _prune_functionally_determined_grain(pks)
            # One producer measure per partition-free identity: a bare and a partition_by= twin collapse to one column.
            canonical_by_identity: Dict = {}
            producer_aggs: List[ValueKey] = []
            canonical_of: Dict[ValueKey, ValueKey] = {}
            for agg in aggs:
                ident = _partition_free_identity(agg)
                canon = canonical_by_identity.get(ident)
                if canon is None:
                    canonical_by_identity[ident] = agg
                    producer_aggs.append(agg)
                    canon = agg
                canonical_of[agg] = canon
            canon_index = {c: i for i, c in enumerate(producer_aggs)}
            # Per-role crossing-input safety for every host-rooted producer answer.
            if producer_model is not None:
                for agg_k in producer_aggs:
                    if isinstance(agg_k, AggregateKey):
                        _assert_local_producer_inputs_safe(
                            agg=agg_k, host_model=producer_model,
                            bundle=bundle,
                            models_by_name={
                                m.name: m for m in bundle.referenced_models
                            },
                        )
            producer_prebound, ordered_pks = _regroup_producer_prebound(
                pks=pks, aggs=producer_aggs, model=producer_model, bundle=bundle,
                inherited=inherited, n_date_range=n_inherited_date,
                partition_order=order_fn, public_alias_by_agg=alias_map,
                explicit_types={
                    dm.bound.value_key: dm.type
                    for dm in prebound.declared_measures
                    if phase == "combined" and dm.type_is_explicit and dm.type is not None
                },
                grain_name_by_key=grain_names,
                window_td_key=prebound.main_time_key if windowed else None,
                to_many_handling=prebound.to_many_handling,
            )
            producer_plan = compile_synthesized(
                producer_prebound,
                source_model=producer_source_model,
                bundle=bundle,
                scope=scope,
                stage_schemas=stage_schemas,
                # A producer re-runs regroup discovery for its strict-subset inner
                # aggregates and for a computed / bare-partitioned dimension in its
                # grain (which needs a nested row attach to group by its value).
                # A transform or attach-owning answer (mixed source / attached
                # parameter) row-attaches its inputs via the sub-plan's own
                # regroup pass, even when windowed (DEV-1859 decision 11).
                enable_producer_regroups=(
                    (not windowed) or any(
                        isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
                        or is_local_partitioned_agg(pk)
                        for pk in pks
                    ) or _answers_need_nested_regroups(producer_aggs)
                ),
                producer_registry=producer_registry,
            )
            # A union-grain producer MAY carry nested attaches at any depth; the
            # complete-grain assert below is the admission rule (DEV-1847).
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
                )
                for agg in aggs
            ]
            # Match each grain key to its producer slot by structural identity, else by projection POSITION.
            producer_grain_ids = list(producer_plan.projection)[:len(ordered_pks)]
            join_pairs = []
            for i, pk in enumerate(ordered_pks):
                slot_id = next(
                    (s.id for s in producer_plan.row_slots if s.key == pk), None,
                )
                if slot_id is None:
                    slot_id = producer_grain_ids[i]
                # A host-side grain key embedding another attach's aggregate
                # renders via that attach's placeholder (DEV-1847 shape B).
                join_pairs.append((substitute_value_keys(pk, mapping), slot_id))
            _assert_attach_covers_producer_grain(
                joined_slot_ids={slot_id for _, slot_id in join_pairs},
                producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
            )
            # A producer whose answer IS a windowed / ranked aggregate carries the matching kernel.
            attach_kwargs: Dict[str, Any] = {}
            if (
                windowed
                and isinstance(producer_aggs[0], AggregateKey)
                and window_kwarg_of(producer_aggs[0]) is not None
            ):
                attach_kwargs["kernel"] = _trailing_window_kernel(
                    producer_plan=producer_plan, agg_key=producer_aggs[0],
                )
            elif (
                isinstance(producer_aggs[0], AggregateKey)
                and producer_aggs[0].agg in RANKED_AGGREGATIONS
            ):
                attach_kwargs["kernel"] = _ranked_kernel(
                    producer_plan=producer_plan, agg_key=producer_aggs[0],
                    root_model=(
                        producer_plan.render_source_model or bundle.source_model
                    ),
                    bundle=bundle,
                )
            attaches.append(RegroupAttachPlan(
                producer_plan=producer_plan,
                alias_hint=(
                    (canonical_aggregate_alias(aggs[0], profile="stage_formula")
                     if isinstance(aggs[0], AggregateKey) else None)
                    or getattr(aggs[0], "agg", None)
                    or getattr(aggs[0], "op", None)
                    or "regroup"
                ),
                attach_phase=phase,
                join_pairs=join_pairs,
                substitutions=substitutions,
                partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
                **attach_kwargs,
            ))

    # One target-rooted producer per distinct cross-model aggregate; roles share one producer + placeholder.
    host_model_for_cm = (
        scope.source_model if isinstance(scope, ModelScope) else bundle.source_model
    )
    models_by_name_cm = bundle.models_by_name
    base_filters_with_text = list(zip(
        prebound.bound_filters,
        prebound.bound_filter_texts
        + [None] * (len(prebound.bound_filters) - len(prebound.bound_filter_texts)),
    ))
    synthesis_context = _ProducerSynthesisContext(
        prebound=prebound, bundle=bundle, host_model=host_model_for_cm,
        models_by_name=models_by_name_cm,
        projected_dim_keys=projected_dim_keys,
        projected_td_keys=projected_td_keys,
        base_filters_with_text=base_filters_with_text, scope=scope,
        stage_schemas=stage_schemas, home_paths=home_paths or {},
    )
    for phase, cm_aggs in (("combined", cm_combined), ("row", cm_row)):
        for agg in cm_aggs:
            attaches.append(_synthesize_cross_model_producer(
                agg=agg, placeholder=mapping[agg], attach_phase=phase,
                public_alias=public_alias_by_agg.get(agg),
                context=synthesis_context, declared_type=cm_type.get(agg),
                producer_registry=producer_registry,
            ))

    # DEV-1847: one producer-over-producer per re-aggregation root.
    for root in reagg_roots:
        attaches.append(_synthesize_reaggregation_producer(
            root=root, placeholder=reagg_mapping[root],
            attach_phase=reagg_phase.get(root, "combined"),
            public_alias=reagg_public_alias.get(root),
            context=synthesis_context,
            declared_type=reagg_declared_type.get(root),
            producer_registry=producer_registry, registry=registry,
            inherited=inherited, n_date_range=n_inherited_date,
        ))

    # The ROW substitution applies ONLY to computed DIMENSIONS; a non-dim measure
    # keeps query-grain (its inners desugar to COMBINED placeholders) — EXCEPT a
    # mixed-source measure's inline inner constituents, which row-attach and so
    # must reach the measure too (DEV-1859).
    combined_mapping: Dict[ValueKey, ValueKey] = {
        agg: mapping[agg]
        for agg in (*combined_aggs, *cm_combined, *mixed_inline_inner)
    }
    rewritten = PreboundQuery(
        declared_measures=[
            DeclaredMeasure(
                bound=BoundExpr(
                    value_key=substitute_value_keys(
                        dm.bound.value_key,
                        mapping if dm.is_dimension else combined_mapping,
                    ),
                ),
                declared_name=dm.declared_name,
                public_name=dm.public_name,
                label=dm.label,
                canonical_alias=dm.canonical_alias,
                type=dm.type,
                type_is_explicit=dm.type_is_explicit,
                preserve_native_type=dm.preserve_native_type,
                format=dm.format,
                description=dm.description,
                is_dimension=dm.is_dimension,
            )
            for dm in prebound.declared_measures
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
            )
            for sp in prebound.order_specs
        ],
        main_time_key=prebound.main_time_key,
        n_dims=prebound.n_dims,
        n_time_dimensions=prebound.n_time_dimensions,
        limit=prebound.limit,
        offset=prebound.offset,
        distinct_dimension_values=prebound.distinct_dimension_values,
        to_many_handling=prebound.to_many_handling,
    )
    # Intern every producer: a structurally identical one becomes the same plan object.
    attaches = [_intern_producer(a, producer_registry) for a in attaches]
    return rewritten, attaches


def _has_inline_population_aggregate(prebound: PreboundQuery) -> bool:
    """A plain host-rooted aggregate survives inline over the population rows —
    every producer-bound term is a placeholder by now, so any real ``AggregateKey``
    with a host-local source, no partition_by, no window and not first/last computes
    directly over the (row-filtered) population. Scans projected measures, ordering
    keys AND filters — an aggregate in a HAVING/combined predicate is inline over
    the population too, so a fanning row filter multiplies it just the same."""
    keys = [dm.bound.value_key for dm in prebound.declared_measures]
    keys += [sp.bound.value_key for sp in prebound.order_specs]
    keys += [bf.value_key for bf in prebound.bound_filters]
    return any(
        isinstance(a, AggregateKey) and not is_cross_model_agg(a)
        and not source_anchor_path(a.source) and a.partition_keys is None
        and window_kwarg_of(a) is None and a.agg not in RANKED_AGGREGATIONS
        for vk in keys for a in walk_value_keys(vk)
    )


def _assert_population_filters_no_fanout(
    *, prebound: PreboundQuery, scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> None:
    """Interim population-filter guard (DEV-1900 decision 7): with an aggregate
    inline over the population, a ROW-filter conjunct reaching the population root
    only across a fanning hop would multiply its rows — fail closed (DEV-1909
    lands association pushdown to the population and retires this)."""
    host_model = scope.source_model if isinstance(scope, ModelScope) else None
    if host_model is None or not _has_inline_population_aggregate(prebound):
        return
    models_by_name = bundle.models_by_name
    texts = prebound.bound_filter_texts
    for i, bf in enumerate(prebound.bound_filters):
        if bf.phase != Phase.ROW:
            continue
        text = texts[i] if i < len(texts) else None
        for cj in split_top_level_and(bf.value_key):
            closure = key_closure(
                key=cj, anchor_model=host_model,
                anchor_relation=host_model.name, bundle=bundle,
            )
            hop = None if closure is None else next(
                (p[-1] for p in closure if p and not safe_reachable(
                    root=host_model, path=p, models_by_name=models_by_name,
                )),
                None,
            )
            check_population_filter_no_fanout(
                filter_text=text or _canonical_name(cj), hop=hop,
                unanalyzable=closure is None,
            )


def compile_synthesized(
    prebound: PreboundQuery,
    *,
    source_model: Optional[str],
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
    enable_producer_regroups: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Elaborate a compiler-synthesized sub-plan — resolving its aggregates' homes
    relative to its OWN root (D3) — then compile it. The recursion guard disables
    host-rooted isolation, so the sub-plan types without splitting."""
    carrier = StrictQueryCarrier(source_model=source_model, prebound=prebound)
    env = elaborate_query(
        query=carrier, prebound=prebound, bundle=bundle, scope=scope,
        stage_schemas=stage_schemas, disable_host_rooted_isolation=True,
    )
    assert env.prebound is not None  # elaborate_query always sets the typed prebound
    return compile_prebound(
        query=carrier, bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        prebound=env.prebound, filter_typings=list(env.filter_typings), env=env,
        disable_host_rooted_isolation=True,
        enable_producer_regroups=enable_producer_regroups,
        producer_registry=producer_registry,
    )


def compile_prebound(  # NOSONAR(S3776) — compiler entry-point dispatcher. The pre-existing complexity is owned by the multi-stage scope / bundle / projection / filter-routing wiring it orchestrates and is tracked as a separate refactor.
    *,
    query: Union[SlayerQuery, StrictQueryCarrier],
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: PreboundQuery,
    filter_typings: List[ConjunctTyping],
    env: ElaboratedQuery,
    disable_host_rooted_isolation: bool = False,
    enable_producer_regroups: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile one typed prebound into a ``PlannedQuery``; ``disable_host_rooted_isolation`` suppresses the LOCAL half of the regroup desugar (recursion guard).

    Every caller arrives typed with an environment: the top-level entry via
    ``compile_query``, a synthesized sub-plan via ``compile_synthesized`` (D3), so
    the aggregate homes always come from ``env.terms`` — there is no local typing."""
    stage_schemas = stage_schemas or {}
    # One interning registry per top-level plan; nested producer calls thread it down.
    if producer_registry is None:
        producer_registry = {}

    # The generator renders FROM / joins against the binder's model (ModelScope → host; StageSchema → None).
    render_source_model = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )

    declared_measures = list(prebound.declared_measures)
    bound_filters = list(prebound.bound_filters)
    n_date_range = prebound.n_date_range
    order_specs = list(prebound.order_specs)
    active_td_key = prebound.main_time_key
    n_dims = prebound.n_dims
    n_tds = prebound.n_time_dimensions
    distinct_dimension_values = prebound.distinct_dimension_values
    # Pre-substitution measure roots, positionally aligned with env.measures.
    _coh_measure_roots = [
        dm.bound.value_key for dm in declared_measures[n_dims + n_tds:]
    ]

    # Desugar partitioned aggregates into producer stages + reserved-leaf placeholders.
    regroup_attach_plans: List[RegroupAttachPlan] = []
    if isinstance(query.source_model, str):
        _producer_source_model = query.source_model
    elif render_source_model is not None:
        _producer_source_model = render_source_model.name
    else:
        _producer_source_model = None
    # The desugar always runs; the LOCAL half is suppressed in a disabled sub-plan, cross-model roots always desugar.
    home_paths = {
        k: t.home_path for k, t in env.terms.items()
        if isinstance(t, Aggregate)
    }
    regroup_result = _plan_regroups(
        prebound=prebound, filter_typings=filter_typings,
        scope=scope, bundle=bundle,
        stage_schemas=stage_schemas,
        producer_source_model=_producer_source_model,
        in_producer=enable_producer_regroups,
        producer_registry=producer_registry,
        local_discovery=(
            not disable_host_rooted_isolation or enable_producer_regroups
        ),
        home_paths=home_paths,
    )
    if regroup_result is not None:
        prebound, regroup_attach_plans = regroup_result
        declared_measures = list(prebound.declared_measures)
        bound_filters = list(prebound.bound_filters)
        n_date_range = prebound.n_date_range
        order_specs = list(prebound.order_specs)
        active_td_key = prebound.main_time_key
        n_dims = prebound.n_dims
        n_tds = prebound.n_time_dimensions
        distinct_dimension_values = prebound.distinct_dimension_values
    # At the top consumer level every cross-model / partitioned leaf must now be a placeholder; sub-plans are exempt.
    if not disable_host_rooted_isolation and not enable_producer_regroups:
        _assert_total_routing(prebound)
        _assert_population_filters_no_fanout(
            prebound=prebound, scope=scope, bundle=bundle,
        )
    _assert_broadcast_coherence(
        env=env, measure_roots=_coh_measure_roots,
        attach_plans=regroup_attach_plans,
    )

    # SlayerModel.filters — Mode-A SQL WHERE, scope-derived so a sub-plan gets its own.
    mode_a_filters: List[ModeAFilter] = []
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        for j, mf in enumerate(scope.source_model.filters or []):
            mode_a_filters.append(validate_model_filter(
                mf=mf, idx=j, model=scope.source_model,
            ))

    source_col_names = _source_column_names(scope)
    host_name = host_model_name(scope)

    # Windowed-measure guards on the pre-projection trees; returns the cleanly-selected windowed AggregateKeys.
    selected_windowed = _guard_windowed_measures(
        measure_vks=[dm.bound.value_key for dm in declared_measures],
        filter_vks=[bf.value_key for bf in bound_filters],
        order_vks=[sp.bound.value_key for sp in order_specs],
        active_td_key=active_td_key,
    )

    projection = ProjectionPlanner().plan(
        measures=declared_measures,
        filters=bound_filters,
        order=order_specs,
        source_column_names=source_col_names,
        host_model_name=host_name,
    )

    row_slots, agg_slots, combined_slots = _bucket_slots(
        projection.registry.slots,
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

    # Classify each ORDER BY target not a declared/public slot: it resolves like a filter ref (aggregate → hidden slot; grouped row column → hidden min/max wrap; transform/composite → hidden outer wrap).
    _has_grouping = bool(agg_slots) or (
        bool(n_dims or n_tds) and distinct_dimension_values
    )
    # ORDER BY targets rewritten to a hidden wrap, keyed by (key, DIRECTION) since ``a ASC, a DESC`` needs MIN(a) and MAX(a).
    order_key_remap: Dict[Tuple[ValueKey, str], ValueKey] = {}
    # Host-grain / crossing wraps synthesized as late producers, and the slots they answer.
    late_wrap_keys: set = set()
    late_attach_answered: set = set()
    host_model_for_wraps = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )
    for spec in order_specs:
        okey = spec.bound.value_key
        osid = projection.registry.find_by_key(okey)
        if osid is not None and not projection.registry.get(osid).hidden:
            continue  # declared / projected output — orders on a real column
        if isinstance(okey, AggregateKey):
            continue  # hidden aggregate (local base or cross-model CTE)
        if isinstance(okey, ColumnKey) and okey.leaf.startswith(REGROUP_LEAF_PREFIX):
            # A combined regroup placeholder resolves via its producer, not a hidden MIN/MAX.
            continue
        if isinstance(okey, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            if not _has_grouping:
                continue  # raw-rows query -> split emission, no wrap needed
            path = _row_key_path(okey)
            # A TimeTruncKey is not a legal aggregate source; wrap its underlying column (DATE_TRUNC is monotonic).
            src = okey.column if isinstance(okey, TimeTruncKey) else okey
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
            # A JOINED wrap, or a local wrap whose source crosses a join, is a HOST-rooted producer synthesized late.
            _wrap_crosses = path or (
                host_model_for_wraps is not None
                and local_crossing_input_paths(
                    key=wrap_key, bundle=bundle,
                    host_model=host_model_for_wraps,
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
                        producer_source_model=_producer_source_model,
                        row_attaches=[
                            a for a in regroup_attach_plans
                            if a.attach_phase == "row"
                        ],
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

    # Each filter conjunct compiles to a hidden whole-predicate slot; the mask entry
    # carries its typing and stratum. Interned after every other slot so no earlier
    # hidden name or slot id shifts; lowering to WHERE/HAVING/outer placements is
    # emission-side (sql.generator).
    masks: List[MaskEntry] = []
    for i, (bf, ct) in enumerate(zip(bound_filters, filter_typings)):
        mask_sid = projection.registry.find_by_key(bf.value_key)
        if mask_sid is None:
            mask_sid = projection.registry.intern(
                key=bf.value_key,
                declared_name=f"__slayer_mask_{i}",
                hidden=True,
                phase=bf.value_key.phase,
            )
        masks.append(MaskEntry(
            slot_id=mask_sid, typing=ct.typing, stratum=ct.stratum,
        ))
    if masks:
        row_slots, agg_slots, combined_slots = _bucket_slots(
            projection.registry.slots,
        )
    # Per-mask structural reachability summary, in this plan's coordinate system.
    reachability_anchor_model = render_source_model or bundle.source_model
    source_relation = (
        query.source_model
        if isinstance(query.source_model, str)
        else host_name
    )
    filter_reachability: List[FilterReachability] = []
    # One expansion cache for the whole plan (both visitors and every filter share it).
    reachability_cache: dict = {}
    for bf, mask in zip(bound_filters, masks):
        filter_reachability.append(FilterReachability(
            filter_id=mask.slot_id,
            crossed_join_paths=compute_key_join_paths(
                key=bf.value_key,
                anchor_model=reachability_anchor_model,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
            has_host_local_ref=key_has_host_local_ref(
                key=bf.value_key,
                anchor_model=reachability_anchor_model,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
        ))
    # Backstop: raise on a cross-model slot that survived the desugar (else it fan-multiplies).
    for slot in agg_slots:
        if slot.id in late_attach_answered:
            continue  # answered by a late host-grain wrap producer
        key = slot.key
        assert not (
            isinstance(key, AggregateKey)
            and source_anchor_path(key.source)
            and key.locus != "host"
        ), (
            f"Cross-model aggregate slot {slot.id!r} survived the regroup "
            f"desugar (DEV-1838 D8); every cross-model aggregate must "
            f"become a target-rooted producer."
        )

    order_entries = []
    for spec in order_specs:
        # A grouped row-column sort key was rewritten to a hidden wrap above; order on that slot.
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
        order_entries.append(OrderEntry(
            slot_id=sid,
            direction=spec.direction,
            phase=order_slot.key.phase,
        ))

    transform_layers = _emit_transform_layers(slots=projection.registry.slots)
    stage_schema = _emit_stage_schema(
        stage_name=query.name, projection=projection,
    )

    # Frame-bound column set: raw columns of this stage's non-hidden time dimensions.
    frame_bound_columns = _frame_bound_columns(row_slots=row_slots)

    # A COMBINED regroup attach is an isolated aggregate (value in the producer CTE, never _base) → still an empty-base spine.
    regroup_combined_slot_ids: set = set()
    for attach in regroup_attach_plans:
        if attach.attach_phase != "combined":
            continue
        for sub in attach.substitutions:
            sid = projection.registry.find_by_key(sub.placeholder)
            if sid is not None:
                regroup_combined_slot_ids.add(sid)
    empty_base_plan = _plan_empty_base_grain(
        projection=projection.public_projection,
        agg_slots=agg_slots,
        windowed_slot_ids=windowed_slot_ids,
        regroup_combined_slot_ids=regroup_combined_slot_ids,
        order_entries=order_entries,
        masks=masks,
        mode_a_filters=mode_a_filters,
    )

    # Assign every slot its materialisation stage / needs-column / series fact
    # (DEV-1800 D3); producer bodies were staged by their own compile_prebound.
    row_slots, agg_slots, combined_slots = stage_slots(
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        combined_expression_slots=combined_slots,
        regroup_attach_plans=regroup_attach_plans,
        masks=masks,
        order=order_entries,
        projection=projection.public_projection,
        distinct_dimension_values=distinct_dimension_values,
    )

    planned = PlannedQuery(
        source_relation=source_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        regroup_attach_plans=regroup_attach_plans,
        combined_expression_slots=combined_slots,
        transform_layers=transform_layers,
        masks=masks,
        n_date_range_masks=n_date_range,
        mode_a_filters=mode_a_filters,
        projection=projection.public_projection,
        order=order_entries,
        limit=prebound.limit,
        offset=prebound.offset,
        stage_schema=stage_schema,
        active_time_dimension_slot_id=active_td_slot_id,
        render_source_model=render_source_model,
        distinct_dimension_values=distinct_dimension_values,
        frame_bound_columns=frame_bound_columns,
        filter_reachability=filter_reachability,
        empty_base_plan=empty_base_plan,
    )
    return planned




def _plan_empty_base_grain(
    *,
    projection: List[SlotId],
    agg_slots: list,
    windowed_slot_ids: AbstractSet[SlotId],
    order_entries: list,
    masks: List[MaskEntry],
    mode_a_filters: List[ModeAFilter],
    regroup_combined_slot_ids: Optional[set] = None,
) -> "EmptyBaseGrainPlan | None":
    """Decide the empty-base spine at plan time — the host base has nothing of its own exactly when every value asked for is an isolated aggregate."""
    isolated = set(windowed_slot_ids)
    isolated |= (regroup_combined_slot_ids or set())
    if not projection or any(sid not in isolated for sid in projection):
        return None
    if any(slot.id not in isolated for slot in agg_slots):
        return None  # a host-local aggregate would give _base a column of its own
    if any(entry.slot_id not in isolated for entry in order_entries):
        return None
    # Field masks gate the host spine; measure masks resolve after attachment.
    host_filter_ids = [
        m.slot_id for m in masks if m.typing == MaskTyping.FIELD
    ] + [mf.id for mf in mode_a_filters]
    return EmptyBaseGrainPlan(host_filter_ids=host_filter_ids)


def _frame_bound_columns(*, row_slots: list) -> List[ValueKey]:
    """Raw column keys of the stage's NON-HIDDEN time dimensions — an explicit bound on one is a FRAME bound (hidden TimeTruncKey slots excluded)."""
    out: List[ValueKey] = []
    seen: set = set()
    for rs in row_slots:
        if rs.hidden or not isinstance(rs.key, TimeTruncKey):
            continue
        col = rs.key.column
        if col in seen:
            continue
        seen.add(col)
        out.append(col)
    return out


def _plan_src_row_filters(
    *,
    producer_plan: PlannedQuery,
) -> "Tuple[List[str], List[SrcFilterRewrite]]":
    """Partition the producer's field masks for a windowed measure's ``_src`` scope into ``(where_filter_ids, src_filter_rewrites)`` by frame-bound membership (Mode-A model filters exempt, date-range masks are frame bounds)."""
    time_cols = frozenset(producer_plan.frame_bound_columns)
    slots_by_id = {
        s.id: s
        for s in (
            *producer_plan.row_slots,
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        )
    }
    where_ids: List[str] = [mf.id for mf in producer_plan.mode_a_filters]
    rewrites: List[SrcFilterRewrite] = []
    date_ids = {
        m.slot_id
        for m in producer_plan.masks[:producer_plan.n_date_range_masks]
    }
    for m in producer_plan.masks:
        if m.typing != MaskTyping.FIELD or m.slot_id in date_ids:
            continue
        key = slots_by_id[m.slot_id].key
        residual = strip_frame_bounds(key=key, time_columns=time_cols)
        if residual is None:
            continue  # wholly a frame bound
        where_ids.append(m.slot_id)
        if residual is not key:
            rewrites.append(SrcFilterRewrite(
                filter_id=m.slot_id,
                expression=BoundExpr(value_key=residual),
            ))
    return where_ids, rewrites


def _topo_sort(queries: List[SlayerQuery]) -> List[SlayerQuery]:
    """Kahn's algorithm: order stages so each follows the siblings it references (unnamed stages appended last); raises on duplicate names or a cycle."""
    if len(queries) <= 1:
        return list(queries)
    named = [q for q in queries if q.name]
    names = [q.name for q in named]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(
            f"Duplicate stage names in source_queries DAG: {duplicates}"
        )
    by_name = {q.name: q for q in named}
    in_degree = {q.name: 0 for q in named}
    edges: Dict[str, List[str]] = {q.name: [] for q in named}
    for q in named:
        # A stage depends on a sibling its source_model reads from (bare-string OR ModelExtension/dict over the sibling).
        dep = source_name_if_sibling(q.source_model, by_name)
        if dep is not None and dep != q.name:
            in_degree[q.name] += 1
            edges[dep].append(q.name)
    sorted_names: List[str] = []
    queue = [n for n, d in in_degree.items() if d == 0]
    while queue:
        n = queue.pop(0)
        sorted_names.append(n)
        for dep in edges[n]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)
    if len(sorted_names) != len(in_degree):
        remaining = sorted(set(in_degree) - set(sorted_names))
        raise ValueError(
            f"Cycle detected in source_queries DAG involving stages: "
            f"{remaining}"
        )
    sorted_named = [by_name[n] for n in sorted_names]
    unnamed = [q for q in queries if q.name is None]
    return sorted_named + unnamed


def _source_column_names(
    scope: Union[ModelScope, StageSchema],
) -> FrozenSet[str]:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return frozenset(c.name for c in scope.source_model.columns)
    if isinstance(scope, StageSchema):
        return frozenset(c.name for c in scope.columns)
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


def _emit_stage_schema(
    *,
    stage_name: Optional[str],
    projection,
) -> StageSchema:
    columns: List[StageColumn] = []
    alias_idx: Dict[str, int] = {}
    for sid in projection.public_projection:
        slot = projection.registry.get(sid)
        if slot.hidden:
            continue
        idx = alias_idx.setdefault(sid, 0)
        if idx < len(slot.public_aliases):
            alias = slot.public_aliases[idx]
        else:
            alias = slot.declared_name
        alias_idx[sid] = idx + 1
        # Downstream bind + CTE column name are the ``__``-flattened form; public_alias keeps the dotted result-key form.
        flat = flat_name(alias)
        # Two distinct public columns flattening to one downstream name would make the CTE column ambiguous.
        check_stage_flatten_collision(
            flat_name=flat, collides=any(c.name == flat for c in columns),
        )
        # A column an upstream stage bucketed carries its granularity downstream,
        # so a re-binding TimeDimension can type-check the re-bucket (DEV-1471).
        upstream_gran = (
            TimeGranularity(slot.key.granularity)
            if isinstance(slot.key, TimeTruncKey) else None
        )
        columns.append(StageColumn(
            name=flat,
            sql_alias=flat,
            public_alias=alias,
            type=slot.type,
            granularity=upstream_gran,
            label=slot.label,
            hidden=False,
            format=slot.format,
            description=slot.description,
        ))
    return StageSchema(
        relation_name=stage_name or "(unnamed_stage)", columns=columns,
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
