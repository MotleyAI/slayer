"""The stage compiler: one typed prebound → ``PlannedQuery`` (``compile_prebound``).
Binding lives in ``bind_inputs``; typing and the checker in ``elaborate_env``."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Hashable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, RANKED_AGGREGATIONS
from slayer.core.errors import AmbiguousJoinPathError, UnreachableFilterDroppedWarning
from slayer.core.keys import AggregateKey, Grain, ArithmeticKey, BetweenKey, ColumnKey, ColumnSqlKey, InKey, LiteralKey, Phase, ScalarCallKey, StarKey, TimeTruncKey, TransformKey, ValueKey, column_leaf, regroup_root_grain, reroot_value_key, substitute_value_keys, walk_value_keys, REGROUP_LEAF_PREFIX, is_cross_model_agg, is_local_partitioned_agg, split_top_level_and, window_kwarg_of, is_reaggregation_key, operand_aggregates
from slayer.core.models import SlayerModel
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths
from slayer.engine.column_filter_paths import compute_column_filter_join_paths
from slayer.core.join_walker import resolve_hop, walk
from slayer.engine.join_safety import (
    UNREACHABLE_NO_PATH,
    attributable_from_root,
    broadcast_reason,
    crossing_local_root_predicate,
    grain_determines,
    grain_member_attributable,
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
    type_and_split_filters,
    type_order_positions,
    check_association_root_unique_key,
    check_association_windowed_ranked,
    check_cross_model_inputs_safe,
    check_cross_model_partition_keys_attributable,
    check_cross_model_source_resolves,
    check_local_producer_inputs_safe,
    check_order_target_has_slot,
    check_parameter_determined,
    check_raw_rows_no_aggregate_slots,
    check_reaggregation_dims_attributable,
    check_reaggregation_no_window,
    check_reaggregation_partition_key_is_query_dim,
    check_windowed_cross_model_time_axis,
    check_windowed_key_supported,
    check_windowed_time_dimension,
)
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
    aggs: List[AggregateKey],
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
            public_alias_by_agg.get(agg)
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
        and not getattr(k.source, "path", ())
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
    for dm in prebound.declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        for k in walk_value_keys(vk):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
        if _is_root(vk) and dm.public_name is not None:
            alias.setdefault(vk, dm.public_name)
    for sp in prebound.order_specs:
        for k in walk_value_keys(sp.bound.value_key):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
    for bf in prebound.bound_filters:
        for k in walk_value_keys(bf.value_key):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
    return out, alias


def _effective_root_grain(
    agg: ValueKey,
    *,
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Tuple[Grain, bool]:
    """A combined-root's producer grain and windowedness.

    An explicitly-partitioned aggregate keeps ``regroup_root_grain``. A bare
    windowed / first-last root takes the FULL projected grain (a windowed root's
    bucket enters via ``window_td_key``, so it is excluded here)."""
    windowed = window_kwarg_of(agg) is not None
    if getattr(agg, "partition_keys", None) is not None:
        grain = regroup_root_grain(agg)
        # A transform over a window= inner gains the active bucket in its union grain
        # and renders windowed; first/last inners are timeless.
        if (
            not windowed and active_bucket is not None
            and any(window_kwarg_of(k) is not None for k in walk_value_keys(agg))
        ):
            return grain | {active_bucket}, True
        return grain, windowed
    if windowed:
        grain = Grain.of(projected_dim_keys) | (
            Grain.of(projected_td_keys) - ({active_bucket} if active_bucket else frozenset())
        )
    else:
        grain = Grain.of([*projected_dim_keys, *projected_td_keys])
    return grain, windowed


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
        agg.column_filter_key,
    )


def _partition_free_identity(agg: ValueKey):  # NOSONAR(S8495) — distinct-shape identity tuples are intentional dict keys: a plain aggregate's 5-field identity and an "other" 2-tuple never collide (different lengths compare unequal)
    if not isinstance(agg, AggregateKey):
        return ("other", agg)
    return (agg.source, agg.agg, tuple(agg.args), tuple(agg.kwargs),
            agg.column_filter_key)


def _cross_model_input_paths(
    *, agg_rooted: AggregateKey, root_model: SlayerModel, root_name: str,
    bundle: ResolvedSourceBundle,
) -> List[Tuple[str, ...]]:
    out: List[Tuple[str, ...]] = []
    if agg_rooted.column_filter_key is not None:
        # referenced_join_paths are OWNER-relative (anchored at the source
        # column's owner via source.path) and are never re-rooted; prefix each
        # with the source path and register every prefix, as filter_reachability
        # does. Today owner == root, so the prefix was empty.
        source_path = key_host_path(agg_rooted.source)
        for p in agg_rooted.column_filter_key.referenced_join_paths:
            full = source_path + tuple(p)
            for i in range(1, len(full) + 1):
                if full[:i] not in out:
                    out.append(full[:i])
    for p in compute_aggregate_input_join_paths(
        key=agg_rooted, anchor_model=root_model, anchor_relation=root_name,
        bundle=bundle,
    ):
        if tuple(p) not in out:
            out.append(tuple(p))
    return out


def _first_unsafe_input_hop(
    *, agg_rooted: AggregateKey, root_model: SlayerModel, root_name: str,
    bundle: ResolvedSourceBundle, models_by_name: Dict[str, SlayerModel],
) -> List[str]:
    # Source-column / kwarg / column-filter refs, in the root's coordinates.
    for path in _cross_model_input_paths(
        agg_rooted=agg_rooted, root_model=root_model, root_name=root_name, bundle=bundle,
    ):
        if not safe_reachable(
            root=root_model, path=path, models_by_name=models_by_name,
        ):
            # First violation wins; the checker raises it.
            return [path[-1] if path else root_name]
    return []


def _first_unattributable_arg_leaf(
    *, agg: AggregateKey, target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str] = None,
) -> List[str]:
    # Positional args and column-valued kwargs in HOST coordinates (a ranking
    # first/last time key, a weight column); a fail-closed backstop under the
    # home rule, which certifies legal inputs upstream. host_name lets an off-home
    # input traverse a proven reverse hop, exactly as _home_path judged it.
    for arg in (*agg.args, *(v for _, v in agg.kwargs)):
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        hp = key_host_path(arg)
        if not attributable_from_root(
            host_path=hp, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
        ):
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            return [leaf]
    return []


def _assert_cross_model_inputs_safe(
    *, agg: AggregateKey, agg_rooted: AggregateKey, root_model: SlayerModel,
    root_name: str, target_path: Tuple[str, ...], bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
) -> None:
    """Resolve every cross-model input's attributability from its root; the checker raises on a fanning/unproven join."""
    unsafe_input_hops = _first_unsafe_input_hop(
        agg_rooted=agg_rooted, root_model=root_model, root_name=root_name,
        bundle=bundle, models_by_name=models_by_name,
    )
    unattributable_arg_leaves = [] if unsafe_input_hops else (
        _first_unattributable_arg_leaf(
            agg=agg, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
        )
    )
    check_cross_model_inputs_safe(
        alias=canonical_aggregate_alias(agg, profile="stage_formula"),
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

    # Crossed predicate + remaining crossed args; the SOURCE's own crossings are exempt.
    gated_crossings: List[str] = []
    if not ranked_crossings:
        gated = local_crossing_input_paths(
            key=agg, bundle=bundle, host_model=host_model, include_source=False,
        )
        gated_crossings = [p[-1] for p in gated if p and not _safe(p)]
    check_local_producer_inputs_safe(
        alias=canonical_aggregate_alias(agg, profile="stage_formula"),
        host=host_model.name,
        ranked_crossings=ranked_crossings,
        gated_crossings=gated_crossings,
    )


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
    models_by_name = {m.name: m for m in bundle.referenced_models}
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
    producer_plan = compile_prebound(
        query=StrictQueryCarrier(
            source_model=producer_source_model, prebound=producer_prebound,
        ),
        bundle=bundle,
        scope=scope,
        stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        # A computed-dimension grain member nests its own producer inside the wrap.
        enable_producer_regroups=any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(pk)
            for pk in projected
        ),
        prebound=producer_prebound,
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
    """Owner-relative join paths a derived column's ``Column.sql`` actually reads."""
    if not isinstance(col, ColumnSqlKey) or bundle is None:
        return ()
    owner = _owning_model(
        col.model, host_model=host_model, models_by_name=models_by_name,
    )
    if owner is None:
        return ()
    column = next((c for c in owner.columns if c.name == col.column_name), None)
    if column is None or not column.sql:
        return ()
    scan_bundle = bundle
    if host_model is not None and bundle.get_referenced_model(host_model.name) is None:
        # A dep can point back at the host, which some bundles keep only as source.
        scan_bundle = bundle.model_copy(update={
            "referenced_models": [*bundle.referenced_models, host_model],
        })
    return compute_column_filter_join_paths(
        canonical_sql=column.sql, anchor_model=owner,
        anchor_relation=owner.name, bundle=scan_bundle,
    )


def _ref_effective_paths(
    r: ValueKey, *, host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel], bundle: Optional[ResolvedSourceBundle],
) -> List[Tuple[str, ...]]:
    """Host-coordinate paths the ref's evaluation reads: its declared path plus,
    for a SQL-defined column, its definition's crossed paths (owner-relative)."""
    col = r.column if isinstance(r, TimeTruncKey) else r
    own = tuple(getattr(col, "path", ()) or ())
    out = [own]
    for rel in _ref_sql_dependency_paths(
        col, host_model=host_model, models_by_name=models_by_name, bundle=bundle,
    ):
        out.append(own + tuple(rel))
    return out


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
    unsafe = next(
        (
            r for r in refs
            if any(
                not attributable_from_root(
                    host_path=ep, target_path=target_path,
                    root_model=root_model, models_by_name=models_by_name,
                    host_name=host_name,
                )
                for ep in _ref_effective_paths(
                    r, host_model=host_model, models_by_name=models_by_name,
                    bundle=bundle,
                )
            )
        ),
        None,
    )
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
    target_path = _home_path(
        agg=agg, host_model=host_model, models_by_name=models_by_name,
        bundle=bundle,
    )
    root_model = walk_key_path(model=host_model, path=target_path, bundle=bundle)
    if root_model is None:  # pragma: no cover — bind resolved the path already
        check_cross_model_source_resolves(
            target_path=target_path, host_name=host_model.name,
        )
    root_name = root_model.name
    alias = public_alias or canonical_aggregate_alias(agg, profile="stage_formula")

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
        hp = key_host_path(g)
        shared = shared_join_key_reroot(
            key=g, target_path=target_path, host_model=host_model,
            models_by_name=models_by_name,
        )
        if shared is not None:
            # The join-key identity needs no join in the producer.
            safe_pairs.append((g, shared))
        elif grain_member_attributable(
            key=g, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ):
            safe_pairs.append((g, reroot_from_root(
                g, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            )))
        else:
            reason = broadcast_reason(
                host_path=hp, target_path=target_path, root_model=root_model,
                models_by_name=models_with_host, host_name=host_model.name,
            )
            unattributable.append(_UnattributableDim(
                key=g, name=_regroup_grain_name(g), reason=reason,
                reachable=reason != UNREACHABLE_NO_PATH,
            ))

    # Associate mode attributes unattributable dims per cell over the root's
    # distinct entities (a host-rooted two-level producer that reaches every query
    # dimension) rather than broadcasting; an explicit partition_by= at such a
    # grain is legal here.
    if mode == "associate" and unattributable:
        return _synthesize_association_producer(
            agg=agg, placeholder=placeholder, attach_phase=attach_phase,
            alias=alias, root_model=root_model, root_name=root_name,
            target_path=target_path, requested=requested, explicit=explicit,
            unattributable=unattributable, context=context,
            declared_type=declared_type, producer_registry=producer_registry,
        )

    # Broadcast / error path: an unattributable explicit key is a hard error.
    check_cross_model_partition_keys_attributable(
        alias=alias, root_name=root_name, explicit=explicit,
        unattributable=[(u.name, u.reason) for u in unattributable],
    )
    broadcast: List[Tuple[str, str]] = [(u.name, u.reason) for u in unattributable]

    if target_path != key_host_path(agg.source):
        # The source sits beyond the home; re-anchor off-home inputs via the host
        # and render it inline as a host-locus aggregate joining the to-one path
        # from the home, never a source-rooted producer.
        agg_rooted = reroot_from_root(
            agg, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ).model_copy(update={"locus": "host"})
    else:
        agg_rooted = reroot_value_key(agg, target_path=target_path)
    _assert_cross_model_inputs_safe(
        agg=agg, agg_rooted=agg_rooted, root_model=root_model, root_name=root_name,
        target_path=target_path, bundle=bundle, models_by_name=models_by_name,
        host_name=host_model.name,
    )

    # A windowed cross-model aggregate folds the active TD into its grain as the bucket (must be attributable from the root).
    window_td_key: Optional[ValueKey] = None
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
            active_td, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        )

    inherited, semi_joins, dropped = _cross_model_inherited_filters(
        base_filters=base_filters_with_text, target_path=target_path,
        root_model=root_model, models_by_name=models_by_name,
        host_name=host_model.name, host_model=host_model, bundle=bundle,
    )

    root_bundle = bundle.model_copy(update={"source_model": root_model})
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
    # A computed-dimension grain member or windowed producer re-enables discovery.
    enable_nested = window_td_key is not None or any(
        isinstance(rr, (ScalarCallKey, ArithmeticKey, TransformKey))
        or is_local_partitioned_agg(rr)
        for rr in grain_keys
    )
    producer_plan = compile_prebound(
        query=StrictQueryCarrier(source_model=root_name, prebound=producer_prebound),
        bundle=root_bundle, scope=root_scope,
        stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        enable_producer_regroups=enable_nested,
        prebound=producer_prebound,
        producer_registry=producer_registry,
    )
    if semi_joins:
        producer_plan = producer_plan.model_copy(
            update={"semi_join_filters": semi_joins},
        )
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
    if window_td_key is not None:
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
        **cm_attach_kwargs,
    )


_BARE_IDENT_RE = re.compile(r"^[A-Za-z_]\w*$")
_DOTTED_PATH_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$")
_IDENT_RE = re.compile(r"[A-Za-z_]\w*")


class _ParamSpec(NamedTuple):
    """A resolved aggregation parameter that references data: a bound
    ``key`` (column / aggregate) or an ``expr_sql`` expression default, at
    ``owner_path``. Literal params never become a ``_ParamSpec``."""
    name: str
    key: Optional[ValueKey]
    expr_sql: Optional[str]


def _column_default_key(
    *, path: Tuple[str, ...], leaf: str, base: Optional[SlayerModel],
) -> ValueKey:
    """A ``ColumnSqlKey`` when ``leaf`` names a derived column on ``base`` (so its
    ``Column.sql`` expands), else a plain ``ColumnKey``."""
    if base is not None:
        col = next((c for c in (base.columns or []) if c.name == leaf), None)
        if col is not None and col.sql:
            return ColumnSqlKey(path=path, model=base.name, column_name=leaf)
    return ColumnKey(path=path, leaf=leaf)


def _default_param_value_key(
    *, sql: str, owner_path: Tuple[str, ...],
    owner_model: Optional[SlayerModel] = None,
    bundle: Optional[ResolvedSourceBundle] = None,
) -> Optional[ValueKey]:
    """A bare-identifier or dotted-path definition default → a structured key in
    the owner's coordinates (so a host column of the same name never captures it,
    and a to-one path like ``regions.pop`` is picked once per cell), a
    ``ColumnSqlKey`` when the named column is derived so its SQL expands; an
    expression or literal default → ``None``."""
    text = sql.strip()
    if _BARE_IDENT_RE.match(text):
        return _column_default_key(path=tuple(owner_path), leaf=text, base=owner_model)
    if _DOTTED_PATH_RE.match(text):
        parts = text.split(".")
        # A leading owner-model qualifier is a self-reference, not a hop.
        if owner_model is not None and parts[0] == owner_model.name:
            parts = parts[1:]
        if len(parts) == 1:
            return _column_default_key(
                path=tuple(owner_path), leaf=parts[0], base=owner_model,
            )
        terminal = (
            walk_key_path(model=owner_model, path=tuple(parts[:-1]), bundle=bundle)
            if owner_model is not None and bundle is not None else None
        )
        return _column_default_key(
            path=tuple(owner_path) + tuple(parts[:-1]), leaf=parts[-1], base=terminal,
        )
    return None


def _expr_default_columns(*, sql: str, owner_model: Optional[SlayerModel]) -> List[str]:
    """Owner columns an expression default references (bare-token match)."""
    names = {c.name for c in (owner_model.columns or [])} if owner_model else set()
    return [t for t in _IDENT_RE.findall(sql) if t in names]


def _longest_common_prefix(paths: List[Tuple[str, ...]]) -> Tuple[str, ...]:
    if not paths:
        return ()
    common = paths[0]
    for p in paths[1:]:
        i = 0
        while i < len(common) and i < len(p) and common[i] == p[i]:
            i += 1
        common = common[:i]
    return common


def _home_path(
    *, agg: AggregateKey, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> Tuple[str, ...]:
    """The home dataset for a cross-model aggregate (Axiom 2): the deepest join
    path that determines every input — the source column plus each column-valued
    arg/kwarg — over provably to-one hops. Candidates are the input paths and
    their longest common prefix, deepest first (ties prefer the source path); the
    first one every input is attributable from wins. Falls back to the source path
    (today's root), where input safety then raises on an unproven hop."""
    source_path = key_host_path(agg.source)
    input_paths: List[Tuple[str, ...]] = [source_path]
    # A ranked aggregate's positional args are its ranking keys, not value inputs;
    # they must stay attributable from the source (checked downstream), never pull
    # the home shallower.
    arg_values = () if agg.agg in RANKED_AGGREGATIONS else agg.args
    for v in (*arg_values, *(val for _, val in agg.kwargs)):
        if isinstance(v, (ColumnKey, ColumnSqlKey)):
            input_paths.append(key_host_path(v))
    candidates = sorted(
        {source_path, _longest_common_prefix(input_paths), *input_paths},
        key=lambda p: (-len(p), p != source_path, p),
    )
    for p in candidates:
        model_at_p = walk_key_path(model=host_model, path=p, bundle=bundle)
        if model_at_p is None:
            continue
        if all(
            attributable_from_root(
                host_path=q, target_path=p, root_model=model_at_p,
                models_by_name=models_by_name, host_name=host_model.name,
            )
            for q in input_paths
        ):
            return p
    return source_path


def _resolve_aggregation_params(
    *, agg: AggregateKey, owner_model: Optional[SlayerModel],
    owner_path: Tuple[str, ...], bundle: Optional[ResolvedSourceBundle] = None,
) -> List[_ParamSpec]:
    """Every aggregation parameter that references data — explicit non-scalar
    args/kwargs and non-overridden definition defaults (a bare-identifier or
    dotted-path default → a column key, a derived one a ``ColumnSqlKey``; an
    expression default → its SQL). Literal params are omitted: they ride the
    existing kwarg/default machinery unchanged."""
    explicit = {name for name, _ in agg.kwargs}
    out: List[_ParamSpec] = [
        _ParamSpec(name=name, key=v, expr_sql=None)
        for name, v in agg.kwargs
        if isinstance(v, (ColumnKey, ColumnSqlKey, AggregateKey))
    ]
    agg_def = next(
        (a for a in (owner_model.aggregations or []) if a.name == agg.agg), None,
    ) if owner_model is not None else None
    if agg_def is not None:
        for p in agg_def.params:
            if p.name in explicit:
                continue
            vk = _default_param_value_key(
                sql=p.sql, owner_path=owner_path, owner_model=owner_model,
                bundle=bundle,
            )
            if vk is not None:
                out.append(_ParamSpec(name=p.name, key=vk, expr_sql=None))
            elif _expr_default_columns(sql=p.sql, owner_model=owner_model):
                out.append(_ParamSpec(name=p.name, key=None, expr_sql=p.sql))
    return out


def _param_is_determined(
    *, spec: _ParamSpec, owner_model: Optional[SlayerModel],
    owner_path: Tuple[str, ...], grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
) -> bool:
    """A parameter is legal iff the dataset grain determines it — the bound key,
    or (for an expression default) every owner column it references."""
    if spec.key is not None:
        return grain_determines(
            key=spec.key, grain=grain, host_model=host_model,
            models_by_name=models_by_name,
        )
    return all(
        grain_determines(
            key=ColumnKey(path=tuple(owner_path), leaf=c), grain=grain,
            host_model=host_model, models_by_name=models_by_name,
        )
        for c in _expr_default_columns(sql=spec.expr_sql or "", owner_model=owner_model)
    )


def _grain_display(grain: Grain) -> str:
    """A readable grain listing for a parameter-typing error."""
    names = [_regroup_grain_name(g) for g in _regroup_partition_order(grain)]
    return ", ".join(names) if names else "the grand total"


def _synthesize_association_producer(  # NOSONAR(S3776) — one cohesive host-rooted association synthesis (eligibility / entity key / grain / filter-inheritance / recursive plan / attach).
    *,
    agg: AggregateKey,
    placeholder: ValueKey,
    attach_phase: str,
    alias: str,
    root_model: SlayerModel,
    root_name: str,
    target_path: Tuple[str, ...],
    requested: List[ValueKey],
    explicit: bool,
    unattributable: List[_UnattributableDim],
    context: _ProducerSynthesisContext,
    declared_type: Optional[DataType] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> RegroupAttachPlan:
    """Build a distinct-entity association producer: HOST-rooted at the full
    requested grain, joining to the aggregate's root, deduped per root entity by
    the ``association`` kernel (level 1 picks per entity, level 2 aggregates)."""
    prebound, bundle = context.prebound, context.bundle
    host_model, models_by_name = context.host_model, context.models_by_name
    base_filters_with_text = context.base_filters_with_text
    scope, stage_schemas = context.scope, context.stage_schemas

    check_association_windowed_ranked(
        alias=alias,
        windowed_or_ranked=window_kwarg_of(agg) is not None or (
            isinstance(agg, AggregateKey) and agg.agg in RANKED_AGGREGATIONS
        ),
    )
    # key_sets also feeds the entity keys below.
    key_sets = _unique_key_sets(root_model)
    check_association_root_unique_key(
        alias=alias, root_name=root_name, has_unique_key=bool(key_sets),
    )
    # An input crossing an unproven/fanning hop is not constant per root entity,
    # so the level-1 per-entity pick would be arbitrary. Input safety is
    # mode-invariant — reject exactly as the broadcast/error path does (DEV-1884
    # certifies such inputs empirically).
    # locus="host" so default-fragment discovery looks the definition up on the
    # source model (not the home) when the source sits beyond the home.
    _assert_cross_model_inputs_safe(
        agg=agg, agg_rooted=reroot_from_root(
            agg, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ).model_copy(update={"locus": "host"}),
        root_model=root_model, root_name=root_name, target_path=target_path,
        bundle=bundle, models_by_name=models_by_name, host_name=host_model.name,
    )
    entity_keys: List[ValueKey] = [
        ColumnKey(path=target_path, leaf=col) for col in key_sets[0]
    ]
    # Type each parameter against the entity grain (requested dims ∪ entity keys)
    # and lift the legal ones — picked once per associated entity alongside the
    # aggregate's own value; the residue is a typed error. Definition defaults are
    # declared on the source column's model, which equals the home only when the
    # home is the source path.
    source_path = key_host_path(agg.source)
    source_model = walk_key_path(
        model=host_model, path=source_path, bundle=bundle,
    ) or root_model
    assoc_grain = Grain.of([*requested, *entity_keys])
    picked_params: List[PickedParam] = []
    for _ps in _resolve_aggregation_params(
        agg=agg, owner_model=source_model, owner_path=source_path, bundle=bundle,
    ):
        check_parameter_determined(
            alias=alias, param_name=_ps.name, grain_display=_grain_display(assoc_grain),
            determined=_param_is_determined(
                spec=_ps, owner_model=source_model, owner_path=source_path,
                grain=assoc_grain, host_model=host_model,
                models_by_name=models_by_name,
            ),
        )
        picked_params.append(PickedParam(
            name=_ps.name, key=_ps.key, sql=_ps.expr_sql,
            anchor_path=tuple(source_path),
        ))

    # A HOST-grain wrap of the aggregate compiles inline at the producer's full
    # grain (never re-routed as unattributable); the kernel's level-1 dedup
    # removes the reverse-hop fan-out.
    assoc_agg = agg.model_copy(update={"locus": "host"})
    grain_keys = Grain.of(requested)
    # A conjunct the metric-root routing cannot handle (e.g. an OR mixing the
    # entity's own column with a host predicate) would inline fan-dependently at
    # host grain — drop it exactly as the cross-model producer would; every other
    # conjunct routes via the host (inline / semi-join).
    survivors: List[Tuple[BoundFilter, Optional[str]]] = []
    root_dropped: List[UnreachableFilterDroppedWarning] = []
    for bf, text in base_filters_with_text:
        if bf.phase != Phase.ROW:
            survivors.append((bf, text))
            continue
        for cj in split_top_level_and(bf.value_key):
            _, _, drop_w = _conjunct_disposition(
                cj, text=text, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
                host_model=host_model, bundle=bundle,
            )
            if drop_w is not None:
                root_dropped.append(drop_w)
            else:
                survivors.append((bound_filter_from_key(cj), text))
    inherited, semi_joins, dropped = _cross_model_inherited_filters(
        base_filters=survivors, target_path=(),
        root_model=host_model, models_by_name=models_by_name,
        host_name=host_model.name, host_model=host_model, bundle=bundle,
    )
    dropped = [*root_dropped, *dropped]
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=grain_keys, aggs=[assoc_agg], model=host_model, bundle=bundle,
        inherited=inherited, n_date_range=0,
        explicit_types=(
            {assoc_agg: declared_type} if declared_type is not None else None
        ),
        to_many_handling=prebound.to_many_handling,
    )
    producer_plan = compile_prebound(
        query=StrictQueryCarrier(
            source_model=host_model.name, prebound=producer_prebound,
        ),
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True, enable_producer_regroups=False,
        prebound=producer_prebound, producer_registry=producer_registry,
    )
    if semi_joins:
        producer_plan = producer_plan.model_copy(
            update={"semi_join_filters": semi_joins},
        )
    producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
    answer_slot = _regroup_answer_slot_id(
        value_slots=[
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        ],
        key=assoc_agg,
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
        join_pairs.append((rr, slot_id))
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    assoc_dims = [u.name for u in unattributable]
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=canonical_aggregate_alias(agg, profile="stage_formula"),
        attach_phase=attach_phase,
        kernel=AssociationProducerKernel(
            entity_keys=entity_keys, picked_params=picked_params,
        ),
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=placeholder, producer_slot_id=answer_slot,
            original_key=agg,
        )],
        partition_display=[_regroup_grain_name(rr) for rr in ordered_pks],
        producer_root_model=host_model.name,
        dropped_filter_warnings=dropped,
        # Explicit partition_by= is a requested grain and does not warn.
        associated_measure=None if explicit else alias,
        associated_dimensions=[] if explicit else assoc_dims,
    )


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


def _discover_reaggregation_roots(prebound: PreboundQuery) -> List[AggregateKey]:
    """Re-aggregation roots reachable from any measure / order / filter, first-seen."""
    seen: set = set()
    out: List[AggregateKey] = []

    def _scan(vk: ValueKey) -> None:
        # A re-aggregation root is opaque below itself — its constituents belong
        # to its carrier, not to a separate main-query attach.
        if is_reaggregation_key(vk):
            if vk not in seen:
                seen.add(vk)
                out.append(vk)
            return
        for c in vk.children():
            _scan(c)

    for dm in prebound.declared_measures:
        _scan(dm.bound.value_key)
    for sp in prebound.order_specs:
        _scan(sp.bound.value_key)
    for bf in prebound.bound_filters:
        _scan(bf.value_key)
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
    models_by_name: Dict[str, SlayerModel],
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
            models_by_name=models_by_name,
        )
    ))


def _reaggregation_determined(
    *, key: ValueKey, union_grain: Grain, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
) -> bool:
    """Is an outer dimension determined by the operand dataset's union grain?
    Delegates to the one determination rule: a grain member, or a
    column reached over provably to-one hops from a model the grain pins."""
    return grain_determines(
        key=key, grain=union_grain, host_model=host_model,
        models_by_name=models_by_name,
    )


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

    # Constituents and their grains; a constituent with no declared partition is
    # typed at the query's dimensions. The union grain is the carrier grain.
    constituents = operand_aggregates(root.source)
    union_grain = Grain.EMPTY
    for c in constituents:
        cg = Grain.of(c.partition_keys) if c.partition_keys is not None else Grain.of(proj)
        union_grain = union_grain | cg

    # A clean, stable name for the re-aggregation (the nested-aggregate source
    # has no canonical alias of its own).
    inner_alias = (
        (canonical_aggregate_alias(constituents[0], profile="stage_formula")
         if constituents else None)
        or (constituents[0].agg if constituents else "reagg")
    )
    alias = (
        public_alias
        or canonical_aggregate_alias(root, profile="stage_formula")
        or f"{root.agg}_{inner_alias}"
    )

    # Checked before TD resolution — name the combination, not a misleading TD error.
    check_reaggregation_no_window(alias=alias, window_val=window_kwarg_of(root))
    # Type each outer parameter against the operand grain; a legal
    # aggregate-valued parameter rides the carrier as an extra constituent, a
    # legal column parameter is picked once per cell. The residue is a typed error.
    reagg_param_specs = _resolve_aggregation_params(
        agg=root, owner_model=host_model, owner_path=(), bundle=bundle,
    )
    for _ps in reagg_param_specs:
        check_parameter_determined(
            alias=alias, param_name=_ps.name, grain_display=_grain_display(union_grain),
            determined=_param_is_determined(
                spec=_ps, owner_model=host_model, owner_path=(),
                grain=union_grain, host_model=host_model,
                models_by_name=models_by_name,
            ),
        )
        if isinstance(_ps.key, AggregateKey) and _ps.key not in constituents:
            constituents.append(_ps.key)

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
            models_by_name=models_by_name,
        ):
            attributable.append(g)
        elif _grain_expression_determined(
            key=g, union_grain=union_grain, host_model=host_model,
            models_by_name=models_by_name,
        ):
            attributable.append(g)
            expression_determined.append(g)
        else:
            if key_host_path(g) and grain_member_attributable(
                key=g, target_path=(), root_model=host_model,
                models_by_name=models_by_name, host_name=host_model.name,
            ):
                # Reachable to-one but not SEEDED by the operand grain.
                reason = (
                    "not determined by the operand grain — add the join's "
                    "entity key to the inner partition_by="
                )
            else:
                reason = broadcast_reason(
                    host_path=key_host_path(g), target_path=(),
                    root_model=host_model,
                    models_by_name=models_by_name, host_name=host_model.name,
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
    carrier_attach = _build_carrier_attach(
        union_grain=union_grain, constituents=constituents,
        constituent_placeholders=constituent_placeholders, host_model=host_model,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        inherited=inherited, n_date_range=n_date_range,
        producer_source_model=host_model.name, producer_registry=producer_registry,
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
    outer_plan = compile_prebound(
        query=StrictQueryCarrier(
            source_model=host_model.name, prebound=outer_prebound,
        ),
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        # An expression grain key (in-grain computed dim) desugars its own
        # nested row attach inside the outer producer.
        enable_producer_regroups=any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(pk)
            for pk in ordered_outer
        ),
        prebound=outer_prebound, producer_registry=producer_registry,
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
    *,
    union_grain: Grain,
    constituents: List[AggregateKey],
    constituent_placeholders: Dict[ValueKey, ValueKey],
    host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
    inherited: List[BoundFilter],
    n_date_range: int,
    producer_source_model: Optional[str],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
) -> RegroupAttachPlan:
    """A row-attach producer at the union grain carrying every constituent (coarser
    ones broadcast within it) — the carrier / level-1 of the re-aggregation."""
    carrier_prebound, ordered_pks = _regroup_producer_prebound(
        pks=union_grain, aggs=constituents, model=host_model, bundle=bundle,
        inherited=inherited, n_date_range=n_date_range,
    )
    carrier_plan = compile_prebound(
        query=StrictQueryCarrier(
            source_model=producer_source_model, prebound=carrier_prebound,
        ),
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        # Discovery must re-run inside the carrier for a coarser constituent
        # (nested broadcast), a constituent that is itself a re-aggregation, or
        # an expression grain key needing its own nested row attach.
        enable_producer_regroups=any(
            (c.partition_keys is not None
             and Grain.of(c.partition_keys) != union_grain)
            or is_reaggregation_key(c)
            for c in constituents
        ) or any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or is_local_partitioned_agg(pk)
            for pk in union_grain
        ),
        prebound=carrier_prebound, producer_registry=producer_registry,
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
        alias_hint=(
            (canonical_aggregate_alias(constituents[0], profile="stage_formula")
             if constituents else None)
            or (constituents[0].agg if constituents else None)
            or "carrier"
        ),
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
) -> Optional[Tuple[PreboundQuery, List[RegroupAttachPlan]]]:
    """Discover partitioned aggregates and desugar into producer stages + reserved-leaf placeholders (row attach at base FROM, combined at the combined SELECT)."""
    # DEV-1847: re-aggregation roots — an aggregate whose operand resolves to
    # attached values. Pre-substitute each with a placeholder so the normal
    # discovery below treats it opaquely (its constituents belong to the carrier,
    # not a main-query attach); the producer-over-producer is synthesized later.
    registry = RegroupPlaceholderRegistry()
    reagg_roots = _discover_reaggregation_roots(prebound) if local_discovery else []
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
        grain, windowed = _effective_root_grain(
            agg, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        # Fold the windowed axis back in: a bare windowed measure IS its producer's answer, so it must be excluded.
        if windowed and active_bucket is not None:
            grain = grain | {active_bucket}
        return grain

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
        lb_models = {m.name: m for m in bundle.referenced_models}
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
                    models_by_name=lb_models, host_name=host_for_local.name,
                )
                for g in lb_grain
            )

        seen_lb: set = set()
        local_broadcast: List[ValueKey] = []

        def _scan_lb(vk: ValueKey) -> None:
            for k in walk_value_keys(vk):
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
            grain, windowed = _effective_root_grain(
                agg, projected_dim_keys=projected_dim_keys,
                projected_td_keys=projected_td_keys, active_bucket=active_bucket,
            )
            ident = _windowed_or_ranked_identity(agg)
            # A crossing-input root needs its OWN producer, else another aggregate's crossed joins fan its rows.
            if ident is None and _is_crossing_local_root(agg):
                ident = ("crossing", agg.source, agg.agg, tuple(agg.args),
                         tuple(agg.kwargs), agg.column_filter_key)
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
            producer_plan = compile_prebound(
                query=StrictQueryCarrier(
                    source_model=producer_source_model, prebound=producer_prebound,
                ),
                bundle=bundle,
                scope=scope,
                stage_schemas=stage_schemas,
                disable_host_rooted_isolation=True,
                # A producer re-runs regroup discovery for its strict-subset inner
                # aggregates and for a computed / bare-partitioned dimension in its
                # grain (which needs a nested row attach to group by its value).
                enable_producer_regroups=(
                    (not windowed) or any(
                        isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
                        or is_local_partitioned_agg(pk)
                        for pk in pks
                    ) or any(isinstance(a, TransformKey) for a in producer_aggs)
                ),
                prebound=producer_prebound,
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
    models_by_name_cm = {m.name: m for m in bundle.referenced_models}
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
        stage_schemas=stage_schemas,
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

    # The ROW substitution applies ONLY to computed DIMENSIONS; a non-dim measure keeps query-grain (its inners desugar to COMBINED placeholders).
    combined_mapping: Dict[ValueKey, ValueKey] = {
        agg: mapping[agg] for agg in (*combined_aggs, *cm_combined)
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


def compile_prebound(  # NOSONAR(S3776) — compiler entry-point dispatcher. The pre-existing complexity is owned by the multi-stage scope / bundle / projection / filter-routing wiring it orchestrates and is tracked as a separate refactor.
    *,
    query: Union[SlayerQuery, StrictQueryCarrier],
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: PreboundQuery,
    filter_typings: Optional[List[ConjunctTyping]] = None,
    env: Optional[ElaboratedQuery] = None,
    disable_host_rooted_isolation: bool = False,
    enable_producer_regroups: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile one typed prebound into a ``PlannedQuery``; ``disable_host_rooted_isolation`` suppresses the LOCAL half of the regroup desugar (recursion guard)."""
    stage_schemas = stage_schemas or {}
    # One interning registry per top-level plan; nested producer calls thread it down.
    if producer_registry is None:
        producer_registry = {}

    # The generator renders FROM / joins against the binder's model (ModelScope → host; StageSchema → None).
    render_source_model = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )

    if filter_typings is None:
        # A compiler-synthesized sub-plan arrives untyped: resolve-then-type
        # every filter conjunct and order target at its original checkpoints
        # (a disabled sub-plan types without splitting); the top-level entry
        # arrives typed by ``elaborate_query`` with its environment attached.
        prebound, filter_typings = type_and_split_filters(
            prebound,
            crossing_root=(
                crossing_local_root_predicate(scope=scope, bundle=bundle)
                if not disable_host_rooted_isolation else None
            ),
            split=not disable_host_rooted_isolation,
        )
        type_order_positions(prebound)
    declared_measures = list(prebound.declared_measures)
    bound_filters = list(prebound.bound_filters)
    n_date_range = prebound.n_date_range
    order_specs = list(prebound.order_specs)
    active_td_key = prebound.main_time_key
    n_dims = prebound.n_dims
    n_tds = prebound.n_time_dimensions
    distinct_dimension_values = prebound.distinct_dimension_values
    # Pre-substitution measure roots, positionally aligned with env.measures.
    _coh_measure_roots = (
        [dm.bound.value_key for dm in declared_measures[n_dims + n_tds:]]
        if env is not None else []
    )

    # Desugar partitioned aggregates into producer stages + reserved-leaf placeholders.
    regroup_attach_plans: List[RegroupAttachPlan] = []
    if isinstance(query.source_model, str):
        _producer_source_model = query.source_model
    elif render_source_model is not None:
        _producer_source_model = render_source_model.name
    else:
        _producer_source_model = None
    # The desugar always runs; the LOCAL half is suppressed in a disabled sub-plan, cross-model roots always desugar.
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
    if env is not None:
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
            and getattr(key.source, "path", ())
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
        columns.append(StageColumn(
            name=flat,
            sql_alias=flat,
            public_alias=alias,
            type=slot.type,
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
    # Fallback: any remaining slots (shouldn't happen) appended in input order.
    seen = set(ordered_ids)
    for s in transform_slots:
        if s.id not in seen:
            ordered_ids.append(s.id)
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
