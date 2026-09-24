"""The stage compiler: one elaborated stage or synthesized producer → ``PlannedQuery``.
Binding lives in ``bind_inputs``; typing and the checker in ``elaborate_env``."""

from __future__ import annotations

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
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType, JoinType, RANKED_AGGREGATIONS, TimeGranularity
from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError
from slayer.core.keys import AggregateKey, Grain, ArithmeticKey, BetweenKey, ColumnKey, ColumnSqlKey, InKey, LiteralKey, Phase, PREDICATE_COMPARISON_OPS, ScalarCallKey, StarKey, TimeTruncKey, TransformKey, ValueKey, column_leaf, effective_root_grain, constituent_grain, attached_parameter_grain, substitute_value_keys, substitute_consumer_keys, walk_value_keys, walk_consumer_keys, REGROUP_LEAF_PREFIX, is_cross_model_agg, split_top_level_and, window_kwarg_of, is_row_attach_root, attached_inputs, operand_aggregates, operand_constituents, source_anchor_path, source_row_leaves
from slayer.core.models import Column, SlayerModel
from slayer.engine.reference_closure import (
    ParamSpec,
    aggregate_input_closure,
    column_default_key,
    first_unanalyzable_filter_column,
    first_unanalyzable_input_column,
    first_unanalyzable_source_row_leaf,
    fragment_closure,
    fragment_null_propagates,
    key_closure,
    requalify_expr_to_paths,
    resolve_aggregation_params,
    source_row_leaf_closure,
)
from slayer.core.join_walker import physical_join_pairs, resolve_hop, walk
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
    check_parameter_determined,
    check_filter_dependencies_analyzable,
    check_raw_rows_no_aggregate_slots,
    check_reaggregation_dims_attributable,
    check_reaggregation_no_window,
    check_reaggregation_partition_key_is_query_dim,
    check_windowed_time_axis_attributable,
    check_window_duration,
    check_windowed_time_dimension,
)
from slayer.engine.elaborate import elaborate_synthesized
from slayer.ir.bound import BoundExpr, BoundFilter, DeclaredMeasure, OrderSpec, bound_filter_from_key
from slayer.engine.compile.discovery import RootDisposition, discover_roots, row_attach_inputs
from slayer.ir.elaborated import ConjunctTyping, ElaboratedProducer, ElaboratedQuery, ElaboratedStage
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
    explicit_ranking_time_arg,
    ordered_row_keys,
    resolve_ranking_time_key,
)
from slayer.engine.compile.projection import (
    ProjectionPlanner,
    _canonical_name,
    _iter_slot_deps,
)
from slayer.engine.compile.shift import carried_placeholders
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
    "compile_stage",
    "compile_synthesized",
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
            check_window_duration(window_val=window_kwarg_of(key))

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
    public_alias_by_agg: Optional[Mapping[ValueKey, str]] = None,
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
    # Non-aggregate constituents (transforms, arithmetic, scalar) fall back to a
    # bare op/name, so two same-op transforms — ``sum(cumsum(a) - cumsum(b))`` —
    # would collide. Disambiguate against the names already taken (grain + prior
    # constituents); the first occurrence keeps its bare name. Wiring is by key,
    # so the label only needs to be unique.
    used_names: set[str] = {
        dm.public_name for dm in grain_dms if dm.public_name is not None
    }

    def _unique(name: str) -> str:
        candidate = name
        i = 2
        while candidate in used_names:
            candidate = f"{name}_{i}"
            i += 1
        used_names.add(candidate)
        return candidate

    agg_dms: List[DeclaredMeasure] = []
    for agg in aggs:
        canonical = _unique(
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
) -> List[Tuple[str, str]]:
    """``[(leaf, reason)]`` for the first explicit column argument (positional or
    kwarg, HOST coordinates) not attributable from the root, judged on its
    dependency closure; host_name lets an off-home input traverse a proven reverse hop."""
    for arg in (*agg.args, *(v for _, v in agg.kwargs)):
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        if host_model is not None:
            attributable = key_attributable_from_root(
                key=arg, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, bundle=bundle,
                host_model=host_model, host_name=host_name,
            )
        else:
            attributable = attributable_from_root(
                host_path=key_host_path(arg), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_name,
            )
        if attributable:
            continue
        leaf = column_leaf(arg.column if isinstance(arg, TimeTruncKey) else arg)
        reason = (
            key_broadcast_reason(
                key=arg, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, bundle=bundle,
                host_model=host_model, host_name=host_name,
            )
            if host_model is not None
            else broadcast_reason(
                host_path=key_host_path(arg), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_name,
            )
        )
        return [(leaf, reason)]
    return []


def _assert_cross_model_inputs_safe(
    *, agg: AggregateKey, agg_rooted: AggregateKey, root_model: SlayerModel,
    root_name: str, target_path: Tuple[str, ...], bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
    host_model: Optional[SlayerModel] = None,
) -> None:
    """Resolve every cross-model input's attributability from its root; the checker
    raises on a fanning/unproven join or an unanalysable derived dependency. Attached
    inputs are opaque (Axiom 2.3): masked by a literal, judged by their own producer."""
    alias = canonical_aggregate_alias(agg, profile="stage_formula")
    attached = attached_inputs(agg_rooted)
    if attached:
        agg_rooted = substitute_value_keys(
            key=agg_rooted,
            mapping={a: LiteralKey(value=Decimal(1)) for a in attached},
        )
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
    # An explicit argument's violation names the column and its hop, ahead of the
    # closure's hop-only message.
    unattributable_arg_leaves = _first_unattributable_arg_leaf(
        agg=agg, target_path=target_path, root_model=root_model,
        models_by_name=models_by_name, host_name=host_name,
        bundle=bundle, host_model=host_model,
    )
    unsafe_input_hops = [] if unattributable_arg_leaves else _first_unsafe_input_hop(
        paths=paths, root_model=root_model, root_name=root_name,
        models_by_name=models_by_name,
    )
    check_cross_model_inputs_safe(
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


def _cross_model_inherited_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
    host_model: SlayerModel, bundle: ResolvedSourceBundle,
) -> Tuple[List[BoundFilter], List[SemiJoinFilter]]:
    """Split base ROW filters into conjuncts and dispose each two ways: fully
    attributable → inherits inline (re-rooted); else pushed as a correlated
    EXISTS semi-join (grouped by shared branch, D3/D4)."""
    inherited: List[BoundFilter] = []
    pushes: List[_Push] = []
    for bf, text in base_filters:
        if bf.phase != Phase.ROW:
            continue
        for cj in split_top_level_and(bf.value_key):
            inherited_bf, pushed = _conjunct_disposition(
                cj, text=text, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_name,
                host_model=host_model, bundle=bundle,
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
    order (DEV-1935 D4). Each hop's ``null_extended`` = declared LEFT ∧ the group's
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
    *, key: ValueKey, root_model: SlayerModel, bundle: ResolvedSourceBundle,
    alias: Optional[str],
) -> Optional[Tuple[str, str]]:
    """The ``(leaf, hop)`` a ranked aggregate's ordering key crosses unsafely from
    ``root_model``, judged by its dependency closure (engine P10); ``None`` when every
    path is provably to-one. An unanalysable definition fails closed through
    ``check_input_dependencies_analyzable`` naming the column (Axiom 2.8)."""
    leaf = column_leaf(key) if isinstance(key, (ColumnKey, ColumnSqlKey)) else str(key)
    closure = key_closure(
        key=key, anchor_model=root_model, anchor_relation=root_model.name, bundle=bundle,
    )
    if closure is None:
        check_input_dependencies_analyzable(alias=alias, column=leaf)
        closure = ()
    for path in closure:
        if not safe_reachable(
            root=root_model, path=path, models_by_name=bundle.models_by_name,
        ):
            return leaf, path[-1]
    return None


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

    alias = canonical_aggregate_alias(agg, profile="stage_formula")
    # Role: crossed ranking argument, explicitly named (a first/last ranking arg),
    # judged by its closure so a derived argument names itself (D1).
    ranked_crossings: List[Tuple[str, str]] = []
    ranking_arg = explicit_ranking_time_arg(agg)
    if ranking_arg is not None:
        crossing = _ranking_key_crossing(
            key=ranking_arg, root_model=host_model, bundle=bundle, alias=alias,
        )
        if crossing is not None:
            ranked_crossings.append(crossing)

    # Crossed predicate + remaining crossed args; the SOURCE's own crossings are separate.
    gated_crossings: List[str] = []
    source_crossings: List[str] = []
    if not ranked_crossings:
        gated = aggregate_input_closure(
            key=agg, anchor_model=host_model, anchor_relation=host_model.name,
            bundle=bundle, include_source=False,
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


def _checked_ranking_time_key(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    target_rooted: bool,
) -> ValueKey:
    """Resolve a ranked producer's ordering key, then judge it by closure (D5): a
    crossing key raises through the same checker its explicit spelling uses — the
    cross-model attributability form when target-rooted, the local ranked form else."""
    resolved = resolve_ranking_time_key(
        key=agg_key, root_model=root_model, bundle=bundle,
        row_keys=ordered_row_keys(
            row_slots=producer_plan.row_slots,
            public_projection=producer_plan.projection,
        ),
    )
    crossing = _ranking_key_crossing(
        key=resolved, root_model=root_model, bundle=bundle, alias=alias,
    )
    if crossing is not None:
        leaf, hop = crossing
        if target_rooted:
            check_cross_model_inputs_safe(
                alias=alias, root_name=root_model.name, unsafe_input_hops=[],
                unattributable_arg_leaves=[(leaf, key_broadcast_reason(
                    key=resolved, target_path=(), root_model=root_model,
                    models_by_name=bundle.models_by_name, bundle=bundle,
                    host_model=root_model, host_name=root_model.name,
                ))],
            )
        else:
            check_local_producer_inputs_safe(
                alias=alias, host=root_model.name,
                ranked_crossings=[(leaf, hop)], gated_crossings=[],
            )
    return resolved


def _trailing_window_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    target_rooted: bool,
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
    # first/last rank the interval rows by the same closure-judged key plain first/last would.
    ranking_time_key = (
        _checked_ranking_time_key(
            producer_plan=producer_plan, agg_key=agg_key, root_model=root_model,
            bundle=bundle, alias=alias, target_rooted=target_rooted,
        )
        if agg_key.agg in RANKED_AGGREGATIONS else None
    )
    # Reference-bearing parameters read per interval row (D4): resolved on the
    # source owner, each key remapped through the sub-plan's regroup substitutions
    # so an attached-aggregate parameter reads its row-attach column (association
    # precedent). Literals are omitted by resolve_aggregation_params.
    source_path = source_anchor_path(agg_key.source)
    owner_model = walk_key_path(
        model=root_model, path=source_path, bundle=bundle,
    ) or root_model
    # The producer roots at the home (``root_model``), so the resolver already
    # returns the picked expression default in producer coordinates (D8); it enters
    # at the producer root.
    picked_params = [
        PickedParam(name=ps.name, key=ps.key, sql=ps.expr_sql)
        for ps in resolve_aggregation_params(
            agg=agg_key, owner_model=owner_model, owner_path=source_path,
            bundle=bundle, root_model=root_model,
        )
    ]
    param_subst = {
        sub.original_key: sub.placeholder
        for a in producer_plan.regroup_attach_plans
        for sub in a.substitutions
    }
    if param_subst:
        picked_params = [
            pp.model_copy(update={
                "key": substitute_value_keys(key=pp.key, mapping=param_subst),
            }) if pp.key is not None else pp
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
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    alias: Optional[str],
    target_rooted: bool,
) -> RankedProducerKernel:
    return RankedProducerKernel(
        agg=agg_key.agg,
        ranking_time_key=_checked_ranking_time_key(
            producer_plan=producer_plan, agg_key=agg_key, root_model=root_model,
            bundle=bundle, alias=alias, target_rooted=target_rooted,
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
    population: "Population",
) -> RegroupAttachPlan:
    """A host-grain ORDER-BY wrap as a HOST-rooted producer synthesized late: a combined attach at the full projected grain whose placeholder IS the wrap key. Inherits the population disposition (D1) like every other host-rooted producer."""
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
        prebound=producer_prebound,
        source_model=producer_source_model,
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


#: Answer name of a composite shifted producer — offset-free, so offsets intern.
_SHIFTED_ANSWER = "shifted"


def _frame_free_filters(
    *, prebound: PreboundQuery, filter_typings: Sequence[ConjunctTyping],
    time_columns: AbstractSet[ValueKey],
) -> List[BoundFilter]:
    """Every field mask of the population (any stratum) minus every frame bound."""
    out: List[BoundFilter] = []
    for idx, (bf, ct) in enumerate(zip(prebound.bound_filters, filter_typings)):
        if ct.typing != MaskTyping.FIELD or idx < prebound.n_date_range:
            continue
        residual = strip_frame_bounds(key=bf.value_key, time_columns=time_columns)
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
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_model: Optional[str],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    population: "Population",
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
    grain = [dm.bound.value_key for dm in prebound.declared_measures[:n_grain]]
    host_grain = [dm.bound.value_key for dm in rewritten.declared_measures[:n_grain]]
    dim_keys = grain[:prebound.n_dims]
    td_keys = grain[prebound.n_dims:]
    consumer_order = {k: i for i, k in enumerate(grain)}
    grain_name_by_key = {
        dm.bound.value_key: dm.declared_name
        for dm in prebound.declared_measures[:n_grain] if dm.declared_name is not None
    }
    inherited = _frame_free_filters(
        prebound=prebound, filter_typings=filter_typings,
        time_columns=frozenset(k.column for k in td_keys if isinstance(k, TimeTruncKey)),
    )
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    # A bare answer is named like its combined producer: the measure's public name.
    public_alias = {
        dm.bound.value_key: dm.public_name
        for dm in reversed(prebound.declared_measures[n_grain:]) if dm.public_name is not None
    }
    out: List[RegroupAttachPlan] = []
    for slot in shift_slots:
        key = slot.key
        assert isinstance(key, TransformKey) and key.time_key is not None
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
        producer_prebound, ordered_pks = _regroup_producer_prebound(
            pks=pks, aggs=[answer], model=producer_model, bundle=bundle,
            inherited=inherited, n_date_range=0,
            partition_order=lambda pks: sorted(
                pks, key=lambda k: consumer_order.get(k, len(consumer_order)),
            ),
            public_alias_by_agg=aliases,
            grain_name_by_key=grain_name_by_key,
            window_td_key=window_td,
            to_many_handling=prebound.to_many_handling,
        )
        producer_plan = compile_synthesized(
            prebound=producer_prebound,
            source_model=producer_source_model,
            bundle=bundle, scope=scope, stage_schemas=stage_schemas,
            producer_registry=producer_registry,
            population=population,
            carried_attaches=carried_attaches,
            reserved_placeholders=frozenset(to_original),
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
        kernel_kwargs = _shifted_kernel_kwargs(
            answer=answer, own_grain=own_grain, producer_plan=producer_plan,
            bundle=bundle,
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
        return public_alias, canonical_aggregate_alias(answer, profile="stage_formula") \
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
    *, answer: ValueKey, resolved: ValueKey, axis: ValueKey, grain: List[ValueKey],
    dim_keys: List[ValueKey], td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Tuple[Grain, Optional[ValueKey], bool]:
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


def _shifted_kernel_kwargs(
    *, answer: ValueKey, own_grain: bool, producer_plan: PlannedQuery,
    bundle: ResolvedSourceBundle,
) -> Dict[str, Any]:
    """The windowed / ranked kernel of a bare own-grain shifted leaf, if any."""
    kernel_root = producer_plan.render_source_model or bundle.source_model
    if not (own_grain and isinstance(answer, AggregateKey) and kernel_root is not None
            and not is_cross_model_agg(answer)
            and _windowed_or_ranked_identity(answer) is not None):
        return {}
    make_kernel = (
        _trailing_window_kernel if window_kwarg_of(answer) is not None
        else _ranked_kernel
    )
    return {"kernel": make_kernel(
        producer_plan=producer_plan, agg_key=answer, root_model=kernel_root,
        bundle=bundle, alias=canonical_aggregate_alias(answer, profile="stage_formula"),
        target_rooted=False,
    )}


class _PushBlocked(Exception):
    """A conjunct outside semi-join pushdown scope; the message is the warning reason."""


def _owning_model(
    name: str, *, host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel],
) -> Optional[SlayerModel]:
    if host_model is not None and name == host_model.name:
        return host_model
    return models_by_name.get(name)


def _derived_column_owner(
    col: ColumnSqlKey, *, host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel],
) -> Optional[Tuple[SlayerModel, Column]]:
    """The derived column a ``ColumnSqlKey`` names, with its owning model."""
    owner = _owning_model(
        col.model, host_model=host_model, models_by_name=models_by_name,
    )
    column = None if owner is None else next(
        (c for c in owner.columns if c.name == col.column_name), None)
    return None if owner is None or column is None else (owner, column)


def _ref_sql_dependency_paths(
    col: ValueKey, *, host_model: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel], bundle: Optional[ResolvedSourceBundle],
    include_filter: bool = True,
) -> Tuple[Tuple[str, ...], ...]:
    """Owner-relative join paths a derived column's ``Column.sql`` AND its
    ``Column.filter`` (DEV-1832) cross (the dependency closure at the owner) — the
    semi-join push tree registers a hop for each; ``include_filter=False`` keeps
    the value definition's paths only. ``bundle.models_by_name`` is host-inclusive
    (DEV-1900), so a dep pointing back at the host resolves without a hand-patched
    bundle."""
    if not isinstance(col, ColumnSqlKey) or bundle is None:
        return ()
    found = _derived_column_owner(
        col, host_model=host_model, models_by_name=models_by_name,
    )
    if found is None:
        return ()
    owner, column = found
    paths: List[Tuple[str, ...]] = []
    for sql in (column.sql, column.filter if include_filter else None):
        if not sql:
            continue
        frag = fragment_closure(
            sql=sql, model=owner, owner_path=(),
            anchor_relation=owner.name, bundle=bundle,
        )
        if frag is None:
            # None = no dialect could analyse the fragment (≠ () = analysed, local).
            # Fail closed: we can't determine the hops it crosses, so block the push.
            raise _PushBlocked(
                f"unanalyzable definition fragment on {owner.name}.{column.name} "
                f"— cannot determine the semi-join hops it crosses"
            )
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
    target_model: str, pairs: List[Tuple[str, str]], declared_left: bool,
) -> None:
    nodes.setdefault(node_path, SemiJoinHop(
        target_model=target_model,
        join_pairs=tuple((s, t) for s, t in pairs),
        node_path=node_path,
        declared_left=declared_left,
    ))


def _forward_hops(
    *, start_model: SlayerModel, rel_path: Tuple[str, ...],
    base_node_path: Tuple[str, ...], models_by_name: Dict[str, SlayerModel],
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> Tuple[str, ...]:
    """Register hops along ``rel_path`` through the shared walker (reverse hops
    and edge-name tokens included); returns the final node path. The node-path
    token is the edge's CANONICAL token (its ``name`` when named, else its
    ``target_model``) so two spellings of one physical edge share one node and
    one alias (DEV-1935 D3); the hop's ``target_model`` is the resolved model.
    An ambiguous hop raises (fail closed)."""
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
        node_path = (*node_path, edge.name or edge.target_model)
        _register_hop(
            nodes, node_path=node_path, target_model=edge.target_model,
            pairs=physical_join_pairs(edge=edge, source=current, target=target),
            declared_left=edge.join_type == JoinType.LEFT,
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
    try:
        fwd = walk(root=host_model, path=target_path, models_by_name=models_by_name)
    except CircularJoinPathError:
        fwd = None  # a revisiting reverse chain blocks the push (one raise site)
    if fwd is None:
        raise _PushBlocked(
            f"unreachable from the aggregate's root (join path "
            f"{'.'.join(target_path)!r} does not resolve from "
            f"{host_model.name})"
        )
    node_path: Tuple[str, ...] = ()
    by_name = {**models_by_name, host_model.name: host_model}
    for edge in reversed(fwd):
        node_path = (*node_path, edge.name or edge.source_model)
        physical = physical_join_pairs(
            edge=edge, source=by_name[edge.source_model], target=by_name[edge.target_model],
        )
        # A reverse hop is the population correlation, not a declared join:
        # never null-extended.
        _register_hop(
            nodes, node_path=node_path, target_model=edge.source_model,
            pairs=[(tgt, src) for src, tgt in physical],
            declared_left=False,
        )
    return node_path


def _remap_ref_path(r: ValueKey, node_path: Tuple[str, ...]) -> ValueKey:
    if isinstance(r, TimeTruncKey):
        return r.model_copy(update={
            "column": r.column.model_copy(update={"path": node_path}),
        })
    return r.model_copy(update={"path": node_path})


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
        # ``host_node`` tokens are canonical (edge name when named), so resolve
        # the base model through the node registry, not a token→model lookup.
        base_node = host_node[: len(tp) - shared]
        base_model = (
            lookup[nodes[base_node].target_model] if base_node else root_model
        )
        return base_model, base_node, hp[shared:], host_node
    return host_model, host_node, hp, host_node


def _register_dep_hops(
    col: ValueKey, *, node_path: Tuple[str, ...], host_model: SlayerModel,
    lookup: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
    nodes: Dict[Tuple[str, ...], SemiJoinHop],
) -> List[Tuple[str, ...]]:
    """Register the hops a ref's Mode-A dependencies cross; return the terminal
    node paths of the VALUE definition's dependencies only — a filtered column is
    ``CASE WHEN f THEN v END``, NULL when ``v`` is, never because ``f``'s
    dependencies are (D5)."""
    dep_rels = _ref_sql_dependency_paths(
        col, host_model=host_model, models_by_name=lookup, bundle=bundle,
    )
    if not dep_rels:
        return []
    value_rels = set(_ref_sql_dependency_paths(
        col, host_model=host_model, models_by_name=lookup, bundle=bundle,
        include_filter=False,
    ))
    owner = _owning_model(
        col.model, host_model=host_model, models_by_name=lookup,
    )
    terminals: List[Tuple[str, ...]] = []
    for dep_rel in dep_rels:
        terminal = _forward_hops(
            start_model=owner, rel_path=tuple(dep_rel),
            base_node_path=node_path, models_by_name=lookup, nodes=nodes,
        )
        if dep_rel in value_rels:
            terminals.append(terminal)
    return terminals


# --------------------------------------------------------------------------- #
# Null-rejection analysis (DEV-1935 D5). A conservative three-valued evaluation
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
    if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey)):
        return any(_is_prefix(h, p) for p in srcs.get(k, frozenset()))
    if isinstance(k, (InKey, BetweenKey)) or (
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
        return _unknown_if_null((cj.column,), h=h, srcs=srcs)
    if isinstance(cj, BetweenKey):
        return _unknown_if_null((cj.column, cj.low, cj.high), h=h, srcs=srcs)
    if not isinstance(cj, ArithmeticKey):
        return _D
    op = cj.op.lower()
    if op in ("and", "or"):
        vals = [_pred_value(o, h=h, srcs=srcs) for o in cj.operands]
        return _fold(vals=vals, order=_AND_ORDER if op == "and" else _OR_ORDER)
    if op == "not":
        return _NOT_VALUE[_pred_value(cj.operands[0], h=h, srcs=srcs)]
    if op in ("is", "is not"):
        return _is_value(
            a=cj.operands[0], b=cj.operands[1], negated=op == "is not", h=h, srcs=srcs,
        )
    if op in PREDICATE_COMPARISON_OPS:
        return _unknown_if_null(cj.operands, h=h, srcs=srcs)
    return _D


def _ref_null_propagates(
    col: ValueKey, *, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> bool:
    """A structural ref is NULL under a NULL node; a derived ref only when its
    expanded definition propagates NULL (D5), else it is data-dependent."""
    if not isinstance(col, ColumnSqlKey):
        return True
    found = _derived_column_owner(
        col, host_model=host_model, models_by_name=models_by_name,
    )
    return found is not None and fragment_null_propagates(
        column=found[1], model=found[0], anchor_relation=found[0].name,
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
    cj: ValueKey, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    host_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    bundle: ResolvedSourceBundle, host_name: Optional[str],
) -> Tuple[ValueKey, Dict[Tuple[str, ...], SemiJoinHop], Dict[Tuple[str, ...], bool]]:
    """Resolve a not-fully-attributable conjunct into a correlation-tree plan:
    the conjunct rewritten into producer-root coordinates (ref paths = tree-node
    paths, root-local refs correlate as outer references), the hop registry, and
    per-hop null-rejection (D5). Raises :class:`_PushBlocked` when a reference has
    no resolvable join path."""
    tp = tuple(target_path)
    lookup = dict(models_by_name)
    lookup.setdefault(host_model.name, host_model)
    nodes: Dict[Tuple[str, ...], SemiJoinHop] = {}
    mapping: Dict[ValueKey, ValueKey] = {}
    srcs: Dict[ValueKey, "frozenset[Tuple[str, ...]]"] = {}
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
        dep_terminals = _register_dep_hops(
            col, node_path=node_path, host_model=host_model, lookup=lookup,
            bundle=bundle, nodes=nodes,
        )
        remapped = _remap_ref_path(r, node_path)
        # Null sources: a ref is null when its own node, an ancestor or a
        # dependency terminal is — unless data-dependent (a star, a
        # non-propagating derived definition).
        if isinstance(r, StarKey) or not _ref_null_propagates(
            col, host_model=host_model, models_by_name=lookup, bundle=bundle,
        ):
            srcs[remapped] = frozenset()
        else:
            srcs[remapped] = frozenset({node_path, *dep_terminals})
        if remapped != r:
            mapping[r] = remapped
    # DEV-1935: pushdown is total over the conjunct's boolean shape — no OR/NOT
    # or single-branch block. Multiple first-level hops are a product; grouping
    # (union-find) merges shared branches downstream.
    remapped_cj = substitute_value_keys(cj, mapping)
    rejects = _conjunct_rejects_by_hop(
        remapped_cj, hop_paths=nodes.keys(), srcs=srcs)
    return remapped_cj, nodes, rejects


def _conjunct_disposition(
    cj: ValueKey, *, text: Optional[str], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: str, host_model: SlayerModel, bundle: ResolvedSourceBundle,
) -> Tuple[Optional[BoundFilter], Optional[_Push]]:
    """Two-way ROW-conjunct disposition (D1): inline / semi-join pushed. A
    bound conjunct always resolves from the host, so a push-plan block is an
    invariant violation and propagates (fail closed), never a silent drop."""
    refs = [
        k for k in walk_value_keys(cj)
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey))
    ]

    def _attributable(r: ValueKey) -> bool:
        # Judge each ref on its dependency closure (DEV-1900) — a derived column
        # crossing a fanning hop is unattributable exactly as a structural one is.
        return key_attributable_from_root(
            key=r, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, bundle=bundle,
            host_model=host_model, host_name=host_name,
        )

    if all(_attributable(r) for r in refs):
        rerooted = reroot_from_root(
            cj, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
        )
        return bound_filter_from_key(rerooted), None
    display = text or _canonical_name(cj)
    # A conjunct whose dependency closure no dialect can analyse is unsafe,
    # never 'crosses nothing' — fail closed for host and producers alike.
    if key_closure(
        key=cj, anchor_model=host_model, anchor_relation=host_model.name,
        bundle=bundle,
    ) is None:
        check_filter_dependencies_analyzable(
            filter_text=display,
            column=first_unanalyzable_filter_column(
                key=cj, anchor_model=host_model,
                anchor_relation=host_model.name, bundle=bundle,
            ),
        )
    key_rewritten, nodes, rejects = _conjunct_push_plan(
        cj, target_path=target_path, root_model=root_model,
        host_model=host_model, models_by_name=models_by_name,
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


Population = Union[InheritedPopulation, NoInheritedPopulation]


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
    host_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    bundle: ResolvedSourceBundle,
) -> _DisposedConjunct:
    """Dispose one top-level population conjunct (D1) — inline or semi-join —
    recording its fanning paths for the per-consumer same-row rule (D2)."""
    inline_bf, pushed = _conjunct_disposition(
        cj, text=text, target_path=(), root_model=host_model,
        models_by_name=models_by_name, host_name=host_model.name,
        host_model=host_model, bundle=bundle,
    )
    closure = key_closure(
        key=cj, anchor_model=host_model,
        anchor_relation=host_model.name, bundle=bundle,
    ) or ()
    fanning = tuple(
        _canonical_path(p, root=host_model, models_by_name=models_by_name)
        for p in closure
        if p and not safe_reachable(
            root=host_model, path=p, models_by_name=models_by_name,
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
    scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> Optional[PopulationFilters]:
    """Dispose the population's ROW-phase, FIELD-typed, stratum-0 filter conjuncts
    once at the host root (D1) — inline / semi-join / excluded — recording each
    conjunct's fanning paths for the per-consumer same-row rule (D2). Raises the
    analyzability error on a conjunct whose closure cannot be analysed."""
    host_model = scope.source_model if isinstance(scope, ModelScope) else None
    if host_model is None:
        return None
    models_by_name = bundle.models_by_name
    texts = prebound.bound_filter_texts
    out: List[_DisposedConjunct] = []
    for i, (bf, ct) in enumerate(zip(prebound.bound_filters, filter_typings)):
        if (bf.phase != Phase.ROW or ct.typing != MaskTyping.FIELD
                or ct.stratum != 0):
            continue
        text = texts[i] if i < len(texts) else None
        is_date = i < prebound.n_date_range
        for cj in split_top_level_and(bf.value_key):
            out.append(_dispose_one_conjunct(
                cj, text=text, is_date=is_date, host_model=host_model,
                models_by_name=models_by_name, bundle=bundle,
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
    path: Sequence[str], *, root: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
) -> Tuple[str, ...]:
    """A join path in canonical hop tokens (edge name, else target model — D3),
    so closure paths and push nodes compare regardless of spelling."""
    try:
        edges = walk(root=root, path=tuple(path), models_by_name=models_by_name)
    except CircularJoinPathError:
        return tuple(path)
    if edges is None:
        return tuple(path)
    return tuple(e.name or e.target_model for e in edges)


def _grain_closure_paths(
    grain_keys: Sequence[ValueKey], *, host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> "set[Tuple[str, ...]]":
    """Every join path (canonical) the grain keys' dependency closures cross —
    the paths a consumer's grain materialises (D2)."""
    out: "set[Tuple[str, ...]]" = set()
    for k in grain_keys:
        closure = key_closure(
            key=k, anchor_model=host_model, anchor_relation=host_model.name,
            bundle=bundle,
        )
        for p in closure or ():
            if p:
                out.add(_canonical_path(
                    p, root=host_model, models_by_name=bundle.models_by_name))
    return out


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

    # One rooting law (Axiom 2.5): re-anchor into the home's coordinates once, a
    # host-side leaf becoming a reverse-hop reference, so every attached input's
    # producer compiles at its own home whatever the mode.
    agg_rooted = reroot_from_root(
        key=agg, target_path=target_path, root_model=root_model,
        models_by_name=models_by_name, host_name=host_model.name,
    )
    # Association only when a DIMENSION is unattributable; attached inputs never need it.
    associate = mode == "associate" and bool(unattributable)
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
        # ASSOCIATION ARM (D2-4, 8): keep every unattributable dimension as a
        # rerooted grain member joined back on the host key, and dedup per home
        # entity via the association kernel — a home entity absent from the
        # population still counts in the cells its own path reaches.
        agg_rooted = agg_rooted.model_copy(update={"locus": "host"})
        picked_params, entity_keys_root, present_keys, assoc_pairs = (
            _association_arm(
                agg=agg, agg_rooted=agg_rooted, alias=alias, root_model=root_model,
                target_path=target_path, unattributable=unattributable,
                host_model=host_model, models_by_name=models_by_name,
                bundle=bundle,
            )
        )
        # The unattributable dims join back on the host key exactly like safe_pairs.
        safe_pairs = [*safe_pairs, *assoc_pairs]
        inherited, restricted_texts = _association_inline_filters(
            base_filters=base_filters_with_text, target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_model=host_model, bundle=bundle,
        )
        associated_measure = None if explicit else alias
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
        if target_path != source_anchor_path(agg.source):
            # The source sits beyond the home: render inline as a host-locus
            # aggregate joining the to-one path from the home, never a
            # source-rooted producer.
            agg_rooted = agg_rooted.model_copy(update={"locus": "host"})
        _assert_cross_model_inputs_safe(
            agg=agg, agg_rooted=agg_rooted, root_model=root_model, root_name=root_name,
            target_path=target_path, bundle=bundle, models_by_name=models_by_name,
            host_name=host_model.name, host_model=host_model,
        )

        # A windowed cross-model aggregate folds the active TD into its grain as the bucket (must be attributable from the root).
        if window_kwarg_of(agg) is not None:
            active_td = prebound.main_time_key
            check_windowed_time_axis_attributable(
                alias=alias, root_name=root_name,
                active_td_name=(
                    None if active_td is None else _regroup_grain_name(active_td)
                ),
                attributable=active_td is not None and key_attributable_from_root(
                    key=active_td, target_path=target_path,
                    root_model=root_model, models_by_name=models_by_name,
                    bundle=bundle, host_model=host_model,
                    host_name=host_model.name,
                ),
            )
            assert active_td is not None  # the checker raised otherwise
            window_td_key = reroot_from_root(
                key=active_td, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            )

        inherited, semi_joins = _cross_model_inherited_filters(
            base_filters=base_filters_with_text, target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_name=host_model.name, host_model=host_model, bundle=bundle,
        )

    # SHARED TAIL: root at the home, compile the producer, attach on the host key.
    _check_attached_params_determined(
        agg=agg, alias=alias, target_path=target_path, root_model=root_model,
        host_model=host_model, models_by_name=models_by_name, bundle=bundle,
        projected_dim_keys=projected_dim_keys, projected_td_keys=projected_td_keys,
        active_bucket=prebound.main_time_key,
    )
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
    producer_plan = compile_synthesized(
        prebound=producer_prebound,
        source_model=root_name,
        bundle=root_bundle, scope=root_scope,
        stage_schemas=stage_schemas,
        producer_registry=producer_registry,
        population=NoInheritedPopulation(
            reason="target-rooted: re-roots and disposes the inherited filters itself",
        ),
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
            root_model=root_model, bundle=root_bundle,
            alias=canonical_aggregate_alias(agg, profile="stage_formula"),
            target_rooted=True,
        )
    elif isinstance(agg_rooted, AggregateKey) and agg_rooted.agg in RANKED_AGGREGATIONS:
        cm_attach_kwargs["kernel"] = _ranked_kernel(
            producer_plan=producer_plan, agg_key=agg_rooted,
            root_model=root_model, bundle=root_bundle,
            alias=canonical_aggregate_alias(agg, profile="stage_formula"),
            target_rooted=True,
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
        semi_join_measure=alias,
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
    projected_dim_keys: Sequence[ValueKey] = (),
    projected_td_keys: Sequence[ValueKey] = (),
    active_bucket: Optional[ValueKey] = None,
) -> bool:
    """A parameter is legal iff the dataset grain determines it — the bound key,
    or (for an expression default) every column it references — judged on each
    reference's dependency closure (DEV-1900). An UNGRAINED aggregate parameter
    types at the query grain, which the operand grain always refines, so it is
    determined (DEV-1859 decision 12). A grained transform parameter (D5) resolves
    to its result grain first; the grain must determine every member."""
    if spec.key is not None:
        if isinstance(spec.key, AggregateKey) and spec.key.partition_keys is None:
            return True
        if isinstance(spec.key, TransformKey):
            pgrain = attached_parameter_grain(
                key=spec.key, projected_dim_keys=list(projected_dim_keys),
                projected_td_keys=list(projected_td_keys), active_bucket=active_bucket,
            )
            return all(
                grain_determines(
                    key=member, grain=grain, host_model=host_model,
                    models_by_name=models_by_name, bundle=bundle,
                )
                for member in (pgrain or ())
            )
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


def _home_determines_grain_member(
    *, key: ValueKey, target_path: Tuple[str, ...], root_model: SlayerModel,
    host_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    bundle: ResolvedSourceBundle,
) -> bool:
    """A grain member the home determines: a column attributable from it (as
    ``safe_pairs`` judges a dimension) or a grained aggregate whose grain it
    determines; an expression key never."""
    if isinstance(key, AggregateKey):
        return key.partition_keys is not None and all(
            _home_determines_grain_member(
                key=pk, target_path=target_path, root_model=root_model,
                host_model=host_model, models_by_name=models_by_name, bundle=bundle,
            )
            for pk in key.partition_keys
        )
    if not isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        return False
    return shared_join_key_reroot(
        key=key, target_path=target_path, host_model=host_model,
        models_by_name=models_by_name,
    ) is not None or grain_member_attributable(
        key=key, target_path=target_path, root_model=root_model,
        models_by_name=models_by_name, bundle=bundle, host_model=host_model,
        host_name=host_model.name,
    )


def _check_attached_params_determined(
    *, agg: AggregateKey, alias: str, target_path: Tuple[str, ...],
    root_model: SlayerModel, host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
    projected_dim_keys: List[ValueKey], projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> None:
    """One home-determination rule, every mode: the home determines every grain
    member of each attached parameter typed at its result grain (Axiom 2.3 / 11.4);
    an ungrained aggregate types at the query grain and is determined by
    construction (DEV-1859 decision 12). A grained transform (D5) resolves to its
    result grain first, so this predicate needs no transform arm."""
    key_sets = _unique_key_sets(root_model)
    grain_display = _grain_display(Grain.of(
        column_default_key(path=target_path, leaf=col, base=root_model)
        for col in key_sets[0]
    )) if key_sets else f"{root_model.name} rows"
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
                    key=member, target_path=target_path, root_model=root_model,
                    host_model=host_model, models_by_name=models_by_name,
                    bundle=bundle,
                )
                for member in grain
            ),
        )


def _reroot_picked_expr(
    *, spec: ParamSpec, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
) -> Optional[str]:
    """An expression default's canonical (query-root) fragment rerooted into the
    producer root ``target_path`` (D8): reroot each reference key exactly as the
    bound key is rerooted, then requalify, so it enters at the producer root — never
    as a reverse join from the owner. ``None`` / raw text ride through unchanged."""
    if spec.expr_sql is None:
        return spec.expr_sql
    abs_refs: List[Tuple[Optional[Tuple[str, ...]], str]] = []
    for ref in spec.expr_refs:
        if not isinstance(ref, (ColumnKey, ColumnSqlKey)):
            abs_refs.append((None, ""))  # None ref fails closed at typing
            continue
        rr = reroot_from_root(
            ref, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
        )
        abs_refs.append((tuple(key_host_path(rr)), column_leaf(rr)))
    return requalify_expr_to_paths(sql=spec.expr_sql, abs_refs=abs_refs)


def _association_arm(
    *, agg: AggregateKey, agg_rooted: AggregateKey, alias: str,
    root_model: SlayerModel, target_path: Tuple[str, ...],
    unattributable: List[_UnattributableDim], host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], bundle: ResolvedSourceBundle,
) -> Tuple[
    List[PickedParam], List[ValueKey], List[ValueKey],
    List[Tuple[ValueKey, ValueKey]],
]:
    """The home-rooted association arm (DEV-1910 D2-3): eligibility + mode-invariant
    input safety on the rerooted host-locus aggregate (compiled inline at its fanning
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
    _assert_cross_model_inputs_safe(
        agg=agg, agg_rooted=agg_rooted, root_model=root_model,
        root_name=root_model.name, target_path=target_path, bundle=bundle,
        models_by_name=models_by_name, host_name=host_model.name,
        host_model=host_model,
    )
    # Type each column-valued / default parameter against the ENTITY grain in HOST
    # coordinates (the aggregation reads it once per associated entity); an attached
    # one is judged by the shared home-determination rule. Lift every parameter,
    # rerooted into the home so the level-1 pick reads it there; the kernel dedups
    # by the entity key in ROOT coordinates.
    host_entity_keys: List[ValueKey] = [
        column_default_key(path=target_path, leaf=col, base=root_model)
        for col in key_sets[0]
    ]
    entity_keys_root: List[ValueKey] = [
        column_default_key(path=(), leaf=col, base=root_model) for col in key_sets[0]
    ]
    # An expression source has no ``.path``; ``key_host_path`` would silently answer
    # the root, so the source anchor names where the definition is resolved (D1).
    source_path = source_anchor_path(agg.source)
    source_model = walk_key_path(
        model=host_model, path=source_path, bundle=bundle,
    ) or root_model
    entity_grain = Grain.of(host_entity_keys)
    picked_params: List[PickedParam] = []
    for _ps in resolve_aggregation_params(
        agg=agg, owner_model=source_model, owner_path=source_path, bundle=bundle,
        root_model=host_model,
    ):
        # Attached kinds (aggregate- and transform-valued) are judged by the shared
        # home-determination tail (D5); only column / default params are column-typed here.
        if not isinstance(_ps.key, (AggregateKey, TransformKey)):
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
            sql=_reroot_picked_expr(
                spec=_ps, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            ),
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
    return picked_params, entity_keys_root, present_keys, assoc_pairs


def _association_inline_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Tuple[List[BoundFilter], List[str]]:
    """Route each ROW conjunct for a home-rooted association producer (DEV-1910
    D4): attributable → inline re-rooted; else inline the re-rooted conjunct too
    (the per-entity dedup makes the fanning join harmless) with its text kept for
    the informational entry. No semi-join is emitted, so membership equals the
    semi-join semantics and a conjunct sharing a hop with an association
    dimension binds to the same related row."""
    inherited: List[BoundFilter] = []
    restricted_texts: List[str] = []
    for bf, text in base_filters:
        if bf.phase != Phase.ROW:
            continue
        for cj in split_top_level_and(bf.value_key):
            inh, pushed = _conjunct_disposition(
                cj=cj, text=text, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
                host_model=host_model, bundle=bundle,
            )
            if inh is not None:
                inherited.append(inh)
            else:
                assert pushed is not None
                inherited.append(bound_filter_from_key(reroot_from_root(
                    key=cj, target_path=target_path, root_model=root_model,
                    models_by_name=models_by_name, host_name=host_model.name,
                )))
                if pushed[1] is not None:
                    restricted_texts.append(pushed[1])
    return inherited, restricted_texts


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
    return [column_default_key(path=back, leaf=src, base=host_model)
            for src, _ in first_hop.join_pairs]


def _substitute_prebound(
    prebound: PreboundQuery, mapping: Mapping[ValueKey, ValueKey],
    *, substitute: Callable[..., ValueKey] = substitute_value_keys,
) -> PreboundQuery:
    """Substitute value keys across a prebound's measures / filters / orders.

    ``substitute`` selects the traversal law: deep by default; the re-aggregation
    pre-substitution passes ``substitute_consumer_keys`` so a root nested where
    discovery does not look (a mixed row-attach source) is never substituted away."""
    return prebound.model_copy(update={
        "declared_measures": [
            dm.model_copy(update={"bound": BoundExpr(
                value_key=substitute(key=dm.bound.value_key, mapping=mapping),
            )})
            for dm in prebound.declared_measures
        ],
        "bound_filters": [
            substitute_in_bound_filter(bf, mapping, substitute=substitute)
            for bf in prebound.bound_filters
        ],
        "order_specs": [
            sp.model_copy(update={"bound": BoundExpr(
                value_key=substitute(key=sp.bound.value_key, mapping=mapping),
            )})
            for sp in prebound.order_specs
        ],
    })


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
    population: "Population",
    population_semi_join_measures: Optional[List[str]] = None,
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
            c=c, projected_dim_keys=context.projected_dim_keys,
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
        root_model=host_model, root_path=(),
    )
    param_constituent_of: Dict[ValueKey, ValueKey] = {}
    for _ps in reagg_param_specs:
        check_parameter_determined(
            alias=alias, param_name=_ps.name, grain_display=_grain_display(union_grain),
            determined=_param_is_determined(
                spec=_ps, grain=union_grain, host_model=host_model,
                models_by_name=models_by_name, bundle=bundle,
                projected_dim_keys=context.projected_dim_keys,
                projected_td_keys=context.projected_td_keys,
                active_bucket=prebound.main_time_key,
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
        elif isinstance(_ps.key, TransformKey) and _ps.key not in constituents:
            # A transform parameter rides the carrier as a constituent at its own
            # (explicit, post-D2) result grain — no ungrained normalisation (D4).
            constituents.append(_ps.key)

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
    # The public measure names this re-aggregation reports under (D6): a standalone
    # root its own alias; a mixed constituent the row-attach roots consuming it.
    if population_semi_join_measures is not None:
        semi_join_names = list(population_semi_join_measures)
    elif public_alias:
        semi_join_names = [public_alias]
    else:
        semi_join_names = []
    carrier_attach = _build_carrier_attach(
        union_grain=union_grain, constituents=constituents,
        constituent_placeholders=constituent_placeholders, host_model=host_model,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        inherited=inherited, n_date_range=n_date_range,
        producer_source_model=host_model.name, producer_registry=producer_registry,
        active_bucket=prebound.main_time_key,
        population=population,
        population_semi_join_measures=semi_join_names,
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
                 if isinstance(_ps.key, (AggregateKey, TransformKey)) else _ps.key),
            sql=_ps.expr_sql,  # host-rooted producer: canonical SQL is already root-frame (D8)
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
        prebound=outer_prebound,
        source_model=host_model.name,
        bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        producer_registry=producer_registry,
        population=population,
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
        population_semi_join_measures=(
            semi_join_names if outer_plan.semi_join_filters else []
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
    active_bucket: Optional[ValueKey],
    population: "Population",
    population_semi_join_measures: Optional[List[str]] = None,
) -> RegroupAttachPlan:
    """A row-attach producer at the union grain carrying every constituent (coarser
    ones broadcast within it) — the carrier / level-1 of the re-aggregation."""
    # A windowed inner carries the query's active bucket into its own grain
    # (Axiom 2.3); thread it as the carrier's main time dimension so the nested
    # producer for the windowed inner resolves it (DEV-1928 F5). The bucket is
    # already a union-grain key, so this only sets main_time_key.
    carrier_windowed = active_bucket is not None and any(
        window_kwarg_of(k) is not None
        for c in constituents for k in walk_value_keys(c)
    )
    carrier_prebound, ordered_pks = _regroup_producer_prebound(
        pks=union_grain, aggs=constituents, model=host_model, bundle=bundle,
        inherited=inherited, n_date_range=n_date_range,
        window_td_key=active_bucket if carrier_windowed else None,
    )
    carrier_plan = compile_synthesized(
        prebound=carrier_prebound,
        source_model=producer_source_model,
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
        # The carrier reads the base rows, so a population semi-join inherited here
        # reports under the enclosing measure names (D6).
        population_semi_join_measures=(
            list(population_semi_join_measures)
            if population_semi_join_measures and carrier_plan.semi_join_filters
            else []
        ),
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


class _ReaggregationRoots(BaseModel):
    """Re-aggregation roots in first-seen order: phase (row wins), alias, type."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    phase: Dict[AggregateKey, Literal["row", "combined"]] = Field(default_factory=dict)
    public_alias: Dict[ValueKey, str] = Field(default_factory=dict)
    declared_type: Dict[ValueKey, DataType] = Field(default_factory=dict)


def _group_reaggregation_roots(dispositions: Sequence[RootDisposition]) -> _ReaggregationRoots:
    out = _ReaggregationRoots()
    for d in dispositions:
        if d.routing != "reaggregation" or not isinstance(d.root, AggregateKey):
            continue
        if out.phase.get(d.root) != "row":
            out.phase[d.root] = d.phase
        if d.consumer_public_names:
            out.public_alias.setdefault(d.root, d.consumer_public_names[0])
        if d.declared_type is not None:
            out.declared_type.setdefault(d.root, d.declared_type)
    return out


class _RoutedRoots(BaseModel):
    """The discovered roots grouped by (phase, routing), in placeholder-mint order."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    row_local: List[ValueKey] = Field(default_factory=list)
    row_target: List[ValueKey] = Field(default_factory=list)
    combined_local: List[ValueKey] = Field(default_factory=list)
    combined_target: List[ValueKey] = Field(default_factory=list)
    reagg_constituents: List[AggregateKey] = Field(default_factory=list)
    inline_inputs: List[ValueKey] = Field(default_factory=list)
    public_alias: Dict[ValueKey, str] = Field(default_factory=dict)
    declared_type: Dict[ValueKey, DataType] = Field(default_factory=dict)
    constituent_consumers: Dict[ValueKey, List[str]] = Field(default_factory=dict)


def _combined_rank(k: ValueKey) -> int:
    """Combined sub-order: partitioned before bare; cross-model before local."""
    partitioned = getattr(k, "partition_keys", None) is not None
    if not is_cross_model_agg(k):
        return 2 if partitioned else 3
    return 0 if partitioned else 1


def _group_routed_roots(  # NOSONAR(S3776) — one grouping pass over the dispositions; the bucket / nesting / inline-demotion arms share the bucket state.
    dispositions: Sequence[RootDisposition], *,
    reagg_mapping: Mapping[ValueKey, ValueKey],
    nests: Callable[[ValueKey, str], bool],
) -> _RoutedRoots:
    """Group non-re-aggregation dispositions, keep only the roots ``nests`` admits
    (an inline root's inputs always attach), and demote a row-attach root whose
    every producer disposition was dropped to inline, attaching its inputs."""
    out = _RoutedRoots()

    def _mapped(k: ValueKey) -> ValueKey:
        return substitute_consumer_keys(k, reagg_mapping) if reagg_mapping else k

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
        if d.routing in ("reaggregation", "shifted", "inline"):
            continue
        if d.routing == "reaggregation_constituent" and isinstance(d.root, AggregateKey):
            _add(out.reagg_constituents, d.root)
            consumers = out.constituent_consumers.setdefault(d.root, [])
            consumers.extend(n for n in d.consumer_public_names if n not in consumers)
            continue
        is_input = d.phase == "row" and d.root in inputs
        k = d.root if is_input else _mapped(d.root)
        if d.phase == "row":
            if is_input or nests(k, "row"):
                _add(out.row_target if d.routing == "target_rooted" else out.row_local, k)
            continue
        if d.consumer_public_names:
            out.public_alias.setdefault(k, d.consumer_public_names[0])
        if d.declared_type is not None:
            out.declared_type.setdefault(k, d.declared_type)
        combined.append((k, d))
    for k, d in sorted(combined, key=lambda kd: _combined_rank(kd[0])):
        if d.routing == "target_rooted":
            _add(out.combined_target, k)
        elif nests(k, "combined"):
            _add(out.combined_local, k)
    producer_bound = {*out.row_local, *out.row_target, *out.combined_local, *out.combined_target}
    demoted = [
        d.root for d in dispositions
        if d.routing in ("local_producer", "target_rooted") and is_row_attach_root(d.root)
        and _mapped(d.root) not in producer_bound and d.root not in inline_roots
    ]
    for root in dict.fromkeys([*inline_roots, *demoted]):
        for inp in (row_attach_inputs(root, names=names_of.get(root, ()))
                    if root in demoted else []):
            if inp.routing == "reaggregation_constituent" and isinstance(inp.root, AggregateKey):
                _add(out.reagg_constituents, inp.root)
                consumers = out.constituent_consumers.setdefault(inp.root, [])
                consumers.extend(n for n in inp.consumer_public_names if n not in consumers)
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
    """``(grain, windowed, roots)`` per producer: one per grain and, for a windowed /
    ranked / crossing-input root, per partition-free identity."""
    groups: Dict[Tuple, List[ValueKey]] = {}
    meta: Dict[Tuple, Tuple[Grain, bool]] = {}
    for agg in roots:
        grain, windowed = effective_root_grain(
            agg=agg, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        ident = _windowed_or_ranked_identity(agg)
        # A crossing-input root needs its OWN producer, else another aggregate's crossed joins fan its rows.
        if ident is None and isinstance(agg, AggregateKey) and crossing(agg):
            ident = ("crossing", agg.source, agg.agg, tuple(agg.args), tuple(agg.kwargs))
        gkey = (grain, ident)
        groups.setdefault(gkey, []).append(agg)
        meta[gkey] = (grain, windowed)
    return [(*meta[g], aggs) for g, aggs in groups.items()]


class _LocalRegroupContext(BaseModel):
    """The enclosing plan's state every host-rooted local regroup producer reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    prebound: PreboundQuery
    bundle: ResolvedSourceBundle
    scope: Union[ModelScope, StageSchema]
    stage_schemas: Dict[str, StageSchema]
    producer_source_model: Optional[str]  # NOSONAR(S8396) — required-nullable: the one caller always decides
    producer_registry: Dict[Hashable, PlannedQuery]
    population: Population
    producer_model: Optional[SlayerModel]  # NOSONAR(S8396) — required-nullable: the one caller always decides
    mapping: Dict[ValueKey, ValueKey]
    inherited: List[Any]
    n_inherited_date: int


def _canonical_producer_aggs(
    aggs: Sequence[ValueKey],
) -> Tuple[List[ValueKey], Dict[ValueKey, ValueKey]]:
    """One producer measure per partition-free identity: a bare and a
    ``partition_by=`` twin collapse to one column."""
    by_identity: Dict = {}
    canonical_of: Dict[ValueKey, ValueKey] = {}
    for agg in aggs:
        canonical_of[agg] = by_identity.setdefault(_partition_free_identity(agg), agg)
    return list(by_identity.values()), canonical_of


def _assert_local_regroup_safe(
    *, producer_aggs: Sequence[ValueKey], producer_model: SlayerModel,
    bundle: ResolvedSourceBundle, active_td: Optional[ValueKey],
    alias_map: Dict[ValueKey, str],
) -> None:
    """Per-role crossing-input safety for every answer, and a PRESENT windowed axis
    attributable from the producer root (decision 12); a missing axis is left to
    the time-resolution guard downstream."""
    for agg in producer_aggs:
        if isinstance(agg, AggregateKey):
            _assert_local_producer_inputs_safe(
                agg=agg, host_model=producer_model, bundle=bundle,
                models_by_name={m.name: m for m in bundle.referenced_models},
            )
    if active_td is None:
        return
    first = producer_aggs[0]
    check_windowed_time_axis_attributable(
        alias=alias_map.get(first) if isinstance(first, AggregateKey) else None,
        root_name=producer_model.name,
        active_td_name=_regroup_grain_name(active_td),
        attributable=key_attributable_from_root(
            key=active_td, target_path=(), root_model=producer_model,
            models_by_name=bundle.models_by_name, bundle=bundle,
            host_model=producer_model, host_name=producer_model.name,
        ),
    )


def _local_regroup_join_pairs(
    *, producer_plan: PlannedQuery, ordered_pks: Sequence[ValueKey],
    mapping: Mapping[ValueKey, ValueKey],
) -> List[Tuple[ValueKey, Any]]:
    """Each grain key joined to its producer slot — by structural identity, else by
    projection position; a host-side key embedding another attach's aggregate
    renders via that attach's placeholder."""
    grain_ids = list(producer_plan.projection)[:len(ordered_pks)]

    def _slot(i: int, pk: ValueKey):
        found = next((s.id for s in producer_plan.row_slots if s.key == pk), None)
        return grain_ids[i] if found is None else found

    join_pairs = [
        (substitute_value_keys(pk, mapping), _slot(i, pk))
        for i, pk in enumerate(ordered_pks)
    ]
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    return join_pairs


def _local_regroup_kernel(
    *, producer_plan: PlannedQuery, answer: ValueKey, windowed: bool,
    bundle: ResolvedSourceBundle,
) -> Dict[str, Any]:
    """The kernel of a producer whose answer IS a windowed / ranked aggregate."""
    if not isinstance(answer, AggregateKey):
        return {}
    if windowed and window_kwarg_of(answer) is not None:
        build = _trailing_window_kernel
    elif answer.agg in RANKED_AGGREGATIONS:
        build = _ranked_kernel
    else:
        return {}
    root_model = producer_plan.render_source_model or bundle.source_model
    assert root_model is not None  # a local producer renders against its host
    return {"kernel": build(
        producer_plan=producer_plan, agg_key=answer,
        root_model=root_model,
        bundle=bundle, alias=canonical_aggregate_alias(answer, profile="stage_formula"),
        target_rooted=False,
    )}


def _synthesize_local_regroup(
    *, ctx: "_LocalRegroupContext", phase: Literal["row", "combined"], pks: Grain,
    windowed: bool, aggs: List[ValueKey], order_fn: Callable[[Grain], List[ValueKey]],
    alias_map: Dict[ValueKey, str], grain_names: Dict[ValueKey, str],
) -> RegroupAttachPlan:
    """One host-rooted producer for a group of local roots at one grain."""
    prebound, bundle, scope = ctx.prebound, ctx.bundle, ctx.scope
    producer_model, mapping = ctx.producer_model, ctx.mapping
    inherited, n_inherited_date = ctx.inherited, ctx.n_inherited_date
    pks = _prune_functionally_determined_grain(pks)
    producer_aggs, canonical_of = _canonical_producer_aggs(aggs)
    canon_index = {c: i for i, c in enumerate(producer_aggs)}
    if producer_model is not None:
        _assert_local_regroup_safe(
            producer_aggs=producer_aggs, producer_model=producer_model, bundle=bundle,
            active_td=prebound.main_time_key if windowed else None, alias_map=alias_map,
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
        prebound=producer_prebound,
        source_model=ctx.producer_source_model,
        bundle=bundle,
        scope=scope,
        stage_schemas=ctx.stage_schemas,
        producer_registry=ctx.producer_registry,
        population=ctx.population,
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
        )
        for agg in aggs
    ]
    join_pairs = _local_regroup_join_pairs(
        producer_plan=producer_plan, ordered_pks=ordered_pks, mapping=mapping,
    )
    attach_kwargs = _local_regroup_kernel(
        producer_plan=producer_plan, answer=producer_aggs[0], windowed=windowed,
        bundle=bundle,
    )
    return RegroupAttachPlan(
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
        # A population semi-join inherited into this producer reports each of
        # the producer's own public measures, not its stage alias. DEV-1944:
        # an aggregate selected under two names warns only the first.
        population_semi_join_measures=(
            [alias_map[a] for a in aggs
             if isinstance(a, AggregateKey) and a in alias_map]
            if producer_plan.semi_join_filters else []
        ),
        **attach_kwargs,
    )



def _rewrite_regrouped_prebound(
    prebound: PreboundQuery, *, mapping: Mapping[ValueKey, ValueKey],
    combined_mapping: Mapping[ValueKey, ValueKey],
) -> PreboundQuery:
    """Every root replaced by its placeholder: a computed dimension takes the full
    mapping, a measure only the combined one (its inners desugar COMBINED)."""
    return PreboundQuery(
        declared_measures=[
            dm.model_copy(update={"bound": BoundExpr(
                value_key=substitute_value_keys(
                    dm.bound.value_key,
                    mapping if dm.is_dimension else combined_mapping,
                ),
            )})
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


def _plan_regroups(
    *,
    prebound: PreboundQuery,
    filter_typings: Sequence[ConjunctTyping],
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_model: Optional[str],
    producer_registry: Dict[Hashable, PlannedQuery],
    dispositions: Sequence[RootDisposition],
    nests: Callable[[ValueKey, str], bool],
    home_paths: Dict[ValueKey, Tuple[str, ...]],
    population: Population,
    reserved_placeholders: AbstractSet[ValueKey] = frozenset(),
) -> Tuple[PreboundQuery, List[RegroupAttachPlan]]:
    """Group the discovered roots and desugar them into producer stages +
    reserved-leaf placeholders (row attach at base FROM, combined at the combined
    SELECT)."""
    registry = RegroupPlaceholderRegistry(reserved=reserved_placeholders)
    reagg = _group_reaggregation_roots(dispositions)
    reagg_roots = list(reagg.phase)
    reagg_mapping: Dict[ValueKey, ValueKey] = {
        root: registry.placeholder_for(root) for root in reagg_roots
    }
    if reagg_mapping:
        # Consumer-scoped, so a root nested in a row-attach root's inputs stays in
        # place for that root's own attach.
        prebound = _substitute_prebound(
            prebound, reagg_mapping, substitute=substitute_consumer_keys,
        )
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    projected_dim_keys = [dm.bound.value_key for dm in dim_dms]
    projected_td_keys = [dm.bound.value_key for dm in td_dms]
    active_bucket = prebound.main_time_key
    _is_crossing_local_root = crossing_local_root_predicate(scope=scope, bundle=bundle)
    routed = _group_routed_roots(
        dispositions, reagg_mapping=reagg_mapping, nests=nests,
    )
    row_aggs, combined_aggs = routed.row_local, routed.combined_local
    cm_row, cm_combined = routed.row_target, routed.combined_target
    public_alias_by_agg = routed.public_alias
    cm_type = routed.declared_type
    mixed_inline_inner = routed.inline_inputs
    reagg_constituents = routed.reagg_constituents
    reagg_constituent_consumers = routed.constituent_consumers
    if (
        not row_aggs and not combined_aggs and not cm_combined and not cm_row
        and not reagg_roots and not reagg_constituents
    ):
        return prebound, []
    # A real column sharing the reserved placeholder prefix would shadow a placeholder at render; reject while a regroup is active.
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    reserved = reserved_prefix_columns(
        producer_model if isinstance(scope, ModelScope) else scope
    )
    check_reserved_regroup_prefix(reserved)
    mapping: Dict[ValueKey, ValueKey] = {
        agg: registry.placeholder_for(agg)
        for agg in (
            *row_aggs, *combined_aggs, *cm_row, *cm_combined, *reagg_constituents,
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
        producer_source_model=producer_source_model, producer_registry=producer_registry,
        population=population, producer_model=producer_model, mapping=mapping,
        inherited=inherited, n_inherited_date=n_inherited_date,
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
        stage_schemas=stage_schemas, home_paths=home_paths,
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
            attach_phase=reagg.phase[root],
            public_alias=reagg.public_alias.get(root),
            context=synthesis_context,
            declared_type=reagg.declared_type.get(root),
            producer_registry=producer_registry, registry=registry,
            inherited=inherited, n_date_range=n_inherited_date,
            population=population,
        ))

    # DEV-1928: a re-aggregation constituent of a row-attach root is the same
    # second-order producer, but synthesized in a context whose projected grain IS
    # the constituent's own grain (so each partition key is attributable without
    # being a query dimension — the compile-time mirror of _reagg_operand_keys) and
    # attached at ROW phase to broadcast per partition onto the source's rows.
    for c in reagg_constituents:
        c_grain = constituent_grain(
            c=c, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        attaches.append(_synthesize_reaggregation_producer(
            root=c, placeholder=mapping[c], attach_phase="row",
            public_alias=None,
            context=synthesis_context.model_copy(update={
                "projected_dim_keys": [
                    k for k in c_grain if not isinstance(k, TimeTruncKey)
                ],
                "projected_td_keys": [
                    k for k in c_grain if isinstance(k, TimeTruncKey)
                ],
            }),
            declared_type=None,
            producer_registry=producer_registry, registry=registry,
            inherited=inherited, n_date_range=n_inherited_date,
            population=population,
            population_semi_join_measures=reagg_constituent_consumers.get(c, []),
        ))

    # The ROW substitution applies ONLY to computed DIMENSIONS; a non-dim measure
    # keeps query-grain (its inners desugar to COMBINED placeholders) — EXCEPT a
    # mixed-source measure's inline inner constituents, which row-attach and so
    # must reach the measure too (DEV-1859).
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


def compile_synthesized(
    prebound: PreboundQuery,
    *,
    source_model: Optional[str],
    bundle: ResolvedSourceBundle,
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
    population: Population,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
    carried_attaches: Sequence[RegroupAttachPlan] = (),
    reserved_placeholders: AbstractSet[ValueKey] = frozenset(),
) -> PlannedQuery:
    """Elaborate a compiler-synthesized producer — its aggregates' homes relative to
    its OWN root (D3) — and compile it. ``carried_attaches`` are outer attaches whose
    placeholders it reads as is; ``reserved_placeholders`` are never minted again."""
    env = elaborate_synthesized(
        prebound, bundle=bundle, scope=scope, stage_schemas=stage_schemas,
        source_model=source_model,
    )
    context = ProducerContext(
        enclosing_grain=Grain.of(dm.bound.value_key for dm in env.prebound.grain_declared_measures),
        population=population,
        carried_attaches=tuple(carried_attaches),
        reserved_placeholders=frozenset(reserved_placeholders),
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

    query: Union[SlayerQuery, StrictQueryCarrier]
    env: Union[ElaboratedStage, ElaboratedProducer]
    typed_prebound: PreboundQuery
    prebound: PreboundQuery
    attaches: List[RegroupAttachPlan]
    population: Population
    producer_registry: Dict[Hashable, PlannedQuery]
    #: Non-series ``time_shift`` roots, each answered by a shifted producer.
    shift_candidates: FrozenSet[ValueKey]


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
    stripped = _strip_redundant_partitions(env)
    dispositions = discover_roots(
        stripped, filter_typings=filter_typings, scope=scope, bundle=bundle,
    )
    routed_prebound, attaches = _plan_regroups(
        prebound=stripped, filter_typings=filter_typings,
        scope=scope, bundle=bundle, stage_schemas=dict(env.stage_schemas),
        producer_source_model=_producer_source_model(env),
        producer_registry=producer_registry,
        dispositions=dispositions,
        nests=lambda _root, _phase: True,
        home_paths=_home_paths(env),
        population=population,
    )
    _assert_total_routing(routed_prebound)
    return _Routed(
        query=env.query, env=env, typed_prebound=prebound, prebound=routed_prebound,
        attaches=attaches, population=population, producer_registry=producer_registry,
        shift_candidates=_shift_candidates(dispositions),
    )


def _route_producer(
    env: ElaboratedProducer, *, context: ProducerContext,
    producer_registry: Dict[Hashable, PlannedQuery],
) -> _Routed:
    prebound, scope, bundle = env.prebound, env.scope, env.bundle
    dispositions = discover_roots(
        prebound, filter_typings=env.filter_typings, scope=scope, bundle=bundle,
    )
    routed_prebound, attaches = _plan_regroups(
        prebound=prebound, filter_typings=list(env.filter_typings),
        scope=scope, bundle=bundle, stage_schemas=dict(env.stage_schemas),
        producer_source_model=_producer_source_model(env),
        producer_registry=producer_registry,
        dispositions=dispositions,
        nests=_producer_nesting_rule(prebound, context=context),
        home_paths=_home_paths(env),
        population=context.population,
        reserved_placeholders=context.reserved_placeholders,
    )
    return _Routed(
        query=env.query, env=env, typed_prebound=prebound, prebound=routed_prebound,
        attaches=[*attaches, *context.carried_attaches], population=context.population,
        producer_registry=producer_registry,
        shift_candidates=_shift_candidates(dispositions),
    )


def _producer_source_model(env: Union[ElaboratedStage, ElaboratedProducer]) -> Optional[str]:
    if isinstance(env.query.source_model, str):
        return env.query.source_model
    if isinstance(env.scope, ModelScope) and env.scope.source_model is not None:
        return env.scope.source_model.name
    return None


def _home_paths(env: ElaboratedQuery) -> Dict[ValueKey, Tuple[str, ...]]:
    return {k: t.home_path for k, t in env.terms.items() if isinstance(t, Aggregate)}


def _strip_redundant_partitions(env: ElaboratedStage) -> PreboundQuery:
    """A LOCAL row-attach root partitioned by exactly the query grain aggregates
    INLINE with its inputs row-attached."""
    prebound, scope, bundle = env.prebound, env.scope, env.bundle
    query_grain = Grain.of(dm.bound.value_key for dm in prebound.grain_declared_measures)
    roots = dict.fromkeys(d.root for d in discover_roots(
        prebound, filter_typings=env.filter_typings, scope=scope, bundle=bundle,
    ))
    strip: Dict[ValueKey, ValueKey] = {
        r: r.model_copy(update={"partition_keys": None})
        for r in roots
        if isinstance(r, AggregateKey) and is_row_attach_root(r)
        and not is_cross_model_agg(r) and window_kwarg_of(r) is None
        and r.partition_keys is not None
        and Grain.of(r.partition_keys) == query_grain
    }
    return _substitute_prebound(prebound, strip) if strip else prebound


def _producer_nesting_rule(
    prebound: PreboundQuery, *, context: ProducerContext,
) -> Callable[[ValueKey, str], bool]:
    """A row root always nests. A combined root nests unless at exactly the
    producer grain (bar a windowed transform input and a ranked / windowed strict
    constituent of a composite answer); off the producer grain only the
    producer's own answers stay inline (their dropped members are the
    synthesizer's disposition)."""
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    projected_dim_keys = [dm.bound.value_key for dm in dim_dms]
    projected_td_keys = [dm.bound.value_key for dm in td_dms]
    answers = {
        dm.bound.value_key for dm in prebound.declared_measures if not dm.is_dimension
    }
    windowed_transform_inputs = {
        k
        for dm in prebound.declared_measures
        for tk in walk_value_keys(dm.bound.value_key)
        if isinstance(tk, TransformKey)
        for k in walk_value_keys(tk.input)
        if window_kwarg_of(k) is not None
    }
    composite_constituents = {
        k
        for dm in prebound.declared_measures
        if not dm.is_dimension
        and isinstance(dm.bound.value_key, (ArithmeticKey, ScalarCallKey))
        for k in walk_consumer_keys(dm.bound.value_key)
    }

    def nests(root: ValueKey, phase: str) -> bool:
        if phase == "row":
            return True
        grain = constituent_grain(
            c=root, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=prebound.main_time_key,
        )
        if not grain.is_subgrain_of(context.enclosing_grain):
            return root not in answers
        return (
            grain != context.enclosing_grain
            or root in windowed_transform_inputs
            or (root in composite_constituents
                and _windowed_or_ranked_identity(root) is not None)
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
    _producer_source_model_name = _producer_source_model(env)
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
        dm.bound.value_key
        for dm in typed_prebound.declared_measures[n_dims + n_tds:]
    ]
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
                and aggregate_input_closure(
                    key=wrap_key, anchor_model=host_model_for_wraps,
                    anchor_relation=host_model_for_wraps.name, bundle=bundle,
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
                        producer_source_model=_producer_source_model_name,
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
    # them as correlated EXISTS on this plan (DEV-1935: total over the boolean
    # shape, never fails closed).
    pop_semi_join_groups: List[SemiJoinFilter] = []
    pushed_conjunct_keys: List[ValueKey] = []
    host_population_gated = False
    if population_filters is not None and render_source_model is not None:
        host_grain_paths = _grain_closure_paths(
            [dm.bound.value_key for dm in declared_measures[:n_dims + n_tds]],
            host_model=render_source_model, bundle=bundle,
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
            _has_inline_population_aggregate(prebound)
            and len(pushed_conjunct_keys) < n_semi
        ), "a spine-covered fanning mask coexists with an inline plain aggregate"
        host_population_gated = bool(pop_semi_join_groups)

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
    for i, (bf, ct) in enumerate(zip(bound_filters, filter_typings)):
        key = _drop_conjuncts(value_key=bf.value_key, drop=pushed_set)
        if key is None:
            continue  # every conjunct of this filter moved to the EXISTS
        mask_sid = projection.registry.find_by_key(key)
        if mask_sid is None:
            mask_sid = projection.registry.intern(
                key=key,
                declared_name=f"__slayer_mask_{i}",
                hidden=True,
                phase=key.phase,
            )
        masks.append(MaskEntry(
            slot_id=mask_sid, typing=ct.typing, stratum=ct.stratum,
        ))
        mask_keys.append(key)
        if i < n_date_range:
            surviving_date_range += 1
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
    for key, mask in zip(mask_keys, masks):
        filter_reachability.append(FilterReachability(
            filter_id=mask.slot_id,
            crossed_join_paths=compute_key_join_paths(
                key=key,
                anchor_model=reachability_anchor_model,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
            has_host_local_ref=key_has_host_local_ref(
                key=key,
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
        root=render_source_model, models_by_name=bundle.models_by_name,
        originals={sub.placeholder: sub.original_key
                   for attach in regroup_attach_plans for sub in attach.substitutions},
        upstream=_upstream_respellings(scope),
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
        host_gated=host_population_gated,
    )

    regroup_attach_plans = [*regroup_attach_plans, *_plan_shifted_attaches(
        slots=projection.registry.slots, attaches=regroup_attach_plans,
        prebound=typed_prebound, rewritten=prebound, filter_typings=filter_typings,
        scope=scope, bundle=bundle, stage_schemas=stage_schemas,
        producer_source_model=_producer_source_model_name,
        producer_registry=producer_registry, population=population,
        candidates=routed.shift_candidates,
    )]
    # Assign every slot its materialisation stage / needs-column / series fact;
    # producer bodies were staged by their own compilation.
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
        n_date_range_masks=surviving_date_range,
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
        semi_join_filters=pop_semi_join_groups,
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
    host_gated: bool = False,
) -> "EmptyBaseGrainPlan | None":
    """Decide the empty-base spine at plan time — the host base has nothing of its own exactly when every value asked for is an isolated aggregate. ``host_gated`` (D7) means a population semi-join gates the spine even with no field mask."""
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
    return EmptyBaseGrainPlan(host_filter_ids=host_filter_ids, host_gated=host_gated)


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


def _value_anchor_path(key: ValueKey) -> Tuple[str, ...]:
    """The join path a path-derived output's auto-name is prefixed with."""
    if isinstance(key, TimeTruncKey):
        key = key.column
    if isinstance(key, (ColumnKey, ColumnSqlKey, StarKey)):
        return tuple(key.path)
    if isinstance(key, AggregateKey):
        return source_anchor_path(key.source)
    return ()


def _respellings(
    *, flat: str, key: ValueKey, root: Optional[SlayerModel],
    models_by_name: Dict[str, SlayerModel], upstream: Mapping[str, Tuple[str, ...]],
) -> Tuple[str, ...]:
    """``flat`` with every non-empty subset of its path's named hops spelled by
    their target model (auto-names are path-prefixed, so re-deriving substitutes
    the prefix)."""
    path = _value_anchor_path(key)
    if not path:
        return _inherited_respellings(flat=flat, key=key, upstream=upstream)
    prefix = "__".join(path) + "__"
    if root is None or not flat.startswith(prefix):
        return ()
    try:
        chain = walk(root=root, path=path, models_by_name=models_by_name)
    except (AmbiguousJoinPathError, CircularJoinPathError):
        return ()
    if not chain:
        return ()
    named = [i for i, e in enumerate(chain) if e.name is not None]
    rest = flat[len(prefix):]
    return tuple(
        "__".join(chain[i].target_model if i in subset else tok
                  for i, tok in enumerate(path)) + "__" + rest
        for r in range(1, len(named) + 1)
        for subset in itertools.combinations(named, r)
    )


def _upstream_respellings(
    scope: Union[ModelScope, StageSchema],
) -> Dict[str, Tuple[str, ...]]:
    """Respellings of the columns a stage reads locally (upstream stage / query-backed)."""
    if isinstance(scope, StageSchema):
        return {c.name: c.respellings for c in scope.columns if c.respellings}
    if scope.source_model is None:
        return {}
    return {c.name: c.respellings for c in scope.source_model.columns if c.respellings}


def _inherited_respellings(
    *, flat: str, key: ValueKey, upstream: Mapping[str, Tuple[str, ...]],
) -> Tuple[str, ...]:
    """``flat`` with the passed-through upstream column's name swapped for each
    of that column's respellings."""
    if isinstance(key, TimeTruncKey):
        key = key.column
    if isinstance(key, AggregateKey):
        key = key.source
    if not isinstance(key, (ColumnKey, ColumnSqlKey)) or key.path:
        return ()
    name = column_leaf(key)
    if not flat.startswith(name):
        return ()
    rest = flat[len(name):]
    return tuple(r + rest for r in upstream.get(name, ()))


def _emit_stage_schema(
    *,
    stage_name: Optional[str],
    projection,
    root: Optional[SlayerModel] = None,
    models_by_name: Optional[Dict[str, SlayerModel]] = None,
    originals: Optional[Mapping[ValueKey, ValueKey]] = None,
    upstream: Optional[Mapping[str, Tuple[str, ...]]] = None,
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
            respellings=() if alias in slot.explicit_aliases else _respellings(
                flat=flat, key=(originals or {}).get(slot.key, slot.key), root=root,
                models_by_name=models_by_name or {}, upstream=upstream or {},
            ),
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
