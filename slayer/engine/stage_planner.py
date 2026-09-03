"""Multi-stage ``source_queries`` planner: SlayerQuery stages → PlannedQuery list.
Topo-sorted stages; downstream binds against the upstream flat ``StageSchema``."""

from __future__ import annotations

from enum import Enum
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Hashable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.format import NumberFormat
from slayer.core.errors import (
    AmbiguousReferenceError,
    DistinctDimensionValuesError,
    UnknownReferenceError,
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
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    column_leaf,
    normalize_scalar,
    reroot_value_key,
    substitute_value_keys,
)
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths
from slayer.engine.join_safety import (
    may_inline_crossing_inputs,
    provably_to_one,
    safe_reachable,
)
from slayer.core.query import (
    ORDER_PLACEHOLDER_NAMES,
    ComputedDimension,
    ModelExtension,
    SlayerQuery,
    TimeDimension,
)
from slayer.core.refs import canonical_agg_name
from slayer.sql.naming import canonical_aggregate_alias
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import (
    BoundExpr as BinderBoundExpr,
    BoundFilter,
    bind_expr,
    bind_filter,
    bind_time_dimension,
    walk_value_keys,
)
from slayer.engine.filter_reachability import (
    compute_key_join_paths,
    key_has_host_local_ref,
)
from slayer.engine.planned import (
    BoundExpr as PlannedBoundExpr,
    BoundFilterId,
    EmptyBaseGrainPlan,
    FilterPhase,
    FilterReachability,
    OrderEntry,
    OrderScope,
    PlannedQuery,
    RankedProducerKernel,
    RegroupAttachPlan,
    RegroupSubstitution,
    SlotId,
    SrcFilterRewrite,
    TrailingWindowProducerKernel,
    TransformLayer,
    ValueSlot,
)
from slayer.engine.ranked_planner import (
    RANKED_AGGREGATIONS,
    ordered_row_keys,
    resolve_ranking_time_key,
)
from slayer.engine.planning import (
    DeclaredMeasure,
    OrderSpec,
    ProjectionPlanner,
    _canonical_name,
    _iter_slot_deps,
    filter_referenced_slot_ids,
    lower_sugar_transforms,
    rewrite_rank_partition_keys,
)
from slayer.engine.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    dimension_key_metadata,
    measure_key_format_description,
    measure_key_type,
    partition_declared_measures,
    walk_key_path,
)
from slayer.engine.regroup_planner import (
    REGROUP_LEAF_PREFIX,
    RegroupPlaceholderRegistry,
    classify_regroup_filter,
    combined_consumer_aggregates,
    conjunct_scope,
    dimension_partitioned_aggregates,
    dimension_regroup_roots,
    is_local_combined_regroup_ref,
    regroup_root_grain,
    reserved_prefix_columns,
    split_top_level_and,
    substitute_in_bound_filter,
)
from slayer.engine.source_bundle import (
    ResolvedSourceBundle,
    _apply_extension_overlay,
    _source_name_if_sibling,
    stage_bundle_with_siblings,
    synthetic_model_from_stage_schema,
)
from slayer.engine.syntax import (
    AggCall,
    DottedRef,
    ParsedExpr,
    Ref,
    TransformCall,
    canonical_measure_text,
    parse_expr,
    parse_filter_expr,
)
from slayer.sql.naming import flat_name
from slayer.sql.sql_expr import has_window_function
from slayer.sql.sql_predicate import parse_sql_predicate


__all__ = [
    "PreboundQuery",
    "StrictQueryCarrier",
    "bind_query_inputs",
    "plan_query",
    "plan_stages",
]


# Transform ops needing a resolvable time dimension for their OVER ORDER BY.
_TIME_NEEDING_TRANSFORM_OPS = TIME_TRANSFORMS


def _attach_time_keys(
    key: ValueKey, *, td_key: TimeTruncKey,
) -> ValueKey:
    """Set ``time_key=td_key`` on every time-needing TransformKey with a null one (identity-preserving)."""
    if isinstance(key, TransformKey):
        new_input = _attach_time_keys(key.input, td_key=td_key)
        out = key
        if new_input is not key.input:
            out = out.model_copy(update={"input": new_input})
        if out.op in _TIME_NEEDING_TRANSFORM_OPS and out.time_key is None:
            out = out.model_copy(update={"time_key": td_key})
        return out
    if isinstance(key, ArithmeticKey):
        new_ops = tuple(
            _attach_time_keys(o, td_key=td_key) for o in key.operands
        )
        if all(a is b for a, b in zip(new_ops, key.operands)):
            return key
        return ArithmeticKey(op=key.op, operands=new_ops)
    if isinstance(key, ScalarCallKey):
        new_args = tuple(
            _attach_time_keys(a, td_key=td_key)
            if isinstance(
                a, (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey),
            )
            else a
            for a in key.args
        )
        if all(a is b for a, b in zip(new_args, key.args)):
            return key
        return ScalarCallKey(name=key.name, args=new_args)
    if isinstance(key, BetweenKey):
        nc = _attach_time_keys(key.column, td_key=td_key)
        nl = _attach_time_keys(key.low, td_key=td_key)
        nh = _attach_time_keys(key.high, td_key=td_key)
        if nc is key.column and nl is key.low and nh is key.high:
            return key
        return BetweenKey(column=nc, low=nl, high=nh)
    if isinstance(key, InKey):
        # Only the LHS column can carry a transform; values are literals.
        nc = _attach_time_keys(key.column, td_key=td_key)
        if nc is key.column:
            return key
        return InKey(column=nc, values=key.values, negated=key.negated)
    return key


def _partition_key_display(pk: ValueKey) -> str:
    if isinstance(pk, ColumnKey):
        return ".".join([*pk.path, pk.leaf])
    if isinstance(pk, ColumnSqlKey):
        return ".".join([*pk.path, pk.column_name])
    if isinstance(pk, TimeTruncKey):
        return _partition_key_display(pk.column)
    return str(pk)


def _row_key_path(key: ValueKey) -> tuple:
    if isinstance(key, TimeTruncKey):
        return _row_key_path(key.column)
    return tuple(getattr(key, "path", ()))


def _find_unresolved_time_needing_op(key: ValueKey) -> Optional[str]:
    if isinstance(key, TransformKey):
        if key.op in _TIME_NEEDING_TRANSFORM_OPS and key.time_key is None:
            return key.op
        return _find_unresolved_time_needing_op(key.input)
    if isinstance(key, ArithmeticKey):
        for o in key.operands:
            found = _find_unresolved_time_needing_op(o)
            if found:
                return found
        return None
    if isinstance(key, ScalarCallKey):
        for a in key.args:
            if isinstance(
                a, (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey),
            ):
                found = _find_unresolved_time_needing_op(a)
                if found:
                    return found
        return None
    if isinstance(key, BetweenKey):
        for k in (key.column, key.low, key.high):
            found = _find_unresolved_time_needing_op(k)
            if found:
                return found
        return None
    if isinstance(key, InKey):
        return _find_unresolved_time_needing_op(key.column)
    return None


def _guard_dimension_temporal_axis(declared_measures) -> None:
    """Fail closed if a time-ordered transform inside a dimension evaluates at a grain not containing its time axis (would duplicate result rows)."""
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        for tk in walk_value_keys(dm.bound.value_key):
            if not isinstance(tk, TransformKey):
                continue
            if tk.op not in TIME_TRANSFORMS or tk.time_key is None:
                continue
            if tk.time_key not in regroup_root_grain(tk):
                axis = _partition_key_display(tk.time_key)
                raise NotImplementedError(
                    f"A time-ordered transform '{tk.op}' inside a computed "
                    f"dimension evaluates at a grain that does not contain its "
                    f"time axis '{axis}'; a producer bucketed by time joined back "
                    f"on the coarser grain would duplicate result rows. Include "
                    f"the time key in the aggregate's partition_by= so the "
                    f"transform accumulates within its own grain (DEV-1839)."
                )


# Duration-windowed measures (``window='90d'``).


def _window_kwarg_of(key: ValueKey):
    if isinstance(key, AggregateKey):
        for k, v in key.kwargs:
            if k == "window":
                return v
    return None


def _windowed_agg_keys(vk: ValueKey) -> list:
    return [k for k in walk_value_keys(vk) if _window_kwarg_of(k) is not None]


def _reject_unsupported_windowed_key(key: AggregateKey) -> None:
    """Per-key windowed guards: sum/avg only, compact-duration-string window."""
    if key.agg not in ("sum", "avg"):
        raise ValueError(
            f"Aggregation parameter 'window' is only supported for sum and avg, "
            f"not '{key.agg}'."
        )
    window_val = _window_kwarg_of(key)
    if not isinstance(window_val, str):
        raise ValueError(
            f"Window duration must be a compact duration string like '90d', got "
            f"{window_val!r}. Use syntax like '1y2m3w5d6h7min8s'."
        )
    parse_window_duration(window_val)  # raises on empty / malformed


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
            _reject_unsupported_windowed_key(key)

    selected_windowed: dict = {}
    for vk in measure_vks:
        if _window_kwarg_of(vk) is not None:
            selected_windowed.setdefault(vk, False)
    # Order-only windowed target: HIDDEN, after the measure loop so an also-declared key keeps hidden=False.
    for vk in order_vks:
        for key in _windowed_agg_keys(vk):
            selected_windowed.setdefault(key, True)

    if active_td_key is None:
        raise ValueError(
            "Windowed measure could not resolve its time dimension. Add a single "
            "time_dimensions entry, or set main_time_dimension to select among "
            "multiple time dimensions."
        )
    return selected_windowed


def _partitioned_agg_keys(
    vk: ValueKey, *, exclude: AbstractSet[AggregateKey] = frozenset(),
) -> list:
    return [
        k for k in walk_value_keys(vk)
        if isinstance(k, AggregateKey)
        and k.partition_keys is not None
        and k not in exclude
    ]


def _guard_partitioned_measures(
    *, measure_vks: list, filter_vks: list, order_vks: list,
    exclude: AbstractSet[AggregateKey] = frozenset(),
) -> None:
    """Reject still-deferred cross-model partition_by shapes (first/last, nested-in-transform, in-filter), excluding computed-dimension aggregates."""
    def _part(vk: ValueKey) -> list:
        return _partitioned_agg_keys(vk, exclude=exclude)

    def _cross_model(k: AggregateKey) -> bool:
        return bool(getattr(k.source, "path", ()))

    all_vks = [*measure_vks, *filter_vks, *order_vks]
    part_keys = [k for vk in all_vks for k in _part(vk)]
    if not part_keys:
        return
    if any(k.agg in ("first", "last") and _cross_model(k) for k in part_keys):
        raise NotImplementedError(
            "partition_by on a cross-model first/last aggregation is not yet "
            "supported (DEV-1824); the aggregate must be local to the query's "
            "source."
        )
    # A cross-model partitioned aggregate nested in a transform is never desugared, so fail closed.
    if any(
        isinstance(tk, TransformKey) and any(_cross_model(k) for k in _part(tk.input))
        for vk in all_vks for tk in walk_value_keys(vk)
    ):
        raise NotImplementedError(
            "A cross-model partition_by aggregate nested inside a transform is "
            "not yet supported (DEV-1824); the partitioned aggregate must be "
            "local to the query's source."
        )
    if any(_cross_model(k) for vk in filter_vks for k in _part(vk)):
        raise NotImplementedError(
            "Filtering on a cross-model partition_by aggregate is not yet "
            "supported (DEV-1824); the aggregate must be local to the query's "
            "source."
        )


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
    if active_td_slot_id is None:
        raise ValueError(
            "Windowed measure could not resolve its time dimension. Add a single "
            "time_dimensions entry, or set main_time_dimension to select among "
            "multiple time dimensions."
        )

    for key in selected_windowed:
        sid = registry.find_by_key(key)
        if sid is None:
            # A missing slot is planner/projection drift; fail rather than degrade to a plain aggregate.
            raise RuntimeError(
                f"Windowed measure {key!r} was selected but has no projection "
                f"slot; planner/projection drift (DEV-1714).",
            )
        windowed_slot_ids.add(sid)
    return windowed_slot_ids


_RAW_ROW_FIX_HINT = (
    "Either remove the measure reference, or set "
    "distinct_dimension_values=True (the default) to keep the "
    "auto-aggregating behaviour."
)


def _iter_expr_children(node):
    for attr in ("input", "left", "right", "this", "operand"):
        child = getattr(node, attr, None)
        if child is not None and not isinstance(child, (str, bool)):
            yield child
    for attr in ("args", "operands", "kwargs"):
        for item in getattr(node, attr, None) or ():
            # kwargs are (name, value) pairs; take the value.
            yield item[1] if isinstance(item, tuple) and len(item) == 2 else item


def _expr_has_measure_ref(
    node, *, measure_names: FrozenSet[str], scope, bundle,
) -> bool:
    if node is None:
        return False
    if isinstance(node, (AggCall, TransformCall)):
        return True
    if isinstance(node, Ref) and node.name in measure_names:
        return True
    # A dotted leaf resolving to a saved measure on the terminal model is a measure ref too.
    if isinstance(node, DottedRef) and _resolve_saved_measure_ref(
        scope=scope, bundle=bundle, formula=".".join(node.parts),
    ) is not None:
        return True
    return any(
        _expr_has_measure_ref(
            child, measure_names=measure_names, scope=scope, bundle=bundle,
        )
        for child in _iter_expr_children(node)
    )


def _reject_measure_refs_for_raw_rows(
    *, query: SlayerQuery, scope, bundle: ResolvedSourceBundle,
) -> None:
    """Raw-rows mode (``distinct_dimension_values=False``): reject any measure reference in filters/order."""
    src = getattr(scope, "source_model", None)
    measure_names: FrozenSet[str] = frozenset(
        m.name for m in (getattr(src, "measures", None) or []) if m.name
    )
    _reject_measure_refs_in_filters(
        query=query, measure_names=measure_names, scope=scope, bundle=bundle,
    )
    _reject_measure_refs_in_order(
        query=query,
        measure_names=measure_names,
        source_name=getattr(src, "name", None),
        scope=scope,
        bundle=bundle,
    )


def _reject_measure_refs_in_filters(
    *, query: SlayerQuery, measure_names: FrozenSet[str], scope,
    bundle: ResolvedSourceBundle,
) -> None:
    for f in (query.filters or []):
        if not isinstance(f, str):
            continue
        try:
            parsed = parse_filter_expr(f)
        except Exception:  # noqa: BLE001 — binder reports parse errors properly
            continue
        if _expr_has_measure_ref(
            parsed, measure_names=measure_names, scope=scope, bundle=bundle,
        ):
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False rejects measure references, "
                f"but filter {f!r} contains one. {_RAW_ROW_FIX_HINT}"
            )


def _parse_order_formula(raw: str):
    try:
        return parse_expr(raw)
    except Exception:  # noqa: BLE001 — binder reports parse errors properly
        return None


def _reject_measure_refs_in_order(
    *,
    query: SlayerQuery,
    measure_names: FrozenSet[str],
    source_name: Optional[str],
    scope,
    bundle: ResolvedSourceBundle,
) -> None:
    for item in (query.order or []):
        raw = getattr(item, "raw_formula", None)
        if raw:
            parsed = _parse_order_formula(raw)
            if parsed is not None and _expr_has_measure_ref(
                parsed, measure_names=measure_names, scope=scope, bundle=bundle,
            ):
                raise DistinctDimensionValuesError(
                    f"distinct_dimension_values=False rejects measure "
                    f"references, but order item {raw!r} contains one. "
                    f"{_RAW_ROW_FIX_HINT}"
                )
        name = getattr(getattr(item, "column", None), "name", None)
        if name and name in measure_names:
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False rejects measure references, "
                f"but order item {name!r} resolves to a saved measure on "
                f"{source_name or 'the source model'!r}. "
                f"{_RAW_ROW_FIX_HINT}"
            )
        # A dotted ORDER BY column whose leaf is a saved measure on the terminal model is a measure ref too.
        full = getattr(getattr(item, "column", None), "full_name", None)
        if full and "." in full and _resolve_saved_measure_ref(
            scope=scope, bundle=bundle, formula=full,
        ) is not None:
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False rejects measure references, "
                f"but order item {full!r} resolves to a saved measure. "
                f"{_RAW_ROW_FIX_HINT}"
            )


def _resolve_scope(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    stage_schemas: Optional[Dict[str, StageSchema]],
) -> Union[ModelScope, StageSchema]:
    source = query.source_model
    if isinstance(source, str) and source in (stage_schemas or {}):
        return (stage_schemas or {})[source]
    return ModelScope(source_model=bundle.source_model)


def _map_bound_keys(
    key_fn: Callable[[ValueKey], ValueKey],
    *,
    declared_measures: List[DeclaredMeasure],
    bound_filters: List[BoundFilter],
    order_specs: List[OrderSpec],
) -> Tuple[List[DeclaredMeasure], List[BoundFilter], List[OrderSpec]]:
    new_measures = [
        DeclaredMeasure(
            bound=BinderBoundExpr(value_key=key_fn(dm.bound.value_key)),
            declared_name=dm.declared_name,
            public_name=dm.public_name,
            label=dm.label,
            canonical_alias=dm.canonical_alias,
            type=dm.type,
            type_is_explicit=dm.type_is_explicit,
            format=dm.format,
            description=dm.description,
            is_dimension=dm.is_dimension,
        )
        for dm in declared_measures
    ]
    new_filters = []
    for bf in bound_filters:
        new_vk = key_fn(bf.value_key)
        new_filters.append(
            BoundFilter(
                value_key=new_vk,
                phase=bf.phase,
                referenced_keys=tuple(walk_value_keys(new_vk)),
            )
        )
    new_specs = [
        OrderSpec(
            bound=BinderBoundExpr(value_key=key_fn(spec.bound.value_key)),
            direction=spec.direction,
        )
        for spec in order_specs
    ]
    return new_measures, new_filters, new_specs


def bind_query_inputs(  # NOSONAR(S3776) — one cohesive bind pass. The stages are strictly sequential and share the growing `declared_measures` / `bound_filters` / `order_specs` triple: parse+bind, time-key attachment, sugar lowering, rank-partition validation. Splitting them would thread the same three lists through four signatures without removing a branch.
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
) -> PreboundQuery:
    """Parse and bind every text surface of a ``SlayerQuery`` (the only door into the parser); returns fully-normalized keys. Model filters excluded (scope-owned)."""
    if scope is None:
        scope = _resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )

    # Runs BEFORE binding so the targeted error wins over the binder's generic one.
    if query.distinct_dimension_values is False:
        _reject_measure_refs_for_raw_rows(query=query, scope=scope, bundle=bundle)

    declared_measures = _declared_measures_from_query(
        query=query, scope=scope, bundle=bundle,
    )

    # Alias lookup for ORDER BY, checked before bind_expr so aggregate aliases resolve via the registry.
    declared_alias_to_bound: Dict[str, BinderBoundExpr] = {}
    for dm in declared_measures:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                declared_alias_to_bound.setdefault(alias, dm.bound)

    # Declared-MEASURE aliases a filter may reference by name, interning onto the same slot as the dotted/colon form.
    n_dims = len(query.dimensions or [])
    n_tds = len(query.time_dimensions or [])
    filter_alias_map: Dict[str, ValueKey] = {}
    _, _, _agg_dms = partition_declared_measures(
        declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    for dm in _agg_dms:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                filter_alias_map.setdefault(alias, dm.bound.value_key)
    # A computed dimension's name is a query-local alias resolvable in filters/order.
    for dm in declared_measures:
        if dm.is_dimension and dm.public_name is not None:
            filter_alias_map.setdefault(dm.public_name, dm.bound.value_key)

    # Filter list in WHERE order: date_range, model filters (Mode-A SQL), then user query filters.
    bound_filters: List[BoundFilter] = []
    # Parallel original filter text (None for date_range bounds), for dropped-filter warnings.
    bound_filter_texts: List[Optional[str]] = []

    # 1. date_range filters (one per TD with a 2-element date_range)
    for td in (query.time_dimensions or []):
        if not td.date_range or len(td.date_range) != 2:
            continue
        if not isinstance(scope, ModelScope):
            continue
        bf = _build_date_range_filter(td=td, scope=scope, bundle=bundle)
        bound_filters.append(bf)
        bound_filter_texts.append(None)
    n_date_range = len(bound_filters)

    # 2. SlayerModel.filters — lifted from scope in plan_query, not here.

    # 3. user query filters (Mode-B DSL). Dedupe by bound key (first wins) so the
    #    alias and dotted/colon forms of a ref don't duplicate the HAVING clause.
    for f in (query.filters or []):
        if not isinstance(f, str):
            continue
        bf = bind_filter(
            parsed=parse_filter_expr(f),
            scope=scope,
            bundle=bundle,
            alias_map=filter_alias_map,
        )
        if any(existing.value_key == bf.value_key for existing in bound_filters):
            continue
        bound_filters.append(bf)
        bound_filter_texts.append(f)

    order_specs = []
    # Host identity for the qualifier check below (StageSchema uses its relation name).
    _order_host_name = _host_model_name(scope)
    for o in (query.order or []):
        col_name = o.column.name
        full_name = o.column.full_name
        # A placeholder ColumnRef means the item is an EXPRESSION: bind raw_formula, skip alias lookups.
        if col_name in ORDER_PLACEHOLDER_NAMES and o.raw_formula:
            order_specs.append(OrderSpec(
                bound=bind_expr(
                    parsed=parse_expr(o.raw_formula),
                    scope=scope,
                    bundle=bundle,
                ),
                direction=o.direction,
            ))
            continue
        # An ORDER BY over a partition_by / window= aggregate must bind raw_formula (the alias shortcut would drop the partition/window).
        if o.raw_formula and (
            "partition_by" in o.raw_formula or "window" in o.raw_formula
        ):
            _part_bound = bind_expr(
                parsed=parse_expr(o.raw_formula),
                scope=scope, bundle=bundle,
            )
            if any(
                isinstance(k, AggregateKey) and (
                    k.partition_keys is not None or _window_kwarg_of(k) is not None
                )
                for k in walk_value_keys(_part_bound.value_key)
            ):
                order_specs.append(OrderSpec(
                    bound=_part_bound, direction=o.direction,
                ))
                continue
        # A FOREIGN-qualified order ref must not resolve to a same-named local column via the bare-leaf shortcut.
        _order_qualifier = getattr(o.column, "model", None)
        _order_host_local = (
            _order_qualifier is None or _order_qualifier == _order_host_name
        )
        # Prefer alias resolution over model-scope binding; try dotted then flattened forms, falling back to raw.
        if _order_host_local and col_name in declared_alias_to_bound:
            bo = declared_alias_to_bound[col_name]
        elif full_name in declared_alias_to_bound:
            bo = declared_alias_to_bound[full_name]
        elif _flatten_dotted(full_name) in declared_alias_to_bound:
            # A joined dim/td is declared flattened; a dotted ORDER BY entry interns onto that slot.
            bo = declared_alias_to_bound[_flatten_dotted(full_name)]
        elif _order_host_local and f"_{col_name}" in declared_alias_to_bound:
            # ``*:count`` surfaces as ``_count``; users order by the bare ``count``.
            bo = declared_alias_to_bound[f"_{col_name}"]
        elif o.raw_formula:
            bo = bind_expr(
                parsed=parse_expr(o.raw_formula),
                scope=scope,
                bundle=bundle,
            )
        else:
            # Bind the FULL reference — a dotted ORDER ColumnRef would otherwise rebind as the wrong host column.
            bo = bind_expr(
                parsed=parse_expr(full_name),
                scope=scope,
                bundle=bundle,
            )
        order_specs.append(OrderSpec(bound=bo, direction=o.direction))

    # Attach the active TD as time_key on every time-needing TransformKey the binder left at None.
    active_td_key: Optional[TimeTruncKey] = None
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        active_td = _resolve_main_time_dimension(
            query=query, model=scope.source_model,
        )
        if active_td is not None:
            active_td_bound = bind_time_dimension(
                td=active_td, scope=scope, bundle=bundle,
            )
            atd_key = active_td_bound.value_key
            assert isinstance(atd_key, TimeTruncKey)
            active_td_key = atd_key

    if active_td_key is not None:
        declared_measures, bound_filters, order_specs = _map_bound_keys(
            lambda vk: _attach_time_keys(vk, td_key=active_td_key),
            declared_measures=declared_measures,
            bound_filters=bound_filters,
            order_specs=order_specs,
        )

    # Any time-needing transform still at time_key=None means no resolvable TD.
    for bucket in (
        [dm.bound.value_key for dm in declared_measures],
        [bf.value_key for bf in bound_filters],
        [spec.bound.value_key for spec in order_specs],
    ):
        for vk in bucket:
            op = _find_unresolved_time_needing_op(vk)
            if op is not None:
                raise ValueError(
                    f"Transform '{op}' requires an unambiguous time "
                    f"dimension. Add a single time_dimensions entry, or "
                    f"set main_time_dimension to select among multiple "
                    f"time dimensions."
                )

    # Sugar lowering runs AFTER patching so the desugared time_shift inherits the patched time_key.
    declared_measures, bound_filters, order_specs = _map_bound_keys(
        lower_sugar_transforms,
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    # Validate every rank-family partition_by column resolves to a query dim/td, rewriting a td source column to its bucket TimeTruncKey. Runs BEFORE interning.
    _dim_dms, _td_dms, _ = partition_declared_measures(
        declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    _dim_key_set = {dm.bound.value_key for dm in _dim_dms}
    # A source column at two granularities maps to two buckets — a bare partition_by is then ambiguous.
    _td_by_source: Dict[ValueKey, TimeTruncKey] = {}
    _td_ambiguous_sources: set = set()
    for dm in _td_dms:
        vk = dm.bound.value_key
        if not isinstance(vk, TimeTruncKey):
            continue
        # Ambiguous only if the same column already mapped to a DIFFERENT bucket.
        if vk.column in _td_by_source and _td_by_source[vk.column] != vk:
            _td_ambiguous_sources.add(vk.column)
        _td_by_source[vk.column] = vk
    _td_key_set = set(_td_by_source.values())
    _available_dims = [dm.declared_name for dm in (*_dim_dms, *_td_dms)]
    # A partitioned aggregate inside a computed dimension declares a producer grain (partition_by may be finer than the query).
    _dim_agg_keys = frozenset(dimension_partitioned_aggregates(declared_measures))
    # A COMBINED-position partitioned aggregate needs query-dimension partition keys
    # for the join-back; local and cross-model partitioned consumers alike.
    _consumers = combined_consumer_aggregates(
        declared_measures=declared_measures, order_specs=order_specs,
        row_agg_set=_dim_agg_keys, bound_filters=bound_filters,
    )
    _combined_consumer_keys = frozenset(
        [*_consumers.local_partitioned, *_consumers.cross_model_partitioned]
    )

    def _validate_partition_keys(key: ValueKey) -> frozenset:
        label = (
            f"Transform '{key.op}'" if isinstance(key, TransformKey)
            else f"Aggregation '{key.agg}'"
        )
        lenient = key in _dim_agg_keys and key not in _combined_consumer_keys
        new_pks = []
        for pk in key.partition_keys or ():
            # A partition key over a join must be attributable from the root; else a hard error.
            _assert_partition_key_attributable(
                key=key, pk=pk, label=label, scope=scope, bundle=bundle,
            )
            if pk in _dim_key_set or pk in _td_key_set:
                new_pks.append(pk)          # already a query dim / td bucket
            elif pk in _td_ambiguous_sources:
                raise ValueError(
                    f"{label}: partition_by column "
                    f"'{_partition_key_display(pk)}' is ambiguous — it is a "
                    f"time dimension at multiple granularities. Partition by a "
                    f"single query dimension instead."
                )
            elif pk in _td_by_source:
                new_pks.append(_td_by_source[pk])  # td source col -> bucket
            elif lenient:
                new_pks.append(pk)          # finer-grain producer key (DEV-1825)
            else:
                raise ValueError(
                    f"{label}: partition_by column "
                    f"'{_partition_key_display(pk)}' is not a query dimension. "
                    f"Add it to dimensions/time_dimensions, or choose one of: "
                    f"{', '.join(_available_dims) or '(none)'}."
                )
        return frozenset(new_pks)

    def _rw(vk: ValueKey) -> ValueKey:
        return rewrite_rank_partition_keys(vk, rewrite_fn=_validate_partition_keys)

    declared_measures, bound_filters, order_specs = _map_bound_keys(
        _rw,
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    _guard_dimension_temporal_axis(declared_measures)

    return PreboundQuery(
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        bound_filter_texts=bound_filter_texts,
        n_date_range=n_date_range,
        order_specs=order_specs,
        main_time_key=active_td_key,
        n_dims=n_dims,
        n_time_dimensions=n_tds,
        limit=query.limit,
        offset=query.offset,
        distinct_dimension_values=query.distinct_dimension_values,
    )


# Regroup desugar: synthesize a producer stage per partition set.


def _regroup_grain_name(pk: ValueKey) -> str:
    if isinstance(pk, TimeTruncKey):
        return f"{column_leaf(pk.column)}_{pk.granularity}"
    path = tuple(getattr(pk, "path", ()) or ())
    leaf = getattr(pk, "leaf", None) or getattr(pk, "column_name", None) or "grain"
    return "__".join([*path, leaf])


def _regroup_partition_order(pks: FrozenSet[ValueKey]) -> List[ValueKey]:
    return sorted(
        pks, key=lambda k: (isinstance(k, TimeTruncKey), _regroup_grain_name(k), repr(k)),
    )


def _regroup_producer_prebound(  # NOSONAR(S3776) — one producer-prebound assembly; the grain / aggregate / inherited-filter / order arms share the prebound under construction.
    *,
    pks: FrozenSet[ValueKey],
    aggs: List[AggregateKey],
    model: Optional[SlayerModel],
    bundle: ResolvedSourceBundle,
    inherited: List[BoundFilter],
    n_date_range: int,
    partition_order: Callable[
        [FrozenSet[ValueKey]], List[ValueKey],
    ] = _regroup_partition_order,
    public_alias_by_agg: Optional[Mapping[AggregateKey, str]] = None,
    explicit_types: Optional[Mapping[ValueKey, DataType]] = None,
    grain_name_by_key: Optional[Mapping[ValueKey, str]] = None,
    window_td_key: Optional[ValueKey] = None,
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
            bound=BinderBoundExpr(value_key=pk),
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
            bound=BinderBoundExpr(value_key=agg),
            declared_name=canonical, public_name=canonical,
            type=explicit_types.get(agg, a_type), format=a_fmt, description=a_desc,
            type_is_explicit=agg in explicit_types,
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
    )
    return prebound, [*dims, *tds]


def _regroup_inherited_filters(
    prebound: PreboundQuery, dim_agg_set: FrozenSet[AggregateKey],
) -> Tuple[List[BoundFilter], int]:
    date_bounds: List[BoundFilter] = []
    others: List[BoundFilter] = []
    for idx, bf in enumerate(prebound.bound_filters):
        if classify_regroup_filter(bf, dim_agg_set) != "row_inherit":
            continue
        if idx < prebound.n_date_range:
            date_bounds.append(bf)
        else:
            others.append(bf)
    return [*date_bounds, *others], len(date_bounds)


def _find_regroup_slot(slots: List[ValueSlot], key: ValueKey, *, role: str) -> SlotId:
    for slot in slots:
        if slot.key == key:
            return slot.id
    raise ValueError(
        f"Regroup producer plan is missing the {role} slot for "
        f"{type(key).__name__}; synthesis and planning disagree on its grain.",
    )


def _regroup_answer_slot_id(
    *, value_slots: List[ValueSlot], key: ValueKey, fallback: Optional[SlotId],
) -> SlotId:
    for slot in value_slots:
        if slot.key == key:
            return slot.id
    if fallback is not None:
        return fallback
    raise ValueError(
        f"Regroup producer plan is missing the answer slot for "
        f"{type(key).__name__}; synthesis and planning disagree on its grain.",
    )


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


def _validate_nested_producer_plan(
    *, producer_plan, producer_grain: FrozenSet[ValueKey],
) -> None:
    """A union-grain producer MAY carry nested COMBINED regroup attaches; admit only well-formed ones (combined-phase, local, no deeper CTE, STRICT-subset grain)."""
    for attach in producer_plan.regroup_attach_plans:
        # A ROW attach groups into ``_base`` at any grain; only COMBINED nested attaches carry the strict-subset rule.
        if attach.attach_phase == "row":
            continue
        nested = attach.producer_plan
        if nested.regroup_attach_plans:
            raise NotImplementedError(
                "A union-grain producer's nested attach itself needs a further "
                "regroup producer CTE, which is not supported (DEV-1839)."
            )
        grain = frozenset(host_key for host_key, _ in attach.join_pairs)
        # A WINDOWED nested attach joins at the FULL union grain, not a strict subset.
        windowed_attach = any(
            _window_kwarg_of(sub.original_key) is not None
            for sub in attach.substitutions
        )
        ok = grain <= producer_grain if windowed_attach else grain < producer_grain
        if not ok:
            raise NotImplementedError(
                "A union-grain producer's nested attach grain is not a subset "
                "of the producer grain; only subset inner grains broadcast "
                "(DEV-1839)."
            )


def _is_local_partitioned_agg(k: ValueKey) -> bool:
    return (
        isinstance(k, AggregateKey)
        and k.partition_keys is not None
        and not getattr(k.source, "path", ())
    )


def _bound_filter_from_key(vk: ValueKey) -> BoundFilter:
    refs = tuple(walk_value_keys(vk))
    phase = max((k.phase for k in refs), default=vk.phase)
    return BoundFilter(value_key=vk, phase=phase, referenced_keys=refs)


def _partitioned_conjunct_scope(
    cj: ValueKey, *, dim_keys: frozenset, row_agg_set: frozenset,
    crossing_root: Optional[Callable[[ValueKey], bool]],
) -> str:
    """The routing scope of one split conjunct: a cross-model / crossing-input aggregate predicate resolves at the combined SELECT (transform-wrapped → POST-phase)."""
    cj_refs = list(walk_value_keys(cj))
    cj_has_transform = any(isinstance(k, TransformKey) for k in cj_refs)
    # A row-role aggregate (the computed dimension's own) stays row-scoped even when
    # cross-model; only non-row refs take the combined-SELECT shortcut.
    combined_refs = [k for k in cj_refs if k not in row_agg_set]
    if not cj_has_transform and (
        any(_is_cross_model_agg(k) for k in combined_refs)
        or (crossing_root is not None and any(crossing_root(k) for k in combined_refs))
    ):
        return "combined"
    return conjunct_scope(cj, dim_keys=dim_keys, row_agg_set=row_agg_set)


def _split_partitioned_filter_conjuncts(
    prebound: PreboundQuery,
    *,
    crossing_root: Optional[Callable[[ValueKey], bool]] = None,
) -> Tuple[PreboundQuery, List[int]]:
    """Split top-level AND conjuncts of any filter referencing a LOCAL partitioned aggregate, each routed to its own phase. Returns rebuilt prebound + COMBINED-scope indices."""
    old = list(prebound.bound_filters)
    # conjunct_scope routes only COMBINED partitioned aggregates; a computed-dimension one is a ROW attach.
    row_agg_set = frozenset(
        dimension_partitioned_aggregates(prebound.declared_measures),
    )

    def _has_partitioned_ref(vk: ValueKey) -> bool:
        # A top-level AND referencing ANY local partitioned aggregate must split so each conjunct routes to its own phase.
        return any(
            is_local_combined_regroup_ref(k, row_agg_set=row_agg_set)
            or _is_local_partitioned_agg(k)
            # A cross-model / crossing-input aggregate resolves after join-back → outer WHERE.
            or _is_cross_model_agg(k)
            or (crossing_root is not None and crossing_root(k))
            for k in walk_value_keys(vk)
        )

    if not any(_has_partitioned_ref(bf.value_key) for bf in old):
        return prebound, []
    dim_keys = frozenset(
        dm.bound.value_key
        for dm in prebound.declared_measures[
            : prebound.n_dims + prebound.n_time_dimensions
        ]
    )
    texts = list(prebound.bound_filter_texts)
    new_filters: List[BoundFilter] = []
    new_texts: List[Optional[str]] = []
    combined_idx: List[int] = []
    for i, bf in enumerate(old):
        if not _has_partitioned_ref(bf.value_key):
            new_filters.append(bf)
            new_texts.append(texts[i])
            continue
        for cj in split_top_level_and(bf.value_key):
            scope = _partitioned_conjunct_scope(
                cj, dim_keys=dim_keys, row_agg_set=row_agg_set,
                crossing_root=crossing_root,
            )
            if scope == "combined":
                combined_idx.append(len(new_filters))
            new_filters.append(_bound_filter_from_key(cj))
            new_texts.append(None)
    updated = prebound.model_copy(update={
        "bound_filters": new_filters,
        "bound_filter_texts": new_texts,
    })
    return updated, combined_idx


# Bare windowed / first-last measures desugar as combined-attach roots.
def _is_bare_local_regroup_root(k: ValueKey) -> bool:
    return (
        isinstance(k, AggregateKey)
        and k.partition_keys is None
        and not getattr(k.source, "path", ())
        and (_window_kwarg_of(k) is not None or k.agg in RANKED_AGGREGATIONS)
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
) -> Tuple[FrozenSet[ValueKey], bool]:
    """A combined-root's producer grain and windowedness.

    An explicitly-partitioned aggregate keeps ``regroup_root_grain``. A bare
    windowed / first-last root takes the FULL projected grain (a windowed root's
    bucket enters via ``window_td_key``, so it is excluded here)."""
    windowed = _window_kwarg_of(agg) is not None
    if getattr(agg, "partition_keys", None) is not None:
        grain = regroup_root_grain(agg)
        # A transform over a window= inner gains the active bucket in its union grain
        # and renders windowed; first/last inners are timeless.
        if (
            not windowed and active_bucket is not None
            and any(_window_kwarg_of(k) is not None for k in walk_value_keys(agg))
        ):
            return grain | {active_bucket}, True
        return grain, windowed
    if windowed:
        grain = frozenset(projected_dim_keys) | (
            frozenset(projected_td_keys) - ({active_bucket} if active_bucket else frozenset())
        )
    else:
        grain = frozenset(projected_dim_keys) | frozenset(projected_td_keys)
    return grain, windowed


def _scalar_free_columns(node: ValueKey, out: set) -> None:
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


def _prune_functionally_determined_grain(
    pks: FrozenSet[ValueKey],
) -> FrozenSet[ValueKey]:
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
    return frozenset(kept)


def _windowed_or_ranked_identity(agg: ValueKey):
    """A hashable, partition-free identity for a windowed / ranked aggregate (own producer each); ``None`` for a plain aggregate."""
    if not isinstance(agg, AggregateKey):
        return None
    windowed = _window_kwarg_of(agg) is not None
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


# Cross-model aggregates as target-rooted regroup producers.
def _is_cross_model_agg(k: ValueKey) -> bool:
    """A cross-model AggregateKey (source names another model); a host-grain wrap (grain="host") is excluded."""
    return (
        isinstance(k, AggregateKey)
        and bool(getattr(k.source, "path", ()))
        and getattr(k, "grain", "target") != "host"
    )


def _key_host_path(key: ValueKey) -> Tuple[str, ...]:
    if isinstance(key, TimeTruncKey):
        return tuple(getattr(key.column, "path", ()) or ())
    return tuple(getattr(key, "path", ()) or ())


def _attributable_from_root(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str] = None,
) -> bool:
    """Is a host-coordinate path attributable from the aggregate's root over provably many-to-one hops only?"""
    tp, hp = tuple(target_path), tuple(host_path)
    if hp[: len(tp)] == tp:
        return safe_reachable(
            root=root_model, path=hp[len(tp):], models_by_name=models_by_name,
        )
    if host_name is None or (tp and host_name == tp[0]):
        return False
    if hp and safe_reachable(root=root_model, path=hp, models_by_name=models_by_name):
        return True
    return safe_reachable(
        root=root_model, path=(host_name, *hp), models_by_name=models_by_name,
    )


def _reroot_leaf_via_host(
    r: ValueKey, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
) -> Optional[ValueKey]:
    if not isinstance(r, (ColumnKey, ColumnSqlKey, StarKey, TimeTruncKey)):
        return None
    hp = _key_host_path(r)
    if hp[: len(target_path)] == target_path:
        return None  # reroot_value_key strips the prefix
    if target_path and host_name == target_path[0]:
        return None
    via_host = (host_name, *hp)
    if not safe_reachable(
        root=root_model, path=via_host, models_by_name=models_by_name,
    ) and hp and safe_reachable(
        root=root_model, path=hp, models_by_name=models_by_name,
    ):
        return None  # resolved through the root's own join to the sibling
    if isinstance(r, TimeTruncKey):
        return r.model_copy(update={
            "column": r.column.model_copy(update={"path": via_host}),
        })
    return r.model_copy(update={"path": via_host})


def _reroot_from_root(
    key: ValueKey, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
) -> ValueKey:
    """Re-anchor a host-coordinate key into the root's coordinates, per leaf, by the same rules ``_attributable_from_root`` proves safety with."""
    tp = tuple(target_path)
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in walk_value_keys(key):
        # walk_value_keys does NOT descend into TimeTruncKey.column; reroot whole.
        rerooted = _reroot_leaf_via_host(
            r, target_path=tp, root_model=root_model,
            models_by_name=models_by_name, host_name=host_name,
        )
        if rerooted is not None:
            mapping[r] = rerooted
    if mapping:
        key = substitute_value_keys(key, mapping)
    return reroot_value_key(key, target_path=tp)


def _broadcast_reason(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
) -> str:
    """Why a dimension broadcasts: unreachable from the root, or crosses an unproven/fanning join hop."""
    tp, hp = tuple(target_path), tuple(host_path)
    if hp[: len(tp)] != tp:
        return "unreachable from the aggregate's root (no join path from it)"
    current = root_model
    for name in hp[len(tp):]:
        join = next((j for j in current.joins if j.target_model == name), None)
        if join is None:
            return "unreachable from the aggregate's root (no join path from it)"
        tgt = models_by_name.get(name)
        if tgt is None or not provably_to_one(join=join, target_model=tgt):
            return f"crosses an unproven join hop to {name}"
        current = tgt
    return "unreachable from the aggregate's root"


def _assert_partition_key_attributable(
    *, key: ValueKey, pk: ValueKey, label: str,
    scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> None:
    """A partition key reached over a join must be attributable from the aggregate's root; an unproven/fanning hop is a hard error."""
    hp = _key_host_path(pk)
    if not hp:
        return  # a local column — no join to cross
    host_m = scope.source_model if isinstance(scope, ModelScope) else None
    if host_m is None:
        return
    agg_target = tuple(key.source.path) if isinstance(key, AggregateKey) else ()
    models_by_name = {m.name: m for m in bundle.referenced_models}
    root = walk_key_path(model=host_m, path=agg_target, bundle=bundle) or host_m
    if _attributable_from_root(
        host_path=hp, target_path=agg_target, root_model=root,
        models_by_name=models_by_name,
        host_name=host_m.name if agg_target else None,
    ):
        return
    reason = _broadcast_reason(
        host_path=hp, target_path=agg_target, root_model=root,
        models_by_name=models_by_name,
    )
    raise ValueError(
        f"{label}: partition_by column '{_partition_key_display(pk)}' {reason}; "
        f"every partition key must be attributable from the aggregate's root — "
        f"declare join cardinality or a covering unique key on the target."
    )


def _shared_join_key_reroot(
    *, key: ValueKey, target_path: Tuple[str, ...], host_model: SlayerModel,
) -> Optional[ValueKey]:
    """A host-local dimension that IS a source-side join column of the single hop to the root: return the root's target-side ColumnKey, else ``None``."""
    if not isinstance(key, ColumnKey) or _key_host_path(key) or len(target_path) != 1:
        return None
    root_join = next(
        (j for j in host_model.joins if j.target_model == target_path[0]), None,
    )
    if root_join is None:
        return None
    for src, tgt in root_join.join_pairs:
        if src == key.leaf:
            return key.model_copy(update={"leaf": tgt, "path": ()})
    return None


def _grain_member_attributable(
    *, key: ValueKey, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
) -> bool:
    """Is a grain member attributable from the aggregate's root? (Every column/aggregate it references must be.)"""
    saw = False
    for r in walk_value_keys(key):
        if isinstance(r, AggregateKey):
            saw = True
            if not _attributable_from_root(
                host_path=tuple(r.source.path), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
            ):
                return False
        elif isinstance(r, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey)):
            saw = True
            if not _attributable_from_root(
                host_path=_key_host_path(r), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_name,
            ):
                return False
    return saw


def _cross_model_input_paths(
    *, agg_rooted: AggregateKey, root_model: SlayerModel, root_name: str,
    bundle: ResolvedSourceBundle,
) -> List[Tuple[str, ...]]:
    out: List[Tuple[str, ...]] = []
    if agg_rooted.column_filter_key is not None:
        for p in agg_rooted.column_filter_key.referenced_join_paths:
            if tuple(p) not in out:
                out.append(tuple(p))
    for p in compute_aggregate_input_join_paths(
        key=agg_rooted, anchor_model=root_model, anchor_relation=root_name,
        bundle=bundle,
    ):
        if tuple(p) not in out:
            out.append(tuple(p))
    return out


def _assert_cross_model_inputs_safe(
    *, agg: AggregateKey, agg_rooted: AggregateKey, root_model: SlayerModel,
    root_name: str, target_path: Tuple[str, ...], bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel],
) -> None:
    """Every input of a cross-model aggregate must be attributable from its root; a fanning/unproven join is a hard error."""
    remedy = "declare join cardinality or a covering unique key on the target"
    # Source-column / kwarg / column-filter refs, in the root's coordinates.
    for path in _cross_model_input_paths(
        agg_rooted=agg_rooted, root_model=root_model, root_name=root_name, bundle=bundle,
    ):
        if not safe_reachable(
            root=root_model, path=path, models_by_name=models_by_name,
        ):
            hop = path[-1] if path else root_name
            raise ValueError(
                f"Cross-model aggregate {canonical_aggregate_alias(agg, profile='stage_formula')!r} "
                f"reads an input across an unproven join hop to {hop} from "
                f"{root_name}; {remedy}."
            )
    # Positional args in HOST coordinates (a ranking first/last time key).
    for arg in agg.args:
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        hp = _key_host_path(arg)
        if not _attributable_from_root(
            host_path=hp, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name,
        ):
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            raise ValueError(
                f"Cross-model aggregate {canonical_aggregate_alias(agg, profile='stage_formula')!r} "
                f"ranks/reads by {leaf}, which is not attributable from "
                f"{root_name} (crosses a fanning join); {remedy}."
            )


def _cross_model_inherited_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
) -> Tuple[List[BoundFilter], List[UnreachableFilterDroppedWarning]]:
    """Split base ROW filters into conjuncts; a fully-attributable one inherits (re-rooted) into the producer, an unreachable one is dropped and warned."""
    inherited: List[BoundFilter] = []
    dropped: List[UnreachableFilterDroppedWarning] = []
    for bf, text in base_filters:
        if bf.phase != Phase.ROW:
            continue
        for cj in split_top_level_and(bf.value_key):
            inherited_bf, dropped_w = _conjunct_disposition(
                cj, text=text, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_name,
            )
            if inherited_bf is not None:
                inherited.append(inherited_bf)
            else:
                dropped.append(dropped_w)
    return inherited, dropped


def _local_crossing_input_paths(
    *, key: AggregateKey, bundle: ResolvedSourceBundle,
    host_model: SlayerModel, include_source: bool = True,
) -> List[Tuple[str, ...]]:
    out: List[Tuple[str, ...]] = []
    if key.column_filter_key is not None:
        for p in key.column_filter_key.referenced_join_paths:
            if p not in out:
                out.append(tuple(p))
    for p in compute_aggregate_input_join_paths(
        key=key,
        anchor_model=host_model,
        anchor_relation=host_model.name,
        bundle=bundle,
        include_source=include_source,
    ):
        if p not in out:
            out.append(tuple(p))
    return out


def _crossing_local_root_predicate(
    *, scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> Callable[[ValueKey], bool]:
    """Predicate for a LOCAL plain aggregate whose inputs cross a join (desugars onto a HOST-rooted producer); windowed / ranked roots excluded."""
    host_model = scope.source_model if isinstance(scope, ModelScope) else None

    def _pred(k: ValueKey) -> bool:
        return (
            isinstance(k, AggregateKey)
            and k.partition_keys is None
            and not getattr(k.source, "path", ())
            and _window_kwarg_of(k) is None
            and k.agg not in RANKED_AGGREGATIONS
            and host_model is not None
            and _crosses(k)
        )

    def _crosses(k: AggregateKey) -> bool:
        crossed = _local_crossing_input_paths(
            key=k, bundle=bundle, host_model=host_model,
        )
        return bool(crossed) and not may_inline_crossing_inputs(crossed)

    return _pred


def _assert_local_producer_inputs_safe(
    *,
    agg: AggregateKey,
    host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel],
) -> None:
    """Per-role crossing-input safety for a HOST-rooted producer answer; a filter/arg crossing an unproven hop is a hard error (host-grain wrap's SOURCE path exempt)."""
    remedy = "declare join cardinality or a covering unique key on the target"
    alias = canonical_aggregate_alias(agg, profile="stage_formula")

    def _safe(path: Tuple[str, ...]) -> bool:
        return safe_reachable(
            root=host_model, path=tuple(path), models_by_name=models_by_name,
        )

    # Role: crossed argument, explicitly named (a first/last ranking arg).
    for arg in agg.args:
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        path = _key_host_path(arg)
        if path and not _safe(path):
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            raise ValueError(
                f"Aggregate {alias!r} ranks/reads by {leaf}, which crosses an "
                f"unproven join hop to {path[-1]} from {host_model.name}; "
                f"{remedy}."
            )

    # Crossed predicate + remaining crossed args; the SOURCE's own crossings are exempt.
    gated = _local_crossing_input_paths(
        key=agg, bundle=bundle, host_model=host_model, include_source=False,
    )
    for path in gated:
        if path and not _safe(path):
            raise ValueError(
                f"Aggregate {alias!r} reads an input across an unproven join "
                f"hop to {path[-1]} from {host_model.name}; {remedy}."
            )


def _trailing_window_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    n_date_range: int,
) -> TrailingWindowProducerKernel:
    window_raw = _window_kwarg_of(agg_key)
    bucket_sid = producer_plan.active_time_dimension_slot_id
    bucket_slot = next(
        (s for s in producer_plan.row_slots if s.id == bucket_sid), None,
    )
    if window_raw is None or bucket_slot is None:
        raise RuntimeError(
            "Windowed producer is missing its window duration or bucket slot; "
            "synthesis and planning disagree (DEV-1838)."
        )
    src_where_ids, src_rewrites = _plan_src_row_filters(
        filters_by_phase=producer_plan.filters_by_phase,
        date_range_fids={f"f{i}" for i in range(n_date_range)},
        frame_bound_columns=producer_plan.frame_bound_columns,
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
        prebound, frozenset(),
    )
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=frozenset(projected), aggs=[wrap_key], model=producer_model,
        bundle=bundle, inherited=inherited, n_date_range=n_inherited_date,
        partition_order=lambda pks: sorted(
            pks, key=lambda k: consumer_order.get(k, len(consumer_order)),
        ),
        grain_name_by_key=grain_name_by_key,
    )
    producer_plan = plan_query(
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
            or _is_local_partitioned_agg(pk)
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


def _conjunct_disposition(
    cj: ValueKey, *, text: Optional[str], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str],
) -> Tuple[Optional[BoundFilter], Optional[UnreachableFilterDroppedWarning]]:
    refs = [
        k for k in walk_value_keys(cj)
        if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey))
    ]
    unsafe = next((r for r in refs if not _attributable_from_root(
        host_path=_key_host_path(r), target_path=target_path,
        root_model=root_model, models_by_name=models_by_name,
        host_name=host_name,
    )), None)
    if unsafe is None:
        rerooted = (
            _reroot_from_root(
                cj, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_name,
            )
            if host_name is not None
            else reroot_value_key(cj, target_path=target_path)
        )
        return _bound_filter_from_key(rerooted), None
    reason = _broadcast_reason(
        host_path=_key_host_path(unsafe), target_path=target_path,
        root_model=root_model, models_by_name=models_by_name,
    )
    return None, UnreachableFilterDroppedWarning(
        filter_text=text or _canonical_name(cj), reason=reason,
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
    target_path = tuple(agg.source.path)
    root_model = walk_key_path(model=host_model, path=target_path, bundle=bundle)
    if root_model is None:  # pragma: no cover — bind resolved the path already
        raise ValueError(
            f"Cross-model aggregate source path {target_path!r} does not resolve "
            f"to a model from {host_model.name}."
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

    # Safe grain S (attributable from R) vs broadcast; an unattributable explicit key is a hard error.
    safe_pairs: List[Tuple[ValueKey, ValueKey]] = []  # (host_key, rerooted_key)
    broadcast: List[Tuple[str, str]] = []
    for g in requested:
        hp = _key_host_path(g)
        shared = _shared_join_key_reroot(
            key=g, target_path=target_path, host_model=host_model,
        )
        if shared is not None:
            # The join-key identity needs no join in the producer.
            safe_pairs.append((g, shared))
        elif _grain_member_attributable(
            key=g, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ):
            safe_pairs.append((g, _reroot_from_root(
                g, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            )))
        elif explicit:
            reason = _broadcast_reason(
                host_path=hp, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name,
            )
            raise ValueError(
                f"Cross-model aggregate {alias!r} declares partition_by="
                f"{_regroup_grain_name(g)}, which {reason}; every explicit "
                f"partition key must be attributable from {root_name} — declare "
                f"join cardinality or a covering unique key on the target."
            )
        else:
            broadcast.append((_regroup_grain_name(g), _broadcast_reason(
                host_path=hp, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name,
            )))

    agg_rooted = reroot_value_key(agg, target_path=target_path)
    _assert_cross_model_inputs_safe(
        agg=agg, agg_rooted=agg_rooted, root_model=root_model, root_name=root_name,
        target_path=target_path, bundle=bundle, models_by_name=models_by_name,
    )

    # A windowed cross-model aggregate folds the active TD into its grain as the bucket (must be attributable from the root).
    window_td_key: Optional[ValueKey] = None
    if _window_kwarg_of(agg) is not None:
        active_td = prebound.main_time_key
        if active_td is None:
            raise ValueError(
                f"Windowed cross-model aggregate {alias!r} has no active time "
                f"dimension; add a single time_dimensions entry."
            )
        if not _attributable_from_root(
            host_path=_key_host_path(active_td), target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_name=host_model.name,
        ):
            raise ValueError(
                f"Windowed cross-model aggregate {alias!r} needs the query's "
                f"active time dimension ('{_regroup_grain_name(active_td)}') "
                f"attributable from {root_name}, but it crosses a fanning join; "
                f"declare join cardinality or a covering unique key on the target."
            )
        window_td_key = _reroot_from_root(
            active_td, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        )

    inherited, dropped = _cross_model_inherited_filters(
        base_filters=base_filters_with_text, target_path=target_path,
        root_model=root_model, models_by_name=models_by_name,
        host_name=host_model.name,
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
    grain_keys = frozenset(rr for _, rr in safe_pairs)
    # The producer measure keeps the CANONICAL alias (root columns could shadow the public name).
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=grain_keys, aggs=[agg_rooted], model=root_model, bundle=root_bundle,
        inherited=inherited, n_date_range=0, window_td_key=window_td_key,
        explicit_types=(
            {agg_rooted: declared_type} if declared_type is not None else None
        ),
    )
    # A computed-dimension grain member or windowed producer re-enables discovery.
    enable_nested = window_td_key is not None or any(
        isinstance(rr, (ScalarCallKey, ArithmeticKey, TransformKey))
        or _is_local_partitioned_agg(rr)
        for rr in grain_keys
    )
    producer_plan = plan_query(
        query=StrictQueryCarrier(source_model=root_name, prebound=producer_prebound),
        bundle=root_bundle, scope=root_scope,
        stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        enable_producer_regroups=enable_nested,
        prebound=producer_prebound,
        producer_registry=producer_registry,
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
            producer_plan=producer_plan, agg_key=agg_rooted, n_date_range=0,
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
                if _is_cross_model_agg(k) or (
                    isinstance(k, AggregateKey) and k.partition_keys is not None
                ):
                    raise ValueError(
                        f"Aggregate {_canonical_name(k)!r} in a {role} received "
                        f"no routing disposition (inline, producer substitution, "
                        f"or explicit rejection) — the planner cannot compile "
                        f"this shape."
                    )


def _structural_fingerprint(obj) -> Hashable:
    if isinstance(obj, BaseModel):
        return (
            type(obj).__name__,
            tuple(
                (name, _structural_fingerprint(getattr(obj, name)))
                for name in type(obj).model_fields
            ),
        )
    if isinstance(obj, (list, tuple)):
        return tuple(_structural_fingerprint(x) for x in obj)
    if isinstance(obj, (set, frozenset)):
        return frozenset(_structural_fingerprint(x) for x in obj)
    if isinstance(obj, dict):
        return tuple(sorted(
            (
                (_structural_fingerprint(k), _structural_fingerprint(v))
                for k, v in obj.items()
            ),
            key=repr,
        ))
    if isinstance(obj, Enum) or obj is None or isinstance(
        obj, (str, int, float, bool, bytes),
    ):
        return obj
    return (type(obj).__name__, repr(obj))


def regroup_producer_identity(attach: RegroupAttachPlan) -> Hashable:
    """Interning identity of a regroup producer: its root plus the full structural spec of the producer body (never the render-level attach coordinates)."""
    return (
        attach.producer_root_model,
        _structural_fingerprint(attach.kernel),
        _structural_fingerprint(attach.producer_plan),
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
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_model: Optional[str],
    in_producer: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
    local_discovery: bool = True,
) -> Optional[Tuple[PreboundQuery, List[RegroupAttachPlan]]]:
    """Discover partitioned aggregates and desugar into producer stages + reserved-leaf placeholders (row attach at base FROM, combined at the combined SELECT)."""
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
    consumers = combined_consumer_aggregates(
        declared_measures=prebound.declared_measures,
        order_specs=prebound.order_specs,
        row_agg_set=frozenset(row_inner_aggs),
        bound_filters=prebound.bound_filters,
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
    _is_crossing_local_root = _crossing_local_root_predicate(
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

    def _root_grain(agg: ValueKey) -> FrozenSet[ValueKey]:
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
        own_grain = frozenset([*projected_dim_keys, *projected_td_keys])
        windowed_transform_inputs = {
            k
            for dm in prebound.declared_measures
            for tk in walk_value_keys(dm.bound.value_key)
            if isinstance(tk, TransformKey)
            for k in walk_value_keys(tk.input)
            if _window_kwarg_of(k) is not None
        }
        combined_aggs = [
            k for k in combined_aggs
            if _root_grain(k) != own_grain or k in windowed_transform_inputs
        ]
        row_aggs = [k for k in row_aggs if regroup_root_grain(k) != own_grain]
    # Cross-model aggregates become target-rooted producers; a cross-model root inside a computed dimension is a ROW-phase producer.
    cm_row = [k for k in row_aggs if _is_cross_model_agg(k)]
    row_aggs = [k for k in row_aggs if not _is_cross_model_agg(k)]
    cm_combined = [
        *consumers.cross_model_partitioned, *consumers.cross_model_bare,
    ]
    cm_type = dict(consumers.declared_type)
    if not row_aggs and not combined_aggs and not cm_combined and not cm_row:
        return None
    # A real column sharing the reserved placeholder prefix would shadow a placeholder at render; reject while a regroup is active.
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    reserved = reserved_prefix_columns(
        producer_model if isinstance(scope, ModelScope) else scope
    )
    if reserved:
        raise ValueError(
            f"Column(s) {reserved!r} use the reserved '__regroup__' prefix, which "
            f"collides with the regroup primitive's placeholders. Rename them."
        )
    registry = RegroupPlaceholderRegistry()
    mapping: Dict[ValueKey, ValueKey] = {
        agg: registry.placeholder_for(agg)
        for agg in (*row_aggs, *combined_aggs, *cm_row, *cm_combined)
    }
    # A cross-model computed-dimension aggregate is a ROW attach; its ROW conjuncts classify like the local ones.
    dim_agg_set = frozenset([*row_inner_aggs, *cm_row])

    inherited, n_inherited_date = _regroup_inherited_filters(prebound, dim_agg_set)

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

    def _combined_order(pks: FrozenSet[ValueKey]) -> List[ValueKey]:
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
        group_meta: Dict[Tuple, Tuple[FrozenSet[ValueKey], bool]] = {}
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
            )
            producer_plan = plan_query(
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
                        or _is_local_partitioned_agg(pk)
                        for pk in pks
                    ) or any(isinstance(a, TransformKey) for a in producer_aggs)
                ),
                prebound=producer_prebound,
                producer_registry=producer_registry,
            )
            # A union-grain producer MAY carry nested COMBINED attaches; admit them
            # after structural validation.
            _validate_nested_producer_plan(
                producer_plan=producer_plan, producer_grain=pks,
            )
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
                join_pairs.append((pk, slot_id))
            _assert_attach_covers_producer_grain(
                joined_slot_ids={slot_id for _, slot_id in join_pairs},
                producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
            )
            # A producer whose answer IS a windowed / ranked aggregate carries the matching kernel.
            attach_kwargs: Dict[str, Any] = {}
            if (
                windowed
                and isinstance(producer_aggs[0], AggregateKey)
                and _window_kwarg_of(producer_aggs[0]) is not None
            ):
                attach_kwargs["kernel"] = _trailing_window_kernel(
                    producer_plan=producer_plan, agg_key=producer_aggs[0],
                    n_date_range=n_inherited_date,
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

    # The ROW substitution applies ONLY to computed DIMENSIONS; a non-dim measure keeps query-grain (its inners desugar to COMBINED placeholders).
    combined_mapping: Dict[ValueKey, ValueKey] = {
        agg: mapping[agg] for agg in (*combined_aggs, *cm_combined)
    }
    rewritten = PreboundQuery(
        declared_measures=[
            DeclaredMeasure(
                bound=BinderBoundExpr(
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
                bound=BinderBoundExpr(
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
    )
    # Intern every producer: a structurally identical one becomes the same plan object.
    attaches = [_intern_producer(a, producer_registry) for a in attaches]
    return rewritten, attaches


def plan_query(  # NOSONAR(S3776) — planner entry-point dispatcher. The DEV-1503 addition is a small trigger-predicate branch + a kwarg pass-through; the function's pre-existing complexity is owned by the multi-stage scope / bundle / projection / filter-routing wiring it orchestrates and is tracked as a separate refactor.
    *,
    query: Union[SlayerQuery, StrictQueryCarrier],
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    disable_host_rooted_isolation: bool = False,
    enable_producer_regroups: bool = False,
    prebound: Optional[PreboundQuery] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile one query into a typed ``PlannedQuery``; ``disable_host_rooted_isolation`` suppresses the LOCAL half of the regroup desugar (recursion guard)."""
    stage_schemas = stage_schemas or {}
    # One interning registry per top-level plan; nested producer calls thread it down.
    if producer_registry is None:
        producer_registry = {}

    if scope is None:
        scope = _resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )

    # The generator renders FROM / joins against the binder's model (ModelScope → host; StageSchema → None).
    render_source_model = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )

    if prebound is None:
        # A StrictQueryCarrier always arrives paired with its prebound; reaching the parser with one is a bug.
        assert isinstance(query, SlayerQuery)
        prebound = bind_query_inputs(
            query=query, bundle=bundle, scope=scope,
            stage_schemas=stage_schemas,
        )
    # Split top-level AND conjuncts of any filter referencing a LOCAL partitioned aggregate; combined_filter_indices → outer WHERE.
    combined_filter_indices: List[int] = []
    if not disable_host_rooted_isolation:
        prebound, combined_filter_indices = _split_partitioned_filter_conjuncts(
            prebound,
            crossing_root=_crossing_local_root_predicate(
                scope=scope, bundle=bundle,
            ),
        )
    declared_measures = list(prebound.declared_measures)
    bound_filters = list(prebound.bound_filters)
    n_date_range = prebound.n_date_range
    order_specs = list(prebound.order_specs)
    active_td_key = prebound.main_time_key
    n_dims = prebound.n_dims
    n_tds = prebound.n_time_dimensions
    distinct_dimension_values = prebound.distinct_dimension_values

    # Deferred partition_by shape guards run on the pre-substitution trees; computed-dimension aggregates are excluded (the desugar consumes them).
    _orig_row_aggs = frozenset(
        dimension_partitioned_aggregates(declared_measures),
    )
    _guard_partitioned_measures(
        measure_vks=[dm.bound.value_key for dm in declared_measures],
        filter_vks=[bf.value_key for bf in bound_filters],
        order_vks=[sp.bound.value_key for sp in order_specs],
        exclude=_orig_row_aggs,
    )

    # Desugar partitioned aggregates into producer stages + reserved-leaf placeholders, AFTER the guard above.
    regroup_attach_plans: List[RegroupAttachPlan] = []
    if isinstance(query.source_model, str):
        _producer_source_model = query.source_model
    elif render_source_model is not None:
        _producer_source_model = render_source_model.name
    else:
        _producer_source_model = None
    # The desugar always runs; the LOCAL half is suppressed in a disabled sub-plan, cross-model roots always desugar.
    regroup_result = _plan_regroups(
        prebound=prebound, scope=scope, bundle=bundle,
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

    # SlayerModel.filters — Mode-A SQL WHERE, scope-derived so a sub-plan gets its own.
    text_filter_entries: List[FilterPhase] = []
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        for j, mf in enumerate(scope.source_model.filters or []):
            text_filter_entries.append(_validate_model_filter(
                mf=mf, idx=j, model=scope.source_model,
            ))

    source_col_names = _source_column_names(scope)
    host_model_name = _host_model_name(scope)

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
        host_model_name=host_model_name,
    )

    row_slots, agg_slots, combined_slots = _bucket_slots(
        projection.registry.slots,
    )

    # Raw-rows mode: any aggregate-phase slot came from a filter or order item (measures rejected upstream), which the flag forbids.
    if distinct_dimension_values is False and agg_slots:
        offender = _canonical_name(agg_slots[0].key)
        raise DistinctDimensionValuesError(
            f"distinct_dimension_values=False rejects measure references, but "
            f"this query references the aggregation {offender!r} in its "
            f"filters or order. Either remove the measure reference, or set "
            f"distinct_dimension_values=True (the default) to keep the "
            f"auto-aggregating behaviour."
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
                grain="host" if path else "target",
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
                and _local_crossing_input_paths(
                    key=wrap_key, bundle=bundle,
                    host_model=host_model_for_wraps,
                )
            )
            if _wrap_crosses and wrap_key not in late_wrap_keys:
                late_wrap_keys.add(wrap_key)
                regroup_attach_plans.append(_intern_producer(
                    _synthesize_wrap_attach(
                        wrap_key=wrap_key, prebound=prebound, scope=scope,
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

    # filters_by_phase in WHERE order: date_range, model.filters, user query filters.
    # A filter referencing a windowed slot is reclassified to Phase.POST (value joined back → predicate on the combined SELECT).
    def _windowed_phase(bf: BoundFilter) -> Phase:
        if windowed_slot_ids and (
            filter_referenced_slot_ids(bf, projection.registry) & windowed_slot_ids
        ):
            return Phase.POST
        return bf.phase

    filters_by_phase: List[FilterPhase] = []
    bound_filter_ids: List[str] = []
    for i, bf in enumerate(bound_filters[:n_date_range]):
        fid = f"f{i}"
        filters_by_phase.append(
            FilterPhase(
                id=fid, phase=_windowed_phase(bf), text=None,
                expression=PlannedBoundExpr(value_key=bf.value_key),
            ),
        )
        bound_filter_ids.append(fid)
    filters_by_phase.extend(text_filter_entries)
    for i, bf in enumerate(bound_filters[n_date_range:], start=n_date_range):
        fid = f"f{i}"
        filters_by_phase.append(
            FilterPhase(
                id=fid, phase=_windowed_phase(bf), text=None,
                expression=PlannedBoundExpr(value_key=bf.value_key),
            ),
        )
        bound_filter_ids.append(fid)
    # Per-filter structural reachability summary, in this plan's coordinate system.
    reachability_anchor_model = render_source_model or bundle.source_model
    source_relation = (
        query.source_model
        if isinstance(query.source_model, str)
        else host_model_name
    )
    filter_reachability: List[FilterReachability] = []
    # One expansion cache for the whole plan (both visitors and every filter share it).
    reachability_cache: dict = {}
    for fp in filters_by_phase:
        if fp.expression is None:
            continue
        filter_reachability.append(FilterReachability(
            filter_id=fp.id,
            crossed_join_paths=compute_key_join_paths(
                key=fp.expression.value_key,
                anchor_model=reachability_anchor_model,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
            has_host_local_ref=key_has_host_local_ref(
                key=fp.expression.value_key,
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
        if (
            isinstance(key, AggregateKey)
            and getattr(key.source, "path", ())
            and getattr(key, "grain", "target") != "host"
        ):
            raise RuntimeError(
                f"Cross-model aggregate slot {slot.id!r} survived the regroup "
                f"desugar (DEV-1838 D8); every cross-model aggregate must "
                f"become a target-rooted producer."
            )

    # Loop-invariant lookups for order-scope classification.
    order_cross_model_slot_ids: set = set()
    # A combined regroup placeholder resolves at the combined SELECT like a cross-model aggregate.
    for _rap in regroup_attach_plans:
        if _rap.attach_phase != "combined":
            continue
        for _sub in _rap.substitutions:
            _psid = projection.registry.find_by_key(_sub.placeholder)
            if _psid is not None:
                order_cross_model_slot_ids.add(_psid)
    order_windowed_slot_ids = set(windowed_slot_ids)
    order_slot_by_key = {s.key: s.id for s in projection.registry.slots}
    order_entries = []
    for spec in order_specs:
        # A grouped row-column sort key was rewritten to a hidden wrap above; order on that slot.
        okey = order_key_remap.get(
            (spec.bound.value_key, spec.direction), spec.bound.value_key,
        )
        sid = projection.registry.find_by_key(okey)
        if sid is None:
            # An unslotted order target would be silently dropped; fail loudly instead.
            raise ValueError(
                f"ORDER BY expression is not supported: "
                f"{type(spec.bound.value_key).__name__} has no materialisable "
                f"slot. Order by an aggregate, a transform, a composite "
                f"arithmetic / scalar expression, a dimension, or declare the "
                f"expression as a measure and order by its name."
            )
        order_slot = projection.registry.get(sid)
        order_entries.append(OrderEntry(
            slot_id=sid,
            direction=spec.direction,
            scope=_classify_order_scope(
                slot=order_slot,
                cross_model_slot_ids=order_cross_model_slot_ids,
                windowed_slot_ids=order_windowed_slot_ids,
                public_projection=projection.public_projection,
                slot_by_key=order_slot_by_key,
            ),
            phase=order_slot.key.phase,
        ))

    transform_layers = _emit_transform_layers(slots=projection.registry.slots)
    stage_schema = _emit_stage_schema(
        stage_name=query.name, projection=projection,
    )

    # Frame-bound column set: raw columns of this stage's non-hidden time dimensions.
    frame_bound_columns = _frame_bound_columns(row_slots=row_slots)

    # A filter conjunct routed to the COMBINED scope resolves only after attachment, so it renders at the outer WHERE.
    outer_where_filter_ids: List[BoundFilterId] = []
    for idx in combined_filter_indices:
        fid = f"f{idx}"
        if fid not in outer_where_filter_ids:
            outer_where_filter_ids.append(fid)

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
        filters_by_phase=filters_by_phase,
        outer_where_filter_ids=outer_where_filter_ids,
    )

    planned = PlannedQuery(
        source_relation=source_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        regroup_attach_plans=regroup_attach_plans,
        combined_expression_slots=combined_slots,
        transform_layers=transform_layers,
        filters_by_phase=filters_by_phase,
        projection=projection.public_projection,
        order=order_entries,
        limit=prebound.limit,
        offset=prebound.offset,
        stage_schema=stage_schema,
        active_time_dimension_slot_id=active_td_slot_id,
        render_source_model=render_source_model,
        distinct_dimension_values=distinct_dimension_values,
        frame_bound_columns=frame_bound_columns,
        outer_where_filter_ids=outer_where_filter_ids,
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
    filters_by_phase: list,
    outer_where_filter_ids: List[BoundFilterId],
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
    routed: set = set(outer_where_filter_ids)
    host_filter_ids = [
        fp.id
        for fp in filters_by_phase
        if fp.phase == Phase.ROW
        and fp.id not in routed
        and (fp.expression is not None or fp.text is not None)
    ]
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
    filters_by_phase: list,
    date_range_fids: set,
    frame_bound_columns: List[ValueKey],
) -> "Tuple[List[str], List[SrcFilterRewrite]]":
    """Partition ROW-phase filters for a windowed measure's ``_src`` scope into ``(where_filter_ids, src_filter_rewrites)`` by frame-bound membership (Mode-A model filters exempt)."""
    time_cols = frozenset(frame_bound_columns)
    where_ids: List[str] = []
    rewrites: List[SrcFilterRewrite] = []
    for fp in filters_by_phase:
        if fp.phase != Phase.ROW or fp.id in date_range_fids:
            continue
        if fp.expression is None:
            where_ids.append(fp.id)  # Mode-A model filter — exempt
            continue
        residual = strip_frame_bounds(
            key=fp.expression.value_key, time_columns=time_cols,
        )
        if residual is None:
            continue  # wholly a frame bound
        where_ids.append(fp.id)
        if residual is not fp.expression.value_key:
            rewrites.append(SrcFilterRewrite(
                filter_id=fp.id, expression=PlannedBoundExpr(value_key=residual),
            ))
    return where_ids, rewrites


def _coerce_extension(spec) -> ModelExtension:
    if isinstance(spec, ModelExtension):
        return spec
    return ModelExtension.model_validate(spec)


def _stage_scope_and_bundle(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    data_source: str,
    is_root: bool,
) -> "Tuple[Union[ModelScope, StageSchema], ResolvedSourceBundle]":
    """Resolve one DAG stage's ``(scope, per-stage bundle)``; each stage binds against its OWN source, with sibling synthetic models threaded in."""
    src = query.source_model
    sibling_names = set(stage_schemas)
    sib = _source_name_if_sibling(src, sibling_names)

    # 1. ModelExtension / dict OVER a sibling: overlay the extra columns onto a synthetic sibling model.
    if sib is not None and not isinstance(src, str):
        base = synthetic_model_from_stage_schema(
            name=sib, schema=stage_schemas[sib], data_source=data_source,
        )
        overlaid = _apply_extension_overlay(base, _coerce_extension(src))
        others = {n: s for n, s in stage_schemas.items() if n != sib}
        sb = stage_bundle_with_siblings(
            bundle=bundle, source_model=overlaid,
            sibling_schemas=others, data_source=data_source,
        )
        return ModelScope(source_model=overlaid), sb

    # 2. Bare-string sibling source (chain): bind against the upstream flat StageSchema.
    if isinstance(src, str) and src in stage_schemas:
        synth = synthetic_model_from_stage_schema(
            name=src, schema=stage_schemas[src], data_source=data_source,
        )
        others = {n: s for n, s in stage_schemas.items() if n != src}
        sb = stage_bundle_with_siblings(
            bundle=bundle, source_model=synth,
            sibling_schemas=others, data_source=data_source,
        )
        return stage_schemas[src], sb

    # 3. Model-scoped: the stage's own resolved source model (root uses the bundle's).
    if is_root:
        stage_model = bundle.source_model
    else:
        stage_model = bundle.stage_source_models.get(query.name) or bundle.source_model
    sb = stage_bundle_with_siblings(
        bundle=bundle, source_model=stage_model,
        sibling_schemas=stage_schemas, data_source=data_source,
    )
    return ModelScope(source_model=stage_model), sb


def plan_stages(
    *,
    queries: List[SlayerQuery],
    bundle: ResolvedSourceBundle,
) -> List[PlannedQuery]:
    """Plan a multi-stage DAG: topo sort, then plan each stage against its own resolved source + already-planned siblings' synthetic models."""
    if len(queries) == 1:
        return [plan_query(
            query=queries[0],
            bundle=bundle,
        )]
    ordered = _topo_sort(queries)
    root = ordered[-1]
    data_source = (
        (bundle.source_model.data_source if bundle.source_model else None)
        or "_stage"
    )
    stage_schemas: Dict[str, StageSchema] = {}
    results: List[PlannedQuery] = []
    for q in ordered:
        scope, stage_bundle = _stage_scope_and_bundle(
            query=q,
            bundle=bundle,
            stage_schemas=stage_schemas,
            data_source=data_source,
            is_root=q is root,
        )
        planned = plan_query(
            query=q,
            bundle=stage_bundle,
            scope=scope,
            stage_schemas=stage_schemas,
        )
        results.append(planned)
        if q.name and planned.stage_schema is not None:
            stage_schemas[q.name] = planned.stage_schema
    return results


# Helpers


def _format_description_for_dimension(
    *, scope: Union[ModelScope, StageSchema], full_name: str,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None, None
    if "." in full_name:
        return None, None
    col = scope.source_model.get_column(full_name)
    if col is None:
        return None, None
    return col.format, col.description


def _format_description_for_measure_formula(
    *, scope: Union[ModelScope, StageSchema], bound,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None, None
    return measure_key_format_description(
        model=scope.source_model, key=bound.value_key,
    )


def _type_for_measure_formula(
    *, scope: Union[ModelScope, StageSchema], bound,
) -> Optional[DataType]:
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    return measure_key_type(model=scope.source_model, key=bound.value_key)


def _joined_column_type(
    *, source_model: SlayerModel, full_name: str, bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    parts = full_name.split(".")
    if parts and parts[0] == source_model.name:  # self-prefix strip
        parts = parts[1:]
    if not parts:
        return None
    *hops, leaf = parts
    current = source_model
    visited = {current.name}
    for hop in hops:
        if not any(j.target_model == hop for j in current.joins):
            return None
        nxt = bundle.get_referenced_model(hop)
        if nxt is None or nxt.name in visited:
            return None
        visited.add(nxt.name)
        current = nxt
    col = current.get_column(leaf)
    return col.type if col is not None else None


def _type_for_dimension(
    *,
    scope: Union[ModelScope, StageSchema],
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    if "." in full_name:
        return _joined_column_type(
            source_model=scope.source_model, full_name=full_name, bundle=bundle,
        )
    col = scope.source_model.get_column(full_name)
    return col.type if col is not None else None


def _opaque_dim_type(
    *,
    scope: Union[ModelScope, StageSchema],
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if isinstance(scope, StageSchema):
        col = scope.get(full_name)
        return col.type if col is not None else None
    return _type_for_dimension(scope=scope, full_name=full_name, bundle=bundle)


def _reject_opaque_grouping_dim(
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> None:
    """Raise if ``full_name`` is an opaque dimension this query will GROUP BY (no equality operator); raw-row mode projects without GROUP BY, so it's legal there."""
    if not (bool(query.measures) or query.distinct_dimension_values):
        return
    dim_type = _opaque_dim_type(scope=scope, full_name=full_name, bundle=bundle)
    if dim_type is not None and dim_type.is_opaque:
        raise ValueError(
            f"Column '{full_name}' cannot be used as a dimension: its type does "
            f"not support the GROUP BY / DISTINCT this query requires. Define a "
            f"derived column that extracts a comparable value instead, e.g. "
            f"sql=\"payload->>'status'\" with type TEXT."
        )


def _terminal_model_for_dotted(
    *, source_model: SlayerModel, hops: List[str], bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    """Walk ``hops`` join targets from ``source_model`` (None on a missing/circular hop), mirroring the binder's join walk."""
    current = source_model
    visited = {current.name}
    for hop in hops:
        if not any(j.target_model == hop for j in current.joins):
            return None
        nxt = bundle.get_referenced_model(hop)
        if nxt is None or nxt.name in visited:
            return None
        visited.add(nxt.name)
        current = nxt
    return current


def _resolve_saved_measure_ref(
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[Tuple[SlayerModel, "ModelMeasure"]]:
    """Return ``(terminal_model, measure)`` if ``formula`` is a bare/dotted saved-measure reference (binder resolution order), else None."""
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    host = scope.source_model
    text = formula.strip()
    if text.isidentifier():
        mm = host.get_measure(text)
        return (host, mm) if mm is not None else None
    parts = text.split(".")
    if len(parts) < 2 or not all(p.isidentifier() for p in parts):
        return None
    if parts[0] == host.name:  # C14 self-prefix strip
        parts = parts[1:]
    if len(parts) == 1:
        mm = host.get_measure(parts[0])
        return (host, mm) if mm is not None else None
    *hops, leaf = parts
    terminal = _terminal_model_for_dotted(
        source_model=host, hops=hops, bundle=bundle,
    )
    if terminal is None:
        return None
    mm = terminal.get_measure(leaf)
    return (terminal, mm) if mm is not None else None


def _saved_model_measure_type(
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[DataType]:
    ref = _resolve_saved_measure_ref(scope=scope, bundle=bundle, formula=formula)
    return ref[1].type if ref is not None else None


def _saved_measure_public_name(
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[str]:
    """Implicit surfaced name for a bare/dotted saved-measure reference — the formula text itself."""
    ref = _resolve_saved_measure_ref(scope=scope, bundle=bundle, formula=formula)
    return formula.strip() if ref is not None else None


def _reject_computed_dim_name_collision(
    *, name: str, query: SlayerQuery, scope: Union[ModelScope, StageSchema],
) -> None:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        model = scope.source_model
        if model.get_column(name) is not None or model.get_measure(name) is not None:
            raise ValueError(
                f"Computed dimension name {name!r} collides with an existing "
                f"column or measure on model {model.name!r}. Choose a different "
                f"name."
            )
    for m in (query.measures or []):
        if m.name == name:
            raise ValueError(
                f"Computed dimension name {name!r} collides with a query measure "
                f"of the same name. Choose a different name."
            )


def _guard_computed_dimension(*, d: ComputedDimension, bound, query: SlayerQuery) -> None:  # NOSONAR(S3776) — sequential fail-closed guard checks over one shared walk (all_keys / transforms / inner_aggs); each arm raises its own contract error, and extracting them scatters the shared state and the ordered narrative.
    """Grain-self-containment rules for a computed dimension: every aggregate must carry ``partition_by=``, fail closed on raw-rows mode; temporal-axis rule runs later."""
    all_keys = list(walk_value_keys(bound.value_key))
    transforms = [k for k in all_keys if isinstance(k, TransformKey)]
    for tk in transforms:
        inner_aggs = [
            k for k in walk_value_keys(tk.input) if isinstance(k, AggregateKey)
        ]
        # A transform is legal in a dimension only over an explicitly-grained aggregate.
        if not inner_aggs or any(a.partition_keys is None for a in inner_aggs):
            raise NotImplementedError(
                f"A transform inside computed dimension {d.name!r} must wrap an "
                f"explicitly-grained aggregate — declare partition_by= on the "
                f"aggregate it transforms (DEV-1824)."
            )
    aggs = [k for k in all_keys if isinstance(k, AggregateKey)]
    if not aggs:
        return  # row-level
    if not query.distinct_dimension_values:
        raise DistinctDimensionValuesError(
            f"Computed dimension {d.name!r} references an aggregate, so it cannot "
            f"be used with distinct_dimension_values=False (raw rows). Remove the "
            f"flag (the default aggregates) or drop the aggregate from the "
            f"dimension."
        )
    for agg in aggs:
        if agg.partition_keys is None:
            raise ValueError(
                f"The aggregate inside computed dimension {d.name!r} must declare "
                f"the grain it aggregates over with partition_by=, e.g. "
                f"'CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END'. "
                f"Without partition_by the group key is a function of the query's "
                f"own dimensions and adds no grouping."
            )
    # A valid partitioned-aggregate dimension is desugared into a producer stage by ``_plan_regroups``.


def _computed_dim_names(query: SlayerQuery) -> FrozenSet[str]:
    return frozenset(
        d.name for d in (query.dimensions or []) if isinstance(d, ComputedDimension)
    )


def _reraise_nested_attach(
    err: UnknownReferenceError, *, computed_dim_names: FrozenSet[str],
) -> None:
    if err.name in computed_dim_names:
        raise NotImplementedError(
            f"An aggregate references the computed dimension {err.name!r} (e.g. "
            f"via partition_by=), which would require a nested attach — not yet "
            f"supported (DEV-1824)."
        ) from err
    raise err


def _declared_computed_dimension(
    d: ComputedDimension,
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> DeclaredMeasure:
    _reject_computed_dim_name_collision(name=d.name, query=query, scope=scope)
    parsed = parse_expr(d.expression)
    try:
        bound = bind_expr(
            parsed=parsed, scope=scope, bundle=bundle, allow_measures=True,
        )
    except UnknownReferenceError as err:
        _reraise_nested_attach(err, computed_dim_names=_computed_dim_names(query))
    _guard_computed_dimension(d=d, bound=bound, query=query)
    dim_type = _type_for_measure_formula(scope=scope, bound=bound)
    return DeclaredMeasure(
        bound=bound,
        declared_name=d.name,
        public_name=d.name,
        type=dim_type,
        is_dimension=True,
    )


def _flatten_collision_message(flat_name: str) -> str:
    return (
        f"Stage column name collision on {flat_name!r}: two projected "
        f"columns flatten to the same downstream name. Give one an "
        f"explicit measure `name` to disambiguate."
    )


def _declared_measures_from_query(  # NOSONAR(S3776) — three sequential projection passes (dimensions incl. computed, time dimensions, measures) building one ordered declared list; each pass is one contract and the order (dims → tds → measures) is the public projection order the function pins.
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> List[DeclaredMeasure]:
    declared: List[DeclaredMeasure] = []
    # Two distinct projected names can flatten to one downstream name; detect it before interning.
    seen_flat: Dict[str, str] = {}

    def _guard_flatten(*, flat_name: str, origin: str) -> None:
        prior = seen_flat.get(flat_name)
        if prior is not None and prior != origin:
            raise ValueError(_flatten_collision_message(flat_name))
        seen_flat[flat_name] = origin

    for d in (query.dimensions or []):
        if isinstance(d, ComputedDimension):
            dm = _declared_computed_dimension(
                d, query=query, scope=scope, bundle=bundle,
            )
            _guard_flatten(flat_name=_flatten_dotted(d.name), origin=d.name)
            declared.append(dm)
            continue
        full = d.full_name
        _reject_opaque_grouping_dim(
            query=query, scope=scope, full_name=full, bundle=bundle,
        )
        bound = bind_expr(
            parsed=parse_expr(full),
            scope=scope,
            bundle=bundle,
        )
        flat_name = _flatten_dotted(full)
        _guard_flatten(flat_name=flat_name, origin=full)
        fmt, desc = _format_description_for_dimension(
            scope=scope, full_name=full,
        )
        dim_type = _type_for_dimension(
            scope=scope, full_name=full, bundle=bundle,
        )
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=d.label,
            type=dim_type,
            format=fmt,
            description=desc,
        ))
    # Time dimensions follow dimensions in the public projection.
    for td in (query.time_dimensions or []):
        full = td.dimension.full_name
        bound = bind_time_dimension(td=td, scope=scope, bundle=bundle)
        flat_name = _flatten_dotted(full)
        _guard_flatten(flat_name=flat_name, origin=full)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=td.label,
            type=DataType.TIMESTAMP,
        ))
    seen_measure_keys: Dict[str, Tuple[str, ValueKey]] = {}
    for m in (query.measures or []):
        formula = m.formula
        explicit_name = m.name
        parsed = parse_expr(formula)
        try:
            bound = bind_expr(
                parsed=parsed, scope=scope, bundle=bundle, allow_measures=True,
            )
        except UnknownReferenceError as err:
            _reraise_nested_attach(
                err, computed_dim_names=_computed_dim_names(query),
            )
        # The parsed tree drives text-shape alias derivation, so both spellings
        # of one formula share an alias (DEV-1826).
        canonical = _canonical_alias_for_formula(
            formula, bound=bound, parsed=parsed,
        )
        # A bare/dotted saved-ModelMeasure reference surfaces under the formula text (explicit query name still wins).
        saved_name = _saved_measure_public_name(
            scope=scope, bundle=bundle, formula=formula,
        )
        alias_name = explicit_name or saved_name
        declared_name = alias_name or canonical
        public_name = alias_name or canonical
        # Two DIFFERENT values whose DERIVED keys collide would silently share
        # a column (e.g. ``sum(amount - cost)`` vs ``sum(amount + cost)`` both
        # sanitize to ``amount_cost_sum``, DEV-1826) — fail loudly. Scoped to
        # unnamed entries: explicit-name collisions keep their dedicated
        # declared-more-than-once errors downstream.
        if alias_name is None:
            prior = seen_measure_keys.get(public_name)
            if prior is not None and prior[1] != bound.value_key:
                raise ValueError(
                    f"Measures {prior[0]!r} and {formula!r} both derive the "
                    f"result key {public_name!r} but compute different "
                    f"values; rename one (set 'name') to disambiguate."
                )
            seen_measure_keys[public_name] = (formula, bound.value_key)
        fmt, desc = _format_description_for_measure_formula(
            scope=scope, bound=bound,
        )
        # Type-priority (highest wins): query m.type, saved ModelMeasure.type,
        # then aggregation-aware inference.
        explicit_type = m.type or _saved_model_measure_type(
            scope=scope, bundle=bundle, formula=formula,
        )
        m_type = explicit_type or _type_for_measure_formula(scope=scope, bound=bound)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=declared_name,
            public_name=public_name,
            label=m.label,
            # Keep the canonical alias when the surfaced name differs, so a colon-form filter / ORDER BY resolves.
            canonical_alias=canonical if alias_name else None,
            type=m_type,
            type_is_explicit=explicit_type is not None,
            format=fmt,
            description=desc,
        ))
    return declared


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
        dep = _source_name_if_sibling(q.source_model, by_name)
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


def _flatten_dotted(name: str) -> str:
    return flat_name(name)


def _canonical_alias_for_formula(
    formula: str,
    *,
    bound: Optional[BinderBoundExpr] = None,
    parsed: Optional[ParsedExpr] = None,
) -> str:
    """Canonical public alias for a measure formula: ``canonical_aggregate_alias``
    for an AggregateKey root, else text-shape recognition sanitised to a valid
    identifier. The text shape runs over the CANONICAL colon-spelling rendering
    of ``parsed`` when given (DEV-1826), so ``cumsum(sum(revenue))`` and
    ``cumsum(revenue:sum)`` derive one alias."""
    if bound is not None and isinstance(bound.value_key, AggregateKey):
        # stage_formula profile prefixes the join path relative to the stage (``customers.*:count`` → ``customers._count``).
        alias = canonical_aggregate_alias(
            bound.value_key, profile="stage_formula",
        )
        if alias is not None:
            return alias
        # None means the source exposes no leaf/column name; use the text-shape path.
    text = (
        canonical_measure_text(parsed) if parsed is not None else formula.strip()
    )
    if ":" in text and "(" not in text:
        base, agg = text.rsplit(":", 1)
        return canonical_agg_name(
            measure_name=base, aggregation_name=agg,
        )
    return (
        text.replace(".", "_").replace(":", "_").replace(" ", "_")
            .replace("(", "_").replace(")", "_").replace(",", "_")
    )


def _source_column_names(
    scope: Union[ModelScope, StageSchema],
) -> FrozenSet[str]:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return frozenset(c.name for c in scope.source_model.columns)
    if isinstance(scope, StageSchema):
        return frozenset(c.name for c in scope.columns)
    return frozenset()


def _host_model_name(
    scope: Union[ModelScope, StageSchema],
) -> str:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return scope.source_model.name
    if isinstance(scope, StageSchema):
        return scope.relation_name
    return "(stage)"


def _composite_reads_an_isolated_cte(
    *,
    key: ValueKey,
    slot_by_key: Dict[ValueKey, SlotId],
    isolated_slot_ids: AbstractSet[SlotId],
) -> bool:
    for dep in walk_value_keys(key):
        # A combined regroup placeholder lives in its producer like a cross-model aggregate → a composite reading one is also outer.
        is_isolated_leaf = isinstance(dep, AggregateKey) or (
            isinstance(dep, ColumnKey) and dep.leaf.startswith(REGROUP_LEAF_PREFIX)
        )
        if is_isolated_leaf and slot_by_key.get(dep) in isolated_slot_ids:
            return True
    return False


def _classify_order_scope(
    *,
    slot: ValueSlot,
    cross_model_slot_ids: Set[SlotId],
    windowed_slot_ids: Set[SlotId],
    public_projection: List[SlotId],
    slot_by_key: Dict[ValueKey, SlotId],
    ranked_slot_ids: AbstractSet[SlotId] = frozenset(),
) -> OrderScope:
    """Name the scope that PRODUCES ``slot``'s value; isolated scopes are checked before the host base (a composite is OUTER_COMPOSITE when any operand lives in an isolated CTE)."""
    if slot.id in cross_model_slot_ids:
        return OrderScope.CROSS_MODEL_CTE
    if slot.id in ranked_slot_ids:
        return OrderScope.RANKED_CTE
    if slot.id in windowed_slot_ids:
        return OrderScope.WINDOWED_CTE
    if isinstance(slot.key, TransformKey):
        return OrderScope.TRANSFORM_STEP
    if isinstance(slot.key, (ArithmeticKey, ScalarCallKey)) and _composite_reads_an_isolated_cte(
        key=slot.key,
        slot_by_key=slot_by_key,
        isolated_slot_ids=(
            cross_model_slot_ids | windowed_slot_ids | set(ranked_slot_ids)
        ),
    ):
        return OrderScope.OUTER_COMPOSITE
    if slot.hidden or slot.id not in public_projection:
        return OrderScope.HOST_BASE_HIDDEN
    return OrderScope.HOST_BASE


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
        flat = _flatten_dotted(alias)
        # Two distinct public columns flattening to one downstream name would make the CTE column ambiguous.
        if any(c.name == flat for c in columns):
            raise ValueError(_flatten_collision_message(flat))
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


def _emit_transform_layers(*, slots: List[ValueSlot]) -> List[TransformLayer]:
    """One TransformLayer per TransformKey slot, in dependency order (innermost first) so an inner window/self-join renders before the outer one consumes it."""
    transform_slots = [
        s for s in slots if isinstance(s.key, TransformKey)
    ]
    # Topological order: a slot whose TransformKey.input references another slot's key must come after it.
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
    by_id = {s.id: s for s in transform_slots}
    return [
        TransformLayer(op=by_id[sid].key.op, slot_ids=[sid])
        for sid in ordered_ids
    ]


# date_range → filter + main-TD disambiguation


def _validate_model_filter(
    *,
    mf: str,
    idx: int,
    model: SlayerModel,
) -> FilterPhase:
    """Validate a ``SlayerModel.filters`` entry and emit a text-only FilterPhase (rejects same-model ModelMeasure and window-function column refs)."""
    parsed = parse_sql_predicate(mf)
    measure_names = {m.name for m in (model.measures or [])}
    windowed_columns = {
        c.name for c in model.columns
        if c.sql and has_window_function(c.sql)
    }
    for col in parsed.columns:
        if col in measure_names:
            raise ValueError(
                f"Model filter {mf!r} references measure {col!r}. "
                f"Model filters can only reference table columns (WHERE). "
                f"Use query-level filters for measure conditions."
            )
        if col in windowed_columns:
            raise ValueError(
                f"Model filter {mf!r} references column {col!r} whose "
                f"SQL contains a window function. Factor it into a "
                f"multi-stage source_queries model or use a rank-family "
                f"transform at query time."
            )
    return FilterPhase(
        id=f"mf{idx}",
        phase=Phase.ROW,
        text=mf,
        expression=None,
    )


def _build_date_range_filter(
    *,
    td: TimeDimension,
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
) -> BoundFilter:
    """Build a row-phase ``BoundFilter`` from a TimeDimension's ``date_range`` as an inclusive ``BetweenKey``, bound against the bare underlying column (not the TimeTruncKey)."""
    full = td.dimension.full_name
    parsed = parse_expr(full)
    bound_col_expr = bind_expr(parsed=parsed, scope=scope, bundle=bundle)
    col_key = bound_col_expr.value_key
    # A derived (Column.sql) temporal column binds to a ColumnSqlKey; BetweenKey accepts both kinds.
    if not isinstance(col_key, (ColumnKey, ColumnSqlKey)):
        raise ValueError(
            f"date_range filter for TimeDimension {full!r} expected a "
            f"column reference; got {type(col_key).__name__}."
        )

    start, end = td.date_range[0], td.date_range[1]
    predicate = BetweenKey(
        column=col_key,
        low=LiteralKey(value=normalize_scalar(start)),
        high=LiteralKey(value=normalize_scalar(end)),
    )
    refs = tuple(walk_value_keys(predicate))
    phase = max((k.phase for k in refs), default=predicate.phase)
    return BoundFilter(
        value_key=predicate, phase=phase, referenced_keys=refs,
    )


def _resolve_main_time_dimension(
    *,
    query: SlayerQuery,
    model: SlayerModel,
) -> Optional[TimeDimension]:
    """Resolve the active time dimension for transform/windowing: 0 TDs → None; 1 → that TD; 2+ → main_time_dimension (full_name then leaf) else default_time_dimension else None."""
    tds = list(query.time_dimensions or [])
    if not tds:
        return None
    if len(tds) == 1:
        return tds[0]

    if query.main_time_dimension:
        target = query.main_time_dimension
        # Prefer full-name (more specific) over leaf match.
        for td in tds:
            if td.dimension.full_name == target:
                return td
        leaf_matches = [td for td in tds if td.dimension.name == target]
        if len(leaf_matches) == 1:
            return leaf_matches[0]
        if len(leaf_matches) > 1:
            # Multiple TDs share the leaf; force disambiguation via full_name.
            raise AmbiguousReferenceError(
                name=target,
                candidates=[td.dimension.full_name for td in leaf_matches],
            )
        raise UnknownReferenceError(
            name=target,
            scope_kind="TimeDimension",
            scope_summary=(
                f"time_dimensions: "
                f"{[td.dimension.full_name for td in tds]}"
            ),
            suggestion=None,
        )

    default = model.default_time_dimension
    if default:
        # The default points only at the host model; prefer a host-local TD over a same-leaf joined one.
        for td in tds:
            if td.dimension.model is None and td.dimension.name == default:
                return td
    return None
