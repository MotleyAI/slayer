"""The query-level bind pass: every text surface of a ``SlayerQuery`` parsed,
bound and normalized into a ``PreboundQuery`` (the elaboration sub-phase that
turns syntax into keys; expression-level binding lives in ``binding``)."""

from __future__ import annotations

from typing import Callable, Dict, FrozenSet, List, Optional, Tuple, Union

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousJoinPathError, AmbiguousReferenceError, UnknownReferenceError
from slayer.core.format import NumberFormat
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.join_walker import resolve_hop, terminal_model
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    Grain,
    InKey,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    lower_collapsing_constituents,
    lower_sugar_transforms,
    normalize_scalar,
    normalize_transform_constituents,
    attached_operand_keys,
    rewrite_rank_partition_keys,
    walk_value_keys,
    window_kwarg_of,
)
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.core.query import (
    ComputedDimension,
    ORDER_PLACEHOLDER_NAMES,
    SlayerQuery,
    TimeDimension,
)
from slayer.core.refs import (
    AGG_REF_RE,
    auto_name_from_expression,
    canonical_agg_name,
)
from slayer.core.scope import ModelScope, StageSchema, host_model_name
from slayer.engine import dimension_routing
from slayer.engine.binding import bind_expr, bind_filter, bind_time_dimension
from slayer.engine.elaborate_env import (
    check_collapsing_transform_not_row_mixed,
    check_computed_dim_name_collision,
    check_computed_dimension,
    check_measure_dedupe_collision,
    check_stage_flatten_collision,
    check_dimension_temporal_axis,
    check_opaque_grouping_dim,
    check_non_shift_transform_row_leaf,
    check_partition_key_resolves,
    check_raw_rows_filter_measure_ref,
    check_raw_rows_order_measure_ref,
    check_time_dimension_date_range,
    check_time_shift_input,
    check_time_transforms_resolved,
)
from slayer.engine.join_safety import assert_partition_key_attributable
from slayer.engine.key_metadata import (
    measure_key_format_description,
    measure_key_preserves_native_type,
    measure_key_type,
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
from slayer.ir.bound import (
    BoundExpr,
    BoundFilter,
    DeclaredMeasure,
    OrderSpec,
    combined_consumer_aggregates,
    dimension_partitioned_aggregates,
)
from slayer.ir.prebound import PreboundQuery, partition_declared_measures
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope
from slayer.sql.naming import canonical_aggregate_alias, flat_name as _flatten_dotted

__all__ = [
    "bind_query_inputs",
]




# Transform ops needing a resolvable time dimension for their OVER ORDER BY.
_TIME_NEEDING_TRANSFORM_OPS = TIME_TRANSFORMS


def _attach_time_to_transform(key: TransformKey, *, td_key: TimeTruncKey) -> ValueKey:
    new_input = _attach_time_keys(key.input, td_key=td_key)
    out = key
    if new_input is not key.input:
        out = out.model_copy(update={"input": new_input})
    if out.op in _TIME_NEEDING_TRANSFORM_OPS and out.time_key is None:
        out = out.model_copy(update={"time_key": td_key})
    return out


def _attach_time_to_arithmetic(key: ArithmeticKey, *, td_key: TimeTruncKey) -> ValueKey:
    new_ops = tuple(
        _attach_time_keys(o, td_key=td_key) for o in key.operands
    )
    if all(a is b for a, b in zip(new_ops, key.operands)):
        return key
    return ArithmeticKey(op=key.op, operands=new_ops)


def _attach_time_to_scalar_call(key: ScalarCallKey, *, td_key: TimeTruncKey) -> ValueKey:
    new_args = tuple(
        _attach_time_keys(a, td_key=td_key)
        if isinstance(
            a, (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey, InKey),
        )
        else a
        for a in key.args
    )
    if all(a is b for a, b in zip(new_args, key.args)):
        return key
    return ScalarCallKey(name=key.name, args=new_args)


def _attach_time_to_between(key: BetweenKey, *, td_key: TimeTruncKey) -> ValueKey:
    nc = _attach_time_keys(key.column, td_key=td_key)
    nl = _attach_time_keys(key.low, td_key=td_key)
    nh = _attach_time_keys(key.high, td_key=td_key)
    if nc is key.column and nl is key.low and nh is key.high:
        return key
    return BetweenKey(column=nc, low=nl, high=nh)


def _attach_time_to_in(key: InKey, *, td_key: TimeTruncKey) -> ValueKey:
    # Only the LHS column can carry a transform; values are literals.
    nc = _attach_time_keys(key.column, td_key=td_key)
    if nc is key.column:
        return key
    return InKey(column=nc, values=key.values, negated=key.negated)


def _attach_time_to_aggregate(key: AggregateKey, *, td_key: TimeTruncKey) -> ValueKey:
    # An aggregated transform constituent gets the query's bucket too — descend
    # source, args and kwargs (identity-preserving, like ``map_children``).
    return key.map_children(lambda c: _attach_time_keys(c, td_key=td_key))


def _attach_time_keys(
    key: ValueKey, *, td_key: TimeTruncKey,
) -> ValueKey:
    """Set ``time_key=td_key`` on every time-needing TransformKey with a null one (identity-preserving)."""
    if isinstance(key, AggregateKey):
        return _attach_time_to_aggregate(key, td_key=td_key)
    if isinstance(key, TransformKey):
        return _attach_time_to_transform(key, td_key=td_key)
    if isinstance(key, ArithmeticKey):
        return _attach_time_to_arithmetic(key, td_key=td_key)
    if isinstance(key, ScalarCallKey):
        return _attach_time_to_scalar_call(key, td_key=td_key)
    if isinstance(key, BetweenKey):
        return _attach_time_to_between(key, td_key=td_key)
    if isinstance(key, InKey):
        return _attach_time_to_in(key, td_key=td_key)
    return key


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
        check_raw_rows_filter_measure_ref(
            offending=f if _expr_has_measure_ref(
                parsed, measure_names=measure_names, scope=scope, bundle=bundle,
            ) else None,
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
            check_raw_rows_order_measure_ref(
                contains=raw if parsed is not None and _expr_has_measure_ref(
                    parsed, measure_names=measure_names, scope=scope, bundle=bundle,
                ) else None,
            )
        name = getattr(getattr(item, "column", None), "name", None)
        check_raw_rows_order_measure_ref(
            saved_name=name if name and name in measure_names else None,
            source_name=source_name,
        )
        # A dotted ORDER BY column whose leaf is a saved measure on the terminal model is a measure ref too.
        full = getattr(getattr(item, "column", None), "full_name", None)
        check_raw_rows_order_measure_ref(
            saved_dotted=full if full and "." in full and _resolve_saved_measure_ref(
                scope=scope, bundle=bundle, formula=full,
            ) is not None else None,
        )


def _map_bound_keys(
    key_fn: Callable[[ValueKey], ValueKey],
    *,
    declared_measures: List[DeclaredMeasure],
    bound_filters: List[BoundFilter],
    order_specs: List[OrderSpec],
    skip_dimensions: bool = False,
) -> Tuple[List[DeclaredMeasure], List[BoundFilter], List[OrderSpec]]:
    new_measures = [
        DeclaredMeasure(
            bound=BoundExpr(
                value_key=(
                    dm.bound.value_key if (skip_dimensions and dm.is_dimension)
                    else key_fn(dm.bound.value_key)
                ),
                routed_dotted=dm.bound.routed_dotted,
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
            bound=BoundExpr(
                value_key=key_fn(spec.bound.value_key),
                routed_dotted=spec.bound.routed_dotted,
            ),
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
        scope = resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )

    # Runs BEFORE binding so the targeted error wins over the binder's generic one.
    if query.distinct_dimension_values is False:
        _reject_measure_refs_for_raw_rows(query=query, scope=scope, bundle=bundle)

    declared_measures = _declared_measures_from_query(
        query=query, scope=scope, bundle=bundle,
    )

    # Alias lookup for ORDER BY, checked before bind_expr so aggregate aliases resolve via the registry.
    declared_alias_to_bound: Dict[str, BoundExpr] = {}
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
    # ...and resolvable inside a filter/order ``partition_by=`` (DEV-1847 shape B).
    _computed_names = frozenset(
        d.name for d in (query.dimensions or []) if isinstance(d, ComputedDimension)
    )
    dim_alias_map: Dict[str, ValueKey] = {
        dm.public_name: dm.bound.value_key
        for dm in declared_measures
        if dm.is_dimension
        and dm.public_name is not None
        and dm.public_name in _computed_names
    }

    # Filter list in WHERE order: date_range, model filters (Mode-A SQL), then user query filters.
    bound_filters: List[BoundFilter] = []
    # Parallel original filter text (None for date_range bounds), for dropped-filter warnings.
    bound_filter_texts: List[Optional[str]] = []

    # 1. date_range filters (one per TD with a 2-element date_range)
    for td in (query.time_dimensions or []):
        if not td.date_range or len(td.date_range) != 2:
            continue
        # Checked before the scope skip so non-ModelScope stages raise too.
        check_time_dimension_date_range(
            full_name=td.dimension.full_name, date_range=td.date_range,
        )
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
            dimension_alias_map=dim_alias_map,
        )
        if any(existing.value_key == bf.value_key for existing in bound_filters):
            continue
        bound_filters.append(bf)
        bound_filter_texts.append(f)

    order_specs = []
    # Host identity for the qualifier check below (StageSchema uses its relation name).
    _order_host_name = host_model_name(scope)
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
                    dimension_alias_map=dim_alias_map,
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
                dimension_alias_map=dim_alias_map,
            )
            if any(
                isinstance(k, AggregateKey) and (
                    k.partition_keys is not None or window_kwarg_of(k) is not None
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
                dimension_alias_map=dim_alias_map,
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
    check_time_transforms_resolved(roots=[
        *(dm.bound.value_key for dm in declared_measures),
        *(bf.value_key for bf in bound_filters),
        *(spec.bound.value_key for spec in order_specs),
    ])

    # time_shift-family input typing runs pre-lowering, where change/change_pct
    # are still single nodes (their desugar duplicates the offending input).
    _roots_for_transform_checks = [
        *(dm.bound.value_key for dm in declared_measures),
        *(bf.value_key for bf in bound_filters),
        *(spec.bound.value_key for spec in order_specs),
    ]
    check_time_shift_input(roots=_roots_for_transform_checks)

    # A non-shift transform over a row-level leaf that refines the query grain
    # inflates the base GROUP BY; reject unless the leaf is a projected grain key.
    _proj_dim_dms, _proj_td_dms, _ = partition_declared_measures(
        declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    check_non_shift_transform_row_leaf(
        roots=_roots_for_transform_checks,
        projected_grain_keys=frozenset(
            dm.bound.value_key for dm in (*_proj_dim_dms, *_proj_td_dms)
        ),
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

    # Normalise transform constituents (D4b, Axiom 11.1): an ungrained, non-windowed,
    # local inner aggregate inside a transform constituent (measure/filter/order, not
    # dimensions) is explicitly grained at the query grain — BEFORE partition-key
    # validation, so the synthesized keys face the same attributability / resolution
    # checks as a user-written partition_by=. Every dependent set below (dim-agg,
    # combined-consumer, reagg-operand) is computed AFTER, over the normalised keys.
    _query_grain = Grain.of([*_dim_key_set, *_td_key_set])
    declared_measures, bound_filters, order_specs = _map_bound_keys(
        lambda vk: normalize_transform_constituents(vk, query_grain=_query_grain),
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
        skip_dimensions=True,
    )

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
    # An attached operand — a re-aggregation constituent (DEV-1847) or a
    # row-attached input / parameter (DEV-1859) — declares an internal producer
    # grain, so its partition keys need not be query dimensions; the enclosing
    # root is the combined consumer and carries the rule.
    _reagg_operand_keys = attached_operand_keys([
        *[dm.bound.value_key for dm in declared_measures],
        *[bf.value_key for bf in bound_filters],
        *[sp.bound.value_key for sp in order_specs],
    ])

    def _validate_partition_keys(key: Union[AggregateKey, TransformKey]) -> Grain:
        label = (
            f"Transform '{key.op}'" if isinstance(key, TransformKey)
            else f"Aggregation '{key.agg}'"
        )
        lenient = (
            key in _dim_agg_keys and key not in _combined_consumer_keys
        ) or key in _reagg_operand_keys
        new_pks = []
        for pk in key.partition_keys or ():
            # A partition key over a join must be attributable from the root; else a hard error.
            assert_partition_key_attributable(
                key=key, pk=pk, label=label, scope=scope, bundle=bundle,
            )
            is_query_dim = pk in _dim_key_set or pk in _td_key_set
            bucket = _td_by_source.get(pk)
            check_partition_key_resolves(
                label=label, pk=pk, is_query_dim=is_query_dim,
                ambiguous=pk in _td_ambiguous_sources,
                maps_to_bucket=bucket is not None, lenient=lenient,
                available_dims=_available_dims,
            )
            # td source col -> bucket; else the key itself (query dim / td bucket, or lenient finer-grain producer key, DEV-1825)
            new_pks.append(bucket if not is_query_dim and bucket is not None else pk)
        return Grain.of(new_pks)

    def _rw(vk: ValueKey) -> ValueKey:
        return rewrite_rank_partition_keys(vk, rewrite_fn=_validate_partition_keys)

    declared_measures, bound_filters, order_specs = _map_bound_keys(
        _rw,
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    check_dimension_temporal_axis(declared_measures)

    # A collapsing transform mixed with a row-level column would collapse to a
    # re-aggregation the row-attach path cannot yet broadcast (D4c deferral); fail
    # closed before the desugar produces that shape.
    check_collapsing_transform_not_row_mixed(roots=[
        *(dm.bound.value_key for dm in declared_measures),
        *(bf.value_key for bf in bound_filters),
        *(sp.bound.value_key for sp in order_specs),
    ])

    # Lower a collapsing transform constituent (first/last, D4c) to an exact
    # per-partition pick AFTER the axis check (which sees the raw transform); the
    # synthesized max's partition_by is a carrier grain key, validated inside the
    # carrier sub-plan, so it runs after _rw. All positions.
    declared_measures, bound_filters, order_specs = _map_bound_keys(
        lower_collapsing_constituents,
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

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
        to_many_handling=query.to_many_handling,
    )


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
    current = terminal_model(
        root=source_model, path=tuple(hops),
        models_by_name=bundle.models_by_name,
    )
    if current is None:
        return None
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


def _terminal_model_for_dotted(
    *, source_model: SlayerModel, hops: List[str], bundle: ResolvedSourceBundle,
) -> Optional[SlayerModel]:
    """Walk ``hops`` from ``source_model`` via the shared walker (None on a
    missing/circular/ambiguous hop), mirroring the binder's join walk."""
    return terminal_model(
        root=source_model, path=tuple(hops),
        models_by_name=bundle.models_by_name,
    )


def _route_short_form_saved_measure(
    *, host: SlayerModel, hops: list[str], leaf: str, bundle: ResolvedSourceBundle
) -> Optional[Tuple[SlayerModel, str]]:
    """``(terminal_model, canonical_ref)`` when a ``len==1`` unresolvable prefix
    short-form routes to its full datasource-scoped path (DEV-1856), else None.
    Routing triggers only when the first hop resolves to no edge; an adjacent
    parallel pair is a fail-closed ambiguous hop (DEV-1853), not a route. This
    resolver also runs in pre-bind raw-rows validation, so it must not route an
    ambiguous hop there — it returns None and lets binding raise the ambiguity."""
    if len(hops) != 1:
        return None
    models_by_name = bundle.models_by_name
    models_by_name.setdefault(host.name, host)
    try:
        if resolve_hop(
            current=host, token=hops[0], models_by_name=models_by_name,
        ) is not None:
            return None
    except AmbiguousJoinPathError:
        return None
    route = dimension_routing.short_form_route_or_none(
        root=host, target_model=hops[0], models_by_name=models_by_name,
    )
    if route is None:
        return None
    terminal = models_by_name.get(hops[0])
    return (terminal, ".".join([*route, leaf])) if terminal is not None else None


def _resolve_saved_measure_ref(
    *,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[Tuple[SlayerModel, "ModelMeasure", str]]:
    """Return ``(terminal_model, measure, canonical_ref)`` if ``formula`` is a
    bare/dotted saved-measure reference (binder resolution order, short-form
    auto-routing included), else None. ``canonical_ref`` is the full routed
    dotted text a short form resolves to, else the formula text unchanged."""
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    host = scope.source_model
    text = formula.strip()
    if text.isidentifier():
        mm = host.get_measure(text)
        return (host, mm, text) if mm is not None else None
    parts = text.split(".")
    if len(parts) < 2 or not all(p.isidentifier() for p in parts):
        return None
    if parts[0] == host.name:  # C14 self-prefix strip
        parts = parts[1:]
    if len(parts) == 1:
        mm = host.get_measure(parts[0])
        return (host, mm, text) if mm is not None else None
    *hops, leaf = parts
    terminal = _terminal_model_for_dotted(
        source_model=host, hops=hops, bundle=bundle,
    )
    canonical_ref = text
    if terminal is None:
        routed = _route_short_form_saved_measure(
            host=host, hops=hops, leaf=leaf, bundle=bundle,
        )
        if routed is None:
            return None
        terminal, canonical_ref = routed
    mm = terminal.get_measure(leaf)
    return (terminal, mm, canonical_ref) if mm is not None else None


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
    """Implicit surfaced name for a bare/dotted saved-measure reference — the
    full routed dotted text (short forms surface under their routed path)."""
    ref = _resolve_saved_measure_ref(scope=scope, bundle=bundle, formula=formula)
    return ref[2] if ref is not None else None


def _reject_computed_dim_name_collision(
    *, name: str, query: SlayerQuery, scope: Union[ModelScope, StageSchema],
) -> None:
    model_collision = None
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        model = scope.source_model
        if model.get_column(name) is not None or model.get_measure(name) is not None:
            model_collision = model.name
    check_computed_dim_name_collision(
        name=name,
        model_name=model_collision,
        query_measure_collision=any(
            m.name == name for m in (query.measures or [])
        ),
    )


def _declared_computed_dimension(
    d: ComputedDimension,
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, ValueKey]] = None,
) -> DeclaredMeasure:
    name = d.name
    assert name is not None  # _fill_name auto-names from the expression
    _reject_computed_dim_name_collision(name=name, query=query, scope=scope)
    parsed = parse_expr(d.expression)
    bound = bind_expr(
        parsed=parsed, scope=scope, bundle=bundle, allow_measures=True,
        dimension_alias_map=dim_alias_map,
    )
    # Grain rules live in the checker (DEV-1871 G9); invoked here to preserve the bind-time firing point / precedence.
    check_computed_dimension(
        name=name, bound=bound,
        distinct_dimension_values=query.distinct_dimension_values,
    )
    dim_type = _type_for_measure_formula(scope=scope, bound=bound)
    return DeclaredMeasure(
        bound=bound,
        declared_name=name,
        public_name=name,
        type=dim_type,
        is_dimension=True,
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
        check_stage_flatten_collision(
            flat_name=flat_name,
            collides=prior is not None and prior != origin,
        )
        seen_flat[flat_name] = origin

    # Computed-dimension names resolve inside later ``partition_by=`` values
    # (DEV-1847 shape B); built in declaration order.
    dim_alias_map: Dict[str, ValueKey] = {}
    for d in (query.dimensions or []):
        if isinstance(d, ComputedDimension):
            dm = _declared_computed_dimension(
                d, query=query, scope=scope, bundle=bundle,
                dim_alias_map=dim_alias_map,
            )
            _guard_flatten(
                flat_name=_flatten_dotted(dm.declared_name),
                origin=dm.declared_name,
            )
            declared.append(dm)
            dim_alias_map[dm.declared_name] = dm.bound.value_key
            continue
        full = d.full_name
        # Bind first: a short-form dotted dim auto-routes, and its full routed
        # path (``bound.routed_dotted``) — not the short form typed — drives the
        # result key, type, opaque guard, and description (DEV-1856).
        bound = bind_expr(
            parsed=parse_expr(full),
            scope=scope,
            bundle=bundle,
        )
        canonical = bound.routed_dotted or full
        # Opaque-grouping rule lives in the checker (DEV-1871 G9); invoked here to preserve the per-dimension firing point.
        check_opaque_grouping_dim(
            full_name=canonical,
            dim_type=_opaque_dim_type(scope=scope, full_name=canonical, bundle=bundle),
            will_group_by=bool(query.measures) or query.distinct_dimension_values,
        )
        flat_name = _flatten_dotted(canonical)
        _guard_flatten(flat_name=flat_name, origin=canonical)
        fmt, desc = _format_description_for_dimension(
            scope=scope, full_name=canonical,
        )
        dim_type = _type_for_dimension(
            scope=scope, full_name=canonical, bundle=bundle,
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
        canonical = bound.routed_dotted or full
        flat_name = _flatten_dotted(canonical)
        _guard_flatten(flat_name=flat_name, origin=canonical)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=td.label,
            type=DataType.TIMESTAMP,
        ))
    seen_measure_keys: Dict[str, Tuple[str, ValueKey, ModelMeasure]] = {}
    for m in (query.measures or []):
        formula = m.formula
        explicit_name = m.name
        parsed = parse_expr(formula)
        bound = bind_expr(
            parsed=parsed, scope=scope, bundle=bundle, allow_measures=True,
            dimension_alias_map=dim_alias_map,
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
        # sanitize to ``amount_cost_sum``, DEV-1826) — fail loudly; the SAME
        # value merges into one column. Scoped to unnamed entries:
        # explicit-name collisions keep their dedicated declared-more-than-once
        # errors downstream.
        if alias_name is None:
            prior = seen_measure_keys.get(public_name)
            if prior is not None:
                check_measure_dedupe_collision(
                    prior_formula=prior[0], formula=formula,
                    public_name=public_name,
                    same_key=prior[1] == bound.value_key,
                    same_meta=(m.label, m.type) == (prior[2].label, prior[2].type),
                )
                continue
            seen_measure_keys[public_name] = (formula, bound.value_key, m)
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
            preserve_native_type=(
                explicit_type is None
                and isinstance(scope, ModelScope)
                and scope.source_model is not None
                and measure_key_preserves_native_type(
                    model=scope.source_model, key=bound.value_key,
                )
            ),
            format=fmt,
            description=desc,
        ))
    return declared


def _canonical_alias_for_formula(
    formula: str,
    *,
    bound: Optional[BoundExpr] = None,
    parsed: Optional[ParsedExpr] = None,
) -> str:
    """Canonical public alias for a measure formula: ``canonical_aggregate_alias``
    for an AggregateKey root, ``canonical_agg_name`` for a plain ``col:agg``
    text shape, else the text sanitised via ``auto_name_from_expression``. The
    text shape runs over the CANONICAL colon-spelling rendering of ``parsed``
    when given (DEV-1826), so ``cumsum(sum(revenue))`` and
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
    # Fullmatch only — a substring heuristic here once mis-captured arithmetic
    # composites and leaked ``:``/``/`` into SQL aliases.
    match = AGG_REF_RE.fullmatch(text)
    if match is not None and match.group(3) is None:
        base, agg = match.group(1), match.group(2)
        if base.endswith(".*"):
            prefix, star = base[:-2], "*"
            return f"{prefix}.{canonical_agg_name(measure_name=star, aggregation_name=agg)}"
        return canonical_agg_name(measure_name=base, aggregation_name=agg)
    return auto_name_from_expression(text)


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
    assert isinstance(col_key, (ColumnKey, ColumnSqlKey)), (
        f"date_range filter for TimeDimension {full!r} expected a "
        f"column reference; got {type(col_key).__name__}."
    )

    date_range = td.date_range
    assert date_range is not None  # caller builds this only for 2-element date_ranges
    start, end = date_range[0], date_range[1]
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


def _named_td_matches(
    *, tds: List[TimeDimension], target: str,
) -> Tuple[Optional[TimeDimension], List[TimeDimension]]:
    """(full-name match, leaf matches) for ``target`` among ``tds``."""
    for td in tds:
        if td.dimension.full_name == target:
            return td, []
    return None, [td for td in tds if td.dimension.name == target]


def _host_local_default_td(
    *, tds: List[TimeDimension], default: str,
) -> Optional[TimeDimension]:
    # The default points only at the host model; prefer a host-local TD over a same-leaf joined one.
    for td in tds:
        if td.dimension.model is None and td.dimension.name == default:
            return td
    return None


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
        full_match, leaf_matches = _named_td_matches(tds=tds, target=target)
        if full_match is not None:
            return full_match
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
        return _host_local_default_td(tds=tds, default=default)
    return None
