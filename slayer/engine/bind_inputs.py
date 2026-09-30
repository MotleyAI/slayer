"""The query-level bind pass: every text surface of a ``SlayerQuery`` parsed,
bound and normalized into a ``PreboundQuery`` (the elaboration sub-phase that
turns syntax into keys; expression-level binding lives in ``binding``)."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from typing import Callable, Dict, FrozenSet, List, Optional, Tuple, cast

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType, TimeGranularity, normalize_aggregation_name
from slayer.core.errors import (
    AmbiguousJoinPathError,
    AmbiguousReferenceError,
    CircularJoinPathError,
    GranularityCallError,
    UnknownReferenceError,
)
from slayer.core.format import NumberFormat
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.time_spine import is_spine, is_spine_query, names_spine
from slayer.core.join_walker import canonical_path, resolve_hop, terminal_model, walk
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    Grain,
    LiteralKey,
    Phase,
    TimePointCmpKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    lower_collapsing_constituents,
    lower_sugar_transforms,
    normalize_transform_constituents,
    attached_operand_keys,
    is_reaggregation_key,
    rewrite_rank_partition_keys,
    walk_value_keys,
    window_kwarg_of,
)
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.core.granularity import Granularity, granularity_key, nests_into, resolve_granularity
from slayer.core.query import (
    ComputedDimension,
    ORDER_PLACEHOLDER_NAMES,
    SlayerQuery,
    TimeDimension,
    call_callee,
    functional_call_column,
    granularity_call_parts,
)
from slayer.core.refs import (
    dotted_key_display,
    AGG_REF_RE,
    auto_name_from_expression,
    canonical_agg_name,
)
from slayer.core.time_bounds import is_frame_bound
from slayer.core.time_points import floor_to
from slayer.core.scope import (
    ModelScope,
    StageSchema,
    host_model_name,
    stale_spelling_position,
)
from slayer.engine import dimension_routing
from slayer.engine.binding import bind_expr, bind_filter, bind_time_dimension, spelled_aggregate_key
from slayer.engine.elaborate_env import (
    check_computed_dim_name_collision,
    check_computed_dimension,
    check_date_operands,
    check_dimension_call_known,
    check_granularity_call_shape,
    check_granularity_known,
    check_measure_dedupe_collision,
    check_spine_clash,
    check_spine_plain_use,
    check_stage_flatten_collision,
    check_dimension_temporal_axis,
    check_opaque_grouping_dim,
    check_partition_key_resolves,
    check_raw_rows_filter_measure_ref,
    check_raw_rows_order_measure_ref,
    check_time_dimension_column,
    check_transform_inputs,
    check_transform_partition_keys_in_operand_grain,
    check_time_transforms_resolved,
    resolve_time_points,
)
from slayer.engine.join_safety import assert_partition_key_attributable
from slayer.engine.key_metadata import (
    measure_key_format_description,
    measure_key_preserves_native_type,
    measure_key_type,
    scope_column_type,
    stage_measure_type,
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
    GranularityConflict,
    BoundExpr,
    BoundFilter,
    DeclaredMeasure,
    OrderSpec,
)
from slayer.ir.prebound import PreboundQuery, partition_declared_measures
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope
from slayer.sql.naming import canonical_aggregate_alias, flat_name as _flatten_dotted

__all__ = [
    "bind_query_inputs",
]




# Transform ops needing a resolvable time dimension for their OVER ORDER BY.
_TIME_NEEDING_TRANSFORM_OPS = TIME_TRANSFORMS


def _attach_time_keys(key: ValueKey, *, td_key: TimeTruncKey) -> ValueKey:
    """Set ``time_key=td_key`` on every time-needing TransformKey with a null one (identity-preserving)."""
    rebuilt = cast(ValueKey, key.map_children(lambda c: _attach_time_keys(c, td_key=td_key)))
    if (
        isinstance(rebuilt, TransformKey)
        and rebuilt.op in _TIME_NEEDING_TRANSFORM_OPS
        and rebuilt.time_key is None
    ):
        return rebuilt.model_copy(update={"time_key": td_key})
    return rebuilt


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
        dm.model_copy(update={"bound": BoundExpr(
            value_key=(
                dm.bound.value_key if (skip_dimensions and dm.is_dimension)
                else key_fn(dm.bound.value_key)
            ),
            routed_dotted=dm.bound.routed_dotted,
        )})
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
    scope: Optional[ModelScope | StageSchema] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
) -> PreboundQuery:
    """Parse and bind every text surface of a ``SlayerQuery`` (the only door into the parser); returns fully-normalized keys. Model filters excluded (scope-owned)."""
    if scope is None:
        scope = resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )
    check_spine_clash(clash=bundle.spine_clash and is_spine_query(query))
    _check_column_granularities(bundle=bundle)
    query = _resolve_granularity_calls(query, bundle=bundle)

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
    # ...and resolvable inside a filter/order ``partition_by=``.
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
    # Parallel original filter text (None for date_range bounds), for semi-join push entries.
    bound_filter_texts: List[Optional[str]] = []

    # 1. date_range filters (one per TD with a date_range)
    for i, td in enumerate(query.time_dimensions or []):
        with stale_spelling_position(f"time_dimensions[{i}]"):
            if not td.date_range:
                continue
            # A stage date_range filters the stage's rows on its bare column, exactly
            # as a model-scope range filters a model's rows.
            bf = _build_date_range_filter(td=td, scope=scope, bundle=bundle)
            bound_filters.append(bf)
            bound_filter_texts.append(None)
    n_date_range = len(bound_filters)

    # 2. SlayerModel.filters — lifted from scope in plan_query, not here.

    # 3. user query filters (Mode-B DSL). Dedupe by bound key (first wins) so the
    #    alias and dotted/colon forms of a ref don't duplicate the HAVING clause.
    for i, f in enumerate(query.filters or []):
        with stale_spelling_position(f"filters[{i}]"):
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
    # Host spelling for the qualifier check below.
    _order_host_name = host_model_name(scope)
    for i, o in enumerate(query.order or []):
        with stale_spelling_position(f"order[{i}]"):
            col_name = o.column.name
            full_name = o.column.full_name
            # A functional ``gran(col)`` order key sorts by the projected time
            # dimension's bucket — resolved to its column binding.
            gran_parts = granularity_call_parts(o.raw_formula) if o.raw_formula else None
            _order_gran = (
                resolve_granularity(gran_parts[1], defined=bundle.granularities)
                if gran_parts is not None else None
            )
            if gran_parts is not None and _order_gran is not None:
                _col, _gran = gran_parts
                matching_td = next(
                    (
                        td for td in (query.time_dimensions or [])
                        if td.dimension.full_name == _col
                        and resolve_granularity(td.granularity, defined=bundle.granularities) == _order_gran
                    ),
                    None,
                )
                if matching_td is None:
                    check_spine_plain_use(
                        offender=o.raw_formula if names_spine(_col) or (
                            isinstance(scope, ModelScope) and scope.source_model is not None
                            and is_spine(scope.source_model) and "." not in _col
                        ) else None,
                        position="order key",
                    )
                    raise GranularityCallError(
                        f"Order key {_gran}({_col}) has no matching projected time "
                        f"dimension. Project a time_dimension on {_col!r} at {_gran} "
                        f"granularity (e.g. {_gran}({_col}) in dimensions) to order by "
                        f"its bucket."
                    )
                order_specs.append(OrderSpec(
                    bound=bind_time_dimension(
                        td=matching_td, scope=scope, bundle=bundle,
                        granularity=_td_granularity(matching_td, bundle=bundle),
                    ).bound,
                    direction=o.direction,
                ))
                continue
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
                # ``count(*)`` surfaces as ``_count``; users order by the bare ``count``.
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

    column_type = scope_column_type(scope=scope, bundle=bundle)
    check_date_operands(
        roots=[
            *(dm.bound.value_key for dm in declared_measures),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        column_type=column_type,
    )
    # Every time point resolves against the execution's one clock reading.
    declared_measures, bound_filters, order_specs = _map_bound_keys(
        lambda vk: resolve_time_points(vk, column_type=column_type, now=bundle.now, units=bundle.granularities),
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )
    granularity_conflicts: List[GranularityConflict] = []
    if query.whole_periods_only:
        bound_filters, bound_filter_texts, n_date_range, granularity_conflicts = _snap_whole_periods(
            bound_filters=bound_filters, bound_filter_texts=bound_filter_texts,
            n_date_range=n_date_range, now=bundle.now,
            td_keys=[
                dm.bound.value_key for dm in partition_declared_measures(
                    declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
                )[1]
            ],
            column_type=column_type,
        )

    # Attach the active TD as time_key on every time-needing TransformKey the binder
    # left at None — the stage's own bucket is the axis on a StageSchema.
    active_td_key: Optional[TimeTruncKey] = None
    _active_model = scope.source_model if isinstance(scope, ModelScope) else None
    active_td = _resolve_main_time_dimension(query=query, model=_active_model)
    if active_td is not None:
        atd_key = bind_time_dimension(
            td=active_td, scope=scope, bundle=bundle,
            granularity=_td_granularity(active_td, bundle=bundle),
        ).bound.value_key
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

    # Transform-input typing runs pre-lowering, where change/change_pct are still
    # single nodes (their desugar duplicates the offending input).
    _proj_dim_dms, _proj_td_dms, _ = partition_declared_measures(
        declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    check_transform_inputs(
        roots=[
            *(dm.bound.value_key for dm in declared_measures),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        projected_grain_keys=frozenset(
            dm.bound.value_key for dm in (*_proj_dim_dms, *_proj_td_dms)
        ),
    )

    declared_measures, bound_filters, order_specs = _map_bound_keys(
        lambda vk: _resolve_shift_units(vk, bundle=bundle),
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
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
    _td_key_set: set[TimeTruncKey] = set()  # every projected bucket, not one per column
    for dm in _td_dms:
        vk = dm.bound.value_key
        if not isinstance(vk, TimeTruncKey):
            continue
        # Ambiguous only if the same column already mapped to a DIFFERENT bucket.
        if vk.column in _td_by_source and _td_by_source[vk.column] != vk:
            _td_ambiguous_sources.add(vk.column)
        _td_by_source[vk.column] = vk
        _td_key_set.add(vk)
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

    # An aggregate's "must be a query dimension" rule is positional — judged by the
    # checker after typing. A transform nested in an attached operand declares an
    # internal producer grain, so its partition keys need not be query dimensions.
    _reagg_operand_keys = attached_operand_keys([
        *[dm.bound.value_key for dm in declared_measures],
        *[bf.value_key for bf in bound_filters],
        *[sp.bound.value_key for sp in order_specs],
    ])

    def _validate_partition_keys(key: AggregateKey | TransformKey) -> Grain:
        label = (
            f"transform {key.op!r}" if isinstance(key, TransformKey)
            else f"aggregation {key.agg!r}"
        )
        lenient = isinstance(key, AggregateKey) or key in _reagg_operand_keys
        new_pks = []
        for pk in key.partition_keys or ():
            # A partition key over a join must be attributable from the root; a
            # re-aggregation's outer keys are judged against its operand grain instead.
            if not is_reaggregation_key(key):
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
            # td source col -> bucket; else the key itself (query dim / td bucket, or lenient finer-grain producer key)
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
    check_transform_partition_keys_in_operand_grain(
        roots=[
            *(dm.bound.value_key for dm in declared_measures),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        query_grain=_query_grain, active_bucket=active_td_key,
    )

    check_dimension_temporal_axis(
        declared_measures, bound_filters=bound_filters, order_specs=order_specs,
    )

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
        granularity_conflicts=granularity_conflicts,
    )


# Helpers


def _format_description_for_dimension(
    *, scope: ModelScope | StageSchema, full_name: str,
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
    *, scope: ModelScope | StageSchema, bound,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None, None
    return measure_key_format_description(
        model=scope.source_model, key=bound.value_key,
    )


def _type_for_measure_formula(
    *, scope: ModelScope | StageSchema, bound, bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if isinstance(scope, StageSchema):
        return stage_measure_type(bound.value_key, schema=scope)
    if scope.source_model is None:
        return None
    return measure_key_type(model=scope.source_model, key=bound.value_key, bundle=bundle)


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
    scope: ModelScope | StageSchema,
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if isinstance(scope, StageSchema):
        col = scope.get(full_name)
        return col.type if col is not None else None
    if scope.source_model is None:
        return None
    if "." in full_name:
        return _joined_column_type(
            source_model=scope.source_model, full_name=full_name, bundle=bundle,
        )
    col = scope.source_model.get_column(full_name)
    return col.type if col is not None else None


def _opaque_dim_type(
    *,
    scope: ModelScope | StageSchema,
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if isinstance(scope, StageSchema):
        col = scope.get(full_name)
        return col.type if col is not None else None
    return _type_for_dimension(scope=scope, full_name=full_name, bundle=bundle)


def _walk_dotted(
    *, source_model: SlayerModel, hops: List[str], bundle: ResolvedSourceBundle,
) -> Optional[Tuple[SlayerModel, Tuple[str, ...]]]:
    """``(terminal_model, canonical hops)`` of ``hops`` from ``source_model`` via the
    shared walker (None on a missing/circular/ambiguous hop), mirroring the binder."""
    models = bundle.models_by_name
    models.setdefault(source_model.name, source_model)
    try:
        chain = walk(root=source_model, path=tuple(hops), models_by_name=models)
    except (AmbiguousJoinPathError, CircularJoinPathError):
        return None
    if chain is None:
        return None
    terminal = models.get(chain[-1].target_model) if chain else source_model
    return (terminal, canonical_path(chain)) if terminal is not None else None


def _route_short_form_saved_measure(
    *, host: SlayerModel, hops: list[str], leaf: str, bundle: ResolvedSourceBundle
) -> Optional[Tuple[SlayerModel, str]]:
    """``(terminal_model, canonical_ref)`` when a ``len==1`` unresolvable prefix
    short-form routes to its full datasource-scoped path, else None.
    Routing triggers only when the first hop resolves to no edge; an adjacent
    parallel pair is a fail-closed ambiguous hop, not a route. This
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
    walked = (
        _walk_dotted(source_model=host, hops=list(route), bundle=bundle)
        if route is not None else None
    )
    if walked is None:
        return None
    terminal, canonical = walked
    return terminal, ".".join([*canonical, leaf])


def _resolve_saved_measure_ref(
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[Tuple[SlayerModel, "ModelMeasure", str]]:
    """Return ``(terminal_model, measure, canonical_ref)`` if ``formula`` is a
    bare/dotted saved-measure reference (binder resolution order, short-form
    auto-routing included), else None. ``canonical_ref`` is the canonical dotted
    path when routed or respelled, else the formula text unchanged."""
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    host = scope.source_model
    text = formula.strip()
    parts = text.split(".")
    if not all(p.isidentifier() for p in parts):
        return None
    if len(parts) > 1 and parts[0] == host.name:  # C14 self-prefix strip
        parts = parts[1:]
    *hops, leaf = parts
    located = (
        _locate_dotted_saved_measure(
            host=host, hops=hops, leaf=leaf, text=text, bundle=bundle,
        )
        if hops else (host, text)
    )
    if located is None:
        return None
    terminal, canonical_ref = located
    mm = terminal.get_measure(leaf)
    return (terminal, mm, canonical_ref) if mm is not None else None


def _locate_dotted_saved_measure(
    *,
    host: SlayerModel,
    hops: List[str],
    leaf: str,
    text: str,
    bundle: ResolvedSourceBundle,
) -> Optional[Tuple[SlayerModel, str]]:
    """``(terminal_model, canonical_ref)`` for a hop-qualified saved measure."""
    walked = _walk_dotted(source_model=host, hops=hops, bundle=bundle)
    if walked is None:
        return _route_short_form_saved_measure(
            host=host, hops=hops, leaf=leaf, bundle=bundle,
        )
    terminal, canonical = walked
    return terminal, (
        text if canonical == tuple(hops) else ".".join((*canonical, leaf))
    )


def _saved_model_measure_type(
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[DataType]:
    ref = _resolve_saved_measure_ref(scope=scope, bundle=bundle, formula=formula)
    return ref[1].type if ref is not None else None


def _saved_measure_public_name(
    *,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
    formula: str,
) -> Optional[str]:
    """Implicit surfaced name for a bare/dotted saved-measure reference — the
    full routed dotted text (short forms surface under their routed path)."""
    ref = _resolve_saved_measure_ref(scope=scope, bundle=bundle, formula=formula)
    return ref[2] if ref is not None else None


def _reject_computed_dim_name_collision(
    *, name: str, query: SlayerQuery, scope: ModelScope | StageSchema,
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
    scope: ModelScope | StageSchema,
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
    # Grain rules live in the checker; invoked here to preserve the bind-time firing point / precedence.
    check_computed_dimension(
        name=name, bound=bound,
        distinct_dimension_values=query.distinct_dimension_values,
    )
    dim_type = _type_for_measure_formula(scope=scope, bound=bound, bundle=bundle)
    return DeclaredMeasure(
        bound=bound,
        declared_name=name,
        public_name=name,
        name_is_explicit=name != auto_name_from_expression(d.expression),
        type=dim_type,
        is_dimension=True,
    )


def _declared_measures_from_query(  # NOSONAR(S3776) — three sequential projection passes (dimensions incl. computed, time dimensions, measures) building one ordered declared list; each pass is one contract and the order (dims → tds → measures) is the public projection order the function pins.
    *,
    query: SlayerQuery,
    scope: ModelScope | StageSchema,
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

    # Computed-dimension names resolve inside later ``partition_by=`` values;
    # built in declaration order.
    dim_alias_map: Dict[str, ValueKey] = {}
    for i, d in enumerate(query.dimensions or []):
        with stale_spelling_position(f"dimensions[{i}]"):
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
            # result key, type, opaque guard, and description.
            bound = bind_expr(
                parsed=parse_expr(full),
                scope=scope,
                bundle=bundle,
            )
            canonical = bound.routed_dotted or full
            # Opaque-grouping rule lives in the checker; invoked here to preserve the per-dimension firing point.
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
    # Time dimensions follow dimensions in the public projection. Same-column
    # time dimensions (distinct granularities) get granularity-suffixed public
    # names so their result keys disambiguate; a lone one keeps the
    # granularity-free key.
    bound_tds: List[Tuple[TimeDimension, BoundExpr, str]] = []
    for i, td in enumerate(query.time_dimensions or []):
        with stale_spelling_position(f"time_dimensions[{i}]"):
            granularity = _td_granularity(td, bundle=bundle)
            btd = bind_time_dimension(td=td, scope=scope, bundle=bundle, granularity=granularity)
            # The temporal / re-bucketing type rules are the checker's (P9).
            check_time_dimension_column(
                name=td.dimension.full_name,
                column_type=btd.column_type,
                upstream_granularity=btd.upstream_granularity,
                requested_granularity=granularity,
            )
            bound_tds.append(
                (td, btd.bound, btd.bound.routed_dotted or td.dimension.full_name)
            )
    _assert_equivalent_tds_agree(bound_tds)
    _td_flat_counts = Counter(_flatten_dotted(canon) for _, _, canon in bound_tds)
    for td, bound, canonical in bound_tds:
        base_flat = _flatten_dotted(canonical)
        assert isinstance(bound.value_key, TimeTruncKey)
        public = (
            f"{base_flat}.{bound.value_key.granularity}"
            if _td_flat_counts[base_flat] > 1 else base_flat
        )
        _guard_flatten(flat_name=_flatten_dotted(public), origin=canonical)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=public,
            public_name=public,
            label=td.label,
            type=DataType.TIMESTAMP,
        ))
    seen_measure_keys: Dict[str, Tuple[str, ValueKey, ModelMeasure]] = {}
    for i, m in enumerate(query.measures or []):
        with stale_spelling_position(f"measures[{i}]"):
            formula = m.formula
            explicit_name = m.name
            parsed = parse_expr(formula)
            bound = bind_expr(
                parsed=parsed, scope=scope, bundle=bundle, allow_measures=True,
                dimension_alias_map=dim_alias_map,
            )
            # The parsed tree drives text-shape alias derivation, so both spellings
            # of one formula share an alias.
            canonical = _canonical_alias_for_formula(
                formula, bound=bound, parsed=parsed, bundle=bundle,
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
            # sanitize to ``amount_cost_sum``) — fail loudly; the SAME
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
            m_type = explicit_type or _type_for_measure_formula(scope=scope, bound=bound, bundle=bundle)
            declared.append(DeclaredMeasure(
                bound=bound,
                declared_name=declared_name,
                public_name=public_name,
                label=m.label,
                # Keep the canonical alias when the surfaced name differs, so a colon-form filter / ORDER BY resolves.
                canonical_alias=canonical if alias_name else None,
                name_is_explicit=explicit_name is not None,
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
    bundle: Optional[ResolvedSourceBundle] = None,
) -> str:
    """Canonical public alias for a measure formula: ``canonical_aggregate_alias``
    for an AggregateKey root, ``canonical_agg_name`` for a plain ``col:agg``
    text shape, else the text sanitised via ``auto_name_from_expression``. The
    text shape runs over the CANONICAL colon-spelling rendering of ``parsed``
    when given, so ``cumsum(sum(revenue))`` and
    ``cumsum(revenue:sum)`` derive one alias."""
    if bound is not None and isinstance(bound.value_key, AggregateKey):
        spelled = bound.value_key if parsed is None or bundle is None else spelled_aggregate_key(
            parsed=parsed, key=bound.value_key, bundle=bundle)
        # stage_formula profile prefixes the join path relative to the stage (``count(customers.*)`` → ``customers._count``).
        alias = canonical_aggregate_alias(spelled, profile="stage_formula")
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


def _resolve_shift_units(key: ValueKey, *, bundle: ResolvedSourceBundle) -> ValueKey:
    """Every ``time_shift`` unit resolved: a built-in stays its name, a custom one becomes its definition."""
    rebuilt = cast(ValueKey, key.map_children(lambda c: _resolve_shift_units(c, bundle=bundle)))
    if not (isinstance(rebuilt, TransformKey) and rebuilt.op == "time_shift"):
        return rebuilt
    kwargs = dict(rebuilt.kwargs)
    unit = kwargs.get("granularity")
    if not isinstance(unit, str):
        return rebuilt
    resolved = check_granularity_known(
        name=unit, granularity=resolve_granularity(unit, defined=bundle.granularities),
        defined=bundle.granularities, where="time_shift",
    )
    kwargs["granularity"] = resolved.value if isinstance(resolved, TimeGranularity) else resolved
    return rebuilt.model_copy(update={"kwargs": tuple(sorted(kwargs.items()))})


def _td_granularity(td: TimeDimension, *, bundle: ResolvedSourceBundle) -> Granularity:
    """A time dimension's granularity resolved against the datasource."""
    return check_granularity_known(
        name=str(td.granularity), granularity=resolve_granularity(td.granularity, defined=bundle.granularities),
        defined=bundle.granularities, where=f"time dimension {td.dimension.full_name!r}",
    )


def _check_column_granularities(*, bundle: ResolvedSourceBundle) -> None:
    """Every model column's declared granularity names a known granularity."""
    for model in bundle.models_by_name.values():
        for column in model.columns:
            if column.granularity is not None:
                check_granularity_known(
                    name=str(column.granularity),
                    granularity=resolve_granularity(column.granularity, defined=bundle.granularities),
                    defined=bundle.granularities, where=f"column {column.name!r} of model {model.name!r}",
                )


def _resolve_granularity_calls(query: SlayerQuery, *, bundle: ResolvedSourceBundle) -> SlayerQuery:
    """Dimension entries calling a datasource granularity become time dimensions, as a built-in
    callee does at construction; any other unknown single-column call fails."""
    kept: list = []
    moved: List[TimeDimension] = []
    for item in query.dimensions or []:
        td = (
            _datasource_time_dimension(item.expression, bundle=bundle)
            if isinstance(item, ComputedDimension) and item.name == auto_name_from_expression(item.expression)
            else None
        )
        if td is None:
            kept.append(item)
        else:
            moved.append(td)
    if not moved:
        return query
    return query.model_copy(update={
        "dimensions": kept or None, "time_dimensions": [*(query.time_dimensions or []), *moved],
    })


def _datasource_time_dimension(entry: str, *, bundle: ResolvedSourceBundle) -> Optional[TimeDimension]:
    node, callee = call_callee(entry)
    if callee is None:
        return None
    column = functional_call_column(node)
    definition = bundle.granularities.get(granularity_key(callee))
    if definition is not None:
        check_granularity_call_shape(entry=entry, column=column)
        return TimeDimension.model_validate({"dimension": column, "granularity": definition.name})
    if column is not None and normalize_aggregation_name(callee) not in BUILTIN_AGGREGATIONS:
        check_dimension_call_known(entry=entry, defined=bundle.granularities)
    return None


def _build_date_range_filter(
    *,
    td: TimeDimension,
    scope: ModelScope | StageSchema,
    bundle: ResolvedSourceBundle,
) -> BoundFilter:
    """A TimeDimension's ``date_range`` as one row-phase filter on its bare column: a single
    point is ``col = P``; a pair is ``col >= lower`` and/or ``col <= upper`` (a null bound is open)."""
    full = td.dimension.full_name
    col_key = bind_expr(parsed=parse_expr(full), scope=scope, bundle=bundle).value_key
    assert isinstance(col_key, (ColumnKey, ColumnSqlKey)), (
        f"date_range filter for TimeDimension {full!r} expected a "
        f"column reference; got {type(col_key).__name__}."
    )
    date_range = td.date_range
    assert date_range  # the caller builds this only for a present date_range
    if len(date_range) == 1:
        assert date_range[0] is not None  # construction rejects a lone null
        comparisons = [TimePointCmpKey(op="=", operand=col_key, point=date_range[0])]
    else:
        lower, upper = date_range
        comparisons = [
            *([] if lower is None else [TimePointCmpKey(op=">=", operand=col_key, point=lower)]),
            *([] if upper is None else [TimePointCmpKey(op="<=", operand=col_key, point=upper)]),
        ]
    predicate = comparisons[0] if len(comparisons) == 1 else ArithmeticKey(op="and", operands=tuple(comparisons))
    refs = tuple(walk_value_keys(predicate))
    return BoundFilter(value_key=predicate, phase=Phase.ROW, referenced_keys=refs)


_LOWER_OPS = frozenset({">=", ">"})
_UPPER_OPS = frozenset({"<", "<="})
_MIRROR = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}


def _as_datetime(value: date) -> datetime:
    return value if isinstance(value, datetime) else datetime(value.year, value.month, value.day)


def _snapped_value(
    value: datetime, *, grans: List[Granularity], is_date: bool,
) -> date:
    """The earliest bucket boundary at or before ``value`` over ``grans``, in the operand's type."""
    snapped = min(floor_to(value, g) for g in grans)
    return floor_to(snapped, TimeGranularity.DAY).date() if is_date else snapped


def _snap_frame_bound(
    cj: ValueKey, *, grans_by_column: Dict[ValueKey, List[Granularity]],
    column_type, now: datetime, upper_seen: set,
) -> ValueKey:
    """A frame-bound conjunct on a time-dimension column snapped to a bucket boundary; others untouched."""
    if isinstance(cj, ArithmeticKey) and cj.op == "and":
        return cj.map_children(lambda c: _snap_frame_bound(
            c, grans_by_column=grans_by_column, column_type=column_type, now=now, upper_seen=upper_seen,
        ))
    if not is_frame_bound(key=cj, time_columns=grans_by_column.keys()):
        return cj
    assert isinstance(cj, ArithmeticKey)
    column, literal = cj.operands
    op = cj.op
    if column not in grans_by_column:
        column, literal, op = literal, column, _MIRROR[op]
    assert isinstance(literal, LiteralKey) and isinstance(literal.value, date)
    value = _as_datetime(literal.value)
    if op in _UPPER_OPS:
        upper_seen.add(column)
        value = min(value, now)
    snapped = _snapped_value(
        value, grans=grans_by_column[column], is_date=column_type(column) is DataType.DATE,
    )
    return ArithmeticKey(
        op=">=" if op in _LOWER_OPS else "<", operands=(column, LiteralKey(value=snapped)),
    )


def _snap_whole_periods(
    *,
    bound_filters: List[BoundFilter],
    bound_filter_texts: List[Optional[str]],
    n_date_range: int,
    td_keys: List[ValueKey],
    column_type,
    now: datetime,
) -> Tuple[List[BoundFilter], List[Optional[str]], int, List[GranularityConflict]]:
    """``whole_periods_only``: snap every frame bound on a time-dimension column down to the
    earliest bucket boundary, the upper bound clamped to now (added when absent); report
    granularity pairs on one column that do not nest."""
    grans_by_column: Dict[ValueKey, List[Granularity]] = {}
    for td_key in td_keys:
        if isinstance(td_key, TimeTruncKey):
            grans = grans_by_column.setdefault(td_key.column, [])
            if td_key.granularity not in grans:
                grans.append(td_key.granularity)
    conflicts = [
        GranularityConflict(column=dotted_key_display(column), granularities=(a, b))
        for column, grans in grans_by_column.items()
        for i, a in enumerate(grans) for b in grans[i + 1:]
        if not (nests_into(a, b) or nests_into(b, a))
    ]
    upper_seen: set = set()
    snapped = []
    for bf in bound_filters:
        if bf.phase != Phase.ROW:
            snapped.append(bf)
            continue
        vk = _snap_frame_bound(
            bf.value_key, grans_by_column=grans_by_column, column_type=column_type,
            now=now, upper_seen=upper_seen,
        )
        snapped.append(bf if vk is bf.value_key else BoundFilter(
            value_key=vk, phase=bf.phase, referenced_keys=tuple(walk_value_keys(vk)),
        ))
    added = []
    for column, grans in grans_by_column.items():
        if column in upper_seen:
            continue
        upper = _snapped_value(now, grans=grans, is_date=column_type(column) is DataType.DATE)
        vk = ArithmeticKey(op="<", operands=(column, LiteralKey(value=upper)))
        added.append(BoundFilter(value_key=vk, phase=Phase.ROW, referenced_keys=tuple(walk_value_keys(vk))))
    return (
        [*snapped[:n_date_range], *added, *snapped[n_date_range:]],
        [*bound_filter_texts[:n_date_range], *([None] * len(added)), *bound_filter_texts[n_date_range:]],
        n_date_range + len(added),
        conflicts,
    )


def _assert_equivalent_tds_agree(
    bound_tds: List[Tuple[TimeDimension, BoundExpr, str]],
) -> None:
    """Time dimensions binding to one ``TimeTruncKey`` (equivalent column spellings) must
    agree on ``date_range``/``label``; otherwise their date-range filters and shared
    projection column silently conflict. Construction dedup catches this once the query is
    rooted — this is the bind-time backstop for spellings that only prove equivalent here
    (full identity matching lands in DEV-1925)."""
    seen: Dict[ValueKey, TimeDimension] = {}
    for td, bound, _ in bound_tds:
        prior = seen.setdefault(bound.value_key, td)
        if prior is not td and (prior.date_range, prior.label) != (td.date_range, td.label):
            raise GranularityCallError(
                f"Conflicting time dimensions on {td.dimension.full_name!r} at "
                f"{td.granularity} granularity: equivalent columns must not "
                f"differ in date range or label."
            )


def _named_td_matches(
    *, tds: List[TimeDimension], target: str,
) -> Tuple[List[TimeDimension], List[TimeDimension]]:
    """(full-name matches, leaf matches) for ``target`` among ``tds``. Two same-column
    buckets share a full_name, so full matches is a list, not a single TD."""
    full = [td for td in tds if td.dimension.full_name == target]
    if full:
        return full, []
    return [], [td for td in tds if td.dimension.name == target]


def _host_local_default_td(
    *, tds: List[TimeDimension], default: str,
) -> Optional[TimeDimension]:
    # The default points only at the host model; prefer a host-local TD over a same-leaf joined one.
    matches = [
        td for td in tds
        if td.dimension.model is None and td.dimension.name == default
    ]
    # Same column at several granularities: the passive model default can't pick a
    # bucket. Resolve to None (not raise) so no-transform queries still run; a
    # transform that needs the axis then fails via check_time_transforms_resolved.
    return matches[0] if len(matches) == 1 else None


def _resolve_main_time_dimension(
    *,
    query: SlayerQuery,
    model: Optional[SlayerModel],
) -> Optional[TimeDimension]:
    """Resolve the active time dimension for transform/windowing: 0 TDs → None; 1 → that TD; 2+ → main_time_dimension (full_name then leaf) else the model's default_time_dimension else None. A stage has no model, so its default step is skipped."""
    tds = list(query.time_dimensions or [])
    if not tds:
        return None
    if len(tds) == 1:
        return tds[0]

    if query.main_time_dimension:
        target = query.main_time_dimension
        # Prefer full-name (more specific) over leaf match.
        full_matches, leaf_matches = _named_td_matches(tds=tds, target=target)
        if len(full_matches) == 1:
            return full_matches[0]
        if len(full_matches) > 1:
            # Same column, several granularities: a bare column can't pick a bucket.
            # Per-granularity selection (e.g. year(created_at)) lands in DEV-1925.
            raise AmbiguousReferenceError(
                name=target,
                candidates=[
                    f"{td.granularity}({td.dimension.full_name})"
                    for td in full_matches
                ],
            )
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

    default = model.effective_default_time_dimension if model is not None else None
    if default:
        return _host_local_default_td(tds=tds, default=default)
    return None
