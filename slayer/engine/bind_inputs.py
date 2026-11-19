"""The query-level bind pass: every text surface of a ``SlayerQuery`` parsed,
bound and normalized into a ``PreboundQuery`` (the elaboration sub-phase that
turns syntax into keys; expression-level binding lives in ``binding``)."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from typing import Callable, Dict, FrozenSet, List, Optional, Tuple, cast

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType, TimeGranularity, normalize_aggregation_name, TIME_TRANSFORMS
from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    QueryExtensionError,
    SlayerError,
    UnresolvableDimensionRelationshipError,
    AmbiguousReferenceError,
    CircularRelationshipPathError,
    GranularityCallError,
    UnknownReferenceError,
)
from slayer.core.format import NumberFormat
from slayer.core.time_spine import TIME_SPINE_FIELD, TIME_SPINE_DATASET, is_spine, is_spine_query
from slayer.core.relationship_walker import follow, resolve_hop, resolved_terminal, walk_terminal
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    FieldKey,
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
    source_anchor_path,
    walk_value_keys,
    window_kwarg_of,
)
from slayer.core.models import DatasetMetric, SlayerDataset
from slayer.core.granularity import Granularity, nests_into, resolve_granularity
from slayer.core.query import (
    FieldRef,
    QueryDimension,
    QueryMetric,
    SlayerQuery,
    TimeDimension,
    call_callee,
    computed_dimension_name,
    functional_call_field,
    granularity_call_parts,
)
from slayer.core.refs import auto_name_from_expression, bucket_remedy, dotted_key_display, first_bucket_remedy
from slayer.core.time_bounds import is_frame_bound
from slayer.core.time_points import floor_to
from slayer.core.scope import (
    DatasetScope,
    StageSchema,
    host_dataset_name,
    root_spelling,
    stale_spelling_position,
)
from slayer.engine.population import dimension_routing
from slayer.engine.extensions import declaring_dataset_path
from slayer.engine.binding import bind_expr, bind_filter, bind_time_dimension, spelled_aggregate_key
from slayer.engine.elaborate_env import (
    check_computed_dim_name_collision,
    check_computed_dimension,
    check_conditional_branches,
    check_date_operands,
    canonical_list_dimension,
    canonical_list_reads,
    check_list_dimension,
    check_list_order_key,
    check_list_operands,
    whole_list_reads,
    coerce_list_arguments,
    ListTyping,
    check_dimension_call_known,
    check_whole_periods_axis,
    check_granularity_known,
    check_metric_dedupe_collision,
    check_spine_clash,
    check_spine_plain_use,
    check_stage_flatten_collision,
    check_dimension_temporal_axis,
    check_opaque_grouping_dim,
    check_partition_key_resolves,
    check_raw_rows_filter_metric_ref,
    check_raw_rows_order_metric_ref,
    check_time_dimension_field,
    check_time_dimension_home,
    check_time_dimension_row_level,
    check_spine_time_dimension,
    time_dimension_type,
    check_transform_inputs,
    check_transform_partition_keys_in_operand_grain,
    check_time_transforms_resolved,
    coerce_text_numbers,
    coerce_zoned_comparisons,
    resolve_time_points,
    date_only_notices,
)
from slayer.engine.population.home import row_expression_crossing
from slayer.engine.cardinality.relationship_safety import assert_partition_key_attributable
from slayer.engine.key_metadata import (
    bundle_dialect,
    key_element_type,
    metric_key_format_description,
    scope_field_element,
    metric_key_preserves_native_type,
    metric_key_type,
    scope_field_type,
    scope_zoned_timestamp,
)
from slayer.core.syntax import (
    AggCall,
    Cmp,
    DottedRef,
    Literal,
    ParsedExpr,
    Ref,
    TransformCall,
    call_keywords,
    dotted_quoting,
    drop_root_qualifier,
    parse_bound_expr,
    parse_expr,
    root_relative,
    text_paths,
    walk_parsed,
)
from slayer.ir.bound import (
    GranularityConflict,
    BoundExpr,
    BoundTimeDimension,
    BoundFilter,
    DeclaredMetric,
    OrderSpec,
)
from slayer.ir.prebound import ModePolicy, PreboundQuery, partition_declared_metrics
from slayer.ir.source_bundle import InnerRestriction, ResolvedSourceBundle, resolve_scope
from slayer.sql.naming import canonical_aggregate_alias, flat_name as _flatten_dotted, expression_text_alias

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
    for attr in ("input", "left", "right", "this", "operand", "value"):
        child = getattr(node, attr, None)
        if child is not None and not isinstance(child, (str, bool)):
            yield child
    for attr in ("args", "operands", "kwargs"):
        for item in getattr(node, attr, None) or ():
            # kwargs are (name, value) pairs; take the value.
            yield item[1] if isinstance(item, tuple) and len(item) == 2 else item


def _expr_has_metric_ref(
    node, *, metric_names: FrozenSet[str], scope, bundle,
) -> bool:
    if node is None:
        return False
    if isinstance(node, (AggCall, TransformCall)):
        return True
    if isinstance(node, Ref) and node.name in metric_names:
        return True
    # A dotted leaf resolving to a saved metric on the terminal dataset is a metric ref too.
    if isinstance(node, DottedRef) and _resolve_saved_metric_ref(
        scope=scope, bundle=bundle, expression=".".join(node.parts),
    ) is not None:
        return True
    return any(
        _expr_has_metric_ref(
            child, metric_names=metric_names, scope=scope, bundle=bundle,
        )
        for child in _iter_expr_children(node)
    )


def _reject_metric_refs_for_raw_rows(
    *, query: SlayerQuery, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> None:
    """Raw-rows mode (``distinct_dimension_values=False``): reject any metric reference in filters/order."""
    src = scope.population
    metric_names: FrozenSet[str] = frozenset(
        m.name for m in (src.metrics if src is not None else []) if m.name
    )
    _reject_metric_refs_in_filters(
        query=query, metric_names=metric_names, scope=scope, bundle=bundle,
    )
    _reject_metric_refs_in_order(
        query=query,
        metric_names=metric_names,
        source_name=getattr(src, "name", None),
        scope=scope,
        bundle=bundle,
    )


def _reject_metric_refs_in_filters(
    *, query: SlayerQuery, metric_names: FrozenSet[str], scope,
    bundle: ResolvedSourceBundle,
) -> None:
    for f in (query.filter_conditions):
        if not isinstance(f, str):
            continue
        try:
            parsed = parse_expr(f)
        except Exception:  # noqa: BLE001 — binder reports parse errors properly
            continue
        check_raw_rows_filter_metric_ref(
            offending=f if _expr_has_metric_ref(
                parsed, metric_names=metric_names, scope=scope, bundle=bundle,
            ) else None,
        )


def _parse_order_expression(raw: str):
    try:
        return parse_expr(raw)
    except Exception:  # noqa: BLE001 — binder reports parse errors properly
        return None


def _reject_metric_refs_in_order(
    *,
    query: SlayerQuery,
    metric_names: FrozenSet[str],
    source_name: Optional[str],
    scope,
    bundle: ResolvedSourceBundle,
) -> None:
    for item in (query.order or []):
        raw = item.raw_expression
        if raw:
            parsed = _parse_order_expression(raw)
            check_raw_rows_order_metric_ref(
                contains=raw if parsed is not None and _expr_has_metric_ref(
                    parsed, metric_names=metric_names, scope=scope, bundle=bundle,
                ) else None,
            )
        if item.ref is not None:
            _reject_metric_ref_in_order_ref(
                ref=item.ref, metric_names=metric_names, source_name=source_name, scope=scope, bundle=bundle,
            )


def _reject_metric_ref_in_order_ref(
    *, ref: FieldRef, metric_names: FrozenSet[str], source_name: Optional[str], scope, bundle: ResolvedSourceBundle,
) -> None:
    name = ref.name
    check_raw_rows_order_metric_ref(
        saved_name=name if name and name in metric_names else None,
        source_name=source_name,
    )
    # A dotted ORDER BY field whose leaf is a saved metric on the terminal dataset is a metric ref too.
    full = ref.full_name
    check_raw_rows_order_metric_ref(
        saved_dotted=full if full and "." in full and _resolve_saved_metric_ref(
            scope=scope, bundle=bundle, expression=full,
        ) is not None else None,
    )


def _map_bound_keys(
    key_fn: Callable[[ValueKey], ValueKey],
    *,
    declared_metrics: List[DeclaredMetric],
    bound_filters: List[BoundFilter],
    order_specs: List[OrderSpec],
    skip_dimensions: bool = False,
) -> Tuple[List[DeclaredMetric], List[BoundFilter], List[OrderSpec]]:
    new_metrics = [
        dm.model_copy(update={"bound": BoundExpr(
            value_key=(
                dm.bound.value_key if (skip_dimensions and dm.is_dimension)
                else key_fn(dm.bound.value_key)
            ),
            routed_dotted=dm.bound.routed_dotted,
        )})
        for dm in declared_metrics
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
            nulls=spec.nulls,
        )
        for spec in order_specs
    ]
    return new_metrics, new_filters, new_specs


def _bind_inner_restriction(
    restriction: InnerRestriction, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> BoundFilter:
    """``restriction`` bound exactly as its filter written in the query; a product read never reaches it."""
    host = scope.population
    if host is not None and host.unrelated_datasets:
        host = host.with_unrelated(frozenset())
        scope, bundle = DatasetScope(population=host), bundle.model_copy(update={"population": host})
    try:
        path = declaring_dataset_path(restriction, host=host, datasets_by_name=bundle.datasets_by_name)
        node = Cmp(
            op="is not", left=DottedRef(parts=(*path, restriction.token, restriction.key), quoted=False),
            right=Literal(value=None), single_eq=False,
        )
        return bind_filter(parsed=node, scope=scope, bundle=bundle)
    except AmbiguousRelationshipPathError as exc:
        raise AmbiguousRelationshipPathError(
            source_dataset=exc.source_dataset, target_dataset=exc.target_dataset, candidates=exc.candidates,
            token=exc.token, context=f"The {restriction.label}",
        ) from exc
    except (UnknownReferenceError, UnresolvableDimensionRelationshipError) as exc:
        raise QueryExtensionError(
            f"The {restriction.label} restricts the population, but no relationship path reaches "
            f"{restriction.dataset!r} from it."
        ) from exc


def bind_query_inputs(  # NOSONAR(S3776) — one cohesive bind pass. The stages are strictly sequential and share the growing `declared_metrics` / `bound_filters` / `order_specs` triple: parse+bind, time-key attachment, sugar lowering, rank-partition validation. Splitting them would thread the same three lists through four signatures without removing a branch.
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[DatasetScope] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
) -> PreboundQuery:
    """Parse and bind every text surface of a ``SlayerQuery`` (the only door into the parser); returns fully-normalized keys. Dataset filters excluded (scope-owned)."""
    if scope is None:
        scope = resolve_scope(bundle=bundle)
    check_spine_clash(clash=bundle.spine_clash and is_spine_query(query))
    _check_field_granularities(bundle=bundle)
    _check_dimension_calls(query, bundle=bundle)
    query = _dedupe_equivalent_time_dimensions(query, scope=scope, bundle=bundle)

    # Runs BEFORE binding so the targeted error wins over the binder's generic one.
    if query.distinct_dimension_values is False:
        _reject_metric_refs_for_raw_rows(query=query, scope=scope, bundle=bundle)

    declared_metrics = _declared_metrics_from_query(
        query=query, scope=scope, bundle=bundle,
    )

    # Alias lookup for ORDER BY, checked before bind_expr so aggregate aliases resolve via the registry.
    declared_alias_to_bound: Dict[str, BoundExpr] = {}
    for dm in declared_metrics:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                declared_alias_to_bound.setdefault(alias, dm.bound)

    # Declared-METRIC aliases a filter may reference by name, interning onto the same slot as the aggregate expression.
    n_dims = len(query.dimensions or [])
    n_tds = len(query.time_dimensions or [])
    filter_alias_map: Dict[str, ValueKey] = {}
    _, _, _agg_dms = partition_declared_metrics(
        declared_metrics=declared_metrics, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    for dm in _agg_dms:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                filter_alias_map.setdefault(alias, dm.bound.value_key)
    # A computed dimension's name is a query-local alias resolvable in filters/order.
    for dm in declared_metrics:
        if dm.is_dimension and dm.public_name is not None:
            filter_alias_map.setdefault(dm.public_name, dm.bound.value_key)
    # ...and resolvable inside a filter/order ``partition_by=``.
    _computed_names = frozenset(d.name for d in (query.dimensions or []) if d.is_computed)
    dim_alias_map: Dict[str, ValueKey] = {
        dm.public_name: dm.bound.value_key
        for dm in declared_metrics
        if dm.is_dimension
        and dm.public_name is not None
        and dm.public_name in _computed_names
    }
    _, _td_alias_dms, _ = partition_declared_metrics(
        declared_metrics=declared_metrics, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    dim_alias_map.update(_time_dimension_aliases([
        (td, dm.bound, "") for td, dm in zip(query.time_dimensions or [], _td_alias_dms)
    ]))

    # Filter list in WHERE order: date_range, dataset filters (Mode-A SQL), then user query filters.
    bound_filters: List[BoundFilter] = []
    # Parallel original filter text (None for date_range bounds), for semi-join push entries.
    bound_filter_texts: List[Optional[str]] = []

    # 1. date_range filters (one per TD with a date_range)
    for i, td in enumerate(query.time_dimensions or []):
        with stale_spelling_position(f"time_dimensions[{i}]"):
            if not td.date_range:
                continue
            # A stage date_range filters the stage's rows on its bare field, exactly
            # as a dataset-scope range filters a dataset's rows.
            bf = _build_date_range_filter(td=td, scope=scope, bundle=bundle)
            bound_filters.append(bf)
            bound_filter_texts.append(None)
    n_date_range = len(bound_filters)

    # 2. SlayerDataset.filters — lifted from scope in plan_query, not here.

    # 3. user query filters (Mode-B DSL). Dedupe by bound key (first wins) so the
    #    alias and aggregate-expression forms of a ref don't duplicate the HAVING clause.
    for i, f in enumerate(query.filter_conditions):
        with stale_spelling_position(f"filter[{i}]"):
            if not isinstance(f, str):
                continue
            bf = bind_filter(
                parsed=parse_bound_expr(f, query.bound_variables),
                scope=scope,
                bundle=bundle,
                alias_map=filter_alias_map,
                dimension_alias_map=dim_alias_map,
            )
            if any(existing.value_key == bf.value_key for existing in bound_filters):
                continue
            bound_filters.append(bf)
            bound_filter_texts.append(f)

    # 4. query-declared INNER joins, each the field filter "its target is matched".
    for restriction in bundle.inner_restrictions:
        bf = _bind_inner_restriction(restriction, scope=scope, bundle=bundle)
        if not any(existing.value_key == bf.value_key for existing in bound_filters):
            bound_filters.append(bf)
            bound_filter_texts.append(None)

    order_specs = []
    _td_buckets = [(td, dm.bound.value_key) for td, dm in zip(query.time_dimensions or [], _td_alias_dms)]
    # Host spelling for the qualifier check below.
    _order_host_name = host_dataset_name(scope)
    for i, o in enumerate(query.order or []):
        # An implied (default) order item was never written: it records no spelling.
        with stale_spelling_position(None if o.implied else f"order[{i}]"):
            order_ref = o.ref
            col_name = order_ref.name if order_ref is not None else o.field
            full_name = order_ref.full_name if order_ref is not None else o.field
            # A ``date_trunc('<unit>', expr)`` order key sorts by the projected time
            # dimension's bucket — resolved to its field binding.
            gran_parts = granularity_call_parts(o.raw_expression) if o.raw_expression else None
            _order_gran = (
                resolve_granularity(gran_parts[1], defined=bundle.granularities)
                if gran_parts is not None else None
            )
            if gran_parts is not None and _order_gran is not None:
                _col, _gran = gran_parts
                matching_td = _projected_bucket_td(
                    expr=_col, granularity=_order_gran, buckets=_td_buckets, scope=scope, bundle=bundle,
                )
                if matching_td is None:
                    check_spine_plain_use(
                        offender=o.raw_expression if _text_reads_spine(_col, scope=scope) else None,
                        position="order key",
                    )
                    raise GranularityCallError(
                        f"Order key date_trunc('{_gran}', {_col}) has no matching projected time "
                        f"dimension. Project date_trunc('{_gran}', {_col}) in dimensions to order by its bucket."
                    )
                order_specs.append(OrderSpec(
                    bound=bind_time_dimension(
                        td=matching_td, scope=scope, bundle=bundle,
                        granularity=_td_granularity(matching_td, bundle=bundle),
                    ).bound,
                    direction=o.direction,
                    nulls=o.nulls or "default",
                ))
                continue
            # An expression item binds its raw_expression, skipping alias lookups.
            if order_ref is None and o.raw_expression:
                order_specs.append(OrderSpec(
                    bound=bind_expr(
                        parsed=parse_bound_expr(o.raw_expression, query.bound_variables),
                        scope=scope,
                        bundle=bundle,
                        alias_map=filter_alias_map,
                        dimension_alias_map=dim_alias_map,
                    ),
                    direction=o.direction,
                    nulls=o.nulls or "default",
                ))
                continue
            # An ORDER BY over a partition_by / window= aggregate must bind raw_expression (the alias shortcut would drop the partition/window).
            if o.raw_expression and _passes_grain_kwargs(o.raw_expression):
                _part_bound = bind_expr(
                    parsed=parse_bound_expr(o.raw_expression, query.bound_variables),
                    scope=scope, bundle=bundle,
                    alias_map=filter_alias_map,
                    dimension_alias_map=dim_alias_map,
                )
                if any(
                    isinstance(k, AggregateKey) and (
                        k.partition_keys is not None or window_kwarg_of(k) is not None
                    )
                    for k in walk_value_keys(_part_bound.value_key)
                ):
                    order_specs.append(OrderSpec(
                        bound=_part_bound, direction=o.direction, nulls=o.nulls or "default",
                    ))
                    continue
            # A FOREIGN-qualified order ref must not resolve to a same-named local field via the bare-leaf shortcut.
            _order_qualifier = order_ref.dataset if order_ref is not None else None
            _order_host_local = (
                _order_qualifier is None or _order_qualifier == _order_host_name
            )
            # Prefer alias resolution over dataset-scope binding; try dotted then flattened forms, falling back to raw.
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
            elif o.raw_expression:
                bo = bind_expr(
                    parsed=parse_bound_expr(o.raw_expression, query.bound_variables),
                    scope=scope,
                    bundle=bundle,
                    alias_map=filter_alias_map,
                    dimension_alias_map=dim_alias_map,
                )
            else:
                # Bind the FULL reference — a dotted order reference would otherwise rebind as the wrong host field.
                bo = bind_expr(
                    parsed=parse_expr(order_ref.spelling if order_ref is not None else o.field),
                    scope=scope,
                    bundle=bundle,
                )
            order_specs.append(OrderSpec(bound=bo, direction=o.direction, nulls=o.nulls or "default"))

    field_type = scope_field_type(scope=scope, bundle=bundle)
    zoned = scope_zoned_timestamp(scope=scope, bundle=bundle)
    lists = _list_typing(scope=scope, bundle=bundle)
    # A list has no sort order: the default order skips it.
    order_specs = [
        spec for o, spec in zip(query.order or [], order_specs, strict=True)
        if not (o.implied and lists.is_list(spec.bound.value_key))
    ]
    check_list_operands(
        roots=[
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        typing=lists,
    )
    for spec in order_specs:
        check_list_order_key(key=spec.bound.value_key, typing=lists)
    check_date_operands(
        roots=[
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        field_type=field_type,
    )
    notices = date_only_notices(
        [
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        field_type=field_type, variables=query.date_variables,
    )
    # Every time point resolves against the execution's one clock reading.
    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        lambda vk: coerce_zoned_comparisons(
            coerce_text_numbers(
                coerce_list_arguments(
                    resolve_time_points(vk, field_type=field_type, now=bundle.now, units=bundle.granularities),
                    typing=lists,
                ),
                field_type=field_type,
            ),
            zoned=zoned, field_type=field_type,
        ),
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )
    check_conditional_branches(
        roots=[
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        field_type=field_type,
    )
    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        lambda vk: canonical_list_reads(vk, typing=lists),
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )
    will_group_by = _will_group_by(query)
    declared_metrics = [
        _rekeyed(dm, canonical_list_dimension(dm.bound.value_key, typing=lists, will_group_by=will_group_by))
        if i < n_dims else dm
        for i, dm in enumerate(declared_metrics)
    ]
    list_reads = whole_list_reads(
        dimensions=[dm.bound.value_key for dm in declared_metrics[:n_dims]],
        roots=[
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
    )
    declared_metrics = _retyped_after_coercion(
        declared_metrics, n_grain=n_dims + n_tds, scope=scope, bundle=bundle,
    )
    granularity_conflicts: List[GranularityConflict] = []
    if query.whole_periods_only:
        check_whole_periods_axis(
            has_time_dimension=bool(n_tds),
            remedy=first_bucket_remedy(dm.bound.value_key for dm in declared_metrics[:n_dims]),
        )
        bound_filters, bound_filter_texts, n_date_range, granularity_conflicts = _snap_whole_periods(
            bound_filters=bound_filters, bound_filter_texts=bound_filter_texts,
            n_date_range=n_date_range, now=bundle.now,
            td_keys=[
                dm.bound.value_key for dm in partition_declared_metrics(
                    declared_metrics=declared_metrics, n_dims=n_dims, n_time_dimensions=n_tds,
                )[1]
            ],
            field_type=field_type,
        )

    # Attach the active TD as time_key on every time-needing TransformKey the binder left at None.
    active_td_key: Optional[TimeTruncKey] = None
    active_td = _resolve_main_time_dimension(query=query, dataset=scope.population, root=root_spelling(scope))
    if active_td is not None:
        atd_key = bind_time_dimension(
            td=active_td, scope=scope, bundle=bundle,
            granularity=_td_granularity(active_td, bundle=bundle),
        ).bound.value_key
        assert isinstance(atd_key, TimeTruncKey)
        active_td_key = atd_key

    if active_td_key is not None:
        declared_metrics, bound_filters, order_specs = _map_bound_keys(
            lambda vk: _attach_time_keys(vk, td_key=active_td_key),
            declared_metrics=declared_metrics,
            bound_filters=bound_filters,
            order_specs=order_specs,
        )

    # Any time-needing transform still at time_key=None means no resolvable TD.
    check_time_transforms_resolved(roots=[
        *(dm.bound.value_key for dm in declared_metrics),
        *(bf.value_key for bf in bound_filters),
        *(spec.bound.value_key for spec in order_specs),
    ], remedy=first_bucket_remedy(dm.bound.value_key for dm in declared_metrics[:n_dims]))

    # Transform-input typing runs pre-lowering, where change/change_pct are still
    # single nodes (their desugar duplicates the offending input).
    _proj_dim_dms, _proj_td_dms, _ = partition_declared_metrics(
        declared_metrics=declared_metrics, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    check_transform_inputs(
        roots=[
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        projected_grain_keys=frozenset(
            dm.bound.value_key for dm in (*_proj_dim_dms, *_proj_td_dms)
        ),
    )

    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        lambda vk: _resolve_shift_units(vk, bundle=bundle),
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    # Sugar lowering runs AFTER patching so the desugared time_shift inherits the patched time_key.
    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        lower_sugar_transforms,
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    # Validate every rank-family partition_by field resolves to a query dim/td, rewriting a td source field to its bucket TimeTruncKey. Runs BEFORE interning.
    _dim_dms, _td_dms, _ = partition_declared_metrics(
        declared_metrics=declared_metrics, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    _dim_key_set = {dm.bound.value_key for dm in _dim_dms}
    # A source field at two granularities maps to two buckets — a bare partition_by is then ambiguous.
    _td_by_source: Dict[ValueKey, TimeTruncKey] = {}
    _td_ambiguous_sources: set = set()
    _td_key_set: set[TimeTruncKey] = set()  # every projected bucket, not one per field
    for dm in _td_dms:
        vk = dm.bound.value_key
        if not isinstance(vk, TimeTruncKey):
            continue
        # Ambiguous only if the same field already mapped to a DIFFERENT bucket.
        if vk.source in _td_by_source and _td_by_source[vk.source] != vk:
            _td_ambiguous_sources.add(vk.source)
        _td_by_source[vk.source] = vk
        _td_key_set.add(vk)
    _available_dims = [dm.declared_name for dm in (*_dim_dms, *_td_dms)]

    # Normalise transform constituents (D4b, Axiom 11.1): an ungrained, non-windowed,
    # local inner aggregate inside a transform constituent (metric/filter/order, not
    # dimensions) is explicitly grained at the query grain — BEFORE partition-key
    # validation, so the synthesized keys face the same attributability / resolution
    # checks as a user-written partition_by=. Every dependent set below (dim-agg,
    # combined-consumer, reagg-operand) is computed AFTER, over the normalised keys.
    _query_grain = Grain.of([*_dim_key_set, *_td_key_set])
    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        lambda vk: normalize_transform_constituents(vk, query_grain=_query_grain),
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
        skip_dimensions=True,
    )

    # An aggregate's "must be a query dimension" rule is positional — judged by the
    # checker after typing. A transform nested in an attached operand declares an
    # internal producer grain, so its partition keys need not be query dimensions.
    _reagg_operand_keys = attached_operand_keys([
        *[dm.bound.value_key for dm in declared_metrics],
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
            # A partition key over a relationship must be attributable from the root; a
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

    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        _rw,
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )
    check_transform_partition_keys_in_operand_grain(
        roots=[
            *(dm.bound.value_key for dm in declared_metrics),
            *(bf.value_key for bf in bound_filters),
            *(spec.bound.value_key for spec in order_specs),
        ],
        query_grain=_query_grain, active_bucket=active_td_key,
    )

    check_dimension_temporal_axis(
        declared_metrics, bound_filters=bound_filters, order_specs=order_specs,
    )

    # Lower a collapsing transform constituent (first/last, D4c) to an exact
    # per-partition pick AFTER the axis check (which sees the raw transform); the
    # synthesized max's partition_by is a carrier grain key, validated inside the
    # carrier sub-plan, so it runs after _rw. All positions.
    declared_metrics, bound_filters, order_specs = _map_bound_keys(
        lower_collapsing_constituents,
        declared_metrics=declared_metrics,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    return PreboundQuery(
        declared_metrics=declared_metrics,
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
        mode_policy=ModePolicy.resolve(query.to_many_handling),
        granularity_conflicts=granularity_conflicts,
        whole_list_reads=list_reads,
        date_only_notices=notices,
    )


# Helpers


def _format_description_for_dimension(
    *, scope: DatasetScope, full_name: str,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    if scope.population is None:
        return None, None
    if "." in full_name:
        return None, None
    col = scope.population.get_field(full_name)
    if col is None:
        return None, None
    return col.format, col.description


def _format_description_for_metric_expression(
    *, scope: DatasetScope, bound, bundle: ResolvedSourceBundle,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    if scope.population is None:
        return None, None
    return metric_key_format_description(
        dataset=scope.population, key=bound.value_key, bundle=bundle,
    )


def _rekeyed(dm: DeclaredMetric, key: ValueKey) -> DeclaredMetric:
    if key is dm.bound.value_key:
        return dm
    return dm.model_copy(update={"bound": dm.bound.model_copy(update={"value_key": key}), "is_dimension": True})


def _list_typing(*, scope: DatasetScope, bundle: ResolvedSourceBundle) -> ListTyping:
    """The list rules' view of ``scope``: field and element types, element access, the database's list support."""
    def element_access(key: object) -> bool:
        path = tuple(getattr(key, "path", ()) or ())
        if not path or scope.population is None:
            return False
        try:
            chain = follow(root=scope.population, path=path, datasets_by_name=bundle.datasets_by_name)
        except AmbiguousRelationshipPathError:
            return False
        return chain is not None and any(e.lateral is not None for e in chain)

    dialect = bundle_dialect(bundle)
    return ListTyping(
        field_type=scope_field_type(scope=scope, bundle=bundle),
        field_element=scope_field_element(scope=scope, bundle=bundle),
        element_access=element_access, native_lists=dialect.native_lists, database=dialect.database_name,
    )


def _type_for_metric_expression(
    *, scope: DatasetScope, bound, bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if scope.population is None:
        return None
    return metric_key_type(dataset=scope.population, key=bound.value_key, bundle=bundle)


def _retyped_after_coercion(
    declared_metrics: List[DeclaredMetric], *, n_grain: int, scope: DatasetScope,
    bundle: ResolvedSourceBundle,
) -> List[DeclaredMetric]:
    """Inferred computed-dimension and metric types re-derived from their coerced keys."""
    return [
        dm.model_copy(update={"datatype": _type_for_metric_expression(scope=scope, bound=dm.bound, bundle=bundle)})
        if dm.is_dimension or (i >= n_grain and not dm.type_is_explicit) else dm
        for i, dm in enumerate(declared_metrics)
    ]


def _joined_field_type(
    *, source_dataset: SlayerDataset, full_name: str, bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    *hops, leaf = drop_root_qualifier(full_name.split("."), source_dataset.spelling, quoted=())
    current = resolved_terminal(
        root=source_dataset, path=tuple(hops),
        datasets_by_name=bundle.datasets_by_name,
    )
    if current is None:
        return None
    col = current.get_field(leaf)
    return col.datatype if col is not None else None


def _type_for_dimension(
    *,
    scope: DatasetScope,
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    if scope.population is None:
        return None
    if "." in full_name:
        return _joined_field_type(
            source_dataset=scope.population, full_name=full_name, bundle=bundle,
        )
    col = scope.population.get_field(full_name)
    return col.datatype if col is not None else None


def _walk_dotted(
    *, source_dataset: SlayerDataset, hops: List[str], bundle: ResolvedSourceBundle,
) -> Optional[Tuple[SlayerDataset, Tuple[str, ...]]]:
    """``(terminal_dataset, canonical hops)`` of ``hops`` from ``source_dataset`` (None on a missing/circular/ambiguous hop)."""
    try:
        return walk_terminal(root=source_dataset, path=hops, datasets_by_name=bundle.datasets_by_name)
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None


def _route_short_form_saved_metric(
    *, host: SlayerDataset, hops: list[str], leaf: str, bundle: ResolvedSourceBundle
) -> Optional[Tuple[SlayerDataset, str]]:
    """``(terminal_dataset, canonical_ref)`` when a ``len==1`` unresolvable prefix
    short-form routes to its full datasource-scoped path, else None.
    Routing triggers only when the first hop resolves to no edge; an adjacent
    parallel pair is a fail-closed ambiguous hop, not a route. This
    resolver also runs in pre-bind raw-rows validation, so it must not route an
    ambiguous hop there — it returns None and lets binding raise the ambiguity."""
    if len(hops) != 1:
        return None
    datasets_by_name = bundle.datasets_by_name
    datasets_by_name.setdefault(host.name, host)
    try:
        if resolve_hop(
            current=host, token=hops[0], datasets_by_name=datasets_by_name,
        ) is not None:
            return None
    except AmbiguousRelationshipPathError:
        return None
    route = dimension_routing.short_form_route_or_none(
        root=host, target_dataset=hops[0], datasets_by_name=datasets_by_name,
    )
    walked = (
        _walk_dotted(source_dataset=host, hops=list(route), bundle=bundle)
        if route is not None else None
    )
    if walked is None:
        return None
    terminal, canonical = walked
    return terminal, ".".join([*canonical, leaf])


def _resolve_saved_metric_ref(
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    expression: str,
) -> Optional[Tuple[SlayerDataset, "DatasetMetric", str]]:
    """Return ``(terminal_dataset, metric, canonical_ref)`` if ``expression`` is a
    bare/dotted saved-metric reference (binder resolution order, short-form
    auto-routing included), else None. ``canonical_ref`` is the canonical dotted
    path when routed or respelled, else the expression text unchanged."""
    if scope.population is None:
        return None
    host = scope.population
    try:
        node = parse_expr(expression)
    except ValueError:
        return None
    if not isinstance(node, (Ref, DottedRef)):
        return None
    parts = drop_root_qualifier(
        (node.name,) if isinstance(node, Ref) else node.parts, host.spelling, quoted=dotted_quoting(node),
    )
    text = ".".join(parts)
    *hops, leaf = parts
    located = (
        _locate_dotted_saved_metric(
            host=host, hops=hops, leaf=leaf, text=text, bundle=bundle,
        )
        if hops else (host, text)
    )
    if located is None:
        return None
    terminal, canonical_ref = located
    mm = terminal.get_metric(leaf)
    return (terminal, mm, canonical_ref) if mm is not None else None


def _locate_dotted_saved_metric(
    *,
    host: SlayerDataset,
    hops: List[str],
    leaf: str,
    text: str,
    bundle: ResolvedSourceBundle,
) -> Optional[Tuple[SlayerDataset, str]]:
    """``(terminal_dataset, canonical_ref)`` for a hop-qualified saved metric."""
    walked = _walk_dotted(source_dataset=host, hops=hops, bundle=bundle)
    if walked is None:
        return _route_short_form_saved_metric(
            host=host, hops=hops, leaf=leaf, bundle=bundle,
        )
    terminal, canonical = walked
    return terminal, (
        text if canonical == tuple(hops) else ".".join((*canonical, leaf))
    )


def _saved_dataset_metric_type(
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    expression: str,
) -> Optional[DataType]:
    ref = _resolve_saved_metric_ref(scope=scope, bundle=bundle, expression=expression)
    return ref[1].datatype if ref is not None else None


def _saved_metric_public_name(
    *,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    expression: str,
) -> Optional[str]:
    """Implicit surfaced name for a bare/dotted saved-metric reference — the
    full routed dotted text (short forms surface under their routed path)."""
    ref = _resolve_saved_metric_ref(scope=scope, bundle=bundle, expression=expression)
    return ref[2] if ref is not None else None


def _will_group_by(query: SlayerQuery) -> bool:
    return bool(query.metrics) or query.distinct_dimension_values


def _reject_computed_dim_name_collision(
    *, name: str, query: SlayerQuery, scope: DatasetScope,
) -> None:
    dataset_collision = None
    if scope.population is not None:
        dataset = scope.population
        if dataset.get_field(name) is not None or dataset.get_metric(name) is not None:
            dataset_collision = dataset.name
    check_computed_dim_name_collision(
        name=name,
        dataset_name=dataset_collision,
        query_metric_collision=any(
            m.alias == name for m in (query.metrics or [])
        ),
    )


def _declared_computed_dimension(
    d: QueryDimension,
    *,
    query: SlayerQuery,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
    dim_alias_map: Optional[Dict[str, ValueKey]] = None,
) -> DeclaredMetric:
    name = d.name
    _reject_computed_dim_name_collision(name=name, query=query, scope=scope)
    parsed = parse_bound_expr(d.field, query.bound_variables)
    bound = bind_expr(
        parsed=parsed, scope=scope, bundle=bundle, allow_metrics=True,
        dimension_alias_map=dim_alias_map,
    )
    # Grain rules live in the checker; invoked here to preserve the bind-time firing point / precedence.
    check_computed_dimension(
        name=name, bound=bound,
        distinct_dimension_values=query.distinct_dimension_values,
    )
    check_list_dimension(name=name, key=bound.value_key, typing=_list_typing(scope=scope, bundle=bundle),
                         will_group_by=_will_group_by(query))
    dim_type = _type_for_metric_expression(scope=scope, bound=bound, bundle=bundle)
    return DeclaredMetric(
        bound=bound,
        declared_name=name,
        public_name=name,
        label=d.label,
        name_is_explicit=name != computed_dimension_name(d.template),
        datatype=dim_type,
        element_type=key_element_type(bound.value_key, scope=scope, bundle=bundle),
        is_dimension=True,
    )


def _declared_metrics_from_query(  # NOSONAR(S3776) — three sequential projection passes (dimensions incl. computed, time dimensions, metrics) building one ordered declared list; each pass is one contract and the order (dims → tds → metrics) is the public projection order the function pins.
    *,
    query: SlayerQuery,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
) -> List[DeclaredMetric]:
    declared: List[DeclaredMetric] = []
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
            ref = d.ref
            if d.is_computed or ref is None:
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
            full = ref.full_name
            # Bind first: a short-form dotted dim auto-routes, and its full routed
            # path (``bound.routed_dotted``) — not the short form typed — drives the
            # result key, type, opaque guard, and description.
            bound = bind_expr(
                parsed=parse_expr(ref.spelling),
                scope=scope,
                bundle=bundle,
            )
            canonical = bound.routed_dotted or full
            check_list_dimension(name=canonical, key=bound.value_key, typing=_list_typing(scope=scope, bundle=bundle),
                                 will_group_by=_will_group_by(query))
            # Opaque-grouping rule lives in the checker; invoked here to preserve the per-dimension firing point.
            check_opaque_grouping_dim(
                full_name=canonical,
                dim_type=_type_for_dimension(scope=scope, full_name=canonical, bundle=bundle),
                will_group_by=_will_group_by(query),
            )
            flat_name = _flatten_dotted(canonical)
            _guard_flatten(flat_name=flat_name, origin=canonical)
            fmt, desc = _format_description_for_dimension(
                scope=scope, full_name=canonical,
            )
            dim_type = _type_for_dimension(
                scope=scope, full_name=canonical, bundle=bundle,
            )
            declared.append(DeclaredMetric(
                bound=bound,
                declared_name=flat_name,
                public_name=flat_name,
                label=d.label,
                datatype=dim_type,
                element_type=key_element_type(bound.value_key, scope=scope, bundle=bundle),
                format=fmt,
                description=desc,
            ))
    # Time dimensions follow dimensions in the public projection. Same-field
    # time dimensions (distinct granularities) get granularity-suffixed public
    # names so their result keys disambiguate; a lone one keeps the
    # granularity-free key.
    bound_tds: List[Tuple[TimeDimension, BoundExpr, str]] = []
    td_types: List[DataType] = []
    for i, td in enumerate(query.time_dimensions or []):
        with stale_spelling_position(f"time_dimensions[{i}]"):
            granularity = _td_granularity(td, bundle=bundle)
            btd = bind_time_dimension(td=td, scope=scope, bundle=bundle, granularity=granularity)
            td_types.append(_check_time_dimension(td=td, btd=btd, scope=scope, bundle=bundle, granularity=granularity))
            field = td.field_ref
            canonical = td.effective_name or btd.bound.routed_dotted or (field.full_name if field else td.field)
            bound_tds.append((td, btd.bound, canonical))
    _assert_equivalent_tds_agree(bound_tds)
    dim_alias_map.update(_time_dimension_aliases(bound_tds))
    _td_source_counts = Counter(_td_source(bound) for _, bound, _ in bound_tds)
    for (td, bound, canonical), td_type in zip(bound_tds, td_types):
        base_flat = _flatten_dotted(canonical)
        assert isinstance(bound.value_key, TimeTruncKey)
        public = (
            f"{base_flat}.{bound.value_key.granularity}"
            if td.alias is None and _td_source_counts[bound.value_key.source] > 1 else base_flat
        )
        _guard_flatten(flat_name=_flatten_dotted(public), origin=canonical)
        declared.append(DeclaredMetric(
            bound=bound,
            declared_name=public,
            public_name=public,
            label=td.label,
            datatype=td_type,
        ))
    seen_metric_keys: Dict[str, Tuple[str, ValueKey, QueryMetric]] = {}
    for i, m in enumerate(query.metrics or []):
        with stale_spelling_position(f"metrics[{i}]"):
            expression = m.metric
            explicit_name = m.alias
            parsed = parse_bound_expr(expression, query.bound_variables)
            bound = bind_expr(
                parsed=parsed, scope=scope, bundle=bundle, allow_metrics=True,
                dimension_alias_map=dim_alias_map,
            )
            # The parsed tree drives text-shape alias derivation, so both spellings
            # of one expression share an alias.
            canonical = _canonical_alias_for_expression(
                expression, bound=bound, parsed=parsed, bundle=bundle, root=root_spelling(scope),
            )
            # A bare/dotted saved-DatasetMetric reference surfaces under the expression text (explicit query name still wins).
            saved_name = _saved_metric_public_name(
                scope=scope, bundle=bundle, expression=expression,
            )
            alias_name = explicit_name or saved_name
            # A substituted expression is named from its template (text path: an aggregate
            # key's alias would embed the value), staying unnamed for dedupe.
            template_name = None if m.template == expression else _canonical_alias_for_expression(
                m.template, parsed=parse_expr(m.template), root=root_spelling(scope),
            )
            declared_name = alias_name or template_name or canonical
            public_name = declared_name
            # Two DIFFERENT values whose DERIVED keys collide would silently share
            # a field (e.g. ``sum(amount - cost)`` vs ``sum(amount + cost)`` both
            # sanitize to ``amount_cost_sum``) — fail loudly; the SAME
            # value merges into one field. Scoped to unnamed entries:
            # explicit-name collisions keep their dedicated declared-more-than-once
            # errors downstream.
            if alias_name is None:
                prior = seen_metric_keys.get(public_name)
                if prior is not None:
                    check_metric_dedupe_collision(
                        prior_expression=prior[0], expression=expression,
                        public_name=public_name,
                        same_key=prior[1] == bound.value_key,
                        same_meta=(m.label, m.datatype) == (prior[2].label, prior[2].datatype),
                    )
                    continue
                seen_metric_keys[public_name] = (expression, bound.value_key, m)
            fmt, desc = _format_description_for_metric_expression(
                scope=scope, bound=bound, bundle=bundle,
            )
            # Type-priority (highest wins): query m.type, saved DatasetMetric.datatype,
            # then aggregation-aware inference.
            explicit_type = m.datatype or _saved_dataset_metric_type(
                scope=scope, bundle=bundle, expression=expression,
            )
            m_type = explicit_type or _type_for_metric_expression(scope=scope, bound=bound, bundle=bundle)
            declared.append(DeclaredMetric(
                bound=bound,
                declared_name=declared_name,
                public_name=public_name,
                label=m.label,
                # Keep the canonical alias when the surfaced name differs, so a filter / ORDER BY naming the aggregate resolves.
                canonical_alias=canonical if alias_name or canonical != public_name else None,
                name_is_explicit=explicit_name is not None,
                datatype=m_type,
                element_type=key_element_type(bound.value_key, scope=scope, bundle=bundle),
                type_is_explicit=explicit_type is not None,
                # A declared Decimal is exact: it keeps the source's native decimal values.
                preserve_native_type=(
                    explicit_type in (None, DataType.DECIMAL)
                    and scope.population is not None
                    and metric_key_preserves_native_type(
                        dataset=scope.population, key=bound.value_key,
                    )
                ),
                format=fmt,
                description=desc,
            ))
    return declared


def _canonical_alias_for_expression(
    expression: str,
    *,
    bound: Optional[BoundExpr] = None,
    parsed: Optional[ParsedExpr] = None,
    bundle: Optional[ResolvedSourceBundle] = None,
    root: Optional[str] = None,
) -> str:
    """Canonical public alias for a metric expression: ``canonical_aggregate_alias`` for an unfiltered
    AggregateKey root, else :func:`expression_text_alias` of ``parsed`` with its ``root`` qualifier dropped (so every
    spelling of one expression derives one alias)."""
    filtered = parsed is not None and any(
        isinstance(n, AggCall) and n.filter is not None for n in walk_parsed(parsed))
    if bound is not None and isinstance(bound.value_key, AggregateKey) and not filtered:
        spelled = bound.value_key if parsed is None or bundle is None else spelled_aggregate_key(
            parsed=parsed, key=bound.value_key, bundle=bundle)
        # stage_formula profile prefixes the relationship path relative to the stage (``count(customers.*)`` → ``customers._count``).
        alias = canonical_aggregate_alias(spelled, profile="stage_expression")
        if alias is not None:
            return alias
        # None means the source exposes no leaf/field name; use the text-shape path.
    if parsed is None:
        return auto_name_from_expression(expression.strip())
    return expression_text_alias(root_relative(parsed, root))


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
        defined=bundle.granularities, where=f"time dimension {td.field!r}",
    )


def _check_field_granularities(*, bundle: ResolvedSourceBundle) -> None:
    """Every dataset field's declared granularity names a known granularity."""
    for dataset in bundle.datasets_by_name.values():
        for field in dataset.fields:
            if field.granularity is not None:
                check_granularity_known(
                    name=str(field.granularity),
                    granularity=resolve_granularity(field.granularity, defined=bundle.granularities),
                    defined=bundle.granularities, where=f"field {field.name!r} of dataset {dataset.name!r}",
                )


def _check_dimension_calls(query: SlayerQuery, *, bundle: ResolvedSourceBundle) -> None:
    """An unnamed single-field call naming no aggregation or granularity fails."""
    for item in query.dimensions or []:
        if item.ref is None and item.alias is None:
            _check_dimension_call(item.field, bundle=bundle)


def _dedupe_equivalent_time_dimensions(
    query: SlayerQuery, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> SlayerQuery:
    """Spellings of one time dimension (one bucket, name, date range and label) project once."""
    tds = query.time_dimensions or []
    if len(tds) < 2:
        return query
    seen: set = set()
    kept: List[TimeDimension] = []
    for i, td in enumerate(tds):
        with stale_spelling_position(f"time_dimensions[{i}]"):
            granularity = _td_granularity(td, bundle=bundle)
            bucket = bind_time_dimension(td=td, scope=scope, bundle=bundle, granularity=granularity).bound.value_key
        identity = (bucket, td.effective_name, tuple(td.date_range or ()), td.label)
        if identity not in seen:
            seen.add(identity)
            kept.append(td)
    return query if len(kept) == len(tds) else query.model_copy(update={"time_dimensions": kept})


def _check_dimension_call(entry: str, *, bundle: ResolvedSourceBundle) -> None:
    node, callee = call_callee(entry)
    if callee is None or functional_call_field(node) is None:
        return
    if normalize_aggregation_name(callee) in BUILTIN_AGGREGATIONS or resolve_granularity(
        callee, defined=bundle.granularities,
    ) is not None:
        return
    check_dimension_call_known(entry=entry, defined=bundle.granularities)


def _build_date_range_filter(
    *,
    td: TimeDimension,
    scope: DatasetScope,
    bundle: ResolvedSourceBundle,
) -> BoundFilter:
    """A TimeDimension's ``date_range`` as one row-phase filter on its untruncated value: a single
    point is ``v = P``; a pair is ``v >= lower`` and/or ``v <= upper`` (a null bound is open)."""
    col_key = bind_time_dimension(td=td, scope=scope, bundle=bundle).bound.value_key
    assert isinstance(col_key, TimeTruncKey)
    col_key = col_key.source
    date_range = td.date_range
    assert date_range  # the caller builds this only for a present date_range
    if len(date_range) == 1:
        assert date_range[0] is not None  # construction rejects a lone null
        comparisons = [TimePointCmpKey(op="=", operand=col_key, point=date_range[0], whole_day=True)]
    else:
        lower, upper = date_range
        comparisons = [
            *([] if lower is None else [TimePointCmpKey(op=">=", operand=col_key, point=lower, whole_day=True)]),
            *([] if upper is None else [TimePointCmpKey(op="<=", operand=col_key, point=upper, whole_day=True)]),
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
    cj: ValueKey, *, grans_by_field: Dict[ValueKey, List[Granularity]],
    field_type, now: datetime, upper_seen: set,
) -> ValueKey:
    """A frame-bound conjunct on a time-dimension field snapped to a bucket boundary; others untouched."""
    if isinstance(cj, ArithmeticKey) and cj.op == "and":
        return cj.map_children(lambda c: _snap_frame_bound(
            c, grans_by_field=grans_by_field, field_type=field_type, now=now, upper_seen=upper_seen,
        ))
    if not is_frame_bound(key=cj, time_fields=grans_by_field.keys()):
        return cj
    assert isinstance(cj, ArithmeticKey)
    field, literal = cj.operands
    op = cj.op
    if field not in grans_by_field:
        field, literal, op = literal, field, _MIRROR[op]
    assert isinstance(literal, LiteralKey) and isinstance(literal.value, date)
    value = _as_datetime(literal.value)
    if op in _UPPER_OPS:
        upper_seen.add(field)
        value = min(value, now)
    snapped = _snapped_value(
        value, grans=grans_by_field[field], is_date=field_type(field) is DataType.DATE,
    )
    return ArithmeticKey(
        op=">=" if op in _LOWER_OPS else "<", operands=(field, LiteralKey(value=snapped)),
    )


def _snap_whole_periods(
    *,
    bound_filters: List[BoundFilter],
    bound_filter_texts: List[Optional[str]],
    n_date_range: int,
    td_keys: List[ValueKey],
    field_type,
    now: datetime,
) -> Tuple[List[BoundFilter], List[Optional[str]], int, List[GranularityConflict]]:
    """``whole_periods_only``: snap every frame bound on a time-dimension field down to the
    earliest bucket boundary, the upper bound clamped to now (added when absent); report
    granularity pairs on one field that do not nest."""
    grans_by_field: Dict[ValueKey, List[Granularity]] = {}
    for td_key in td_keys:
        if isinstance(td_key, TimeTruncKey):
            grans = grans_by_field.setdefault(td_key.source, [])
            if td_key.granularity not in grans:
                grans.append(td_key.granularity)
    conflicts = [
        GranularityConflict(field=dotted_key_display(field), granularities=(a, b))
        for field, grans in grans_by_field.items()
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
            bf.value_key, grans_by_field=grans_by_field, field_type=field_type,
            now=now, upper_seen=upper_seen,
        )
        snapped.append(bf if vk is bf.value_key else bf.model_copy(update={
            "value_key": vk, "referenced_keys": tuple(walk_value_keys(vk)),
        }))
    added = []
    for field, grans in grans_by_field.items():
        if field in upper_seen:
            continue
        upper = _snapped_value(now, grans=grans, is_date=field_type(field) is DataType.DATE)
        vk = ArithmeticKey(op="<", operands=(field, LiteralKey(value=upper)))
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
    """Time dimensions binding to one ``TimeTruncKey`` (equivalent field spellings) must
    agree on ``date_range``/``label``; otherwise their date-range filters and shared
    projection field silently conflict. Construction dedup catches this once the query is
    rooted — this is the bind-time backstop for spellings that only prove equivalent here
    (full identity matching lands in DEV-1925)."""
    seen: Dict[ValueKey, TimeDimension] = {}
    for td, bound, _ in bound_tds:
        prior = seen.setdefault(bound.value_key, td)
        if prior is not td and (prior.date_range, prior.alias, prior.label) != (td.date_range, td.alias, td.label):
            raise GranularityCallError(
                f"Conflicting time dimensions on {td.field!r} at "
                f"{td.granularity} granularity: equivalent spellings must not "
                f"differ in date range, name or label."
            )


def _projected_bucket_td(
    *, expr: str, granularity: Granularity, buckets: List[Tuple[TimeDimension, ValueKey]],
    scope: DatasetScope, bundle: ResolvedSourceBundle,
) -> Optional[TimeDimension]:
    """The projected time dimension bucketing ``expr`` at ``granularity``: its text, else its bound identity."""
    by_text = next((td for td, key in buckets if td.field == expr and _bucket_granularity(key) == granularity), None)
    if by_text is not None:
        return by_text
    try:
        source = bind_expr(parsed=parse_expr(expr), scope=scope, bundle=bundle).value_key
    except SlayerError:
        return None
    wanted = TimeTruncKey(source=source, granularity=granularity)
    return next((td for td, key in buckets if key == wanted), None)


def _bucket_granularity(key: ValueKey) -> Optional[Granularity]:
    return key.granularity if isinstance(key, TimeTruncKey) else None


def _td_source(bound: BoundExpr) -> ValueKey:
    assert isinstance(bound.value_key, TimeTruncKey)
    return bound.value_key.source


def _time_dimension_aliases(bound_tds: List[Tuple[TimeDimension, BoundExpr, str]]) -> Dict[str, ValueKey]:
    """Each time dimension's name (explicit, else an expression's sanitised text) → its bucket,
    for ``partition_by=``; a name two buckets share resolves to neither."""
    named = [(td.effective_name, bound.value_key) for td, bound, _ in bound_tds if td.effective_name]
    counts = Counter(name for name, _ in named)
    return {name: key for name, key in named if counts[name] == 1}


def _host_is_spine(scope: DatasetScope) -> bool:
    return scope.population is not None and is_spine(scope.population)


def _text_reads_spine(text: str, *, scope: DatasetScope) -> bool:
    """Whether Mode-B ``text`` reads the spine: by a dotted path, or bare from a spine root."""
    return any(
        path[0] == TIME_SPINE_DATASET if len(path) > 1 else _host_is_spine(scope)
        for path in text_paths(text, root=root_spelling(scope))
    )


def _reads_spine(source: ValueKey, *, scope: DatasetScope) -> bool:
    """Whether ``source`` reads ``time_spine.timestamp``."""
    host_is_spine = _host_is_spine(scope)
    return any(
        isinstance(k, FieldKey) and k.leaf == TIME_SPINE_FIELD
        and (k.path[-1:] == (TIME_SPINE_DATASET,) or (not k.path and host_is_spine))
        for k in walk_value_keys(source)
    )


def _check_time_dimension(
    *, td: TimeDimension, btd: BoundTimeDimension, scope: DatasetScope,
    bundle: ResolvedSourceBundle, granularity: Granularity,
) -> DataType:
    """The checker's time-dimension rules (P9): row-level and homed (an expression), temporal, re-bucketing; its checked type."""
    field_type = btd.field_type
    if btd.is_expression:
        source = _td_source(btd.bound)
        check_spine_time_dimension(
            offender=td.field if _reads_spine(source, scope=scope) else None, remedy=bucket_remedy(source),
        )
        check_time_dimension_row_level(name=td.field, source=source)
        if scope.population is not None:
            check_time_dimension_home(
                name=td.field,
                crossing=row_expression_crossing(source=source, host_dataset=scope.population, bundle=bundle),
                anchor=".".join((scope.population.name, *source_anchor_path(source))),
            )
        field_type = time_dimension_type(source, field_type=scope_field_type(scope=scope, bundle=bundle))
    check_time_dimension_field(
        name=td.field,
        field_type=field_type,
        requested_granularity=granularity,
        bucketed_leaves=btd.bucketed_leaves,
        is_expression=btd.is_expression,
    )
    assert field_type is not None  # the checker refuses a non-temporal one
    return field_type


def _passes_grain_kwargs(text: str) -> bool:
    """Whether ``text`` passes ``partition_by=`` or ``window=`` to a call."""
    return any(
        name in ("partition_by", "window")
        for node in walk_parsed(parse_expr(text)) if isinstance(node, (AggCall, TransformCall))
        for name, _ in call_keywords(node)
    )


def _td_field_parts(td: TimeDimension, *, root: Optional[str]) -> Optional[Tuple[str, ...]]:
    ref = td.field_ref
    return drop_root_qualifier(ref.parts, root, quoted=ref.quoted_parts) if ref is not None else None


def _reference_parts(text: str, *, root: Optional[str]) -> Tuple[str, ...]:
    """A reference's root-relative path as parsed; unparseable text is one name."""
    try:
        node = parse_expr(text)
    except ValueError:
        return (text,)
    if isinstance(node, DottedRef):
        return drop_root_qualifier(node.parts, root, quoted=dotted_quoting(node))
    if isinstance(node, Ref):
        return (node.name,)
    return (text,)


def _named_td_matches(
    *, tds: List[TimeDimension], target: str, root: Optional[str],
) -> Tuple[List[TimeDimension], List[TimeDimension]]:
    """(name or full-name matches, leaf matches) for ``target`` among ``tds``: a time
    dimension's name, else a field's full or leaf name. Two same-field buckets share
    a full name, so full matches is a list, not a single TD."""
    parts = _reference_parts(target, root=root)
    named = [td for td in tds if len(parts) == 1 and td.effective_name == parts[0]]
    if named:
        return named, []
    full = [td for td in tds if _td_field_parts(td, root=root) == parts]
    if full:
        return full, []
    return [], [td for td in tds if len(parts) == 1 and td.field_ref is not None and td.field_ref.name == parts[0]]


def _host_local_default_td(
    *, tds: List[TimeDimension], default: str, root: Optional[str],
) -> Optional[TimeDimension]:
    # The default points only at the host dataset; prefer a host-local TD over a same-leaf joined one.
    matches = [td for td in tds if _td_field_parts(td, root=root) == (default,)]
    # Same field at several granularities: the passive dataset default can't pick a
    # bucket. Resolve to None (not raise) so no-transform queries still run; a
    # transform that needs the axis then fails via check_time_transforms_resolved.
    return matches[0] if len(matches) == 1 else None


def _resolve_main_time_dimension(
    *,
    query: SlayerQuery,
    dataset: Optional[SlayerDataset],
    root: Optional[str] = None,
) -> Optional[TimeDimension]:
    """Resolve the active time dimension for transform/windowing: 0 TDs → None; 1 → that TD; 2+ → main_time_dimension (full_name then leaf) else the dataset's default_time_dimension else None."""
    tds = list(query.time_dimensions or [])
    if not tds:
        return None
    if len(tds) == 1:
        return tds[0]

    if query.main_time_dimension:
        target = query.main_time_dimension
        # Prefer full-name (more specific) over leaf match.
        full_matches, leaf_matches = _named_td_matches(tds=tds, target=target, root=root)
        if len(full_matches) == 1:
            return full_matches[0]
        if len(full_matches) > 1:
            # Same field, several granularities: a bare field can't pick a bucket.
            # Per-granularity selection (e.g. date_trunc('year', created_at)) lands in DEV-1925.
            raise AmbiguousReferenceError(
                name=target,
                candidates=[
                    f"date_trunc('{td.granularity}', {td.field})"
                    for td in full_matches
                ],
            )
        if len(leaf_matches) == 1:
            return leaf_matches[0]
        if len(leaf_matches) > 1:
            # Multiple TDs share the leaf; force disambiguation via full_name.
            raise AmbiguousReferenceError(
                name=target,
                candidates=[td.field for td in leaf_matches],
            )
        raise UnknownReferenceError(
            name=target,
            scope_kind="TimeDimension",
            scope_summary=(
                f"time_dimensions: "
                f"{[td.field for td in tds]}"
            ),
            suggestion=None,
        )

    default = dataset.effective_default_time_dimension if dataset is not None else None
    if default:
        return _host_local_default_td(tds=tds, default=default, root=root)
    return None
