"""Stage 7a.7 (DEV-1450) — multi-stage source_queries planner.

Orchestrates a list of ``SlayerQuery`` stages into a list of
``PlannedQuery``s, the typed input the SQL generator (stage 7b) will
consume.

Per-stage pipeline:

  raw SlayerQuery → parse (per measure / filter / order) → bind →
  ProjectionPlanner → PlannedQuery (+ emitted StageSchema)

Multi-stage:

* Stages are topologically sorted so each stage appears after the
  siblings it references via ``source_model``.
* Downstream stages bind against the upstream ``StageSchema`` (P6) —
  flat namespace, no dotted-join walking. ``IllegalScopeReferenceError``
  on dotted refs (DEV-1449).
* Each stage's ``StageSchema`` columns use the user-supplied ``name``
  (or canonical alias) as the column ``name`` (DEV-1448).

Dormant in 7a — no engine wiring. Stage 7b's engine cutover flips
``engine.execute`` / ``engine.save_model`` over to ``plan_stages``.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, Optional, Union

from slayer.core.errors import AmbiguousReferenceError, UnknownReferenceError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    normalize_scalar,
)
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.core.refs import canonical_agg_name
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import (
    BoundExpr as BinderBoundExpr,
    BoundFilter,
    bind_expr,
    bind_filter,
    bind_time_dimension,
    walk_value_keys,
)
from slayer.engine.cross_model_planner import (
    CrossModelPlanner,
    HostFilterRouting,
    IsolatedCteCrossModelPlanner,
)
from slayer.engine.measure_expansion import expand_model_measures
from slayer.engine.planned import (
    BoundExpr as PlannedBoundExpr,
    FilterPhase,
    OrderEntry,
    PlannedQuery,
    TransformLayer,
    ValueSlot,
)
from slayer.engine.planning import (
    DeclaredMeasure,
    OrderSpec,
    ProjectionPlanner,
    _iter_slot_deps,
    filter_referenced_slot_ids,
    lower_sugar_transforms,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import parse_expr
from slayer.engine.column_expansion import _is_trivial_base
from slayer.sql.sql_expr import has_window_function
from slayer.sql.sql_predicate import parse_sql_predicate


__all__ = ["plan_query", "plan_stages"]


# Stage 7b.10 — TIME_NEEDING transform ops that require a resolvable
# time dimension to render their OVER ``ORDER BY``. Mirrors the legacy
# ``TIME_TRANSFORMS`` set at ``slayer/core/formula.py:33``.
_TIME_NEEDING_TRANSFORM_OPS = frozenset({
    "cumsum",
    "change",
    "change_pct",
    "time_shift",
    "first",
    "last",
    "lag",
    "lead",
    "consecutive_periods",
})


def _attach_time_keys(
    key: ValueKey, *, td_key: TimeTruncKey,
) -> ValueKey:
    """Walk ``key``; for every ``TransformKey`` whose op needs a time
    dimension and whose ``time_key`` is ``None``, return a copy with
    ``time_key=td_key``. Identity-preserving when nothing changes.

    Mirrors ``lower_sugar_transforms``' walker shape so identity
    semantics line up: nested TransformKey/ArithmeticKey/ScalarCallKey/
    BetweenKey trees are rebuilt only on the path containing a patch.
    """
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
    return key


def _find_unresolved_time_needing_op(key: ValueKey) -> Optional[str]:
    """Return the op name of the first time-needing TransformKey reached
    that has ``time_key is None``, or ``None`` if every time-needing
    transform in the tree is resolved.
    """
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
    return None


def plan_query(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    cross_model_planner: Optional[CrossModelPlanner] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
) -> PlannedQuery:
    """Compile one ``SlayerQuery`` into a typed ``PlannedQuery``.

    ``scope`` defaults to a ``ModelScope`` over ``bundle.source_model``;
    pass an explicit ``StageSchema`` to bind against an upstream stage.
    ``stage_schemas`` is a name → StageSchema map used by
    ``plan_stages`` to wire multi-stage references.
    """
    stage_schemas = stage_schemas or {}
    cross_model_planner = (
        cross_model_planner or IsolatedCteCrossModelPlanner()
    )

    if scope is None:
        source = query.source_model
        if isinstance(source, str) and source in stage_schemas:
            scope = stage_schemas[source]
        else:
            scope = ModelScope(source_model=bundle.source_model)

    declared_measures = _declared_measures_from_query(
        query=query, scope=scope, bundle=bundle,
    )

    # DEV-1450 stage 7b.8 — alias lookup for ORDER BY resolution.
    # A user-supplied order column may reference the declared measure
    # by its public name (user-supplied ``name``), declared name
    # (canonical OR user), or canonical alias. The order pass below
    # checks this map BEFORE falling back to ``bind_expr`` so refs to
    # aggregate aliases like ``amount_sum`` resolve through the
    # projection registry rather than against model scope (where they
    # don't exist as columns).
    declared_alias_to_bound: Dict[str, BinderBoundExpr] = {}
    for dm in declared_measures:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                declared_alias_to_bound.setdefault(alias, dm.bound)

    # DEV-1450 stage 7b.9 — filter list construction in legacy WHERE
    # order: date_range filters first, then SlayerModel.filters
    # (Mode-A SQL), then user query filters (Mode-B DSL). The legacy
    # generator emits date_range BEFORE iterating ``enriched.filters``
    # (slayer/sql/generator.py:2527 vs :2540), and ``enriched.filters``
    # itself is model filters then query filters (enrichment.py:1192).
    #
    # ``bound_filters`` carries the typed-BoundFilter entries (date_range
    # + query filters) for the cross-model routing and projection
    # planner passes. Model filters bypass ``bound_filters`` since
    # they're Mode-A SQL text without a typed value-key — they're
    # appended directly to ``filters_by_phase`` between the two
    # bound-filter buckets.
    bound_filters: List[BoundFilter] = []
    text_filter_entries: List[FilterPhase] = []

    # 1. date_range filters (one per TD with a 2-element date_range)
    for td in (query.time_dimensions or []):
        if not td.date_range or len(td.date_range) != 2:
            continue
        if not isinstance(scope, ModelScope):
            continue
        bf = _build_date_range_filter(td=td, scope=scope, bundle=bundle)
        bound_filters.append(bf)
    n_date_range = len(bound_filters)

    # 2. SlayerModel.filters — Mode-A SQL, always-applied WHERE.
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        for j, mf in enumerate(scope.source_model.filters or []):
            text_filter_entries.append(_validate_model_filter(
                mf=mf, idx=j, model=scope.source_model,
            ))

    # 3. user query filters (Mode-B DSL).
    for f in (query.filters or []):
        if not isinstance(f, str):
            continue
        bf = bind_filter(parse_expr(f), scope=scope, bundle=bundle)
        bound_filters.append(bf)

    order_specs = []
    for o in (query.order or []):
        col_name = o.column.name
        # Prefer declared-measure alias resolution over model-scope
        # binding (DEV-1450 stage 7b.8 — gap fix): aggregate canonical
        # aliases like ``amount_sum`` are not columns on the model, so
        # ``bind_expr`` would raise. The alias map covers user-supplied
        # ``name``, canonical alias, and the declared name itself.
        if col_name in declared_alias_to_bound:
            bo = declared_alias_to_bound[col_name]
        else:
            bo = bind_expr(parse_expr(col_name), scope=scope, bundle=bundle)
        order_specs.append(OrderSpec(bound=bo, direction=o.direction))

    # Stage 7b.10 — attach the active TD as ``time_key`` on every
    # time-needing TransformKey (cumsum / lag / lead / first / last /
    # time_shift / consecutive_periods / change / change_pct) whose
    # binder-output left ``time_key`` as ``None``. Closes the 7b.4
    # carry-over gap: ``_bind_transform`` does not have query / scope
    # context to resolve the TD, so the planner does it here after all
    # binding completes. Validation mirrors legacy
    # ``enrichment.py:564-569`` -- any time-needing transform with no
    # resolvable TD raises with the legacy phrase.
    active_td_key: Optional[TimeTruncKey] = None
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        active_td = _resolve_main_time_dimension(
            query=query, model=scope.source_model,
        )
        if active_td is not None:
            active_td_bound = bind_time_dimension(
                active_td, scope=scope, bundle=bundle,
            )
            atd_key = active_td_bound.value_key
            assert isinstance(atd_key, TimeTruncKey)
            active_td_key = atd_key

    if active_td_key is not None:
        declared_measures = [
            DeclaredMeasure(
                bound=BinderBoundExpr(
                    value_key=_attach_time_keys(
                        dm.bound.value_key, td_key=active_td_key,
                    ),
                ),
                declared_name=dm.declared_name,
                public_name=dm.public_name,
                label=dm.label,
                canonical_alias=dm.canonical_alias,
            )
            for dm in declared_measures
        ]
        bound_filters = [
            BoundFilter(
                value_key=_attach_time_keys(
                    bf.value_key, td_key=active_td_key,
                ),
                phase=bf.phase,
                referenced_keys=tuple(
                    walk_value_keys(
                        _attach_time_keys(
                            bf.value_key, td_key=active_td_key,
                        ),
                    ),
                ),
            )
            for bf in bound_filters
        ]
        order_specs = [
            OrderSpec(
                bound=BinderBoundExpr(
                    value_key=_attach_time_keys(
                        spec.bound.value_key, td_key=active_td_key,
                    ),
                ),
                direction=spec.direction,
            )
            for spec in order_specs
        ]

    # Validation: any time-needing transform that still has
    # ``time_key=None`` after patching means there was no resolvable TD.
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

    # Sugar lowering for ``change`` / ``change_pct`` runs AFTER the
    # patching pass so the desugared ``time_shift`` inherits the patched
    # ``time_key`` (DEV-1446 identity preservation still holds — the
    # inner AggregateKey instance is not rebuilt by lowering).
    declared_measures = [
        DeclaredMeasure(
            bound=BinderBoundExpr(
                value_key=lower_sugar_transforms(dm.bound.value_key),
            ),
            declared_name=dm.declared_name,
            public_name=dm.public_name,
            label=dm.label,
            canonical_alias=dm.canonical_alias,
        )
        for dm in declared_measures
    ]
    bound_filters = [
        BoundFilter(
            value_key=lower_sugar_transforms(bf.value_key),
            phase=bf.phase,
            referenced_keys=tuple(
                walk_value_keys(lower_sugar_transforms(bf.value_key)),
            ),
        )
        for bf in bound_filters
    ]
    order_specs = [
        OrderSpec(
            bound=BinderBoundExpr(
                value_key=lower_sugar_transforms(spec.bound.value_key),
            ),
            direction=spec.direction,
        )
        for spec in order_specs
    ]

    source_col_names = _source_column_names(scope)
    host_model_name = _host_model_name(scope)

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

    # Build filters_by_phase in legacy WHERE order:
    #   1. date_range bound filters (bound_filters[:n_date_range])
    #   2. model.filters (text_filter_entries)
    #   3. user query bound filters (bound_filters[n_date_range:])
    # bound_filter_ids preserves the mapping back to bound_filters for
    # the cross-model routing pass that follows (text_filter_entries
    # are excluded — model filters never feed cross-model routing).
    filters_by_phase: List[FilterPhase] = []
    bound_filter_ids: List[str] = []
    for i, bf in enumerate(bound_filters[:n_date_range]):
        fid = f"f{i}"
        filters_by_phase.append(
            FilterPhase(
                id=fid, phase=bf.phase, text=None,
                expression=PlannedBoundExpr(value_key=bf.value_key),
            ),
        )
        bound_filter_ids.append(fid)
    filters_by_phase.extend(text_filter_entries)
    for i, bf in enumerate(bound_filters[n_date_range:], start=n_date_range):
        fid = f"f{i}"
        filters_by_phase.append(
            FilterPhase(
                id=fid, phase=bf.phase, text=None,
                expression=PlannedBoundExpr(value_key=bf.value_key),
            ),
        )
        bound_filter_ids.append(fid)
    # Stage 7b.5 — cross-model planner wiring. For every aggregate slot
    # whose source carries a non-empty join path (cross-model agg-ref
    # like ``customers.revenue:sum``), invoke the cross_model_planner
    # to produce a CrossModelAggregatePlan with explicit WHERE/HAVING/
    # target_model_filters routes. HostFilterRouting records carry the
    # post-projection slot ids each filter references (via
    # filter_referenced_slot_ids — Codex HIGH #3/#4 fold-in).
    # host_filter_routings only carries entries that have a typed
    # BoundFilter (date_range + user filters). Model.filters (text-only)
    # are always row-phase host-local WHERE and never need to be routed
    # to a cross-model CTE — they're skipped here.
    host_filter_routings: List[HostFilterRouting] = []
    for fid, bf in zip(bound_filter_ids, bound_filters):
        host_filter_routings.append(HostFilterRouting(
            filter_id=fid,
            phase=bf.phase,
            referenced_slot_ids=sorted(filter_referenced_slot_ids(
                bf, projection.registry,
            )),
            text=None,
        ))

    cross_model_plans = []
    host_slots_for_classifier = projection.registry.slots
    for slot in agg_slots:
        key = slot.key
        if not isinstance(key, AggregateKey):
            continue
        agg_path = getattr(key.source, "path", ())
        if not agg_path:
            continue
        plan = cross_model_planner.plan(
            aggregate_slot_id=slot.id,
            aggregate_key=key,
            bundle=bundle,
            host_slots=host_slots_for_classifier,
            host_filters=host_filter_routings,
            public_alias=slot.public_name,
            hidden=slot.hidden,
        )
        cross_model_plans.append(plan)

    order_entries = []
    for spec in order_specs:
        sid = projection.registry.find_by_key(spec.bound.value_key)
        if sid is not None:
            order_entries.append(
                OrderEntry(slot_id=sid, direction=spec.direction),
            )

    transform_layers = _emit_transform_layers(slots=projection.registry.slots)
    stage_schema = _emit_stage_schema(
        query=query, projection=projection,
    )
    source_relation = (
        query.source_model
        if isinstance(query.source_model, str)
        else host_model_name
    )

    # Stage 7b.10 — surface the active TD's slot id so the generator can
    # render ``ORDER BY <td-alias>`` in OVER clauses without re-walking
    # the model graph. ``None`` when there is no TD (validation already
    # ran above; we only reach here if no time-needing transform exists).
    active_td_slot_id = (
        projection.registry.find_by_key(active_td_key)
        if active_td_key is not None
        else None
    )

    return PlannedQuery(
        source_relation=source_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        cross_model_aggregate_plans=cross_model_plans,
        combined_expression_slots=combined_slots,
        transform_layers=transform_layers,
        filters_by_phase=filters_by_phase,
        projection=projection.public_projection,
        order=order_entries,
        limit=query.limit,
        offset=query.offset,
        stage_schema=stage_schema,
        active_time_dimension_slot_id=active_td_slot_id,
    )


def plan_stages(
    *,
    queries: List[SlayerQuery],
    bundle: ResolvedSourceBundle,
    cross_model_planner: Optional[CrossModelPlanner] = None,
) -> List[PlannedQuery]:
    """Plan a multi-stage DAG. Topo sort, then plan each stage."""
    if len(queries) == 1:
        return [plan_query(
            query=queries[0],
            bundle=bundle,
            cross_model_planner=cross_model_planner,
        )]
    ordered = _topo_sort(queries)
    stage_schemas: Dict[str, StageSchema] = {}
    results: List[PlannedQuery] = []
    for q in ordered:
        planned = plan_query(
            query=q,
            bundle=bundle,
            cross_model_planner=cross_model_planner,
            stage_schemas=stage_schemas,
        )
        results.append(planned)
        if q.name and planned.stage_schema is not None:
            stage_schemas[q.name] = planned.stage_schema
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _declared_measures_from_query(
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> List[DeclaredMeasure]:
    declared: List[DeclaredMeasure] = []
    for d in (query.dimensions or []):
        full = d.full_name
        bound = bind_expr(parse_expr(full), scope=scope, bundle=bundle)
        flat_name = _flatten_dotted(full)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=d.label,
        ))
    # Time dimensions follow dimensions in the public projection — matches
    # the legacy ``user_projection`` order (dims, then time dims, then
    # measures).
    for td in (query.time_dimensions or []):
        full = td.dimension.full_name
        bound = bind_time_dimension(td, scope=scope, bundle=bundle)
        flat_name = _flatten_dotted(full)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=td.label,
        ))
    for m in (query.measures or []):
        formula = m.formula
        explicit_name = m.name
        parsed = parse_expr(formula)
        # DEV-1450 stage 7b.8 — pre-bind ModelMeasure expansion. A bare
        # ``Ref`` whose name matches a saved ``ModelMeasure`` on the
        # host model is rewritten to the measure's formula AST so the
        # binder resolves the underlying columns. Only applies against
        # ModelScope (downstream stages bind against StageSchema and
        # don't expose saved measures).
        if isinstance(scope, ModelScope) and scope.source_model is not None:
            parsed = expand_model_measures(
                expr=parsed,
                model=scope.source_model,
            )
        bound = bind_expr(parsed, scope=scope, bundle=bundle)
        # Stage 7b.10: sugar-lowering of ``change`` / ``change_pct`` now
        # runs in ``plan_query`` AFTER time-key patching, so the inner
        # ``time_shift`` inherits a patched ``time_key`` instead of
        # ``None``. Identity-preservation for the inner aggregate slot
        # (DEV-1446) still holds — ``lower_sugar_transforms`` keeps the
        # inner ``AggregateKey`` instance unchanged.
        canonical = _canonical_alias_for_formula(formula)
        declared_name = explicit_name or canonical
        public_name = explicit_name or canonical
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=declared_name,
            public_name=public_name,
            label=m.label,
            canonical_alias=canonical if explicit_name else None,
        ))
    return declared


def _topo_sort(queries: List[SlayerQuery]) -> List[SlayerQuery]:
    """Kahn's algorithm: order stages so each appears after its
    siblings it references via ``source_model``.

    Raises ``ValueError`` on:
    * duplicate stage names,
    * a cycle in the dependency graph.

    Stages without a ``name`` (typically the final / root) are appended
    last in input order.
    """
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
        src = q.source_model
        if isinstance(src, str) and src in by_name and src != q.name:
            in_degree[q.name] += 1
            edges[src].append(q.name)
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
    return name.replace(".", "__")


def _canonical_alias_for_formula(formula: str) -> str:
    """Compute the canonical public alias for a measure formula.

    Mirrors ``canonical_agg_name`` for the simple ``<source>:<agg>``
    shape. For arbitrary formulas (transforms, arithmetic), sanitise
    the formula text so the alias remains a valid identifier.
    """
    text = formula.strip()
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
    query: SlayerQuery,
    projection,
) -> StageSchema:
    """Build the StageSchema from the projection plan.

    Only public slots appear (hidden slots are trimmed). One column per
    occurrence in ``public_projection`` so multi-alias declarations
    (same key with two ``name``s) emit one column per alias rather
    than two copies of ``public_aliases[0]``.
    """
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
        columns.append(StageColumn(
            name=alias,
            sql_alias=alias,
            public_alias=alias,
            type=slot.type,
            label=slot.label,
            hidden=False,
        ))
    relation_name = query.name or "(unnamed_stage)"
    return StageSchema(relation_name=relation_name, columns=columns)


def _emit_transform_layers(*, slots: List[ValueSlot]) -> List[TransformLayer]:
    """One TransformLayer per ``TransformKey`` slot, emitted in
    dependency order (innermost transform first).

    Nested transforms (``cumsum(change(amount:sum))``) require
    per-slot layers so the generator can render the inner window /
    self-join before the outer one consumes it. Repeated ops at
    different nesting levels stay in separate layers; collapsing by
    op would lose the ordering invariant.

    Per-slot transform metadata (partition_keys, time_key, args,
    kwargs) lives on the slot's ``key`` (TransformKey); the generator
    slices read it from there.
    """
    transform_slots = [
        s for s in slots if isinstance(s.key, TransformKey)
    ]
    # Topological order: a slot whose TransformKey.input references
    # another slot's key must come AFTER that other slot. Walk
    # `_iter_slot_deps` to discover dependencies among transform slots.
    slot_by_key = {s.key: s for s in transform_slots}
    in_degree = {s.id: 0 for s in transform_slots}
    deps_of: Dict[str, List[str]] = {s.id: [] for s in transform_slots}
    for s in transform_slots:
        # The slot's transform depends on whatever transform slots
        # appear inside its ValueKey tree (e.g. cumsum(change(...))'s
        # cumsum slot depends on the change/time_shift slot).
        for dep in _iter_slot_deps(s.key):
            if dep is s.key or not isinstance(dep, TransformKey):
                continue
            dep_slot = slot_by_key.get(dep)
            if dep_slot is None:
                continue
            deps_of[dep_slot.id].append(s.id)
            in_degree[s.id] += 1
    # Kahn's algorithm: start from independent layers.
    ready = [s.id for s in transform_slots if in_degree[s.id] == 0]
    ordered_ids: List[str] = []
    while ready:
        nxt = ready.pop(0)
        ordered_ids.append(nxt)
        for child in deps_of[nxt]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
    # Fallback: any remaining slots (shouldn't happen with the typed
    # pipeline's identity-via-key, but guard) get appended in input order.
    seen = set(ordered_ids)
    for s in transform_slots:
        if s.id not in seen:
            ordered_ids.append(s.id)
    by_id = {s.id: s for s in transform_slots}
    return [
        TransformLayer(op=by_id[sid].key.op, slot_ids=[sid])
        for sid in ordered_ids
    ]


# ---------------------------------------------------------------------------
# Stage 7b.3c — date_range → filter + main-TD disambiguation
# ---------------------------------------------------------------------------


def _validate_model_filter(
    *,
    mf: str,
    idx: int,
    model: SlayerModel,
) -> FilterPhase:
    """Validate a ``SlayerModel.filters`` entry and emit a text-only
    ``FilterPhase`` for it.

    Replicates legacy validation (``slayer/engine/enrichment.py:1138-1219``):

    * ``parse_sql_predicate`` rejects DSL constructs (colon aggregation,
      transform calls) and raw ``OVER(...)`` window functions.
    * Reject references to a ``ModelMeasure`` declared on the same
      model — model filters are WHERE-clause SQL, can't reference
      aggregates (legacy ``enrichment.py:1147-1153``).
    * Reject references to a column whose ``Column.sql`` contains a
      window function (legacy ``enrichment.py:1205-1219``).
    * DEV-1450 stage 7b.9 deliberately defers references to derived
      ``Column.sql`` (non-windowed) columns to a follow-up — legacy
      inlines the column's SQL via ``resolve_filter_columns``; the
      new path only knows bare-name qualification at render time, so
      a derived-column reference would produce wrong SQL. Raises
      ``NotImplementedError`` explicitly so the failure mode is clear.
    """
    parsed = parse_sql_predicate(mf)
    measure_names = {m.name for m in (model.measures or [])}
    windowed_columns = {
        c.name for c in model.columns
        if c.sql and has_window_function(c.sql)
    }
    # Non-trivial derived columns (Column.sql is set AND not a bare
    # identifier remap like ``sql="amount"``). Trivial base columns
    # qualify the same as bare-table columns — legacy semantics
    # (`_is_trivial_base` in slayer/engine/column_expansion.py).
    derived_columns = {
        c.name for c in model.columns
        if c.sql
        and not has_window_function(c.sql)
        and not _is_trivial_base(column=c)
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
        if col in derived_columns:
            raise NotImplementedError(
                f"DEV-1450 stage 7b.9: model filter {mf!r} references "
                f"derived column {col!r} (Column.sql is set with a "
                f"non-trivial expression). The typed pipeline does not "
                f"yet inline derived-column SQL inside model.filters; a "
                f"follow-up will bridge this. For now, factor the "
                f"predicate into a query-level filter or reference the "
                f"underlying table column directly."
            )
    return FilterPhase(
        id=f"mf{idx}",
        phase=Phase.ROW,
        text=mf,
        text_columns=tuple(parsed.columns),
        expression=None,
    )


def _build_date_range_filter(
    *,
    td: TimeDimension,
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
) -> BoundFilter:
    """Build a row-phase ``BoundFilter`` from a ``TimeDimension``'s
    ``date_range``.

    The predicate binds against the bare underlying ``ColumnKey``
    (not the ``TimeTruncKey``) so generator slice 7b.11 can apply the
    filter to the outer projection while the shifted self-join CTE
    reads raw data. Shape:

        BetweenKey(column=col, low=start, high=end)

    Inclusive on both sides — matches legacy ``column BETWEEN start
    AND end``. The typed BetweenKey lets the SQL generator emit
    ``exp.Between`` rather than ``col >= start AND col <= end``,
    closing the syntactic parity gap with the legacy generator
    (DEV-1450 stage 7b.9).

    Bound literals are normalised via ``normalize_scalar``; strings
    pass through unchanged.
    """
    full = td.dimension.full_name
    parsed = parse_expr(full)
    bound_col_expr = bind_expr(parsed, scope=scope, bundle=bundle)
    col_key = bound_col_expr.value_key
    if not isinstance(col_key, ColumnKey):
        raise ValueError(
            f"date_range filter for TimeDimension {full!r} expected a "
            f"ColumnKey; got {type(col_key).__name__}."
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
    """Resolve the active time dimension for transform / windowing.

    * 0 TDs → ``None``.
    * 1 TD → that TD (``query.main_time_dimension`` is ignored —
      matches legacy semantics).
    * 2+ TDs:
      * ``query.main_time_dimension`` set → match by ``full_name``
        first, then by ``leaf``; raise ``UnknownReferenceError`` if
        neither matches.
      * Else ``model.default_time_dimension`` set → match by leaf;
        return ``None`` if it doesn't match a TD in this query
        (legacy graceful no-op — the default points at a column the
        user didn't include in this query's time_dimensions).
      * Else → ``None``.
    """
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
            # Ambiguous: multiple TDs share the same leaf (e.g.
            # ``customers.created_at`` and ``payments.created_at``).
            # Force the user to disambiguate via full_name.
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
        # Legacy ``_resolve_time_alias`` returns
        # ``f"{model.name}.{default_time_dimension}"``, which only points
        # at the host model — never at a joined TD. Preserve that: prefer
        # a host-local TD (``td.dimension.model is None``) over any
        # joined TD that happens to share the leaf name.
        for td in tds:
            if td.dimension.model is None and td.dimension.name == default:
                return td
    return None
