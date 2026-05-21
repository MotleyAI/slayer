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

from slayer.core.keys import Phase
from slayer.core.query import SlayerQuery
from slayer.core.refs import canonical_agg_name
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import bind_expr, bind_filter
from slayer.engine.cross_model_planner import (
    CrossModelPlanner,
    IsolatedCteCrossModelPlanner,
)
from slayer.engine.planned import (
    FilterPhase,
    OrderEntry,
    PlannedQuery,
    ValueSlot,
)
from slayer.engine.planning import (
    DeclaredMeasure,
    OrderSpec,
    ProjectionPlanner,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import parse_expr


__all__ = ["plan_query", "plan_stages"]


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

    bound_filters = []
    for f in (query.filters or []):
        if not isinstance(f, str):
            continue
        bf = bind_filter(parse_expr(f), scope=scope, bundle=bundle)
        bound_filters.append(bf)

    order_specs = []
    for o in (query.order or []):
        col_name = o.column.name
        bo = bind_expr(parse_expr(col_name), scope=scope, bundle=bundle)
        order_specs.append(OrderSpec(bound=bo, direction=o.direction))

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

    filters_by_phase = [
        FilterPhase(id=f"f{i}", phase=bf.phase, text=None)
        for i, bf in enumerate(bound_filters)
    ]
    order_entries = []
    for spec in order_specs:
        sid = projection.registry.find_by_key(spec.bound.value_key)
        if sid is not None:
            order_entries.append(
                OrderEntry(slot_id=sid, direction=spec.direction),
            )

    stage_schema = _emit_stage_schema(
        query=query, projection=projection,
    )
    source_relation = (
        query.source_model
        if isinstance(query.source_model, str)
        else host_model_name
    )

    return PlannedQuery(
        source_relation=source_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        combined_expression_slots=combined_slots,
        filters_by_phase=filters_by_phase,
        projection=projection.public_projection,
        order=order_entries,
        limit=query.limit,
        offset=query.offset,
        stage_schema=stage_schema,
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
    for m in (query.measures or []):
        formula = m.formula
        explicit_name = m.name
        bound = bind_expr(parse_expr(formula), scope=scope, bundle=bundle)
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
