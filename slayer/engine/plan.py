"""Planning orchestration (DEV-1871 G16): elaborate → compile.

``plan_query`` is the planning door — it elaborates (bind + type + typing
environment) and compiles the result; ``plan_stages`` orders and threads a
multi-stage DAG through it.
"""

from __future__ import annotations

from typing import Dict, Hashable, List, Optional, Tuple, Union

from slayer.core.query import ModelExtension, SlayerQuery
from slayer.core.scope import ModelScope, StageSchema, stale_spelling_stage
from slayer.engine.compile import compile_query
from slayer.engine.compile.stages import _topo_sort
from slayer.engine.elaborate import elaborate_query
from slayer.ir.planned import PlannedQuery
from slayer.ir.prebound import PreboundQuery
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    apply_extension_overlay,
    source_name_if_sibling,
    stage_bundle_with_siblings,
    synthetic_model_from_stage_schema,
)

__all__ = [
    "plan_query",
    "plan_stages",
]


def plan_query(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: Optional[PreboundQuery] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Plan one user-authored query stage: elaborate, then compile."""
    elaborated = elaborate_query(
        query=query,
        bundle=bundle,
        scope=scope,
        stage_schemas=stage_schemas,
        prebound=prebound,
    )
    return compile_query(
        elaborated=elaborated,
        producer_registry=producer_registry,
    )


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
    sib = source_name_if_sibling(spec=src, sibling_names=sibling_names)

    # 1. ModelExtension OVER a sibling: overlay the extra columns onto a synthetic sibling model.
    if sib is not None and isinstance(src, ModelExtension):
        base = synthetic_model_from_stage_schema(
            name=sib, schema=stage_schemas[sib], data_source=data_source,
        )
        overlaid = apply_extension_overlay(base, src)
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
    for index, q in enumerate(ordered):
        scope, stage_bundle = _stage_scope_and_bundle(
            query=q,
            bundle=bundle,
            stage_schemas=stage_schemas,
            data_source=data_source,
            is_root=q is root,
        )
        with stale_spelling_stage(f"stage {q.name!r}" if q.name else f"stages[{index}]"):
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
