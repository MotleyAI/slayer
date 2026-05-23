"""Stage 2 (DEV-1450) — ResolvedSourceBundle: eagerly resolved query inputs (P11).

The orchestrator builds this once at the top of execute; the binder reads
from it purely. No ContextVar machinery, no callback re-resolution — the
binder is provably scope-only because everything it needs is in the bundle.

Contents (per DEV-1450 spec):
- Source model (the host of the query).
- All other referenced models (joined targets, sibling stage hosts).
- Inline ``ModelExtension`` overlays (extra columns / measures / joins).
- Named query siblings (raw ``SlayerQuery``s; the stage planner compiles
  each to its own ``StageSchema`` as siblings are traversed in
  topological order).
- ``query_variables`` (merged precedence: runtime > stage > outer > model).
- Datasource hint (the ``data_source=`` kwarg that wins over the priority
  list).

Per I2 of the DEV-1450 execution plan, ``source_model`` is ``Optional``
from day one. DEV-1450's binder asserts ``source_model is not None``;
the type-level optionality is the extension point for a future
anchor-less mode.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.variables import merge_query_variables

if TYPE_CHECKING:
    from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_model: Optional[SlayerModel] = None
    referenced_models: List[SlayerModel] = Field(default_factory=list)
    inline_extensions: List[ModelExtension] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    query_variables: Dict[str, Any] = Field(default_factory=dict)
    datasource_hint: Optional[str] = None

    def get_referenced_model(self, name: str) -> Optional[SlayerModel]:
        """Linear lookup by name. The list is small (handful of joined
        models per query), so the O(n) scan is fine.
        """
        for m in self.referenced_models:
            if m.name == name:
                return m
        return None


# Anything accepted as ``SlayerQuery.source_model``.
SourceSpec = Union[str, SlayerModel, ModelExtension, Dict[str, Any]]


def _apply_extension_overlay(
    base: SlayerModel, ext: ModelExtension
) -> SlayerModel:
    """Extend ``base`` with the extra columns / measures / joins of ``ext``.

    Mirrors the ``ModelExtension`` branch of
    ``SlayerQueryEngine._resolve_query_model`` so the typed pipeline sees the
    same overlaid model the legacy path produced.
    """
    extra_cols = [
        Column.model_validate(c) if isinstance(c, dict) else c
        for c in (ext.columns or [])
    ]
    extra_measures = [
        ModelMeasure.model_validate(m) if isinstance(m, dict) else m
        for m in (ext.measures or [])
    ]
    extra_joins = [
        ModelJoin.model_validate(j) if isinstance(j, dict) else j
        for j in (ext.joins or [])
    ]
    return base.model_copy(
        update={
            "columns": list(base.columns) + extra_cols,
            "measures": list(base.measures) + extra_measures,
            "joins": list(base.joins) + extra_joins,
        }
    )


def _follow_sibling_chain(
    spec: SourceSpec, named_queries: Dict[str, SlayerQuery]
) -> SourceSpec:
    """Resolve a ``source_model`` that points at a named sibling stage down
    to the real base spec it ultimately reads from.

    ``plan_query`` binds every non-sibling-sourced stage against
    ``bundle.source_model`` (the StageSchema branch only fires when the
    ``source_model`` string matches a sibling). So the bundle's
    ``source_model`` must be the real base the chain bottoms out at — not the
    sibling name. A cycle raises ``ValueError`` (mirrors the legacy
    ``_resolve_model`` circular-reference guard); returns the first
    non-sibling spec otherwise.
    """
    seen: List[str] = []
    while isinstance(spec, str) and spec in named_queries:
        if spec in seen:
            chain = " -> ".join([*seen, spec])
            raise ValueError(
                f"Circular reference detected in source_queries DAG: {chain}"
            )
        seen.append(spec)
        spec = named_queries[spec].source_model
    return spec


async def _resolve_source_spec(
    spec: SourceSpec,
    *,
    storage: "StorageBackend",
    data_source: Optional[str],
) -> SlayerModel:
    """Resolve any ``source_model`` spec to a concrete ``SlayerModel``.

    Storage-only and read-only (P11). Handles the four input shapes the
    public API accepts: stored-model name, inline ``SlayerModel``,
    ``ModelExtension`` overlay, and the dict forms of both.
    """
    if isinstance(spec, SlayerModel):
        return spec
    if isinstance(spec, ModelExtension):
        base = await storage.get_model(spec.source_name, data_source=data_source)
        if base is None:
            raise ValueError(f"Model '{spec.source_name}' not found")
        return _apply_extension_overlay(base, spec)
    if isinstance(spec, str):
        model = await storage.get_model(spec, data_source=data_source)
        if model is None:
            raise ValueError(f"Model '{spec}' not found")
        return model
    if isinstance(spec, dict):
        if "source_name" in spec:
            ext = ModelExtension.model_validate(spec)
            return await _resolve_source_spec(
                ext, storage=storage, data_source=data_source
            )
        return SlayerModel.model_validate(spec)
    raise ValueError(f"Invalid source_model type: {type(spec)!r}")


async def build_resolved_source_bundle(
    *,
    query: SlayerQuery,
    storage: "StorageBackend",
    data_source: Optional[str] = None,
    runtime_variables: Optional[Dict[str, Any]] = None,
    outer_variables: Optional[Dict[str, Any]] = None,
    named_queries: Optional[Dict[str, SlayerQuery]] = None,
) -> ResolvedSourceBundle:
    """Eagerly assemble the :class:`ResolvedSourceBundle` for one execution (P11).

    Resolves the query's source model (every input shape), walks the join
    graph transitively to collect every model the binder may hop through,
    threads the named-query sibling map, merges the variable layers, and
    records the datasource hint. Storage is consulted here and only here;
    the binder then reads from the bundle purely.

    Variable precedence (highest first): runtime > query (stage) > outer >
    source-model defaults. ``outer_variables`` is the enclosing-query layer
    for a query-backed model resolved as a nested source — left ``None`` for
    plain top-level execution.
    """
    named_queries = named_queries or {}

    # The bundle's source_model is the real base the non-sibling stages bind
    # against — follow the sibling chain past any named-stage indirection.
    root_spec = _follow_sibling_chain(query.source_model, named_queries)
    source_model = await _resolve_source_spec(
        root_spec, storage=storage, data_source=data_source
    )

    # Joins never cross datasource boundaries: scope the graph walk by the
    # source model's own data_source (matches engine._expand_join_graph),
    # falling back to the execution hint only when the model carries none.
    walk_ds = source_model.data_source or data_source or None

    referenced_models = await _collect_referenced_models(
        source_model=source_model,
        named_queries=named_queries,
        storage=storage,
        data_source=walk_ds,
    )

    query_variables = merge_query_variables(
        runtime=runtime_variables,
        stage=query.variables,
        outer=outer_variables,
        model_defaults=source_model.query_variables,
    )

    return ResolvedSourceBundle(
        source_model=source_model,
        referenced_models=referenced_models,
        named_queries=dict(named_queries),
        query_variables=query_variables,
        datasource_hint=data_source,
    )


async def _collect_referenced_models(
    *,
    source_model: SlayerModel,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
) -> List[SlayerModel]:
    """Transitive join-graph walk (Kahn-free BFS), best-effort.

    Seeds: the source model, plus the real base model of every named sibling
    stage (so a sibling's own ``plan_query`` can hop through targets the root
    stage never touches). Follows each model's ``joins[].target_model`` within
    ``data_source``; absent targets are skipped silently (mirrors
    ``SlayerQueryEngine._expand_join_graph``). The source model is returned
    first so ``get_referenced_model`` finds the host before any same-named
    join target.
    """
    # Models we already hold concretely (host + each sibling's real base,
    # overlay-resolved), keyed by name. Resolving siblings through
    # ``_resolve_source_spec`` means an extension-added join on a sibling is
    # walked too. Best-effort: a sibling whose base is absent is skipped.
    preseeded: Dict[str, SlayerModel] = {source_model.name: source_model}
    for sib in named_queries.values():
        spec = _follow_sibling_chain(sib.source_model, named_queries)
        try:
            sib_model = await _resolve_source_spec(
                spec, storage=storage, data_source=data_source
            )
        except ValueError as exc:
            logger.debug("sibling source resolution failed for %r: %s", spec, exc)
            continue
        preseeded.setdefault(sib_model.name, sib_model)

    collected: Dict[str, SlayerModel] = {}
    visited: set[str] = set()
    frontier: List[str] = list(preseeded)
    while frontier:
        name = frontier.pop()
        if name in visited:
            continue
        visited.add(name)
        model = preseeded.get(name)
        if model is None:
            try:
                model = await storage.get_model(name, data_source=data_source)
            except Exception as exc:  # best-effort; absent target is fine
                logger.debug("join-target lookup failed for %r: %s", name, exc)
                model = None
        if model is None:
            continue
        collected.setdefault(name, model)
        for join in model.joins:
            if join.target_model not in visited:
                frontier.append(join.target_model)

    ordered = [source_model]
    ordered.extend(m for n, m in collected.items() if n != source_model.name)
    return ordered
