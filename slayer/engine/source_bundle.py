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

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.variables import merge_query_variables

if TYPE_CHECKING:
    from slayer.core.scope import StageSchema
    from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_model: Optional[SlayerModel] = None
    referenced_models: List[SlayerModel] = Field(default_factory=list)
    inline_extensions: List[ModelExtension] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    # DEV-1450 stage 7b.15d — per-named-stage resolved source model, keyed by
    # stage name. Populated for siblings whose source resolves to a concrete
    # model (a stored model, an inline ``SlayerModel``, or a ``ModelExtension``
    # over a stored base). Siblings sourced FROM another sibling (chain or a
    # ``ModelExtension`` over a sibling) are omitted — the planner resolves
    # those against the upstream ``StageSchema`` at plan time. Lets each stage
    # in a heterogeneous DAG bind against its OWN source rather than the root's.
    stage_source_models: Dict[str, SlayerModel] = Field(default_factory=dict)
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


def _source_name_if_sibling(
    spec: SourceSpec, sibling_names: "set[str] | Dict[str, Any]"
) -> Optional[str]:
    """Return the sibling stage name a ``source_model`` spec reads from, if any.

    Covers the bare-string form (``source_model="kpis"``) AND the
    ``ModelExtension`` / dict-with-``source_name`` form
    (``source_model={"source_name": "kpis", ...}``) — both reference a sibling
    when the name is in ``sibling_names``. Returns ``None`` otherwise.
    """
    if isinstance(spec, str):
        return spec if spec in sibling_names else None
    if isinstance(spec, ModelExtension):
        return spec.source_name if spec.source_name in sibling_names else None
    if isinstance(spec, dict) and isinstance(spec.get("source_name"), str):
        nm = spec["source_name"]
        return nm if nm in sibling_names else None
    return None


def _follow_sibling_chain(
    spec: SourceSpec, named_queries: Dict[str, SlayerQuery]
) -> SourceSpec:
    """Resolve a ``source_model`` that points at a named sibling stage down
    to the real base spec it ultimately reads from.

    The bundle's ``source_model`` must be the real base the root chain bottoms
    out at — not a sibling name — so a query-backed datasource lookup and the
    single-model binding path resolve correctly. Follows both the bare-string
    sibling form and the ``ModelExtension`` / dict-over-sibling form down to
    the first spec that does NOT read from a sibling. A cycle raises
    ``ValueError`` (mirrors the legacy ``_resolve_model`` circular-reference
    guard).
    """
    seen: List[str] = []
    while True:
        sib = _source_name_if_sibling(spec, named_queries)
        if sib is None:
            return spec
        if sib in seen:
            chain = " -> ".join([*seen, sib])
            raise ValueError(
                f"Circular reference detected in source_queries DAG: {chain}"
            )
        seen.append(sib)
        spec = named_queries[sib].source_model


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
    sibling_names = set(named_queries)

    # The bundle's source_model is the real base the root chain bottoms out
    # at — follow the sibling chain past any named-stage indirection. When the
    # ROOT source is a ``ModelExtension`` over a NON-sibling base, the overlay
    # is recorded in ``inline_extensions`` so the engine can re-apply it AFTER
    # a query-backed base expands (expansion derives columns from the backing
    # query and would otherwise drop the overlay's extra columns).
    root_spec = _follow_sibling_chain(query.source_model, named_queries)
    inline_extensions: List[ModelExtension] = []
    if _source_name_if_sibling(root_spec, sibling_names) is None:
        ext = _as_extension_over_nonsibling(root_spec, sibling_names)
        if ext is not None:
            inline_extensions.append(ext)
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

    # Per-named-stage source models — each non-sibling-sourced sibling resolves
    # to its OWN concrete model so heterogeneous DAGs (stage A over ``orders``,
    # stage B over ``customers``) bind each stage against the right host.
    stage_source_models: Dict[str, SlayerModel] = {}
    for nm, nq in named_queries.items():
        if _source_name_if_sibling(nq.source_model, sibling_names) is not None:
            continue  # sibling-sourced: planner resolves via upstream StageSchema
        # A non-sibling-sourced stage's source MUST resolve to a concrete model;
        # a failure here (typoed / missing model) is a genuine error, not a
        # best-effort skip — swallowing it would silently fall back to the root
        # source and emit wrong SQL when column names overlap.
        stage_source_models[nm] = await _resolve_source_spec(
            nq.source_model, storage=storage, data_source=walk_ds or data_source
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
        inline_extensions=inline_extensions,
        named_queries=dict(named_queries),
        stage_source_models=stage_source_models,
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


def _as_extension_over_nonsibling(
    spec: SourceSpec, sibling_names: "set[str]"
) -> Optional[ModelExtension]:
    """Return the ``ModelExtension`` if ``spec`` overlays a NON-sibling base.

    Used to record the root overlay so the engine can re-apply it after a
    query-backed base expands. Returns ``None`` for plain strings, inline
    models, and overlays over a sibling (those are handled by the planner).
    """
    if isinstance(spec, ModelExtension):
        ext = spec
    elif isinstance(spec, dict) and isinstance(spec.get("source_name"), str):
        ext = ModelExtension.model_validate(spec)
    else:
        return None
    if ext.source_name in sibling_names:
        return None
    return ext


def synthetic_model_from_stage_schema(
    *, name: str, schema: "StageSchema", data_source: str
) -> SlayerModel:
    """A stand-in ``SlayerModel`` whose ``sql_table`` is a stage's CTE name and
    whose columns are that stage's flat output columns.

    Lets the binder / cross-model planner resolve a join (or cross-model ref)
    targeting a sibling stage, and the generator emit ``FROM <cte> AS <cte>`` /
    ``LEFT JOIN <cte> ...`` — the stage is materialised as a CTE elsewhere
    (``generate_planned_stages``); this is the rendering vehicle for that CTE
    relation. ``StageColumn.name`` is already the ``__``-flattened downstream
    bind name, so the synthetic column names match how downstream refs bind.
    """
    return SlayerModel(
        name=name,
        data_source=data_source or "_stage",
        sql_table=name,
        columns=[
            Column(name=c.name, type=c.type or DataType.DOUBLE)
            for c in schema.columns
        ],
    )


def stage_bundle_with_siblings(
    *,
    bundle: ResolvedSourceBundle,
    source_model: SlayerModel,
    sibling_schemas: Dict[str, "StageSchema"],
    data_source: str,
) -> ResolvedSourceBundle:
    """Per-stage bundle: ``source_model`` is the stage's own host; synthetic
    sibling models (one per already-emitted ``StageSchema``) are threaded into
    ``referenced_models`` so a join / cross-model ref to a sibling resolves.

    The host comes first (``get_referenced_model`` finds it before any same-
    named join target), then the synthetic siblings, then the original bundle's
    referenced models (minus any shadowed by the host or a synthetic sibling).
    """
    synths = [
        synthetic_model_from_stage_schema(
            name=n, schema=s, data_source=data_source
        )
        for n, s in sibling_schemas.items()
    ]
    shadow = {source_model.name} | {s.name for s in synths}
    referenced = (
        [source_model]
        + synths
        + [m for m in bundle.referenced_models if m.name not in shadow]
    )
    return bundle.model_copy(
        update={"source_model": source_model, "referenced_models": referenced}
    )


async def expand_query_backed_models_in_bundle(
    *,
    bundle: ResolvedSourceBundle,
    outer_vars: Optional[Dict[str, Any]],
    runtime_kwarg: Optional[Dict[str, Any]],
    dry_run_placeholders: bool,
    expander,
    _resolving: Optional["set[str]"] = None,
) -> ResolvedSourceBundle:
    """Expand every query-backed model in the bundle and re-apply any root
    ``ModelExtension`` overlay (DEV-1452 Stage B decision F).

    Three expansion blocks, mirroring ``_execute_pipeline`` exactly:

    1. Source model — if ``bundle.source_model.source_queries`` is set,
       expand to ``sql``-mode and re-apply every ``bundle.inline_extensions``
       overlay (expansion derives columns from the backing query and would
       otherwise drop the overlay's extra columns).
    2. Referenced models — every join / cross-model target with
       ``source_queries`` set expands so the generator renders it as a
       backing-SQL subquery rather than a bare table.
    3. Stage source models — a non-root stage whose own source is a stored
       query-backed model likewise expands to ``sql``-mode before the
       planner binds it.

    ``expander`` is the callback that performs the actual per-model
    expansion. ``_execute_pipeline`` and the migrated
    ``_expand_query_backed_model`` both pass
    ``self._expand_query_backed_model``; the callback signature is
    ``async (model, *, outer_vars, runtime_kwarg, dry_run_placeholders,
    _resolving) -> SlayerModel``.

    Returns a fresh ``ResolvedSourceBundle`` with the expanded models;
    ``inline_extensions`` is preserved as-is for traceability but the
    overlay has already been folded into ``source_model``.

    ``_resolving`` is the recursion guard: a set of model names already
    being expanded in this asyncio task. A query-backed join target that
    transitively references its parent (or itself) is short-circuited
    with the cached ``backing_query_sql`` if available, otherwise left
    unchanged — mirrors the legacy ``_render_query_backed_join_target``
    contract.
    """
    resolving: "set[str]" = set(_resolving) if _resolving is not None else set()

    async def _expand_or_short_circuit(model: SlayerModel) -> SlayerModel:
        if model.name in resolving:
            # Re-entry: use cached backing SQL if available, otherwise
            # return unchanged (the binder / generator will surface a
            # clear error when no sql_table / sql is set).
            if model.backing_query_sql:
                return model.model_copy(
                    update={"sql": model.backing_query_sql},
                )
            return model
        resolving.add(model.name)
        try:
            return await expander(
                model=model,
                outer_vars=outer_vars,
                runtime_kwarg=runtime_kwarg,
                dry_run_placeholders=dry_run_placeholders,
                _resolving=resolving,
            )
        finally:
            resolving.discard(model.name)

    # 1. Source model + inline_extensions re-apply.
    if bundle.source_model is not None and bundle.source_model.source_queries:
        expanded = await _expand_or_short_circuit(bundle.source_model)
        for ext in bundle.inline_extensions:
            expanded = _apply_extension_overlay(expanded, ext)
        bundle = bundle.model_copy(
            update={
                "source_model": expanded,
                "referenced_models": [expanded]
                + [
                    m
                    for m in bundle.referenced_models
                    if m.name != expanded.name
                ],
            }
        )
    source_model = bundle.source_model

    # 2. Referenced models (skip the source model itself — handled above).
    if source_model is not None and any(
        rm.name != source_model.name and rm.source_queries
        for rm in bundle.referenced_models
    ):
        expanded_refs: List[SlayerModel] = []
        for rm in bundle.referenced_models:
            if rm.name != source_model.name and rm.source_queries:
                rm = await _expand_or_short_circuit(rm)
            expanded_refs.append(rm)
        bundle = bundle.model_copy(update={"referenced_models": expanded_refs})

    # 3. Stage source models.
    if any(m.source_queries for m in bundle.stage_source_models.values()):
        expanded_stage_sources: Dict[str, SlayerModel] = {}
        for nm, sm in bundle.stage_source_models.items():
            if sm.source_queries:
                sm = await _expand_or_short_circuit(sm)
            expanded_stage_sources[nm] = sm
        bundle = bundle.model_copy(
            update={"stage_source_models": expanded_stage_sources}
        )

    return bundle
