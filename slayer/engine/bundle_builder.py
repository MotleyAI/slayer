"""Storage-backed builders for :class:`ResolvedSourceBundle` (P11)."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from slayer.core.models import SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    SourceSpec,
    _apply_extension_overlay,
    _as_extension_over_nonsibling,
    _follow_sibling_chain,
    _source_name_if_sibling,
    _spec_adds_measures,
)
from slayer.ir.variables import merge_query_variables

if TYPE_CHECKING:
    from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


#: Cap on concurrent peer-model reads during the join-graph walk.
_PEER_LOAD_CONCURRENCY = 8


async def expand_query_backed_models_in_bundle(  # NOSONAR(S3776) — three sequential expansion blocks (source + referenced + stage_source) that mutate the bundle in series. Each block guards on its own condition; splitting forces ``bundle`` to ping-pong through helpers without simplifying anything. The inner recursion guard is the only shared state.
    *,
    bundle: ResolvedSourceBundle,
    outer_vars: Optional[Dict[str, Any]],
    runtime_kwarg: Optional[Dict[str, Any]],
    dry_run_placeholders: bool,
    expander,
    _resolving: Optional["set[str]"] = None,
) -> ResolvedSourceBundle:
    """Expand every query-backed model in the bundle and re-apply any root overlay.

    Three expansion blocks mirroring ``_execute_pipeline``: (1) source model,
    re-applying every ``inline_extensions`` overlay; (2) referenced join /
    cross-model targets; (3) stage source models. ``expander`` performs the
    per-model expansion; ``_resolving`` is the re-entry recursion guard (a
    transitively self-referencing target short-circuits to cached
    ``backing_query_sql`` if available, else is left unchanged). Returns a fresh
    bundle; ``inline_extensions`` is preserved for traceability.
    """
    resolving: "set[str]" = set(_resolving) if _resolving is not None else set()

    async def _expand_or_short_circuit(model: SlayerModel) -> SlayerModel:
        if model.name in resolving:
            # Re-entry: cached backing SQL if available, else return unchanged.
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

    # Referenced models (source model handled above).
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

    Storage is consulted here and only here; the binder then reads the bundle
    purely. Variable precedence (highest first): runtime > query (stage) >
    outer > source-model defaults.
    """
    named_queries = named_queries or {}
    sibling_names = set(named_queries)
    storage = cast("StorageBackend", _ModelReadCache(storage))

    # source_model is the real base the root chain bottoms out at. A ROOT
    # ModelExtension over a NON-sibling base is recorded in inline_extensions so
    # the engine can re-apply the overlay after a query-backed base expands.
    root_spec = _follow_sibling_chain(query.source_model, named_queries)
    inline_extensions: List[ModelExtension] = []
    if _source_name_if_sibling(root_spec, sibling_names) is None:
        ext = _as_extension_over_nonsibling(root_spec, sibling_names)
        if ext is not None:
            inline_extensions.append(ext)
    source_model = await _resolve_source_spec(
        root_spec, storage=storage, data_source=data_source
    )

    # Joins never cross datasource boundaries: scope the walk by the source
    # model's own data_source, falling back to the hint only when it carries none.
    walk_ds = source_model.data_source or data_source or None

    referenced_models = await _collect_referenced_models(
        source_model=source_model,
        named_queries=named_queries,
        storage=storage,
        data_source=walk_ds,
    )

    # Per-named-stage source models — each non-sibling-sourced sibling resolves
    # to its OWN concrete model so heterogeneous DAGs bind against the right host.
    stage_source_models: Dict[str, SlayerModel] = {}
    for nm, nq in named_queries.items():
        if _source_name_if_sibling(nq.source_model, sibling_names) is not None:
            continue  # sibling-sourced: planner resolves via upstream StageSchema
        # MUST resolve to a concrete model; a failure is a genuine error, not a
        # best-effort skip (would silently fall back to the root source).
        resolved_stage = await _resolve_source_spec(
            nq.source_model, storage=storage, data_source=walk_ds or data_source
        )
        # Deferred-measure re-application is wired only for the ROOT source, not
        # stage sources — a stage extension's measures would silently drop.
        if resolved_stage.source_queries and _spec_adds_measures(nq.source_model):
            raise ValueError(
                f"Stage {nm!r}: a ModelExtension over query-backed model "
                f"{resolved_stage.name!r} may not add measures — deferred-overlay "
                f"re-application is not wired for stage sources, so the measures "
                f"would be silently dropped. Reference them from the root source "
                f"or a table-backed base."
            )
        stage_source_models[nm] = resolved_stage

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
    """Transitive join-graph walk (BFS) over the bidirectional edge set,
    best-effort.

    Seeds: the source model plus the real base of every named sibling stage.
    Follows each edge in either direction — a model's ``joins[].target_model``
    and any datasource model that declares a join *into* the frontier model
    (DEV-1853) — so the closure is the datasource's connected component. The
    source model is returned first.
    """
    # Models held concretely (host + each sibling's overlay-resolved base).
    # Best-effort: a sibling whose base is absent is skipped.
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

    # Load the datasource's models once so reverse edges (a peer declaring a
    # join into a frontier model) are discoverable. Bidirectional traversal
    # makes the reachable set the connected component, not just forward targets.
    ds = data_source or source_model.data_source
    all_models: Dict[str, SlayerModel] = dict(preseeded)
    try:
        peer_names = await storage.list_models(ds) if ds is not None else []
    except Exception as exc:  # best-effort; ambiguous/absent ds → forward only
        # Sanitize for log injection (S5145): strip CR/LF before logging.
        safe_ds = str(ds).replace("\r", "\\r").replace("\n", "\\n")
        logger.warning(
            "list_models failed for ds '%s' (%s): reverse join edges will not "
            "be discoverable for this query", safe_ds, exc,
        )
        peer_names = []
    sem = asyncio.Semaphore(_PEER_LOAD_CONCURRENCY)

    async def _load_peer(nm: str) -> "tuple[str, Optional[SlayerModel]]":
        async with sem:
            try:
                return nm, await storage.get_model(nm, data_source=ds)
            except Exception as exc:  # best-effort; a broken peer is skipped
                logger.debug("peer model load failed for %r: %s", nm, exc)
                return nm, None

    to_load = [nm for nm in peer_names if nm not in all_models]
    for nm, m in await asyncio.gather(*(_load_peer(nm) for nm in to_load)):
        if m is not None:
            all_models[nm] = m
    incoming: Dict[str, List[str]] = {}
    for m in all_models.values():
        for join in m.joins:
            incoming.setdefault(join.target_model, []).append(m.name)

    collected: Dict[str, SlayerModel] = {}
    visited: set[str] = set()
    frontier: List[str] = list(preseeded)
    while frontier:
        name = frontier.pop()
        if name in visited:
            continue
        visited.add(name)
        model = all_models.get(name)
        if model is None:
            try:
                model = await storage.get_model(name, data_source=ds)
            except Exception as exc:  # best-effort; absent target is fine
                logger.debug("join-target lookup failed for %r: %s", name, exc)
                model = None
        if model is None:
            continue
        collected.setdefault(name, model)
        for join in model.joins:
            if join.target_model not in visited:
                frontier.append(join.target_model)
        for src_name in incoming.get(name, ()):
            if src_name not in visited:
                frontier.append(src_name)

    ordered = [source_model]
    ordered.extend(m for n, m in collected.items() if n != source_model.name)
    return ordered


async def _resolve_source_spec(
    spec: SourceSpec,
    *,
    storage: "StorageBackend",
    data_source: Optional[str],
) -> SlayerModel:
    """Resolve any ``source_model`` spec to a concrete ``SlayerModel`` (read-only)."""
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


class _ModelReadCache:
    """Read-through ``get_model`` cache scoped to one bundle build (delegates the rest)."""

    def __init__(self, inner: "StorageBackend") -> None:
        self._inner = inner
        self._models: Dict[tuple, Optional[SlayerModel]] = {}

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def get_model(
        self, name: str, data_source: Optional[str] = None
    ) -> Optional[SlayerModel]:
        key = (name, data_source)
        if key not in self._models:
            model = await self._inner.get_model(name, data_source=data_source)
            self._models[key] = model
            if model is not None:
                # a hint-less read also answers the concrete-datasource key
                self._models.setdefault((name, model.data_source), model)
        return self._models[key]
