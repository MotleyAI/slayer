"""Storage-backed builders for :class:`ResolvedSourceBundle` (P11)."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from slayer.core.errors import QueryBackedCycleError
from slayer.core.models import SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery, SourceSpec
from slayer.core.scope import StageDisplay
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    apply_extension_overlay,
    as_extension_over_nonsibling,
    follow_sibling_chain,
    source_name_if_sibling,
    spec_adds_measures,
)
from slayer.ir.variables import merge_query_variables
from slayer.sql.dialects import dialect_for_ds_type

if TYPE_CHECKING:
    from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


#: Cap on concurrent peer-model reads during the join-graph walk.
_PEER_LOAD_CONCURRENCY = 8


async def build_resolved_source_bundle(
    *,
    query: SlayerQuery,
    storage: "StorageBackend",
    data_source: Optional[str] = None,
    runtime_variables: Optional[Dict[str, Any]] = None,
    outer_variables: Optional[Dict[str, Any]] = None,
    named_queries: Optional[Dict[str, SlayerQuery]] = None,
    stage_displays: Optional[Dict[str, StageDisplay]] = None,
    splice_chain: Tuple[str, ...] = (),
    dry_run_placeholders: bool = False,
) -> ResolvedSourceBundle:
    """Eagerly assemble the :class:`ResolvedSourceBundle` for one execution (P11).

    Storage is consulted here and only here; the binder then reads the bundle
    purely. Variable precedence (highest first): runtime > query (stage) >
    outer > source-model defaults. Stored query-backed models are collected as
    splice placeholders, never as referenced models.
    """
    named_queries = named_queries or {}
    stage_displays = stage_displays or {}
    sibling_names = set(named_queries)
    storage = cast("StorageBackend", _ModelReadCache(storage))
    for q in [*named_queries.values(), query]:
        _reject_chain_source(spec=q.source_model, chain=splice_chain)

    # source_model is the real base the root chain bottoms out at.
    root_spec = follow_sibling_chain(spec=query.source_model, named_queries=named_queries)
    inline_extensions: List[ModelExtension] = []
    if source_name_if_sibling(spec=root_spec, sibling_names=sibling_names) is None:
        ext = as_extension_over_nonsibling(spec=root_spec, sibling_names=sibling_names)
        if ext is not None:
            inline_extensions.append(ext)
    source_model = await _resolve_source_spec(
        root_spec, storage=storage, data_source=data_source
    )
    if source_model.source_queries and isinstance(root_spec, ModelExtension):
        # The overlay applies once, over the spliced stage (design decision 8).
        source_model = await _resolve_source_spec(
            root_spec.source_name, storage=storage, data_source=data_source,
        )

    # Joins never cross datasource boundaries: scope the walk by the source
    # model's own data_source, falling back to the hint only when it carries none.
    walk_ds = source_model.data_source or data_source or None

    component = await _collect_referenced_models(
        source_model=source_model,
        named_queries=named_queries,
        storage=storage,
        data_source=walk_ds,
    )
    component.extend(await _query_written_targets(
        queries=[*named_queries.values(), query], known={m.name for m in component},
        sibling_names=sibling_names, storage=storage, data_source=walk_ds,
    ))
    referenced_models, query_backed = await _split_query_backed(
        models=component, storage=storage, data_source=walk_ds, chain=splice_chain,
    )

    # Per-named-stage source models — each non-sibling-sourced sibling resolves
    # to its OWN concrete model so heterogeneous DAGs bind against the right host;
    # a query-backed source becomes a spliced sibling instead.
    stage_source_models: Dict[str, SlayerModel] = {}
    for nm, nq in named_queries.items():
        if source_name_if_sibling(spec=nq.source_model, sibling_names=sibling_names) is not None:
            continue  # sibling-sourced: planner resolves via upstream StageSchema
        # MUST resolve to a concrete model; a failure is a genuine error, not a
        # best-effort skip (would silently fall back to the root source).
        resolved_stage = await _resolve_source_spec(
            nq.source_model, storage=storage, data_source=walk_ds or data_source
        )
        if resolved_stage.source_queries:
            if spec_adds_measures(nq.source_model):
                label = stage_displays[nm].label if nm in stage_displays else f"stage {nm!r}"
                raise ValueError(
                    f"{label[0].upper()}{label[1:]}: a ModelExtension over query-backed "
                    f"model {resolved_stage.name!r} may not add measures; define them in "
                    f"a later stage over it instead."
                )
            continue
        stage_source_models[nm] = resolved_stage

    query_variables = merge_query_variables(
        runtime=runtime_variables,
        stage=query.variables,
        outer=outer_variables,
        # A query-backed root's own defaults layer its spliced stages, not the root.
        model_defaults=None if source_model.source_queries else source_model.query_variables,
    )

    ds = await storage.get_datasource(source_model.data_source) if source_model.data_source else None
    return ResolvedSourceBundle(
        dialect=dialect_for_ds_type(ds.type if ds else None).sqlglot_name,
        source_model=source_model,
        referenced_models=referenced_models,
        inline_extensions=inline_extensions,
        named_queries=dict(named_queries),
        stage_source_models=stage_source_models,
        query_variables=query_variables,
        datasource_hint=data_source,
        stage_displays=dict(stage_displays),
        query_backed=query_backed,
        splice_chain=tuple(splice_chain),
        runtime_variables=dict(runtime_variables or {}),
        dry_run_placeholders=dry_run_placeholders,
    )


async def _query_written_targets(
    *,
    queries: List[SlayerQuery],
    known: "set[str]",
    sibling_names: "set[str]",
    storage: "StorageBackend",
    data_source: Optional[str],
) -> List[SlayerModel]:
    """The components of models the queries name as join targets but the join-graph
    walk did not reach (an extension over a sibling carries its own joins)."""
    out: List[SlayerModel] = []
    for q in queries:
        spec = q.source_model
        for join in (spec.joins or []) if spec is not None and not isinstance(spec, str) else []:
            name = join.target_model
            if name in known or name in sibling_names:
                continue
            model = await _frontier_model(name=name, all_models={}, storage=storage, ds=data_source)
            if model is None:
                continue
            for m in await _collect_referenced_models(
                source_model=model, named_queries={}, storage=storage, data_source=data_source,
            ):
                if m.name not in known:
                    known.add(m.name)
                    out.append(m)
    return out


def _spec_base_name(spec: SourceSpec | None) -> Optional[str]:
    if isinstance(spec, str):
        return spec
    if isinstance(spec, ModelExtension):
        return spec.source_name
    return None


def _reject_chain_source(*, spec: SourceSpec | None, chain: Tuple[str, ...]) -> None:
    """A source naming a model on the in-flight splice chain is a cycle."""
    name = _spec_base_name(spec)
    if name is not None and name in chain:
        raise QueryBackedCycleError(path=[*chain[chain.index(name):], name])


async def _split_query_backed(
    *,
    models: List[SlayerModel],
    storage: "StorageBackend",
    data_source: Optional[str],
    chain: Tuple[str, ...],
) -> "Tuple[List[SlayerModel], Dict[str, SlayerModel]]":
    """Split ``models`` into referenced models and query-backed placeholders, closing
    over each placeholder's stage bases (and their components). Placeholders keep
    their stored form with joins dropped; on-chain names are never loaded."""
    referenced: Dict[str, SlayerModel] = {}
    query_backed: Dict[str, SlayerModel] = {}
    pending = list(models)
    while pending:
        model = pending.pop(0)
        if model.name in referenced or model.name in query_backed:
            continue
        if not model.source_queries:
            referenced[model.name] = model
            continue
        query_backed[model.name] = model.model_copy(update={"joins": []})
        private = {q.name: q for q in model.source_queries if q.name}
        for stage in model.source_queries:
            spec = follow_sibling_chain(spec=stage.source_model, named_queries=private)
            name = _spec_base_name(spec)
            if spec is None or (name is not None and (name in chain or name in query_backed)):
                continue
            try:
                base = await _resolve_source_spec(spec, storage=storage, data_source=data_source)
            except ValueError as exc:
                logger.debug("query-backed stage base %r unresolved: %s", name, exc)
                continue
            if base.name in referenced or base.name in query_backed:
                continue
            pending.extend(await _collect_referenced_models(
                source_model=base, named_queries={}, storage=storage, data_source=data_source,
            ))
    return list(referenced.values()), query_backed


async def _preseed_sibling_models(
    *,
    source_model: SlayerModel,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
) -> Dict[str, SlayerModel]:
    """Models held concretely: host + each sibling's overlay-resolved base.
    Best-effort — a sibling whose base is absent is skipped."""
    preseeded: Dict[str, SlayerModel] = {source_model.name: source_model}
    for sib in named_queries.values():
        spec = follow_sibling_chain(spec=sib.source_model, named_queries=named_queries)
        try:
            sib_model = await _resolve_source_spec(
                spec, storage=storage, data_source=data_source
            )
        except ValueError as exc:
            logger.debug("sibling source resolution failed for %r: %s", spec, exc)
            continue
        preseeded.setdefault(sib_model.name, sib_model)
    return preseeded


async def _load_datasource_peers(
    *,
    preseeded: Dict[str, SlayerModel],
    ds: Optional[str],
    storage: "StorageBackend",
) -> Dict[str, SlayerModel]:
    """Preseeded plus every loadable datasource peer, so reverse edges (a peer
    declaring a join *into* a frontier model) are discoverable."""
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
    return all_models


async def _frontier_model(
    *,
    name: str,
    all_models: Dict[str, SlayerModel],
    storage: "StorageBackend",
    ds: Optional[str],
) -> Optional[SlayerModel]:
    """The named model, loading it on demand; absent targets resolve to None."""
    model = all_models.get(name)
    if model is not None:
        return model
    try:
        return await storage.get_model(name, data_source=ds)
    except Exception as exc:  # best-effort; absent target is fine
        logger.debug("join-target lookup failed for %r: %s", name, exc)
        return None


async def _bfs_connected_component(
    *,
    seeds: List[str],
    all_models: Dict[str, SlayerModel],
    incoming: Dict[str, List[str]],
    storage: "StorageBackend",
    ds: Optional[str],
) -> Dict[str, SlayerModel]:
    """Walk joins in both directions from the seeds; absent targets skipped."""
    collected: Dict[str, SlayerModel] = {}
    visited: set[str] = set()
    frontier: List[str] = list(seeds)
    while frontier:
        name = frontier.pop()
        if name in visited:
            continue
        visited.add(name)
        model = await _frontier_model(
            name=name, all_models=all_models, storage=storage, ds=ds,
        )
        if model is None:
            continue
        collected.setdefault(name, model)
        neighbors = [j.target_model for j in model.joins]
        neighbors.extend(incoming.get(name, ()))
        frontier.extend(n for n in neighbors if n not in visited)
    return collected


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
    and any datasource model that declares a join *into* the frontier model —
    so the closure is the datasource's connected component. The source model
    is returned first.
    """
    preseeded = await _preseed_sibling_models(
        source_model=source_model, named_queries=named_queries,
        storage=storage, data_source=data_source,
    )
    ds = data_source or source_model.data_source
    all_models = await _load_datasource_peers(
        preseeded=preseeded, ds=ds, storage=storage,
    )
    incoming: Dict[str, List[str]] = {}
    for m in all_models.values():
        for join in m.joins:
            incoming.setdefault(join.target_model, []).append(m.name)
    collected = await _bfs_connected_component(
        seeds=list(preseeded), all_models=all_models, incoming=incoming,
        storage=storage, ds=ds,
    )
    ordered = [source_model]
    ordered.extend(m for n, m in collected.items() if n != source_model.name)
    return ordered


async def _resolve_source_spec(
    spec: SourceSpec | None,
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
        return apply_extension_overlay(base, spec)
    if isinstance(spec, str):
        model = await storage.get_model(spec, data_source=data_source)
        if model is None:
            raise ValueError(f"Model '{spec}' not found")
        return model
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
