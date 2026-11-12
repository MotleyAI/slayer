"""Storage-backed builders for :class:`ResolvedSourceBundle` (P11)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, cast

from slayer.core.errors import ModelNotFoundError, QueryBackedCycleError
from slayer.core.join_walker import incident_model_names
from slayer.core.names import match_name
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery, SourceSpec, extension_model_names
from slayer.core.scope import StageDisplay
from slayer.core.time_spine import TIME_SPINE_MODEL, is_spine, query_host, spine_model
from slayer.engine.column_dependency import definition_heads
from slayer.engine.cardinality.unrelated import first_hop_names
from slayer.engine.extensions import respelled_stages
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    follow_sibling_chain,
    source_name_if_sibling,
)
from slayer.ir.variables import merge_query_variables
from slayer.sql.dialects import dialect_for_ds_type
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures

logger = logging.getLogger(__name__)


async def _referenced_models(
    *, query: SlayerQuery, source_model: SlayerModel, named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend", data_source: Optional[str], chain: Tuple[str, ...], spine_clash: bool,
    failures: DocumentLoadFailures,
) -> Tuple[List[SlayerModel], Dict[str, SlayerModel]]:
    """The root's join component plus query-written targets (the spine unless a stored model clashes with it), and the stored query-backed models among them."""
    component = await _collect_referenced_models(
        source_model=source_model, named_queries=named_queries, storage=storage, data_source=data_source,
        failures=failures,
    )
    known = {m.name for m in component}
    component.extend(await _query_written_targets(
        queries=[*named_queries.values(), query], known=known,
        sibling_names=set(named_queries), storage=storage, data_source=data_source, failures=failures,
    ))
    referenced, query_backed = await _split_query_backed(
        models=[m for m in component if not is_spine(m)], storage=storage, data_source=data_source, chain=chain,
        failures=failures,
    )
    if not spine_clash and data_source is not None:
        referenced.append(source_model if is_spine(source_model) else spine_model(data_source=data_source))
    return referenced, query_backed


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
    now: Optional[datetime] = None,
    failures: Optional[DocumentLoadFailures] = None,
) -> ResolvedSourceBundle:
    """Eagerly assemble the :class:`ResolvedSourceBundle` for one execution (P11).

    Storage is consulted here and only here; the binder then reads the bundle
    purely. Variable precedence (highest first): runtime > query (stage) >
    outer > source-model defaults. Stored query-backed models are collected as
    splice placeholders, never as referenced models. ``now`` is the execution's
    clock reading (default: the host clock); ``failures`` records skipped unloadable peers.
    """
    failures = failures or DocumentLoadFailures()
    named_queries = named_queries or {}
    stage_displays = stage_displays or {}
    storage = cast("StorageBackend", _ModelReadCache(storage))
    for q in [*named_queries.values(), query]:
        _reject_chain_source(spec=q.source_model, chain=splice_chain)

    source_model = await _resolve_source_spec(
        follow_sibling_chain(spec=query.source_model, named_queries=named_queries),
        storage=storage, data_source=data_source,
    )

    # Joins never cross datasource boundaries: scope the walk by the source
    # model's own data_source, falling back to the hint only when it carries none.
    walk_ds = source_model.data_source or data_source or None
    spine_clash = walk_ds is not None and (
        await storage.get_model(TIME_SPINE_MODEL, data_source=walk_ds)
    ) is not None
    peer_names, peer_edge_names = await _datasource_names(storage=storage, data_source=walk_ds, failures=failures)
    query, named_queries = respelled_stages(query, named_queries, models=peer_names)
    # Again: an inline source's join targets are respelled now.
    source_model = await _resolve_source_spec(
        follow_sibling_chain(spec=query.source_model, named_queries=named_queries),
        storage=storage, data_source=data_source,
    )
    referenced_models, query_backed = await _referenced_models(
        query=query, source_model=source_model, named_queries=named_queries,
        storage=storage, data_source=walk_ds, chain=splice_chain, spine_clash=spine_clash, failures=failures,
    )
    stage_referenced_models, stage_backed = await _shadowed_stage_universe(
        query=query, source_model=source_model, named_queries=named_queries,
        storage=storage, data_source=walk_ds, chain=splice_chain, spine_clash=spine_clash, failures=failures,
    )
    query_backed = {**stage_backed, **query_backed}
    source_model = query_host(source_model, query=query)

    stage_source_models = await _stage_source_models(
        named_queries=named_queries, storage=storage, data_source=walk_ds or data_source,
    )
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
        datasource_type=ds.type if ds else None,
        source_model=source_model,
        referenced_models=referenced_models,
        inline_extensions=[e for q in [*named_queries.values(), query] for e in q.extensions or []],
        named_queries=dict(named_queries),
        query=query,
        stage_source_models=stage_source_models,
        query_variables=query_variables,
        datasource_hint=data_source,
        stage_displays=dict(stage_displays),
        query_backed=query_backed,
        splice_chain=tuple(splice_chain),
        runtime_variables=dict(runtime_variables or {}),
        now=datetime.now() if now is None else now,
        granularities=ds.granularity_definitions if ds else {},
        spine_clash=spine_clash,
        datasource_model_names=peer_names,
        datasource_edge_names=peer_edge_names,
        stage_referenced_models=stage_referenced_models,
    )


async def _shadowed_stage_universe(
    *, query: SlayerQuery, source_model: SlayerModel, named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend", data_source: Optional[str], chain: Tuple[str, ...], spine_clash: bool,
    failures: DocumentLoadFailures,
) -> Tuple[Optional[List[SlayerModel]], Dict[str, SlayerModel]]:
    """The other stages' universe and its query-backed models when the root's inline source shadows a stored model
    of its name, else ``(None, {})``."""
    stored = (
        await stored_model(storage, name=source_model.name, data_source=data_source)
        if source_model.inline and named_queries and data_source is not None else None
    )
    if stored is None:
        return None, {}
    return await _referenced_models(
        query=query, source_model=stored, named_queries=named_queries,
        storage=storage, data_source=data_source, chain=chain, spine_clash=spine_clash, failures=failures,
    )


async def _datasource_names(
    *, storage: "StorageBackend", data_source: Optional[str], failures: DocumentLoadFailures,
) -> "Tuple[frozenset[str], frozenset[str]]":
    """The model names and stored edge names of ``data_source``."""
    peers = failures.skip(await storage.load_models(data_source=data_source)) if data_source is not None else []
    return frozenset(m.name for m in peers), frozenset(j.name for m in peers for j in m.joins if j.name)


async def _stage_source_models(
    *,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
) -> Dict[str, SlayerModel]:
    """Each non-sibling-sourced stage's OWN concrete model, so heterogeneous DAGs
    bind against the right host; a query-backed source becomes a spliced sibling instead."""
    sibling_names = set(named_queries)
    out: Dict[str, SlayerModel] = {}
    for nm, nq in named_queries.items():
        if source_name_if_sibling(spec=nq.source_model, sibling_names=sibling_names) is not None:
            continue  # sibling-sourced: planner resolves via upstream StageSchema
        # MUST resolve; a failure is a genuine error (a skip would fall back to the root source).
        resolved = await _resolve_source_spec(nq.source_model, storage=storage, data_source=data_source)
        if not resolved.source_queries:
            out[nm] = query_host(resolved, query=nq)
    return out


def _written_targets(query: SlayerQuery) -> List[str]:
    """The models ``query`` names: its inline source's join targets and its extensions' models, then its
    references' first hops."""
    spec = query.source_model
    joins = [j.target_model for j in spec.joins] if isinstance(spec, SlayerModel) else []
    return list(dict.fromkeys([*joins, *extension_model_names(query), *sorted(first_hop_names(query))]))


async def _query_written_targets(
    *,
    queries: List[SlayerQuery],
    known: "set[str]",
    sibling_names: "set[str]",
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
) -> List[SlayerModel]:
    """The components of models the queries name — as join targets or first hops — but the
    join-graph walk did not reach (an extension over a sibling carries its own joins)."""
    names = [n for q in queries for n in _written_targets(q) if n not in sibling_names]
    return await _named_components(
        names=names, known=known, storage=storage, data_source=data_source, failures=failures,
    )


async def _named_components(
    *,
    names: List[str],
    known: "set[str]",
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
) -> List[SlayerModel]:
    """The components of the ``names`` models not yet ``known`` (absent names skipped)."""
    out: List[SlayerModel] = []
    for name in names:
        if name in known:
            continue
        model = await _frontier_model(name=name, all_models={}, storage=storage, ds=data_source)
        if model is None:
            continue
        fresh = [
            m for m in await _collect_referenced_models(
                source_model=model, named_queries={}, storage=storage, data_source=data_source,
                failures=failures,
            )
            if m.name not in known
        ]
        known.update(m.name for m in fresh)
        out.extend(fresh)
    return out


def _spec_base_name(spec: SourceSpec | None) -> Optional[str]:
    return spec if isinstance(spec, str) else None


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
    failures: DocumentLoadFailures,
) -> "Tuple[List[SlayerModel], Dict[str, SlayerModel]]":
    """Split ``models`` into referenced models and query-backed placeholders, closing
    over each placeholder's stage bases and each model's definition-named models (and
    their components). Placeholders keep their stored form, including declared joins; on-chain
    names are never loaded."""
    referenced: Dict[str, SlayerModel] = {}
    query_backed: Dict[str, SlayerModel] = {}
    pending = list(models)
    while pending:
        model = pending.pop(0)
        if model.name in referenced or model.name in query_backed:
            continue
        if not model.source_queries:
            referenced[model.name] = model
            pending.extend(m for m in await _named_components(
                names=definition_heads([model]),
                known={*chain, *referenced, *query_backed, *(p.name for p in pending)},
                storage=storage, data_source=data_source, failures=failures,
            ) if not is_spine(m))
            continue
        query_backed[model.name] = model
        pending.extend(await _stage_base_components(
            model=model, skip=[*chain, *query_backed], known={*referenced, *query_backed},
            storage=storage, data_source=data_source, failures=failures,
        ))
    return list(referenced.values()), query_backed


async def _stage_base_components(
    *,
    model: SlayerModel,
    skip: List[str],
    known: "set[str]",
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
) -> List[SlayerModel]:
    """The components of ``model``'s stage bases, bar bases named in ``skip`` or resolving into ``known``."""
    out: List[SlayerModel] = []
    private = {q.name: q for q in model.source_queries or [] if q.name}
    for stage in model.source_queries or []:
        spec = follow_sibling_chain(spec=stage.source_model, named_queries=private)
        name = _spec_base_name(spec)
        if spec is None or (name is not None and name in skip):
            continue
        try:
            base = await _resolve_source_spec(spec, storage=storage, data_source=data_source)
        except ValueError as exc:
            logger.debug("query-backed stage base %r unresolved: %s", name, exc)
            continue
        if base.name not in known:
            out.extend(await _collect_referenced_models(
                source_model=base, named_queries={}, storage=storage, data_source=data_source,
                failures=failures,
            ))
    out.extend(await _query_written_targets(
        queries=list(model.source_queries or []), known={*known, *skip, *(m.name for m in out)},
        sibling_names=set(private), storage=storage, data_source=data_source, failures=failures,
    ))
    return out


async def _preseed_sibling_models(
    *,
    source_model: SlayerModel,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
) -> Dict[str, SlayerModel]:
    """Models held concretely: host + each sibling's overlay-resolved stored base (an inline one stays in its stage).
    Best-effort — a sibling whose base is absent is skipped."""
    preseeded: Dict[str, SlayerModel] = {source_model.name: source_model}
    for sib in named_queries.values():
        spec = follow_sibling_chain(spec=sib.source_model, named_queries=named_queries)
        if isinstance(spec, SlayerModel):
            continue
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
    failures: DocumentLoadFailures,
) -> Dict[str, SlayerModel]:
    """Preseeded plus every loadable datasource peer, so reverse edges (a peer
    declaring a join *into* a frontier model) are discoverable."""
    peers = failures.skip(await storage.load_models(data_source=ds)) if ds is not None else []
    return {**preseeded, **{m.name: m for m in peers if m.name not in preseeded}}


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
    universe: Dict[str, SlayerModel],
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
        frontier.extend(
            n for n in incident_model_names(model=model, models_by_name={**universe, name: model})
            if n not in visited
        )
    return collected


async def _collect_referenced_models(
    *,
    source_model: SlayerModel,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
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
        preseeded=preseeded, ds=ds, storage=storage, failures=failures,
    )
    universe = {**all_models, TIME_SPINE_MODEL: source_model} if is_spine(source_model) else all_models
    collected = await _bfs_connected_component(
        # The spine reaches every dataset (a query-backed one's axis is known only once spliced).
        seeds=list(all_models if is_spine(source_model) else preseeded),
        all_models=all_models, universe=universe, storage=storage, ds=ds,
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
        return spec.as_inline()
    if spec == TIME_SPINE_MODEL and data_source is not None:
        return spine_model(data_source=data_source)
    if isinstance(spec, str):
        model = await stored_model(storage, name=spec, data_source=data_source)
        if model is None:
            raise ModelNotFoundError(spec)
        return model
    raise ValueError(f"Invalid source_model type: {type(spec)!r}")


async def stored_model(
    storage: "StorageBackend", *, name: str, data_source: Optional[str],
) -> Optional[SlayerModel]:
    """The stored model ``name`` reaches (``slayer.core.names.match_name``)."""
    model = await storage.get_model(name, data_source=data_source)
    if model is not None:
        return model
    found = match_name(name=name, declared=await storage.list_models(data_source=data_source), quoted=False)
    return None if found is None else await storage.get_model(found, data_source=data_source)


class _ModelReadCache:
    """Read-through ``get_model`` cache scoped to one bundle build (delegates the rest)."""

    def __init__(self, inner: "StorageBackend") -> None:
        self._inner = inner
        self._models: Dict[tuple, Optional[SlayerModel]] = {}

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def load_models(self, **kwargs: Any) -> Any:
        """The inner backend's ``load_models``, reading through this cache."""
        return await StorageBackend.load_models(cast(StorageBackend, self), **kwargs)

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
