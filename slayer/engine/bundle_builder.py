"""Storage-backed builders for :class:`ResolvedSourceBundle` (P11)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, cast

from slayer.core.errors import AmbiguousReferenceError, DatasetNotFoundError, QueryBackedCycleError
from slayer.core.relationship_walker import incident_dataset_names
from slayer.core.names import match_name
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery, SourceSpec, extension_dataset_names
from slayer.core.scope import StageDisplay
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine, is_spine_spelling, query_host, spine_dataset
from slayer.engine.field_dependency import definition_heads
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


async def _referenced_datasets(
    *, query: SlayerQuery, source_dataset: SlayerDataset, named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend", data_source: Optional[str], chain: Tuple[str, ...], spine_clash: bool,
    failures: DocumentLoadFailures,
) -> Tuple[List[SlayerDataset], Dict[str, SlayerDataset]]:
    """The root's relationship component plus query-written targets (the spine unless a stored dataset clashes with it), and the stored query-backed datasets among them."""
    component = await _collect_referenced_datasets(
        source_dataset=source_dataset, named_queries=named_queries, storage=storage, data_source=data_source,
        failures=failures,
    )
    known = {m.name for m in component}
    component.extend(await _query_written_targets(
        queries=[*named_queries.values(), query], known=known,
        sibling_names=set(named_queries), storage=storage, data_source=data_source, failures=failures,
    ))
    referenced, query_backed = await _split_query_backed(
        datasets=[m for m in component if not is_spine(m)], storage=storage, data_source=data_source, chain=chain,
        failures=failures,
    )
    if not spine_clash and data_source is not None:
        referenced.append(source_dataset if is_spine(source_dataset) else spine_dataset(data_source=data_source))
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
    respelled_spine: frozenset[str] = frozenset(),
) -> ResolvedSourceBundle:
    """Eagerly assemble the :class:`ResolvedSourceBundle` for one execution (P11).

    Storage is consulted here and only here; the binder then reads the bundle
    purely. Variable precedence (highest first): runtime > query (stage) >
    outer > source-dataset defaults. Stored query-backed datasets are collected as
    splice placeholders, never as referenced datasets. ``now`` is the execution's
    clock reading (default: the host clock); ``failures`` records skipped unloadable peers;
    ``respelled_spine`` are the query's spellings of the spine respelled to ``time_spine``.
    """
    failures = failures or DocumentLoadFailures()
    named_queries = named_queries or {}
    stage_displays = stage_displays or {}
    storage = cast("StorageBackend", _DatasetReadCache(storage))
    for q in [*named_queries.values(), query]:
        _reject_chain_source(spec=q.population, chain=splice_chain)

    source_dataset = await _resolve_source_spec(
        follow_sibling_chain(spec=query.population, named_queries=named_queries),
        storage=storage, data_source=data_source,
    )

    # Relationships never cross datasource boundaries: scope the walk by the source
    # dataset's own data_source, falling back to the hint only when it carries none.
    walk_ds = source_dataset.data_source or data_source or None
    await _check_spine_spellings(storage=storage, spellings=respelled_spine, data_source=walk_ds)
    spine_clash = walk_ds is not None and (
        await storage.get_dataset(TIME_SPINE_DATASET, data_source=walk_ds)
    ) is not None
    peer_names, peer_edge_names = await _datasource_names(storage=storage, data_source=walk_ds, failures=failures)
    query, named_queries = respelled_stages(query, named_queries, datasets=[*peer_names, TIME_SPINE_DATASET])
    referenced_datasets, query_backed = await _referenced_datasets(
        query=query, source_dataset=source_dataset, named_queries=named_queries,
        storage=storage, data_source=walk_ds, chain=splice_chain, spine_clash=spine_clash, failures=failures,
    )
    source_dataset = query_host(source_dataset, query=query)

    stage_source_datasets = await _stage_source_datasets(
        named_queries=named_queries, storage=storage, data_source=walk_ds or data_source,
    )
    query_variables = merge_query_variables(
        runtime=runtime_variables,
        stage=query.variables,
        outer=outer_variables,
        # A query-backed root's own defaults layer its spliced stages, not the root.
        dataset_defaults=None if source_dataset.source_queries else source_dataset.query_variables,
    )

    ds = await storage.get_datasource(source_dataset.data_source) if source_dataset.data_source else None
    return ResolvedSourceBundle(
        dialect=dialect_for_ds_type(ds.type if ds else None).sqlglot_name,
        datasource_type=ds.type if ds else None,
        population=source_dataset,
        referenced_datasets=referenced_datasets,
        named_queries=dict(named_queries),
        query=query,
        stage_source_datasets=stage_source_datasets,
        query_variables=query_variables,
        datasource_hint=data_source,
        stage_displays=dict(stage_displays),
        query_backed=query_backed,
        splice_chain=tuple(splice_chain),
        runtime_variables=dict(runtime_variables or {}),
        now=datetime.now() if now is None else now,
        granularities=ds.granularity_definitions if ds else {},
        spine_clash=spine_clash,
        datasource_dataset_names=peer_names,
        datasource_edge_names=peer_edge_names,
    )


async def _check_spine_spellings(
    *, storage: "StorageBackend", spellings: frozenset[str], data_source: Optional[str],
) -> None:
    """A spelling respelled to the spine is ambiguous in a datasource storing a dataset named like it in another case."""
    if not spellings or data_source is None:
        return
    variants = sorted(
        n for ds, n in await storage._list_all_dataset_identities()
        if ds == data_source and n != TIME_SPINE_DATASET and is_spine_spelling(n)
    )
    if variants:
        raise AmbiguousReferenceError(
            min(spellings), [TIME_SPINE_DATASET, *variants],
            suggestion=f"Spell the built-in spine {TIME_SPINE_DATASET!r} or the stored dataset exactly.",
        )


async def _datasource_names(
    *, storage: "StorageBackend", data_source: Optional[str], failures: DocumentLoadFailures,
) -> "Tuple[frozenset[str], frozenset[str]]":
    """The dataset names and stored edge names of ``data_source``."""
    peers = failures.skip(await storage.load_datasets(data_source=data_source)) if data_source is not None else []
    return frozenset(m.name for m in peers), frozenset(j.name for m in peers for j in m.relationships if j.name)


async def _stage_source_datasets(
    *,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
) -> Dict[str, SlayerDataset]:
    """Each non-sibling-sourced stage's OWN concrete dataset, so heterogeneous DAGs
    bind against the right host; a query-backed source becomes a spliced sibling instead."""
    sibling_names = set(named_queries)
    out: Dict[str, SlayerDataset] = {}
    for nm, nq in named_queries.items():
        if source_name_if_sibling(spec=nq.population, sibling_names=sibling_names) is not None:
            continue  # sibling-sourced: planner resolves via upstream StageSchema
        # MUST resolve; a failure is a genuine error (a skip would fall back to the root source).
        resolved = await _resolve_source_spec(nq.population, storage=storage, data_source=data_source)
        if not resolved.source_queries:
            out[nm] = query_host(resolved, query=nq)
    return out


def _written_targets(query: SlayerQuery) -> List[str]:
    """The datasets ``query`` names: its extensions' datasets, then its references' first hops."""
    return list(dict.fromkeys([*extension_dataset_names(query), *sorted(first_hop_names(query))]))


async def _query_written_targets(
    *,
    queries: List[SlayerQuery],
    known: "set[str]",
    sibling_names: "set[str]",
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
) -> List[SlayerDataset]:
    """The components of datasets the queries name — as relationship targets or first hops — but the
    relationship-graph walk did not reach (an extension over a sibling carries its own relationships)."""
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
) -> List[SlayerDataset]:
    """The components of the ``names`` datasets not yet ``known`` (absent names skipped)."""
    out: List[SlayerDataset] = []
    for name in names:
        if name in known:
            continue
        dataset = await _frontier_dataset(name=name, all_datasets={}, storage=storage, ds=data_source)
        if dataset is None:
            continue
        fresh = [
            m for m in await _collect_referenced_datasets(
                source_dataset=dataset, named_queries={}, storage=storage, data_source=data_source,
                failures=failures,
            )
            if m.name not in known
        ]
        known.update(m.name for m in fresh)
        out.extend(fresh)
    return out


def _reject_chain_source(*, spec: SourceSpec | None, chain: Tuple[str, ...]) -> None:
    """A source naming a dataset on the in-flight splice chain is a cycle."""
    if spec is not None and spec in chain:
        raise QueryBackedCycleError(path=[*chain[chain.index(spec):], spec])


async def _split_query_backed(
    *,
    datasets: List[SlayerDataset],
    storage: "StorageBackend",
    data_source: Optional[str],
    chain: Tuple[str, ...],
    failures: DocumentLoadFailures,
) -> "Tuple[List[SlayerDataset], Dict[str, SlayerDataset]]":
    """Split ``datasets`` into referenced datasets and query-backed placeholders, closing
    over each placeholder's stage bases and each dataset's definition-named datasets (and
    their components). Placeholders keep their stored form, including declared relationships; on-chain
    names are never loaded."""
    referenced: Dict[str, SlayerDataset] = {}
    query_backed: Dict[str, SlayerDataset] = {}
    pending = list(datasets)
    while pending:
        dataset = pending.pop(0)
        if dataset.name in referenced or dataset.name in query_backed:
            continue
        if not dataset.source_queries:
            referenced[dataset.name] = dataset
            pending.extend(m for m in await _named_components(
                names=definition_heads([dataset]),
                known={*chain, *referenced, *query_backed, *(p.name for p in pending)},
                storage=storage, data_source=data_source, failures=failures,
            ) if not is_spine(m))
            continue
        query_backed[dataset.name] = dataset
        pending.extend(await _stage_base_components(
            dataset=dataset, skip=[*chain, *query_backed], known={*referenced, *query_backed},
            storage=storage, data_source=data_source, failures=failures,
        ))
    return list(referenced.values()), query_backed


async def _stage_base_components(
    *,
    dataset: SlayerDataset,
    skip: List[str],
    known: "set[str]",
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
) -> List[SlayerDataset]:
    """The components of ``dataset``'s stage bases, bar bases named in ``skip`` or resolving into ``known``."""
    out: List[SlayerDataset] = []
    own_stages: list[SlayerQuery] = dataset.source_queries or []
    private = {q.name: q for q in own_stages if q.name}
    for stage in own_stages:
        spec = follow_sibling_chain(spec=stage.population, named_queries=private)
        if spec is None or spec in skip:
            continue
        try:
            base = await _resolve_source_spec(spec, storage=storage, data_source=data_source)
        except ValueError as exc:
            logger.debug("query-backed stage base %r unresolved: %s", spec, exc)
            continue
        if base.name not in known:
            out.extend(await _collect_referenced_datasets(
                source_dataset=base, named_queries={}, storage=storage, data_source=data_source,
                failures=failures,
            ))
    out.extend(await _query_written_targets(
        queries=list(dataset.source_queries or []), known={*known, *skip, *(m.name for m in out)},
        sibling_names=set(private), storage=storage, data_source=data_source, failures=failures,
    ))
    return out


async def _preseed_sibling_datasets(
    *,
    source_dataset: SlayerDataset,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
) -> Dict[str, SlayerDataset]:
    """Datasets held concretely: host + each sibling's stored base. Best-effort — a sibling whose base is absent is
    skipped."""
    preseeded: Dict[str, SlayerDataset] = {source_dataset.name: source_dataset}
    for sib in named_queries.values():
        spec = follow_sibling_chain(spec=sib.population, named_queries=named_queries)
        try:
            sib_dataset = await _resolve_source_spec(
                spec, storage=storage, data_source=data_source
            )
        except ValueError as exc:
            logger.debug("sibling source resolution failed for %r: %s", spec, exc)
            continue
        preseeded.setdefault(sib_dataset.name, sib_dataset)
    return preseeded


async def _load_datasource_peers(
    *,
    preseeded: Dict[str, SlayerDataset],
    ds: Optional[str],
    storage: "StorageBackend",
    failures: DocumentLoadFailures,
) -> Dict[str, SlayerDataset]:
    """Preseeded plus every loadable datasource peer, so reverse edges (a peer
    declaring a relationship *into* a frontier dataset) are discoverable."""
    peers = failures.skip(await storage.load_datasets(data_source=ds)) if ds is not None else []
    return {**preseeded, **{m.name: m for m in peers if m.name not in preseeded}}


async def _frontier_dataset(
    *,
    name: str,
    all_datasets: Dict[str, SlayerDataset],
    storage: "StorageBackend",
    ds: Optional[str],
) -> Optional[SlayerDataset]:
    """The named dataset, loading it on demand; absent targets resolve to None."""
    dataset = all_datasets.get(name)
    if dataset is not None:
        return dataset
    try:
        return await storage.get_dataset(name, data_source=ds)
    except Exception as exc:  # best-effort; absent target is fine
        logger.debug("relationship-target lookup failed for %r: %s", name, exc)
        return None


async def _bfs_connected_component(
    *,
    seeds: List[str],
    all_datasets: Dict[str, SlayerDataset],
    universe: Dict[str, SlayerDataset],
    storage: "StorageBackend",
    ds: Optional[str],
) -> Dict[str, SlayerDataset]:
    """Walk relationships in both directions from the seeds; absent targets skipped."""
    collected: Dict[str, SlayerDataset] = {}
    visited: set[str] = set()
    frontier: List[str] = list(seeds)
    while frontier:
        name = frontier.pop()
        if name in visited:
            continue
        visited.add(name)
        dataset = await _frontier_dataset(
            name=name, all_datasets=all_datasets, storage=storage, ds=ds,
        )
        if dataset is None:
            continue
        collected.setdefault(name, dataset)
        frontier.extend(
            n for n in incident_dataset_names(dataset=dataset, datasets_by_name={**universe, name: dataset})
            if n not in visited
        )
        frontier.extend(_case_ambiguous_targets(dataset=dataset, universe=universe))
    return collected


def _case_ambiguous_targets(*, dataset: SlayerDataset, universe: Dict[str, SlayerDataset]) -> List[str]:
    """The datasets a relationship target of ``dataset`` equals only case-insensitively, when several: a hop through it then
    fails as ambiguous instead of unknown."""
    out: List[str] = []
    for relationship in dataset.relationships:
        if relationship.to in universe:
            continue
        folded = relationship.to.casefold()
        hits = [n for n in universe if n.casefold() == folded]
        out.extend(hits if len(hits) > 1 else [])
    return out


async def _collect_referenced_datasets(
    *,
    source_dataset: SlayerDataset,
    named_queries: Dict[str, SlayerQuery],
    storage: "StorageBackend",
    data_source: Optional[str],
    failures: DocumentLoadFailures,
) -> List[SlayerDataset]:
    """Transitive relationship-graph walk (BFS) over the bidirectional edge set,
    best-effort.

    Seeds: the source dataset plus the real base of every named sibling stage.
    Follows each edge in either direction — a dataset's ``relationships[].to``
    and any datasource dataset that declares a relationship *into* the frontier dataset —
    so the closure is the datasource's connected component. The source dataset
    is returned first.
    """
    preseeded = await _preseed_sibling_datasets(
        source_dataset=source_dataset, named_queries=named_queries,
        storage=storage, data_source=data_source,
    )
    ds = data_source or source_dataset.data_source
    all_datasets = await _load_datasource_peers(
        preseeded=preseeded, ds=ds, storage=storage, failures=failures,
    )
    universe = {**all_datasets, TIME_SPINE_DATASET: source_dataset} if is_spine(source_dataset) else all_datasets
    collected = await _bfs_connected_component(
        # The spine reaches every dataset (a query-backed one's axis is known only once spliced).
        seeds=list(all_datasets if is_spine(source_dataset) else preseeded),
        all_datasets=all_datasets, universe=universe, storage=storage, ds=ds,
    )
    ordered = [source_dataset]
    ordered.extend(m for n, m in collected.items() if n != source_dataset.name)
    return ordered


async def _resolve_source_spec(
    spec: SourceSpec | None,
    *,
    storage: "StorageBackend",
    data_source: Optional[str],
) -> SlayerDataset:
    """Resolve a ``source_dataset`` name to its stored ``SlayerDataset`` (read-only)."""
    if spec == TIME_SPINE_DATASET and data_source is not None:
        return spine_dataset(data_source=data_source)
    if spec is None:
        raise ValueError("A query without population has no stored source to resolve.")
    dataset = await stored_dataset(storage, name=spec, data_source=data_source)
    if dataset is None:
        raise DatasetNotFoundError(spec)
    return dataset


async def stored_dataset(
    storage: "StorageBackend", *, name: str, data_source: Optional[str],
) -> Optional[SlayerDataset]:
    """The stored dataset ``name`` reaches (``slayer.core.names.match_name``)."""
    dataset = await storage.get_dataset(name, data_source=data_source)
    if dataset is not None:
        return dataset
    found = match_name(name=name, declared=await storage.list_datasets(data_source=data_source), quoted=False)
    return None if found is None else await storage.get_dataset(found, data_source=data_source)


class _DatasetReadCache:
    """Read-through ``get_dataset`` cache scoped to one bundle build (delegates the rest)."""

    def __init__(self, inner: "StorageBackend") -> None:
        self._inner = inner
        self._datasets: Dict[tuple, Optional[SlayerDataset]] = {}

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def load_datasets(self, **kwargs: Any) -> Any:
        """The inner backend's ``load_datasets``, reading through this cache."""
        return await StorageBackend.load_datasets(cast(StorageBackend, self), **kwargs)

    async def get_dataset(
        self, name: str, data_source: Optional[str] = None
    ) -> Optional[SlayerDataset]:
        key = (name, data_source)
        if key not in self._datasets:
            dataset = await self._inner.get_dataset(name, data_source=data_source)
            self._datasets[key] = dataset
            if dataset is not None:
                # a hint-less read also answers the concrete-datasource key
                self._datasets.setdefault((name, dataset.data_source), dataset)
        return self._datasets[key]
