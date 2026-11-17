"""Query-scoped extensions: one overlay step per stage over the models it reads, and the static scope its readers share."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Dict, FrozenSet, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import JoinType
from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    AmbiguousReferenceError,
    CircularRelationshipPathError,
    DatasetNotFoundError,
    QueryExtensionError,
    TimeSpineError,
)
from slayer.core.relationship_edges import EdgeMerge
from slayer.core.relationship_walker import OrientedRelationship, resolve_hop, walk
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.names import match_name
from slayer.core.query import ComputedDimension, DatasetExtension, SlayerQuery, extension_dataset_names
from slayer.core.scope import DatasetScope, StageSchema
from slayer.core.syntax import drop_root_hop, drop_root_qualifier, text_paths
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine
from slayer.engine.population import dimension_routing
from slayer.ir.source_bundle import (
    InnerRestriction,
    ResolvedSourceBundle,
    follow_sibling_chain,
    overlay_metrics,
)

__all__ = [
    "FieldKind",
    "QueryScope",
    "apply_query_extensions",
    "declaring_dataset_path",
    "extension_of",
    "query_field_texts",
    "query_reference_parts",
    "query_scopes",
    "resolve_path",
    "respelled",
    "respelled_stages",
    "with_own_extension",
]

FieldKind = Literal["ref", "expression", "filter"]
_Lookup = Callable[[str, Dict[str, SlayerDataset]], Optional[SlayerDataset]]


def extension_of(query: SlayerQuery, name: str) -> Optional[DatasetExtension]:
    """``query``'s extension of model ``name``, if any."""
    return next((e for e in query.extensions or [] if e.source_name == name), None)


def respelled(query: SlayerQuery, *, tiers: Sequence[Iterable[str]]) -> SlayerQuery:
    """``query`` with each model name it writes — its source and its extensions' models — spelled as the first of
    ``tiers`` that declares it (``slayer.core.names.match_name``); an undeclared name stays
    as written."""
    declared = [list(dict.fromkeys(tier)) for tier in tiers]

    def spell(name: str) -> str:
        found = (match_name(name=name, declared=names, quoted=False) for names in declared)
        return next((f for f in found if f is not None), name)

    def relationships(written: Optional[List[DatasetRelationship]]) -> Optional[List[DatasetRelationship]]:
        if not any(spell(j.to) != j.to for j in written or []):
            return written
        return [j.model_copy(update={"to": spell(j.to)}) for j in written or []]

    def extension(e: DatasetExtension) -> DatasetExtension:
        name, own = spell(e.source_name), relationships(e.relationships)
        return e if name == e.source_name and own is e.relationships else e.model_copy(update={"source_name": name, "relationships": own})

    update: Dict[str, object] = {}
    spec = query.population
    if spec is not None and spell(spec) != spec:
        update["source_model"] = spell(spec)
    extensions = [extension(e) for e in query.extensions or []]
    if any(new is not old for new, old in zip(extensions, query.extensions or [])):
        update["extensions"] = extensions
    return query.model_copy(update=update) if update else query


def respelled_stages(
    query: SlayerQuery, named_queries: Mapping[str, SlayerQuery], *, datasets: Iterable[str],
) -> Tuple[SlayerQuery, Dict[str, SlayerQuery]]:
    """``respelled`` per stage: the stages shadow ``datasets``."""
    datasets = list(datasets)

    def one(stage: SlayerQuery) -> SlayerQuery:
        return respelled(stage, tiers=_stage_tiers(stages=named_queries, datasets=datasets))

    return one(query), {name: one(stage) for name, stage in named_queries.items()}


def _stage_tiers(*, stages: Iterable[str], datasets: Iterable[str]) -> List[List[str]]:
    return [list(stages), list(datasets)]


def with_own_extension(dataset: Optional[SlayerDataset], query: SlayerQuery) -> Optional[SlayerDataset]:
    """``dataset`` with the measures ``query`` extends it with."""
    extension = extension_of(query, dataset.name) if dataset is not None else None
    return overlay_metrics(dataset, extension) if dataset is not None and extension is not None else dataset


def query_field_texts(query: SlayerQuery) -> Iterator[Tuple[FieldKind, str]]:
    """Every reference-bearing text of ``query`` — dimensions, time dimensions, measures, filters, order — and its kind."""
    yield from _dimension_texts(query)
    for measure in query.metrics or []:
        if measure.expression:
            yield "expression", measure.expression
    for f in query.filter or []:
        yield "filter", f
    for item in query.order or []:
        yield ("expression", item.raw_expression) if item.raw_expression else ("ref", item.field.full_name)


def _dimension_texts(query: SlayerQuery) -> Iterator[Tuple[FieldKind, str]]:
    for dim in query.dimensions or []:
        yield ("expression", dim.expression) if isinstance(dim, ComputedDimension) else ("ref", dim.full_name)
    for td in query.time_dimensions or []:
        yield "expression", td.field


def query_reference_parts(query: SlayerQuery) -> List[Tuple[str, ...]]:
    """The dotted parts of every reference ``query_field_texts`` yields, aggregation arguments included."""
    out: List[Tuple[str, ...]] = []
    for kind, text in query_field_texts(query):
        out.extend([tuple(text.split("."))] if kind == "ref" else text_paths(text, root=None))
    return out


def extend_datasets(
    query: SlayerQuery, datasets: Mapping[str, SlayerDataset], *, lookup: _Lookup,
    on_conflict: Callable[[ValueError], None], where: Optional[str] = None,
    reserved_names: Sequence[str] = (), stored_edge_names: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, SlayerDataset], List[InnerRestriction]]:
    """``datasets`` as ``query`` extends them, and its INNER joins' population filters.

    ``lookup`` resolves a model an extension names (``None`` skips it); ``on_conflict`` sees each conflicting join.
    """
    out = dict(datasets)
    for extension in query.extensions or []:
        model = lookup(extension.source_name, out)
        if model is not None:
            out[model.name] = overlay_metrics(model, extension)
    merge = EdgeMerge(datasets=out, reserved_names=reserved_names, stored_edge_names=stored_edge_names)
    inner = [
        (extension.source_name, join)
        for extension in query.extensions or [] if extension.source_name in merge.datasets
        for join in extension.relationships or []
        if _merged(declaring=extension.source_name, relationship=join, merge=merge, lookup=lookup, on_conflict=on_conflict)
        and join.join_type is JoinType.INNER
    ]
    # Tokens come from the final graph: a later declaration may name an edge an earlier one created.
    final = EdgeMerge(datasets=merge.datasets)
    return merge.datasets, [_restriction(declaring=d, relationship=j, merge=final, where=where) for d, j in inner]


def _merged(
    *, declaring: str, relationship: DatasetRelationship, merge: EdgeMerge, lookup: _Lookup, on_conflict: Callable[[ValueError], None],
) -> bool:
    """Merge one extension join into ``merge``; whether it merged."""
    if relationship.to not in merge.datasets and lookup(relationship.to, merge.datasets) is None:
        return False
    try:
        merge.add(declaring=declaring, relationship=relationship)
    except ValueError as exc:  # QueryExtensionError included
        on_conflict(exc)
        return False
    return True


def _restriction(*, declaring: str, relationship: DatasetRelationship, merge: EdgeMerge, where: Optional[str]) -> InnerRestriction:
    """The population filter of the INNER ``relationship``, read off the merged graph it already belongs to."""
    merged = merge.add(declaring=declaring, relationship=relationship)
    source, target = merge.datasets[declaring], merge.datasets[relationship.to]
    return InnerRestriction(
        dataset=source.spelling, token=merged.token, key=merged.edge.join_pairs[0][1],
        label=f"INNER join from {source.spelling!r} to {target.spelling!r}" + (f" in {where}" if where else ""),
    )


def declaring_dataset_path(
    restriction: InnerRestriction, *, host: Optional[SlayerDataset], datasets_by_name: Dict[str, SlayerDataset],
) -> Tuple[str, ...]:
    """The population's path to ``restriction.model``: as spelled when root or adjacent, else short-form routed."""
    spelled = (restriction.dataset,)
    if host is None or not drop_root_qualifier((*spelled, restriction.key), host.spelling, quoted=())[:-1]:
        return spelled
    datasets_by_name = {**datasets_by_name, host.name: datasets_by_name.get(host.name, host)}
    try:
        if resolve_hop(current=host, token=restriction.dataset, datasets_by_name=datasets_by_name) is not None:
            return spelled
    except AmbiguousRelationshipPathError:
        return spelled
    return tuple(dimension_routing.route_dotted_target(
        root=host, target_dataset=restriction.dataset, leaf=restriction.token, datasets_by_name=datasets_by_name,
    ))


def resolve_path(
    hops: Sequence[str], *, root: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Tuple[Tuple[str, ...], List[OrientedRelationship]]]:
    """``hops`` from ``root`` as the binder resolves them — its own qualifier dropped, walked, a lone unmatched hop
    short-form routed — with the edges walked; ``None`` when unresolvable or ambiguous."""
    hops = drop_root_hop(hops, root.spelling, head_quoted=False)
    try:
        chain = walk(root=root, path=hops, datasets_by_name=datasets_by_name)
        if chain is None and len(hops) == 1 and resolve_hop(
                current=root, token=hops[0], datasets_by_name=datasets_by_name) is None:
            route = dimension_routing.short_form_route_or_none(
                root=root, target_dataset=hops[0], datasets_by_name=datasets_by_name)
            hops = tuple(route or ())
            chain = walk(root=root, path=hops, datasets_by_name=datasets_by_name) if route else None
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    return (hops, chain) if chain is not None else None


class QueryScope(BaseModel):
    """One query object's models as the engine resolves them, for the static readers (decision 18).

    Built with ``model_construct``: its models are already validated, and a sibling stage's host has no source.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    data_source: str
    # The query's source as bound; ``None`` when inferred.
    host: Optional[SlayerDataset] = None
    # The stored model a sibling stage's chain ends on (``None``: unresolved); ``""`` keys the host's own.
    bases: Dict[str, Optional[str]] = Field(default_factory=dict)
    datasets: Dict[str, SlayerDataset] = Field(default_factory=dict)
    # Each INNER extension join's population filter reference from the host: hops, edge token, target key.
    inner_refs: Tuple[Tuple[str, ...], ...] = ()
    # Stored model names a sibling stage stands in for.
    shadowed: FrozenSet[str] = frozenset()
    # The query object with its extension model names respelled (``respelled``).
    query: Optional[SlayerQuery] = None

    @property
    def siblings(self) -> FrozenSet[str]:
        return frozenset(n for n in self.bases if n)

    @property
    def base(self) -> Optional[str]:
        return self.bases.get("")

    def resolve(self, hops: Sequence[str], *, root: Optional[SlayerDataset] = None):
        """``resolve_path`` over this scope, from ``root`` (default: the host)."""
        start = root if root is not None else self.host
        return None if start is None else resolve_path(hops, root=start, datasets_by_name=self.datasets)


def query_scopes(
    queries: Sequence[SlayerQuery], *, datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]],
    data_source: Optional[str],
) -> List[List[QueryScope]]:
    """Per query of a query list (the root last), its scope in each datasource it may read (lenient: conflicts skip)."""
    staged = _respelled_leniently(queries, stored={})
    named = {q.name: q for q in staged[:-1] if q.name}
    respelled_by_ds: Dict[str, List[SlayerQuery]] = {}

    def scope(index: int, ds: str) -> QueryScope:
        stored = datasets_by_ds.get(ds, {})
        if ds not in respelled_by_ds:
            respelled_by_ds[ds] = _respelled_leniently(staged, stored=stored)
        stages = respelled_by_ds[ds]
        own = {q.name: q for q in stages[:-1] if q.name}
        return _scope(stages[index], data_source=ds, named=own, stored=stored)

    return [
        [scope(i, ds) for ds in _data_sources(query, named=named, datasets_by_ds=datasets_by_ds, data_source=data_source)]
        for i, query in enumerate(staged)
    ]


def _respelled_leniently(queries: Sequence[SlayerQuery], *, stored: Mapping[str, SlayerDataset]) -> List[SlayerQuery]:
    """``respelled_stages`` over ``stored``; an ambiguous name stays as written (the engine refuses it)."""
    stages = [q.name for q in queries[:-1] if q.name]
    out: List[SlayerQuery] = []
    for query in queries:
        try:
            out.append(respelled(query, tiers=_stage_tiers(stages=stages, datasets=stored)))
        except AmbiguousReferenceError:
            out.append(query)
    return out


def _chain_base(spec, *, named: Mapping[str, SlayerQuery]) -> Tuple[bool, Optional[str]]:
    """``(resolved, base name)`` of a sibling chain; a cycle does not resolve."""
    try:
        return True, follow_sibling_chain(spec=spec, named_queries=dict(named))
    except ValueError:
        return False, None


def _stored_base(spec, *, named: Mapping[str, SlayerQuery]) -> Optional[str]:
    """The stored model a sibling chain ends on; ``None`` for a cycle."""
    resolved, base = _chain_base(spec, named=named)
    return base if resolved else None


def _chain(query: SlayerQuery, *, named: Mapping[str, SlayerQuery]) -> Iterator[SlayerQuery]:
    """``query`` and the sibling stages its source chain passes through."""
    seen: set = set()
    current: Optional[SlayerQuery] = query
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        spec = current.population
        current = named.get(spec) if isinstance(spec, str) else None


def _data_sources(
    query: SlayerQuery, *, named: Mapping[str, SlayerQuery], datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]],
    data_source: Optional[str],
) -> List[str]:
    """The datasources ``query`` may read: ``data_source`` when it holds a model the query's chain names, else every
    datasource that does."""
    resolved, base = _chain_base(query.population, named=named)
    if not resolved:
        return [data_source] if data_source is not None else []
    names = {base} if base is not None else {
        n for q in _chain(query, named=named)
        for n in [*extension_dataset_names(q), *(p[0] for p in query_reference_parts(q) if len(p) > 1)]
    }
    folded = {n.casefold() for n in names}

    def holds(datasets: Mapping[str, SlayerDataset]) -> bool:
        return any(m.casefold() in folded for m in datasets)

    if data_source is not None and (not names or holds(datasets_by_ds.get(data_source, {}))):
        return [data_source]
    holders = sorted(ds for ds, models in datasets_by_ds.items() if holds(models))
    return holders or ([data_source] if data_source is not None else [])


def _scope(
    query: SlayerQuery, *, data_source: str, named: Mapping[str, SlayerQuery], stored: Mapping[str, SlayerDataset],
) -> QueryScope:
    siblings = [n for n in named if n != query.name]
    spec = query.population
    datasets: Dict[str, SlayerDataset] = {
        **stored, **{n: _sibling_host(n, query=query, data_source=data_source) for n in siblings}}
    host_name = spec if _chain_base(spec, named=named)[0] else None
    datasets, restrictions = extend_datasets(
        query, datasets, lookup=lambda name, universe: universe.get(name), on_conflict=lambda _exc: None)
    host = datasets.get(host_name) if host_name is not None else None
    bases = {"": _stored_base(spec, named=named), **{n: _stored_base(n, named=named) for n in siblings}}
    return QueryScope.model_construct(
        data_source=data_source, host=host, bases=bases, datasets=datasets, shadowed=frozenset(siblings),
        query=query,
        inner_refs=tuple(r for r in (_inner_ref(x, host=host, datasets=datasets) for x in restrictions) if r is not None),
    )


def _sibling_host(name: str, *, query: SlayerQuery, data_source: str) -> SlayerDataset:
    """Sibling stage ``name`` as ``query`` reads it: the output fields its extension joins name, on either side."""
    keys = [
        pair[0 if e.source_name == name else 1]
        for e in query.extensions or [] for j in e.relationships or [] if name in (e.source_name, j.to)
        for pair in j.join_pairs
    ]
    return SlayerDataset.model_construct(
        name=name, data_source=data_source, fields=[DatasetField(name=k) for k in dict.fromkeys(keys)])


def _inner_ref(
    restriction: InnerRestriction, *, host: Optional[SlayerDataset], datasets: Dict[str, SlayerDataset],
) -> Optional[Tuple[str, ...]]:
    try:
        path = declaring_dataset_path(restriction, host=host, datasets_by_name=datasets)
    except ValueError:  # an unreachable or ambiguous declaring model fails the engine, not the static reader
        return None
    return (*drop_root_hop(path, host.spelling if host is not None else None, head_quoted=False), restriction.token,
            restriction.key)


def _extended(
    name: str, *, datasets: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle, where: Optional[str],
) -> SlayerDataset:
    model = datasets.get(name)
    if name == TIME_SPINE_DATASET or (model is not None and is_spine(model)):
        raise TimeSpineError(
            summary=f"{TIME_SPINE_DATASET!r} cannot be extended.",
            suggestion="Bucket its timestamp in time_dimensions; add columns or joins to the dataset models instead.",
        )
    if model is not None:
        return model
    failure = bundle.splice_failures.get(name)
    if failure is not None:
        raise failure
    raise DatasetNotFoundError(name, context=f"An extension of {where}" if where else None)


def _reserved_names(datasets: Dict[str, SlayerDataset], bundle: ResolvedSourceBundle) -> List[str]:
    return [
        *bundle.datasource_dataset_names, *(m.name for m in datasets.values() if m.explicit_spelling is None),
        *(m.explicit_spelling for m in datasets.values() if m.explicit_spelling is not None),
        *(d.name for d in bundle.stage_displays.values()),
    ]


def apply_query_extensions(
    *, query: SlayerQuery, scope: "DatasetScope | StageSchema | None", bundle: ResolvedSourceBundle,
) -> "Tuple[DatasetScope | StageSchema | None, ResolvedSourceBundle]":
    """``query``'s extensions applied to the stage's models: measures overlaid, joins merged into the join graph,
    and each INNER join recorded as a population filter."""
    if not query.extensions:
        return scope, bundle
    display = bundle.stage_displays.get(query.name) if query.name else None
    where = display.label if display is not None else None
    universe = _universe(bundle)

    def conflict(exc: ValueError) -> None:
        if isinstance(exc, QueryExtensionError):
            raise QueryExtensionError(f"{where}: {exc}" if where else str(exc)) from exc
        raise exc

    models, restrictions = extend_datasets(
        query, universe, where=where, on_conflict=conflict,
        lookup=lambda name, models: _extended(name, datasets=models, bundle=bundle, where=where),
        reserved_names=_reserved_names(universe, bundle),
        stored_edge_names=[
            *bundle.datasource_edge_names,
            *(j.name for m in universe.values() for j in m.relationships if j.name and not j.query_local),
        ],
    )
    source = models[bundle.population.name] if bundle.population is not None else None
    bundle = bundle.model_copy(update={
        "population": source,
        "referenced_datasets": [models.get(m.name, m) for m in bundle.referenced_datasets],
        "inner_restrictions": (*bundle.inner_restrictions, *restrictions),
    })
    return _extended_scope(scope, query=query, datasets=models, source=source), bundle


def _universe(bundle: ResolvedSourceBundle) -> Dict[str, SlayerDataset]:
    out: Dict[str, SlayerDataset] = {}
    if bundle.population is not None:
        out[bundle.population.name] = bundle.population
    for model in bundle.referenced_datasets:
        out.setdefault(model.name, model)
    return out


def _extended_scope(
    scope: "DatasetScope | StageSchema | None", *, query: SlayerQuery, datasets: Dict[str, SlayerDataset],
    source: Optional[SlayerDataset],
) -> "DatasetScope | StageSchema | None":
    if isinstance(scope, DatasetScope) and scope.population is not None:
        return DatasetScope(population=datasets.get(scope.population.name, scope.population))
    if isinstance(scope, StageSchema) and source is not None and _touches(query, source.name):
        return DatasetScope(population=source)
    return scope


def _touches(query: SlayerQuery, name: str) -> bool:
    """Whether ``query`` extends model ``name`` or joins to it."""
    return extension_of(query, name) is not None or any(
        j.to == name for e in query.extensions or [] for j in e.relationships or [])
