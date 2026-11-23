"""Stored queries written before qualified references, upgraded to them (``models/stored-upgrade``).

Each reference keeps the path its pre-2.0 reading took from the stage's population: kept as written when the anchored
reading takes the same path along a route no longer than written, else spelled as its explicit route from the
population, which must resolve back to the same relationships. A query that cannot keep its meaning is unconvertible.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.errors import AmbiguousReferenceError, UnqualifiableReferenceError
from slayer.core.models import SlayerDataset
from slayer.core.names import match_name, quoted_name, spelled_name
from slayer.core.query import (
    BarePosition, QueryDimension, QueryMetric, SlayerQuery, TimeDimension, computed_dimension_name, declared_aliases,
    extension_dataset_names, pre_qualified_reading,
)
from slayer.core.relationship_walker import canonical_path
from slayer.core.syntax import (
    DottedRef,
    Ref,
    StarSource,
    parse_expr,
    reference_spans,
    root_relative,
    walk_references,
)
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine_spelling, spine_dataset
from slayer.engine.extensions import (
    extend_datasets,
    query_reference_parts,
    resolve_anchored_path,
    resolve_path,
    respelled,
    sibling_host,
    stage_output_names,
)
from slayer.sql.naming import expression_text_alias

__all__ = ["StageReading", "qualified_stages", "stage_readings"]



def _same_chain(a: Sequence[Any], b: Sequence[Any]) -> bool:
    return len(a) == len(b) and all(x == y for x, y in zip(a, b))


class _PopulationNames(BaseModel):
    """The field and metric names a stage's population offers its bare names."""

    model_config = ConfigDict(frozen=True)

    fields: Tuple[str, ...] = ()
    metrics: Tuple[str, ...] = ()


class _Stage:
    """One stored stage's references read from its population ``population``; with ``names``, a bare name keeps its
    pre-2.0 positional reading."""

    def __init__(self, *, query: SlayerQuery, host: Optional[SlayerDataset], stages: frozenset[str],
                 datasets_by_name: Mapping[str, SlayerDataset], names: Optional[_PopulationNames] = None) -> None:
        population = query.population_name
        assert population is not None
        self.population = population
        self.prefix = spelled_name(host.spelling if host is not None else population)
        self.host = host
        self.stages = stages
        self.datasets = dict(datasets_by_name)
        self.aliases = declared_aliases(query)
        self.names = names

    def text(self, value: str, *, position: BarePosition, own: Optional[str] = None) -> Tuple[str, List[str]]:
        """``value`` with its references qualified, and the anchors they start at; ``own`` is its entry's alias."""
        parsed, spans = reference_spans(value)
        edits: Dict[Tuple[int, int], str] = {}
        anchors: List[str] = []
        for node, _agg in walk_references(parsed):
            span = spans.get(id(node))
            written = value[span[0]:span[1]] if span is not None else None
            replacement = self._reference(node, written=written, position=position, own=own)
            if replacement is None:
                continue
            anchors.append(replacement.split(".", 1)[0])
            if span is None or replacement == written:
                continue
            edits[span] = replacement
        out = value
        for (start, end), replacement in sorted(edits.items(), reverse=True):
            out = out[:start] + replacement + out[end:]
        return out, anchors

    def _reference(self, node: Any, *, written: Optional[str], position: BarePosition, own: Optional[str]
                   ) -> Optional[str]:
        """``node``'s qualified spelling; ``None`` for an alias of the stage."""
        if isinstance(node, StarSource):
            return f"{self.prefix}.*"
        if isinstance(node, Ref):
            return None if self._reads_alias(node, position=position, own=own) else (
                f"{self.prefix}.{written or spelled_name(node.name)}")
        assert isinstance(node, DottedRef)
        text = written or ".".join(node.parts)
        if node.parts[0].casefold() == TIME_SPINE_DATASET:
            return text
        if self.host is None:
            first = node.parts[0]
            same = match_name(name=first, declared=[self.population], quoted=node.quoted_at(0)) is not None
            return text if same else f"{self.prefix}.{text}"
        return self._dotted(node, text=text)

    def _reads_alias(self, node: Ref, *, position: BarePosition, own: Optional[str]) -> bool:
        if self.names is None:
            return bool(self.aliases) and match_name(name=node.name, declared=self.aliases, quoted=node.quoted) is not None
        aliases = [a for a in self.aliases if a != own]
        return pre_qualified_reading(node.name, quoted=node.quoted, position=position, aliases=aliases,
                                     fields=self.names.fields, metrics=self.names.metrics) == "alias"

    def _dotted(self, node: DottedRef, *, text: str) -> str:
        """A dotted reference kept, or respelled as the explicit route keeping its pre-2.0 path."""
        assert self.host is not None
        hops = tuple(node.parts[:-1])
        quoted = tuple(node.quoted_at(i) for i in range(len(hops)))
        old = resolve_path(hops, root=self.host, datasets_by_name=self.datasets, quoted=quoted)
        if old is None and self.host.get_field(hops[0]) is not None:
            return f"{self.prefix}.{text}"  # a list field's elements
        if old is None:
            raise UnqualifiableReferenceError(reference=text, reason=f"does not resolve from {self.population!r}")
        old_chain = old[1]
        new = resolve_anchored_path(hops, reader=self.host, datasets_by_name=self.datasets, quoted=quoted)
        anchored_here = match_name(name=hops[0], declared=[self.host.spelling], quoted=node.quoted_at(0)) is not None
        written_len = len(hops) - (1 if anchored_here else 0)
        if new is not None and _same_chain(new[1], old_chain) and len(old_chain) == written_len:
            return text
        explicit = (self.prefix, *canonical_path(old_chain))
        again = resolve_anchored_path(explicit, reader=self.host, datasets_by_name=self.datasets)
        if again is None or not _same_chain(again[1], old_chain):
            raise UnqualifiableReferenceError(
                reference=text, reason=f"reads a path from {self.population!r} that no qualified spelling names")
        leaf = node.parts[-1]
        spelled = quoted_name(leaf) if node.quoted_at(len(hops)) else spelled_name(leaf)
        return ".".join((*explicit, leaf if leaf == "*" else spelled))


def _anchorless(anchors: List[str]) -> bool:
    return len({a.casefold() for a in anchors}) > 1


def _qualified_stage(stage: _Stage, query: SlayerQuery) -> SlayerQuery:
    """``query`` with every reference qualified; an anchorless unaliased expression keeps its column name as alias."""
    data: Dict[str, Any] = {name: getattr(query, name) for name in query.model_fields_set | {"version"}}
    if query.dimensions:
        data["dimensions"] = [_qualified_dimension(stage, d) for d in query.dimensions]
    if query.time_dimensions:
        data["time_dimensions"] = [_qualified_time_dimension(stage, td) for td in query.time_dimensions]
    if query.metrics:
        data["metrics"] = [_qualified_metric(stage, m) for m in query.metrics]
    if isinstance(query.filter, str):
        data["filter"] = stage.text(query.filter, position="filter")[0]
    elif query.filter:
        data["filter"] = [stage.text(c, position="filter")[0] for c in query.filter]
    if query.order:
        data["order"] = [o.model_copy(update={
            "field": stage.text(o.field, position="filter")[0],
            **({"raw_expression": stage.text(o.raw_expression, position="filter")[0]} if o.raw_expression else {}),
        }) for o in query.order]
    if query.main_time_dimension:
        data["main_time_dimension"] = stage.text(query.main_time_dimension, position="entry")[0]
    return SlayerQuery.model_validate(data)


def _qualified_dimension(stage: _Stage, d: QueryDimension) -> QueryDimension:
    field, anchors = stage.text(d.field, position="entry", own=d.name if d.alias or d.ref is None else None)
    update: Dict[str, Any] = {"field": field}
    if d.alias is None and d.ref is None and _anchorless(anchors):
        update["alias"] = d.name
    return d.model_copy(update=update)


def _qualified_time_dimension(stage: _Stage, td: TimeDimension) -> TimeDimension:
    field, anchors = stage.text(td.field, position="entry", own=td.alias)
    update: Dict[str, Any] = {"field": field}
    if td.alias is None and td.field_ref is None and _anchorless(anchors):
        update["alias"] = computed_dimension_name(td.field)
    return td.model_copy(update=update)


def _qualified_metric(stage: _Stage, m: QueryMetric) -> QueryMetric:
    text, anchors = stage.text(m.metric, position="entry", own=m.alias)
    update: Dict[str, Any] = {"metric": text}
    if m.alias is None and _anchorless(anchors):
        update["alias"] = expression_text_alias(root_relative(parse_expr(m.metric), stage.population))
    return m.model_copy(update=update)


def qualified_stages(
    queries: Sequence[SlayerQuery], *, pending: Sequence[bool], datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]],
    data_source: Optional[str], priority: Sequence[str],
) -> List[SlayerQuery]:
    """``queries`` (one stored query list) with each ``pending`` stage's references qualified, keeping their meaning."""
    stages = frozenset(q.name for q in queries if q.name)
    by_name = {q.name: q for q in queries if q.name}
    out: List[SlayerQuery] = []
    for query, todo in zip(queries, pending):
        if not todo:
            out.append(query)
            continue
        datasets = _stage_universe(query, stages=stages, datasets_by_ds=datasets_by_ds, data_source=data_source,
                                   priority=priority)
        # An ambiguous spine spelling is qualified as written: it stays as ambiguous as it was, refused where it runs.
        name, is_stage = _read_from(query, stages=stages, datasets=datasets)
        host = datasets[name] if name is not None and not is_stage else None
        read_from = query if query.population_name is not None else query.model_copy(update={"population": name})
        names = None if query.population_name is None else _population_names(
            host=host, stage=by_name.get(name) if is_stage and name is not None else None)
        qualified = _qualified_stage(
            _Stage(query=read_from, host=host, stages=stages, datasets_by_name=datasets, names=names), query)
        out.append(qualified)
        if query.name:
            by_name[query.name] = qualified
    return out


def _population_names(*, host: Optional[SlayerDataset], stage: Optional[SlayerQuery]) -> Optional[_PopulationNames]:
    """What a stage's population offers its bare names: an earlier stage's or an unexpanded query-backed dataset's
    output columns as fields; ``None`` when unknown."""
    if stage is not None:
        return _PopulationNames(fields=tuple(stage_output_names(stage)))
    if host is None:
        return None
    metrics = tuple(m.name for m in host.metrics if m.name)
    if not host.awaits_fields:
        return _PopulationNames(fields=tuple(f.name for f in host.fields), metrics=metrics)
    last = (host.source_queries or [None])[-1]
    return _PopulationNames(fields=tuple(stage_output_names(last)), metrics=metrics) if isinstance(
        last, SlayerQuery) else None


class StageReading(BaseModel):
    """The datasource a stored stage runs in, and its references' reading: its population (``host``; ``None`` when
    unreadable) over ``datasets``.

    Built with ``model_construct``: its datasets are already validated, and a stage's host has no source.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    data_source: Optional[str] = None
    host: Optional[SlayerDataset] = None
    datasets: Dict[str, SlayerDataset] = Field(default_factory=dict)


def stage_readings(
    queries: Sequence[SlayerQuery], *, datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]],
    data_source: Optional[str], priority: Sequence[str],
) -> List[StageReading]:
    """Per stage of ``queries`` (one stored query list), the reading qualification keeps, each other stage standing in
    as the host the static readers build; unreadable where qualification refuses."""
    stages = frozenset(q.name for q in queries if q.name)
    out: List[StageReading] = []
    for query in queries:
        kwargs = {"stages": stages, "datasets_by_ds": datasets_by_ds, "data_source": data_source, "priority": priority}
        ds, _ = _datasource_of(query, datasets_by_ds=datasets_by_ds, data_source=data_source, priority=priority)
        try:
            name, _ = _read_from(query, stages=stages, datasets=_stage_universe(query, **kwargs))
        except (AmbiguousReferenceError, UnqualifiableReferenceError):
            out.append(StageReading.model_construct(data_source=ds, host=None, datasets={}))
            continue
        datasets = _stage_universe(query, **kwargs, siblings=True)
        out.append(StageReading.model_construct(
            data_source=ds, host=datasets.get(name) if name is not None else None, datasets=datasets))
    return out


def _read_from(query: SlayerQuery, *, stages: frozenset[str], datasets: Mapping[str, SlayerDataset]
               ) -> Tuple[Optional[str], bool]:
    """The dataset, or stage (flagged), a stage reads its references from; ``None`` for a spine spelling another stored
    dataset shares. Raises when it names neither."""
    population = query.population_name
    if population is None:
        return _inferred_population(query, datasets=datasets), False
    stage = match_name(name=population, declared=stages, quoted=False)
    if stage is not None:
        return stage, True
    if _ambiguous_spine(population, datasets=datasets):
        return None, False
    dataset = _population_dataset(population, datasets=datasets)
    if dataset is None:
        raise UnqualifiableReferenceError(reference=population, reason="names no stored dataset or stage")
    return dataset, False


def _inferred_population(query: SlayerQuery, *, datasets: Mapping[str, SlayerDataset]) -> str:
    """The population a root-less stage was read from before 2.0: the dataset that reaches every reference its
    dimensions and filters read outside aggregations, in the fewest hops; unconvertible when none or several do."""
    items = list(dict.fromkeys(_determination_refs(query)))
    hops: Dict[str, int] = {}
    for name, dataset in datasets.items():
        if name == TIME_SPINE_DATASET or not items:
            continue
        counts = [_old_reach(item, root=dataset, datasets=datasets) for item in items]
        if all(c is not None for c in counts):
            hops[name] = sum(c for c in counts if c is not None)
    best = [n for n, h in hops.items() if h == min(hops.values())] if hops else []
    if len(best) != 1:
        raise UnqualifiableReferenceError(
            reference=", ".join(".".join(i) for i in items) or "(no dimension or filter)",
            reason="names no population, and no single dataset reaches what it reads" if not best
            else f"names no population, and {sorted(best)} each reach what it reads",
        )
    return best[0]


def _determination_refs(query: SlayerQuery) -> List[Tuple[str, ...]]:
    """The references a root-less stage's dimensions, time dimensions and filters read outside aggregations."""
    texts = [*(d.field for d in query.dimensions or []), *(td.field for td in query.time_dimensions or []),
             *query.filter_conditions]
    refs: List[Tuple[str, ...]] = []
    for text in texts:
        try:
            parsed = parse_expr(text)
        except ValueError:
            continue
        for node, aggregation in walk_references(parsed):
            if aggregation is None and isinstance(node, (Ref, DottedRef)):
                refs.append((node.name,) if isinstance(node, Ref) else tuple(node.parts))
    return refs


def _old_reach(parts: Tuple[str, ...], *, root: SlayerDataset, datasets: Mapping[str, SlayerDataset]) -> Optional[int]:
    """How many hops the pre-2.0 reading of ``parts`` from ``root`` took; ``None`` when it did not resolve."""
    if len(parts) == 1:
        return 0 if root.get_field(parts[0]) is not None or root.get_metric(parts[0]) is not None else None
    old = resolve_path(tuple(parts[:-1]), root=root, datasets_by_name=dict(datasets))
    return None if old is None else len(old[1])


def _ambiguous_spine(population: str, *, datasets: Mapping[str, SlayerDataset]) -> bool:
    """Whether ``population`` spells the spine in another case beside a stored dataset that spells it too."""
    return (population not in datasets and is_spine_spelling(population)
            and any(n != TIME_SPINE_DATASET and is_spine_spelling(n) for n in datasets))


def _population_dataset(population: str, *, datasets: Mapping[str, SlayerDataset]) -> Optional[str]:
    """The dataset a population names as the engine reads it: an exact name, else the spine in any case, else by case."""
    if population in datasets:
        return population
    if is_spine_spelling(population) and TIME_SPINE_DATASET in datasets:
        return TIME_SPINE_DATASET
    return match_name(name=population, declared=datasets, quoted=False)


def _stage_universe(
    query: SlayerQuery, *, stages: frozenset[str], datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]],
    data_source: Optional[str], priority: Sequence[str], siblings: bool = False,
) -> Dict[str, SlayerDataset]:
    """The datasets a stage reads, as the engine sees them: its datasource's, the spine, (with ``siblings``) each other
    stage's host, and its extensions."""
    ds, stored = _datasource_of(query, datasets_by_ds=datasets_by_ds, data_source=data_source, priority=priority)
    universe = dict(stored)
    if ds is not None and TIME_SPINE_DATASET not in universe:
        universe[TIME_SPINE_DATASET] = spine_dataset(data_source=ds)
    if siblings:
        universe.update({n: sibling_host(n, query=query, data_source=ds or "") for n in stages if n != query.name})
    if not query.extensions:
        return universe
    try:
        spelled = respelled(query, tiers=[stages, universe])
    except AmbiguousReferenceError:
        spelled = query
    extended, _ = extend_datasets(spelled, universe, lookup=lambda name, held: held.get(name),
                                  on_conflict=lambda _exc: None)
    return extended


def _datasource_of(
    query: SlayerQuery, *, datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]], data_source: Optional[str],
    priority: Sequence[str],
) -> Tuple[Optional[str], Mapping[str, SlayerDataset]]:
    """The datasource a stage runs in, and its datasets: the pinned one, else the first holding its population, else
    (on the spine, which every datasource holds) the first holding a dataset it names, else the first."""
    if data_source is not None:
        return data_source, datasets_by_ds.get(data_source, {})
    population = query.population_name or ""
    holding = _holding([population], datasets_by_ds=datasets_by_ds)
    if not holding and is_spine_spelling(population):
        named = [*extension_dataset_names(query), *(p[0] for p in query_reference_parts(query) if len(p) > 1)]
        holding = _holding([n for n in named if not is_spine_spelling(n)], datasets_by_ds=datasets_by_ds) or list(
            datasets_by_ds)
    ordered = [ds for ds in priority if ds in holding] + sorted(ds for ds in holding if ds not in priority)
    return (ordered[0], datasets_by_ds[ordered[0]]) if ordered else (None, {})


def _holding(names: Sequence[str], *, datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]]) -> List[str]:
    return [ds for ds, datasets in datasets_by_ds.items() if any(_holds(datasets, name=n) for n in names)]


def _holds(datasets: Mapping[str, SlayerDataset], *, name: str) -> bool:
    try:
        return match_name(name=name, declared=datasets, quoted=False) is not None
    except AmbiguousReferenceError:
        return True
