"""Pre-2.0 root-relative query references rewritten to their qualified twins, until SLayer 3.0.

In a query that names its population ``P``: a bare name that is no alias of the query reads ``P.<name>``,
``*`` reads ``P.*``, and a dotted reference whose first segment names no dataset, stage or time spine reads
``P.<reference>``. The rewrite edits only the reference spans, so the rest of each text stays as written.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.deprecation import deprecated_input
from slayer.core.enums import BUILTIN_AGGREGATION_PARAM_ORDER, BUILTIN_AGGREGATIONS, normalize_aggregation_name
from slayer.core.errors import AmbiguousReferenceError
from slayer.core.names import match_name, spelled_name
from slayer.core.query import QueryDimension, QueryMetric, QueryRefinement, SlayerQuery, TimeDimension, declared_aliases
from slayer.core.models import SlayerDataset
from slayer.core.syntax import (
    AggCall, DottedRef, NamedArg, Ref, ScalarCall, StarSource, keyword_spelling, reference_spans, walk_references,
)
from slayer.core.time_spine import TIME_SPINE_DATASET, TIME_SPINE_FIELD, is_spine_spelling
from slayer.core.warnings import DeprecatedInputWarning
from slayer.storage.base import StorageBackend

__all__ = [
    "AnchorNames", "Conversions", "anchor_names", "convert_query", "convert_refinement", "convert_text",
    "population_relative", "qualified_input",
]


class AnchorNames(BaseModel):
    """The names a first segment anchors at (datasets, query stages, the time spine), and per folded population name
    its folded field and metric names (``members``) and custom aggregation names (``aggregations``)."""

    model_config = ConfigDict(frozen=True)

    names: frozenset[str]
    members: Dict[str, frozenset[str]] = {}
    aggregations: Dict[str, frozenset[str]] = {}
    #: Per folded population name, the names a pre-2.0 dotted first segment read from it: its fields and edges.
    heads: Dict[str, frozenset[str]] = {}
    #: Model-scoped metric names, which a bare name reads before any conversion.
    model_metrics: frozenset[str] = frozenset()

    def has_member(self, *, population: str, name: str) -> bool:
        """Whether ``population`` declares a field or metric ``name`` (unknown population: assumed)."""
        members = self.members.get(population.casefold())
        return members is None or name.casefold() in members

    def names_model_metric(self, name: str, *, quoted: bool) -> bool:
        return _is_alias(name, declared=list(self.model_metrics), quoted=quoted)

    def reads_head(self, *, population: str, name: str, quoted: bool) -> bool:
        """Whether a dotted reference starting at ``name`` read from ``population`` before 2.0 (unknown: assumed)."""
        declared = self.heads.get(population.casefold())
        if declared is None:
            return True
        try:
            return match_name(name=name, declared=declared, quoted=quoted) is not None
        except AmbiguousReferenceError:
            return True

    def names_parameter(self, *, population: str, agg: str, name: str) -> bool:
        """Whether ``name = …`` in ``agg(…)`` may name one of its parameters: any in a custom (or overridden)
        aggregation, else a built-in's or a routing one."""
        canonical = normalize_aggregation_name(agg)
        if canonical not in BUILTIN_AGGREGATIONS or agg.casefold() in self.aggregations.get(population.casefold(), ()):
            return True
        builtin = BUILTIN_AGGREGATION_PARAM_ORDER.get(canonical, [])
        return name.casefold() in {"partition_by", "window", *(p.casefold() for p in builtin)}

    def reads_keyword(self, *, population: str, call: str, name: str) -> bool:
        """Whether ``name = …`` in a ``call(…)`` read from ``population`` was no reference before 2.0: a parameter of
        the population's custom aggregation ``call``, or (when the population is known) none of its members."""
        pop = population.casefold()
        if call.casefold() in self.aggregations.get(pop, ()):
            return True
        return pop in self.members and name.casefold() not in self.members[pop]

    def anchors(self, name: str, *, quoted: bool) -> bool:
        try:
            return match_name(name=name, declared=self.names, quoted=quoted) is not None
        except AmbiguousReferenceError:
            return True


class Conversions:
    """The conversions of one input document: each distinct one reported once (none when ``report`` is off)."""

    def __init__(self, *, report: bool = True) -> None:
        self._reported: Dict[Tuple[str, str], DeprecatedInputWarning] = {}
        self._report = report

    def note(self, old: str, new: str) -> None:
        if self._report and (old, new) not in self._reported:
            self._reported[(old, new)] = deprecated_input(old, new, removal="3.0")

    @property
    def payloads(self) -> List[DeprecatedInputWarning]:
        return list(self._reported.values())


def convert_text(
    text: str, *, population: str, aliases: Iterable[str], anchors: AnchorNames, conversions: Conversions,
    edges: Iterable[str] = (),
) -> str:
    """``text`` with each root-relative reference read from ``population``; each conversion is noted."""
    parsed, spans = reference_spans(text)
    declared = list(aliases)
    prefix = spelled_name(population)
    keyword_names = _keyword_names(parsed)
    local_edges = list(edges)
    edits: Dict[Tuple[int, int], str] = {}
    for call, arg in _named_args(parsed):
        span = spans.get(id(arg))
        if span is not None and _converts_argument(call=call.agg, name=arg.name, population=population,
                                                   anchors=anchors, declared=declared):
            written = text[span[0]:span[1]]
            conversions.note(written, f"{prefix}.{written}")
            edits[span] = f"{prefix}.{written}"
    for node, aggregation in walk_references(parsed):
        span = spans.get(id(node))
        call = keyword_names.get(id(node))
        if span is None or (call is not None and anchors.reads_keyword(population=population, call=call,
                                                                        name=node.name)):
            continue
        written = text[span[0]:span[1]]
        if isinstance(node, StarSource):
            agg = aggregation or "count"
            conversions.note(f"{agg}(*)", f"{agg}({prefix}.*)")
            edits[span] = f"{prefix}.*"
        elif _converts_reference(node, population=population, anchors=anchors, declared=declared, edges=local_edges):
            conversions.note(written, f"{prefix}.{written}")
            edits[span] = f"{prefix}.{written}"
    out = text
    for (start, end), replacement in sorted(edits.items(), reverse=True):
        out = out[:start] + replacement + out[end:]
    return out


def _converts_argument(*, call: str, name: str, population: str, anchors: AnchorNames, declared: List[str]) -> bool:
    """Whether ``name = …`` in ``call(…)`` was a root-relative reference."""
    if anchors.names_parameter(population=population, agg=call, name=name):
        return False
    if anchors.reads_keyword(population=population, call=call, name=name):
        return False
    return not (declared and _is_alias(name, declared=declared, quoted=False))


def _converts_reference(node: Any, *, population: str, anchors: AnchorNames, declared: List[str],
                        edges: List[str]) -> bool:
    """Whether the ``Ref`` or ``DottedRef`` ``node`` was read from ``population``."""
    if isinstance(node, Ref):
        return not ((declared and _is_alias(node.name, declared=declared, quoted=node.quoted))
                    or anchors.names_model_metric(node.name, quoted=node.quoted)
                    or not anchors.has_member(population=population, name=node.name))
    head, quoted = node.parts[0], node.quoted_at(0)
    if anchors.anchors(head, quoted=quoted):
        return False
    return anchors.reads_head(population=population, name=head, quoted=quoted) or _is_alias(
        head, declared=edges, quoted=quoted)


def _named_args(node: Any) -> List[Tuple[AggCall, NamedArg]]:
    """Every ``name = value`` argument of an aggregation call in ``node``, with its call."""
    out: List[Tuple[AggCall, NamedArg]] = []
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, AggCall):
            out.extend((current, a) for a in (current.source, *current.args) if isinstance(a, NamedArg))
        if isinstance(current, BaseModel):
            stack.extend(getattr(current, f) for f in type(current).model_fields)
        elif isinstance(current, (tuple, list)):
            stack.extend(current)
    return out


def _keyword_names(node: Any) -> Dict[int, str]:
    """The ``name`` Refs of every ``name = value`` argument of a scalar call in ``node``, by id, to the call's name."""
    out: Dict[int, str] = {}
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, ScalarCall):
            for arg in current.args:
                if keyword_spelling(arg) is not None:
                    out[id(arg.left)] = current.name
        if isinstance(current, BaseModel):
            stack.extend(getattr(current, f) for f in type(current).model_fields)
        elif isinstance(current, (tuple, list)):
            stack.extend(current)
    return out


def _is_alias(name: str, *, declared: List[str], quoted: bool) -> bool:
    try:
        return match_name(name=name, declared=declared, quoted=quoted) is not None
    except AmbiguousReferenceError:
        return True


class _Converter(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    population: str
    aliases: List[str]
    anchors: AnchorNames
    conversions: Any
    edges: List[str] = []

    def text(self, value: str, *, own_alias: Optional[str] = None) -> str:
        """``value`` converted; ``own_alias`` (the entry's own) shadows nothing in its own field."""
        aliases = [a for a in self.aliases if a != own_alias] if own_alias else self.aliases
        try:
            return convert_text(value, population=self.population, aliases=aliases, anchors=self.anchors,
                                conversions=self.conversions, edges=self.edges)
        except ValueError:
            return value  # an unparseable text is reported where it binds

    def entries(self, data: Dict[str, Any], source: Any) -> None:
        """``data`` with the converted entries of ``source`` (a query or refinement) in place of its own."""
        dims = getattr(source, "dimensions", None)
        if dims:
            data["dimensions"] = [self._dimension(d) for d in dims]
        tds = getattr(source, "time_dimensions", None)
        if tds:
            data["time_dimensions"] = [self._time_dimension(td) for td in tds]
        metrics = getattr(source, "metrics", None)
        if metrics:
            data["metrics"] = [self._metric(m) for m in metrics]
        flt = getattr(source, "filter", None)
        if isinstance(flt, str):
            data["filter"] = self.text(flt)
        elif flt:
            data["filter"] = [self.text(c) for c in flt]
        order = getattr(source, "order", None)
        if order:
            data["order"] = [o.model_copy(update={
                "field": self.text(o.field),
                **({"raw_expression": self.text(o.raw_expression)} if o.raw_expression else {}),
            }) for o in order]
        main = getattr(source, "main_time_dimension", None)
        if main:
            data["main_time_dimension"] = self.text(main)

    def _dimension(self, d: QueryDimension) -> QueryDimension:
        new = self.text(d.field, own_alias=d.alias)
        if new == d.field:
            return d
        return d.model_copy(update={"field": new, **({"alias": None} if _noop_alias(d) else {})})

    def _time_dimension(self, td: TimeDimension) -> TimeDimension:
        new = self.text(td.field, own_alias=td.alias)
        return td if new == td.field else td.model_copy(update={"field": new})

    def _metric(self, m: QueryMetric) -> QueryMetric:
        new = self.text(m.metric, own_alias=m.alias)
        return m if new == m.metric else m.model_copy(update={"metric": new})


def _extension_edges(query: SlayerQuery, *, population: str) -> List[str]:
    """The names of the edges ``query``'s extensions declare at ``population``, on either side."""
    folded = population.casefold()
    return [r.name for e in query.extensions or [] for r in e.relationships or []
            if r.name and folded in (e.dataset.casefold(), r.to.casefold())]


def _noop_alias(d: QueryDimension) -> Optional[str]:
    """A 1.x alias naming a plain reference by its own name, which renamed nothing."""
    return d.alias if d.alias is not None and d.ref is not None and not d.is_computed else None


def _live_aliases(*sources: Any) -> List[str]:
    """The declared aliases of ``sources`` that rename something."""
    noop = {a for s in sources for d in (getattr(s, "dimensions", None) or []) if (a := _noop_alias(d))}
    return [a for a in declared_aliases(*sources) if a not in noop]


def _rebuilt(entity: Any, data: Dict[str, Any]) -> Any:
    """``entity``'s class validated over ``data``, keeping ``entity``'s recorded deprecations."""
    rebuilt = type(entity).model_validate(data)
    rebuilt._deprecated_inputs = entity._deprecated_inputs
    return rebuilt


def convert_query(query: SlayerQuery, *, anchors: AnchorNames, conversions: Conversions) -> SlayerQuery:
    """``query`` with its root-relative references qualified by its population; one that names none is kept."""
    population = query.population_name
    if population is None:
        return query
    converter = _Converter(population=population, aliases=_live_aliases(query), anchors=anchors,
                           conversions=conversions, edges=_extension_edges(query, population=population))
    data: Dict[str, Any] = {name: getattr(query, name) for name in query.model_fields_set | {"version"}}
    before = dict(data)
    converter.entries(data, query)
    if all(data[k] is before.get(k) or data[k] == before.get(k) for k in data):
        return query
    return _rebuilt(query, data)


def convert_refinement(
    refinement: QueryRefinement, *, saved: SlayerQuery, anchors: AnchorNames, conversions: Conversions,
) -> QueryRefinement:
    """``refinement`` with its root-relative references qualified by the population of ``saved``."""
    population = saved.population_name
    if population is None:
        return refinement
    converter = _Converter(population=population, aliases=_live_aliases(saved, refinement), anchors=anchors,
                           conversions=conversions, edges=_extension_edges(saved, population=population))
    data: Dict[str, Any] = {name: getattr(refinement, name) for name in refinement.model_fields_set}
    converter.entries(data, refinement)
    data = {k: v for k, v in data.items() if k in refinement.model_fields_set}
    return _rebuilt(refinement, data)


def population_relative(query: SlayerQuery) -> SlayerQuery:
    """``query``, whose every reference is spelled from its population (an internal producer's spelling), as
    qualified references with the same meaning; nothing is reported."""
    return convert_query(query, anchors=AnchorNames(names=frozenset()), conversions=Conversions(report=False))


async def anchor_names(storage: StorageBackend, queries: Sequence[SlayerQuery], *,
                       data_source: Optional[str], refinement: Optional[QueryRefinement] = None) -> AnchorNames:
    """What a first segment of ``queries`` (and ``refinement``) may anchor at: the datasets the caller sees in their
    populations' datasources (``data_source`` when pinned; a query-backed dataset's empty one pins nothing), their
    stages and the spine."""
    identities = await storage._list_all_dataset_identities()
    stage_names = {q.name for q in queries if q.name}
    if not data_source:
        sources = {ds for q in queries if (p := q.population_name) and p not in stage_names
                   for ds, n in identities if n == _matched(p, identities=identities, data_source=ds)}
        chosen = sources or {ds for ds, _ in identities}
    else:
        chosen = {data_source}
    names = {n for ds, n in identities if ds in chosen}
    names |= stage_names | {q.population_name for q in queries if q.population_name}
    names.add(TIME_SPINE_DATASET)
    dotted = _dotted_heads([*queries, *([refinement] if refinement is not None else [])])
    members: Dict[str, frozenset[str]] = {}
    aggregations: Dict[str, frozenset[str]] = {}
    heads: Dict[str, frozenset[str]] = {}
    for population in {q.population_name for q in queries if q.population_name} - stage_names:
        found = await _population_dataset(storage, population, identities=identities, chosen=chosen)
        if found is None:
            if is_spine_spelling(population):
                members[population.casefold()] = heads[population.casefold()] = frozenset({TIME_SPINE_FIELD})
            continue
        dataset_source, dataset = found
        members[population.casefold()] = frozenset(
            n.casefold() for n in [*(f.name for f in dataset.fields), *(m.name for m in dataset.metrics if m.name)])
        aggregations[population.casefold()] = frozenset(a.name.casefold() for a in dataset.aggregations)
        own = [*(f.name for f in dataset.fields), *(r.name for r in dataset.relationships if r.name)]
        if any(not _is_alias(h, declared=[*names, *own], quoted=q) for h, q in dotted):
            # Only an edge declared on a peer is left: reading every peer is the expensive part.
            own += await storage.incident_edge_names(dataset.name, data_source=dataset_source)
        heads[population.casefold()] = frozenset(own)
    model_metrics: set[str] = set()
    for ds in sorted(chosen):
        model_metrics.update(m.name for m in await storage.list_model_scoped_metrics(ds))
    return AnchorNames(names=frozenset(names), members=members, aggregations=aggregations, heads=heads,
                       model_metrics=frozenset(model_metrics))


def _dotted_heads(sources: Sequence[Any]) -> List[Tuple[str, bool]]:
    """``(first segment, quoted)`` of every dotted reference in the entries of ``sources`` (queries, refinements)."""
    out: List[Tuple[str, bool]] = []
    for text in (t for source in sources for t in _entry_texts(source)):
        try:
            parsed = reference_spans(text)[0]
        except ValueError:
            continue
        out.extend((n.parts[0], n.quoted_at(0)) for n, _ in walk_references(parsed) if isinstance(n, DottedRef))
    return out


def _entry_texts(source: Any) -> List[str]:
    texts = [d.field for d in getattr(source, "dimensions", None) or []]
    texts += [td.field for td in getattr(source, "time_dimensions", None) or []]
    texts += [m.metric for m in getattr(source, "metrics", None) or []]
    flt = getattr(source, "filter", None)
    texts += [flt] if isinstance(flt, str) else list(flt or [])
    for o in getattr(source, "order", None) or []:
        texts += [o.field, *([o.raw_expression] if o.raw_expression else [])]
    main = getattr(source, "main_time_dimension", None)
    return [*texts, *([main] if main else [])]


async def _population_dataset(
    storage: StorageBackend, population: str, *, identities: Sequence[Tuple[str, str]], chosen: Iterable[str],
) -> Optional[Tuple[str, SlayerDataset]]:
    """The datasource and stored dataset ``population`` names in the ``chosen`` datasources, when exactly one does."""
    hits = [(ds, n) for ds, n in identities
            if ds in set(chosen) and n == _matched(population, identities=identities, data_source=ds)]
    if len(hits) != 1:
        return None
    dataset = await storage.get_dataset(hits[0][1], data_source=hits[0][0])
    return None if dataset is None else (hits[0][0], dataset)


def _matched(population: str, *, identities: Sequence[Tuple[str, str]], data_source: str) -> Optional[str]:
    """The dataset of ``data_source`` that ``population`` names (unquoted)."""
    try:
        return match_name(name=population, declared=[n for ds, n in identities if ds == data_source], quoted=False)
    except AmbiguousReferenceError:
        return None


async def qualified_input(queries: List[Any], *, storage: StorageBackend, data_source: Optional[str],
                          deprecations_out: List[DeprecatedInputWarning]) -> List[Any]:
    """``queries`` (one input document) with their root-relative references qualified; deprecations to the list."""
    stages = [q for q in queries if isinstance(q, SlayerQuery)]
    if not any(q.population is not None for q in stages):
        return queries
    anchors = await anchor_names(storage, stages, data_source=data_source)
    conversions = Conversions()
    out = [convert_query(q, anchors=anchors, conversions=conversions) if isinstance(q, SlayerQuery) else q
           for q in queries]
    deprecations_out.extend(conversions.payloads)
    return out
