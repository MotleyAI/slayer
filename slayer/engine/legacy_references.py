"""Pre-2.0 root-relative query references rewritten to their qualified twins, until SLayer 3.0.

In a query that names its population ``P``: a bare name that is no alias of the query reads ``P.<name>``,
``*`` reads ``P.*``, and a dotted reference whose first segment names no dataset, stage or time spine reads
``P.<reference>``. The rewrite edits only the reference spans, so the rest of each text stays as written.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.deprecation import deprecated_input
from slayer.core.errors import AmbiguousReferenceError
from slayer.core.names import NAME_PATTERN, match_name, quoted_name
from slayer.core.query import QueryDimension, QueryMetric, QueryRefinement, SlayerQuery, TimeDimension, declared_aliases
from slayer.core.syntax import DottedRef, Ref, StarSource, reference_spans, walk_references
from slayer.core.warnings import DeprecatedInputWarning

__all__ = ["AnchorNames", "Conversions", "convert_query", "convert_refinement", "convert_text"]


class AnchorNames(BaseModel):
    """The names a first segment anchors at: datasets, query stages and the time spine."""

    model_config = ConfigDict(frozen=True)

    names: frozenset[str]

    def anchors(self, name: str, *, quoted: bool) -> bool:
        try:
            return match_name(name=name, declared=self.names, quoted=quoted) is not None
        except AmbiguousReferenceError:
            return True


class Conversions:
    """The conversions of one input document: each distinct one reported once."""

    def __init__(self) -> None:
        self._reported: Dict[Tuple[str, str], DeprecatedInputWarning] = {}

    def note(self, old: str, new: str) -> None:
        if (old, new) not in self._reported:
            self._reported[(old, new)] = deprecated_input(old, new, removal="3.0")

    @property
    def payloads(self) -> List[DeprecatedInputWarning]:
        return list(self._reported.values())


def _spelling(name: str) -> str:
    return name if NAME_PATTERN.match(name) else quoted_name(name)


def convert_text(
    text: str, *, population: str, aliases: Iterable[str], anchors: AnchorNames, conversions: Conversions,
) -> str:
    """``text`` with each root-relative reference read from ``population``; each conversion is noted."""
    parsed, spans = reference_spans(text)
    declared = list(aliases)
    prefix = _spelling(population)
    edits: Dict[Tuple[int, int], str] = {}
    for node, aggregation in walk_references(parsed):
        span = spans.get(id(node))
        if span is None:
            continue
        start, end = span
        written = text[start:end]
        if isinstance(node, StarSource):
            agg = aggregation or "count"
            conversions.note(f"{agg}(*)", f"{agg}({prefix}.*)")
            edits[span] = f"{prefix}.*"
            continue
        if isinstance(node, Ref):
            if declared and _is_alias(node.name, declared=declared, quoted=node.quoted):
                continue
        elif anchors.anchors(node.parts[0], quoted=node.quoted_at(0)):
            continue
        replacement = f"{prefix}.{written}"
        conversions.note(written, replacement)
        edits[span] = replacement
    out = text
    for (start, end), replacement in sorted(edits.items(), reverse=True):
        out = out[:start] + replacement + out[end:]
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

    def text(self, value: str) -> str:
        try:
            return convert_text(value, population=self.population, aliases=self.aliases, anchors=self.anchors,
                                conversions=self.conversions)
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
        new = self.text(d.field)
        return d if new == d.field else d.model_copy(update={"field": new})

    def _time_dimension(self, td: TimeDimension) -> TimeDimension:
        new = self.text(td.field)
        return td if new == td.field else td.model_copy(update={"field": new})

    def _metric(self, m: QueryMetric) -> QueryMetric:
        new = self.text(m.metric)
        return m if new == m.metric else m.model_copy(update={"metric": new})


def _rebuilt(model: Any, data: Dict[str, Any]) -> Any:
    """``model``'s class validated over ``data``, keeping ``model``'s recorded deprecations."""
    rebuilt = type(model).model_validate(data)
    rebuilt._deprecated_inputs = model._deprecated_inputs
    return rebuilt


def convert_query(query: SlayerQuery, *, anchors: AnchorNames, conversions: Conversions) -> SlayerQuery:
    """``query`` with its root-relative references qualified by its population; one that names none is kept."""
    population = query.population_name
    if population is None:
        return query
    converter = _Converter(population=population, aliases=declared_aliases(query), anchors=anchors,
                           conversions=conversions)
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
    converter = _Converter(population=population, aliases=declared_aliases(saved, refinement), anchors=anchors,
                           conversions=conversions)
    data: Dict[str, Any] = {name: getattr(refinement, name) for name in refinement.model_fields_set}
    converter.entries(data, refinement)
    data = {k: v for k, v in data.items() if k in refinement.model_fields_set}
    return _rebuilt(refinement, data)
