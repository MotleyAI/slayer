"""Stored queries written before qualified references, upgraded to them (``models/stored-upgrade``).

Each reference keeps the path its pre-2.0 reading took from the stage's population: kept as written when the anchored
reading takes the same path along a route no longer than written, else spelled as its explicit route from the
population, which must resolve back to the same relationships. A query that cannot keep its meaning is unconvertible.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple

from slayer.core.errors import UnqualifiableReferenceError
from slayer.core.models import SlayerDataset
from slayer.core.names import NAME_PATTERN, match_name, quoted_name
from slayer.core.query import SlayerQuery, computed_dimension_name, declared_aliases
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
from slayer.core.time_spine import TIME_SPINE_DATASET
from slayer.engine.extensions import resolve_anchored_path, resolve_path
from slayer.sql.naming import expression_text_alias

__all__ = ["qualified_stages"]


def _spelled(name: str) -> str:
    return name if NAME_PATTERN.match(name) else quoted_name(name)


def _same_chain(a: Sequence[Any], b: Sequence[Any]) -> bool:
    return len(a) == len(b) and all(x == y for x, y in zip(a, b))


class _Stage:
    """One stored stage's references read from its population ``population``."""

    def __init__(self, *, query: SlayerQuery, host: Optional[SlayerDataset], stages: frozenset[str],
                 datasets_by_name: Mapping[str, SlayerDataset]) -> None:
        population = query.population_name
        assert population is not None
        self.population = population
        self.prefix = _spelled(host.spelling if host is not None else population)
        self.host = host
        self.stages = stages
        self.datasets = dict(datasets_by_name)
        self.aliases = declared_aliases(query)

    def text(self, value: str) -> Tuple[str, List[str]]:
        """``value`` with its references qualified, and the anchors they start at."""
        parsed, spans = reference_spans(value)
        edits: Dict[Tuple[int, int], str] = {}
        anchors: List[str] = []
        for node, _agg in walk_references(parsed):
            span = spans.get(id(node))
            written = value[span[0]:span[1]] if span is not None else None
            replacement = self._reference(node, written=written)
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

    def _reference(self, node: Any, *, written: Optional[str]) -> Optional[str]:
        """``node``'s qualified spelling; ``None`` for an alias of the stage."""
        if isinstance(node, StarSource):
            return f"{self.prefix}.*"
        if isinstance(node, Ref):
            if self.aliases and match_name(name=node.name, declared=self.aliases, quoted=node.quoted) is not None:
                return None
            return f"{self.prefix}.{written or _spelled(node.name)}"
        assert isinstance(node, DottedRef)
        text = written or ".".join(node.parts)
        if node.parts[0].casefold() == TIME_SPINE_DATASET:
            return text
        if self.host is None:
            first = node.parts[0]
            same = match_name(name=first, declared=[self.population], quoted=node.quoted_at(0)) is not None
            return text if same else f"{self.prefix}.{text}"
        return self._dotted(node, text=text)

    def _dotted(self, node: DottedRef, *, text: str) -> str:
        """A dotted reference kept, or respelled as the explicit route keeping its pre-2.0 path."""
        assert self.host is not None
        hops = tuple(node.parts[:-1])
        old = resolve_path(hops, root=self.host, datasets_by_name=self.datasets)
        if old is None and self.host.get_field(hops[0]) is not None:
            return f"{self.prefix}.{text}"  # a list field's elements
        if old is None:
            raise UnqualifiableReferenceError(reference=text, reason=f"does not resolve from {self.population!r}")
        old_chain = old[1]
        new = resolve_anchored_path(hops, reader=self.host, datasets_by_name=self.datasets)
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
        return ".".join((*explicit, leaf if leaf == "*" else _spelled(leaf)))


def _anchorless(anchors: List[str]) -> bool:
    return len({a.casefold() for a in anchors}) > 1


def _qualified_stage(stage: _Stage, query: SlayerQuery) -> SlayerQuery:
    """``query`` with every reference qualified; an anchorless unaliased expression keeps its column name as alias."""
    data: Dict[str, Any] = {name: getattr(query, name) for name in query.model_fields_set | {"version"}}
    if query.dimensions:
        dims = []
        for d in query.dimensions:
            field, anchors = stage.text(d.field)
            update: Dict[str, Any] = {"field": field}
            if d.alias is None and d.ref is None and _anchorless(anchors):
                update["alias"] = d.name
            dims.append(d.model_copy(update=update))
        data["dimensions"] = dims
    if query.time_dimensions:
        tds = []
        for td in query.time_dimensions:
            field, anchors = stage.text(td.field)
            update = {"field": field}
            if td.alias is None and td.field_ref is None and _anchorless(anchors):
                update["alias"] = computed_dimension_name(td.field)
            tds.append(td.model_copy(update=update))
        data["time_dimensions"] = tds
    if query.metrics:
        metrics = []
        for m in query.metrics:
            text, anchors = stage.text(m.metric)
            update = {"metric": text}
            if m.alias is None and _anchorless(anchors):
                update["alias"] = expression_text_alias(root_relative(parse_expr(m.metric), stage.population))
            metrics.append(m.model_copy(update=update))
        data["metrics"] = metrics
    if isinstance(query.filter, str):
        data["filter"] = stage.text(query.filter)[0]
    elif query.filter:
        data["filter"] = [stage.text(c)[0] for c in query.filter]
    if query.order:
        data["order"] = [o.model_copy(update={
            "field": stage.text(o.field)[0],
            **({"raw_expression": stage.text(o.raw_expression)[0]} if o.raw_expression else {}),
        }) for o in query.order]
    if query.main_time_dimension:
        data["main_time_dimension"] = stage.text(query.main_time_dimension)[0]
    return SlayerQuery.model_validate(data)


def qualified_stages(
    queries: Sequence[SlayerQuery], *, pending: Sequence[bool], datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]],
    data_source: Optional[str], priority: Sequence[str],
) -> List[SlayerQuery]:
    """``queries`` (one stored query list) with each ``pending`` stage's references qualified, keeping their meaning."""
    stages = frozenset(q.name for q in queries if q.name)
    out: List[SlayerQuery] = []
    for query, todo in zip(queries, pending):
        population = query.population_name
        if not todo or population is None:
            out.append(query)
            continue
        datasets = _datasets_holding(population, datasets_by_ds=datasets_by_ds, data_source=data_source,
                                     priority=priority)
        host = None if population in stages else datasets.get(population)
        if host is None and population not in stages:
            raise UnqualifiableReferenceError(reference=population, reason="names no stored dataset or stage")
        out.append(_qualified_stage(_Stage(query=query, host=host, stages=stages, datasets_by_name=datasets), query))
    return out


def _datasets_holding(
    name: str, *, datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]], data_source: Optional[str],
    priority: Sequence[str],
) -> Mapping[str, SlayerDataset]:
    """The datasets of the datasource a stage reading ``name`` runs in."""
    if data_source is not None:
        return datasets_by_ds.get(data_source, {})
    holding = [ds for ds, datasets in datasets_by_ds.items() if name in datasets]
    ordered = [ds for ds in priority if ds in holding] + sorted(ds for ds in holding if ds not in priority)
    return datasets_by_ds[ordered[0]] if ordered else {}
