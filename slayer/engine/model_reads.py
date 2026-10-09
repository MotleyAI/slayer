"""The models a model or a query reads: stage sources, query-written joins, and every hop of every reference path."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any, Literal

from pydantic import BaseModel

from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError
from slayer.core.join_walker import walk
from slayer.core.models import SlayerModel
from slayer.core.query import ComputedDimension, ModelExtension, SlayerQuery, SourceSpec, render_probe_text
from slayer.engine.param_binding import default_reference_sites
from slayer.engine.reference_closure import parse_fragment_any_dialect
from slayer.engine.syntax import DottedRef, Ref, parse_expr, parse_filter_expr
from slayer.ir.source_bundle import apply_extension_overlay, follow_sibling_chain, source_name_if_sibling
from slayer.sql.column_expansion import is_trivial_base, reference_sites, root_scope_column_ids

Identity = tuple[str, str]


def models_read_by(
    subject: SlayerModel | SlayerQuery, *, models: Sequence[SlayerModel], data_source: str | None = None,
) -> frozenset[Identity]:
    """``(data_source, name)`` of every stored model ``subject`` reads, resolved against ``models``."""
    reads = _Reads(models)
    if isinstance(subject, SlayerModel):
        reads.model(subject)
    else:
        reads.stages([subject], data_source=data_source)
    return frozenset(reads.out)


def narrowing_models(models: Sequence[SlayerModel]) -> dict[Identity, list[Identity]]:
    """Each narrowed model's transitively read tagged models whose tags do not cover its own (any, when it is untagged)."""
    by_id = {(m.data_source, m.name): m for m in models}
    reads = {key: models_read_by(m, models=models) for key, m in by_id.items()}
    out: dict[Identity, list[Identity]] = {}
    for key, model in by_id.items():
        own = set(model.access_tags)
        narrowing = sorted(
            r for r in _closure(key, reads=reads)
            if r in by_id and by_id[r].access_tags and not (own and own <= set(by_id[r].access_tags))
        )
        if narrowing:
            out[key] = narrowing
    return out


def _closure(start: Identity, *, reads: dict[Identity, frozenset[Identity]]) -> set[Identity]:
    seen: set[Identity] = set()
    stack = list(reads.get(start, ()))
    while stack:
        key = stack.pop()
        if key not in seen and key != start:
            seen.add(key)
            stack.extend(reads.get(key, ()))
    return seen


FieldKind = Literal["ref", "formula", "filter"]


def query_field_texts(query: SlayerQuery) -> Iterator[tuple[FieldKind, str]]:
    """Every reference-bearing text of ``query`` — dimensions, time dimensions, measures, filters, order — and its kind."""
    for dim in query.dimensions or []:
        if isinstance(dim, ComputedDimension):
            yield "formula", dim.expression
        else:
            yield "ref", dim.full_name
    for td in query.time_dimensions or []:
        yield "ref", td.dimension.full_name
    for measure in query.measures or []:
        if measure.formula:
            yield "formula", measure.formula
    for f in query.filters or []:
        yield "filter", f
    for item in query.order or []:
        if item.raw_formula:
            yield "formula", item.raw_formula
        else:
            yield "ref", item.column.full_name


def query_reference_parts(query: SlayerQuery) -> list[tuple[str, ...]]:
    """The dotted parts of every reference ``query_field_texts`` yields, aggregation arguments included."""
    out: list[tuple[str, ...]] = []
    for kind, text in query_field_texts(query):
        if kind == "ref":
            out.append(tuple(text.split(".")))
        elif kind == "formula":
            out.extend(_expr_parts(text, parse=parse_expr))
        else:
            out.extend(_expr_parts(render_probe_text(text), parse=parse_filter_expr))
    return out


def _expr_parts(text: str, *, parse) -> list[tuple[str, ...]]:
    try:
        parsed = parse(text)
    except Exception:  # noqa: BLE001 — unparseable text reads nothing; the binder reports it
        return []
    return list(_node_parts(parsed))


def _node_parts(node: Any) -> Iterator[tuple[str, ...]]:
    """Parts of every ``Ref`` / ``DottedRef`` in a parsed tree, aggregation and transform arguments included."""
    if isinstance(node, Ref):
        yield (node.name,)
    elif isinstance(node, DottedRef):
        yield tuple(node.parts)
    elif isinstance(node, BaseModel):
        for name in type(node).model_fields:
            yield from _node_parts(getattr(node, name))
    elif isinstance(node, (list, tuple)):
        for item in node:
            yield from _node_parts(item)


class _Reads:
    """Accumulates read identities over the stored ``models``."""

    def __init__(self, models: Sequence[SlayerModel]) -> None:
        self.by_ds: dict[str, dict[str, SlayerModel]] = {}
        for m in models:
            self.by_ds.setdefault(m.data_source, {})[m.name] = m
        self.out: set[Identity] = set()
        self._seen: set[tuple[str, str, str]] = set()

    def resolve(self, name: str, *, data_source: str | None) -> SlayerModel | None:
        """A stored model by name, preferring ``data_source``, else the only datasource holding it."""
        if data_source is not None and name in self.by_ds.get(data_source, {}):
            return self.by_ds[data_source][name]
        holders = [peers[name] for peers in self.by_ds.values() if name in peers]
        return holders[0] if len(holders) == 1 else None

    def peers(self, host: SlayerModel) -> dict[str, SlayerModel]:
        return {**self.by_ds.get(host.data_source, {}), host.name: host}

    def read(self, model: SlayerModel) -> None:
        if model.name in self.by_ds.get(model.data_source, {}):
            self.out.add((model.data_source, model.name))

    def model(self, model: SlayerModel) -> None:
        if model.source_queries:
            self.stages(list(model.source_queries), data_source=model.data_source or None)
        self.definitions(model)

    def definitions(self, model: SlayerModel) -> None:
        for column in model.columns:
            self.column(host=model, name=column.name)
        for sql in model.filters:
            self.fragment(sql, host=model)
        for agg in model.aggregations:
            for quals, leaf in default_reference_sites(agg):
                self.ref((*quals, leaf), host=model)
        for measure in model.measures:
            if measure.name:
                self.measure(host=model, name=measure.name)

    def column(self, *, host: SlayerModel, name: str) -> None:
        col = host.get_column(name)
        if col is None or not self._first_visit(host, f"column:{name}"):
            return
        if col.sql is not None and not is_trivial_base(column=col):
            self.fragment(col.sql, host=host)
        if col.filter:
            self.fragment(col.filter, host=host)

    def measure(self, *, host: SlayerModel, name: str) -> None:
        measure = host.get_measure(name)
        if measure is None or not measure.formula or not self._first_visit(host, f"measure:{name}"):
            return
        for parts in _expr_parts(measure.formula, parse=parse_expr):
            self.ref(parts, host=host)

    def _first_visit(self, host: SlayerModel, key: str) -> bool:
        mark = (host.data_source, host.name, key)
        if mark in self._seen:
            return False
        self._seen.add(mark)
        return True

    def fragment(self, sql: str, *, host: SlayerModel) -> None:
        parsed = parse_fragment_any_dialect(sql)
        if parsed is None:
            return
        for _node, quals, leaf in reference_sites(parsed, root_scope_column_ids(parsed=parsed)):
            self.ref((*quals, leaf), host=host)

    def path(self, hops: Sequence[str], *, host: SlayerModel) -> SlayerModel | None:
        """The model ``hops`` lands on from ``host``, recording each hop; ``None`` when unresolvable."""
        hops = tuple(hops)
        if hops[:1] == (host.name,):
            hops = hops[1:]
        if not hops:
            return host
        peers = self.peers(host)
        try:
            chain = walk(root=host, path=hops, models_by_name=peers)
        except (AmbiguousJoinPathError, CircularJoinPathError):
            return None
        if not chain:
            return None
        for edge in chain:
            self.out.add((host.data_source, edge.target_model))
        return peers.get(chain[-1].target_model)

    def ref(self, parts: Sequence[str], *, host: SlayerModel) -> None:
        """A reference rooted at ``host``: its hops, then the definition of a derived column or saved measure it names."""
        terminal = self.path(tuple(parts[:-1]), host=host)
        if terminal is None:
            return
        self.column(host=terminal, name=parts[-1])
        self.measure(host=terminal, name=parts[-1])

    def stages(self, queries: list[SlayerQuery], *, data_source: str | None) -> None:
        named = {q.name: q for q in queries[:-1] if q.name}
        for query in queries:
            if isinstance(query.source_model, ModelExtension):
                self.written_joins(query.source_model, data_source=data_source)
            try:
                spec = follow_sibling_chain(spec=query.source_model, named_queries=named)
            except ValueError:
                continue
            host = self.source(spec, data_source=data_source)
            if source_name_if_sibling(spec=query.source_model, sibling_names=named) is not None:
                continue  # fields name the sibling's flat columns
            for parts in query_reference_parts(query):
                self.query_ref(parts, host=host, data_source=data_source)

    def source(self, spec: SourceSpec | None, *, data_source: str | None) -> SlayerModel | None:
        """The host a stage source resolves to, recording what it reads."""
        if isinstance(spec, str):
            model = self.resolve(spec, data_source=data_source)
            if model is not None:
                self.read(model)
            return model
        if isinstance(spec, ModelExtension):
            self.written_joins(spec, data_source=data_source)
            base = self.resolve(spec.source_name, data_source=data_source)
            if base is None:
                return None
            self.read(base)
            host = apply_extension_overlay(base, spec)
            self.definitions(host)
            return host
        if isinstance(spec, SlayerModel):
            self.written_joins(spec, data_source=spec.data_source or data_source)
            self.definitions(spec)
            return spec
        return None

    def written_joins(self, spec: ModelExtension | SlayerModel, *, data_source: str | None) -> None:
        for join in spec.joins or []:
            target = self.resolve(join.target_model, data_source=data_source)
            if target is not None:
                self.read(target)

    def query_ref(self, parts: tuple[str, ...], *, host: SlayerModel | None, data_source: str | None) -> None:
        if host is not None:
            self.ref(parts, host=host)
            return
        if len(parts) < 2:
            return
        root = self.resolve(parts[0], data_source=data_source)
        if root is not None:
            self.read(root)
            self.ref(parts[1:], host=root)
