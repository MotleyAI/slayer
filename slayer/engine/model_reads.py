"""The models a model or a query reads: stage sources, query-written joins, and every hop of every reference path."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal


from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError
from slayer.core.join_walker import OrientedJoin, walk
from slayer.core.models import SlayerModel
from slayer.core.query import ComputedDimension, SlayerQuery, SourceSpec, extension_model_names
from slayer.engine.param_binding import default_reference_sites
from slayer.engine.reference_closure import parse_fragment_any_dialect
from slayer.core.syntax import drop_root_hop, text_paths
from slayer.ir.source_bundle import follow_sibling_chain, overlay_columns_and_measures, source_name_if_sibling
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


def models_read_closure(query: SlayerQuery, *, host: SlayerModel, models: Sequence[SlayerModel]) -> list[SlayerModel]:
    """``host`` and every model ``query``, rooted at ``host``, reads — directly or through what it reads."""
    reads = _Reads(models)
    reads.model(host)
    for parts in query_reference_parts(query):
        reads.ref(parts, host=host)
    by_id = {(m.data_source, m.name): m for m in models}
    pending, seen = list(reads.out), set()
    while pending:
        key = pending.pop()
        if key in seen or key not in by_id:
            continue
        seen.add(key)
        pending.extend(models_read_by(by_id[key], models=models))
    return [host, *(by_id[key] for key in sorted(seen) if by_id[key] is not host)]


def placeholder(identity: Identity) -> SlayerModel:
    """Stand-in for a stored model that cannot be loaded: no source, columns or joins, so a path can only end on it."""
    data_source, name = identity
    return SlayerModel.model_construct(name=name, data_source=data_source)


def _is_placeholder(model: SlayerModel) -> bool:
    return not (model.sql_table or model.sql or model.source_queries)


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
    yield from _dimension_texts(query)
    for measure in query.measures or []:
        if measure.formula:
            yield "formula", measure.formula
    for f in query.filters or []:
        yield "filter", f
    for item in query.order or []:
        yield ("formula", item.raw_formula) if item.raw_formula else ("ref", item.column.full_name)


def _dimension_texts(query: SlayerQuery) -> Iterator[tuple[FieldKind, str]]:
    for dim in query.dimensions or []:
        yield ("formula", dim.expression) if isinstance(dim, ComputedDimension) else ("ref", dim.full_name)
    for td in query.time_dimensions or []:
        yield "formula", td.dimension


def query_reference_parts(query: SlayerQuery) -> list[tuple[str, ...]]:
    """The dotted parts of every reference ``query_field_texts`` yields, aggregation arguments included."""
    out: list[tuple[str, ...]] = []
    for kind, text in query_field_texts(query):
        out.extend([tuple(text.split("."))] if kind == "ref" else text_paths(text, root=None))
    return out


def _prefix_onto_placeholder(
    hops: tuple[str, ...], *, host: SlayerModel, peers: dict[str, SlayerModel],
) -> list[OrientedJoin]:
    """The longest walkable prefix of ``hops`` when it ends on a placeholder, else ``[]``."""
    for end in range(len(hops) - 1, 0, -1):
        prefix = walk(root=host, path=hops[:end], models_by_name=peers)
        if prefix is not None:
            return prefix if prefix and _is_placeholder(peers[prefix[-1].target_model]) else []
    return []


class _Reads:
    """Accumulates read identities over the stored ``models``."""

    def __init__(self, models: Sequence[SlayerModel]) -> None:
        self.by_ds: dict[str, dict[str, SlayerModel]] = {}
        for m in models:
            self.by_ds.setdefault(m.data_source, {})[m.name] = m
        self.out: set[Identity] = set()
        # Keyed by host object, not name: two stages' overlays of one model may define a field differently.
        self._seen: set[tuple[int, str]] = set()
        self._hosts: list[SlayerModel] = []  # keeps each visited host alive, so its id is never reused

    def holders(self, name: str, *, data_source: str | None) -> list[SlayerModel]:
        """The stored model ``name`` in ``data_source`` if there is one, else every datasource's model of that name."""
        if data_source is not None and name in self.by_ds.get(data_source, {}):
            return [self.by_ds[data_source][name]]
        return [peers[name] for peers in self.by_ds.values() if name in peers]

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
        for parts in text_paths(measure.formula, root=None):
            self.ref(parts, host=host)

    def _first_visit(self, host: SlayerModel, key: str) -> bool:
        mark = (id(host), key)
        if mark in self._seen:
            return False
        self._seen.add(mark)
        self._hosts.append(host)
        return True

    def fragment(self, sql: str, *, host: SlayerModel) -> None:
        parsed = parse_fragment_any_dialect(sql)
        if parsed is None:
            return
        for _node, quals, leaf in reference_sites(parsed, root_scope_column_ids(parsed=parsed)):
            self.ref((*quals, leaf), host=host)

    def path(self, hops: Sequence[str], *, host: SlayerModel) -> SlayerModel | None:
        """The model ``hops`` lands on from ``host``, recording each hop; ``None`` when unresolvable."""
        hops = drop_root_hop(hops, host.spelling)
        if not hops:
            return host
        peers = self.peers(host)
        try:
            chain = walk(root=host, path=hops, models_by_name=peers)
            if chain is None:
                self._record(_prefix_onto_placeholder(hops, host=host, peers=peers), host=host)
                return None
        except (AmbiguousJoinPathError, CircularJoinPathError):
            return None
        if not chain:
            return None
        self._record(chain, host=host)
        return peers.get(chain[-1].target_model)

    def _record(self, chain: Sequence[OrientedJoin], *, host: SlayerModel) -> None:
        for edge in chain:
            self.out.add((host.data_source, edge.target_model))

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
            self.extended_models(query, data_source=data_source)
            try:
                spec = follow_sibling_chain(spec=query.source_model, named_queries=named)
            except ValueError:
                continue
            hosts = [self._extended(host, query=query) for host in self.source(spec, data_source=data_source)]
            if source_name_if_sibling(spec=query.source_model, sibling_names=named) is not None:
                continue  # fields name the sibling's flat columns
            for parts in query_reference_parts(query):
                for host in hosts:
                    self.ref(parts, host=host)
                if not hosts:
                    self.unhosted_ref(parts, data_source=data_source)

    def extended_models(self, query: SlayerQuery, *, data_source: str | None) -> None:
        for name in extension_model_names(query):
            for model in self.holders(name, data_source=data_source):
                self.read(model)

    def source(self, spec: SourceSpec | None, *, data_source: str | None) -> list[SlayerModel]:
        """The hosts a stage source may resolve to (every holder of an ambiguous name), recording what they read."""
        if isinstance(spec, str):
            models = self.holders(spec, data_source=data_source)
            for model in models:
                self.read(model)
            return models
        if isinstance(spec, SlayerModel):
            self.written_joins(spec, data_source=spec.data_source or data_source)
            self.definitions(spec)
            return [spec]
        return []

    def _extended(self, host: SlayerModel, *, query: SlayerQuery) -> SlayerModel:
        """``host`` with the columns and measures ``query`` extends it with, their definitions read."""
        extension = next((e for e in query.extensions or [] if e.source_name == host.name), None)
        if extension is None:
            return host
        extended = overlay_columns_and_measures(host, extension)
        self.definitions(extended)
        return extended

    def written_joins(self, spec: SlayerModel, *, data_source: str | None) -> None:
        for join in spec.joins:
            for target in self.holders(join.target_model, data_source=data_source):
                self.read(target)

    def unhosted_ref(self, parts: tuple[str, ...], *, data_source: str | None) -> None:
        if len(parts) < 2:
            return
        for root in self.holders(parts[0], data_source=data_source):
            self.read(root)
            self.ref(parts[1:], host=root)
