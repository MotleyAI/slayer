"""The models a model or a query reads: stage sources, query-written joins, and every hop of every reference path."""

from __future__ import annotations

from collections.abc import Sequence

from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError
from slayer.core.join_walker import OrientedJoin, walk
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery, extension_model_names
from slayer.core.syntax import drop_root_hop, text_paths
from slayer.engine.extensions import QueryScope, extension_of, query_reference_parts, query_scopes
from slayer.engine.param_binding import default_reference_sites
from slayer.engine.reference_closure import parse_fragment_any_dialect
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
    scope = reads.stored_scope(host)
    for parts in query_reference_parts(query):
        reads.ref(parts, host=host, scope=scope)
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


def _prefix_onto_placeholder(
    hops: tuple[str, ...], *, host: SlayerModel, peers: dict[str, SlayerModel],
) -> list[OrientedJoin]:
    """The longest walkable prefix of ``hops`` when it ends on a placeholder, else ``[]``."""
    hops = drop_root_hop(hops, host.spelling, head_quoted=False)
    for end in range(len(hops) - 1, 0, -1):
        try:
            prefix = walk(root=host, path=hops[:end], models_by_name=peers)
        except (AmbiguousJoinPathError, CircularJoinPathError):
            return []
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

    def stored_scope(self, model: SlayerModel) -> QueryScope:
        """The scope a stored definition of ``model`` reads in: its datasource's models."""
        return QueryScope.model_construct(data_source=model.data_source, host=model, models={
            **self.by_ds.get(model.data_source, {}), model.name: model})

    def read(self, name: str, *, scope: QueryScope) -> None:
        if name not in scope.shadowed and name in self.by_ds.get(scope.data_source, {}):
            self.out.add((scope.data_source, name))

    def model(self, model: SlayerModel) -> None:
        if model.source_queries:
            self.stages(list(model.source_queries), data_source=model.data_source or None)
        self.definitions(model, scope=self.stored_scope(model))

    def definitions(self, model: SlayerModel, *, scope: QueryScope) -> None:
        for column in model.columns:
            self.column(host=model, name=column.name, scope=scope)
        for sql in model.filters:
            self.fragment(sql, host=model, scope=scope)
        for agg in model.aggregations:
            for quals, leaf in default_reference_sites(agg):
                self.ref((*quals, leaf), host=model, scope=scope)
        for measure in model.measures:
            if measure.name:
                self.measure(host=model, name=measure.name, scope=scope)

    def column(self, *, host: SlayerModel, name: str, scope: QueryScope) -> None:
        col = host.get_column(name, quoted=True)
        if col is None or not self._first_visit(host, f"column:{name}"):
            return
        if col.sql is not None and not is_trivial_base(column=col):
            self.fragment(col.sql, host=host, scope=scope)
        if col.filter:
            self.fragment(col.filter, host=host, scope=scope)

    def measure(self, *, host: SlayerModel, name: str, scope: QueryScope) -> None:
        measure = host.get_measure(name)
        if measure is None or not measure.formula or not self._first_visit(host, f"measure:{name}"):
            return
        for parts in text_paths(measure.formula, root=None):
            self.ref(parts, host=host, scope=scope)

    def _first_visit(self, host: SlayerModel, key: str) -> bool:
        mark = (id(host), key)
        if mark in self._seen:
            return False
        self._seen.add(mark)
        self._hosts.append(host)
        return True

    def fragment(self, sql: str, *, host: SlayerModel, scope: QueryScope) -> None:
        parsed = parse_fragment_any_dialect(sql)
        if parsed is None:
            return
        for _node, quals, leaf in reference_sites(parsed, root_scope_column_ids(parsed=parsed)):
            self.ref((*quals, leaf), host=host, scope=scope)

    def path(self, hops: Sequence[str], *, host: SlayerModel, scope: QueryScope) -> SlayerModel | None:
        """The model ``hops`` lands on from ``host``, recording each hop; ``None`` when unresolvable."""
        if not drop_root_hop(hops, host.spelling, head_quoted=False):
            return host
        peers = {**scope.models, host.name: host}
        resolved = scope.resolve(hops, root=host)
        if resolved is None:
            self._record(_prefix_onto_placeholder(tuple(hops), host=host, peers=peers), scope=scope)
            return None
        chain = resolved[1]
        if not chain:
            return None
        self._record(chain, scope=scope)
        return peers.get(chain[-1].target_model)

    def _record(self, chain: Sequence[OrientedJoin], *, scope: QueryScope) -> None:
        for edge in chain:
            self.read(edge.target_model, scope=scope)

    def ref(self, parts: Sequence[str], *, host: SlayerModel, scope: QueryScope) -> None:
        """A reference rooted at ``host``: its hops, then the definition of a derived column or saved measure it names;
        one no path reaches from ``host`` is a product read of the model it names."""
        terminal = self.path(tuple(parts[:-1]), host=host, scope=scope)
        if terminal is None:
            if parts[0] != host.spelling:
                self.product_ref(tuple(parts), scope=scope)
            return
        self.column(host=terminal, name=parts[-1], scope=scope)
        self.measure(host=terminal, name=parts[-1], scope=scope)

    def stages(self, queries: list[SlayerQuery], *, data_source: str | None) -> None:
        scopes = query_scopes(queries, models_by_ds=self.by_ds, data_source=data_source)
        for query, query_scopes_ in zip(queries, scopes):
            for scope in query_scopes_:
                self.stage(query, scope=scope)

    def stage(self, query: SlayerQuery, *, scope: QueryScope) -> None:
        """What one query object reads in ``scope``: its source, the models its extensions name, what their own
        definitions read, every reference, and each INNER join's population filter."""
        self.sources(query, scope=scope)
        host = scope.host
        if host is not None and (isinstance(query.source_model, SlayerModel) or extension_of(query, host.name) is not None):
            self.definitions(host, scope=scope)
        self.extension_fields(query, scope=scope)
        for parts in [*query_reference_parts(query), *scope.inner_refs]:
            if host is not None:
                self.ref(parts, host=host, scope=scope)
            else:
                self.product_ref(parts, scope=scope)

    def sources(self, query: SlayerQuery, *, scope: QueryScope) -> None:
        """The stored models ``query`` names as its source, its inline source's join targets and its extensions."""
        spec = query.source_model
        names = list(extension_model_names(query))
        if isinstance(spec, str):
            names.append(spec)
        elif isinstance(spec, SlayerModel):
            names.extend(j.target_model for j in spec.joins)
        for name in names:
            self.read(name, scope=scope)

    def extension_fields(self, query: SlayerQuery, *, scope: QueryScope) -> None:
        """What each of ``query``'s extension columns and measures reads, over the model it extends."""
        for extension in query.extensions or []:
            extended = scope.models.get(extension.source_name)
            if extended is None:
                continue
            for name in [f.name for f in [*(extension.columns or []), *(extension.measures or [])] if f.name]:
                self.column(host=extended, name=name, scope=scope)
                self.measure(host=extended, name=name, scope=scope)

    def product_ref(self, parts: tuple[str, ...], *, scope: QueryScope) -> None:
        """A reference rooted at the model of the scope its head names."""
        root = scope.models.get(parts[0]) if len(parts) > 1 else None
        if root is not None:
            self.read(root.name, scope=scope)
            self.ref(parts[1:], host=root, scope=scope)
