"""The datasets a dataset or a query reads: stage sources, query-written relationships, and every hop of every reference path."""

from __future__ import annotations

from collections.abc import Sequence

from slayer.core.errors import AmbiguousRelationshipPathError, CircularRelationshipPathError
from slayer.core.relationship_walker import OrientedRelationship, walk
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery, extension_dataset_names
from slayer.core.syntax import drop_root_hop, text_paths
from slayer.engine.extensions import QueryScope, extension_of, query_reference_parts, query_scopes
from slayer.engine.param_binding import default_reference_sites
from slayer.engine.reference_closure import parse_fragment_any_dialect
from slayer.sql.field_expansion import is_trivial_base, reference_sites, root_scope_field_ids

Identity = tuple[str, str]


def datasets_read_by(
    subject: SlayerDataset | SlayerQuery, *, datasets: Sequence[SlayerDataset], data_source: str | None = None,
) -> frozenset[Identity]:
    """``(data_source, name)`` of every stored dataset ``subject`` reads, resolved against ``datasets``."""
    reads = _Reads(datasets)
    if isinstance(subject, SlayerDataset):
        reads.dataset(subject)
    else:
        reads.stages([subject], data_source=data_source)
    return frozenset(reads.out)


def datasets_read_closure(query: SlayerQuery, *, host: SlayerDataset, datasets: Sequence[SlayerDataset]) -> list[SlayerDataset]:
    """``host`` and every dataset ``query``, rooted at ``host``, reads — directly or through what it reads."""
    reads = _Reads(datasets)
    reads.dataset(host)
    scope = reads.stored_scope(host)
    for parts in query_reference_parts(query):
        reads.ref(parts, host=host, scope=scope)
    by_id = {(m.data_source, m.name): m for m in datasets}
    pending, seen = list(reads.out), set()
    while pending:
        key = pending.pop()
        if key in seen or key not in by_id:
            continue
        seen.add(key)
        pending.extend(datasets_read_by(by_id[key], datasets=datasets))
    return [host, *(by_id[key] for key in sorted(seen) if by_id[key] is not host)]


def placeholder(identity: Identity) -> SlayerDataset:
    """Stand-in for a stored dataset that cannot be loaded: no source, fields or relationships, so a path can only end on it."""
    data_source, name = identity
    return SlayerDataset.model_construct(name=name, data_source=data_source)


def _is_placeholder(dataset: SlayerDataset) -> bool:
    return not (dataset.sql_table or dataset.sql or dataset.source_queries)


def narrowing_datasets(datasets: Sequence[SlayerDataset]) -> dict[Identity, list[Identity]]:
    """Each narrowed dataset's transitively read tagged datasets whose tags do not cover its own (any, when it is untagged)."""
    by_id = {(m.data_source, m.name): m for m in datasets}
    reads = {key: datasets_read_by(m, datasets=datasets) for key, m in by_id.items()}
    out: dict[Identity, list[Identity]] = {}
    for key, dataset in by_id.items():
        own = set(dataset.access_tags)
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
    hops: tuple[str, ...], *, host: SlayerDataset, peers: dict[str, SlayerDataset],
) -> list[OrientedRelationship]:
    """The longest walkable prefix of ``hops`` when it ends on a placeholder, else ``[]``."""
    hops = drop_root_hop(hops, host.spelling, head_quoted=False)
    for end in range(len(hops) - 1, 0, -1):
        try:
            prefix = walk(root=host, path=hops[:end], datasets_by_name=peers)
        except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
            return []
        if prefix is not None:
            return prefix if prefix and _is_placeholder(peers[prefix[-1].to]) else []
    return []


class _Reads:
    """Accumulates read identities over the stored ``datasets``."""

    def __init__(self, datasets: Sequence[SlayerDataset]) -> None:
        self.by_ds: dict[str, dict[str, SlayerDataset]] = {}
        for m in datasets:
            self.by_ds.setdefault(m.data_source, {})[m.name] = m
        self.out: set[Identity] = set()
        # Keyed by host object, not name: two stages' overlays of one dataset may define a field differently.
        self._seen: set[tuple[int, str]] = set()
        self._hosts: list[SlayerDataset] = []  # keeps each visited host alive, so its id is never reused

    def stored_scope(self, dataset: SlayerDataset) -> QueryScope:
        """The scope a stored definition of ``dataset`` reads in: its datasource's datasets."""
        return QueryScope.model_construct(data_source=dataset.data_source, host=dataset, datasets={
            **self.by_ds.get(dataset.data_source, {}), dataset.name: dataset})

    def read(self, name: str, *, scope: QueryScope) -> None:
        if name not in scope.shadowed and name in self.by_ds.get(scope.data_source, {}):
            self.out.add((scope.data_source, name))

    def dataset(self, dataset: SlayerDataset) -> None:
        if dataset.source_queries:
            self.stages(list(dataset.source_queries), data_source=dataset.data_source or None)
        self.definitions(dataset, scope=self.stored_scope(dataset))

    def definitions(self, dataset: SlayerDataset, *, scope: QueryScope) -> None:
        for field in dataset.fields:
            self.field(host=dataset, name=field.name, scope=scope)
        for sql in dataset.filters:
            self.fragment(sql, host=dataset, scope=scope)
        for agg in dataset.aggregations:
            for quals, leaf in default_reference_sites(agg):
                self.ref((*quals, leaf), host=dataset, scope=scope)
        for metric in dataset.metrics:
            if metric.name:
                self.metric(host=dataset, name=metric.name, scope=scope)

    def field(self, *, host: SlayerDataset, name: str, scope: QueryScope) -> None:
        col = host.get_field(name, quoted=True)
        if col is None or not self._first_visit(host, f"field:{name}"):
            return
        if col.sql is not None and not is_trivial_base(field=col):
            self.fragment(col.sql, host=host, scope=scope)
        if col.filter:
            self.fragment(col.filter, host=host, scope=scope)

    def metric(self, *, host: SlayerDataset, name: str, scope: QueryScope) -> None:
        metric = host.get_metric(name)
        if metric is None or not metric.expression or not self._first_visit(host, f"metric:{name}"):
            return
        for parts in text_paths(metric.expression, root=None):
            self.ref(parts, host=host, scope=scope)

    def _first_visit(self, host: SlayerDataset, key: str) -> bool:
        mark = (id(host), key)
        if mark in self._seen:
            return False
        self._seen.add(mark)
        self._hosts.append(host)
        return True

    def fragment(self, sql: str, *, host: SlayerDataset, scope: QueryScope) -> None:
        parsed = parse_fragment_any_dialect(sql)
        if parsed is None:
            return
        for _node, quals, leaf in reference_sites(parsed, root_scope_field_ids(parsed=parsed)):
            self.ref((*quals, leaf), host=host, scope=scope)

    def path(self, hops: Sequence[str], *, host: SlayerDataset, scope: QueryScope) -> SlayerDataset | None:
        """The dataset ``hops`` lands on from ``host``, recording each hop; ``None`` when unresolvable."""
        if not drop_root_hop(hops, host.spelling, head_quoted=False):
            return host
        peers = {**scope.datasets, host.name: host}
        resolved = scope.resolve(hops, root=host)
        if resolved is None:
            self._record(_prefix_onto_placeholder(tuple(hops), host=host, peers=peers), scope=scope)
            return None
        chain = resolved[1]
        if not chain:
            return None
        self._record(chain, scope=scope)
        return peers.get(chain[-1].to)

    def _record(self, chain: Sequence[OrientedRelationship], *, scope: QueryScope) -> None:
        for edge in chain:
            self.read(edge.to, scope=scope)

    def ref(self, parts: Sequence[str], *, host: SlayerDataset, scope: QueryScope) -> None:
        """A reference rooted at ``host``: its hops, then the definition of a derived field or saved metric it names;
        one no path reaches from ``host`` is a product read of the dataset it names."""
        terminal = self.path(tuple(parts[:-1]), host=host, scope=scope)
        if terminal is None:
            if parts[0] != host.spelling:
                self.product_ref(tuple(parts), scope=scope)
            return
        self.field(host=terminal, name=parts[-1], scope=scope)
        self.metric(host=terminal, name=parts[-1], scope=scope)

    def stages(self, queries: list[SlayerQuery], *, data_source: str | None) -> None:
        scopes = query_scopes(queries, datasets_by_ds=self.by_ds, data_source=data_source)
        for query, query_scopes_ in zip(queries, scopes):
            for scope in query_scopes_:
                self.stage(query, scope=scope)

    def stage(self, query: SlayerQuery, *, scope: QueryScope) -> None:
        """What one query object reads in ``scope``: its source, the datasets its extensions name, what their own
        definitions read, every reference, and each INNER join's population filter."""
        query = scope.query or query
        self.sources(query, scope=scope)
        host = scope.host
        if host is not None and extension_of(query, host.name) is not None:
            self.definitions(host, scope=scope)
        self.extension_fields(query, scope=scope)
        for parts in [*query_reference_parts(query), *scope.inner_refs]:
            if host is not None:
                self.ref(parts, host=host, scope=scope)
            else:
                self.product_ref(parts, scope=scope)

    def sources(self, query: SlayerQuery, *, scope: QueryScope) -> None:
        """The stored datasets ``query`` names as its source and its extensions."""
        names = list(extension_dataset_names(query))
        if query.population is not None:
            names.append(query.population)
        for name in names:
            self.read(name, scope=scope)

    def extension_fields(self, query: SlayerQuery, *, scope: QueryScope) -> None:
        """What each of ``query``'s extension metrics reads, over the dataset it extends."""
        for extension in query.extensions or []:
            extended = scope.datasets.get(extension.source_name)
            if extended is None:
                continue
            for name in [m.name for m in extension.metrics or [] if m.name]:
                self.field(host=extended, name=name, scope=scope)
                self.metric(host=extended, name=name, scope=scope)

    def product_ref(self, parts: tuple[str, ...], *, scope: QueryScope) -> None:
        """A reference rooted at the dataset of the scope its head names."""
        root = scope.datasets.get(parts[0]) if len(parts) > 1 else None
        if root is not None:
            self.read(root.name, scope=scope)
            self.ref(parts[1:], host=root, scope=scope)
