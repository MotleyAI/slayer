"""Unrelated datasets: what a query frame names that no relationship path reaches from its population."""

from __future__ import annotations

from typing import Iterable, Iterator, Optional, Tuple


from slayer.core.relationship_search import count_routes
from slayer.core.relationship_walker import product_token
from slayer.core.keys import (
    AggregateKey,
    FieldKey,
    FieldSqlKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
)
from slayer.core.models import SlayerDataset
from slayer.core.query import FieldRef, SlayerQuery
from slayer.core.time_spine import is_spine
from slayer.engine.reference_closure import key_closure
from slayer.core.syntax import text_paths
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = ["ProductSides", "first_hop_names", "stamp_unrelated", "unrelated_targets"]


def _parsed_heads(text: str) -> Iterator[str]:
    """The anchor of every dotted reference in ``text``."""
    return (path[0] for path in text_paths(text, root=None) if len(path) > 1)


def _path_head(ref: FieldRef) -> Iterator[str]:
    if len(ref.parts) > 1:
        yield ref.parts[0]


def first_hop_names(query: SlayerQuery) -> set[str]:
    """The anchor of every dotted reference in ``query``'s own fields."""
    out: set[str] = set()
    for m in query.metrics or []:
        out.update(_parsed_heads(m.metric))
    for d in query.dimensions or []:
        ref = d.ref
        out.update(_parsed_heads(d.field) if d.is_computed or ref is None else _path_head(ref))
    for td in query.time_dimensions or []:
        out.update(_parsed_heads(td.field))
    for f in query.filter_conditions:
        out.update(_parsed_heads(f))
    for o in query.order or []:
        ref = o.ref
        out.update(_path_head(ref) if ref is not None else _parsed_heads(o.raw_expression or o.field))
    return out


def _named(token: str, datasets: Iterable[SlayerDataset]) -> Optional[SlayerDataset]:
    """The dataset ``token`` names: a query stage's spelling first, then a dataset name."""
    datasets = list(datasets)
    return next((m for m in datasets if m.explicit_spelling == token), None) or next(
        (m for m in datasets if m.explicit_spelling is None and m.name == token), None,
    )


def unrelated_targets(
    *, host: SlayerDataset, names: Iterable[str], datasets_by_name: dict[str, SlayerDataset],
) -> frozenset[str]:
    """The anchors ``names`` spell that no route reaches from ``host``; a relationship spelled the same is no route."""
    universe = {**datasets_by_name, host.name: host}
    out: set[str] = set()
    for token in names:
        target = _named(token, universe.values())
        if target is None or is_spine(target) or target.name == host.name or token == host.spelling:
            continue
        if count_routes(root=host, target=target.name, datasets_by_name=universe) == 0:
            out.add(target.name)
    return frozenset(out)


def stamp_unrelated(
    *, query: SlayerQuery, bundle: ResolvedSourceBundle, host: SlayerDataset, extra: Iterable[SlayerDataset] = (),
) -> SlayerDataset:
    """``host`` as ``query``'s population, carrying the unrelated datasets the frame names
    among the bundle's datasets plus ``extra`` (unspliced query-backed placeholders)."""
    datasets = {**bundle.datasets_by_name, **{m.name: m for m in extra}}
    found = unrelated_targets(host=host, names=first_hop_names(query), datasets_by_name=datasets)
    return host.with_unrelated(found) if found != host.unrelated_datasets else host


def crosses_product_hop(
    *, host: SlayerDataset, path: Tuple[str, ...], datasets_by_name: dict[str, SlayerDataset],
) -> bool:
    """``path`` from ``host`` starts with a product hop to an unrelated dataset."""
    return bool(path) and path[0] in _product_tokens(host=host, datasets_by_name=datasets_by_name)


def _product_tokens(*, host: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> dict[str, str]:
    """Each product hop's token from ``host`` → its target's spelling."""
    universe = {**datasets_by_name, host.name: host}
    return {product_token(source=host, target=universe[n], datasets_by_name=universe): universe[n].spelling
            for n in host.unrelated_datasets if n in universe}


class ProductSides:
    """Which side of each product hop a population's references read, by dependency closure (engine P10)."""

    def __init__(self, *, host: SlayerDataset, bundle: ResolvedSourceBundle) -> None:
        datasets = bundle.datasets_by_name
        self.host = host
        self.bundle = bundle
        self.tokens = _product_tokens(host=host, datasets_by_name=datasets)

    def crossed(self, leaf: ValueKey) -> frozenset[str]:
        """The spellings of the unrelated datasets ``leaf``'s dependency closure reaches."""
        if not self.tokens:
            return frozenset()
        closure = key_closure(key=leaf, anchor_dataset=self.host, anchor_relation=self.host.name, bundle=self.bundle)
        paths = closure if closure is not None else (_leaf_path(leaf),)
        return frozenset(self.tokens[p[0]] for p in paths if p and p[0] in self.tokens)

    def row_level_reads(self, root: ValueKey) -> Iterator[Tuple[ValueKey, str]]:
        """``(reference, dataset)`` for every read across a product hop outside an aggregation's inputs."""
        if not self.tokens:
            return
        yield from self._walk(root, inputs=False)

    def _walk(self, node: ValueKey, *, inputs: bool) -> Iterator[Tuple[ValueKey, str]]:
        if isinstance(node, AggregateKey):
            yield from self._walk_aggregate(node)
        elif isinstance(node, TransformKey):
            yield from self._walk_transform(node, inputs=inputs)
        elif isinstance(node, TimeTruncKey):
            yield from self._walk(node.source, inputs=inputs)
        elif isinstance(node, (FieldKey, FieldSqlKey, StarKey)):
            if not inputs:
                yield from ((node, dataset) for dataset in sorted(self.crossed(node)))
        else:
            for child in node.children():
                yield from self._walk(child, inputs=inputs)

    def _walk_aggregate(self, node: AggregateKey) -> Iterator[Tuple[ValueKey, str]]:
        for pk in node.partition_keys or ():
            yield from self._walk(pk, inputs=False)
        for child in (node.source, *node.args, *(v for _, v in node.kwargs)):
            if isinstance(child, ValueKey):
                yield from self._walk(child, inputs=True)

    def _walk_transform(self, node: TransformKey, *, inputs: bool) -> Iterator[Tuple[ValueKey, str]]:
        yield from self._walk(node.input, inputs=inputs)
        for k in (*node.partition_keys, node.time_key):
            if k is not None:
                yield from self._walk(k, inputs=False)

    def mixed_sides(self, conjunct: ValueKey) -> Optional[frozenset[str]]:
        """The unrelated datasets a row-level conjunct reads when it also reads anything else, else ``None``."""
        if not self.tokens:
            return None
        datasets: set[str] = set()
        other = False
        for leaf in _row_leaves(conjunct):
            crossed = self.crossed(leaf)
            datasets |= crossed
            other = other or not crossed
        return frozenset(datasets) if datasets and (other or len(datasets) > 1) else None


def _leaf_path(leaf: ValueKey) -> Tuple[str, ...]:
    return tuple(getattr(leaf, "path", ()) or ())


def _row_leaves(node: ValueKey) -> Iterator[ValueKey]:
    """Field leaves outside aggregates and transforms (attached values belong to no side)."""
    if isinstance(node, (AggregateKey, TransformKey)):
        return
    if isinstance(node, TimeTruncKey):
        yield from _row_leaves(node.source)
        return
    if isinstance(node, (FieldKey, FieldSqlKey, StarKey)):
        yield node
        return
    for child in node.children():
        yield from _row_leaves(child)
