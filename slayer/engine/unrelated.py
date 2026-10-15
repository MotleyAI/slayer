"""Unrelated datasets: what a query frame names that no join path reaches from its population."""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Optional, Tuple

from pydantic import BaseModel

from slayer.core.join_search import count_routes
from slayer.core.join_walker import matching_edges, neighbors
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
)
from slayer.core.models import SlayerModel
from slayer.core.query import ComputedDimension, SlayerQuery, render_probe_text
from slayer.core.time_spine import is_spine
from slayer.engine.reference_closure import key_closure
from slayer.engine.syntax import DottedRef, parse_expr, parse_filter_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

__all__ = ["ProductSides", "first_hop_names", "stamp_unrelated", "unrelated_targets"]


def _dotted_heads(node: Any) -> Iterator[str]:
    """The first token of every dotted reference anywhere in a parsed tree."""
    if isinstance(node, DottedRef):
        if len(node.parts) > 1:
            yield node.parts[0]
        return
    if isinstance(node, BaseModel):
        for name in type(node).model_fields:
            yield from _dotted_heads(getattr(node, name))
    elif isinstance(node, (tuple, list)):
        for item in node:
            yield from _dotted_heads(item)


def _parsed_heads(text: str, *, as_filter: bool) -> Iterator[str]:
    try:
        parsed = parse_filter_expr(render_probe_text(text)) if as_filter else parse_expr(text)
    except Exception:  # noqa: BLE001 — the binder reports a malformed expression
        return
    yield from _dotted_heads(parsed)


def _path_head(full_name: str) -> Iterator[str]:
    if "." in full_name:
        yield full_name.split(".", 1)[0]


def first_hop_names(query: SlayerQuery) -> set[str]:
    """The first hop of every dotted reference in ``query``'s own fields."""
    out: set[str] = set()
    for m in query.measures or []:
        out.update(_parsed_heads(m.formula, as_filter=False))
    for d in query.dimensions or []:
        out.update(_parsed_heads(d.expression, as_filter=False) if isinstance(d, ComputedDimension)
                   else _path_head(d.full_name))
    for td in query.time_dimensions or []:
        out.update(_path_head(td.dimension.full_name))
    for f in query.filters or []:
        out.update(_parsed_heads(f, as_filter=True))
    for o in query.order or []:
        out.update(_parsed_heads(o.raw_formula, as_filter=False) if o.raw_formula else _path_head(o.column.full_name))
    return out


def _named(token: str, models: Iterable[SlayerModel]) -> Optional[SlayerModel]:
    """The dataset ``token`` names: a query stage's spelling first, then a model name."""
    models = list(models)
    return next((m for m in models if m.explicit_spelling == token), None) or next(
        (m for m in models if m.explicit_spelling is None and m.name == token), None,
    )


def unrelated_targets(
    *, host: SlayerModel, names: Iterable[str], models_by_name: dict[str, SlayerModel],
) -> frozenset[str]:
    """The datasets ``names`` spell that neither an incident edge nor a route reaches from ``host``."""
    universe = {**models_by_name, host.name: host}
    incident = neighbors(model=host, models_by_name=universe)
    out: set[str] = set()
    for token in names:
        if token in (host.name, host.spelling) or matching_edges(incident, token):
            continue
        target = _named(token, universe.values())
        if target is None or is_spine(target) or target.name == host.name:
            continue
        if count_routes(root=host, target=target.name, models_by_name=universe) == 0:
            out.add(target.name)
    return frozenset(out)


def stamp_unrelated(*, query: SlayerQuery, bundle: ResolvedSourceBundle, host: SlayerModel) -> SlayerModel:
    """``host`` as ``query``'s population, carrying the unrelated datasets the frame names."""
    found = unrelated_targets(host=host, names=first_hop_names(query), models_by_name=bundle.models_by_name)
    return host.with_unrelated(found) if found != host.unrelated_datasets else host


def crosses_product_hop(
    *, host: SlayerModel, path: Tuple[str, ...], models_by_name: dict[str, SlayerModel],
) -> bool:
    """``path`` from ``host`` starts with a product hop to an unrelated dataset."""
    return bool(path) and any(
        models_by_name[n].spelling == path[0] for n in host.unrelated_datasets if n in models_by_name
    )


class ProductSides:
    """Which side of each product hop a population's references read, by dependency closure (engine P10)."""

    def __init__(self, *, host: SlayerModel, bundle: ResolvedSourceBundle) -> None:
        models = bundle.models_by_name
        self.host = host
        self.bundle = bundle
        # A product hop's canonical token is its target's spelling.
        self.tokens = {models[n].spelling: n for n in host.unrelated_datasets if n in models}

    def crossed(self, leaf: ValueKey) -> frozenset[str]:
        """The spellings of the unrelated datasets ``leaf``'s dependency closure reaches."""
        if not self.tokens:
            return frozenset()
        closure = key_closure(key=leaf, anchor_model=self.host, anchor_relation=self.host.name, bundle=self.bundle)
        paths = closure if closure is not None else (_leaf_path(leaf),)
        return frozenset(p[0] for p in paths if p and p[0] in self.tokens)

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
            yield from self._walk(node.column, inputs=inputs)
        elif isinstance(node, (ColumnKey, ColumnSqlKey, StarKey)):
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
    """Column leaves outside aggregates and transforms (attached values belong to no side)."""
    if isinstance(node, (AggregateKey, TransformKey)):
        return
    if isinstance(node, TimeTruncKey):
        yield from _row_leaves(node.column)
        return
    if isinstance(node, (ColumnKey, ColumnSqlKey, StarKey)):
        yield node
        return
    for child in node.children():
        yield from _row_leaves(child)
