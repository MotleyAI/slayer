"""How a home reads what its population reads (``queries/references``): each reference routed from the home by its
anchored spelling, never through the population's route."""

from __future__ import annotations

from typing import Dict, Iterable, Iterator, Optional, Tuple, TypeVar

from slayer.core.keys import (
    AggregateKey,
    anchor_token,
    FieldKey,
    FieldSqlKey,
    Grain,
    ValueKey,
    field_leaf,
    framed_aggregate,
    is_framed_path,
    reroot_value_key,
    source_leaf_paths,
    substitute_value_keys,
    token_anchor,
)
from slayer.core.errors import AmbiguousRelationshipPathError, UnresolvableDimensionRelationshipError
from slayer.core.models import SlayerDataset
from slayer.core.relationship_walker import follow, neighbors, reaches_spine, reverse_token
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine
from slayer.engine.cardinality.relationship_safety import home_spine_route
from slayer.engine.population.anchors import anchor_route

__all__ = ["Frame", "anchored_spelling", "home_frame", "home_read", "outside_aggregates", "product_anchors", "reads_the_spine"]

_KeyT = TypeVar("_KeyT", bound=ValueKey)

#: An aggregation's frame and its home's path from it.
Frame = Tuple[SlayerDataset, Tuple[str, ...]]


def anchored_spelling(key: FieldKey | FieldSqlKey, *, reader: SlayerDataset) -> Tuple[str, ...]:
    """A reference's anchor name and canonical route (Principle 1), read by ``reader``."""
    return key.anchor if key.anchor is not None else (reader.name, *key.path)


def home_frame(
    home_path: Tuple[str, ...], *, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[Frame]:
    """The frame an aggregation homed at ``home_path`` (from ``host``) is read from, and the home's path from it."""
    framed = token_anchor(home_path[0]) if home_path else None
    if framed is None:
        return host, home_path
    frame = datasets_by_name.get(framed)
    return None if frame is None else (frame, home_path[1:])


def _read_leaf(
    r: FieldKey | FieldSqlKey, *, home: SlayerDataset, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    frame: Optional[Frame] = None, fanning: bool = False,
) -> Optional[ValueKey]:
    """``r`` read from ``home``: its anchor reached by the home's own route, one anchored at the aggregation's
    ``frame`` read back along the source's path; ``None`` when the home reaches it by no relationship path (an
    ambiguous route raises)."""
    if r.anchor is None and reaches_spine(root=host, path=r.path[:1], datasets_by_name=datasets_by_name):
        # Anchored at the spine: each home reaches it by its own nearest axis (Axiom 15).
        route = _spine_route(home, datasets_by_name=datasets_by_name, leaf=field_leaf(r), fanning=fanning)
        return None if route is None else r.model_copy(update={"path": (*route, *r.path[1:])})
    spelling = anchored_spelling(r, reader=host)
    anchor = _anchor_of(spelling, host=host, datasets_by_name=datasets_by_name)
    if anchor is None:
        return None
    if anchor.name == home.name:
        return r.model_copy(update={"path": spelling[1:], "anchor": None})
    if is_spine(anchor):
        route = _spine_route(home, datasets_by_name=datasets_by_name, leaf=field_leaf(r), fanning=fanning)
        return None if route is None else r.model_copy(update={"path": (*route, *spelling[1:]), "anchor": None})
    if frame is not None and anchor.name == frame[0].name:
        return _read_from_frame(r, frame=frame, spelling=spelling, datasets_by_name=datasets_by_name)
    reached = anchor_route(reader=home, anchor=anchor, datasets_by_name=datasets_by_name,
                           rest=(*spelling[1:], field_leaf(r)))
    if reached.unrelated and anchor.name not in home.unrelated_datasets:
        return None
    return r.model_copy(update={"path": (*reached.route, *spelling[1:]), "anchor": spelling})


def _anchor_of(
    spelling: Tuple[str, ...], *, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[SlayerDataset]:
    return host if spelling[0] == host.name else datasets_by_name.get(spelling[0])


def _read_from_frame(
    r: FieldKey | FieldSqlKey, *, frame: Frame, spelling: Tuple[str, ...], datasets_by_name: Dict[str, SlayerDataset],
) -> ValueKey:
    """``r`` anchored at the source's own frame: read back along the source's written path to the frame, then
    forward; a reference at or beyond the home reads the home row itself."""
    home_path, route = frame[1], spelling[1:]
    if route[:len(home_path)] == home_path:
        return r.model_copy(update={"path": route[len(home_path):], "anchor": spelling})
    chain = follow(root=frame[0], path=home_path, datasets_by_name=datasets_by_name) or []
    back = tuple(reverse_token(e) for e in reversed(chain))
    return r.model_copy(update={"path": (*back, *route), "anchor": spelling})


def product_anchors(
    keys: Iterable[ValueKey], *, home: SlayerDataset, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> frozenset[str]:
    """The anchors of ``keys``' references that ``home`` meets only as a product (semantics Axiom 3.1)."""
    met = (
        _product_anchor(r, home=home, host=host, datasets_by_name=datasets_by_name)
        for key in keys for r in outside_aggregates(key) if isinstance(r, (FieldKey, FieldSqlKey))
    )
    return frozenset(name for name in met if name is not None)


def _product_anchor(
    r: FieldKey | FieldSqlKey, *, home: SlayerDataset, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[str]:
    """``r``'s anchor when ``home`` meets it only as a product, else ``None``."""
    spelling = anchored_spelling(r, reader=host)
    anchor = _anchor_of(spelling, host=host, datasets_by_name=datasets_by_name)
    if anchor is None or anchor.name == home.name or is_spine(anchor) or reaches_spine(
            root=anchor, path=spelling[1:], datasets_by_name=datasets_by_name):
        return None
    try:
        unrelated = anchor_route(reader=home, anchor=anchor, datasets_by_name=datasets_by_name).unrelated
    except UnresolvableDimensionRelationshipError:
        return None
    return anchor.name if unrelated else None


def reads_the_spine(key: ValueKey, *, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset]) -> bool:
    """``key`` reads a reference anchored at the time spine."""
    return any(isinstance(r, (FieldKey, FieldSqlKey)) and (
        reaches_spine(root=host, path=r.path[:1], datasets_by_name=datasets_by_name)
        or (r.anchor is None and is_spine(host))) for r in outside_aggregates(key))


def _spine_route(
    home: SlayerDataset, *, datasets_by_name: Dict[str, SlayerDataset], leaf: str, fanning: bool = False,
) -> Optional[Tuple[str, ...]]:
    """``home``'s route to the spine (across its nearest fanning axis when ``fanning``); equal-length routes fail
    spelled from the spine."""
    hops = [e for e in neighbors(dataset=home, datasets_by_name=datasets_by_name)
            if e.to == TIME_SPINE_DATASET and e.product is None]
    if len(hops) > 1:  # parallel hops to the spine are refused
        raise AmbiguousRelationshipPathError(source_dataset=home.name, target_dataset=TIME_SPINE_DATASET,
                                             candidates=hops, token=TIME_SPINE_DATASET)
    return home_spine_route(root_dataset=home, datasets_by_name=datasets_by_name, leaf=leaf, fanning=fanning)


def outside_aggregates(key: ValueKey) -> Iterator[ValueKey]:
    """``key``'s nodes, an aggregate yielded whole (its interior is its own)."""
    yield key
    if not isinstance(key, AggregateKey):
        for c in key.children():
            yield from outside_aggregates(c)


def _aggregate_at_home(
    agg: AggregateKey, *, home: SlayerDataset, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
) -> AggregateKey:
    """An aggregate the population reads, as ``home`` reads it: its interior from its own frame (no anchor hop when
    that frame is the home), its grain from the home."""
    leaf_paths = source_leaf_paths(agg.source)
    framed = any(is_framed_path(p) for p in leaf_paths)
    own_frame = anchor_token(home.name)
    if framed and own_frame in {p[0] for p in leaf_paths if p}:
        interior = reroot_value_key(agg.ungrained(), target_path=(own_frame,))
    elif leaf_paths and not framed and host.name != home.name:
        interior = framed_aggregate(agg, frame=host.name)
    else:
        interior = agg
    if agg.partition_keys is None:
        return interior
    grain = (home_read(p, home=home, host=host, datasets_by_name=datasets_by_name) or p for p in agg.partition_keys)
    return interior.model_copy(update={"partition_keys": Grain.of(grain), "grain_defaulted": agg.grain_defaulted})


def home_read(
    key: _KeyT, *, home: SlayerDataset, host: SlayerDataset, datasets_by_name: Dict[str, SlayerDataset],
    frame: Optional[Frame] = None, fanning: bool = False,
) -> Optional[_KeyT]:
    """``key``, as the population ``host`` reads it, read from ``home``: each reference routed from the home by its
    anchored spelling (queries/references), an aggregate read from its own frame; ``None`` when the home reaches a
    reference by no relationship path."""
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in outside_aggregates(key):
        if r in mapping:
            continue
        if isinstance(r, AggregateKey):
            mapping[r] = _aggregate_at_home(r, home=home, host=host, datasets_by_name=datasets_by_name)
        elif isinstance(r, (FieldKey, FieldSqlKey)):
            read = _read_leaf(r, home=home, host=host, datasets_by_name=datasets_by_name, frame=frame, fanning=fanning)
            if read is None:
                return None
            mapping[r] = read
    return substitute_value_keys(key, mapping) if mapping else key
