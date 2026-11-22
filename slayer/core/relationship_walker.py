"""The shared bidirectional relationship walker.

Every declared relationship is a symmetric edge between its two datasets. Traversing it
against the declared direction *orients* it — join pairs swap and the
cardinality label inverts — so which side stores the declaration is storage
trivia. This module is the one traversal substrate: binding, reroot, safety,
spine building, route enumeration, and BFS closures all read edges through it.

Two faces:

* total enumeration (:func:`edges_between`, :func:`neighbors`) — never errors;
  used by route counting, closures, and rendering;
* strict resolution (:func:`resolve_hop`, :func:`walk`) — a path token matches
  an incident edge's name, else the opposite-endpoint dataset name; ``None`` only on
  an unknown/unloaded hop so callers keep their own errors, raises
  :class:`AmbiguousRelationshipPathError` when two or more edges span the hop, and
  :class:`CircularRelationshipPathError` when a hop revisits a dataset already on the path;
  :func:`follow` walks an already-resolved composed key path, where a revisit is
  a separate occurrence (revisits are frame-local).
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, Generator, Iterable, Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, RelationshipCardinality, JoinType, invert_cardinality
from slayer.core.errors import AmbiguousHopError, AmbiguousRelationshipPathError, CircularRelationshipPathError, SelfRelationshipTraversalError
from slayer.core.keys import FieldElementFn, FieldKey, FieldSqlKey, FieldTypeFn, ValueKey, source_anchor_path
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset, is_key_set_unique, relationship_key_error
from slayer.core.names import match_name
from slayer.core.time_spine import PRODUCT_JOIN_TYPE, TIME_SPINE_DATASET, axis_field, is_spine, spine_relationships

__all__ = [
    "OrientedRelationship",
    "OrientedLike",
    "addressable_token",
    "addresses_alone",
    "aggregation_owner",
    "canonical_path",
    "canonical_token",
    "edges_between",
    "follow",
    "incident_dataset_names",
    "is_product_edge",
    "is_lateral_edge",
    "is_unrelated_edge",
    "PRODUCT_TOKEN_PREFIX",
    "product_token",
    "list_child",
    "list_child_name",
    "with_list_children",
    "dataset_field_element",
    "relationship_key_fields",
    "matching_edges",
    "neighbors",
    "orient_relationship",
    "observe_traversals",
    "provably_to_one",
    "resolve_hop",
    "resolved_terminal",
    "reverse_token",
    "terminal_dataset",
    "cancelling_revisit",
    "unique_key_sets",
    "WalkMiss",
    "walk",
    "walk_cancelling",
    "walk_miss",
    "walk_terminal",
]


#: A product hop's kind: the population to its spine, the spine to a dataset with no axis route,
#: or the population to a dataset no relationship path reaches (minted only by strict resolution).
ProductKind = Literal["spine", "detached", "unrelated"]


class OrientedRelationship(BaseModel):
    """A declared relationship viewed from a chosen traversal source.

    ``source_dataset``/``to`` are the traversal direction; ``column_pairs``
    and ``cardinality`` are oriented to match it. ``declaring_dataset`` is the dataset
    that stores the underlying :class:`DatasetRelationship` (storage trivia, kept for
    diagnostics). Frozen so a resolved hop cannot be mutated in place.
    """

    model_config = ConfigDict(frozen=True)

    source_dataset: str
    to: str
    column_pairs: list[tuple[str, str]]
    join_type: JoinType
    cardinality: RelationshipCardinality | None
    name: str | None
    declaring_dataset: str
    # A query stage's user spelling of an endpoint (``None`` for an ordinary dataset).
    source_spelling: str | None = None
    target_spelling: str | None = None
    # A keyless hop pairing every source row with every target row; ``None`` for a declared relationship.
    product: ProductKind | None = None
    # A list field's hop between its dataset and its element child (the list field's name); ``None`` otherwise.
    lateral: str | None = None
    # A query object's local aliases of the edge.
    aliases: tuple[str, ...] = ()


_traversals: ContextVar[Optional[Dict[str, bool]]] = ContextVar(
    "slayer_join_traversals", default=None,
)


@contextmanager
def observe_traversals() -> Generator[Dict[str, bool], None, None]:
    """Record every dataset a traversal in the block yields: name → strictly resolved
    (a hop walked) rather than only enumerated. Output-only; never feeds resolution."""
    sink: Dict[str, bool] = {}
    token = _traversals.set(sink)
    try:
        yield sink
    finally:
        _traversals.reset(token)


@contextmanager
def unobserved() -> Generator[None, None, None]:
    """Traversals in the block go unrecorded: a validation walk, not a read the planner must serve."""
    token = _traversals.set(None)
    try:
        yield
    finally:
        _traversals.reset(token)


def _observe(targets: Iterable[str], *, strict: bool) -> None:
    sink = _traversals.get()
    if sink is None:
        return
    for name in targets:
        sink[name] = sink.get(name, False) or strict


def canonical_token(edge: OrientedRelationship) -> str:
    """A hop's canonical spelling: the edge name, else the target's spelling."""
    return edge.name or edge.target_spelling or edge.to


def reverse_token(edge: OrientedRelationship) -> str:
    """The spelling of ``edge`` walked backwards: the edge name, else the source's spelling."""
    return edge.name or edge.source_spelling or edge.source_dataset


def _spellings(edge: OrientedRelationship) -> tuple[tuple[str, ...], ...]:
    """An edge's names by precedence: edge name, a query stage's spelling (it shadows a same-named dataset), dataset name
    (never a stage's minted identity), then its query-local aliases."""
    dataset = edge.to if edge.target_spelling is None else None
    return (
        tuple(n for n in (edge.name,) if n is not None), tuple(n for n in (edge.target_spelling,) if n is not None),
        tuple(n for n in (dataset,) if n is not None), tuple(edge.aliases),
    )


def matching_edges(edges: list[OrientedRelationship], token: str, *, quoted: bool = False) -> list[OrientedRelationship]:
    """The edges ``token`` names, by precedence (``_spellings``): an exact spelling at any precedence, else (unquoted)
    a case-insensitive one."""
    for exact in (True,) if quoted else (True, False):
        for tier in range(4):
            names = [n for e in edges for n in _spellings(e)[tier]]
            found = match_name(name=token, declared=names, quoted=exact)
            if found is not None:
                return [e for e in edges if found in _spellings(e)[tier]]
    return []


def canonical_path(chain: Sequence[OrientedRelationship]) -> tuple[str, ...]:
    """The canonical spelling of a resolved chain."""
    return tuple(canonical_token(e) for e in chain)


def _orient(
    *, relationship: DatasetRelationship, declaring: str, from_dataset: str,
    spellings: Dict[str, Optional[str]],
) -> OrientedRelationship:
    """Orient ``relationship`` (declared on ``declaring``) so its source is ``from_dataset``."""
    if from_dataset == declaring:
        to_dataset = relationship.to
        pairs = list(relationship.column_pairs)
        cardinality = relationship.cardinality
    else:
        to_dataset = declaring
        pairs = [(p[1], p[0]) for p in relationship.column_pairs]
        cardinality = invert_cardinality(relationship.cardinality)
    return OrientedRelationship(
        source_dataset=from_dataset,
        to=to_dataset,
        column_pairs=pairs,
        join_type=relationship.join_type,
        cardinality=cardinality,
        name=relationship.name,
        declaring_dataset=declaring,
        source_spelling=spellings.get(from_dataset),
        target_spelling=spellings.get(to_dataset),
        aliases=relationship.aliases,
    )


def orient_relationship(*, relationship: DatasetRelationship, declaring: SlayerDataset, other: SlayerDataset | None, from_dataset: str) -> OrientedRelationship:
    """``relationship`` (declared on ``declaring``, reaching ``other``) oriented so its source is ``from_dataset``."""
    spellings = {declaring.name: declaring.explicit_spelling}
    if other is not None:
        spellings[other.name] = other.explicit_spelling
    return _orient(relationship=relationship, declaring=declaring.name, from_dataset=from_dataset, spellings=spellings)


def _product_edge(dataset: SlayerDataset) -> OrientedRelationship:
    """A population factor's hop to the spine: the product, each factor row fanning to every instant."""
    return OrientedRelationship(
        source_dataset=dataset.name, to=TIME_SPINE_DATASET, column_pairs=[],
        join_type=PRODUCT_JOIN_TYPE, cardinality=RelationshipCardinality.ONE_TO_MANY,
        name=TIME_SPINE_DATASET, declaring_dataset=dataset.name,
        source_spelling=dataset.explicit_spelling, product="spine",
    )


#: An oriented hop, or a declared relationship read in its declared orientation — both
#: expose ``cardinality`` and target-side ``column_pairs``, all the proof reads.
OrientedLike = OrientedRelationship | DatasetRelationship


def unique_key_sets(dataset: SlayerDataset) -> list[list[str]]:
    """``dataset``'s unique key-sets by field name: its (composite) PK, then each solo-unique field."""
    pk = [c.name for c in dataset.fields if c.primary_key]
    return ([pk] if pk else []) + [[c.name] for c in dataset.fields if c.unique]


def _to_one(*, cardinality: RelationshipCardinality | None, target_keys: list[str], target: SlayerDataset) -> bool:
    return cardinality in (RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_ONE) or is_key_set_unique(
        key_fields=target_keys, unique_key_sets=unique_key_sets(target),
    )


def provably_to_one(*, edge: OrientedLike, target_dataset: SlayerDataset) -> bool:
    """Is ``edge`` provably many-to-one onto ``target_dataset`` in its orientation?
    True iff the oriented cardinality is m:1/1:1, or the traversal-target fields
    fully cover a unique key-set of ``target_dataset``."""
    return _to_one(cardinality=edge.cardinality, target_keys=[p[1] for p in edge.column_pairs], target=target_dataset)


def _detached_names(datasets_by_name: dict[str, SlayerDataset]) -> frozenset[str]:
    """Datasets with no provably to-one chain to an axis: the spine meets them only as a product."""
    datasets = {n: m for n, m in datasets_by_name.items() if not is_spine(m)}
    reached = {n for n, m in datasets.items() if axis_field(m) is not None}
    to_one_into: dict[str, set[str]] = {}
    for m in datasets.values():
        for j in m.relationships:
            t = datasets.get(j.to)
            if t is None:
                continue
            if _to_one(cardinality=j.cardinality, target_keys=[p[1] for p in j.column_pairs], target=t):
                to_one_into.setdefault(t.name, set()).add(m.name)
            if _to_one(cardinality=invert_cardinality(j.cardinality), target_keys=[p[0] for p in j.column_pairs],
                       target=m):
                to_one_into.setdefault(m.name, set()).add(t.name)
    frontier = list(reached)
    while frontier:
        fresh = to_one_into.get(frontier.pop(), set()) - reached
        reached |= fresh
        frontier.extend(fresh)
    return frozenset(datasets.keys() - reached)


def _detached_edge(*, source: SlayerDataset, target: SlayerDataset) -> OrientedRelationship:
    """The spine and a dataset without any route to it, meeting only as a product."""
    return OrientedRelationship(
        source_dataset=source.name, to=target.name, column_pairs=[],
        join_type=PRODUCT_JOIN_TYPE, cardinality=RelationshipCardinality.MANY_TO_MANY,
        name=None, declaring_dataset=TIME_SPINE_DATASET,
        source_spelling=source.explicit_spelling, target_spelling=target.explicit_spelling,
        product="detached",
    )


#: A product hop's token when its source has a relationship spelled like its target (reserved: never a declared name).
PRODUCT_TOKEN_PREFIX = "__slayer_product__"


def product_token(*, source: SlayerDataset, target: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> str:
    """The token of the product hop ``source → target``: the target's spelling, else, when one of ``source``'s
    relationships is spelled the same, a reserved token naming the target alone."""
    incident = neighbors(dataset=source, datasets_by_name=datasets_by_name)
    return PRODUCT_TOKEN_PREFIX + target.name if matching_edges(incident, target.spelling) else target.spelling


def _unrelated_edge(*, source: SlayerDataset, target: SlayerDataset, token: str | None = None) -> OrientedRelationship:
    """The population and a dataset no relationship path reaches, meeting only as a product (``token`` when it is not
    the target's spelling)."""
    return OrientedRelationship(
        source_dataset=source.name, to=target.name, column_pairs=[],
        join_type=PRODUCT_JOIN_TYPE, cardinality=RelationshipCardinality.MANY_TO_MANY,
        name=token, declaring_dataset=source.name,
        source_spelling=source.explicit_spelling, target_spelling=target.explicit_spelling,
        product="unrelated",
    )


#: The element field of a list child.
LIST_ELEMENT_FIELD = "value"


#: A list child's identity prefix (reserved: no stored dataset carries it).
LIST_CHILD_PREFIX = "__slayer_list__"


def list_child_name(*, parent: str, field: str) -> str:
    """A list child's minted dataset identity; the length prefix keeps ``__`` in names unambiguous."""
    return f"{LIST_CHILD_PREFIX}{len(parent)}_{parent}__{field}"


def list_child(*, parent: SlayerDataset, field: DatasetField) -> SlayerDataset:
    """The element child of ``parent``'s list ``field``: one row per non-NULL element, its value ``value``.
    Cached on ``parent`` under everything the child is built from, so a copied or edited parent never reuses a
    stale one."""
    key = (parent.name, parent.data_source, field.name, field.element_type)
    cached = parent._list_children.get(key)
    if cached is not None:
        return cached
    name = list_child_name(parent=parent.name, field=field.name)
    # Built valid, then renamed: the name validator refuses the reserved prefix. Never a relation of its own.
    child = SlayerDataset(
        name="list_child", sql_table="list_child", data_source=parent.data_source or "list_child",
        fields=[DatasetField(name=LIST_ELEMENT_FIELD, datatype=field.element_type or DataType.OPAQUE)],
    ).model_copy(update={"name": name, "sql_table": name, "data_source": parent.data_source})
    child._list_of = (parent.name, field.name)
    return parent._list_children.setdefault(key, child)


def with_list_children(datasets_by_name: Dict[str, SlayerDataset]) -> Dict[str, SlayerDataset]:
    """``datasets_by_name`` plus the element child of every list field."""
    out = dict(datasets_by_name)
    for dataset in datasets_by_name.values():
        for field in dataset.list_fields():
            name = list_child_name(parent=dataset.name, field=field.name)
            if name not in out:
                out[name] = list_child(parent=dataset, field=field)
    return out


def _lateral_edge(*, parent: SlayerDataset, field: str, forward: bool) -> OrientedRelationship:
    """The hop between ``parent`` and its ``field`` child: one-to-many forward, the child's own row back."""
    child = list_child_name(parent=parent.name, field=field)
    return OrientedRelationship(
        source_dataset=parent.name if forward else child, to=child if forward else parent.name,
        column_pairs=[], join_type=JoinType.INNER,
        cardinality=RelationshipCardinality.ONE_TO_MANY if forward else RelationshipCardinality.MANY_TO_ONE,
        name=field, declaring_dataset=parent.name,
        source_spelling=parent.explicit_spelling if forward else None,
        target_spelling=None if forward else parent.explicit_spelling,
        lateral=field,
    )


def _list_edges(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> list[OrientedRelationship]:
    """``dataset``'s hops to its list children, and a list child's hop back to its parent."""
    out = [_lateral_edge(parent=dataset, field=c.name, forward=True) for c in dataset.list_fields()]
    if dataset.list_of is not None:
        parent = datasets_by_name.get(dataset.list_of[0])
        if parent is not None:
            out.append(_lateral_edge(parent=parent, field=dataset.list_of[1], forward=False))
    return out


def is_lateral_edge(edge: OrientedRelationship) -> bool:
    return edge.lateral is not None


def _hop_target(*, edge: OrientedRelationship, current: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> SlayerDataset | None:
    """The dataset ``edge`` reaches from ``current``; a list child is minted when the map lacks it."""
    target = datasets_by_name.get(edge.to)
    if target is None and edge.lateral is not None and edge.cardinality is RelationshipCardinality.ONE_TO_MANY:
        field = current.get_field(edge.lateral)
        if field is not None:
            return list_child(parent=current, field=field)
    return target


def is_product_edge(edge: OrientedRelationship) -> bool:
    """A keyless product hop (never a declared or axis edge)."""
    return edge.product is not None


def is_unrelated_edge(edge: OrientedRelationship) -> bool:
    return edge.product == "unrelated"


def _declared_toward(dataset: SlayerDataset, target: str, *, spine: bool) -> list[DatasetRelationship]:
    relationships = spine_relationships(dataset, datasets_by_name={TIME_SPINE_DATASET: dataset}) if spine else dataset.relationships
    return [j for j in relationships if j.to == target]


def edges_between(*, source: SlayerDataset, target: SlayerDataset) -> list[OrientedRelationship]:
    """Every edge connecting ``source`` and ``target``, oriented source→target.

    Inspects both datasets' declarations (and the spine's virtual edges), so the answer
    is independent of which side stores the relationship. Never raises — parallel edges
    surface as a length-≥2 list for the caller to reject.
    """
    out: list[OrientedRelationship] = []
    if source.name == target.name:
        return out  # a self-relationship takes no part in paths
    spellings = {m.name: m.explicit_spelling for m in (source, target)}
    if source.population_spine and is_spine(target):
        out.append(_product_edge(source))
    for j in _declared_toward(source, target.name, spine=is_spine(target)):
        out.append(_orient(relationship=j, declaring=source.name, from_dataset=source.name,
                           spellings=spellings))
    for j in _declared_toward(target, source.name, spine=is_spine(source)):
        out.append(_orient(relationship=j, declaring=target.name, from_dataset=source.name,
                           spellings=spellings))
    out.extend(e for e in _list_edges(dataset=source, datasets_by_name={target.name: target})
               if e.to == target.name)
    _observe((e.to for e in out), strict=False)
    return out


def relationship_key_fields(
    *, edge: OrientedRelationship, source: SlayerDataset, target: SlayerDataset,
) -> list[tuple[DatasetField, DatasetField]]:
    """``edge``'s key pairs as their declared fields; raises ``RelationshipKeyError`` on a non-base key."""
    declared_target = (edge.to if edge.declaring_dataset == edge.source_dataset
                       else edge.source_dataset)

    def field(key: str, dataset: SlayerDataset) -> DatasetField:
        err = relationship_key_error(dataset=edge.declaring_dataset, target=declared_target, key=key,
                             side=dataset.name, fields=dataset.fields)
        if err is not None:
            raise err
        col = dataset.get_field(key)
        assert col is not None
        return col

    return [(field(src, source), field(tgt, target)) for src, tgt in edge.column_pairs]


def _virtual_spine_edges(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> list[OrientedRelationship]:
    """``dataset``'s product and detached-dataset hops (none without the spine in the universe)."""
    spine = datasets_by_name.get(TIME_SPINE_DATASET)
    if spine is None:
        return []
    if dataset.population_spine:
        return [_product_edge(dataset)]
    if is_spine(dataset):
        return [_detached_edge(source=dataset, target=datasets_by_name[n]) for n in sorted(_detached_names(datasets_by_name))]
    # An axis is a to-one chain of its own: only an axis-less dataset can be detached.
    if axis_field(dataset) is None and dataset.name in _detached_names(datasets_by_name):
        return [_detached_edge(source=dataset, target=spine)]
    return []


def neighbors(
    *, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]
) -> list[OrientedRelationship]:
    """Every hop incident to ``dataset``, oriented from ``dataset``.

    Outgoing declarations first, then edges declared on other datasets that reach
    ``dataset`` (inverted). Never raises.
    """
    spellings = {n: m.explicit_spelling for n, m in datasets_by_name.items()}
    spellings[dataset.name] = dataset.explicit_spelling
    out = _virtual_spine_edges(dataset=dataset, datasets_by_name=datasets_by_name)
    out.extend(_list_edges(dataset=dataset, datasets_by_name=datasets_by_name))
    for j in spine_relationships(dataset, datasets_by_name=datasets_by_name):
        if j.to != dataset.name:
            out.append(_orient(relationship=j, declaring=dataset.name, from_dataset=dataset.name,
                               spellings=spellings))
    for other in datasets_by_name.values():
        if other.name == dataset.name:
            continue
        for j in spine_relationships(other, datasets_by_name=datasets_by_name):
            if j.to == dataset.name:
                out.append(_orient(relationship=j, declaring=other.name, from_dataset=dataset.name,
                                   spellings=spellings))
    _observe((e.to for e in out), strict=False)
    return out


def incident_dataset_names(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> list[str]:
    """Names of every dataset sharing a declared edge with ``dataset``, loaded or not."""
    return sorted({e.to for e in neighbors(dataset=dataset, datasets_by_name=datasets_by_name) if e.lateral is None})


def resolve_hop(
    *,
    current: SlayerDataset,
    token: str,
    datasets_by_name: dict[str, SlayerDataset],
    quoted: bool = False,
) -> OrientedRelationship | None:
    """Resolve one path ``token`` from ``current``.

    A token matches an incident edge's ``name`` first, then a query stage's
    spelling, else the opposite-endpoint dataset name. Returns ``None`` when nothing matches (the caller raises its own
    reference error) and raises :class:`AmbiguousRelationshipPathError` when two or more
    edges qualify, :class:`AmbiguousHopError` when it names an edge and another edge's target.
    """
    incident = neighbors(dataset=current, datasets_by_name=datasets_by_name)
    candidates = matching_edges(incident, token, quoted=quoted)
    if not candidates:
        _refuse_self_hop(current=current, token=token, quoted=quoted)
        candidates = _unrelated_hops(current=current, token=token, datasets_by_name=datasets_by_name, quoted=quoted)
    if not candidates:
        return None
    if len(candidates) > 1:
        raise AmbiguousRelationshipPathError(
            source_dataset=current.name,
            target_dataset=candidates[0].to,
            candidates=candidates,
            token=token,
        )
    _refuse_edge_target_clash(current=current, edge=candidates[0], incident=incident, token=token, quoted=quoted)
    _observe([candidates[0].to], strict=True)
    return candidates[0]


def _refuse_edge_target_clash(
    *, current: SlayerDataset, edge: OrientedRelationship, incident: list[OrientedRelationship], token: str,
    quoted: bool,
) -> None:
    """Raise when ``token`` names ``edge`` by its edge name and is also the target dataset of an incident edge into
    another dataset."""
    if edge.name is None or match_name(name=token, declared=[edge.name], quoted=quoted) is None:
        return
    other = next((e for e in incident if e.to != edge.to and e.lateral is None and not is_product_edge(e)
                  and match_name(name=token, declared=[e.target_spelling or e.to], quoted=quoted) is not None), None)
    if other is not None:
        raise AmbiguousHopError(dataset=current.spelling, segment=token, edge=edge.name,
                                other_edge=canonical_token(other))


def _refuse_self_hop(*, current: SlayerDataset, token: str, quoted: bool) -> None:
    """Raise when ``token`` spells a hop over one of ``current``'s self-relationships."""
    spellings = {current.name: current.explicit_spelling}
    loops = [_orient(relationship=j, declaring=current.name, from_dataset=current.name, spellings=spellings)
             for j in current.relationships if j.to == current.name]
    found = matching_edges(loops, token, quoted=quoted)
    if found:
        raise SelfRelationshipTraversalError(dataset=current.spelling, relationship=token)


def _unrelated_hops(
    *, current: SlayerDataset, token: str, datasets_by_name: dict[str, SlayerDataset], quoted: bool = False,
) -> list[OrientedRelationship]:
    """The product hop from a query frame's population to the unrelated dataset ``token`` names."""
    targets = [datasets_by_name[n] for n in sorted(current.unrelated_datasets) if n in datasets_by_name]
    edges = []
    for t in targets:
        hop = product_token(source=current, target=t, datasets_by_name=datasets_by_name)
        edges.append(_unrelated_edge(source=current, target=t, token=None if hop == t.spelling else hop))
    return matching_edges(edges, token, quoted=quoted)


def addresses_alone(*, edge: OrientedRelationship, incident: list[OrientedRelationship]) -> bool:
    """Whether ``edge``'s canonical token names it alone among ``incident`` (its source's edges)."""
    return matching_edges(incident, canonical_token(edge)) == [edge]


def addressable_token(
    *, current: SlayerDataset, edge: OrientedRelationship, datasets_by_name: dict[str, SlayerDataset],
) -> str | None:
    """``edge``'s hop token from ``current`` (name first), or ``None`` when the hop is unaddressable."""
    incident = neighbors(dataset=current, datasets_by_name=datasets_by_name)
    return canonical_token(edge) if addresses_alone(edge=edge, incident=incident) else None


class WalkMiss(BaseModel):
    """Why a walk returned ``None``: the failing hop and whether it has no edge, an unloaded target, or crosses the spine."""

    model_config = ConfigDict(frozen=True)

    index: int
    token: str
    kind: Literal["no_edge", "unloaded", "spine"]
    target: str | None = None


def _walk(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
    revisits: bool,
    quoted: Sequence[bool] = (),
) -> tuple[list[OrientedRelationship] | None, WalkMiss | None]:
    current = root
    visited = {root.name}
    chain: list[OrientedRelationship] = []
    for i, token in enumerate(path):
        if chain and is_spine(current):
            return None, WalkMiss(index=i, token=token, kind="spine")  # a path may end at the spine, never cross it
        edge = resolve_hop(
            current=current, token=token, datasets_by_name=datasets_by_name, quoted=i < len(quoted) and quoted[i],
        )
        if edge is None:
            return None, WalkMiss(index=i, token=token, kind="no_edge")
        if not revisits and edge.to in visited:
            raise CircularRelationshipPathError(
                reference=".".join(path),
                root_dataset=root.spelling,
                revisited=edge.to,
                hop=token,
                via=current.name,
            )
        nxt = _hop_target(edge=edge, current=current, datasets_by_name=datasets_by_name)
        if nxt is None:
            return None, WalkMiss(index=i, token=token, kind="unloaded", target=edge.to)
        visited.add(edge.to)
        chain.append(edge)
        current = nxt
    return chain, None


def walk(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
    quoted: Sequence[bool] = (),
) -> list[OrientedRelationship] | None:
    """Resolve a full ``path`` of tokens spelled in one frame from ``root`` into an oriented chain.

    Returns the chain, ``None`` only on an unknown/unloaded hop, propagates
    :class:`AmbiguousRelationshipPathError` from any ambiguous hop, and raises
    :class:`CircularRelationshipPathError` when a hop revisits a dataset already on the path
    (a definite malformation, not a miss).
    """
    return _walk(root=root, path=path, datasets_by_name=datasets_by_name, revisits=False, quoted=quoted)[0]


def walk_miss(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
    quoted: Sequence[bool] = (),
) -> WalkMiss | None:
    """Why :func:`walk` returns ``None`` for ``path`` (``None`` when it does not); raises as :func:`walk` does."""
    return _walk(root=root, path=path, datasets_by_name=datasets_by_name, revisits=False, quoted=quoted)[1]


def walk_terminal(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
    quoted: Sequence[bool] = (),
) -> tuple[SlayerDataset, tuple[str, ...]] | None:
    """``(terminal dataset, canonical path)`` of :func:`walk`, ``None`` where it misses; raises as it does."""
    datasets = with_list_children({**datasets_by_name, root.name: datasets_by_name.get(root.name, root)})
    chain = walk(root=root, path=path, datasets_by_name=datasets, quoted=quoted)
    if chain is None:
        return None
    return (datasets[chain[-1].to] if chain else root), canonical_path(chain)


def follow(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> list[OrientedRelationship] | None:
    """:func:`walk` for an already-resolved, possibly composed key path: revisits are separate occurrences."""
    return _walk(root=root, path=path, datasets_by_name=datasets_by_name, revisits=True)[0]


def _owner_stack(
    *, root: SlayerDataset, owner_path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> list[SlayerDataset] | None:
    """The occurrence sequence of the (trusted, already-resolved) ``owner_path`` from ``root``;
    ``None`` when it does not resolve."""
    try:
        chain = follow(root=root, path=owner_path, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return None
    if chain is None:
        return None
    datasets = with_list_children(datasets_by_name)
    return [root, *(datasets[e.to] for e in chain)]


def _incident_named(
    *, current: SlayerDataset, token: str, datasets_by_name: dict[str, SlayerDataset],
) -> OrientedRelationship | None:
    """The incident edge of ``current`` whose declared ``name`` is ``token``, or
    ``None``. Raises :class:`AmbiguousRelationshipPathError` when two share the name."""
    incident = neighbors(dataset=current, datasets_by_name=datasets_by_name)
    named = [e for e in incident if e.name == token]
    if not named:
        return None
    if len(named) > 1:
        raise AmbiguousRelationshipPathError(
            source_dataset=current.name, target_dataset=named[0].to,
            candidates=named, token=token,
        )
    _refuse_edge_target_clash(current=current, edge=named[0], incident=incident, token=token, quoted=False)
    _observe([named[0].to], strict=True)
    return named[0]


def walk_cancelling(
    *,
    root: SlayerDataset,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> tuple[str, ...] | None:
    """Resolve a definition default's qualifier ``tokens`` in the owner's frame
    (the owner is ``root`` followed along ``owner_path``), with reverse-hop
    cancellation.

    Per token, precedence is incident edge name → a dataset name on the path
    (cancel) → a dataset-name hop: an incident edge-name token always hops and
    never cancels; a token equal to a dataset already on ``root + owner_path``
    truncates the absolute path back to its innermost occurrence — keeping ``owner_path``'s spelling —
    then resolution continues forward from there. Returns the absolute canonical
    path from ``root`` (``()`` = the root itself), ``None`` on a miss or a hop
    onto an already-visited dataset (see :func:`cancelling_revisit`); ambiguity raises."""
    path, _revisit = _cancelling_walk(
        root=root, owner_path=owner_path, tokens=tokens, datasets_by_name=datasets_by_name,
    )
    return path


def cancelling_revisit(
    *,
    root: SlayerDataset,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> tuple[str, str, str] | None:
    """``(hop, revisited, via)`` when :func:`walk_cancelling` misses because a hop
    lands on a dataset already on the path, else ``None``."""
    _path, revisit = _cancelling_walk(
        root=root, owner_path=owner_path, tokens=tokens, datasets_by_name=datasets_by_name,
    )
    return revisit


def _cancelling_walk(
    *,
    root: SlayerDataset,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> tuple[tuple[str, ...] | None, tuple[str, str, str] | None]:
    datasets = dict(datasets_by_name)
    datasets.setdefault(root.name, root)
    datasets = with_list_children(datasets)
    stack = _owner_stack(root=root, owner_path=owner_path, datasets_by_name=datasets)
    if stack is None:
        return None, None
    path_tokens: list[str] = list(owner_path)
    for token in tokens:
        edge = _incident_named(current=stack[-1], token=token, datasets_by_name=datasets)
        if edge is None:
            cancel_at = next((i for i in reversed(range(len(stack))) if stack[i].spelling == token), None)
            if cancel_at is not None:
                del stack[cancel_at + 1:]
                del path_tokens[cancel_at:]
                continue
            edge = resolve_hop(current=stack[-1], token=token, datasets_by_name=datasets)
        if edge is None:
            return None, None
        if any(m.name == edge.to for m in stack):
            return None, (token, edge.to, stack[-1].name)
        nxt = datasets.get(edge.to)
        if nxt is None:
            return None, None
        stack.append(nxt)
        path_tokens.append(canonical_token(edge))
    return tuple(path_tokens), None


def _terminal(
    *, root: SlayerDataset, path: Sequence[str], datasets_by_name: dict[str, SlayerDataset], revisits: bool,
) -> SlayerDataset | None:
    datasets = dict(datasets_by_name)
    datasets.setdefault(root.name, root)
    datasets = with_list_children(datasets)
    try:
        chain, _miss = _walk(root=root, path=path, datasets_by_name=datasets, revisits=revisits)
    except (AmbiguousRelationshipPathError, CircularRelationshipPathError):
        return None
    if chain is None:
        return None
    return datasets.get(chain[-1].to) if chain else root


def terminal_dataset(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> SlayerDataset | None:
    """Best-effort terminal dataset of ``path`` from ``root``: ``None`` on an
    unresolvable, revisiting, or ambiguous hop — for callers whose contract is
    to skip rather than raise (the strict doors fail closed on their own)."""
    return _terminal(root=root, path=path, datasets_by_name=datasets_by_name, revisits=False)


def resolved_terminal(
    *,
    root: SlayerDataset,
    path: Sequence[str],
    datasets_by_name: dict[str, SlayerDataset],
) -> SlayerDataset | None:
    """:func:`terminal_dataset` of an already-resolved, possibly composed key path (revisits allowed)."""
    return _terminal(root=root, path=path, datasets_by_name=datasets_by_name, revisits=True)


def dataset_field(
    *, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], key: object,
) -> Optional[DatasetField]:
    """The field leaf ``key`` names, rooted at ``dataset``; ``None`` for anything unresolvable."""
    if isinstance(key, FieldSqlKey):
        owner = dataset if key.dataset == dataset.name else datasets_by_name.get(key.dataset)
        leaf = key.field_name
    elif isinstance(key, FieldKey):
        owner = resolved_terminal(root=dataset, path=key.path, datasets_by_name=datasets_by_name)
        leaf = key.leaf
    else:
        return None
    return owner.get_field(leaf) if owner is not None else None


def dataset_field_type(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> FieldTypeFn:
    """Declared types of field leaves rooted at ``dataset``; ``None`` for anything unresolvable."""
    def field_type(key: ValueKey) -> Optional[DataType]:
        col = dataset_field(dataset=dataset, datasets_by_name=datasets_by_name, key=key)
        return col.datatype if col is not None else None
    return field_type


def dataset_field_element(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> FieldElementFn:
    """Element types of list-typed field leaves rooted at ``dataset``; ``None`` for anything else."""
    def field_element(key: ValueKey) -> Optional[DataType]:
        col = dataset_field(dataset=dataset, datasets_by_name=datasets_by_name, key=key)
        return col.element_type if col is not None else None
    return field_element


def aggregation_owner(
    *, root: SlayerDataset | None, source: ValueKey, datasets_by_name: dict[str, SlayerDataset],
) -> SlayerDataset | None:
    """The dataset declaring ``source``'s aggregation: ``root`` walked along the source
    anchor; ``None`` with no root or an unresolvable hop."""
    if root is None:
        return None
    return resolved_terminal(
        root=root, path=source_anchor_path(source), datasets_by_name=datasets_by_name,
    )
