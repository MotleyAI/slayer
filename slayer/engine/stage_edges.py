"""The edges a stage inherits on its pass-through key columns, and how declared edges absorb them."""

from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from slayer.core.enums import JoinType, RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError
from slayer.core.keys import FieldKey, ValueKey
from slayer.core.models import DatasetRelationship, RelationshipOrigin, SlayerDataset
from slayer.core.relationship_walker import OrientedRelationship, follow, neighbors, unobserved
from slayer.core.scope import InheritedEdge, PassThrough, StageField

__all__ = [
    "inherited_edges",
    "pass_through",
]

_EdgeId = Tuple[str, str, str, Optional[str], frozenset]
_Position = Tuple[_EdgeId, ...]


def pass_through(
    *, key: ValueKey, root: Optional[SlayerDataset], datasets_by_name: Dict[str, SlayerDataset],
) -> Optional[PassThrough]:
    """What a row-level output of ``key`` passes through unchanged; ``None`` unless ``key`` is a plain field."""
    if not isinstance(key, FieldKey) or root is None:
        return None
    try:
        with unobserved():
            chain = follow(root=root, path=key.path, datasets_by_name=datasets_by_name)
    except AmbiguousRelationshipPathError:
        return None
    return None if chain is None else PassThrough(path=tuple(chain), field=key.leaf)


def _edge_id(edge: OrientedRelationship) -> _EdgeId:
    return (edge.declaring_dataset, edge.source_dataset, edge.to, edge.name, frozenset(edge.column_pairs))


def _pairs(pairs: Iterable[Sequence[str]]) -> frozenset:
    return frozenset((p[0], p[1]) for p in pairs)


def _declaration(edge: OrientedRelationship, datasets_by_name: Mapping[str, SlayerDataset]) -> Optional[DatasetRelationship]:
    declaring = datasets_by_name.get(edge.declaring_dataset)
    if declaring is None:
        return None
    forward = edge.source_dataset == edge.declaring_dataset
    target = edge.to if forward else edge.source_dataset
    pairs = _pairs(edge.column_pairs if forward else [(t, s) for s, t in edge.column_pairs])
    return next((j for j in declaring.relationships
                 if j.to == target and j.name == edge.name and _pairs(j.column_pairs) == pairs), None)


def _origin(
    edge: OrientedRelationship, declaration: DatasetRelationship, datasets_by_name: Mapping[str, SlayerDataset],
) -> RelationshipOrigin:
    forward = edge.source_dataset == edge.declaring_dataset
    inherited = declaration.inherited_from
    if inherited is not None:
        return inherited.model_copy(update={"forward": inherited.forward == forward})
    declaring = datasets_by_name[edge.declaring_dataset]
    target = datasets_by_name.get(declaration.to)
    return RelationshipOrigin(
        dataset=declaring.name, dataset_spelling=declaring.spelling,
        relationship=declaration.name or (target.spelling if target is not None else declaration.to),
        to=declaration.to, name=declaration.name, pairs=_pairs(declaration.column_pairs), forward=forward,
    )


def _one(cardinality: Optional[RelationshipCardinality], *, target_side: bool) -> Optional[bool]:
    if cardinality is None:
        return None
    if target_side:
        return cardinality in (RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_ONE)
    return cardinality in (RelationshipCardinality.ONE_TO_MANY, RelationshipCardinality.ONE_TO_ONE)


_COMPOSED = {
    (True, True): RelationshipCardinality.ONE_TO_ONE,
    (True, False): RelationshipCardinality.ONE_TO_MANY,
    (False, True): RelationshipCardinality.MANY_TO_ONE,
    (False, False): RelationshipCardinality.MANY_TO_MANY,
}


def _cardinality(
    edge: OrientedRelationship, *, columns: Sequence[str], grain: Optional[Sequence[str]],
) -> Optional[RelationshipCardinality]:
    """The target side keeps the edge's arity; the stage side is one iff the grain is covered by the key."""
    target_one = _one(edge.cardinality, target_side=True)
    if target_one is None:
        return None
    stage_one = grain is not None and set(grain) <= set(columns)
    return _COMPOSED[(stage_one, target_one)]


def inherited_edges(
    *, fields: Sequence[StageField], grain: Optional[Sequence[str]], root: Optional[SlayerDataset],
    datasets_by_name: Dict[str, SlayerDataset],
) -> Tuple[InheritedEdge, ...]:
    """Every edge touching a dataset the stage reads whose key the stage passes through, oriented from the stage."""
    if root is None:
        return ()
    columns: Dict[_Position, Dict[str, str]] = {(): {}}
    positions: Dict[_Position, Tuple[OrientedRelationship, ...]] = {(): ()}
    for c in fields:
        if c.pass_through is None:
            continue
        path = c.pass_through.path
        for i in range(len(path) + 1):
            key = tuple(_edge_id(e) for e in path[:i])
            positions.setdefault(key, path[:i])
            columns.setdefault(key, {})
        columns[tuple(_edge_id(e) for e in path)].setdefault(c.pass_through.field, c.name)
    out: List[InheritedEdge] = []
    seen: set = set()
    with unobserved():
        for key, path in positions.items():
            dataset = root if not path else datasets_by_name.get(path[-1].to)
            if dataset is None or dataset.list_of is not None:
                continue
            for edge in neighbors(dataset=dataset, datasets_by_name=datasets_by_name):
                inherited = _inherit(edge, at=key, columns=columns, grain=grain, datasets_by_name=datasets_by_name)
                if inherited is not None and inherited.origin not in seen:
                    seen.add(inherited.origin)
                    out.append(inherited)
    return tuple(out)


def _inherit(
    edge: OrientedRelationship, *, at: _Position, columns: Mapping[_Position, Mapping[str, str]],
    grain: Optional[Sequence[str]], datasets_by_name: Mapping[str, SlayerDataset],
) -> Optional[InheritedEdge]:
    if edge.product is not None or edge.lateral is not None or edge.to not in datasets_by_name:
        return None
    own, proxy = columns.get(at, {}), columns.get((*at, _edge_id(edge)), {})
    picked = [own.get(d) or proxy.get(t) for d, t in edge.column_pairs]
    declaration = _declaration(edge, datasets_by_name)
    if declaration is None or not picked or any(c is None for c in picked):
        return None
    from_columns = tuple(c for c in picked if c is not None)
    return InheritedEdge(
        to=edge.to, from_columns=from_columns, to_columns=tuple(t for _, t in edge.column_pairs), name=edge.name,
        join_type=JoinType.LEFT if declaration.query_local else edge.join_type,
        cardinality=_cardinality(edge, columns=from_columns, grain=grain),
        origin=_origin(edge, declaration, datasets_by_name),
    )
