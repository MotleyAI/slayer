"""Join-edge identity: the declaration reference and the FK relationship signature."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import RelationshipCardinality, JoinType, invert_cardinality
from slayer.core.errors import QueryExtensionError
from slayer.core.relationship_walker import OrientedRelationship, canonical_token, orient_relationship
from slayer.core.models import DatasetRelationship, SlayerDataset, _check_relationship_keys, _validate_dataset_name

__all__ = [
    "EdgeMerge",
    "ExtensionEdge",
    "RelationshipEdgeRef",
    "RelationshipSignature",
    "edge_reference",
    "is_addressable",
    "is_valid_edge_name",
    "relationship_signature",
    "resolve_relationship_ref",
    "resolve_relationship_refs",
    "without_relationships",
]

#: ``(lower model, higher model, key pairs oriented lower → higher)``.
RelationshipSignature = tuple[str, str, frozenset[tuple[str, str]]]


class RelationshipEdgeRef(BaseModel):
    """One join declaration of a model, addressed exactly."""

    to: str
    name: str | None = None
    join_pairs: list[list[str]]

    @classmethod
    def of(cls, relationship: DatasetRelationship) -> RelationshipEdgeRef:
        return cls(to=relationship.to, name=relationship.name, join_pairs=[list(p) for p in relationship.join_pairs])

    def matches(self, relationship: DatasetRelationship) -> bool:
        return (
            relationship.to == self.to
            and _pair_set(relationship.join_pairs) == _pair_set(self.join_pairs)
            and (self.name is None or relationship.name == self.name)
        )

    def matches_hop(self, edge: OrientedRelationship) -> bool:
        """Whether ``edge`` walks this declaration, in either direction."""
        forward = edge.source_dataset == edge.declaring_dataset
        pairs = edge.join_pairs if forward else [[t, s] for s, t in edge.join_pairs]
        return (
            (edge.to if forward else edge.source_dataset) == self.to
            and _pair_set(pairs) == _pair_set(self.join_pairs)
            and (self.name is None or edge.name == self.name)
        )


def _pair_set(pairs: list[list[str]]) -> frozenset[tuple[str, str]]:
    return frozenset((p[0], p[1]) for p in pairs)


class ExtensionEdge(BaseModel):
    """A query extension's join as merged: the edge oriented from its declaring model, and the token reaching it."""

    model_config = ConfigDict(frozen=True)

    edge: OrientedRelationship
    token: str


class _Candidate(BaseModel):
    """An edge between the declaring model and the target, read from the declaring model."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    declaring: str
    index: int
    relationship: DatasetRelationship
    pairs: frozenset[tuple[str, str]]
    cardinality: RelationshipCardinality | None


def _check_unique(matches: list[_Candidate], *, label: str) -> None:
    if len(matches) > 1:
        names = ", ".join(repr(c.relationship.name or c.relationship.to) for c in matches)
        raise QueryExtensionError(f"The {label} matches {len(matches)} edges ({names}); name the edge it means.")


def _cardinality_label(cardinality: RelationshipCardinality | None) -> str:
    return cardinality.value if cardinality is not None else "unset"


class EdgeMerge:
    """Merges query extension joins into a model universe: a join repeating an edge, in either orientation, is it."""

    def __init__(
        self, *, datasets: Mapping[str, SlayerDataset], reserved_names: Iterable[str] = (),
        stored_edge_names: Iterable[str] | None = None,
    ) -> None:
        self.datasets: dict[str, SlayerDataset] = dict(datasets)
        self._reserved = frozenset(reserved_names)
        self._edge_names = frozenset(
            stored_edge_names if stored_edge_names is not None
            else (j.name for m in datasets.values() for j in m.relationships if j.name and not j.query_local)
        )
        self._local_names: set[str] = set()
        self._alias_edges: dict[str, tuple[str, int]] = {}

    def add(self, *, declaring: str, relationship: DatasetRelationship) -> ExtensionEdge:
        """Merge ``relationship``, declared by an extension of ``declaring``; raises ``QueryExtensionError`` on a conflict."""
        source = self.datasets[declaring]
        target = self.datasets.get(relationship.to)
        label = f"extension {_describe(relationship)} of {source.spelling!r}"
        if relationship.to == declaring:
            raise QueryExtensionError(f"The {label} joins the model to itself.")
        candidates = self._candidates(source=source, target_name=relationship.to)
        pairs = _pair_set(relationship.join_pairs)
        named = [c for c in candidates if relationship.name is not None and c.relationship.name == relationship.name]
        _check_unique(named, label=label)
        if named:
            match = named[0]
            if match.pairs != pairs:
                raise QueryExtensionError(
                    f"The {label} is named like the edge {relationship.name!r}, which joins on "
                    f"{sorted(match.pairs)}; use another name or the edge's join_pairs."
                )
            return self._reuse(match, relationship=relationship, extended=declaring, label=label, alias=None)
        matches = [c for c in candidates if c.pairs == pairs]
        _check_unique(matches, label=label)
        target_spelling = target.spelling if target is not None else relationship.to
        declared = relationship.name if relationship.name != target_spelling else None
        if matches:
            return self._reuse(matches[0], relationship=relationship, extended=declaring, label=label, alias=declared)
        return self._add_new(source=source, target=target, relationship=relationship, name=declared, label=label)

    def _candidates(self, *, source: SlayerDataset, target_name: str) -> list[_Candidate]:
        out = [
            _Candidate(declaring=source.name, index=i, relationship=j, pairs=_pair_set(j.join_pairs), cardinality=j.cardinality)
            for i, j in enumerate(source.relationships) if j.to == target_name
        ]
        target = self.datasets.get(target_name)
        for i, j in enumerate(target.relationships if target is not None else []):
            if j.to == source.name:
                out.append(_Candidate(
                    declaring=target_name, index=i, relationship=j, pairs=frozenset((t, s) for s, t in _pair_set(j.join_pairs)),
                    cardinality=invert_cardinality(j.cardinality),
                ))
        return out

    def _reuse(
        self, match: _Candidate, *, relationship: DatasetRelationship, extended: str, label: str, alias: str | None,
    ) -> ExtensionEdge:
        if None not in (relationship.cardinality, match.cardinality) and relationship.cardinality != match.cardinality:
            raise QueryExtensionError(
                f"The {label} declares cardinality {_cardinality_label(relationship.cardinality)}, but that edge is "
                f"{_cardinality_label(match.cardinality)} in the same direction."
            )
        stored = match.relationship
        if stored.query_local:
            stored = self._name_local(match, name=alias, label=label)
            alias = None
        elif alias is not None and alias in self._edge_names:
            raise QueryExtensionError(f"The {label} names another stored edge {alias!r}.")
        elif alias is not None:
            self._claim_alias(alias, edge=(match.declaring, match.index), label=label)
        if alias is not None and alias not in stored.aliases:
            stored = self._replace(match, stored.with_aliases((*stored.aliases, alias)))
        return self._edge(declaring=match.declaring, relationship=stored, from_dataset=extended, token=alias)

    def _name_local(self, match: _Candidate, *, name: str | None, label: str) -> DatasetRelationship:
        """A query-local edge declared again: one name, or none, is one edge."""
        local = match.relationship
        if name is None or name == local.name:
            return local
        if local.name is not None:
            raise QueryExtensionError(
                f"The {label} names the relationship {name!r}, which another extension names {local.name!r}; "
                f"declare one name."
            )
        self._check_new_name(name, label=label)
        return self._replace(match, local.model_copy(update={"name": name}))

    def _add_new(
        self, *, source: SlayerDataset, target: SlayerDataset | None, relationship: DatasetRelationship, name: str | None, label: str,
    ) -> ExtensionEdge:
        if name is not None:
            self._check_new_name(name, label=label)
        new = relationship.model_copy(update={"name": name, "join_type": JoinType.LEFT}).as_query_local()
        if not source.awaits_fields:
            _check_relationship_keys(dataset_name=source.name, fields=source.fields, relationships=[new])
        self.datasets[source.name] = source.model_copy(update={"relationships": [*source.relationships, new]})
        edge = orient_relationship(relationship=new, declaring=self.datasets[source.name], other=target, from_dataset=source.name)
        return ExtensionEdge(edge=edge, token=canonical_token(edge))

    def _check_new_name(self, name: str, *, label: str) -> None:
        if name in self._edge_names or name in self._local_names:
            raise QueryExtensionError(f"The {label} is named {name!r}, which names another edge.")
        if name in self._reserved:
            raise QueryExtensionError(f"The {label} is named {name!r}, which names a model or stage.")
        self._local_names.add(name)

    def _claim_alias(self, alias: str, *, edge: tuple[str, int], label: str) -> None:
        """Register ``alias`` for the stored ``edge``; one alias names one edge."""
        if self._alias_edges.get(alias) == edge:
            return
        self._check_new_name(alias, label=label)
        self._alias_edges[alias] = edge

    def _replace(self, match: _Candidate, relationship: DatasetRelationship) -> DatasetRelationship:
        model = self.datasets[match.declaring]
        joins = list(model.relationships)
        joins[match.index] = relationship
        self.datasets[match.declaring] = model.model_copy(update={"relationships": joins})
        return relationship

    def _edge(self, *, declaring: str, relationship: DatasetRelationship, from_dataset: str, token: str | None) -> ExtensionEdge:
        """``relationship`` (stored on ``declaring``) oriented from the extended model ``from_dataset``."""
        edge = orient_relationship(
            relationship=relationship, declaring=self.datasets[declaring], other=self.datasets.get(relationship.to), from_dataset=from_dataset,
        )
        return ExtensionEdge(edge=edge, token=token or canonical_token(edge))


def _relationships_to(dataset: SlayerDataset, target: str) -> list[DatasetRelationship]:
    return [j for j in dataset.relationships if j.to == target]


def edge_reference(*, dataset: SlayerDataset, relationship: DatasetRelationship) -> str:
    """The target when ``relationship`` is ``dataset``'s only join to it, else the edge name."""
    if len(_relationships_to(dataset, relationship.to)) == 1 or not relationship.name:
        return relationship.to
    return relationship.name


def is_addressable(*, dataset: SlayerDataset, relationship: DatasetRelationship) -> bool:
    """Whether ``relationship``'s edge reference names it alone on ``dataset``."""
    return bool(relationship.name) or len(_relationships_to(dataset, relationship.to)) == 1


def is_valid_edge_name(name: str) -> bool:
    """Whether ``name`` passes the model-name identifier rules edge names follow."""
    try:
        _validate_dataset_name(name, "Join")
    except ValueError:
        return False
    return bool(name)


def _describe(relationship: DatasetRelationship) -> str:
    label = f" '{relationship.name}'" if relationship.name else ""
    return f"join{label} to '{relationship.to}' on {relationship.join_pairs}"


def resolve_relationship_ref(*, dataset: SlayerDataset, ref: str | RelationshipEdgeRef) -> DatasetRelationship:
    """The one join of ``dataset`` that ``ref`` addresses; raises ``ValueError`` on none or several."""
    if isinstance(ref, RelationshipEdgeRef):
        hits = [j for j in dataset.relationships if ref.matches(j)]
        label = f"{ref.to!r} on {ref.join_pairs}" + (f" named {ref.name!r}" if ref.name else "")
    else:
        hits = [j for j in dataset.relationships if j.name == ref] or _relationships_to(dataset, ref)
        label = repr(ref)
    if not hits:
        raise ValueError(f"Join {label} not found on model '{dataset.name}'.")
    if len(hits) > 1:
        candidates = "; ".join(_describe(j) for j in hits)
        raise ValueError(
            f"Join {label} on model '{dataset.name}' matches {len(hits)} joins ({candidates}); "
            f"address one by its edge name or by its exact target and join_pairs."
        )
    return hits[0]


def resolve_relationship_refs(*, dataset: SlayerDataset, refs: Iterable[str | RelationshipEdgeRef]) -> list[DatasetRelationship]:
    """The distinct joins ``refs`` address; raises ``ValueError`` on a missing or ambiguous one."""
    out: list[DatasetRelationship] = []
    for ref in refs:
        join = resolve_relationship_ref(dataset=dataset, ref=ref)
        if not any(join is j for j in out):
            out.append(join)
    return out


def without_relationships(*, dataset: SlayerDataset, relationships: list[DatasetRelationship]) -> list[DatasetRelationship]:
    """``dataset``'s joins minus ``relationships`` (by identity)."""
    return [j for j in dataset.relationships if not any(j is r for r in relationships)]


def relationship_signature(*, dataset_name: str, relationship: DatasetRelationship) -> RelationshipSignature:
    """Name- and orientation-insensitive identity of the relationship ``relationship`` declares."""
    forward = _pair_set(relationship.join_pairs)
    backward = frozenset((t, s) for s, t in forward)
    if dataset_name < relationship.to:
        return (dataset_name, relationship.to, forward)
    if dataset_name > relationship.to:
        return (relationship.to, dataset_name, backward)
    return (dataset_name, dataset_name, min(forward, backward, key=sorted))
