"""Join-edge identity: the declaration reference and the FK relationship signature."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import JoinCardinality, JoinType, invert_cardinality
from slayer.core.errors import QueryExtensionError
from slayer.core.join_walker import OrientedJoin, canonical_token, orient_join
from slayer.core.models import ModelJoin, SlayerModel, _check_join_keys, _validate_model_name

__all__ = [
    "EdgeMerge",
    "ExtensionEdge",
    "JoinEdgeRef",
    "RelationshipSignature",
    "edge_reference",
    "is_addressable",
    "is_valid_edge_name",
    "relationship_signature",
    "resolve_join_ref",
    "resolve_join_refs",
    "without_joins",
]

#: ``(lower model, higher model, key pairs oriented lower → higher)``.
RelationshipSignature = tuple[str, str, frozenset[tuple[str, str]]]


class JoinEdgeRef(BaseModel):
    """One join declaration of a model, addressed exactly."""

    target_model: str
    name: str | None = None
    join_pairs: list[list[str]]

    @classmethod
    def of(cls, join: ModelJoin) -> JoinEdgeRef:
        return cls(target_model=join.target_model, name=join.name, join_pairs=[list(p) for p in join.join_pairs])

    def matches(self, join: ModelJoin) -> bool:
        return (
            join.target_model == self.target_model
            and _pair_set(join.join_pairs) == _pair_set(self.join_pairs)
            and (self.name is None or join.name == self.name)
        )

    def matches_hop(self, edge: OrientedJoin) -> bool:
        """Whether ``edge`` walks this declaration, in either direction."""
        forward = edge.source_model == edge.declaring_model
        pairs = edge.join_pairs if forward else [[t, s] for s, t in edge.join_pairs]
        return (
            (edge.target_model if forward else edge.source_model) == self.target_model
            and _pair_set(pairs) == _pair_set(self.join_pairs)
            and (self.name is None or edge.name == self.name)
        )


def _pair_set(pairs: list[list[str]]) -> frozenset[tuple[str, str]]:
    return frozenset((p[0], p[1]) for p in pairs)


class ExtensionEdge(BaseModel):
    """A query extension's join as merged: the edge oriented from its declaring model, and the token reaching it."""

    model_config = ConfigDict(frozen=True)

    edge: OrientedJoin
    token: str


class _Candidate(BaseModel):
    """An edge between the declaring model and the target, read from the declaring model."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    declaring: str
    index: int
    join: ModelJoin
    pairs: frozenset[tuple[str, str]]
    cardinality: JoinCardinality | None


def _cardinality_label(cardinality: JoinCardinality | None) -> str:
    return cardinality.value if cardinality is not None else "unset"


class EdgeMerge:
    """Merges query extension joins into a model universe: a join repeating an edge, in either orientation, is it."""

    def __init__(
        self, *, models: Mapping[str, SlayerModel], reserved_names: Iterable[str] = (),
        stored_edge_names: Iterable[str] | None = None,
    ) -> None:
        self.models: dict[str, SlayerModel] = dict(models)
        self._reserved = frozenset(reserved_names)
        self._edge_names = frozenset(
            stored_edge_names if stored_edge_names is not None
            else (j.name for m in models.values() for j in m.joins if j.name and not j.query_local)
        )
        self._local_names: set[str] = set()

    def add(self, *, declaring: str, join: ModelJoin) -> ExtensionEdge:
        """Merge ``join``, declared by an extension of ``declaring``; raises ``QueryExtensionError`` on a conflict."""
        source = self.models[declaring]
        target = self.models.get(join.target_model)
        label = f"extension {_describe(join)} of {source.spelling!r}"
        if join.target_model == declaring:
            raise QueryExtensionError(f"The {label} joins the model to itself.")
        candidates = self._candidates(source=source, target_name=join.target_model)
        pairs = _pair_set(join.join_pairs)
        named = [c for c in candidates if join.name is not None and c.join.name == join.name]
        if named:
            match = named[0]
            if match.pairs != pairs:
                raise QueryExtensionError(
                    f"The {label} is named like the edge {join.name!r}, which joins on "
                    f"{sorted(match.pairs)}; use another name or the edge's join_pairs."
                )
            return self._reuse(match, join=join, extended=declaring, label=label, alias=None)
        matches = [c for c in candidates if c.pairs == pairs]
        if len(matches) > 1:
            names = ", ".join(repr(c.join.name or c.join.target_model) for c in matches)
            raise QueryExtensionError(
                f"The {label} matches {len(matches)} edges ({names}); name the edge it means."
            )
        target_spelling = target.spelling if target is not None else join.target_model
        declared = join.name if join.name != target_spelling else None
        if matches:
            return self._reuse(matches[0], join=join, extended=declaring, label=label, alias=declared)
        return self._add_new(source=source, target=target, join=join, name=declared, label=label)

    def _candidates(self, *, source: SlayerModel, target_name: str) -> list[_Candidate]:
        out = [
            _Candidate(declaring=source.name, index=i, join=j, pairs=_pair_set(j.join_pairs), cardinality=j.cardinality)
            for i, j in enumerate(source.joins) if j.target_model == target_name
        ]
        target = self.models.get(target_name)
        for i, j in enumerate(target.joins if target is not None else []):
            if j.target_model == source.name:
                out.append(_Candidate(
                    declaring=target_name, index=i, join=j, pairs=frozenset((t, s) for s, t in _pair_set(j.join_pairs)),
                    cardinality=invert_cardinality(j.cardinality),
                ))
        return out

    def _reuse(
        self, match: _Candidate, *, join: ModelJoin, extended: str, label: str, alias: str | None,
    ) -> ExtensionEdge:
        if None not in (join.cardinality, match.cardinality) and join.cardinality != match.cardinality:
            raise QueryExtensionError(
                f"The {label} declares cardinality {_cardinality_label(join.cardinality)}, but that edge is "
                f"{_cardinality_label(match.cardinality)} in the same direction."
            )
        stored = match.join
        if stored.query_local:
            stored = self._name_local(match, name=alias, label=label)
            alias = None
        elif alias is not None and alias in self._edge_names:
            raise QueryExtensionError(f"The {label} names another stored edge {alias!r}.")
        if alias is not None and alias not in stored.aliases:
            stored = self._replace(match, stored.with_aliases((*stored.aliases, alias)))
        return self._edge(declaring=match.declaring, join=stored, from_model=extended, token=alias)

    def _name_local(self, match: _Candidate, *, name: str | None, label: str) -> ModelJoin:
        """A query-local edge declared again: one name, or none, is one edge."""
        local = match.join
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
        self, *, source: SlayerModel, target: SlayerModel | None, join: ModelJoin, name: str | None, label: str,
    ) -> ExtensionEdge:
        if name is not None:
            self._check_new_name(name, label=label)
        new = join.model_copy(update={"name": name, "join_type": JoinType.LEFT}).as_query_local()
        if not source.awaits_columns:
            _check_join_keys(model_name=source.name, columns=source.columns, joins=[new])
        self.models[source.name] = source.model_copy(update={"joins": [*source.joins, new]})
        edge = orient_join(join=new, declaring=self.models[source.name], other=target, from_model=source.name)
        return ExtensionEdge(edge=edge, token=canonical_token(edge))

    def _check_new_name(self, name: str, *, label: str) -> None:
        if name in self._edge_names or name in self._local_names:
            raise QueryExtensionError(f"The {label} is named {name!r}, which names another edge.")
        if name in self._reserved:
            raise QueryExtensionError(f"The {label} is named {name!r}, which names a model or stage.")
        self._local_names.add(name)

    def _replace(self, match: _Candidate, join: ModelJoin) -> ModelJoin:
        model = self.models[match.declaring]
        joins = list(model.joins)
        joins[match.index] = join
        self.models[match.declaring] = model.model_copy(update={"joins": joins})
        return join

    def _edge(self, *, declaring: str, join: ModelJoin, from_model: str, token: str | None) -> ExtensionEdge:
        """``join`` (stored on ``declaring``) oriented from the extended model ``from_model``."""
        edge = orient_join(
            join=join, declaring=self.models[declaring], other=self.models.get(join.target_model), from_model=from_model,
        )
        return ExtensionEdge(edge=edge, token=token or canonical_token(edge))


def _joins_to(model: SlayerModel, target: str) -> list[ModelJoin]:
    return [j for j in model.joins if j.target_model == target]


def edge_reference(*, model: SlayerModel, join: ModelJoin) -> str:
    """The target when ``join`` is ``model``'s only join to it, else the edge name."""
    if len(_joins_to(model, join.target_model)) == 1 or not join.name:
        return join.target_model
    return join.name


def is_addressable(*, model: SlayerModel, join: ModelJoin) -> bool:
    """Whether ``join``'s edge reference names it alone on ``model``."""
    return bool(join.name) or len(_joins_to(model, join.target_model)) == 1


def is_valid_edge_name(name: str) -> bool:
    """Whether ``name`` passes the model-name identifier rules edge names follow."""
    try:
        _validate_model_name(name, "Join")
    except ValueError:
        return False
    return bool(name)


def _describe(join: ModelJoin) -> str:
    label = f" '{join.name}'" if join.name else ""
    return f"join{label} to '{join.target_model}' on {join.join_pairs}"


def resolve_join_ref(*, model: SlayerModel, ref: str | JoinEdgeRef) -> ModelJoin:
    """The one join of ``model`` that ``ref`` addresses; raises ``ValueError`` on none or several."""
    if isinstance(ref, JoinEdgeRef):
        hits = [j for j in model.joins if ref.matches(j)]
        label = f"{ref.target_model!r} on {ref.join_pairs}" + (f" named {ref.name!r}" if ref.name else "")
    else:
        hits = [j for j in model.joins if j.name == ref] or _joins_to(model, ref)
        label = repr(ref)
    if not hits:
        raise ValueError(f"Join {label} not found on model '{model.name}'.")
    if len(hits) > 1:
        candidates = "; ".join(_describe(j) for j in hits)
        raise ValueError(
            f"Join {label} on model '{model.name}' matches {len(hits)} joins ({candidates}); "
            f"address one by its edge name or by its exact target and join_pairs."
        )
    return hits[0]


def resolve_join_refs(*, model: SlayerModel, refs: Iterable[str | JoinEdgeRef]) -> list[ModelJoin]:
    """The distinct joins ``refs`` address; raises ``ValueError`` on a missing or ambiguous one."""
    out: list[ModelJoin] = []
    for ref in refs:
        join = resolve_join_ref(model=model, ref=ref)
        if not any(join is j for j in out):
            out.append(join)
    return out


def without_joins(*, model: SlayerModel, joins: list[ModelJoin]) -> list[ModelJoin]:
    """``model``'s joins minus ``joins`` (by identity)."""
    return [j for j in model.joins if not any(j is r for r in joins)]


def relationship_signature(*, model_name: str, join: ModelJoin) -> RelationshipSignature:
    """Name- and orientation-insensitive identity of the relationship ``join`` declares."""
    forward = _pair_set(join.join_pairs)
    backward = frozenset((t, s) for s, t in forward)
    if model_name < join.target_model:
        return (model_name, join.target_model, forward)
    if model_name > join.target_model:
        return (join.target_model, model_name, backward)
    return (model_name, model_name, min(forward, backward, key=sorted))
