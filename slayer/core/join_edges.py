"""Join-edge identity: the declaration reference and the FK relationship signature."""

from __future__ import annotations

from collections.abc import Iterable

from pydantic import BaseModel

from slayer.core.join_walker import OrientedJoin
from slayer.core.models import ModelJoin, SlayerModel, _validate_model_name

__all__ = [
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
