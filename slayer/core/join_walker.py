"""The shared bidirectional join walker.

Every declared join is a symmetric edge between its two models. Traversing it
against the declared direction *orients* it — join pairs swap and the
cardinality label inverts — so which side stores the declaration is storage
trivia. This module is the one traversal substrate: binding, reroot, safety,
spine building, route enumeration, and BFS closures all read edges through it.

Two faces:

* total enumeration (:func:`edges_between`, :func:`neighbors`) — never errors;
  used by route counting, closures, and rendering;
* strict resolution (:func:`resolve_hop`, :func:`walk`) — a path token matches
  an incident edge's name, else the opposite-endpoint model name; ``None`` on a
  miss so callers keep their own errors, and raises
  :class:`AmbiguousJoinPathError` when two or more edges span the hop.
"""

from __future__ import annotations

from typing import Sequence

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import JoinCardinality, JoinType, invert_cardinality
from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.models import ModelJoin, SlayerModel

__all__ = [
    "OrientedJoin",
    "edges_between",
    "neighbors",
    "resolve_hop",
    "walk",
]


class OrientedJoin(BaseModel):
    """A declared join viewed from a chosen traversal source.

    ``source_model``/``target_model`` are the traversal direction; ``join_pairs``
    and ``cardinality`` are oriented to match it. ``declaring_model`` is the model
    that stores the underlying :class:`ModelJoin` (storage trivia, kept for
    diagnostics). Frozen so a resolved hop cannot be mutated in place.
    """

    model_config = ConfigDict(frozen=True)

    source_model: str
    target_model: str
    join_pairs: list[list[str]]
    join_type: JoinType
    cardinality: JoinCardinality | None
    name: str | None
    declaring_model: str


def _orient(*, join: ModelJoin, declaring: str, from_model: str) -> OrientedJoin:
    """Orient ``join`` (declared on ``declaring``) so its source is ``from_model``."""
    if from_model == declaring:
        to_model = join.target_model
        pairs = [list(p) for p in join.join_pairs]
        cardinality = join.cardinality
    else:
        to_model = declaring
        pairs = [[p[1], p[0]] for p in join.join_pairs]
        cardinality = invert_cardinality(join.cardinality)
    return OrientedJoin(
        source_model=from_model,
        target_model=to_model,
        join_pairs=pairs,
        join_type=join.join_type,
        cardinality=cardinality,
        name=join.name,
        declaring_model=declaring,
    )


def edges_between(*, source: SlayerModel, target: SlayerModel) -> list[OrientedJoin]:
    """Every edge connecting ``source`` and ``target``, oriented source→target.

    Inspects both models' declarations, so the answer is independent of which
    side stores the join. Never raises — parallel edges surface as a length-≥2
    list for the caller to reject.
    """
    out: list[OrientedJoin] = []
    for j in source.joins:
        if j.target_model == target.name:
            out.append(_orient(join=j, declaring=source.name, from_model=source.name))
    for j in target.joins:
        if j.target_model == source.name:
            out.append(_orient(join=j, declaring=target.name, from_model=source.name))
    return out


def neighbors(
    *, model: SlayerModel, models_by_name: dict[str, SlayerModel]
) -> list[OrientedJoin]:
    """Every hop incident to ``model``, oriented from ``model``.

    Outgoing declarations first, then edges declared on other models that reach
    ``model`` (inverted). Never raises.
    """
    out: list[OrientedJoin] = []
    for j in model.joins:
        out.append(_orient(join=j, declaring=model.name, from_model=model.name))
    for other in models_by_name.values():
        if other.name == model.name:
            continue
        for j in other.joins:
            if j.target_model == model.name:
                out.append(_orient(join=j, declaring=other.name, from_model=model.name))
    return out


def resolve_hop(
    *,
    current: SlayerModel,
    token: str,
    models_by_name: dict[str, SlayerModel],
) -> OrientedJoin | None:
    """Resolve one path ``token`` from ``current``.

    A token matches an incident edge's ``name`` first, else the opposite-endpoint
    model name. Returns ``None`` when nothing matches (the caller raises its own
    reference error) and raises :class:`AmbiguousJoinPathError` when two or more
    edges qualify.
    """
    incident = neighbors(model=current, models_by_name=models_by_name)
    candidates = [e for e in incident if e.name == token]
    if not candidates:
        candidates = [e for e in incident if e.target_model == token]
    if not candidates:
        return None
    if len(candidates) > 1:
        raise AmbiguousJoinPathError(
            source_model=current.name,
            target_model=candidates[0].target_model,
            candidates=candidates,
            token=token,
        )
    return candidates[0]


def walk(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> list[OrientedJoin] | None:
    """Resolve a full ``path`` of tokens from ``root`` into an oriented chain.

    Returns the chain, ``None`` on an unresolvable or revisiting hop, and
    propagates :class:`AmbiguousJoinPathError` from any ambiguous hop.
    """
    current = root
    visited = {root.name}
    chain: list[OrientedJoin] = []
    for token in path:
        edge = resolve_hop(current=current, token=token, models_by_name=models_by_name)
        if edge is None or edge.target_model in visited:
            return None
        nxt = models_by_name.get(edge.target_model)
        if nxt is None:
            return None
        visited.add(edge.target_model)
        chain.append(edge)
        current = nxt
    return chain
