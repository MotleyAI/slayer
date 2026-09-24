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
  an incident edge's name, else the opposite-endpoint model name; ``None`` only on
  an unknown/unloaded hop so callers keep their own errors, raises
  :class:`AmbiguousJoinPathError` when two or more edges span the hop, and
  :class:`CircularJoinPathError` when a hop revisits a model already on the path.
"""

from __future__ import annotations

from typing import Sequence

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import JoinCardinality, JoinType, invert_cardinality
from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError
from slayer.core.models import ModelJoin, SlayerModel, join_key_error

__all__ = [
    "OrientedJoin",
    "canonical_path",
    "canonical_token",
    "edges_between",
    "neighbors",
    "physical_join_pairs",
    "resolve_hop",
    "terminal_model",
    "walk",
    "walk_cancelling",
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


def canonical_token(edge: OrientedJoin) -> str:
    """A hop's canonical spelling: the edge name, else the traversal-target model."""
    return edge.name if edge.name is not None else edge.target_model


def canonical_path(chain: Sequence[OrientedJoin]) -> tuple[str, ...]:
    """The canonical spelling of a resolved chain."""
    return tuple(canonical_token(e) for e in chain)


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


def physical_join_pairs(
    *, edge: OrientedJoin, source: SlayerModel, target: SlayerModel,
) -> list[tuple[str, str]]:
    """``edge``'s key pairs in physical spelling; raises ``JoinKeyError`` on a non-base key."""
    declared_target = (edge.target_model if edge.declaring_model == edge.source_model
                       else edge.source_model)

    def physical(key: str, model: SlayerModel) -> str:
        err = join_key_error(model=edge.declaring_model, target=declared_target, key=key,
                             side=model.name, columns=model.columns)
        if err is not None:
            raise err
        col = model.get_column(key)
        assert col is not None
        return col.physical_name

    return [(physical(src, source), physical(tgt, target)) for src, tgt in edge.join_pairs]


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

    Returns the chain, ``None`` only on an unknown/unloaded hop, propagates
    :class:`AmbiguousJoinPathError` from any ambiguous hop, and raises
    :class:`CircularJoinPathError` when a hop revisits a model already on the path
    (a definite malformation, not a miss).
    """
    current = root
    visited = {root.name}
    chain: list[OrientedJoin] = []
    for token in path:
        edge = resolve_hop(current=current, token=token, models_by_name=models_by_name)
        if edge is None:
            return None
        if edge.target_model in visited:
            raise CircularJoinPathError(
                reference=".".join(path),
                root_model=root.name,
                revisited=edge.target_model,
                hop=token,
                via=current.name,
            )
        nxt = models_by_name.get(edge.target_model)
        if nxt is None:
            return None
        visited.add(edge.target_model)
        chain.append(edge)
        current = nxt
    return chain


def _owner_stack(
    *, root: SlayerModel, owner_path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> list[SlayerModel] | None:
    """The model sequence of the (trusted, already-resolved) ``owner_path`` from
    ``root``. Only the target model of each token is needed, so parallel edges
    that agree on it never raise here (the default's own tokens resolve strictly);
    ``None`` on an unresolvable or revisiting owner hop."""
    stack: list[SlayerModel] = [root]
    for token in owner_path:
        incident = neighbors(model=stack[-1], models_by_name=models_by_name)
        cands = [e for e in incident if e.name == token] or [
            e for e in incident if e.target_model == token]
        target = {e.target_model for e in cands}
        if len(target) != 1:
            return None
        nxt = models_by_name.get(next(iter(target)))
        if nxt is None or any(m.name == nxt.name for m in stack):
            return None
        stack.append(nxt)
    return stack


def _incident_named(
    *, current: SlayerModel, token: str, models_by_name: dict[str, SlayerModel],
) -> OrientedJoin | None:
    """The incident edge of ``current`` whose declared ``name`` is ``token``, or
    ``None``. Raises :class:`AmbiguousJoinPathError` when two share the name."""
    named = [e for e in neighbors(model=current, models_by_name=models_by_name)
             if e.name == token]
    if not named:
        return None
    if len(named) > 1:
        raise AmbiguousJoinPathError(
            source_model=current.name, target_model=named[0].target_model,
            candidates=named, token=token,
        )
    return named[0]


def walk_cancelling(
    *,
    root: SlayerModel,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> tuple[str, ...] | None:
    """Resolve a definition default's qualifier ``tokens`` in the owner's frame
    (the owner is ``root`` walked along ``owner_path``), with reverse-hop
    cancellation.

    Per token, precedence is incident edge name → a model name on the path
    (cancel) → a model-name hop: an incident edge-name token always hops and
    never cancels; a token equal to a dataset already on ``root + owner_path``
    truncates the absolute path back to it — keeping ``owner_path``'s spelling —
    then resolution continues forward from there. Returns the absolute canonical
    path from ``root`` (``()`` = the root itself), ``None`` on a miss or a hop
    onto an already-visited model; ambiguity raises."""
    models = dict(models_by_name)
    models.setdefault(root.name, root)
    stack = _owner_stack(root=root, owner_path=owner_path, models_by_name=models)
    if stack is None:
        return None
    path_tokens: list[str] = list(owner_path)
    for token in tokens:
        edge = _incident_named(current=stack[-1], token=token, models_by_name=models)
        if edge is not None:  # edge name wins over a same-named path model
            nxt = models.get(edge.target_model)
            if nxt is None or any(m.name == edge.target_model for m in stack):
                return None
            stack.append(nxt)
            path_tokens.append(canonical_token(edge))
            continue
        cancel_at = next((i for i, m in enumerate(stack) if m.name == token), None)
        if cancel_at is not None:
            del stack[cancel_at + 1:]
            del path_tokens[cancel_at:]
            continue
        edge = resolve_hop(current=stack[-1], token=token, models_by_name=models)
        if edge is None or edge.target_model in {m.name for m in stack}:
            return None
        nxt = models.get(edge.target_model)
        if nxt is None:
            return None
        stack.append(nxt)
        path_tokens.append(canonical_token(edge))
    return tuple(path_tokens)


def terminal_model(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> SlayerModel | None:
    """Best-effort terminal model of ``path`` from ``root``: ``None`` on an
    unresolvable, revisiting, or ambiguous hop — for callers whose contract is
    to skip rather than raise (the strict doors fail closed on their own)."""
    models = dict(models_by_name)
    models.setdefault(root.name, root)
    try:
        chain = walk(root=root, path=path, models_by_name=models)
    except (AmbiguousJoinPathError, CircularJoinPathError):
        return None
    if chain is None:
        return None
    return models.get(chain[-1].target_model) if chain else root
