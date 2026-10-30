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
  :class:`CircularJoinPathError` when a hop revisits a model already on the path;
  :func:`follow` walks an already-resolved composed key path, where a revisit is
  a separate occurrence (revisits are frame-local).
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Callable, Dict, Iterable, Iterator, Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, JoinCardinality, JoinType, invert_cardinality
from slayer.core.errors import AmbiguousJoinPathError, CircularJoinPathError
from slayer.core.keys import ColumnElementFn, ColumnKey, ColumnSqlKey, ColumnTypeFn, ValueKey, source_anchor_path
from slayer.core.models import Column, ModelJoin, SlayerModel, is_key_set_unique, join_key_error
from slayer.core.time_spine import PRODUCT_JOIN_TYPE, TIME_SPINE_MODEL, axis_column, is_spine, spine_joins

__all__ = [
    "OrientedJoin",
    "OrientedLike",
    "addressable_token",
    "addresses_alone",
    "aggregation_owner",
    "canonical_path",
    "canonical_token",
    "edges_between",
    "follow",
    "incident_model_names",
    "is_product_edge",
    "is_lateral_edge",
    "is_unrelated_edge",
    "list_child",
    "list_child_name",
    "with_list_children",
    "model_column_element",
    "join_key_columns",
    "matching_edges",
    "neighbors",
    "observe_traversals",
    "physical_join_pairs",
    "provably_to_one",
    "resolve_hop",
    "resolved_terminal",
    "reverse_token",
    "terminal_model",
    "cancelling_revisit",
    "unique_key_sets",
    "WalkMiss",
    "walk",
    "walk_cancelling",
    "walk_miss",
    "walk_terminal",
]


#: A product hop's kind: the population to its spine, the spine to a dataset with no axis route,
#: or the population to a dataset no join path reaches (minted only by strict resolution).
ProductKind = Literal["spine", "detached", "unrelated"]


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
    # A query stage's user spelling of an endpoint (``None`` for an ordinary model).
    source_spelling: str | None = None
    target_spelling: str | None = None
    # A keyless hop pairing every source row with every target row; ``None`` for a declared join.
    product: ProductKind | None = None
    # A list column's hop between its model and its element child (the list column's name); ``None`` otherwise.
    lateral: str | None = None


_traversals: ContextVar[Optional[Dict[str, bool]]] = ContextVar(
    "slayer_join_traversals", default=None,
)


@contextmanager
def observe_traversals() -> Iterator[Dict[str, bool]]:
    """Record every model a traversal in the block yields: name → strictly resolved
    (a hop walked) rather than only enumerated. Output-only; never feeds resolution."""
    sink: Dict[str, bool] = {}
    token = _traversals.set(sink)
    try:
        yield sink
    finally:
        _traversals.reset(token)


def _observe(targets: Iterable[str], *, strict: bool) -> None:
    sink = _traversals.get()
    if sink is None:
        return
    for name in targets:
        sink[name] = sink.get(name, False) or strict


def canonical_token(edge: OrientedJoin) -> str:
    """A hop's canonical spelling: the edge name, else the target's spelling."""
    return edge.name or edge.target_spelling or edge.target_model


def reverse_token(edge: OrientedJoin) -> str:
    """The spelling of ``edge`` walked backwards: the edge name, else the source's spelling."""
    return edge.name or edge.source_spelling or edge.source_model


def matching_edges(edges: list[OrientedJoin], token: str) -> list[OrientedJoin]:
    """The edges ``token`` names, by precedence: edge name, then a query stage's
    spelling (it shadows a same-named model), then a model name — never a
    stage's minted identity."""
    for match in (
        lambda e: e.name == token,
        lambda e: e.target_spelling == token,
        lambda e: e.target_spelling is None and e.target_model == token,
    ):
        hits = [e for e in edges if match(e)]
        if hits:
            return hits
    return []


def canonical_path(chain: Sequence[OrientedJoin]) -> tuple[str, ...]:
    """The canonical spelling of a resolved chain."""
    return tuple(canonical_token(e) for e in chain)


def _orient(
    *, join: ModelJoin, declaring: str, from_model: str,
    spellings: Dict[str, Optional[str]],
) -> OrientedJoin:
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
        source_spelling=spellings.get(from_model),
        target_spelling=spellings.get(to_model),
    )


def _product_edge(model: SlayerModel) -> OrientedJoin:
    """A population factor's hop to the spine: the product, each factor row fanning to every instant."""
    return OrientedJoin(
        source_model=model.name, target_model=TIME_SPINE_MODEL, join_pairs=[],
        join_type=PRODUCT_JOIN_TYPE, cardinality=JoinCardinality.ONE_TO_MANY,
        name=TIME_SPINE_MODEL, declaring_model=model.name,
        source_spelling=model.explicit_spelling, product="spine",
    )


#: An oriented hop, or a declared join read in its declared orientation — both
#: expose ``cardinality`` and target-side ``join_pairs``, all the proof reads.
OrientedLike = OrientedJoin | ModelJoin


def unique_key_sets(model: SlayerModel) -> list[list[str]]:
    """``model``'s unique key-sets by column name: its (composite) PK, then each solo-unique column."""
    pk = [c.name for c in model.columns if c.primary_key]
    return ([pk] if pk else []) + [[c.name] for c in model.columns if c.unique]


def _to_one(*, cardinality: JoinCardinality | None, target_keys: list[str], target: SlayerModel) -> bool:
    return cardinality in (JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE) or is_key_set_unique(
        key_columns=target_keys, unique_key_sets=unique_key_sets(target),
    )


def provably_to_one(*, edge: OrientedLike, target_model: SlayerModel) -> bool:
    """Is ``edge`` provably many-to-one onto ``target_model`` in its orientation?
    True iff the oriented cardinality is m:1/1:1, or the traversal-target columns
    fully cover a unique key-set of ``target_model``."""
    return _to_one(cardinality=edge.cardinality, target_keys=[p[1] for p in edge.join_pairs], target=target_model)


def _detached_names(models_by_name: dict[str, SlayerModel]) -> frozenset[str]:
    """Datasets with no provably to-one chain to an axis: the spine meets them only as a product."""
    datasets = {n: m for n, m in models_by_name.items() if not is_spine(m)}
    reached = {n for n, m in datasets.items() if axis_column(m) is not None}
    to_one_into: dict[str, set[str]] = {}
    for m in datasets.values():
        for j in m.joins:
            t = datasets.get(j.target_model)
            if t is None:
                continue
            if _to_one(cardinality=j.cardinality, target_keys=[p[1] for p in j.join_pairs], target=t):
                to_one_into.setdefault(t.name, set()).add(m.name)
            if _to_one(cardinality=invert_cardinality(j.cardinality), target_keys=[p[0] for p in j.join_pairs],
                       target=m):
                to_one_into.setdefault(m.name, set()).add(t.name)
    frontier = list(reached)
    while frontier:
        fresh = to_one_into.get(frontier.pop(), set()) - reached
        reached |= fresh
        frontier.extend(fresh)
    return frozenset(datasets.keys() - reached)


def _detached_edge(*, source: SlayerModel, target: SlayerModel) -> OrientedJoin:
    """The spine and a dataset without any route to it, meeting only as a product."""
    return OrientedJoin(
        source_model=source.name, target_model=target.name, join_pairs=[],
        join_type=PRODUCT_JOIN_TYPE, cardinality=JoinCardinality.MANY_TO_MANY,
        name=None, declaring_model=TIME_SPINE_MODEL,
        source_spelling=source.explicit_spelling, target_spelling=target.explicit_spelling,
        product="detached",
    )


def _unrelated_edge(*, source: SlayerModel, target: SlayerModel) -> OrientedJoin:
    """The population and a dataset no join path reaches, meeting only as a product."""
    return OrientedJoin(
        source_model=source.name, target_model=target.name, join_pairs=[],
        join_type=PRODUCT_JOIN_TYPE, cardinality=JoinCardinality.MANY_TO_MANY,
        name=None, declaring_model=source.name,
        source_spelling=source.explicit_spelling, target_spelling=target.explicit_spelling,
        product="unrelated",
    )


#: The element column of a list child.
LIST_ELEMENT_COLUMN = "value"


#: A list child's identity prefix (reserved: no stored model carries it).
LIST_CHILD_PREFIX = "__slayer_list__"


def list_child_name(*, parent: str, column: str) -> str:
    """A list child's minted model identity; the length prefix keeps ``__`` in names unambiguous."""
    return f"{LIST_CHILD_PREFIX}{len(parent)}_{parent}__{column}"


def list_child(*, parent: SlayerModel, column: Column) -> SlayerModel:
    """The element child of ``parent``'s list ``column``: one row per non-NULL element, its value ``value``.
    Cached on ``parent`` under everything the child is built from, so a copied or edited parent never reuses a
    stale one."""
    key = (parent.name, parent.data_source, column.name, column.element_type)
    cached = parent._list_children.get(key)
    if cached is not None:
        return cached
    name = list_child_name(parent=parent.name, column=column.name)
    # Built valid, then renamed: the name validator refuses the reserved prefix. Never a relation of its own.
    child = SlayerModel(
        name="list_child", sql_table="list_child", data_source=parent.data_source or "list_child",
        columns=[Column(name=LIST_ELEMENT_COLUMN, type=column.element_type or DataType.UNKNOWN)],
    ).model_copy(update={"name": name, "sql_table": name, "data_source": parent.data_source})
    child._list_of = (parent.name, column.name)
    return parent._list_children.setdefault(key, child)


def with_list_children(models_by_name: Dict[str, SlayerModel]) -> Dict[str, SlayerModel]:
    """``models_by_name`` plus the element child of every list column."""
    out = dict(models_by_name)
    for model in models_by_name.values():
        for column in model.list_columns():
            name = list_child_name(parent=model.name, column=column.name)
            if name not in out:
                out[name] = list_child(parent=model, column=column)
    return out


def _lateral_edge(*, parent: SlayerModel, column: str, forward: bool) -> OrientedJoin:
    """The hop between ``parent`` and its ``column`` child: one-to-many forward, the child's own row back."""
    child = list_child_name(parent=parent.name, column=column)
    return OrientedJoin(
        source_model=parent.name if forward else child, target_model=child if forward else parent.name,
        join_pairs=[], join_type=JoinType.INNER,
        cardinality=JoinCardinality.ONE_TO_MANY if forward else JoinCardinality.MANY_TO_ONE,
        name=column, declaring_model=parent.name,
        source_spelling=parent.explicit_spelling if forward else None,
        target_spelling=None if forward else parent.explicit_spelling,
        lateral=column,
    )


def _list_edges(*, model: SlayerModel, models_by_name: dict[str, SlayerModel]) -> list[OrientedJoin]:
    """``model``'s hops to its list children, and a list child's hop back to its parent."""
    out = [_lateral_edge(parent=model, column=c.name, forward=True) for c in model.list_columns()]
    if model.list_of is not None:
        parent = models_by_name.get(model.list_of[0])
        if parent is not None:
            out.append(_lateral_edge(parent=parent, column=model.list_of[1], forward=False))
    return out


def is_lateral_edge(edge: OrientedJoin) -> bool:
    return edge.lateral is not None


def _hop_target(*, edge: OrientedJoin, current: SlayerModel, models_by_name: dict[str, SlayerModel]) -> SlayerModel | None:
    """The model ``edge`` reaches from ``current``; a list child is minted when the map lacks it."""
    target = models_by_name.get(edge.target_model)
    if target is None and edge.lateral is not None and edge.cardinality is JoinCardinality.ONE_TO_MANY:
        column = current.get_column(edge.lateral)
        if column is not None:
            return list_child(parent=current, column=column)
    return target


def is_product_edge(edge: OrientedJoin) -> bool:
    """A keyless product hop (never a declared or axis edge)."""
    return edge.product is not None


def is_unrelated_edge(edge: OrientedJoin) -> bool:
    return edge.product == "unrelated"


def _declared_toward(model: SlayerModel, target: str, *, spine: bool) -> list[ModelJoin]:
    joins = spine_joins(model, models_by_name={TIME_SPINE_MODEL: model}) if spine else model.joins
    return [j for j in joins if j.target_model == target]


def edges_between(*, source: SlayerModel, target: SlayerModel) -> list[OrientedJoin]:
    """Every edge connecting ``source`` and ``target``, oriented source→target.

    Inspects both models' declarations (and the spine's virtual edges), so the answer
    is independent of which side stores the join. Never raises — parallel edges
    surface as a length-≥2 list for the caller to reject.
    """
    out: list[OrientedJoin] = []
    spellings = {m.name: m.explicit_spelling for m in (source, target)}
    if source.population_spine and is_spine(target):
        out.append(_product_edge(source))
    for j in _declared_toward(source, target.name, spine=is_spine(target)):
        out.append(_orient(join=j, declaring=source.name, from_model=source.name,
                           spellings=spellings))
    for j in _declared_toward(target, source.name, spine=is_spine(source)):
        out.append(_orient(join=j, declaring=target.name, from_model=source.name,
                           spellings=spellings))
    out.extend(e for e in _list_edges(model=source, models_by_name={target.name: target})
               if e.target_model == target.name)
    _observe((e.target_model for e in out), strict=False)
    return out


def join_key_columns(
    *, edge: OrientedJoin, source: SlayerModel, target: SlayerModel,
) -> list[tuple[Column, Column]]:
    """``edge``'s key pairs as their declared columns; raises ``JoinKeyError`` on a non-base key."""
    declared_target = (edge.target_model if edge.declaring_model == edge.source_model
                       else edge.source_model)

    def column(key: str, model: SlayerModel) -> Column:
        err = join_key_error(model=edge.declaring_model, target=declared_target, key=key,
                             side=model.name, columns=model.columns)
        if err is not None:
            raise err
        col = model.get_column(key)
        assert col is not None
        return col

    return [(column(src, source), column(tgt, target)) for src, tgt in edge.join_pairs]


def physical_join_pairs(
    *, edge: OrientedJoin, source: SlayerModel, target: SlayerModel,
) -> list[tuple[str, str]]:
    """``edge``'s key pairs in physical spelling; raises ``JoinKeyError`` on a non-base key."""
    return [(s.physical_name, t.physical_name) for s, t in join_key_columns(edge=edge, source=source, target=target)]


def _virtual_spine_edges(*, model: SlayerModel, models_by_name: dict[str, SlayerModel]) -> list[OrientedJoin]:
    """``model``'s product and detached-dataset hops (none without the spine in the universe)."""
    spine = models_by_name.get(TIME_SPINE_MODEL)
    if spine is None:
        return []
    if model.population_spine:
        return [_product_edge(model)]
    if is_spine(model):
        return [_detached_edge(source=model, target=models_by_name[n]) for n in sorted(_detached_names(models_by_name))]
    # An axis is a to-one chain of its own: only an axis-less dataset can be detached.
    if axis_column(model) is None and model.name in _detached_names(models_by_name):
        return [_detached_edge(source=model, target=spine)]
    return []


def neighbors(
    *, model: SlayerModel, models_by_name: dict[str, SlayerModel]
) -> list[OrientedJoin]:
    """Every hop incident to ``model``, oriented from ``model``.

    Outgoing declarations first, then edges declared on other models that reach
    ``model`` (inverted). Never raises.
    """
    spellings = {n: m.explicit_spelling for n, m in models_by_name.items()}
    spellings[model.name] = model.explicit_spelling
    out = _virtual_spine_edges(model=model, models_by_name=models_by_name)
    out.extend(_list_edges(model=model, models_by_name=models_by_name))
    for j in spine_joins(model, models_by_name=models_by_name):
        out.append(_orient(join=j, declaring=model.name, from_model=model.name,
                           spellings=spellings))
    for other in models_by_name.values():
        if other.name == model.name:
            continue
        for j in spine_joins(other, models_by_name=models_by_name):
            if j.target_model == model.name:
                out.append(_orient(join=j, declaring=other.name, from_model=model.name,
                                   spellings=spellings))
    _observe((e.target_model for e in out), strict=False)
    return out


def incident_model_names(*, model: SlayerModel, models_by_name: dict[str, SlayerModel]) -> list[str]:
    """Names of every model sharing a declared edge with ``model``, loaded or not."""
    return sorted({e.target_model for e in neighbors(model=model, models_by_name=models_by_name) if e.lateral is None})


def resolve_hop(
    *,
    current: SlayerModel,
    token: str,
    models_by_name: dict[str, SlayerModel],
) -> OrientedJoin | None:
    """Resolve one path ``token`` from ``current``.

    A token matches an incident edge's ``name`` first, then a query stage's
    spelling, else the opposite-endpoint model name. Returns ``None`` when nothing matches (the caller raises its own
    reference error) and raises :class:`AmbiguousJoinPathError` when two or more
    edges qualify.
    """
    candidates = matching_edges(neighbors(model=current, models_by_name=models_by_name), token)
    if not candidates:
        candidates = _unrelated_hops(current=current, token=token, models_by_name=models_by_name)
    if not candidates:
        return None
    if len(candidates) > 1:
        raise AmbiguousJoinPathError(
            source_model=current.name,
            target_model=candidates[0].target_model,
            candidates=candidates,
            token=token,
        )
    _observe([candidates[0].target_model], strict=True)
    return candidates[0]


def _unrelated_hops(
    *, current: SlayerModel, token: str, models_by_name: dict[str, SlayerModel],
) -> list[OrientedJoin]:
    """The product hop from a query frame's population to the unrelated dataset ``token`` names."""
    targets = [models_by_name[n] for n in sorted(current.unrelated_datasets) if n in models_by_name]
    return matching_edges([_unrelated_edge(source=current, target=t) for t in targets], token)


def addresses_alone(*, edge: OrientedJoin, incident: list[OrientedJoin]) -> bool:
    """Whether ``edge``'s canonical token names it alone among ``incident`` (its source's edges)."""
    return matching_edges(incident, canonical_token(edge)) == [edge]


def addressable_token(
    *, current: SlayerModel, edge: OrientedJoin, models_by_name: dict[str, SlayerModel],
) -> str | None:
    """``edge``'s hop token from ``current`` (name first), or ``None`` when the hop is unaddressable."""
    incident = neighbors(model=current, models_by_name=models_by_name)
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
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
    revisits: bool,
) -> tuple[list[OrientedJoin] | None, WalkMiss | None]:
    current = root
    visited = {root.name}
    chain: list[OrientedJoin] = []
    for i, token in enumerate(path):
        if chain and is_spine(current):
            return None, WalkMiss(index=i, token=token, kind="spine")  # a path may end at the spine, never cross it
        edge = resolve_hop(current=current, token=token, models_by_name=models_by_name)
        if edge is None:
            return None, WalkMiss(index=i, token=token, kind="no_edge")
        if not revisits and edge.target_model in visited:
            raise CircularJoinPathError(
                reference=".".join(path),
                root_model=root.name,
                revisited=edge.target_model,
                hop=token,
                via=current.name,
            )
        nxt = _hop_target(edge=edge, current=current, models_by_name=models_by_name)
        if nxt is None:
            return None, WalkMiss(index=i, token=token, kind="unloaded", target=edge.target_model)
        visited.add(edge.target_model)
        chain.append(edge)
        current = nxt
    return chain, None


def walk(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> list[OrientedJoin] | None:
    """Resolve a full ``path`` of tokens spelled in one frame from ``root`` into an oriented chain.

    Returns the chain, ``None`` only on an unknown/unloaded hop, propagates
    :class:`AmbiguousJoinPathError` from any ambiguous hop, and raises
    :class:`CircularJoinPathError` when a hop revisits a model already on the path
    (a definite malformation, not a miss).
    """
    return _walk(root=root, path=path, models_by_name=models_by_name, revisits=False)[0]


def walk_miss(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> WalkMiss | None:
    """Why :func:`walk` returns ``None`` for ``path`` (``None`` when it does not); raises as :func:`walk` does."""
    return _walk(root=root, path=path, models_by_name=models_by_name, revisits=False)[1]


def walk_terminal(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> tuple[SlayerModel, tuple[str, ...]] | None:
    """``(terminal model, canonical path)`` of :func:`walk`, ``None`` where it misses; raises as it does."""
    models = with_list_children({**models_by_name, root.name: models_by_name.get(root.name, root)})
    chain = walk(root=root, path=path, models_by_name=models)
    if chain is None:
        return None
    return (models[chain[-1].target_model] if chain else root), canonical_path(chain)


def follow(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> list[OrientedJoin] | None:
    """:func:`walk` for an already-resolved, possibly composed key path: revisits are separate occurrences."""
    return _walk(root=root, path=path, models_by_name=models_by_name, revisits=True)[0]


def _owner_stack(
    *, root: SlayerModel, owner_path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> list[SlayerModel] | None:
    """The occurrence sequence of the (trusted, already-resolved) ``owner_path`` from ``root``;
    ``None`` when it does not resolve."""
    try:
        chain = follow(root=root, path=owner_path, models_by_name=models_by_name)
    except AmbiguousJoinPathError:
        return None
    if chain is None:
        return None
    models = with_list_children(models_by_name)
    return [root, *(models[e.target_model] for e in chain)]


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
    _observe([named[0].target_model], strict=True)
    return named[0]


def walk_cancelling(
    *,
    root: SlayerModel,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> tuple[str, ...] | None:
    """Resolve a definition default's qualifier ``tokens`` in the owner's frame
    (the owner is ``root`` followed along ``owner_path``), with reverse-hop
    cancellation.

    Per token, precedence is incident edge name → a model name on the path
    (cancel) → a model-name hop: an incident edge-name token always hops and
    never cancels; a token equal to a dataset already on ``root + owner_path``
    truncates the absolute path back to its innermost occurrence — keeping ``owner_path``'s spelling —
    then resolution continues forward from there. Returns the absolute canonical
    path from ``root`` (``()`` = the root itself), ``None`` on a miss or a hop
    onto an already-visited model (see :func:`cancelling_revisit`); ambiguity raises."""
    path, _revisit = _cancelling_walk(
        root=root, owner_path=owner_path, tokens=tokens, models_by_name=models_by_name,
    )
    return path


def cancelling_revisit(
    *,
    root: SlayerModel,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> tuple[str, str, str] | None:
    """``(hop, revisited, via)`` when :func:`walk_cancelling` misses because a hop
    lands on a model already on the path, else ``None``."""
    _path, revisit = _cancelling_walk(
        root=root, owner_path=owner_path, tokens=tokens, models_by_name=models_by_name,
    )
    return revisit


def _cancelling_walk(
    *,
    root: SlayerModel,
    owner_path: Sequence[str],
    tokens: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> tuple[tuple[str, ...] | None, tuple[str, str, str] | None]:
    models = dict(models_by_name)
    models.setdefault(root.name, root)
    models = with_list_children(models)
    stack = _owner_stack(root=root, owner_path=owner_path, models_by_name=models)
    if stack is None:
        return None, None
    path_tokens: list[str] = list(owner_path)
    for token in tokens:
        edge = _incident_named(current=stack[-1], token=token, models_by_name=models)
        if edge is None:
            cancel_at = next((i for i in reversed(range(len(stack))) if stack[i].spelling == token), None)
            if cancel_at is not None:
                del stack[cancel_at + 1:]
                del path_tokens[cancel_at:]
                continue
            edge = resolve_hop(current=stack[-1], token=token, models_by_name=models)
        if edge is None:
            return None, None
        if any(m.name == edge.target_model for m in stack):
            return None, (token, edge.target_model, stack[-1].name)
        nxt = models.get(edge.target_model)
        if nxt is None:
            return None, None
        stack.append(nxt)
        path_tokens.append(canonical_token(edge))
    return tuple(path_tokens), None


def _terminal(
    *, root: SlayerModel, path: Sequence[str], models_by_name: dict[str, SlayerModel], revisits: bool,
) -> SlayerModel | None:
    models = dict(models_by_name)
    models.setdefault(root.name, root)
    models = with_list_children(models)
    try:
        chain, _miss = _walk(root=root, path=path, models_by_name=models, revisits=revisits)
    except (AmbiguousJoinPathError, CircularJoinPathError):
        return None
    if chain is None:
        return None
    return models.get(chain[-1].target_model) if chain else root


def terminal_model(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> SlayerModel | None:
    """Best-effort terminal model of ``path`` from ``root``: ``None`` on an
    unresolvable, revisiting, or ambiguous hop — for callers whose contract is
    to skip rather than raise (the strict doors fail closed on their own)."""
    return _terminal(root=root, path=path, models_by_name=models_by_name, revisits=False)


def resolved_terminal(
    *,
    root: SlayerModel,
    path: Sequence[str],
    models_by_name: dict[str, SlayerModel],
) -> SlayerModel | None:
    """:func:`terminal_model` of an already-resolved, possibly composed key path (revisits allowed)."""
    return _terminal(root=root, path=path, models_by_name=models_by_name, revisits=True)


def model_column_lookup(
    *, model: SlayerModel, models_by_name: dict[str, SlayerModel],
) -> Callable[[object], Optional[Column]]:
    """The declared ``Column`` of a column leaf rooted at ``model``; ``None`` for anything unresolvable."""
    def lookup(key: object) -> Optional[Column]:
        if isinstance(key, ColumnSqlKey):
            owner = model if key.model == model.name else models_by_name.get(key.model)
            leaf = key.column_name
        elif isinstance(key, ColumnKey):
            owner = resolved_terminal(root=model, path=key.path, models_by_name=models_by_name)
            leaf = key.leaf
        else:
            return None
        return owner.get_column(leaf) if owner is not None else None
    return lookup


def model_column_type(*, model: SlayerModel, models_by_name: dict[str, SlayerModel]) -> ColumnTypeFn:
    """Declared types of column leaves rooted at ``model``; ``None`` for anything unresolvable."""
    lookup = model_column_lookup(model=model, models_by_name=models_by_name)

    def column_type(key: ValueKey) -> Optional[DataType]:
        col = lookup(key)
        return col.type if col is not None else None
    return column_type


def model_column_element(*, model: SlayerModel, models_by_name: dict[str, SlayerModel]) -> ColumnElementFn:
    """Element types of list-typed column leaves rooted at ``model``; ``None`` for anything else."""
    lookup = model_column_lookup(model=model, models_by_name=models_by_name)

    def column_element(key: ValueKey) -> Optional[DataType]:
        col = lookup(key)
        return col.element_type if col is not None else None
    return column_element


def aggregation_owner(
    *, root: SlayerModel | None, source: ValueKey, models_by_name: dict[str, SlayerModel],
) -> SlayerModel | None:
    """The model declaring ``source``'s aggregation: ``root`` walked along the source
    anchor; ``None`` with no root or an unresolvable hop."""
    if root is None:
        return None
    return resolved_terminal(
        root=root, path=source_anchor_path(source), models_by_name=models_by_name,
    )
