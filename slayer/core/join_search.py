"""Join-graph search over the walker's edges: reachability, shortest routes, route enumeration.

Every emitting search walks *addressable* hops only — an edge whose canonical token
resolves back to exactly that edge — and spells each hop canonically. No search
crosses the time spine: a route may begin or end there, never pass through it. Ties
among equal-length routes break by fewer fanning hops, then the smallest token sequence.
"""

from __future__ import annotations

import heapq
import itertools
from collections import deque
from collections.abc import Callable, Iterator
from enum import Enum

from pydantic import BaseModel, ConfigDict

from slayer.core.join_walker import OrientedJoin, addresses_alone, canonical_token, neighbors, provably_to_one
from slayer.core.models import SlayerModel
from slayer.core.time_spine import is_spine

__all__ = [
    "HopFilter",
    "Route",
    "UnaddressablePair",
    "addressable_hops",
    "blocking_pairs",
    "count_routes",
    "distances",
    "paths_from",
    "reachable",
    "routes",
    "shortest_route",
    "shortest_routes",
    "unaddressable_pairs",
]


class HopFilter(str, Enum):
    """Which hops a search may take: every edge, or only provably to-one ones."""

    ALL = "all"
    TO_ONE = "to_one"


class Route(BaseModel):
    """An oriented chain from a root, its canonical tokens, and how many hops fan out."""

    model_config = ConfigDict(frozen=True)

    edges: tuple[OrientedJoin, ...] = ()
    tokens: tuple[str, ...] = ()
    fanning: int = 0

    def rank(self) -> tuple[int, int, tuple[str, ...]]:
        return len(self.edges), self.fanning, self.tokens


class UnaddressablePair(BaseModel):
    """A model pair with edges no token addresses (an unnamed edge in a parallel set)."""

    model_config = ConfigDict(frozen=True)

    source_model: str
    target_model: str
    edges: tuple[OrientedJoin, ...]

    def describe(self) -> str:
        return (f"{self.source_model}↔{self.target_model} is joined by edges no path token addresses; "
                f"name the edges to make them addressable")


class _Hop(BaseModel):
    model_config = ConfigDict(frozen=True)

    edge: OrientedJoin
    token: str
    to_one: bool
    addressable: bool


class _Graph:
    """Per-search adjacency memo: one ``neighbors`` call per model."""

    def __init__(self, models_by_name: dict[str, SlayerModel]) -> None:
        self.models = models_by_name
        self._memo: dict[str, list[_Hop]] = {}

    def incident(self, name: str) -> list[_Hop]:
        hops = self._memo.get(name)
        if hops is None:
            model = self.models[name]
            edges = neighbors(model=model, models_by_name=self.models)
            hops = []
            for e in edges:
                target = self.models.get(e.target_model)
                if target is None:
                    continue
                hops.append(_Hop(edge=e, token=canonical_token(e), to_one=provably_to_one(edge=e, target_model=target),
                                 addressable=addresses_alone(edge=e, incident=edges)))
            hops.sort(key=lambda h: (h.token, h.edge.target_model))
            self._memo[name] = hops
        return hops

    def hops(self, name: str, hop_filter: HopFilter) -> list[_Hop]:
        return [h for h in self.incident(name)
                if h.addressable and (hop_filter is HopFilter.ALL or h.to_one)]

    def passes(self, name: str, *, root: str) -> bool:
        """A route may continue through ``name``: the root, or anything but the spine."""
        return name == root or not is_spine(self.models[name])

    def relevant(self, *, root: str | None, target: str) -> set[str]:
        """Models a simple route ``root → target`` can visit (reverse BFS, never through the root or spine);
        with no ``root``, every model with a route to ``target``."""
        seen = {target}
        frontier = deque([target])
        while frontier:
            node = frontier.popleft()
            if node == root or (node != target and is_spine(self.models[node])):
                continue
            for h in self.incident(node):
                if h.edge.target_model not in seen:
                    seen.add(h.edge.target_model)
                    frontier.append(h.edge.target_model)
        return seen


def _extend(route: Route, hop: _Hop) -> Route:
    return Route(edges=(*route.edges, hop.edge), tokens=(*route.tokens, hop.token),
                 fanning=route.fanning + (0 if hop.to_one else 1))


def addressable_hops(
    *, model: SlayerModel, models_by_name: dict[str, SlayerModel], hop_filter: HopFilter = HopFilter.ALL,
) -> list[OrientedJoin]:
    """The addressable hops off ``model``, ordered by canonical token."""
    graph = _Graph({**models_by_name, model.name: model})
    return [h.edge for h in graph.hops(model.name, hop_filter)]


def unaddressable_pairs(*, models_by_name: dict[str, SlayerModel]) -> list[UnaddressablePair]:
    """Every model pair with an unaddressable edge, one entry per unordered pair."""
    graph = _Graph(models_by_name)
    out: dict[frozenset[str], UnaddressablePair] = {}
    for name in sorted(models_by_name):
        by_target: dict[str, list[OrientedJoin]] = {}
        for h in graph.incident(name):
            if not h.addressable:
                by_target.setdefault(h.edge.target_model, []).append(h.edge)
        for target, edges in sorted(by_target.items()):
            out.setdefault(frozenset((name, target)), UnaddressablePair(
                source_model=name, target_model=target, edges=tuple(edges)))
    return list(out.values())


def blocking_pairs(
    *, root: SlayerModel, target: str, models_by_name: dict[str, SlayerModel],
) -> list[UnaddressablePair]:
    """The unaddressable pairs between what ``root`` routes to and ``target``."""
    models = {**models_by_name, root.name: root}
    reached = set(shortest_routes(root=root, models_by_name=models))
    if target in reached or target not in models:
        return []
    reaching = _Graph(models).relevant(root=None, target=target)
    return [
        p for p in unaddressable_pairs(models_by_name=models)
        if any(a in reached and b in reaching
               for a, b in ((p.source_model, p.target_model), (p.target_model, p.source_model)))
    ]


def distances(
    *, root: SlayerModel, models_by_name: dict[str, SlayerModel], hop_filter: HopFilter = HopFilter.ALL,
) -> dict[str, int]:
    """BFS hop distance from ``root`` to every model it reaches over any edge (addressable or not)."""
    graph = _Graph({**models_by_name, root.name: root})
    dist = {root.name: 0}
    frontier = deque([root.name])
    while frontier:
        node = frontier.popleft()
        if not graph.passes(node, root=root.name):
            continue
        for hop in graph.incident(node):
            nxt = hop.edge.target_model
            if nxt not in dist and (hop_filter is HopFilter.ALL or hop.to_one):
                dist[nxt] = dist[node] + 1
                frontier.append(nxt)
    return dist


def reachable(
    *, root: SlayerModel, models_by_name: dict[str, SlayerModel], hop_filter: HopFilter = HopFilter.ALL,
) -> set[str]:
    """Every model ``root`` reaches over any edge, ``root`` included."""
    return set(distances(root=root, models_by_name=models_by_name, hop_filter=hop_filter))


def shortest_routes(
    *, root: SlayerModel, models_by_name: dict[str, SlayerModel], hop_filter: HopFilter = HopFilter.ALL,
) -> dict[str, Route]:
    """The tie-break-best shortest addressable route from ``root`` to every model it routes to."""
    graph = _Graph({**models_by_name, root.name: root})
    best: dict[str, Route] = {root.name: Route()}
    layer = [root.name]
    while layer:
        found: dict[str, Route] = {}
        for node in layer:
            for nxt, cand in _successors(graph, node=node, route=best[node], root=root.name,
                                         hop_filter=hop_filter, admit=lambda n: n not in best):
                if nxt not in found or cand.rank() < found[nxt].rank():
                    found[nxt] = cand
        best.update(found)
        layer = sorted(found)
    return best


def _successors(
    graph: _Graph, *, node: str, route: Route, root: str, hop_filter: HopFilter, admit: Callable[[str], bool],
) -> Iterator[tuple[str, Route]]:
    """``route`` extended by each filtered addressable hop off ``node`` into a model ``admit`` accepts."""
    if not graph.passes(node, root=root):
        return
    for hop in graph.hops(node, hop_filter):
        if admit(hop.edge.target_model):
            yield hop.edge.target_model, _extend(route, hop)


def shortest_route(
    *, root: SlayerModel, target: str, models_by_name: dict[str, SlayerModel],
    hop_filter: HopFilter = HopFilter.ALL,
) -> Route | None:
    """The tie-break-best shortest route ``root → target``; ``None`` when unreachable."""
    return shortest_routes(root=root, models_by_name=models_by_name, hop_filter=hop_filter).get(target)


def routes(
    *, root: SlayerModel, target: str, models_by_name: dict[str, SlayerModel],
    hop_filter: HopFilter = HopFilter.ALL, cap: int = 2, shortest_only: bool = False,
) -> list[Route]:
    """Up to ``cap`` simple routes ``root → target`` in tie-break order (best-first)."""
    graph = _Graph({**models_by_name, root.name: root})
    if target not in graph.models:
        return []
    if root.name == target:
        return [Route()]
    relevant = graph.relevant(root=root.name, target=target)
    out: list[Route] = []
    heap: list[tuple[tuple[int, int, tuple[str, ...]], int, Route, str]] = [((0, 0, ()), 0, Route(), root.name)]
    tie = itertools.count(1)
    while heap and len(out) < cap:
        _rank, _n, route, node = heapq.heappop(heap)
        if node == target:
            if shortest_only and out and len(route.edges) > len(out[0].edges):
                break
            out.append(route)
            continue
        visited = {root.name, *(e.target_model for e in route.edges)}
        for nxt, ext in _successors(graph, node=node, route=route, root=root.name, hop_filter=hop_filter,
                                    admit=lambda n: n not in visited and n in relevant):
            heapq.heappush(heap, (ext.rank(), next(tie), ext, nxt))
    return out


def count_routes(
    *, root: SlayerModel, target: str, models_by_name: dict[str, SlayerModel], cap: int = 2,
) -> int:
    """Simple routes ``root → target`` over every edge (parallel edges are distinct), capped at ``cap``."""
    graph = _Graph({**models_by_name, root.name: root})
    if target not in graph.models:
        return 0
    if root.name == target:
        return 1
    relevant = graph.relevant(root=root.name, target=target)
    if root.name not in relevant:
        return 0
    count = 0
    visited = {root.name}

    def dfs(node: str) -> None:
        nonlocal count
        for hop in graph.incident(node):
            if count >= cap:
                return
            nxt = hop.edge.target_model
            if nxt == target:
                count += 1
            elif nxt not in visited and nxt in relevant and graph.passes(nxt, root=root.name):
                visited.add(nxt)
                dfs(nxt)
                visited.discard(nxt)

    dfs(root.name)
    return min(count, cap)


def paths_from(
    *, root: SlayerModel, models_by_name: dict[str, SlayerModel], depth: int,
    hop_filter: HopFilter = HopFilter.ALL, passable: Callable[[SlayerModel], bool] | None = None,
) -> list[Route]:
    """Every simple route of 1..``depth`` hops from ``root`` into models ``passable`` admits, by token order."""
    graph = _Graph({**models_by_name, root.name: root})
    out: list[Route] = []

    def dfs(node: str, route: Route, visited: set[str]) -> None:
        if len(route.edges) >= depth or not graph.passes(node, root=root.name):
            return
        for hop in graph.hops(node, hop_filter):
            nxt = hop.edge.target_model
            if nxt in visited or (passable is not None and not passable(graph.models[nxt])):
                continue
            ext = _extend(route, hop)
            out.append(ext)
            dfs(nxt, ext, visited | {nxt})

    dfs(root.name, Route(), {root.name})
    return out
