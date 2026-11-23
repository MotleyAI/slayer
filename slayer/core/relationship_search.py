"""Relationship-graph search over the walker's edges: reachability, shortest routes, route enumeration.

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

from slayer.core.relationship_walker import OrientedRelationship, addresses_alone, canonical_token, neighbors, provably_to_one
from slayer.core.models import SlayerDataset
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
    "unreachable_target",
]


class HopFilter(str, Enum):
    """Which hops a search may take: every edge, or only provably to-one ones."""

    ALL = "all"
    TO_ONE = "to_one"


class Route(BaseModel):
    """An oriented chain from a root, its canonical tokens, and how many hops fan out."""

    model_config = ConfigDict(frozen=True)

    edges: tuple[OrientedRelationship, ...] = ()
    tokens: tuple[str, ...] = ()
    fanning: int = 0

    def rank(self) -> tuple[int, int, tuple[str, ...]]:
        return len(self.edges), self.fanning, self.tokens


class UnaddressablePair(BaseModel):
    """A dataset pair with edges no token addresses (an unnamed edge in a parallel set)."""

    model_config = ConfigDict(frozen=True)

    source_dataset: str
    to: str
    edges: tuple[OrientedRelationship, ...]

    def describe(self) -> str:
        pairs = "; ".join(", ".join(f"{a}={b}" for a, b in e.column_pairs) for e in self.edges)
        return (f"{self.source_dataset}↔{self.to} is joined by edges no path token addresses ({pairs}); "
                f"name the edges to make them addressable")


class _Hop(BaseModel):
    model_config = ConfigDict(frozen=True)

    edge: OrientedRelationship
    token: str
    to_one: bool
    addressable: bool


class _Graph:
    """Per-search adjacency memo: one ``neighbors`` call per dataset."""

    def __init__(self, datasets_by_name: dict[str, SlayerDataset]) -> None:
        self.datasets = datasets_by_name
        self._memo: dict[str, list[_Hop]] = {}

    def incident(self, name: str) -> list[_Hop]:
        hops: list[_Hop] | None = self._memo.get(name)
        if hops is None:
            dataset = self.datasets[name]
            edges = neighbors(dataset=dataset, datasets_by_name=self.datasets)
            hops = []
            for e in edges:
                target = self.datasets.get(e.to)
                if target is None:
                    continue
                hops.append(_Hop(edge=e, token=canonical_token(e), to_one=provably_to_one(edge=e, target_dataset=target),
                                 addressable=addresses_alone(edge=e, incident=edges)))
            hops.sort(key=lambda h: (h.token, h.edge.to))
            self._memo[name] = hops
        return hops

    def hops(self, name: str, hop_filter: HopFilter) -> list[_Hop]:
        return [h for h in self.incident(name)
                if h.addressable and (hop_filter is HopFilter.ALL or h.to_one)]

    def passes(self, name: str, *, root: str) -> bool:
        """A route may continue through ``name``: the root, or anything but the spine."""
        return name == root or not is_spine(self.datasets[name])

    def relevant(self, *, root: str | None, target: str) -> set[str]:
        """Datasets a simple route ``root → target`` can visit (reverse BFS, never through the root or spine);
        with no ``root``, every dataset with a route to ``target``."""
        seen = {target}
        frontier = deque([target])
        while frontier:
            node = frontier.popleft()
            if node == root or (node != target and is_spine(self.datasets[node])):
                continue
            for h in self.incident(node):
                if h.edge.to not in seen:
                    seen.add(h.edge.to)
                    frontier.append(h.edge.to)
        return seen


def _extend(route: Route, hop: _Hop) -> Route:
    return Route(edges=(*route.edges, hop.edge), tokens=(*route.tokens, hop.token),
                 fanning=route.fanning + (0 if hop.to_one else 1))


def addressable_hops(
    *, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], hop_filter: HopFilter = HopFilter.ALL,
) -> list[OrientedRelationship]:
    """The addressable hops off ``dataset``, ordered by canonical token."""
    graph = _Graph({**datasets_by_name, dataset.name: dataset})
    return [h.edge for h in graph.hops(dataset.name, hop_filter)]


def unaddressable_pairs(*, datasets_by_name: dict[str, SlayerDataset]) -> list[UnaddressablePair]:
    """Every dataset pair with an unaddressable edge, one entry per unordered pair."""
    graph = _Graph(datasets_by_name)
    out: dict[frozenset[str], UnaddressablePair] = {}
    for name in sorted(datasets_by_name):
        by_target: dict[str, list[OrientedRelationship]] = {}
        for h in graph.incident(name):
            if not h.addressable:
                by_target.setdefault(h.edge.to, []).append(h.edge)
        for target, edges in sorted(by_target.items()):
            out.setdefault(frozenset((name, target)), UnaddressablePair(
                source_dataset=name, to=target, edges=tuple(edges)))
    return list(out.values())


def blocking_pairs(
    *, root: SlayerDataset, target: str, datasets_by_name: dict[str, SlayerDataset],
) -> list[UnaddressablePair]:
    """The unaddressable pairs between what ``root`` routes to and ``target``."""
    datasets = {**datasets_by_name, root.name: root}
    reached = set(shortest_routes(root=root, datasets_by_name=datasets))
    if target in reached or target not in datasets:
        return []
    reaching = _Graph(datasets).relevant(root=None, target=target)
    return [
        p for p in unaddressable_pairs(datasets_by_name=datasets)
        if any(a in reached and b in reaching
               for a, b in ((p.source_dataset, p.to), (p.to, p.source_dataset)))
    ]


def distances(
    *, root: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], hop_filter: HopFilter = HopFilter.ALL,
) -> dict[str, int]:
    """BFS hop distance from ``root`` to every dataset it reaches over any edge (addressable or not)."""
    graph = _Graph({**datasets_by_name, root.name: root})
    dist = {root.name: 0}
    frontier = deque([root.name])
    while frontier:
        node = frontier.popleft()
        if not graph.passes(node, root=root.name):
            continue
        for hop in graph.incident(node):
            nxt = hop.edge.to
            if nxt not in dist and (hop_filter is HopFilter.ALL or hop.to_one):
                dist[nxt] = dist[node] + 1
                frontier.append(nxt)
    return dist


def reachable(
    *, root: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], hop_filter: HopFilter = HopFilter.ALL,
) -> set[str]:
    """Every dataset ``root`` reaches over any edge, ``root`` included."""
    return set(distances(root=root, datasets_by_name=datasets_by_name, hop_filter=hop_filter))


def shortest_routes(
    *, root: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], hop_filter: HopFilter = HopFilter.ALL,
) -> dict[str, Route]:
    """The tie-break-best shortest addressable route from ``root`` to every dataset it routes to."""
    graph = _Graph({**datasets_by_name, root.name: root})
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
    admit_edge: Callable[[OrientedRelationship], bool] | None = None,
) -> Iterator[tuple[str, Route]]:
    """``route`` extended by each filtered addressable hop off ``node`` (that ``admit_edge`` admits) into a dataset
    ``admit`` accepts."""
    if not graph.passes(node, root=root):
        return
    for hop in graph.hops(node, hop_filter):
        if admit(hop.edge.to) and (admit_edge is None or admit_edge(hop.edge)):
            yield hop.edge.to, _extend(route, hop)


def shortest_route(
    *, root: SlayerDataset, target: str, datasets_by_name: dict[str, SlayerDataset],
    hop_filter: HopFilter = HopFilter.ALL,
) -> Route | None:
    """The tie-break-best shortest route ``root → target``; ``None`` when unreachable."""
    return shortest_routes(root=root, datasets_by_name=datasets_by_name, hop_filter=hop_filter).get(target)


def routes(
    *, root: SlayerDataset, target: str, datasets_by_name: dict[str, SlayerDataset],
    hop_filter: HopFilter = HopFilter.ALL, cap: int = 2, shortest_only: bool = False,
    admit_edge: Callable[[OrientedRelationship], bool] | None = None,
) -> list[Route]:
    """Up to ``cap`` simple routes ``root → target`` in tie-break order (best-first), over edges ``admit_edge`` admits."""
    graph = _Graph({**datasets_by_name, root.name: root})
    if target not in graph.datasets:
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
        visited = {root.name, *(e.to for e in route.edges)}
        for nxt, ext in _successors(graph, node=node, route=route, root=root.name, hop_filter=hop_filter,
                                    admit=lambda n: n not in visited and n in relevant, admit_edge=admit_edge):
            heapq.heappush(heap, (ext.rank(), next(tie), ext, nxt))
    return out


def count_routes(
    *, root: SlayerDataset, target: str, datasets_by_name: dict[str, SlayerDataset], cap: int = 2,
) -> int:
    """Simple routes ``root → target`` over every edge (parallel edges are distinct), capped at ``cap``."""
    graph = _Graph({**datasets_by_name, root.name: root})
    if target not in graph.datasets:
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
            nxt = hop.edge.to
            if nxt == target:
                count += 1
            elif nxt not in visited and nxt in relevant and graph.passes(nxt, root=root.name):
                visited.add(nxt)
                dfs(nxt)
                visited.discard(nxt)

    dfs(root.name)
    return min(count, cap)


def paths_from(
    *, root: SlayerDataset, datasets_by_name: dict[str, SlayerDataset], depth: int,
    hop_filter: HopFilter = HopFilter.ALL, passable: Callable[[SlayerDataset], bool] | None = None,
) -> list[Route]:
    """Every simple route of 1..``depth`` hops from ``root`` into datasets ``passable`` admits, by token order."""
    graph = _Graph({**datasets_by_name, root.name: root})
    out: list[Route] = []

    def dfs(node: str, route: Route, visited: set[str]) -> None:
        if len(route.edges) >= depth or not graph.passes(node, root=root.name):
            return
        for hop in graph.hops(node, hop_filter):
            nxt = hop.edge.to
            if nxt in visited or (passable is not None and not passable(graph.datasets[nxt])):
                continue
            ext = _extend(route, hop)
            out.append(ext)
            dfs(nxt, ext, visited | {nxt})

    dfs(root.name, Route(), {root.name})
    return out


def unreachable_target(*, root: SlayerDataset, token: str, datasets_by_name: dict[str, SlayerDataset]) -> str | None:
    """The dataset ``token`` names when neither an incident edge nor a route reaches it from ``root``."""
    target = datasets_by_name.get(token)
    if target is None or token == root.name or is_spine(target):
        return None
    universe = {**datasets_by_name, root.name: root.with_unrelated(frozenset())}
    if any(e.name == token or e.to == token for e in neighbors(dataset=root, datasets_by_name=universe)):
        return None
    return token if count_routes(root=root, target=token, datasets_by_name=universe) == 0 else None
