"""Pure in-memory join-graph routing primitive (DEV-1626, DEV-1853).

``JoinGraph`` builds an undirected multigraph from a set of models' declared
joins and answers reachability / route-counting / shortest-path questions.
Every declared join is a symmetric edge traversable in either direction
(DEV-1853), so which side declares it is irrelevant here; parallel edges
between the same pair of models are distinct routes.

Emitted paths are *executable*: a hop is a bare model-name token when exactly
one edge connects the pair, an edge-name token when parallel edges need
disambiguation, and no token at all (the pair is unroutable) when parallel
edges are unnamed — matching what the query-time walker will accept.

The module is dependency-light (only ``slayer.core.models`` for typing) and
free of storage / async, so it is trivially unit-testable and reusable —
``SlayerQueryEngine._expand_join_graph`` delegates its reachability here.
"""

from __future__ import annotations

from collections import deque

from slayer.core.models import SlayerModel


class JoinGraph:
    """Undirected multigraph over model names built from declared joins."""

    def __init__(
        self, *, nodes: set[str], edges: list[tuple[str, str, str | None]]
    ) -> None:
        self._nodes: set[str] = set(nodes)
        # Each edge is ``(model_a, model_b, name)`` — undirected; parallel
        # edges between the same pair are kept distinct.
        self._edges = list(edges)
        self._adj: dict[str, list[int]] = {n: [] for n in self._nodes}
        for idx, (a, b, _name) in enumerate(self._edges):
            self._adj.setdefault(a, []).append(idx)
            self._adj.setdefault(b, []).append(idx)

    @classmethod
    def build_from_models(cls, models: list[SlayerModel]) -> "JoinGraph":
        """Build from a single datasource's models. Node keys are model names
        (unique within a datasource); each declared join whose target is also
        in the model set becomes one undirected edge (edges to unknown targets
        are skipped).
        """
        names = {m.name for m in models}
        edges: list[tuple[str, str, str | None]] = []
        for m in models:
            for j in m.joins:
                if j.target_model in names:
                    edges.append((m.name, j.target_model, j.name))
        return cls(nodes=names, edges=edges)

    def _incident(self, node: str):
        """Yield ``(neighbor, edge_index, name)`` for every edge on ``node``."""
        for idx in self._adj.get(node, ()):  # noqa: SIM118 — .get default
            a, b, name = self._edges[idx]
            yield (b if node == a else a), idx, name

    def reachable_from(self, root: str) -> set[str]:
        """Set of nodes reachable from ``root`` (including ``root``) over the
        bidirectional edge set. Visited-guarded for cyclic graphs."""
        seen: set[str] = {root}
        frontier: deque[str] = deque([root])
        while frontier:
            node = frontier.popleft()
            for nbr, _idx, _name in self._incident(node):
                if nbr not in seen:
                    seen.add(nbr)
                    frontier.append(nbr)
        return seen

    def count_simple_paths(self, root: str, target: str, *, cap: int = 2) -> int:
        """Number of distinct simple (acyclic) routes ``root → target``, capped
        at ``cap`` with early-stop.

        ``0`` = unreachable, ``1`` = unique route, ``>= cap`` = ambiguous.
        Parallel edges between the same pair are distinct routes. Counts ALL
        simple paths, not just shortest ones: a 2-hop plus a 3-hop route to the
        same target is genuinely ambiguous. The DFS is confined to nodes still
        connected to ``target`` and iterates edges in sorted order; the visited
        node set keeps it finite on cyclic graphs. ``root == target`` returns
        ``1`` (trivial empty route)."""
        if root == target:
            return 1
        relevant = self.reachable_from(target)
        if root not in relevant:
            return 0

        count = 0
        visited: set[str] = {root}

        def dfs(node: str) -> None:
            nonlocal count
            for nbr, idx, _name in sorted(
                self._incident(node), key=lambda t: (t[0], t[1])
            ):
                if count >= cap:
                    return
                if nbr == target:
                    count += 1
                    continue
                if nbr in visited or nbr not in relevant:
                    continue
                visited.add(nbr)
                dfs(nbr)
                visited.discard(nbr)

        dfs(root)
        return min(count, cap)

    def _executable_tokens(self, node: str) -> list[tuple[str, str]]:
        """``(neighbor, token)`` for every executable hop off ``node``.

        Mirrors the walker's ``resolve_hop`` semantics — a token matches an
        incident edge's name first, then a neighbour model name, and must be
        globally unambiguous among ``node``'s incident edges. A pair joined by
        exactly one edge yields the bare neighbor model name (unless an
        incident edge name shadows it); parallel edges yield one entry per
        uniquely-named edge. Anything else is not executable."""
        incident = list(self._incident(node))
        name_counts: dict[str, int] = {}
        for _nbr, _idx, name in incident:
            if name is not None:
                name_counts[name] = name_counts.get(name, 0) + 1
        by_nbr: dict[str, list[str | None]] = {}
        for nbr, _idx, name in incident:
            by_nbr.setdefault(nbr, []).append(name)
        out: list[tuple[str, str]] = []
        for nbr, names in by_nbr.items():
            # Bare model-name token: only for a unique edge, and only when no
            # incident edge name shadows it (name resolves first).
            if len(names) == 1 and nbr not in name_counts:
                out.append((nbr, nbr))
            else:
                out.extend(
                    (nbr, nm) for nm in names
                    if nm is not None and name_counts[nm] == 1
                )
        out.sort()
        return out

    def shortest_path(self, root: str, target: str) -> list[str] | None:
        """Executable hop-token sequence ``root → target`` (excluding ``root``),
        or ``None`` when no unambiguous executable route exists.

        ``[]`` when ``root == target``. Among all minimal-distance paths, the
        lexicographically-smallest token sequence is returned so diamond graphs
        resolve deterministically. A hop across unnamed parallel edges is not
        executable and is skipped, so an item reachable only across such a pair
        reports unreachable rather than emitting a path that would fail."""
        if root == target:
            return []
        dist: dict[str, int] = {root: 0}
        frontier: deque[str] = deque([root])
        while frontier:
            node = frontier.popleft()
            for nbr, _token in self._executable_tokens(node):
                if nbr not in dist:
                    dist[nbr] = dist[node] + 1
                    frontier.append(nbr)
        if target not in dist:
            return None

        best: dict[str, list[str]] = {root: []}
        nodes_by_dist: dict[int, list[str]] = {}
        for node, d in dist.items():
            nodes_by_dist.setdefault(d, []).append(node)
        for d in range(1, dist[target] + 1):
            for v in nodes_by_dist.get(d, []):
                cands = [
                    best[u] + [token]
                    for u in nodes_by_dist.get(d - 1, [])
                    for nbr, token in self._executable_tokens(u)
                    if nbr == v
                ]
                best[v] = min(cands)
        return best[target]


def min_hops_root(
    graph: "JoinGraph", candidates: list[str], mentioned: set[str]
) -> str | None:
    """Pick the root that reaches every ``mentioned`` model over ``graph``.

    Shared selection core (DEV-1626 / DEV-1643): among ``candidates`` that reach
    all mentioned models, minimize total hops summed over the mentioned set,
    prefer a mentioned candidate on ties, then the lexicographically smallest
    name. Returns ``None`` when no candidate reaches every mentioned model. An
    empty ``mentioned`` set makes every candidate trivially valid (0 hops), so
    the lexicographically smallest candidate is returned.
    """
    def total_hops(root: str) -> int:
        return sum(len(graph.shortest_path(root, m) or []) for m in mentioned)

    def reaches_all(root: str) -> bool:
        return all(graph.shortest_path(root, m) is not None for m in mentioned)

    valid = [c for c in candidates if reaches_all(c)]
    if not valid:
        return None
    return min(valid, key=lambda n: (total_hops(n), 0 if n in mentioned else 1, n))
