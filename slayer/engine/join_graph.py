"""Pure in-memory join-graph routing primitive (DEV-1626).

``JoinGraph`` builds a directed adjacency from a set of models' *stored
outgoing* joins and answers reachability / shortest-path questions. It is
join-type-agnostic: it simply reads each model's outgoing ``joins``.

INNER joins are kept symmetric by the storage layer
(``slayer/storage/join_sync.py`` materialises a reverse ``B→A`` edge for
every ``A→B`` INNER join) — the same invariant the query engine's own
``_walk_join_chain`` relies on. So a symmetric INNER pair appears here as
two directed edges and is therefore traversable in both directions, and
every path this primitive emits is walkable by the engine at query time.

The module is dependency-light (only ``slayer.core.models`` for typing)
and free of storage / async, so it is trivially unit-testable and reusable
— ``SlayerQueryEngine._expand_join_graph`` delegates its directed
reachability here.
"""

from __future__ import annotations

from collections import deque

from slayer.core.models import SlayerModel


class JoinGraph:
    """Directed graph over model names built from stored outgoing joins."""

    def __init__(self, adjacency: dict[str, set[str]]) -> None:
        # Every referenced node (source or target present in the model set)
        # is a key; targets not in the node set are dropped at build time.
        self._adj: dict[str, set[str]] = adjacency

    @classmethod
    def build_from_models(cls, models: list[SlayerModel]) -> "JoinGraph":
        """Build from a single datasource's models. Node keys are model
        names (unique within a datasource); edges point ``source →
        join.target_model`` for every stored join whose target is also in
        the given model set (edges to unknown targets are skipped).
        """
        names = {m.name for m in models}
        adj: dict[str, set[str]] = {name: set() for name in names}
        for m in models:
            for j in m.joins:
                if j.target_model in names:
                    adj[m.name].add(j.target_model)
        return cls(adj)

    def reachable_from(self, root: str) -> set[str]:
        """Set of nodes reachable from ``root`` (including ``root``) by
        following directed edges. Visited-guarded for cyclic graphs."""
        seen: set[str] = {root}
        frontier: deque[str] = deque([root])
        while frontier:
            node = frontier.popleft()
            for nbr in self._adj.get(node, ()):  # noqa: SIM118 — .get default
                if nbr not in seen:
                    seen.add(nbr)
                    frontier.append(nbr)
        return seen

    def shortest_path(self, root: str, target: str) -> list[str] | None:
        """Return the hop-name sequence from ``root`` to ``target``
        (excluding ``root``), or ``None`` if unreachable.

        ``[]`` when ``root == target``. Among all minimal-distance paths,
        the lexicographically-smallest hop-name sequence is returned so
        diamond graphs resolve deterministically. Distances are computed
        first (BFS); the lexicographically-smallest path at each node's
        minimal distance is then propagated layer by layer.
        """
        if root == target:
            return []
        if target not in self._adj and root not in self._adj:
            return None

        # BFS layer assignment: dist[node] = min hop count from root.
        dist: dict[str, int] = {root: 0}
        frontier: deque[str] = deque([root])
        while frontier:
            node = frontier.popleft()
            for nbr in sorted(self._adj.get(node, ())):
                if nbr not in dist:
                    dist[nbr] = dist[node] + 1
                    frontier.append(nbr)
        if target not in dist:
            return None

        # Propagate the lexicographically-smallest hop sequence per node,
        # in order of increasing distance. best[v] = min over predecessors
        # u at dist-1 of (best[u] + [v]); since [v] is fixed for v, this
        # reduces to min(best[u]) + [v].
        best: dict[str, list[str]] = {root: []}
        nodes_by_dist: dict[int, list[str]] = {}
        for node, d in dist.items():
            nodes_by_dist.setdefault(d, []).append(node)
        for d in range(1, dist[target] + 1):
            for v in nodes_by_dist.get(d, []):
                preds = [
                    u for u in dist
                    if dist[u] == d - 1 and v in self._adj.get(u, ())
                ]
                best[v] = min(best[u] for u in preds) + [v]
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
