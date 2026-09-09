"""DEV-1856 — pure route resolver for short-form dotted-dimension auto-routing.

A short-form dotted ref names only ``Target.column``; this module resolves the
join route from the query root to ``Target`` over the datasource's bidirectional
multigraph (DEV-1853). Trichotomy from :meth:`JoinGraph.count_simple_paths`:
unreachable, unique route, or ambiguous (>=2 routes). Ambiguity resolves iff
exactly one route is fan-out-free — every hop provably many-to-one on its
traversal orientation — judged on a *directed* safe-hop set (:func:`_safe_hops`),
since one orientation of an edge may be to-one while the other fans out.

Routed and suggested paths are executable token sequences (edge-name token where
parallel edges need disambiguation, bare model name otherwise; a hop across an
unnamed parallel pair is inexecutable and never routed or suggested), mirroring
:meth:`JoinGraph.shortest_path` / ``join_walker.resolve_hop``.
"""

from __future__ import annotations

from collections import deque
from typing import Optional, Tuple

from slayer.core.errors import UnresolvableDimensionJoinError
from slayer.core.join_walker import neighbors
from slayer.core.models import SlayerModel
from slayer.engine.join_graph import JoinGraph
from slayer.engine.join_safety import provably_to_one

__all__ = ["short_form_route_or_none", "route_dotted_target"]


def _safe_hops_for_neighbor(
    *, nbr: str, edges: list, target: SlayerModel, name_counts: dict[str, int]
) -> list[Tuple[str, str]]:
    """Executable to-one hop tokens from a model to one neighbour: the bare
    neighbour name for a lone unshadowed edge, else one entry per uniquely-named
    parallel edge; each kept only if provably many-to-one on its orientation."""
    if len(edges) == 1 and nbr not in name_counts:
        return (
            [(nbr, nbr)]
            if provably_to_one(edge=edges[0], target_model=target)
            else []
        )
    return [
        (e.name, nbr)
        for e in edges
        if e.name is not None
        and name_counts[e.name] == 1
        and provably_to_one(edge=e, target_model=target)
    ]


def _safe_hops(
    *, model: SlayerModel, models_by_name: dict[str, SlayerModel]
) -> list[Tuple[str, str]]:
    """Oriented provably-to-one executable hops off ``model`` as ``(token,
    target_model)``. Token resolution mirrors ``resolve_hop`` /
    ``JoinGraph._executable_tokens``: a single unshadowed edge yields the bare
    neighbour name, parallel edges yield one entry per uniquely-named edge, and
    an unnamed parallel hop is dropped. Each candidate is then kept only if it is
    provably many-to-one on its traversal orientation."""
    incident = neighbors(model=model, models_by_name=models_by_name)
    name_counts: dict[str, int] = {}
    for e in incident:
        if e.name is not None:
            name_counts[e.name] = name_counts.get(e.name, 0) + 1
    by_nbr: dict[str, list] = {}
    for e in incident:
        by_nbr.setdefault(e.target_model, []).append(e)
    out: list[Tuple[str, str]] = []
    for nbr, edges in by_nbr.items():
        target = models_by_name.get(nbr)
        if target is None:
            continue
        out.extend(
            _safe_hops_for_neighbor(
                nbr=nbr, edges=edges, target=target, name_counts=name_counts
            )
        )
    return out


def _safe_routes(
    *,
    root: SlayerModel,
    target_model: str,
    models_by_name: dict[str, SlayerModel],
    cap: int = 2,
) -> list[list[str]]:
    """Up to ``cap`` distinct simple fan-out-free token routes ``root ->
    target_model`` over the directed safe-hop set."""
    routes: list[list[str]] = []

    def dfs(*, current: str, tokens: list[str], visited: set[str]) -> None:
        if len(routes) >= cap:
            return
        model = models_by_name.get(current)
        if model is None:
            return
        for token, nbr in _safe_hops(model=model, models_by_name=models_by_name):
            if len(routes) >= cap:
                return
            if nbr == target_model:
                routes.append([*tokens, token])
                continue
            if nbr in visited:
                continue
            dfs(current=nbr, tokens=[*tokens, token], visited=visited | {nbr})

    dfs(current=root.name, tokens=[], visited={root.name})
    return routes


def _safe_bfs_dist(
    *, root: SlayerModel, models_by_name: dict[str, SlayerModel]
) -> dict[str, int]:
    """BFS layer distances from ``root`` over the directed safe-hop set."""
    dist: dict[str, int] = {root.name: 0}
    frontier: deque[str] = deque([root.name])
    while frontier:
        node = frontier.popleft()
        model = models_by_name.get(node)
        if model is None:
            continue
        for _, nbr in _safe_hops(model=model, models_by_name=models_by_name):
            if nbr not in dist:
                dist[nbr] = dist[node] + 1
                frontier.append(nbr)
    return dist


def _shortest_safe_route(
    *, root: SlayerModel, target_model: str, models_by_name: dict[str, SlayerModel]
) -> Optional[list[str]]:
    """Lexicographically-smallest shortest fan-out-free token route, or ``None``."""
    dist = _safe_bfs_dist(root=root, models_by_name=models_by_name)
    if target_model not in dist:
        return None
    best: dict[str, list[str]] = {root.name: []}
    nodes_by_dist: dict[int, list[str]] = {}
    for node, d in dist.items():
        nodes_by_dist.setdefault(d, []).append(node)
    for d in range(1, dist[target_model] + 1):
        for v in nodes_by_dist.get(d, []):
            cands = [
                [*best[u], token]
                for u in nodes_by_dist.get(d - 1, [])
                if u in best
                for token, nbr in _safe_hops(
                    model=models_by_name[u], models_by_name=models_by_name
                )
                if nbr == v
            ]
            if cands:
                best[v] = min(cands)
    return best.get(target_model)


def _resolve_route(
    *, root: SlayerModel, target_model: str, models_by_name: dict[str, SlayerModel]
) -> Tuple[Optional[list[str]], str]:
    """``(route, status)`` where status is ``"ok"`` / ``"ambiguous"`` /
    ``"unreachable"``. A unique full-graph route resolves (even if it fans out);
    among two or more routes, the sole fan-out-free one resolves."""
    graph = JoinGraph.build_from_models(list(models_by_name.values()))
    n = graph.count_simple_paths(root=root.name, target=target_model, cap=2)
    if n == 0:
        return None, "unreachable"
    if n == 1:
        route = graph.shortest_path(root=root.name, target=target_model)
        return (route, "ok") if route is not None else (None, "ambiguous")
    safe = _safe_routes(
        root=root, target_model=target_model, models_by_name=models_by_name, cap=2
    )
    if len(safe) == 1:
        return safe[0], "ok"
    return None, "ambiguous"


def short_form_route_or_none(
    *, root: SlayerModel, target_model: str, models_by_name: dict[str, SlayerModel]
) -> Optional[list[str]]:
    """The executable hop-token route (excl. ``root``, incl. ``target_model``)
    for a uniquely-routable target, else ``None``. Never raises."""
    route, status = _resolve_route(
        root=root, target_model=target_model, models_by_name=models_by_name
    )
    return route if status == "ok" else None


def route_dotted_target(
    *,
    root: SlayerModel,
    target_model: str,
    leaf: str,
    models_by_name: dict[str, SlayerModel],
) -> list[str]:
    """Like :func:`short_form_route_or_none`, but raise
    :class:`UnresolvableDimensionJoinError` (route-aware ``suggested_path``
    including ``leaf``) on an ambiguous or unreachable target."""
    route, status = _resolve_route(
        root=root, target_model=target_model, models_by_name=models_by_name
    )
    if status == "ok":
        assert route is not None
        return route
    suggested: Optional[str] = None
    if status == "ambiguous":
        base = _shortest_safe_route(
            root=root, target_model=target_model, models_by_name=models_by_name
        )
        if base is None:
            graph = JoinGraph.build_from_models(list(models_by_name.values()))
            base = graph.shortest_path(root=root.name, target=target_model)
        if base is not None:
            suggested = ".".join([*base, leaf])
    raise UnresolvableDimensionJoinError(
        reference=f"{target_model}.{leaf}",
        root_model=root.name,
        reason=(
            "ambiguous route" if status == "ambiguous" else "no route to target"
        ),
        suggested_path=suggested,
    )
