"""Pure route resolver for short-form dotted-dimension auto-routing.

A short-form dotted ref names only ``Target.field``; this module resolves the
relationship route from the query root to ``Target`` over the datasource's bidirectional
multigraph (:mod:`slayer.core.relationship_search`). Trichotomy from route counting:
unreachable, unique route, or ambiguous (>=2 routes). Ambiguity resolves iff
exactly one addressable route consists of Ossie steps, else exactly one is fan-out-free —
every hop provably many-to-one on its traversal orientation. Routed and suggested paths are
canonical token sequences.
"""

from __future__ import annotations

from typing import Optional, Tuple

from slayer.core.errors import UnresolvableDimensionRelationshipError
from slayer.core.relationship_search import HopFilter, blocking_pairs, count_routes, routes, shortest_route
from slayer.core.relationship_walker import reverse_token
from slayer.core.models import SlayerDataset
from slayer.core.time_spine import is_spine
from slayer.engine.cardinality.relationship_safety import spine_route_error, spine_routes

__all__ = [
    "resolve_route",
    "safe_route_reachable",
    "short_form_route_or_none",
    "route_dotted_target",
]


def resolve_route(
    *, root: SlayerDataset, target_dataset: str, datasets_by_name: dict[str, SlayerDataset]
) -> Tuple[Optional[list[str]], str]:
    """``(route, status)`` where status is ``"ok"`` / ``"ambiguous"`` /
    ``"unreachable"``. A unique full-graph route resolves (even if it fans out);
    among two or more routes, the sole path of Ossie steps resolves, else the sole fan-out-free one."""
    if is_spine(root):
        dataset = datasets_by_name.get(target_dataset)
        found = spine_routes(dataset=dataset, datasets_by_name=datasets_by_name) if dataset is not None else []
        if not found:
            return None, "unreachable"
        if len(found) > 1:
            return None, "ambiguous"
        return [reverse_token(e) for e in reversed(found[0])], "ok"
    n = count_routes(root=root, target=target_dataset, datasets_by_name=datasets_by_name)
    if n == 0:
        return None, "unreachable"
    if n == 1:
        route = shortest_route(root=root, target=target_dataset, datasets_by_name=datasets_by_name)
        return (list(route.tokens), "ok") if route is not None else (None, "ambiguous")
    ossie = routes(root=root, target=target_dataset, datasets_by_name=datasets_by_name, hop_filter=HopFilter.OSSIE)
    if len(ossie) == 1:
        return list(ossie[0].tokens), "ok"
    safe = routes(root=root, target=target_dataset, datasets_by_name=datasets_by_name, hop_filter=HopFilter.TO_ONE)
    if len(safe) == 1:
        return list(safe[0].tokens), "ok"
    return None, "ambiguous"


def safe_route_reachable(
    *, root: SlayerDataset, target_dataset: str, datasets_by_name: dict[str, SlayerDataset]
) -> bool:
    """Whether at least one fan-out-free executable route reaches ``target_dataset``."""
    return shortest_route(root=root, target=target_dataset, datasets_by_name=datasets_by_name,
                          hop_filter=HopFilter.TO_ONE) is not None


def short_form_route_or_none(
    *, root: SlayerDataset, target_dataset: str, datasets_by_name: dict[str, SlayerDataset]
) -> Optional[list[str]]:
    """The executable hop-token route (excl. ``root``, incl. ``target_dataset``)
    for a uniquely-routable target, else ``None``. Never raises."""
    route, status = resolve_route(
        root=root, target_dataset=target_dataset, datasets_by_name=datasets_by_name
    )
    return route if status == "ok" else None


def route_dotted_target(
    *,
    root: SlayerDataset,
    target_dataset: str,
    leaf: str,
    datasets_by_name: dict[str, SlayerDataset],
) -> list[str]:
    """Like :func:`short_form_route_or_none`, but raise
    :class:`UnresolvableDimensionRelationshipError` (route-aware ``suggested_path``
    including ``leaf``) on an ambiguous or unreachable target."""
    route, status = resolve_route(
        root=root, target_dataset=target_dataset, datasets_by_name=datasets_by_name
    )
    if status == "ok":
        assert route is not None
        return route
    if status == "ambiguous" and is_spine(root):
        raise spine_route_error(
            dataset=target_dataset, leaf=leaf,
            routes=spine_routes(dataset=datasets_by_name[target_dataset], datasets_by_name=datasets_by_name),
        )
    suggested: Optional[str] = None
    if status == "ambiguous":
        base = (shortest_route(root=root, target=target_dataset, datasets_by_name=datasets_by_name,
                               hop_filter=HopFilter.TO_ONE)
                or shortest_route(root=root, target=target_dataset, datasets_by_name=datasets_by_name))
        if base is not None:
            suggested = ".".join([*base.tokens, leaf])
    reason = "ambiguous route" if status == "ambiguous" else "no route to target"
    if suggested is None:
        pairs = blocking_pairs(root=root, target=target_dataset, datasets_by_name=datasets_by_name)
        reason = "; ".join([reason, *(p.describe() for p in pairs)])
    raise UnresolvableDimensionRelationshipError(
        reference=f"{target_dataset}.{leaf}",
        root_dataset=root.spelling,
        reason=reason,
        suggested_path=suggested,
    )
