"""Stage 4 (DEV-1450) — aggregation registry helpers.

Lifts the agg-name collection BFS from ``enrichment.py`` and the
parameter-resolution helpers from ``sql/generator.py`` so the new binder
modules don't have to reach into those tangles. The helpers are pure:
given a model + a resolve_join_target callback, they produce structured
results without touching storage or spawning side maps.

Public surface:
- ``collect_reachable_agg_names`` — BFS the join graph for custom
  aggregation names.
- ``resolve_aggregation`` — find an ``Aggregation`` definition by name.
- ``is_known_aggregation_name`` — built-in or in the custom set.
- ``required_params_for`` — required built-in params (e.g.,
  ``weighted_avg`` requires ``weight``).
- ``merge_agg_params`` — defaults from the agg-def overridden by
  query-time kwargs.

These are dormant in stage 4 — the existing call sites still inline
their own logic. Stages 7a/7b switch them over.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, FrozenSet, List, Optional, Tuple

from slayer.core.enums import (
    BUILTIN_AGGREGATION_REQUIRED_PARAMS,
    BUILTIN_AGGREGATIONS,
)
from slayer.core.models import Aggregation, SlayerModel


# ---------------------------------------------------------------------------
# Agg-name collection
# ---------------------------------------------------------------------------


ResolveJoinTarget = Callable[..., Awaitable[Optional[Tuple[Any, SlayerModel]]]]


async def collect_reachable_agg_names(
    source_model: SlayerModel,
    resolve_join_target: ResolveJoinTarget,
    named_queries: Optional[Dict] = None,
) -> Optional[FrozenSet[str]]:
    """Collect custom aggregation names from ``source_model`` and every
    join-reachable model.

    BFS bounded only by the visited set (no fixed depth cap — dotted-path
    resolution supports arbitrary depth, so the agg-name rewrite must too).
    Returns ``None`` when no custom aggregations exist anywhere in the
    reachable subgraph.

    ``resolve_join_target`` is the existing engine callback whose return
    shape is ``(target_sql, target_model) | None``; this helper only uses
    the ``target_model`` element.
    """
    names: set[str] = set()
    visited: set[str] = set()
    queue: List[SlayerModel] = [source_model]

    while queue:
        current = queue.pop(0)
        if current.name in visited:
            continue
        visited.add(current.name)

        if current.aggregations:
            names.update(a.name for a in current.aggregations)

        for join in current.joins:
            if join.target_model in visited:
                continue
            target_info = await resolve_join_target(
                target_model_name=join.target_model,
                named_queries=named_queries or {},
            )
            if target_info:
                _, target_model_obj = target_info
                if target_model_obj is not None:
                    queue.append(target_model_obj)

    return frozenset(names) if names else None


# ---------------------------------------------------------------------------
# Name-based lookups
# ---------------------------------------------------------------------------


def is_known_aggregation_name(
    name: str,
    custom_names: Optional[FrozenSet[str]],
) -> bool:
    """``True`` if ``name`` is a built-in aggregation or appears in the
    model-collected custom set.
    """
    if name in BUILTIN_AGGREGATIONS:
        return True
    return bool(custom_names) and name in custom_names


def resolve_aggregation(
    name: str,
    available_aggs: List[Aggregation],
) -> Optional[Aggregation]:
    """Return the ``Aggregation`` definition for ``name`` if one is
    declared in ``available_aggs``, else ``None``.

    A model-level entry whose ``name`` matches a built-in is treated as
    an override and returned. ``None`` for a built-in name with no
    override is the signal to use the default built-in formula.
    """
    for agg in available_aggs:
        if agg.name == name:
            return agg
    return None


# ---------------------------------------------------------------------------
# Parameter resolution
# ---------------------------------------------------------------------------


def required_params_for(agg_name: str) -> Tuple[str, ...]:
    """Required parameter names for a built-in aggregation (e.g.,
    ``weighted_avg`` requires ``weight``).

    Custom aggregations declare their parameter shape via
    ``Aggregation.params``; this helper only knows about the built-in
    table in ``slayer.core.enums``. Returns an empty tuple for unknown
    names so callers can branch on emptiness rather than ``KeyError``.
    """
    return tuple(BUILTIN_AGGREGATION_REQUIRED_PARAMS.get(agg_name, []))


def merge_agg_params(
    agg_def: Optional[Aggregation],
    query_kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    """Combine ``Aggregation.params`` defaults with query-time kwargs.

    Query-time kwargs override defaults. Kwargs not declared by the
    ``agg_def`` pass through unchanged — validation of param names
    (e.g., rejecting unknown ones) is the binder's responsibility,
    not this helper's.
    """
    if agg_def is None:
        return dict(query_kwargs)
    defaults = {p.name: p.sql for p in agg_def.params}
    return {**defaults, **query_kwargs}
