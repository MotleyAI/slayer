"""Stage 3 (DEV-1450) — join-graph walker extracted from query_engine.

Single source of truth for both dimension and cross-model-measure
resolution (DEV-1369 consolidated the two prior near-duplicates;
DEV-1450 stage 3 lifts the consolidated walker out of the
``SlayerQueryEngine`` class so it's directly importable by the new
binder modules without dragging the engine in).

Existing call sites in ``slayer/engine/query_engine.py`` keep their
method signature unchanged via a thin shim that supplies
``self._resolve_model`` as the ``resolve_model`` callback.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional, Tuple

from slayer.core.models import ModelJoin, SlayerModel


class NoJoinError(Exception):
    """Sentinel raised by ``walk_join_chain`` when
    ``strict_missing_join=False`` and a hop has no matching join.

    Lets lenient callers (dimension resolution) map a missing join to a
    ``None`` return without re-walking the path. Strict callers
    (cross-model measure resolution) use the ``ValueError`` branch
    instead, which carries a more actionable available-joins message.
    """

    def __init__(self, hop_name: str) -> None:
        super().__init__(f"no join target named {hop_name!r}")
        self.hop_name = hop_name


# Type alias for the resolve_model callable. The signature mirrors
# ``SlayerQueryEngine._resolve_model`` so the engine method can be passed
# directly without an adapter.
ResolveModel = Callable[..., Awaitable[SlayerModel]]


async def walk_join_chain(
    *,
    source_model: SlayerModel,
    hop_names: list[str],
    resolve_model: ResolveModel,
    named_queries: Optional[dict] = None,
    strict_missing_join: bool = True,
) -> Tuple[SlayerModel, Optional[ModelJoin]]:
    """Walk the join graph from ``source_model`` through ``hop_names``,
    returning ``(terminal_model, first_join)``.

    Cycle detection: a hop name that already appears on the visited
    stack (including ``source_model.name``) raises ``ValueError`` with
    the offending path.

    Missing-join behavior:

    * ``strict_missing_join=True`` — raise ``ValueError`` listing the
      available joins on the current model. Used by cross-model-measure
      resolution where a missing join is a user error worth surfacing.
    * ``strict_missing_join=False`` — raise ``NoJoinError`` so the
      caller can map to a ``None`` return. Used by dimension resolution
      where a missing intermediate join just means the column can't be
      reached this way and the caller should keep looking.

    ``resolve_model`` is an async callable
    ``(model_name=, named_queries=, prefer_data_source=) -> SlayerModel``.
    Typically ``SlayerQueryEngine._resolve_model``.
    """
    current_model = source_model
    visited = {source_model.name}
    first_join: Optional[ModelJoin] = None

    nq: Any = named_queries if named_queries is not None else {}

    for i, hop_name in enumerate(hop_names):
        if hop_name in visited:
            raise ValueError(
                f"Circular join detected while resolving "
                f"'{'.'.join(hop_names)}': '{hop_name}' already visited "
                f"({' → '.join(visited)} → {hop_name})"
            )
        join = next(
            (j for j in current_model.joins if j.target_model == hop_name),
            None,
        )
        if join is None:
            if strict_missing_join:
                raise ValueError(
                    f"Model '{current_model.name}' has no join to "
                    f"'{hop_name}'. Available joins: "
                    f"{[j.target_model for j in current_model.joins]}"
                )
            raise NoJoinError(hop_name)
        if i == 0:
            first_join = join
        current_model = await resolve_model(
            model_name=hop_name,
            named_queries=nq,
            prefer_data_source=current_model.data_source or None,
        )
        visited.add(hop_name)

    return current_model, first_join
