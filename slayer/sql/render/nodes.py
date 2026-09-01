"""Renderer pipeline nodes (design D1).

One render pipeline builds every statement: base → aggregate → combined →
steps → post. Each phase that materialises a relation contributes a
:class:`Node` — a named member of the statement's one flat WITH chain that
also carries the slot schema it exposes, so the next phase reads its inputs
off the node instead of a separately-threaded alias dict. The POST phase
(projection trim / ORDER / LIMIT) wraps the tail and emits no node.

Fusion (design D2) is an emission decision, never semantic: adjacent phases
collapse into one SELECT only when no blocker holds (:func:`fusion_blockers`).
A plain query fuses end to end into a single SELECT; a producer body fuses
into a single CTE body — those are the fusion fixed points, pinned
byte-for-byte by the fusion snapshot goldens.
"""

from __future__ import annotations

from typing import Dict, List, Literal

from pydantic import Field

from slayer.sql.render.cte_assembly import CteEntry

__all__ = ["Node", "fusion_blockers"]

NodePhase = Literal["producer", "base", "combined", "step"]


class Node(CteEntry):
    """One pipeline node: a relation in the flat WITH chain plus the slot
    schema it exposes (slot id → the aliases later phases may reference)."""

    phase: NodePhase = "step"
    schema_by_slot: Dict[str, List[str]] = Field(default_factory=dict)


def fusion_blockers(
    *,
    has_combined_phase: bool,
    has_transform_steps: bool,
    trims_hidden_columns: bool,
) -> List[str]:
    """Why the base node may NOT fuse into the final statement (D2).

    Empty means the whole pipeline collapses to a single SELECT with ORDER /
    LIMIT applied inline — today's plain fast path. Each blocker forces the
    base into the WITH chain (or, for a hidden-column trim, under a wrapper
    that re-projects the public schema).
    """
    blockers: List[str] = []
    if has_combined_phase:
        blockers.append(
            "combined phase: producer join-backs read the base as a relation",
        )
    if has_transform_steps:
        blockers.append("step CTEs consume the base schema by name")
    if trims_hidden_columns:
        blockers.append(
            "hidden-column trim boundary: the post wrap re-projects the "
            "public schema",
        )
    return blockers
