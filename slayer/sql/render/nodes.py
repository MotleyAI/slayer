"""Renderer pipeline nodes: named WITH-chain relations carrying the slot schema
each phase exposes to the next. Fusion is an emission decision, never semantic."""

from __future__ import annotations

from typing import Dict, List, Literal

from pydantic import Field

from slayer.sql.render.cte_assembly import CteEntry

__all__ = ["Node", "fusion_blockers"]

NodePhase = Literal["producer", "base", "combined", "step"]


class Node(CteEntry):
    """A WITH-chain relation plus the slot schema it exposes (slot id → aliases)."""

    phase: NodePhase = "step"
    schema_by_slot: Dict[str, List[str]] = Field(default_factory=dict)


def fusion_blockers(
    *,
    has_combined_phase: bool,
    has_transform_steps: bool,
    trims_hidden_columns: bool,
) -> List[str]:
    """Reasons the base node cannot fuse into the final statement (empty = one SELECT)."""
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
