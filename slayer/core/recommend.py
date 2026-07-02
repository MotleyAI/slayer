"""Result models + rendering for ``recommend_root_model`` (DEV-1626).

Given a set of ``model.column`` / ``model.metric`` items an agent wants in
one query, the engine recommends a root model (query ``source_model``) and
the join-qualified reference path for each item from that root. These
models are the caller-facing shape returned by the engine and echoed
verbatim through MCP / REST / CLI / SlayerClient.
"""

from __future__ import annotations

from pydantic import BaseModel


class ItemPath(BaseModel):
    """One input item paired with its join-qualified reference path from
    the recommended root (relative to the root, root name excluded — the
    same form used inside a ``SlayerQuery`` whose ``source_model`` is the
    root). Any aggregation suffix is preserved verbatim."""

    input_item: str
    path: str


class CandidateCoverage(BaseModel):
    """A partial-root candidate for the no-common-root diagnostic: which
    input items it can reach and which it cannot."""

    model_name: str
    reachable_items: list[str]
    unreachable_items: list[str]


class RootModelRecommendation(BaseModel):
    """Outcome of :meth:`SlayerQueryEngine.recommend_root_model`.

    When ``reachable`` is ``True``, ``root_model`` names the recommended
    ``source_model`` and ``item_paths`` gives every item's path from it.
    When ``False``, no single model reaches all items: ``root_model`` is
    ``None``, ``item_paths`` is empty, and ``coverage`` lists the Pareto
    frontier of partial roots so the caller can split the work into a
    multi-stage query.
    """

    data_source: str
    root_model: str | None
    reachable: bool
    item_paths: list[ItemPath] = []
    coverage: list[CandidateCoverage] = []
    message: str = ""
    warnings: list[str] = []


def render_recommendation_markdown(rec: RootModelRecommendation) -> str:
    """Render a recommendation as human-readable markdown (MCP / CLI)."""
    lines: list[str] = []
    lines.append(f"Datasource: `{rec.data_source}`")
    if rec.reachable and rec.root_model is not None:
        lines.append(f"Recommended root model: `{rec.root_model}`")
        lines.append("")
        lines.append("| Input item | Path from root |")
        lines.append("|------------|----------------|")
        for ip in rec.item_paths:
            lines.append(f"| `{ip.input_item}` | `{ip.path}` |")
    else:
        lines.append("Recommended root model: _none_ — no single model "
                     "reaches every item.")
        if rec.message:
            lines.append("")
            lines.append(rec.message)
        if rec.coverage:
            lines.append("")
            lines.append("Best partial roots (Pareto frontier):")
            lines.append("")
            lines.append("| Candidate root | Reaches | Cannot reach |")
            lines.append("|----------------|---------|--------------|")
            for c in rec.coverage:
                reaches = ", ".join(f"`{i}`" for i in c.reachable_items) or "_(none)_"
                misses = ", ".join(f"`{i}`" for i in c.unreachable_items) or "_(none)_"
                lines.append(f"| `{c.model_name}` | {reaches} | {misses} |")
    if rec.warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"- {w}" for w in rec.warnings)
    return "\n".join(lines)
