"""Result models + rendering for ``recommend_population``.

Given a set of ``dataset.field`` / ``dataset.metric`` items an agent wants in
one query, the engine recommends a root dataset (query ``population``) and
the relationship-qualified reference path for each item from that root. These
models are the caller-facing shape returned by the engine and echoed
verbatim through MCP / REST / CLI / SlayerClient.
"""

from __future__ import annotations

from pydantic import BaseModel


class ItemPath(BaseModel):
    """One input item paired with its relationship-qualified reference path from
    the recommended root (relative to the root, root name excluded — the
    same form used inside a ``SlayerQuery`` whose ``population`` is the
    root). Any aggregation suffix is preserved verbatim."""

    input_item: str
    path: str
    # True for a saved metric / aggregation-suffixed item — reachable
    # from the root but not a determination item that steers the recommendation.
    attachment: bool = False
    # An attachment on a dataset no relationship path reaches: its path is the root's product hop.
    unrelated: bool = False


class CandidateCoverage(BaseModel):
    """A partial-root candidate for the no-common-root diagnostic: which
    input items it can reach and which it cannot."""

    dataset_name: str
    reachable_items: list[str]
    unreachable_items: list[str]


class PopulationRecommendation(BaseModel):
    """Outcome of :meth:`SlayerQueryEngine.recommend_population`.

    When ``reachable`` is ``True``, ``population`` names the recommended
    ``population`` and ``item_paths`` gives every item's path from it.
    When ``False``, no single dataset reaches all items: ``population`` is
    ``None``, ``item_paths`` is empty, and ``coverage`` lists the Pareto
    frontier of partial roots so the caller can split the work into a
    multi-stage query.
    """

    data_source: str
    population: str | None
    reachable: bool
    item_paths: list[ItemPath] = []
    coverage: list[CandidateCoverage] = []
    message: str = ""
    warnings: list[str] = []


def _render_item_paths(rec: PopulationRecommendation) -> list[str]:
    lines = [
        f"Recommended root dataset: `{rec.population}`",
        "",
        "| Input item | Path from root |",
        "|------------|----------------|",
    ]
    for ip in rec.item_paths:
        note = " (unrelated: no relationship path, aggregated over its own rows)" if ip.unrelated else ""
        lines.append(f"| `{ip.input_item}` | `{ip.path}`{note} |")
    return lines


def _render_coverage(rec: PopulationRecommendation) -> list[str]:
    lines = ["Recommended root dataset: _none_ — no single dataset reaches every item."]
    if rec.message:
        lines.extend(["", rec.message])
    if rec.coverage:
        lines.extend([
            "",
            "Best partial roots (Pareto frontier):",
            "",
            "| Candidate root | Reaches | Cannot reach |",
            "|----------------|---------|--------------|",
        ])
    for c in rec.coverage:
        reaches = ", ".join(f"`{i}`" for i in c.reachable_items) or "_(none)_"
        misses = ", ".join(f"`{i}`" for i in c.unreachable_items) or "_(none)_"
        lines.append(f"| `{c.dataset_name}` | {reaches} | {misses} |")
    return lines


def render_recommendation_markdown(rec: PopulationRecommendation) -> str:
    """Render a recommendation as human-readable markdown (MCP / CLI)."""
    lines = [f"Datasource: `{rec.data_source}`"]
    if rec.reachable and rec.population is not None:
        lines.extend(_render_item_paths(rec))
    else:
        lines.extend(_render_coverage(rec))
    if rec.warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"- {w}" for w in rec.warnings)
    return "\n".join(lines)
