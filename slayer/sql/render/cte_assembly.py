"""WITH-chain assembly in dependency order from caller-declared dependencies
(never AST-scanned); insertion order tiebreaks so the SQL is byte-stable."""

from __future__ import annotations

from typing import AbstractSet, Dict, List, Sequence

from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp

__all__ = ["CteEntry", "assemble_with_chain"]


class CteEntry(BaseModel):
    """One CTE: its allocator-minted name, its query, and what it reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    query: exp.Expression
    #: CTEs this one references; a name absent here is ordered by the enclosing assembly.
    depends_on: List[str] = Field(default_factory=list)


def _index_entries(
    entries: Sequence[CteEntry], *, external_names: AbstractSet[str],
) -> Dict[str, CteEntry]:
    """Name → entry, rejecting duplicate names and dangling dependencies (both
    wiring bugs); ``external_names`` (enclosing-assembly CTEs) are exempt."""
    by_name: Dict[str, CteEntry] = {}
    for entry in entries:
        if entry.name in by_name:
            raise ValueError(
                f"duplicate CTE name {entry.name!r} in one WITH chain",
            )
        by_name[entry.name] = entry
    for entry in entries:
        unknown = [
            d for d in entry.depends_on
            if d not in by_name and d not in external_names
        ]
        if unknown:
            raise ValueError(
                f"CTE {entry.name!r} declares unknown dependencies "
                f"{unknown!r}; known CTEs are {sorted(by_name)}",
            )
    return by_name


def _topological_order(
    *, entries: Sequence[CteEntry], by_name: Dict[str, CteEntry],
) -> List[CteEntry]:
    """Depth-first emit: each dependency before the entry needing it, else declaration order."""
    ordered: List[CteEntry] = []
    emitted: set[str] = set()
    visiting: List[str] = []

    def _visit(entry: CteEntry) -> None:
        if entry.name in emitted:
            return
        if entry.name in visiting:
            cycle = " -> ".join(
                [*visiting[visiting.index(entry.name):], entry.name],
            )
            raise ValueError(f"dependency cycle between CTEs: {cycle}")
        visiting.append(entry.name)
        for dep in entry.depends_on:
            target = by_name.get(dep)
            if target is not None:
                _visit(target)
        visiting.pop()
        emitted.add(entry.name)
        ordered.append(entry)

    for entry in entries:
        _visit(entry)
    return ordered


def assemble_with_chain(
    *,
    entries: Sequence[CteEntry],
    final: exp.Select,
    external_names: AbstractSet[str] = frozenset(),
) -> exp.Select:
    """Attach ``entries`` to ``final`` as a WITH clause in dependency order;
    returns ``final`` unchanged when empty. ``final`` must not already carry a WITH
    clause — the assembler owns the CTE list and would strand a pre-attached one."""
    if final.args.get("with_") is not None:
        raise ValueError(
            "assemble_with_chain owns the WITH clause, but `final` already "
            "carries one; merge those CTEs into `entries` (with their "
            "dependencies declared) rather than attaching them beforehand",
        )
    if not entries:
        return final

    by_name = _index_entries(entries, external_names=external_names)
    ordered = _topological_order(entries=entries, by_name=by_name)

    out = final.copy()
    for entry in ordered:
        out = out.with_(entry.name, as_=entry.query.copy(), copy=False)
    return out
