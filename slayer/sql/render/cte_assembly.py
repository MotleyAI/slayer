"""WITH-chain assembly in topological order (§5.6).

The cross-model paths used to splice their WITH chain out of f-strings::

    cte_strs = [f"{name} AS (\\n{sql}\\n)" for name, sql in all_ctes[:-1]]
    sql = f"WITH {', '.join(cte_strs)}\\n{combined_select_sql}"

so the emitted order was whatever order the python list happened to be built
in, and the transform chain read its predecessor positionally
(``prev_cte = ctes[-1][0]``). That works only while one hard-coded sequence
stays correct; it carries no statement of what actually depends on what.

Here a caller DECLARES each CTE's dependencies and the assembler emits a stable
topological order, with insertion order as the tiebreak so independent CTEs
keep declaration order and the SQL is byte-stable across runs.

Dependencies are declared, never discovered by scanning the rendered AST. A
scan cannot tell a CTE reference from a same-named real table, is defeated by
quoting and case folding, and — worst — would silently mis-order rather than
fail. The caller already knows the answer structurally (``_wm_`` reads
``_base``; transform step N reads step N-1; ``_cm_`` reads nothing), so it says
so.

Ordering is the only thing this module owns. Name collisions remain
``assert_unique_cte_names``' job, which validates the emitted statement per
WITH scope and case-folds on dialects that fold.
"""

from __future__ import annotations

from typing import Dict, List, Sequence

from pydantic import BaseModel, ConfigDict
from sqlglot import exp

__all__ = ["CteEntry", "assemble_with_chain"]


class CteEntry(BaseModel):
    """One CTE: its allocator-minted name, its query, and what it reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    query: exp.Expression
    #: Names of CTEs this one references. Must all be present in the same
    #: assembly; a dangling name is a wiring bug, not a no-op.
    depends_on: List[str] = []


def _index_entries(entries: Sequence[CteEntry]) -> Dict[str, CteEntry]:
    """Name → entry, rejecting a duplicate name or a dangling dependency.

    Both are wiring bugs whose SQL would be invalid or silently wrong, so they
    fail here rather than at the database.
    """
    by_name: Dict[str, CteEntry] = {}
    for entry in entries:
        if entry.name in by_name:
            raise ValueError(
                f"duplicate CTE name {entry.name!r} in one WITH chain",
            )
        by_name[entry.name] = entry
    for entry in entries:
        unknown = [d for d in entry.depends_on if d not in by_name]
        if unknown:
            raise ValueError(
                f"CTE {entry.name!r} declares unknown dependencies "
                f"{unknown!r}; known CTEs are {sorted(by_name)}",
            )
    return by_name


def _topological_order(
    *, entries: Sequence[CteEntry], by_name: Dict[str, CteEntry],
) -> List[CteEntry]:
    """Depth-first emit in declaration order: the first entry that is ready goes
    first, and a dependency is emitted immediately before the entry needing it.
    Declaration order is preserved wherever dependencies permit.
    """
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
            _visit(by_name[dep])
        visiting.pop()
        emitted.add(entry.name)
        ordered.append(entry)

    for entry in entries:
        _visit(entry)
    return ordered


def assemble_with_chain(
    *, entries: Sequence[CteEntry], final: exp.Select,
) -> exp.Select:
    """Attach ``entries`` to ``final`` as a WITH clause in dependency order.

    Returns ``final`` unchanged when there are no entries — an empty ``WITH`` is
    not valid SQL.

    ``final`` must not already carry a WITH clause. The assembler owns the
    statement's CTE list, and silently discarding one the caller had attached
    would leave its references dangling — a live hazard for the transform
    chains, which build a statement that already has CTEs before wrapping it.
    """
    if final.args.get("with_") is not None:
        raise ValueError(
            "assemble_with_chain owns the WITH clause, but `final` already "
            "carries one; merge those CTEs into `entries` (with their "
            "dependencies declared) rather than attaching them beforehand",
        )
    if not entries:
        return final

    by_name = _index_entries(entries)
    ordered = _topological_order(entries=entries, by_name=by_name)

    out = final.copy()
    for entry in ordered:
        out = out.with_(entry.name, as_=entry.query.copy(), copy=False)
    return out
