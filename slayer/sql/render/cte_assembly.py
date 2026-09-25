"""WITH-chain assembly in dependency order from caller-declared dependencies
(never AST-scanned); insertion order tiebreaks so the SQL is byte-stable."""

from __future__ import annotations

from typing import AbstractSet, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp
from sqlglot.expressions.core import Expression

__all__ = [
    "CteEntry",
    "assemble_with_chain",
    "cte_entry",
    "reachable_cte_entries",
    "rename_embedded_ctes",
]


class CteEntry(BaseModel):
    """One CTE: its allocator-minted name, its query, and what it reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    query: exp.Expression
    #: CTEs that must precede this one; a name absent here is ordered by the enclosing assembly.
    depends_on: List[str] = Field(default_factory=list)
    #: The CTE's column list (``n(k)``), if its definition carries one.
    columns: List[exp.Identifier] = Field(default_factory=list)
    #: Defined under ``WITH RECURSIVE``; the assembled WITH is recursive if any entry is.
    recursive: bool = False
    quoted: bool = False


def cte_entry(*, cte: exp.CTE, name: str, depends_on: List[str]) -> CteEntry:
    """A hoisted ``cte`` as an entry named ``name``, keeping its column list, recursion and quoting."""
    alias = cte.args.get("alias")
    ident = alias.this if isinstance(alias, exp.TableAlias) else None
    with_node = cte.parent
    return CteEntry(
        name=name, query=cte.this.copy(), depends_on=depends_on,
        columns=[c.copy() for c in (alias.columns if isinstance(alias, exp.TableAlias) else [])],
        recursive=isinstance(with_node, exp.With) and bool(with_node.args.get("recursive")),
        quoted=isinstance(ident, exp.Identifier) and bool(ident.quoted),
    )


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
    out.set("with_", exp.With(
        expressions=[
            exp.CTE(
                this=entry.query.copy(),
                alias=exp.TableAlias(
                    this=exp.to_identifier(entry.name, quoted=entry.quoted or None),
                    columns=[c.copy() for c in entry.columns] or None,
                ),
            )
            for entry in ordered
        ],
        recursive=any(entry.recursive for entry in ordered) or None,
    ))
    return out


def reachable_cte_entries(
    *, entries: Sequence[CteEntry], seeds: AbstractSet[str],
) -> List[CteEntry]:
    """The ``entries`` reachable from ``seeds`` along declared ``depends_on``, in order."""
    by_name = {e.name: e for e in entries}
    keep: set[str] = set()
    frontier = [n for n in seeds if n in by_name]
    while frontier:
        name = frontier.pop()
        if name in keep:
            continue
        keep.add(name)
        frontier.extend(d for d in by_name[name].depends_on if d in by_name)
    return [e for e in entries if e.name in keep]


def _ident_key(ident: exp.Identifier) -> str:
    """Unquoted identifiers compare case-insensitively, quoted ones exactly."""
    return ident.name if ident.quoted else ident.name.lower()


def _binding_with(table: exp.Table) -> Optional[Tuple[exp.With, str]]:
    """The ``WITH`` whose CTE ``table`` references, by SQL scoping: the nearest
    enclosing definition, where a CTE body sees only earlier siblings (or all of
    them, itself included, under ``RECURSIVE``); ``None`` for a physical table."""
    if table.args.get("db") is not None or not isinstance(table.this, exp.Identifier):
        return None
    key = _ident_key(table.this)
    prev: object = table
    node = table.parent
    while node is not None:
        scope = _visible_ctes(node, prev=prev)
        if scope is not None and key in scope[1]:
            return scope[0], key
        prev, node = node, node.parent
    return None


def _visible_ctes(node: exp.Expr, *, prev: object) -> Optional[Tuple[exp.With, List[str]]]:
    """The ``WITH`` ``node`` holds, and the CTE keys visible from its child ``prev``."""
    if isinstance(node, exp.With):
        names = [_ident_key(c.args["alias"].this) for c in node.expressions]
        if node.args.get("recursive"):
            return node, names
        return node, names[:next(i for i, c in enumerate(node.expressions) if c is prev)]
    with_ = node.args.get("with_")
    if with_ is None or with_ is prev:
        return None
    return with_, [_ident_key(c.args["alias"].this) for c in with_.expressions]


def rename_embedded_ctes(statement: Expression, *, allocate: Callable[[str], str]) -> None:
    """Rename every CTE ``statement`` defines through ``allocate``, in place, rebinding
    only the table references bound to that definition (nested shadowing, alias
    reuse, recursion and quoting preserved); a renamed reference keeps its old
    spelling as its alias, so column qualifiers stay valid."""
    withs = list(statement.find_all(exp.With))
    if not withs:
        return
    bindings = [(t, _binding_with(t)) for t in statement.find_all(exp.Table)]
    new_names: Dict[Tuple[int, str], str] = {}
    for with_node in withs:
        for cte in with_node.expressions:
            ident = cte.args["alias"].this
            new = allocate(ident.name)
            new_names[(id(with_node), _ident_key(ident))] = new
            cte.args["alias"].set("this", exp.to_identifier(new, quoted=ident.quoted or None))
    for table, binding in bindings:
        if binding is None:
            continue
        old = table.this
        table.set("this", exp.to_identifier(
            new_names[(id(binding[0]), binding[1])], quoted=old.quoted or None,
        ))
        if table.args.get("alias") is None:
            table.set("alias", exp.TableAlias(this=old.copy()))
