"""Scope-closure validator for generated SQL.

C1 (binding): every column qualifier binds to its own scope's FROM / JOIN sources.
C2 (projection): a reference into a CTE / derived table names a column it projects.
Sound on the generator's corpus: raises only on a provable leak (unqualified refs and
physical-table columns are unverifiable). Runs on post-mangle, pre-RLS output (pre-mangle
dotted aliases would false-flag); ``allow_rls_correlation`` admits the session policy's
``_rls_src`` EXISTS reference. ``SLAYER_VALIDATE_SCOPES=1`` checks every emitted statement.
"""

from __future__ import annotations

import os
from typing import Dict, Optional

import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, ScopeType, traverse_scope

from slayer.sql.naming import assert_unique_cte_names, dialect_folds_case

# ``session_policy._RLS_SRC``: the only legal correlated ref in post-RLS SQL.
_RLS_SRC = "_rls_src"

_ENV_FLAG = "SLAYER_VALIDATE_SCOPES"
_TRUTHY = frozenset({"1", "true", "yes", "on"})


class ScopeLeak(BaseModel):
    """A single provable out-of-scope reference."""

    model_config = ConfigDict(frozen=True)

    kind: str  # "unbound_table" | "unprojected_column"
    reference: str  # e.g. "regions.population"
    scope: str = ""  # human label of the leaking scope
    bound_sources: tuple[str, ...] = ()


class ScopeCheckResult(BaseModel):
    """Structured result of :func:`check_scope_closed`."""

    model_config = ConfigDict(frozen=True)

    closed: bool
    leaks: tuple[ScopeLeak, ...] = ()
    skipped: bool = False
    skip_reason: Optional[str] = None


class ScopeLeakError(AssertionError):
    """Raised by :func:`assert_scope_closed` when a statement is not closed."""


class CteOrderError(AssertionError):
    """A CTE reads a sibling declared later in its ``WITH`` (SQLite tolerates it, DuckDB rejects it)."""


def assert_dependency_ordered_ctes(sql: str, *, dialect: str = "postgres") -> None:
    """Raise :class:`CteOrderError` if a CTE reads a sibling declared later in its ``WITH`` (python.sql.arc42.md P6).

    Judges unqualified references against the nearest defining ``WITH``, folding names as the
    allocator does (``casefold`` would over-equate ``ß``/``ss``). A self reference is not flagged:
    it is indistinguishable from a CTE shadowing a same-named physical table."""
    ast = sqlglot.parse_one(sql, dialect=dialect)
    if ast is None:
        return
    fold = (lambda s: s.lower()) if dialect_folds_case(dialect) else (lambda s: s)
    for tbl in ast.find_all(exp.Table):
        if tbl.args.get("db") is not None or tbl.args.get("catalog") is not None:
            continue  # schema-qualified → physical, never a bare CTE reference
        name = fold(tbl.name)
        binding = _nearest_defining_with(tbl=tbl, name=name, fold=fold)
        if binding is None:
            continue
        with_node, positions = binding
        containing = _containing_cte_index(tbl=tbl, with_node=with_node)
        if containing is None:
            continue  # a main-body reference is always after every CTE
        if positions[name] > containing:
            raise CteOrderError(
                f"CTE at position {containing} in a WITH reads sibling "
                f"{tbl.name!r} declared later (position {positions[name]}); a "
                f"forward CTE reference is undeclared dependency order.\n"
                f"SQL:\n{sql}"
            )


def _cte_positions(*, with_node: exp.With, fold) -> Dict[str, int]:
    """CTE name (``fold``-normalised) → declaration index within ``with_node``."""
    return {
        fold(cte.alias_or_name): i
        for i, cte in enumerate(with_node.expressions)
    }


def _nearest_defining_with(*, tbl: exp.Table, name: str, fold):
    """The nearest ancestor ``WITH`` declaring CTE ``name`` (folded) with its position map, else ``None``."""
    node = tbl.parent
    while node is not None:
        if isinstance(node, exp.With):
            positions = _cte_positions(with_node=node, fold=fold)
            if name in positions:
                return node, positions
        node = node.parent
    return None


def _containing_cte_index(*, tbl: exp.Table, with_node: exp.With) -> Optional[int]:
    """Index of the ``with_node`` CTE whose body contains ``tbl``; ``None`` in the main query body."""
    node = tbl.parent
    while node is not None and node is not with_node:
        if isinstance(node, exp.CTE) and node.parent is with_node:
            for i, cte in enumerate(with_node.expressions):
                if cte is node:
                    return i
        node = node.parent
    return None


def check_scope_closed(
    sql: str, *, dialect: str = "postgres", allow_rls_correlation: bool = False
) -> ScopeCheckResult:
    """Analyse ``sql`` into a :class:`ScopeCheckResult`; never raises for a leak."""
    ast = sqlglot.parse_one(sql, dialect=dialect)
    if ast is None:
        return ScopeCheckResult(closed=True, skipped=True, skip_reason="empty parse")

    leaks: list[ScopeLeak] = []
    seen: set = set()
    for scope in traverse_scope(ast):
        for leak in _scope_leaks(scope, allow_rls_correlation=allow_rls_correlation):
            key = (leak.kind, leak.reference, leak.scope)
            if key not in seen:
                seen.add(key)
                leaks.append(leak)
    return ScopeCheckResult(closed=not leaks, leaks=tuple(leaks))


def assert_scope_closed(
    sql: str, *, dialect: str = "postgres", allow_rls_correlation: bool = False
) -> None:
    """Raise :class:`ScopeLeakError` if ``sql`` is not scope-closed."""
    result = check_scope_closed(
        sql, dialect=dialect, allow_rls_correlation=allow_rls_correlation
    )
    if result.leaks:
        raise ScopeLeakError(_format_leaks(result.leaks, sql))


def maybe_validate_scopes(sql: str, *, dialect: str = "postgres") -> None:
    """Run the closure, CTE-name and CTE-order checks iff ``SLAYER_VALIDATE_SCOPES`` is truthy (read per call)."""
    value = os.environ.get(_ENV_FLAG)
    if value is not None and value.strip().lower() in _TRUTHY:
        assert_scope_closed(sql=sql, dialect=dialect)
        assert_unique_cte_names(sql=sql, dialect=dialect)
        assert_dependency_ordered_ctes(sql=sql, dialect=dialect)


def _scope_leaks(scope: Scope, *, allow_rls_correlation: bool) -> list[ScopeLeak]:
    leaks: list[ScopeLeak] = []
    label = _scope_label(scope)
    bound = tuple(scope.sources.keys())
    for col in scope.columns:
        qualifier = col.table
        if not qualifier or _array_joined(col, qualifier):
            continue  # unverifiable without a schema
        source = _resolve_source(scope=scope, qualifier=qualifier)
        if source is None:
            source = _resolve_correlated(scope=scope, qualifier=qualifier)
        if source is None:
            # C1: the qualifier is not bound in this scope.
            if allow_rls_correlation and qualifier == _RLS_SRC:
                continue
            leaks.append(ScopeLeak(
                kind="unbound_table", reference=_ref(col), scope=label,
                bound_sources=bound,
            ))
        elif isinstance(source, Scope):
            # C2: bound to an inner SELECT scope — the name must be projected.
            if not _projects(source, col.name):
                leaks.append(ScopeLeak(
                    kind="unprojected_column", reference=_ref(col), scope=label,
                    bound_sources=bound,
                ))
    return leaks


def _array_joined(col: exp.Column, qualifier: str) -> bool:
    """Whether a SELECT enclosing ``col`` ClickHouse-``ARRAY JOIN``s ``… AS qualifier`` (unverifiable without a schema)."""
    select = col.find_ancestor(exp.Select)
    while select is not None:
        joins = select.args.get("joins") or []
        if any(j.args.get("kind") == "ARRAY" and isinstance(j.this, exp.Alias) and j.this.alias == qualifier
               for j in joins):
            return True
        select = select.find_ancestor(exp.Select)
    return False


def _resolve_source(scope: Scope, qualifier: str):
    """Look up ``qualifier`` among the scope's sources, case-insensitively."""
    source = scope.sources.get(qualifier)
    if source is not None:
        return source
    folded = qualifier.casefold()
    for name, src in scope.sources.items():
        if name.casefold() == folded:
            return src
    return None


def _resolve_correlated(scope: Scope, qualifier: str):
    """Resolve ``qualifier`` in ancestor scopes across expression-subquery, table-function (``UNNEST``) and
    ``LATERAL`` boundaries only (where SQL permits correlation); any other derived table sees nothing of its parent."""
    current = scope
    while current.parent is not None:
        kind = current.scope_type
        lateral = _is_lateral(current)
        current = current.parent
        if kind in (ScopeType.SUBQUERY, ScopeType.UDTF) or lateral:
            source = _resolve_source(scope=current, qualifier=qualifier)
            if source is not None:
                return source
        elif kind != ScopeType.DERIVED_TABLE:
            break
    return None


def _is_lateral(scope: Scope) -> bool:
    """A ``LATERAL (…)`` derived table: it reads the FROM items before it."""
    wrapper = scope.expression.parent
    return isinstance(wrapper, exp.Subquery) and isinstance(wrapper.parent, exp.Lateral)


def _projects(inner: Scope, name: str) -> bool:
    """Whether the inner scope projects ``name``; a star exports every name its ``EXCEPT`` list keeps."""
    folded = name.casefold()
    expr = inner.expression
    if inner.scope_type == ScopeType.UDTF:
        # A table function (``UNNEST``, ``LATERAL (…)``): its alias's column list, else its subquery's outputs.
        alias = expr.args.get("alias")
        if isinstance(alias, exp.TableAlias) and alias.columns and alias.this is not None:
            return folded in {c.name.casefold() for c in alias.columns}
        if not (isinstance(expr, exp.Lateral) and isinstance(expr.this, exp.Subquery)):
            return True  # unverifiable without the function's signature
        expr = expr.this.this
    alias = expr.parent.args.get("alias") if isinstance(expr.parent, (exp.CTE, exp.Subquery)) else None
    if isinstance(alias, exp.TableAlias) and alias.columns:
        return folded in {c.name.casefold() for c in alias.columns}  # ``n(k)`` renames the outputs
    while isinstance(expr, exp.SetOperation):
        expr = expr.this  # left leg carries the public output names
    selects = getattr(expr, "selects", None) or []
    explicit: set = set()
    for projection in selects:
        star = _star_of(projection)
        if star is not None:
            excepted = {
                _identifier_name(x).casefold() for x in (star.args.get("except_") or [])
            }
            if folded in excepted:
                continue  # this star explicitly drops the name
            return True  # otherwise the star exports everything, including name
        explicit.add(projection.alias_or_name.casefold())
    return folded in explicit


def _star_of(projection: exp.Expression) -> Optional[exp.Star]:
    if isinstance(projection, exp.Star):
        return projection
    if isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star):
        return projection.this
    return None


def _identifier_name(node: exp.Expression) -> str:
    return node.name if isinstance(node, (exp.Column, exp.Identifier)) else node.sql()


def _ref(col: exp.Column) -> str:
    return f"{col.table}.{col.name}"


def _scope_label(scope: Scope) -> str:
    node = scope.expression
    parent = node.parent
    if isinstance(parent, exp.CTE):
        return f"CTE {parent.alias}"
    if isinstance(parent, exp.Subquery) and parent.alias:
        return f"subquery {parent.alias}"
    if parent is None:
        return "top-level SELECT"
    return "SELECT"


def _format_leaks(leaks: tuple[ScopeLeak, ...], sql: str) -> str:
    header = f"Scope not closed — {len(leaks)} out-of-scope reference(s):"
    body = [
        f"  - [{leak.kind}] {leak.reference} in {leak.scope} "
        f"(bound sources: {list(leak.bound_sources)})"
        for leak in leaks
    ]
    return "\n".join([header, *body, f"SQL:\n{sql}"])
