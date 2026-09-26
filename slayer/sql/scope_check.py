"""DEV-1705 Stage 1 — mechanical scope-closure validator.

``assert_scope_closed(sql)`` walks every SELECT scope of a statement and checks
two closure laws (DEV-1703 Law-2):

* **C1 — binding.** Every table qualifier a column references must bind to that
  scope's own ``FROM`` / ``JOIN`` sources (a physical table, a CTE, or a derived
  table). An unbound qualifier is an out-of-scope leak.
* **C2 — projection.** A reference that resolves into another SELECT scope
  (a CTE or derived table) must name a column that scope *projects*. A plain
  star projection (``*`` / ``rel.*``, optionally with ``REPLACE``) exports every
  name and is allowed; a ``* EXCEPT (c)`` star does not export ``c``.

The validator is deliberately **sound on the generator's corpus**: it only ever
raises on a *provable* leak. Unqualified / ambiguous references (no schema to
resolve them) and a physical table's column names (no catalog) are treated as
unverifiable and never flagged — there are no false positives.

Pre-RLS by default. ``allow_rls_correlation=True`` whitelists the one correlated
reference the session-policy transform (``slayer/sql/session_policy.py``)
intentionally injects post-generation: a ``_rls_src``-qualified column inside a
forced-filter ``EXISTS``. Dialect-aware via sqlglot.

Intended to run on **post-mangle, pre-RLS** generator output (dialect alias
mangling collapses BigQuery/T-SQL dotted aliases to ``___`` first — pre-mangle
those dotted refs parse as ``table.column`` and would false-flag; mangling is
identity for other dialects). DEV-1713 finalised the BigQuery naming/mangling,
which closed the last dotted-alias shape that made sqlglot raise ``TypeError``
parsing BigQuery output — the prior ``_SQLGLOT_TYPEERROR_DIALECTS`` carve-out is
therefore removed, and BigQuery output is now fully validated like every other
dialect. Setting ``SLAYER_VALIDATE_SCOPES=1`` makes the generator call
``maybe_validate_scopes`` on every emitted statement (runtime debugging); the
test harness enables it suite-wide.
"""

from __future__ import annotations

import os
from typing import Dict, Optional

import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, ScopeType, traverse_scope

from slayer.sql.naming import assert_unique_cte_names, dialect_folds_case

# The session-policy transform's correlated-EXISTS source alias (see
# ``session_policy._RLS_SRC``); the only legal correlated ref in post-RLS SQL.
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
    """A CTE reads a sibling declared LATER in the same ``WITH`` (a forward
    reference: SQLite tolerates it, DuckDB and others reject it)."""


def assert_dependency_ordered_ctes(sql: str, *, dialect: str = "postgres") -> None:
    """Raise :class:`CteOrderError` if any CTE reads a sibling declared later in
    its enclosing ``WITH`` — the emission-order backstop for sql.arc42 P6.

    A table reference flags only when it carries no catalog/schema qualifier and
    matches a CTE the NEAREST enclosing ``WITH`` (defining that name) declares
    strictly AFTER the CTE containing the reference. Name folding matches the
    allocator (``slayer/sql/naming.py``): ``str.lower()`` on a case-insensitive
    dialect (BigQuery et al.), identity on a case-sensitive one (ClickHouse keeps
    ``Foo``/``foo`` distinct); ``casefold`` is avoided (it over-equates ``ß``/``ss``).
    It validates SLayer's own emitted SQL — unquoted, allocator-folded names — so
    it does not model per-dialect quoted-identifier case rules.
    Physical tables, schema-qualified names, derived-table aliases and
    nested-``WITH`` scopes are left alone; a reference from the main query body is
    always backward. A self reference (position == containing) is NOT flagged:
    without full scope resolution it is indistinguishable from a non-recursive CTE
    shadowing a physical table of the same name (a legal pattern), so — like
    :func:`assert_scope_closed` — this backstop stays sound on the corpus (no false
    positives)."""
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
    """The nearest ancestor ``WITH`` that declares a CTE named ``name`` (already
    ``fold``-normalised), with its position map; ``None`` when none defines it."""
    node = tbl.parent
    while node is not None:
        if isinstance(node, exp.With):
            positions = _cte_positions(with_node=node, fold=fold)
            if name in positions:
                return node, positions
        node = node.parent
    return None


def _containing_cte_index(*, tbl: exp.Table, with_node: exp.With) -> Optional[int]:
    """Index of the ``with_node`` CTE whose body contains ``tbl``, or ``None`` when
    ``tbl`` sits in the query the WITH decorates rather than in a CTE body."""
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
    """Analyse ``sql`` and return a :class:`ScopeCheckResult` (never raises for
    a leak — inspect ``.leaks``). Parsing is dialect-aware."""
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
    """Validate the emitted ``sql`` iff ``SLAYER_VALIDATE_SCOPES`` is truthy.

    Runs two checks on the final generator output: scope closure
    (:func:`assert_scope_closed`) and, as the DEV-1692 belt, per-``WITH``-scope
    CTE-name uniqueness (:func:`slayer.sql.naming.assert_unique_cte_names`). The
    env flag is read on every call so the test harness (autouse) and runtime
    debugging can toggle it after import; production pays nothing.
    """
    value = os.environ.get(_ENV_FLAG)
    if value is not None and value.strip().lower() in _TRUTHY:
        assert_scope_closed(sql=sql, dialect=dialect)
        # DEV-1692 belt: per-WITH-scope CTE-name uniqueness on the final output.
        assert_unique_cte_names(sql=sql, dialect=dialect)
        # DEV-1942 belt: no CTE reads a sibling declared later (sql P6).
        assert_dependency_ordered_ctes(sql=sql, dialect=dialect)


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #
def _scope_leaks(scope: Scope, *, allow_rls_correlation: bool) -> list[ScopeLeak]:
    leaks: list[ScopeLeak] = []
    label = _scope_label(scope)
    bound = tuple(scope.sources.keys())
    for col in scope.columns:
        qualifier = col.table
        if not qualifier:
            # Unqualified / ambiguous — unverifiable without schema. Never a
            # false positive (J1 = sound-on-corpus).
            continue
        source = _resolve_source(scope=scope, qualifier=qualifier)
        if source is None:
            # An expression subquery (EXISTS / IN / scalar) may legally
            # correlate to an ancestor scope's sources (DEV-1840); derived
            # tables and CTEs may not.
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
        # else: a physical ``exp.Table`` — qualifier bound; the column name is
        # unverifiable without a catalog, so it is not flagged.
    return leaks


def _resolve_source(scope: Scope, qualifier: str):
    """Look up ``qualifier`` among the scope's sources, case-insensitively
    (so quoted / mixed-case identifiers resolve against sqlglot's own key)."""
    source = scope.sources.get(qualifier)
    if source is not None:
        return source
    folded = qualifier.casefold()
    for name, src in scope.sources.items():
        if name.casefold() == folded:
            return src
    return None


def _resolve_correlated(scope: Scope, qualifier: str):
    """Resolve ``qualifier`` against ancestor scopes, crossing only expression-
    subquery boundaries — the one place SQL permits correlation. A derived table
    sees nothing of its own parent (sibling FROM items need LATERAL) but, nested
    in an expression subquery, correlates to that subquery's ancestors."""
    current = scope
    while current.parent is not None:
        kind = current.scope_type
        current = current.parent
        if kind == ScopeType.SUBQUERY:
            source = _resolve_source(scope=current, qualifier=qualifier)
            if source is not None:
                return source
        elif kind != ScopeType.DERIVED_TABLE:
            break
    return None


def _projects(inner: Scope, name: str) -> bool:
    """Whether the inner SELECT scope projects a column named ``name``.

    Set-operation scopes expose their first leg's output names (positional).
    A plain / ``REPLACE`` star exports every name; a ``* EXCEPT (name)`` star
    does not export ``name``.
    """
    folded = name.casefold()
    expr = inner.expression
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
            # sqlglot keys the EXCEPT/EXCLUDE list under ``except_``. REPLACE /
            # RENAME keep every name, so only EXCEPT drops one.
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
