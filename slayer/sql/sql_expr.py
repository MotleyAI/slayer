"""Stage 7a.4 (DEV-1450) — Mode-A sqlglot wrapper.

Single entry point for parsing Mode-A SQL into a structural-identity
``SqlExprKey`` plus uniform window-function detection. Consumed by the
binder (stage 7a.5) when constructing ``AggregateKey.column_filter_key``
and by the SQL generator (stage 7b) when canonicalising arbitrary
expressions.

Public surface:

* ``parse_sql_expr(text, *, dialect=None) -> SqlExprKey``
* ``canonicalize_sql(text, *, dialect=None) -> str``
* ``has_window_function(text) -> bool``
* ``assert_no_window_in_filter(text, *, source) -> None``

Dialect-specific rewrites:

* SQLite: ``json_extract(col, '$.path')`` is preserved as a function
  call (not rewritten to the ``->`` operator). The operator returns the
  JSON-quoted form, which silently breaks equality checks against
  bare-string literals; the function form returns the unquoted scalar.
  Lives in ``slayer/sql/sqlite_dialect.py``.

* ``log10(x)`` and ``log2(x)`` are preserved as written rather than
  canonicalised to ``LOG(10, x)`` / ``LOG(2, x)``. sqlglot's default
  normalises both into a generic ``Log(base, expression)`` node, which
  is correct numerically but breaks formula round-tripping for benchmark
  agents reading ``last_sql`` and trips dialects without a 2-arg
  ``LOG``. Allowlists mirror ``slayer/sql/generator.py``.

Dormant in 7a — no engine wiring. The binder is the first consumer.
"""

from __future__ import annotations

from typing import Optional

import sqlglot
from sqlglot import exp

from slayer.core.errors import IllegalWindowInFilterError
from slayer.core.keys import SqlExprKey
from slayer.sql.sqlite_dialect import rewrite_sqlite_json_extract
from slayer.sql.window_detect import has_window_function as _has_window_function

__all__ = [
    "assert_no_window_in_filter",
    "canonicalize_sql",
    "has_window_function",
    "parse_sql_expr",
]


# Dialect allowlists for log10 / log2 preservation. Mirrors
# ``slayer/sql/generator.py`` — keep in sync.
_LOG10_NATIVE_DIALECTS: frozenset[str] = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "snowflake", "bigquery", "redshift",
    "trino", "presto", "databricks", "spark", "tsql",
})
_LOG2_NATIVE_DIALECTS: frozenset[str] = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "bigquery", "trino", "presto", "databricks", "spark",
})


def _rewrite_log_aliases_for(
    node: exp.Expression, *, dialect: Optional[str],
) -> exp.Expression:
    """Restore ``log10(x)`` / ``log2(x)`` from sqlglot's generic
    ``Log(base, expression)`` for dialects with native single-arg aliases.

    No-op on non-``Log`` nodes and on ``Log`` nodes with non-literal or
    non-{10, 2} bases. Mirrors ``SQLGenerator._rewrite_log_aliases``.
    """
    if not isinstance(node, exp.Log):
        return node
    base = node.args.get("this")
    arg = node.args.get("expression")
    if arg is None or not isinstance(base, exp.Literal) or base.is_string:
        return node
    try:
        base_val = float(base.this)
    except (TypeError, ValueError):
        return node
    if base_val == 10 and dialect in _LOG10_NATIVE_DIALECTS:
        return exp.Anonymous(this="log10", expressions=[arg.copy()])
    if base_val == 2 and dialect in _LOG2_NATIVE_DIALECTS:
        return exp.Anonymous(this="log2", expressions=[arg.copy()])
    return node


def _parse_inner(text: str, *, dialect: Optional[str]) -> exp.Expression:
    """Parse ``text`` as a scalar expression / predicate.

    Wraps as ``SELECT (<text>) AS _`` before parsing so dialect-specific
    keyword conflicts (notably ``REPLACE`` on SQLite / MySQL, which
    sqlglot otherwise falls back to a ``Command`` node) resolve to
    function calls. The wrapper paren is stripped on extraction.

    Mirrors the precedent in ``slayer/sql/generator.py`` which uses
    ``SELECT 1 WHERE ...`` for predicate parsing; the ``SELECT (...) AS _``
    form handles both predicates and expressions uniformly.
    """
    wrapper = f"SELECT ({text}) AS __slayer_inner__"
    try:
        select = sqlglot.parse_one(wrapper, dialect=dialect)
    except Exception as e:
        raise ValueError(f"Invalid Mode-A SQL expression {text!r}: {e}")
    expressions = select.args.get("expressions") or []
    if not expressions:
        raise ValueError(
            f"Invalid Mode-A SQL expression {text!r}: parser returned "
            f"no select expression."
        )
    alias = expressions[0]
    inner = alias.this if isinstance(alias, exp.Alias) else alias
    if isinstance(inner, exp.Paren):
        inner = inner.this
    return inner


def parse_sql_expr(
    text: str, *, dialect: Optional[str] = None,
) -> SqlExprKey:
    """Parse a Mode-A SQL expression and return its structural-identity key.

    Two structurally-equal inputs (differing only in whitespace and
    casing of keywords) produce equal keys. Dialect-specific rewrites
    are applied so the canonical form matches what the generator will
    emit.
    """
    if not text or not text.strip():
        raise ValueError("Empty Mode-A SQL expression.")
    parsed = _parse_inner(text, dialect=dialect)

    if dialect == "sqlite":
        parsed = rewrite_sqlite_json_extract(parsed)

    parsed = parsed.transform(
        lambda n: _rewrite_log_aliases_for(n, dialect=dialect),
    )
    canonical = parsed.sql(dialect=dialect)
    return SqlExprKey(canonical_sql=canonical)


def canonicalize_sql(text: str, *, dialect: Optional[str] = None) -> str:
    """Return the canonical sqlglot form of ``text``.

    Equal to ``parse_sql_expr(text, dialect=dialect).canonical_sql`` by
    construction; exposed as a helper so callers that don't need the
    typed key (debug, logging) don't have to unpack one.
    """
    return parse_sql_expr(text, dialect=dialect).canonical_sql


def has_window_function(text: str) -> bool:
    """Return ``True`` if ``text`` contains a window function
    (``<func>(...) OVER (...)``).

    Thin re-export of ``slayer/sql/window_detect.py:has_window_function``.
    """
    return _has_window_function(text)


def assert_no_window_in_filter(text: str, *, source: str) -> None:
    """Raise ``IllegalWindowInFilterError`` if ``text`` contains a
    window function.

    ``source`` identifies the call site (e.g. ``"Column.filter on
    orders.foo"``); it's surfaced in the exception message so the
    binder can produce actionable errors without re-formatting.
    """
    if not text:
        return
    if _has_window_function(text):
        raise IllegalWindowInFilterError(
            filter_expr=text,
            source=source,
            suggestion=(
                "use a rank-family transform (rank, dense_rank, "
                "percent_rank, ntile) or move the windowed expression "
                "into an earlier multi-stage source_query."
            ),
        )
