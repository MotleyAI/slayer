"""One parse of free SQL text into a SLayer-normalised sqlglot AST.

``SQLGenerator._parse`` and ``SQLGenerator._parse_predicate`` were byte-for-byte
identical apart from a single line — how the text reaches sqlglot. Everything
after that (the dialect-keyed parse rewrite, the log-alias rewrite, mixed-case
identifier quoting, the dialect-keyed target rewrite) was duplicated. Both now
delegate here, so the Mode-A door on :class:`~slayer.sql.scope.ScopeFrame` and
the generator normalise a fragment exactly the same way — which is what lets the
door take over a call site without changing the SQL it emits.

The two surfaces differ ONLY in the parse step, and that difference is a
property of the SQL *grammar* being read, not a behaviour flag:

* :func:`parse_expression` — a scalar expression (``Column.sql``).
* :func:`parse_predicate` — a boolean predicate (``Column.filter``,
  ``SlayerModel.filters``). Wrapped as ``SELECT 1 WHERE ...`` so sqlglot reads a
  leading function name that is also a statement keyword (``replace(x, ',', '')``
  on SQLite/MySQL) as a function call rather than a statement.
"""

from __future__ import annotations

from typing import Optional

import sqlglot
from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect
from slayer.sql.naming import quote_mixed_case_identifiers
from slayer.sql.reserved_keywords import prequote_reserved_identifiers


def rewrite_log_aliases(node: exp.Expression, *, dialect: SqlDialect) -> exp.Expression:
    """DEV-1337: rewrite ``Log(this=Literal(10|2), expression=X)`` back to
    ``Anonymous(this='log10'|'log2', expressions=[X])`` for dialects with native
    single-arg aliases.

    Applied to every parsed AST so the rewrite survives sqlglot's re-parse
    passes, which would otherwise turn ``LOG10(x)`` back into a generic ``Log``
    node and re-emit it as ``LOG(10, x)``. No-op on non-``Log`` nodes and on
    ``Log`` nodes with a non-literal or non-{10,2} base.
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
    if base_val == 10 and dialect.should_use_native_log(10):
        return exp.Anonymous(this="log10", expressions=[arg.copy()])
    if base_val == 2 and dialect.should_use_native_log(2):
        return exp.Anonymous(this="log2", expressions=[arg.copy()])
    return node


def apply_ast_rewrites(
    *,
    tree: exp.Expression,
    target_dialect: SqlDialect,
    parse_dialect: SqlDialect,
) -> exp.Expression:
    """The SLayer normalisation every parsed fragment gets, in order.

    ``parse_dialect`` keys the rewrite that depends on how the text was READ
    (DEV-1716 — SQLite rewrites ``JSONExtract`` to the function-call form);
    ``target_dialect`` keys the log-alias rewrite and the emit-side rewrite
    (Postgres casts the first argument of a 2-arg ``ROUND``). They differ only
    when a caller parses one dialect's text for another dialect's output.
    """
    tree = parse_dialect.rewrite_parsed_ast(tree)
    tree = tree.transform(
        lambda node: rewrite_log_aliases(node, dialect=target_dialect),
    )
    # DEV-1645: quote mixed-case column/table identifiers so case-folding
    # dialects reach the right physical object.
    tree = tree.transform(quote_mixed_case_identifiers)
    return target_dialect.rewrite_target_ast(tree)


def _prequote(sql: str, *, parse_dialect: SqlDialect) -> str:
    """DEV-1686: quote reserved-word qualifiers/leaves (``grant.id`` →
    ``"grant".id``) before parsing, so a bare reserved word does not fail at
    parse time. No-op for ordinary SQL and idempotent when already quoted."""
    return prequote_reserved_identifiers(sql, dialect=parse_dialect.sqlglot_name)


def parse_expression(
    *,
    sql: str,
    target_dialect: SqlDialect,
    parse_dialect: Optional[SqlDialect] = None,
    prequote: bool = True,
) -> exp.Expression:
    """Parse a scalar SQL expression and apply the SLayer AST rewrites.

    ``prequote=False`` is for callers that have already prequoted and need the
    prequoted text kept as a distinct representation (the Mode-A door, which
    parses the prequoted form twice — once raw, once expanded).
    """
    parse_dialect = parse_dialect or target_dialect
    if prequote:
        sql = _prequote(sql, parse_dialect=parse_dialect)
    tree = sqlglot.parse_one(sql, dialect=parse_dialect.sqlglot_name)
    return apply_ast_rewrites(
        tree=tree, target_dialect=target_dialect, parse_dialect=parse_dialect,
    )


def parse_predicate(
    *,
    sql: str,
    target_dialect: SqlDialect,
    parse_dialect: Optional[SqlDialect] = None,
    prequote: bool = True,
) -> exp.Expression:
    """Parse a bare WHERE/HAVING predicate and apply the SLayer AST rewrites.

    ``sqlglot.parse_one`` falls back to a ``Command`` statement parse when an
    expression starts with a function name that is also a statement keyword in
    the target dialect. Wrapping in ``SELECT 1 WHERE ...`` puts sqlglot in
    expression context, where the same text reads as a function call.
    """
    parse_dialect = parse_dialect or target_dialect
    if prequote:
        sql = _prequote(sql, parse_dialect=parse_dialect)
    wrapped = sqlglot.parse_one(
        f"SELECT 1 WHERE {sql}", dialect=parse_dialect.sqlglot_name,
    )
    where = wrapped.args.get("where")
    if where is None or where.this is None:  # pragma: no cover — defensive
        raise ValueError(
            f"Could not extract WHERE predicate from {sql!r} "
            f"(dialect={parse_dialect.sqlglot_name!r})"
        )
    return apply_ast_rewrites(
        tree=where.this,
        target_dialect=target_dialect,
        parse_dialect=parse_dialect,
    )
