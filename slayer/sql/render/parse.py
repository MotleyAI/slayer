"""The one parse door for free SQL text: tables, scalar expressions, predicates and read-only queries."""

from __future__ import annotations

from typing import Optional, TypeGuard

import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.errors import NotReadOnlyQueryError
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.naming import quote_mixed_case_identifiers
from slayer.sql.reserved_keywords import prequote_reserved_identifiers

# A read-only source parses to a query root with no DML/DDL nested (a data-modifying CTE hides DML under a SELECT).
_READ_ONLY_ROOTS = (
    exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Subquery, exp.Values,
)
_DATA_MODIFYING_NODES = (
    exp.Insert, exp.Update, exp.Delete, exp.Merge,
    exp.Create, exp.Drop, exp.Alter, exp.TruncateTable,
    exp.Into,  # SELECT ... INTO creates a table
    exp.Lock,  # SELECT ... FOR UPDATE / FOR SHARE
    exp.NextValueFor,  # advances sequence state
)


def rewrite_log_aliases(node: Expression, *, dialect: SqlDialect) -> Expression:
    """``Log(10|2, X)`` back to ``log10(X)`` / ``log2(X)`` where the dialect has the native alias."""
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
    tree: Expression,
    target_dialect: SqlDialect,
    parse_dialect: SqlDialect,
) -> Expression:
    """The SLayer normalisation every parsed fragment gets: read-side rewrite, log aliases,
    mixed-case quoting, emit-side rewrite."""
    tree = parse_dialect.rewrite_parsed_ast(tree)
    tree = tree.transform(
        lambda node: rewrite_log_aliases(node, dialect=target_dialect),
    )
    tree = tree.transform(quote_mixed_case_identifiers)
    return target_dialect.rewrite_target_ast(tree)


def _prequote(sql: str, *, parse_dialect: SqlDialect) -> str:
    """Quote reserved-word qualifiers/leaves (``grant.id`` → ``"grant".id``) so they parse."""
    return prequote_reserved_identifiers(sql, dialect=parse_dialect.sqlglot_name)


def _parse_in_expression_context(sql: str, *, parse_dialect: SqlDialect) -> Expression:
    """``sql`` read as an expression, never as a statement (``replace(...)`` on SQLite/MySQL)."""
    wrapped = sqlglot.parse_one(f"SELECT 1 WHERE {sql}", dialect=parse_dialect.sqlglot_name)
    where = wrapped.args.get("where")
    if where is None or where.this is None:  # pragma: no cover — defensive
        raise ValueError(
            f"Could not extract WHERE predicate from {sql!r} "
            f"(dialect={parse_dialect.sqlglot_name!r})"
        )
    return where.this


def parse_table(*, name: str, dialect: SqlDialect) -> exp.Table:
    """A (possibly qualified) table name read with ``dialect``'s identifier rules, mixed case quoted."""
    table = exp.to_table(name, dialect=dialect.sqlglot_name).transform(quote_mixed_case_identifiers)
    assert isinstance(table, exp.Table)
    return table


ReadOnlyRoot = exp.Select | exp.Union | exp.Except | exp.Intersect | exp.Subquery | exp.Values


def is_read_only_query(tree: Expr) -> TypeGuard[ReadOnlyRoot]:
    """Whether ``tree`` is a query root with nothing data-modifying nested."""
    return isinstance(tree, _READ_ONLY_ROOTS) and tree.find(*_DATA_MODIFYING_NODES) is None


def parse_query(
    *,
    sql: str,
    target_dialect: SqlDialect,
    parse_dialect: Optional[SqlDialect] = None,
) -> exp.Query:
    """A read-only query (comments dropped) with the SLayer AST rewrites; anything else raises."""
    parse_dialect = parse_dialect or target_dialect
    tree = sqlglot.parse_one(_prequote(sql, parse_dialect=parse_dialect), dialect=parse_dialect.sqlglot_name)
    if not is_read_only_query(tree):
        raise NotReadOnlyQueryError(sql=sql)
    for node in tree.walk():
        node.comments = None
    if isinstance(tree, exp.Values):
        tree = exp.select("*").from_(exp.alias_(tree, "_values", table=True))
    tree = apply_ast_rewrites(tree=tree, target_dialect=target_dialect, parse_dialect=parse_dialect)
    assert isinstance(tree, exp.Query)
    return tree


def parse_expression(
    *,
    sql: str,
    target_dialect: SqlDialect,
    parse_dialect: Optional[SqlDialect] = None,
    prequote: bool = True,
) -> Expression:
    """Parse a scalar SQL expression and apply the SLayer AST rewrites.

    ``prequote=False`` is for callers that already prequoted (the Mode-A door).
    """
    parse_dialect = parse_dialect or target_dialect
    if prequote:
        sql = _prequote(sql, parse_dialect=parse_dialect)
    tree = sqlglot.parse_one(sql, dialect=parse_dialect.sqlglot_name)
    if isinstance(tree, exp.Command):
        tree = _parse_in_expression_context(sql, parse_dialect=parse_dialect)
    return apply_ast_rewrites(
        tree=tree, target_dialect=target_dialect, parse_dialect=parse_dialect,
    )


def parse_predicate(
    *,
    sql: str,
    target_dialect: SqlDialect,
    parse_dialect: Optional[SqlDialect] = None,
    prequote: bool = True,
) -> Expression:
    """Parse a bare WHERE/HAVING predicate and apply the SLayer AST rewrites."""
    parse_dialect = parse_dialect or target_dialect
    if prequote:
        sql = _prequote(sql, parse_dialect=parse_dialect)
    return apply_ast_rewrites(
        tree=_parse_in_expression_context(sql, parse_dialect=parse_dialect),
        target_dialect=target_dialect,
        parse_dialect=parse_dialect,
    )
