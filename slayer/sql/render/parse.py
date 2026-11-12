"""The one parse door for free SQL text: tables, scalar expressions, predicates and read-only queries."""

from __future__ import annotations

import re
from collections.abc import Collection, Sequence
from typing import Optional, TypeGuard

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expr, Expression
from sqlglot.tokenizer_core import TokenType

from slayer.core.errors import NotReadOnlyQueryError
from slayer.sql.dialects.base import SqlDialect, ascii_lower
from slayer.sql.naming import quote_mixed_case_identifiers
from slayer.sql.reserved_keywords import prequote_reserved_identifiers, prequote_reserved_identifiers_mapped

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


class FoldedSql(BaseModel):
    """Source SQL with its unquoted identifiers in SLayer's spelling."""

    text: str
    # Unquoted identifiers holding an upper-case letter, when there is no dialect to fold them with.
    undecidable: list[str] = []
    # The text did not parse, and an unquoted word in it holds an upper-case letter.
    unreadable: bool = False


def _has_upper(text: str) -> bool:
    return any(c.isupper() for c in text)


def _masked(sql: str, masks: Sequence[re.Pattern[str]]) -> str:
    """``sql`` with each mask match replaced by a quoted identifier of the same length."""
    for mask in masks:
        sql = mask.sub(lambda m: f'"{"_" * (len(m.group(0)) - 2)}"' if len(m.group(0)) > 2 else m.group(0), sql)
    return sql


def _parsed_for_fold(text: str, *, read: str, table: bool) -> tuple[Expr, int] | None:
    """``text``'s parse tree and the offset its spans are shifted by, or ``None`` when it does not parse."""
    if table:
        try:
            found = sqlglot.parse_one(text, read=read, into=exp.Table)
        except SqlglotError:
            return None
        return (found, 0) if isinstance(found, exp.Table) and not found.alias else None
    try:
        tree = sqlglot.parse_one(text, read=read)
        if not isinstance(tree, exp.Command):
            return tree, 0
    except SqlglotError:
        pass
    try:
        return sqlglot.parse_one(f"SELECT {text}", read=read), len("SELECT ")
    except SqlglotError:
        return None


def _mixed_case_words(text: str, *, read: str) -> list[str]:
    """Unquoted words of ``text`` holding an upper-case letter, by token scan."""
    try:
        tokens = sqlglot.tokenize(text, dialect=read)
    except Exception:  # noqa: BLE001 — an unlexable text may hold any word
        keywords = Dialect.get_or_raise(read).tokenizer_class.KEYWORDS
        return [w for w in re.findall(r"[^\W\d]\w*", text) if _has_upper(w) and w.upper() not in keywords]
    return [t.text for t in tokens if t.token_type is TokenType.VAR and _has_upper(t.text)]


def stored_identifier(word: str, *, dialect: SqlDialect) -> str:
    """The spelling SLayer reads as the name the database folds unquoted ``word`` to."""
    return ascii_lower(word) if dialect.identifier_folding == "lower" else word.lower()


def _qualified_by(table: exp.Identifier, qualifiers: Collection[str]) -> bool:
    """Whether ``table`` is one of ``qualifiers`` (an unquoted one matching in any case)."""
    return table.name in qualifiers if table.quoted else table.name.lower() in {q.lower() for q in qualifiers}


def _logical_identifiers(tree: Expr, qualifiers: Collection[str], kept: Collection[str] = ()) -> set[int]:
    """Ids of the identifiers of columns qualified by one of ``qualifiers``, and of each ``kept`` qualifier."""
    out: set[int] = set()
    for col in tree.find_all(exp.Column):
        table = col.args.get("table")
        if not isinstance(table, exp.Identifier):
            continue
        if _qualified_by(table, qualifiers):
            out.update(id(i) for i in col.find_all(exp.Identifier))
        elif _qualified_by(table, kept):
            out.add(id(table))
    return out


def _fold_span(node: Expr, *, shift: int, origin: list[int | None], text: str) -> tuple[int, int, str] | None:
    """``(start, end, name)`` in the original text of an unquoted identifier or user-function name, else ``None``."""
    start, end = node.meta.get("start"), node.meta.get("end")
    if start is None or end is None or not isinstance(node.this, str):
        return None
    start, end = start - shift, end - shift
    if isinstance(node, exp.Identifier) and node.quoted:
        if origin[start] is not None:
            return None  # quoted in the source
        start, end = start + 1, end - 1  # quoted only so a reserved word parses
    first, last = origin[start], origin[end]
    if first is not None and last is not None and text[first:last + 1] == node.this:
        return first, last, node.this
    return None


def _fold_spans(tree: Expr, *, shift: int, origin: list[int | None], text: str,
                logical: set[int]) -> list[tuple[int, int, str]]:
    """``(start, end, name)`` in the original text of each unquoted identifier and user-function name."""
    spans = (_fold_span(node, shift=shift, origin=origin, text=text) for node in tree.walk()
             if isinstance(node, (exp.Identifier, exp.Anonymous)) and id(node) not in logical)
    return sorted({span for span in spans if span is not None})


def fold_source_identifiers(
    sql: str, *, dialect: SqlDialect | None, masks: Sequence[re.Pattern[str]] = (), read: str | None = None,
    table: bool = False, logical_qualifiers: Collection[str] = (), kept_qualifiers: Collection[str] = (),
) -> FoldedSql:
    """``sql`` with each unquoted identifier read by ``dialect``'s folding, the rest byte-identical.

    ``masks`` match logical references kept as written; ``read`` is the sqlglot dialect to parse with (default:
    ``dialect``'s); ``table`` reads ``sql`` as a table name; columns qualified by ``logical_qualifiers`` are kept, and
    a ``kept_qualifiers`` qualifier is kept while its column folds.
    ``dialect=None`` folds nothing and reports the identifiers whose meaning depends on it."""
    if dialect is not None and dialect.identifier_folding == "none":
        return FoldedSql(text=sql)
    masked = _masked(sql, masks)
    if not _has_upper(masked):
        return FoldedSql(text=sql)
    read = read or (dialect.sqlglot_name if dialect is not None else "postgres")
    prequoted, origin = prequote_reserved_identifiers_mapped(masked, dialect=read)
    parsed = _parsed_for_fold(prequoted, read=read, table=table)
    if parsed is None:
        words = _mixed_case_words(masked, read=read)
        if dialect is None:
            return FoldedSql(text=sql, undecidable=list(dict.fromkeys(words)))
        return FoldedSql(text=sql, unreadable=bool(words))
    tree, shift = parsed
    spans = [s for s in _fold_spans(tree, shift=shift, origin=origin, text=masked,
                                    logical=_logical_identifiers(tree, logical_qualifiers, kept_qualifiers))
             if _has_upper(s[2])]
    if dialect is None:
        return FoldedSql(text=sql, undecidable=list(dict.fromkeys(name for _, _, name in spans)))
    for start, end, name in reversed(spans):
        sql = sql[:start] + stored_identifier(name, dialect=dialect) + sql[end + 1:]
    return FoldedSql(text=sql)


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
