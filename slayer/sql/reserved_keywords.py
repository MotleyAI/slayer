"""DEV-1686: quote SQL reserved words used as identifiers.

sqlglot's per-dialect ``Generator.RESERVED_KEYWORDS`` is empty for Postgres,
T-SQL, SQLite, ClickHouse, Snowflake, Databricks, Spark, and Oracle, so a
SLayer model / column / alias named after a reserved word (``grant``, ``order``,
``user``, ``select``, ...) is emitted BARE and produces syntactically-invalid
SQL (``syntax error at or near "grant"``). Table *names* are quoted; table
*aliases* and *qualifiers* were not.

This module is the single source of truth for the reserved-word set, consumed by
two mechanisms:

1. :func:`install_reserved_keywords` unions the set into every dialect generator
   SLayer targets, so sqlglot's ``identifier_sql`` quotes reserved-word
   identifiers built as AST (base FROM alias + qualifiers, cross-model CTEs,
   physical names) at emit time.
2. :func:`prequote_reserved_identifiers` token-quotes reserved qualifiers/leaves
   in a SLayer-generated string *before* it is re-parsed (``join_cond``,
   ``measure.filter_sql``, qualified WHERE, the first/last ranked subquery, and
   the pre-generator ``Column.sql`` parses in ``column_expansion``). Emit-time
   quoting cannot help there because a bare reserved word fails at *parse* time.

NOTE: :func:`install_reserved_keywords` mutates sqlglot ``Generator`` classes
process-globally. This is deliberate and idempotent; the only observable effect
on unrelated in-process sqlglot use is strictly-more-correct quoting.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

# ANSI SQL:2016 + Postgres reserved words (lowercase). Curated to the
# "cannot be a bare identifier" set. Type-ish NON-reserved words that show up as
# real column names (date, time, timestamp, name, value, count, sum, id, text,
# number, ...) are intentionally EXCLUDED so we never quote a common column.
SLAYER_RESERVED_KEYWORDS: frozenset[str] = frozenset({
    "all", "alter", "analyse", "analyze", "and", "any", "array", "as", "asc",
    "asymmetric", "authorization", "between", "binary", "both", "case", "cast",
    "check", "collate", "collation", "column", "concurrently", "constraint",
    "create", "cross", "current_catalog", "current_date", "current_role",
    "current_time", "current_timestamp", "current_user", "default",
    "deferrable", "desc", "distinct", "do", "drop", "else", "end", "except",
    "false", "fetch", "for", "foreign", "freeze", "from", "full", "glob",
    "grant", "group", "having", "ilike", "in", "initially", "inner", "insert",
    "intersect", "into", "is", "isnull", "join", "lateral", "leading", "left",
    "like", "limit", "localtime", "localtimestamp", "natural", "not", "notnull",
    "null", "offset", "on", "only", "or", "order", "out", "outer", "overlaps",
    "partitioned_by", "placing", "primary", "qualify", "references", "regexp",
    "returning", "revoke", "right", "rlike", "rollback", "select",
    "session_user", "similar", "some", "symmetric", "table", "tablesample",
    "then", "to", "trailing", "true", "uncache", "union", "unique", "user",
    "using", "variadic", "verbose", "when", "where", "window", "with", "xor",
})


def install_reserved_keywords() -> None:
    """Idempotently union :data:`SLAYER_RESERVED_KEYWORDS` into the
    ``RESERVED_KEYWORDS`` of every generator SLayer targets.

    Assigns a FRESH set per generator class so we never mutate sqlglot's shared
    base empty-set singleton (Postgres / T-SQL / SQLite / ... all inherit the
    same ``Generator.RESERVED_KEYWORDS`` object). Each dialect keeps its own
    native reserved words (union, not replace).
    """
    from sqlglot.dialects.dialect import Dialect

    from slayer.sql.dialects import _ALL_DIALECTS

    for d in _ALL_DIALECTS:
        gen_cls = Dialect.get_or_raise(d.sqlglot_name).generator_class
        gen_cls.RESERVED_KEYWORDS = set(gen_cls.RESERVED_KEYWORDS) | SLAYER_RESERVED_KEYWORDS


def prequote_reserved_identifiers(sql: str, *, dialect: str) -> str:
    """Quote reserved-word identifiers sitting in QUALIFIER (``word.``) or LEAF
    (``.word``) position so a generated string embedding a bare reserved
    qualifier/leaf parses.

    Token-based (via ``sqlglot.tokenize``) so it is literal/comment/quoted-ident
    safe: a reserved word inside ``'...'`` / ``E'...'`` / ``$$...$$`` / ``--`` /
    ``/* */`` / an already-quoted identifier is a distinct token type and is
    never rewritten. Quotes for the PARSE ``dialect`` (some callers parse as
    postgres while the target is mysql/tsql/bigquery), and the resulting quoted
    identifier re-emits with the target dialect's quote char downstream.

    Does NOT mutate stored metadata — callers pass a copy of the SQL string, so
    metadata scans (e.g. ``_window_referenced_aliases`` over
    ``measure.filter_sql``) keep seeing the original unquoted text.
    """
    try:
        toks = sqlglot.tokenize(sql, dialect=dialect)
    except Exception:  # unsupported lexer construct — leave unchanged
        return sql
    edits: list[tuple[int, int, str]] = []
    for i, tok in enumerate(toks):
        if tok.text.lower() not in SLAYER_RESERVED_KEYWORDS:
            continue
        # Defensive: offsets must map back to the original text (Token.end is
        # inclusive). Skip anything that doesn't round-trip cleanly.
        if sql[tok.start:tok.end + 1] != tok.text:
            continue
        prev_tok = toks[i - 1] if i else None
        next_tok = toks[i + 1] if i + 1 < len(toks) else None
        adj_dot = (
            (prev_tok is not None and prev_tok.token_type == TokenType.DOT)
            or (next_tok is not None and next_tok.token_type == TokenType.DOT)
        )
        if not adj_dot:
            continue
        quoted = exp.Identifier(this=tok.text, quoted=True).sql(dialect=dialect)
        edits.append((tok.start, tok.end, quoted))
    for start, end, replacement in sorted(edits, reverse=True):
        sql = sql[:start] + replacement + sql[end + 1:]
    return sql
