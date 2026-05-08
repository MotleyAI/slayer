"""SQL-mode predicate parser for model-side filters (DEV-1369).

``Column.filter`` and each entry of ``SlayerModel.filters`` are SQL-mode
expressions: arbitrary SQL function calls and operators are accepted
(``json_extract``, ``coalesce``, ``CASE WHEN``, …); SLayer DSL
constructs (aggregation colon syntax, transform calls, raw ``OVER (...)``)
are rejected with a clear actionable error.

The parser is sqlglot-based so the full SQL grammar (including ``CASE
WHEN``, ``BETWEEN``, ``EXISTS``, every dialect's function library) is
available without us re-implementing it. Python-style operator
spellings ``==`` / ``!=`` are pre-rewritten to ``=`` / ``<>`` for
backward compat with existing fixtures.
"""
from __future__ import annotations

import re
from typing import List

import sqlglot
from sqlglot import exp

from slayer.core.formula import ALL_TRANSFORMS, ParsedFilter
from slayer.core.refs import AGG_REF_RE
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function

_STRING_LITERAL_RE = re.compile(r"'(?:[^'\\]|\\.)*'")

_DSL_TRANSFORM_CALL_RE = re.compile(
    r"\b(" + "|".join(sorted(ALL_TRANSFORMS, key=len, reverse=True)) + r")\s*\(",
    re.IGNORECASE,
)


def _strip_string_literals(formula: str) -> str:
    """Return ``formula`` with every string literal replaced by ``''`` so
    further regex scans don't false-match identifiers inside literals."""
    return _STRING_LITERAL_RE.sub("''", formula)


def _reject_dsl_constructs(formula: str) -> None:
    """Raise if the SQL-mode predicate contains a SLayer DSL construct."""
    stripped = _strip_string_literals(formula)
    agg_match = AGG_REF_RE.search(stripped)
    if agg_match is not None:
        raise ValueError(
            f"SQL-mode filter cannot contain SLayer aggregation colon syntax "
            f"({agg_match.group(0)!r}). Aggregations are a DSL construct — "
            f"put them in a query filter (`SlayerQuery.filters`) or in a "
            f"`ModelMeasure.formula`. The filter was: {formula!r}"
        )
    tx_match = _DSL_TRANSFORM_CALL_RE.search(stripped)
    if tx_match is not None:
        raise ValueError(
            f"SQL-mode filter cannot contain SLayer transform calls "
            f"({tx_match.group(1)!r}). Transforms are a DSL construct — "
            f"put them in a query filter (`SlayerQuery.filters`) or in a "
            f"`ModelMeasure.formula`. The filter was: {formula!r}"
        )


def _normalize_operators(formula: str) -> str:
    """Rewrite Python-style operators and string-literal escapes outside
    string literals (operators) and inside (escapes) so sqlglot's
    SQL-standard tokenizer accepts the result.

    Outside literals: ``==`` → ``=`` and ``!=`` → ``<>`` (backward compat
    for existing fixtures that use Python operator spellings).

    Inside literals: ``\\'`` → ``''`` and ``\\\\`` → ``\\`` (backward
    compat for fixtures that authored string contents with Python-style
    backslash escapes; the canonical SQL spelling is ``''``).
    """
    parts = _STRING_LITERAL_RE.split(formula)
    literals = _STRING_LITERAL_RE.findall(formula)
    rewritten = []
    for i, part in enumerate(parts):
        # `==` → `=`. Negative lookbehind/ahead avoids touching `>=` / `<=` / `!=`.
        part = re.sub(r"(?<![<>=!])==(?!=)", "=", part)
        # `!=` → `<>`. SQL standard inequality. Plain literal — no regex needed.
        part = part.replace("!=", "<>")
        rewritten.append(part)
        if i < len(literals):
            literal = literals[i]
            # Convert Python-style escapes in the literal body to SQL-standard.
            inner = literal[1:-1]
            inner = inner.replace("\\\\", "\x00").replace("\\'", "''").replace("\x00", "\\")
            rewritten.append(f"'{inner}'")
    return "".join(rewritten)


def _collect_columns(expression: exp.Expression) -> List[str]:
    """Walk a sqlglot AST and return every column reference in source order.

    A column with a table qualifier (``customers__regions.name``) is
    returned as the dotted form so downstream join-detection can pick
    it up; bare names (``status``) are returned as-is.
    """
    cols: List[str] = []
    for node in expression.walk():
        if isinstance(node, exp.Column):
            table_alias = node.table
            col_name = node.name
            if table_alias:
                cols.append(f"{table_alias}.{col_name}")
            else:
                cols.append(col_name)
    return cols


def parse_sql_predicate(formula: str) -> ParsedFilter:
    """Parse a SQL-mode predicate string into a :class:`ParsedFilter`.

    Pre-rejects DSL constructs (aggregation colon, transform calls) and
    raw ``OVER (...)`` window-function syntax, then parses with sqlglot.
    Returns a :class:`ParsedFilter` whose ``sql`` is the canonicalised
    predicate (sqlglot-emitted) and whose ``columns`` is the list of
    referenced column identifiers.
    """
    if has_window_function(formula):
        raise ValueError(f"Filter '{formula}' {WINDOW_IN_FILTER_ERROR}")
    _reject_dsl_constructs(formula)
    normalized = _normalize_operators(formula)
    try:
        parsed = sqlglot.parse_one(normalized, into=exp.Condition)
    except sqlglot.errors.ParseError as e:  # type: ignore[attr-defined]
        raise ValueError(f"Invalid filter syntax: {formula!r} — {e}")
    columns = _collect_columns(parsed)
    return ParsedFilter(sql=parsed.sql(), columns=columns)
