"""SQL-mode predicate validator for model-side filters (DEV-1369).

``Column.filter`` and each entry of ``SlayerModel.filters`` are SQL-mode
expressions: arbitrary SQL function calls and operators are accepted
(``json_extract``, ``coalesce``, ``CASE WHEN``, dialect-specific operators
like Postgres ``@>`` / ``ILIKE ANY``, MySQL ``<=>``); SLayer DSL
constructs (aggregation colon syntax, transform calls, raw ``OVER (...)``)
are rejected with a clear actionable error.

DEV-1369 round 2: this validator does **not** invoke sqlglot. It only
checks that no DSL construct is present. Full sqlglot parsing — with
the appropriate ``read=<dialect>`` — happens at SQL generation time
where the model's datasource (and therefore its dialect) is known.
This avoids the false-rejection failure mode where a Postgres-specific
operator like ``payload @> '{"k":1}'`` fails sqlglot's generic grammar
at construction time even though it's perfectly valid against the
model's actual backend.
"""
from __future__ import annotations

import re

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


def _bare_column_refs(formula: str) -> list[str]:
    """Best-effort extraction of column-like identifiers from a SQL-mode
    predicate, used so downstream join-detection sees the same shape it
    saw before DEV-1369 round 2 dropped sqlglot from this path. Skips
    string literals, then walks tokens that look like identifiers
    (``customers__regions.name`` → ``customers__regions.name``,
    ``status`` → ``status``) and excludes function-call heads.
    """
    stripped = _strip_string_literals(formula)
    refs: list[str] = []
    for match in re.finditer(r"\b[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*\b", stripped):
        token = match.group(0)
        # Skip if immediately followed by `(` (function call)
        end = match.end()
        if end < len(stripped) and stripped[end] == "(":
            continue
        # Skip SQL keywords that aren't column references
        if token.upper() in _SQL_KEYWORDS:
            continue
        refs.append(token)
    return refs


_SQL_KEYWORDS = frozenset({
    "AND", "OR", "NOT", "IS", "NULL", "IN", "LIKE", "ILIKE",
    "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "ANY", "ALL",
    "TRUE", "FALSE", "ARRAY",
})


def parse_sql_predicate(formula: str) -> ParsedFilter:
    """Validate a SQL-mode predicate string and return a :class:`ParsedFilter`.

    Pre-rejects DSL constructs (aggregation colon, transform calls) and
    raw ``OVER (...)`` window-function syntax. Does NOT invoke sqlglot —
    full dialect-aware SQL parsing happens at generation time.

    The ``ParsedFilter.sql`` field is the original ``formula`` unchanged;
    ``columns`` is a best-effort regex extraction of column-shaped
    identifiers (used by downstream join-detection on the strict path).
    """
    if has_window_function(formula):
        raise ValueError(f"Filter '{formula}' {WINDOW_IN_FILTER_ERROR}")
    _reject_dsl_constructs(formula)
    return ParsedFilter(sql=formula, columns=_bare_column_refs(formula))
