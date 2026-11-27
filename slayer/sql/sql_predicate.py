"""SQL-mode predicate validator for dataset-side filters.

``DatasetField.filter`` and each entry of ``SlayerDataset.filters`` are SQL-mode
expressions: arbitrary SQL function calls and operators are accepted
(``json_extract``, ``coalesce``, ``CASE WHEN``, dialect-specific operators
like Postgres ``@>`` / ``ILIKE ANY``, MySQL ``<=>``); SLayer DSL
constructs (aggregation references, transform calls, raw ``OVER (...)``)
are rejected with a clear actionable error.

This validator does **not** invoke sqlglot. It only
checks that no DSL construct is present. Full sqlglot parsing — with
the appropriate ``read=<dialect>`` — happens at SQL generation time
where the dataset's datasource (and therefore its dialect) is known.
This avoids the false-rejection failure mode where a Postgres-specific
operator like ``payload @> '{"k":1}'`` fails sqlglot's generic grammar
at construction time even though it's perfectly valid against the
dataset's actual backend.
"""
from __future__ import annotations

import re

from pydantic import BaseModel, Field

import slayer.core.enums


class ParsedFilter(BaseModel):
    """A validated SQL-mode predicate ready for SQL generation."""
    sql: str = Field(description="SQL WHERE condition, e.g. \"status = 'completed'\"")
    fields: list[str] = Field(description="Field names referenced in the filter")


_STRING_LITERAL_RE = re.compile(r"'(?:[^'\\]|\\.)*'")
# A Mode-B colon aggregation (``revenue:sum``, ``*:count``); never a ``::`` cast or a ``:name`` bind.
_COLON_AGG_RE = re.compile(r"(?<![\w.*:])(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?):(?!:)[a-zA-Z_]\w*")

_DSL_TRANSFORM_CALL_RE = re.compile(
    r"\b(" + "|".join(sorted(slayer.core.enums.ALL_TRANSFORMS, key=len, reverse=True)) + r")\s*\(",
    re.IGNORECASE,
)


def _strip_string_literals(expression: str) -> str:
    """Return ``expression`` with every string literal replaced by ``''`` so
    further regex scans don't false-match identifiers inside literals."""
    return _STRING_LITERAL_RE.sub("''", expression)


def _reject_dsl_constructs(expression: str) -> None:
    """Raise if the SQL-mode predicate contains a SLayer DSL construct."""
    stripped = _strip_string_literals(expression)
    agg_match = _COLON_AGG_RE.search(stripped)
    if agg_match is not None:
        raise ValueError(
            f"SQL-mode filter cannot contain a SLayer aggregation reference "
            f"({agg_match.group(0)!r}). Aggregations are a DSL construct — "
            f"put them in a query filter (`SlayerQuery.filter`) or in a "
            f"`DatasetMetric.expression`. The filter was: {expression!r}"
        )
    tx_match = next((m for m in _DSL_TRANSFORM_CALL_RE.finditer(stripped) if not _windowed_call(stripped, m.end())),
                    None)
    if tx_match is not None:
        raise ValueError(
            f"SQL-mode filter cannot contain SLayer transform calls "
            f"({tx_match.group(1)!r}). Transforms are a DSL construct — "
            f"put them in a query filter (`SlayerQuery.filter`) or in a "
            f"`DatasetMetric.expression`. The filter was: {expression!r}"
        )


_OVER_RE = re.compile(r"\s*over\s*\(", re.IGNORECASE)


def _windowed_call(text: str, opened: int) -> bool:
    """Whether the call whose ``(`` ends at ``opened`` is followed by ``OVER (``: a SQL window, not a transform."""
    depth = 1
    for i in range(opened, len(text)):
        depth += (text[i] == "(") - (text[i] == ")")
        if depth == 0:
            return _OVER_RE.match(text, i + 1) is not None
    return False


def _bare_field_refs(expression: str) -> list[str]:
    """Best-effort extraction of field-like identifiers from a SQL-mode
    predicate. The render-time relationship-path detection use was superseded by structured
    extraction (``reference_closure``) and the Mode-A door; this now serves
    only VALIDATION — the dataset-filter metric rejection in
    ``stage_planner`` and the ``schema_drift`` check. Skips string literals,
    then walks tokens that look like identifiers
    (``customers__regions.name`` → ``customers__regions.name``,
    ``status`` → ``status``) and excludes function-call heads.
    """
    stripped = _strip_string_literals(expression)
    refs: list[str] = []
    for match in re.finditer(r"\b[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*\b", stripped):
        token = match.group(0)
        # Skip if immediately followed by `(` (function call)
        end = match.end()
        if end < len(stripped) and stripped[end] == "(":
            continue
        # Skip SQL keywords that aren't field references
        if token.upper() in _SQL_KEYWORDS:
            continue
        refs.append(token)
    return refs


_SQL_KEYWORDS = frozenset({
    "AND", "OR", "NOT", "IS", "NULL", "IN", "LIKE", "ILIKE",
    "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "ANY", "ALL",
    "TRUE", "FALSE", "ARRAY",
})


def parse_sql_predicate(expression: str) -> ParsedFilter:
    """Validate a SQL-mode predicate string and return a :class:`ParsedFilter`.

    Pre-rejects DSL constructs (aggregation references, transform calls). Does NOT invoke sqlglot —
    full dialect-aware SQL parsing happens at generation time.

    The ``ParsedFilter.sql`` field is the original ``expression`` unchanged;
    ``fields`` is a best-effort regex extraction of field-shaped
    identifiers, used only for validation (see :func:`_bare_field_refs`).
    """
    _reject_dsl_constructs(expression)
    return ParsedFilter(sql=expression, fields=_bare_field_refs(expression))
