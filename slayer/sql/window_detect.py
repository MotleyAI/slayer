"""Detect SQL window functions: a raw ``OVER (...)`` in filter or metric text is refused, and a field whose native SQL
holds one is read as a column its dataset's relation materialises."""

import re

import sqlglot
import sqlglot.errors
from sqlglot import exp

_OVER_RE = re.compile(r"\bover\s*\(", re.IGNORECASE)


def has_window_function(sql: str) -> bool:
    """Return True if `sql` contains a window function call (`OVER (...)`).

    Uses a regex on the canonical `<func>(...) OVER (...)` shape. Robust enough
    for the contexts SLayer needs: detecting `OVER` inside `DatasetField.sql`,
    `DatasetMetric.expression`, and filter strings. We don't try to disambiguate
    e.g. `OVER` inside a string literal; SLayer's field SQL and filter
    expressions don't legitimately need that.
    """
    if not sql:
        return False
    return bool(_OVER_RE.search(sql))


def holds_window(sql: str, *, dialect: str | None = None) -> bool:
    """Whether native ``sql`` parses holding a window function (an ``OVER (`` inside a string literal is not one)."""
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.ParseError:
        return False
    return parsed.find(exp.Window) is not None


WINDOW_IN_FILTER_ERROR = (
    "contains a window function (OVER clause). Window functions are not "
    "allowed in WHERE on SQLite or most dialects. Either: (a) use a SLayer "
    "transform — rank(), first(), last(), lag(), lead() — e.g. "
    "`rank(<metric>, direction='desc') <= 3`; (b) define the window expression as a "
    "DatasetField.sql on the dataset and filter on the field; or (c) compute it "
    "in an earlier stage of a multi-stage dataset."
)
