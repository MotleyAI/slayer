"""Detect SQL window functions (`OVER (...)`) in column SQL or filter strings.

A window function cannot appear in a WHERE clause on most dialects: a filter on a windowed
`Column.sql` auto-promotes to a post-aggregation outer WHERE, and a raw window in a filter or
measure formula gets an actionable error.
"""

import re

_OVER_RE = re.compile(r"\bover\s*\(", re.IGNORECASE)


def has_window_function(sql: str) -> bool:
    """Return True if `sql` contains a window function call (`OVER (...)`).

    Uses a regex on the canonical `<func>(...) OVER (...)` shape. Robust enough
    for the contexts SLayer needs: detecting `OVER` inside `Column.sql`,
    `ModelMeasure.formula`, and filter strings. We don't try to disambiguate
    e.g. `OVER` inside a string literal; SLayer's column SQL and filter
    formulas don't legitimately need that.
    """
    if not sql:
        return False
    return bool(_OVER_RE.search(sql))


WINDOW_IN_FILTER_ERROR = (
    "contains a window function (OVER clause). Window functions are not "
    "allowed in WHERE on SQLite or most dialects. Either: (a) use a SLayer "
    "transform — rank(), first(), last(), lag(), lead() — e.g. "
    "`rank(<measure>, direction='desc') <= 3`; (b) define the window expression as a "
    "Column.sql on the model and filter on the column; or (c) compute it "
    "in an earlier stage of a multi-stage model."
)
