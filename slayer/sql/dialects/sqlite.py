"""DEV-1542: SqliteDialect + the SQLite-specific helpers it depends on.

This module folds in the content previously in ``slayer/sql/sqlite_dialect.py``
(the ``rewrite_sqlite_json_extract`` AST rewrite) and
``slayer/sql/sqlite_udfs.py`` (the Python aggregate/scalar UDFs registered
on every fresh SQLite connection).

The helpers are module-level — ``rewrite_sqlite_json_extract`` and
``register_sqlite_udfs`` and the ``_*Agg`` classes are directly
importable (used by ``tests/test_sqlite_json_extract.py`` and
``tests/test_sqlite_udfs.py``). ``SqliteDialect`` is a thin wrapper that
delegates to them through the ``SqlDialect`` interface.
"""

from __future__ import annotations

import math
from collections.abc import Callable

from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.base import SqlDialect


# ===========================================================================
# JSON-extract AST rewrite (DEV-1331; was slayer/sql/sqlite_dialect.py)
# ===========================================================================


def rewrite_sqlite_json_extract(node: exp.Expression) -> exp.Expression:
    """Rewrite every ``exp.JSONExtract`` in the tree rooted at ``node`` to the
    function-call form.

    sqlglot's default SQLite generator emits ``exp.JSONExtract`` as
    ``col -> '$.path'``. In SQLite the ``->`` operator returns the
    JSON-typed form (e.g. ``'"Owned"'`` with literal quotes), whereas
    ``json_extract`` and ``->>`` (``exp.JSONExtractScalar``) return the
    unquoted scalar. The mismatch silently breaks ``CASE WHEN`` /
    equality matches against bare-string literals.

    Returns the (possibly new) root node — callers must use the return
    value because ``node`` itself may be a ``JSONExtract`` (e.g. when
    parsing a ``Column.sql`` whose entire expression is
    ``json_extract(col, path)``), in which case ``Expression.replace``
    is a no-op and a fresh root must be returned. Non-root rewrites
    happen in place.

    Loops to a fixed point so nested forms like
    ``json_extract(json_extract(j, '$.outer'), '$.inner')`` get
    rewritten at every level.
    """
    while True:
        if isinstance(node, exp.JSONExtract):
            node = _to_anonymous(node)
            continue
        je = node.find(exp.JSONExtract)
        if je is None:
            return node
        je.replace(_to_anonymous(je))


def _to_anonymous(je: exp.JSONExtract) -> exp.Anonymous:
    return exp.Anonymous(
        this="JSON_EXTRACT",
        expressions=[je.this, je.expression],
    )


# ===========================================================================
# Python aggregate / scalar UDFs (DEV-1317 / DEV-1337; was sqlite_udfs.py)
# ===========================================================================
# SQLite has a much smaller built-in math/stat catalog than Postgres,
# DuckDB, MySQL, or ClickHouse. To bring SQLite to per-row and
# per-aggregate parity, this section registers Python implementations
# on every new SQLite connection via SQLAlchemy's ``connect`` event.


# ---------------------------------------------------------------------------
# Median / percentile (existing — unchanged from sqlite_udfs.py)
# ---------------------------------------------------------------------------


class _MedianAgg:
    """1-arg median: average of the two middle values for even N."""

    def __init__(self) -> None:
        self._vals: list[float] = []

    def step(self, value) -> None:
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> float | None:
        if not self._vals:
            return None
        s = sorted(self._vals)
        n = len(s)
        mid = n // 2
        if n % 2:
            return s[mid]
        return (s[mid - 1] + s[mid]) / 2.0


class _PercentileContAgg:
    """2-arg PERCENTILE_CONT(value, p): linear interpolation, matches Postgres."""

    def __init__(self) -> None:
        self._vals: list[float] = []
        self._p: float | None = None

    def step(self, value, p) -> None:
        if p is not None:
            p_float = float(p)
            if not 0.0 <= p_float <= 1.0:
                raise ValueError(f"percentile p must be in [0, 1], got {p_float}")
            self._p = p_float
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> float | None:
        if not self._vals or self._p is None:
            return None
        s = sorted(self._vals)
        n = len(s)
        if n == 1:
            return s[0]
        rank = self._p * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        return s[lo] + (rank - lo) * (s[hi] - s[lo])


class _PercentileDiscAgg:
    """2-arg PERCENTILE_DISC(value, p): smallest value v with cume_dist(v) >= p."""

    def __init__(self) -> None:
        self._vals: list[float] = []
        self._p: float | None = None

    def step(self, value, p) -> None:
        if p is not None:
            p_float = float(p)
            if not 0.0 <= p_float <= 1.0:
                raise ValueError(f"percentile p must be in [0, 1], got {p_float}")
            self._p = p_float
        if value is not None:
            self._vals.append(value)

    def finalize(self):
        if not self._vals or self._p is None:
            return None
        s = sorted(self._vals)
        n = len(s)
        # cume_dist of element at index k (0-based) is (k+1)/n.
        # Smallest k with (k+1)/n >= p  =>  k = ceil(p*n) - 1.
        k = max(0, math.ceil(self._p * n) - 1)
        return s[k]


# ---------------------------------------------------------------------------
# Statistical aggregates (DEV-1317): Welford's online algorithm
# ---------------------------------------------------------------------------


class _OneVarWelford:
    """Shared online-stats state for the four 1-arg stat aggregates.

    Maintains ``(n, mean, M2)`` where ``M2 = sum((x_i - mean)^2)``.
    Subclasses pick how to turn it into stddev_samp / stddev_pop /
    var_samp / var_pop in ``finalize()``.

    NULL inputs are skipped (don't contribute to ``n``), matching
    Postgres semantics for the whole stat-aggregate family.
    """

    def __init__(self) -> None:
        self._n: int = 0
        self._mean: float = 0.0
        self._m2: float = 0.0

    def step(self, value) -> None:
        if value is None:
            return
        self._n += 1
        delta = value - self._mean
        self._mean += delta / self._n
        self._m2 += delta * (value - self._mean)


class _StddevSampAgg(_OneVarWelford):
    """Sample standard deviation. NULL when N <= 1."""

    def finalize(self) -> float | None:
        if self._n <= 1:
            return None
        return math.sqrt(self._m2 / (self._n - 1))


class _StddevPopAgg(_OneVarWelford):
    """Population standard deviation. NULL at N=0; 0 at N=1."""

    def finalize(self) -> float | None:
        if self._n == 0:
            return None
        if self._n == 1:
            return 0
        return math.sqrt(self._m2 / self._n)


class _VarSampAgg(_OneVarWelford):
    """Sample variance. NULL when N <= 1."""

    def finalize(self) -> float | None:
        if self._n <= 1:
            return None
        return self._m2 / (self._n - 1)


class _VarPopAgg(_OneVarWelford):
    """Population variance. NULL at N=0; 0 at N=1."""

    def finalize(self) -> float | None:
        if self._n == 0:
            return None
        if self._n == 1:
            return 0
        return self._m2 / self._n


class _PairAgg:
    """Shared 2-variable Welford state for corr / covar_samp / covar_pop."""

    def __init__(self) -> None:
        self._n: int = 0
        self._mean_x: float = 0.0
        self._mean_y: float = 0.0
        self._m2_x: float = 0.0
        self._m2_y: float = 0.0
        self._c: float = 0.0

    def step(self, x, y) -> None:
        if x is None or y is None:
            return
        self._n += 1
        dx = x - self._mean_x
        self._mean_x += dx / self._n
        dy = y - self._mean_y
        self._mean_y += dy / self._n
        self._m2_x += dx * (x - self._mean_x)
        self._m2_y += dy * (y - self._mean_y)
        self._c += dx * (y - self._mean_y)


class _CorrAgg(_PairAgg):
    """Pearson correlation. NULL when fewer than 2 non-null pairs OR
    when either side has zero variance (matches Postgres CORR)."""

    def finalize(self) -> float | None:
        if self._n < 2:
            return None
        if self._m2_x == 0 or self._m2_y == 0:
            return None
        return self._c / math.sqrt(self._m2_x * self._m2_y)


class _CovarSampAgg(_PairAgg):
    """Sample covariance. NULL when N <= 1."""

    def finalize(self) -> float | None:
        if self._n <= 1:
            return None
        return self._c / (self._n - 1)


class _CovarPopAgg(_PairAgg):
    """Population covariance. NULL at N=0; 0 at N=1."""

    def finalize(self) -> float | None:
        if self._n == 0:
            return None
        if self._n == 1:
            return 0
        return self._c / self._n


# ---------------------------------------------------------------------------
# Scalar wrappers (DEV-1317)
# ---------------------------------------------------------------------------


def _ln(x):
    if x is None:
        return None
    return math.log(x)


def _log10(x):
    if x is None:
        return None
    return math.log10(x)


def _log2(x):
    # DEV-1337: overrides SQLite >=3.35's built-in to give strict
    # "errors propagate" semantics matching Postgres.
    if x is None:
        return None
    return math.log2(x)


def _log_base_x(b, x):
    """``log(B, X)`` returns log_B(X). Base first, value second."""
    if b is None or x is None:
        return None
    return math.log(x, b)


def _exp(x):
    if x is None:
        return None
    return math.exp(x)


def _sqrt(x):
    if x is None:
        return None
    return math.sqrt(x)


def _pow(x, n):
    """``pow(x, n)`` / ``power(x, n)`` — uses ``math.pow`` rather than ``**``.

    ``math.pow`` raises on negative-base-non-integer-exponent (clean
    OperationalError at the SQLite boundary) and overflows into IEEE-754
    ``inf`` rather than building an unbounded big-int.
    """
    if x is None or n is None:
        return None
    return math.pow(x, n)


def register_sqlite_udfs(dbapi_connection) -> None:
    """Register all SLayer SQLite UDFs on a freshly-opened DBAPI connection.

    Wired in via SQLAlchemy's ``connect`` event in ``slayer.sql.client``,
    so this is called once per new connection and again on pool refresh.
    Idempotent: re-registering a UDF on the same connection replaces the
    previous one (sqlite3 default behaviour).
    """
    # --- Scalar UDFs ------------------------------------------------------
    dbapi_connection.create_function("ln", 1, _ln)
    dbapi_connection.create_function("log10", 1, _log10)
    dbapi_connection.create_function("log2", 1, _log2)
    # SQLite >=3.35 ships a built-in ``log(B, X)`` that silently returns
    # NULL on math-domain inputs. The UDF overrides that with strict
    # error-propagating semantics matching Postgres.
    dbapi_connection.create_function("log", 2, _log_base_x)
    dbapi_connection.create_function("exp", 1, _exp)
    dbapi_connection.create_function("sqrt", 1, _sqrt)
    dbapi_connection.create_function("pow", 2, _pow)
    dbapi_connection.create_function("power", 2, _pow)

    # --- Aggregate UDFs ---------------------------------------------------
    dbapi_connection.create_aggregate("median", 1, _MedianAgg)
    dbapi_connection.create_aggregate("percentile_cont", 2, _PercentileContAgg)
    dbapi_connection.create_aggregate("percentile_disc", 2, _PercentileDiscAgg)

    # Statistical aggregates. Register each under its canonical Postgres-
    # style name AND under the name sqlglot rewrites it to on SQLite, so
    # generator output that goes through sqlglot still resolves at runtime.
    dbapi_connection.create_aggregate("stddev_samp", 1, _StddevSampAgg)
    dbapi_connection.create_aggregate("stddev_pop", 1, _StddevPopAgg)
    dbapi_connection.create_aggregate("var_samp", 1, _VarSampAgg)
    dbapi_connection.create_aggregate("variance", 1, _VarSampAgg)
    dbapi_connection.create_aggregate("var_pop", 1, _VarPopAgg)
    dbapi_connection.create_aggregate("variance_pop", 1, _VarPopAgg)
    dbapi_connection.create_aggregate("corr", 2, _CorrAgg)
    dbapi_connection.create_aggregate("covar_samp", 2, _CovarSampAgg)
    dbapi_connection.create_aggregate("covar_pop", 2, _CovarPopAgg)


# ===========================================================================
# SqliteDialect — overrides for STRFTIME date_trunc, DATETIME-modifier
# time arithmetic, percentile UDF call shape, JSON rewrite, UDF registration.
# ===========================================================================


# DATETIME-modifier unit names (SQLite has no INTERVAL syntax). ``week``
# is folded into ``days`` because SQLite has no week unit either.
_WINDOW_UNIT_SQLITE = {
    "y": "years",
    "m": "months",
    "w": "days",
    "d": "days",
    "h": "hours",
    "min": "minutes",
    "s": "seconds",
}


class SqliteDialect(SqlDialect):
    sqlglot_name: str = "sqlite"
    ds_type_aliases: frozenset[str] = frozenset({"sqlite"})
    explain_prefix: str | None = "EXPLAIN QUERY PLAN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def build_date_trunc(
        self,
        col_expr: exp.Expression,
        granularity: TimeGranularity,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """SQLite has no DATE_TRUNC — use STRFTIME (with CASE WHEN for
        quarter, weekday-modifier for week)."""
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # DEV-1572: delegate to the base generic shift, which composes
            # SQLite's own day-offset (DATE(col, 'N days')) around SQLite's
            # Monday-week truncation — yielding the Sunday-anchored bucket.
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity, parse=parse,
            )
        gran_str = granularity.value
        fmt_map = {
            "year": "%Y-01-01",
            "month": "%Y-%m-01",
            "day": "%Y-%m-%d",
            "hour": "%Y-%m-%d %H:00:00",
            "minute": "%Y-%m-%d %H:%M:00",
            "second": "%Y-%m-%d %H:%M:%S",
        }
        if gran_str == "week":
            # SQLite weekday 0=Sunday; use date() with weekday modifier
            # to back up to the preceding Monday-equivalent start.
            return parse(
                f"DATE({col_expr.sql(dialect='sqlite')}, 'weekday 0', '-6 days')"
            )
        if gran_str == "quarter":
            col_sql = col_expr.sql(dialect="sqlite")
            return parse(
                f"STRFTIME('%Y-', {col_sql}) || CASE "
                f"WHEN CAST(STRFTIME('%m', {col_sql}) AS INTEGER) <= 3 THEN '01-01' "
                f"WHEN CAST(STRFTIME('%m', {col_sql}) AS INTEGER) <= 6 THEN '04-01' "
                f"WHEN CAST(STRFTIME('%m', {col_sql}) AS INTEGER) <= 9 THEN '07-01' "
                f"ELSE '10-01' END"
            )
        fmt = fmt_map.get(gran_str, "%Y-%m-%d")
        return exp.Anonymous(
            this="STRFTIME",
            expressions=[exp.Literal.string(fmt), col_expr],
        )

    def build_time_offset_expr(
        self,
        col_expr: exp.Expression,
        offset: int,
        granularity: str,
    ) -> exp.Expression:
        """SQLite uses ``DATE(col, 'N units')`` — no INTERVAL syntax.

        Granularity normalization: ``quarter`` → ``val * 3`` of ``months``;
        ``week`` → ``val * 7`` of ``days`` (SQLite has no week unit).
        """
        sqlite_units = {
            "year": "years", "month": "months", "day": "days",
            "quarter": "months", "week": "days", "week_sunday": "days",
            "hour": "hours", "minute": "minutes", "second": "seconds",
        }
        sqlite_unit = sqlite_units.get(granularity, granularity.lower() + "s")
        val = offset * 3 if granularity == "quarter" else offset
        sqlite_val = val * 7 if granularity in ("week", "week_sunday") else val
        return exp.Anonymous(
            this="DATE",
            expressions=[
                col_expr,
                exp.Literal.string(f"{sqlite_val} {sqlite_unit}"),
            ],
        )

    def duration_interval_exprs(
        self,
        parts: list[tuple[int, str]],
        sign: int = 1,
    ) -> list[exp.Expression]:
        """SQLite uses DATETIME-modifier string literals with sign baked in.
        Week is converted to ``N*7 days`` (no native week unit)."""
        prefix = "+" if sign >= 0 else "-"
        return [
            exp.Literal.string(
                f"{prefix}{(amount * 7 if unit == 'w' else amount)} "
                f"{_WINDOW_UNIT_SQLITE[unit]}"
            )
            for amount, unit in parts
        ]

    def add_intervals_expr(
        self,
        expr: exp.Expression,
        intervals: list[exp.Expression],
        sign: int = 1,
    ) -> exp.Expression:
        """SQLite wraps as ``DATETIME(expr, mod1, mod2, ...)``.

        The sign is already baked into each modifier by
        ``duration_interval_exprs`` — the ``sign`` arg is intentionally
        ignored here.
        """
        return exp.Anonymous(this="DATETIME", expressions=[expr, *intervals])

    def build_median(
        self,
        inner: exp.Expression,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """SQLite: parses ``median(inner)`` — registered UDF.

        sqlglot's SQLite generator transpiles ``exp.Median`` to
        ``PERCENTILE_CONT(x, 0.5)`` at emission, matching the pair-form
        ``percentile_cont`` UDF signature.
        """
        inner_sql = inner.sql(dialect="sqlite")
        return parse(f"median({inner_sql})")

    def build_percentile(
        self,
        p_str: str,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """SQLite: ``percentile_cont(value, p)`` — registered UDF.

        ``p_str`` is the original user-supplied string, preserved verbatim
        (no float normalization).
        """
        return parse(f"percentile_cont({col_sql}, {p_str})")

    def rewrite_parsed_ast(self, tree: exp.Expression) -> exp.Expression:
        """SQLite override: rewrites every ``exp.JSONExtract`` to
        ``Anonymous(this='JSON_EXTRACT', ...)`` so the emission is the
        function-call form (DEV-1331)."""
        return rewrite_sqlite_json_extract(tree)

    def register_udfs(self, dbapi_connection) -> None:
        """Register the Python aggregate / scalar UDFs on the connection.

        Idempotent — re-registering on the same connection replaces the
        previous one (sqlite3 default behaviour).
        """
        register_sqlite_udfs(dbapi_connection)


