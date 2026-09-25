"""SqliteDialect + the SQLite-specific helpers it depends on.

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

from sqlglot import exp
from sqlglot.expressions.core import Expression

from typing import Optional

from slayer.core.enums import DataType, TimeGranularity
from slayer.sql.dialects.base import SqlDialect, TimeUnit


# ===========================================================================
# JSON-extract AST rewrite
# ===========================================================================


def rewrite_sqlite_json_extract(node: Expression) -> Expression:
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


def _strftime(fmt: str, col_expr: Expression) -> exp.Anonymous:
    return exp.Anonymous(this="STRFTIME", expressions=[exp.Literal.string(fmt), col_expr.copy()])


def _to_anonymous(je: exp.JSONExtract) -> exp.Anonymous:
    return exp.Anonymous(
        this="JSON_EXTRACT",
        expressions=[je.this, je.expression],
    )


# ===========================================================================
# Python aggregate / scalar UDFs
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
# Statistical aggregates: Welford's online algorithm
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
# Scalar wrappers
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
    # Overrides SQLite >=3.35's built-in to give strict
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
    # Numeric affinity stores INTEGER/REAL — nothing exact to preserve.
    exact_decimal_native: bool = False
    max_identifier_bytes: int | None = None  # unbounded

    def declared_cast_type(self, dt: Optional[DataType]) -> Optional[DataType]:
        """SQLite stores dates as text under numeric affinity, so a declared DATE / TIMESTAMP cast collapses a text date to its leading year — suppress it (P2)."""
        if dt in (DataType.DATE, DataType.TIMESTAMP):
            return None
        return dt

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """SQLite's ``IS`` is null-safe on every supported version;
        ``IS NOT DISTINCT FROM`` (what sqlglot emits for ``NullSafeEQ``) needs
        SQLite ≥ 3.39, so anchor on bare ``IS`` instead."""
        return exp.Is(this=left, expression=right)

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """SQLite has no DATE_TRUNC — use STRFTIME (with CASE WHEN for
        quarter, weekday-modifier for week)."""
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # Delegate to the base generic shift, which composes
            # SQLite's own day-offset (DATE(col, 'N days')) around SQLite's
            # Monday-week truncation — yielding the Sunday-anchored bucket.
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity,
            )
        if granularity == TimeGranularity.WEEK:
            # weekday 0 = Sunday; back up to the preceding Monday.
            return exp.Anonymous(this="DATE", expressions=[
                col_expr.copy(), exp.Literal.string("weekday 0"), exp.Literal.string("-6 days"),
            ])
        if granularity == TimeGranularity.QUARTER:
            return exp.DPipe(this=_strftime("%Y-", col_expr), expression=exp.Case(
                ifs=[
                    exp.If(
                        this=exp.LTE(
                            this=exp.Cast(this=_strftime("%m", col_expr), to=exp.DataType.build("INTEGER")),
                            expression=exp.Literal.number(last_month),
                        ),
                        true=exp.Literal.string(start),
                    )
                    for last_month, start in ((3, "01-01"), (6, "04-01"), (9, "07-01"))
                ],
                default=exp.Literal.string("10-01"),
            ))
        fmt_map = {
            TimeGranularity.YEAR: "%Y-01-01",
            TimeGranularity.MONTH: "%Y-%m-01",
            TimeGranularity.DAY: "%Y-%m-%d",
            TimeGranularity.HOUR: "%Y-%m-%d %H:00:00",
            TimeGranularity.MINUTE: "%Y-%m-%d %H:%M:00",
            TimeGranularity.SECOND: "%Y-%m-%d %H:%M:%S",
        }
        return _strftime(fmt_map[granularity], col_expr)

    def build_time_offset_expr(
        self,
        col_expr: Expression,
        offset: int,
        granularity: TimeGranularity | TimeUnit,
    ) -> Expression:
        """SQLite uses ``DATE(col, 'N units')`` — no INTERVAL syntax.

        Granularity normalization: ``quarter`` → ``val * 3`` of ``months``;
        ``week`` → ``val * 7`` of ``days`` (SQLite has no week unit).
        """
        sqlite_units = {
            "year": "years", "month": "months", "day": "days",
            "quarter": "months", "week": "days", "week_sunday": "days",
            "hour": "hours", "minute": "minutes", "second": "seconds",
        }
        granularity = TimeGranularity(granularity)
        sqlite_unit = sqlite_units[granularity.value]
        val = offset * 3 if granularity == TimeGranularity.QUARTER else offset
        sqlite_val = val * 7 if granularity in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY) else val
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
    ) -> list[Expression]:
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
        expr: Expression,
        intervals: list[Expression],
        sign: int = 1,
    ) -> Expression:
        """SQLite wraps as ``DATETIME(expr, mod1, mod2, ...)``.

        The sign is already baked into each modifier by
        ``duration_interval_exprs`` — the ``sign`` arg is intentionally
        ignored here.
        """
        return exp.Anonymous(this="DATETIME", expressions=[expr, *intervals])

    def frame_time_operand(self, expr: Expression) -> Expression:
        """Under numeric affinity a bare-date column (``'2025-02-01'``) string-sorts
        BEFORE a DATETIME frame bound (``'2025-02-01 00:00:00'``), leaking the
        exclusive ``bucket_end`` row into the previous bucket. Wrap it in DATETIME
        so both sides carry the time part and the half-open interval is exact."""
        return exp.Anonymous(this="DATETIME", expressions=[expr])

    def build_median(self, inner: Expression) -> Expression:
        """SQLite: ``exp.Median``, emitted as the registered ``PERCENTILE_CONT(x, 0.5)`` UDF."""
        return exp.Median(this=inner.copy())

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """SQLite: ``percentile_cont(value, p)`` — registered UDF."""
        return exp.PercentileCont(this=col_expr.copy(), expression=p.copy())

    def rewrite_parsed_ast(self, tree: Expression) -> Expression:
        """SQLite override: rewrites every ``exp.JSONExtract`` to
        ``Anonymous(this='JSON_EXTRACT', ...)`` so the emission is the
        function-call form."""
        return rewrite_sqlite_json_extract(tree)

    def register_udfs(self, dbapi_connection) -> None:
        """Register the Python aggregate / scalar UDFs on the connection.

        Idempotent — re-registering on the same connection replaces the
        previous one (sqlite3 default behaviour).
        """
        register_sqlite_udfs(dbapi_connection)


