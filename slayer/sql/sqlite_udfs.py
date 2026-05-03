"""Python UDFs registered on every SQLite connection.

SQLite has a much smaller built-in math/stat catalog than Postgres, DuckDB,
MySQL, or ClickHouse. To bring SQLite to per-row and per-aggregate parity
with the other dialects SLayer supports, this module registers Python
implementations on every new SQLite connection via SQLAlchemy's
``connect`` event.

Two flavours of UDF live here:

* **Scalar UDFs** — registered via ``dbapi_connection.create_function``.
  Used inside ``Column.sql`` / ``ModelMeasure.formula`` /
  ``Aggregation.formula`` for per-row math (``ln``, ``log10``, ``log``,
  ``exp``, ``sqrt``, ``pow`` / ``power``).
* **Aggregate UDFs** — registered via
  ``dbapi_connection.create_aggregate``. Used at query time via colon
  syntax (``revenue:median``, ``latency:stddev_samp``,
  ``price:corr(other=quantity)``).

NULL handling: all wrappers return ``None`` for any ``None`` input so SQL
NULL propagation matches the other dialects. Math-domain errors
(``ln(0)``, ``sqrt(-1)``, ``pow(0, -1)``) propagate as Python exceptions
and surface as ``sqlite3.OperationalError`` — matching Postgres's strict
semantics rather than SQLite ≥3.35's silent-NULL built-in behaviour.

``log(B, X)`` argument order
----------------------------

The 2-arg ``log`` UDF takes **base first, value second**:
``log(B, X)`` returns ``log_B(X)``. This matches:

* SQLite ≥3.35 built-in ``log(B, X)``
* Postgres ``LOG(b, x)``
* sqlglot's emission across dialects

Python's ``math.log`` reverses this (``math.log(x, base)``), so the
wrapper internally re-orders the args. The UDF registers on **every**
SQLite version, including ≥3.35 where it deliberately overrides the
built-in. The built-in silently returns NULL on math-domain inputs
(``log(0, 10)``); the UDF raises ``OperationalError``, matching the
Postgres-style strict semantics promised by DEV-1317.

Aggregate edge-case semantics (matches Postgres exactly)
--------------------------------------------------------

* ``stddev_samp`` / ``var_samp``: NULL when N ≤ 1 (Bessel-corrected
  denominator is undefined).
* ``stddev_pop`` / ``var_pop``: NULL when N = 0; **0** when N = 1
  (population SD/VAR of a single sample is zero, not undefined).
* ``corr``: NULL when fewer than 2 non-null pairs OR when either side
  has zero variance. A pair is dropped entirely if **either** value is
  NULL.

sqlglot transpilation aliases
-----------------------------

sqlglot rewrites ``var_samp(x) → VARIANCE(x)`` and ``var_pop(x) →
VARIANCE_POP(x)`` on SQLite (and others). To make those rewritten names
resolve at query time, the same Python aggregate class is registered
under both the canonical and the sqlglot-emitted names. SQLite UDF
lookup is case-insensitive.
"""

from __future__ import annotations

import math
from typing import Optional


# ---------------------------------------------------------------------------
# Median / percentile (existing — unchanged)
# ---------------------------------------------------------------------------


class _MedianAgg:
    """1-arg median: average of the two middle values for even N."""

    def __init__(self) -> None:
        self._vals: list[float] = []

    def step(self, value) -> None:
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> Optional[float]:
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
        self._p: Optional[float] = None

    def step(self, value, p) -> None:
        if p is not None:
            p_float = float(p)
            if not 0.0 <= p_float <= 1.0:
                raise ValueError(f"percentile p must be in [0, 1], got {p_float}")
            self._p = p_float
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> Optional[float]:
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
        self._p: Optional[float] = None

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
# Statistical aggregates (DEV-1317): two-pass list shape, mirrors _MedianAgg.
# ---------------------------------------------------------------------------


def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals)


def _ssd(vals: list[float]) -> float:
    """Sum of squared deviations from the mean."""
    m = _mean(vals)
    return sum((v - m) ** 2 for v in vals)


class _StddevSampAgg:
    """Sample standard deviation. NULL when N <= 1."""

    def __init__(self) -> None:
        self._vals: list[float] = []

    def step(self, value) -> None:
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> Optional[float]:
        n = len(self._vals)
        if n <= 1:
            return None
        return math.sqrt(_ssd(self._vals) / (n - 1))


class _StddevPopAgg:
    """Population standard deviation. NULL at N=0; 0 at N=1."""

    def __init__(self) -> None:
        self._vals: list[float] = []

    def step(self, value) -> None:
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> Optional[float]:
        n = len(self._vals)
        if n == 0:
            return None
        if n == 1:
            return 0
        return math.sqrt(_ssd(self._vals) / n)


class _VarSampAgg:
    """Sample variance. NULL when N <= 1."""

    def __init__(self) -> None:
        self._vals: list[float] = []

    def step(self, value) -> None:
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> Optional[float]:
        n = len(self._vals)
        if n <= 1:
            return None
        return _ssd(self._vals) / (n - 1)


class _VarPopAgg:
    """Population variance. NULL at N=0; 0 at N=1."""

    def __init__(self) -> None:
        self._vals: list[float] = []

    def step(self, value) -> None:
        if value is not None:
            self._vals.append(value)

    def finalize(self) -> Optional[float]:
        n = len(self._vals)
        if n == 0:
            return None
        if n == 1:
            return 0
        return _ssd(self._vals) / n


class _PairAgg:
    """Shared base for 2-arg paired aggregates (corr, covar_samp, covar_pop).

    Skips a pair entirely if either x or y is NULL — matching Postgres
    semantics for all three.
    """

    def __init__(self) -> None:
        self._xs: list[float] = []
        self._ys: list[float] = []

    def step(self, x, y) -> None:
        if x is None or y is None:
            return
        self._xs.append(x)
        self._ys.append(y)


class _CorrAgg(_PairAgg):
    """Pearson correlation between two columns.

    Returns NULL when fewer than 2 non-null pairs OR when either side
    has zero variance — matching Postgres's CORR semantics.
    """

    def finalize(self) -> Optional[float]:
        n = len(self._xs)
        if n < 2:
            return None
        mx = _mean(self._xs)
        my = _mean(self._ys)
        sxx = sum((x - mx) ** 2 for x in self._xs)
        syy = sum((y - my) ** 2 for y in self._ys)
        if sxx == 0 or syy == 0:
            return None
        sxy = sum((x - mx) * (y - my) for x, y in zip(self._xs, self._ys))
        return sxy / math.sqrt(sxx * syy)


class _CovarSampAgg(_PairAgg):
    """Sample covariance between two columns. NULL when N <= 1."""

    def finalize(self) -> Optional[float]:
        n = len(self._xs)
        if n <= 1:
            return None
        mx = _mean(self._xs)
        my = _mean(self._ys)
        return sum((x - mx) * (y - my) for x, y in zip(self._xs, self._ys)) / (n - 1)


class _CovarPopAgg(_PairAgg):
    """Population covariance between two columns. NULL at N=0; 0 at N=1.

    Unlike Pearson correlation, covariance is well-defined even when one
    or both sides are constant — it just returns 0.
    """

    def finalize(self) -> Optional[float]:
        n = len(self._xs)
        if n == 0:
            return None
        if n == 1:
            return 0
        mx = _mean(self._xs)
        my = _mean(self._ys)
        return sum((x - mx) * (y - my) for x, y in zip(self._xs, self._ys)) / n


# ---------------------------------------------------------------------------
# Scalar wrappers (DEV-1317).
# ---------------------------------------------------------------------------


def _ln(x):
    if x is None:
        return None
    return math.log(x)


def _log10(x):
    if x is None:
        return None
    return math.log10(x)


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
    if x is None or n is None:
        return None
    return x ** n


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


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
    # SQLite >= 3.35 ships a built-in ``log(B, X)`` that silently returns
    # NULL on math-domain inputs (``log(0, 10)``, ``log(-1, 10)``). DEV-1317
    # promises Postgres-style "errors propagate" semantics, so we register
    # our UDF on every version — including >=3.35 — to override the
    # built-in's silent-NULL behaviour. Same B-first arg order in both, so
    # only the error policy changes.
    dbapi_connection.create_function("log", 2, _log_base_x)
    dbapi_connection.create_function("exp", 1, _exp)
    dbapi_connection.create_function("sqrt", 1, _sqrt)
    dbapi_connection.create_function("pow", 2, _pow)
    # `power` is the Postgres/MySQL spelling; sqlglot may emit either.
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
    # sqlglot: var_samp(x) -> VARIANCE(x); var_pop(x) -> VARIANCE_POP(x)
    dbapi_connection.create_aggregate("var_samp", 1, _VarSampAgg)
    dbapi_connection.create_aggregate("variance", 1, _VarSampAgg)
    dbapi_connection.create_aggregate("var_pop", 1, _VarPopAgg)
    dbapi_connection.create_aggregate("variance_pop", 1, _VarPopAgg)
    dbapi_connection.create_aggregate("corr", 2, _CorrAgg)
    dbapi_connection.create_aggregate("covar_samp", 2, _CovarSampAgg)
    dbapi_connection.create_aggregate("covar_pop", 2, _CovarPopAgg)
