"""SqliteDialect plus its JSON-extract rewrite and Python UDFs."""

from __future__ import annotations

import math
import re
from calendar import monthrange
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache

from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expression

from typing import Callable, ClassVar, Optional

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.core.json_path import JsonPath
from slayer.core.keys import cast_data_type
from slayer.core.like_pattern import LikePattern, LikeWildcard, parse_like
from slayer.core.regex_pattern import RegexGroupRef, RegexPattern, RegexReplacement, parse_replacement
from slayer.core.time_points import parse_temporal_value
from slayer.sql.dialects.base import (
    COMPARISON_NODES,
    FALSE_WORDS,
    TRUE_WORDS,
    VENDOR_ONLY_CAST_TYPES,
    ExtremumFn,
    MathFn,
    SqlDialect,
    TemporalComparisonOp,
    TextFn,
    anon,
    as_text,
    case,
    iso_text,
    lit,
)
from slayer.sql.dialects.patterns import PYTHON, RegexFlavour, like_text


def rewrite_sqlite_json_extract(node: Expression) -> Expression:
    """Rewrite every ``exp.JSONExtract`` to ``json_extract(...)``; returns the possibly new root.

    SQLite's ``->`` returns JSON-quoted text (``'"Owned"'``), breaking equality with bare strings.
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


def _normalised_timestamp(expr: Expression) -> Expression:
    """``expr`` as ``YYYY-MM-DD HH:MM:SS.SSS`` text, whatever ISO spelling it is stored in."""
    return exp.Anonymous(this="STRFTIME", expressions=[exp.Literal.string("%Y-%m-%d %H:%M:%f"), expr])


def _strftime_int(fmt: str, col_expr: Expression) -> Expression:
    return exp.Cast(this=_strftime(fmt, col_expr), to=exp.DataType.build("INTEGER"))


def _days_since_monday(col_expr: Expression) -> Expression:
    """0 for Monday … 6 for Sunday (``%w`` counts from Sunday)."""
    return exp.Mod(
        this=exp.Paren(this=exp.Add(this=_strftime_int("%w", col_expr), expression=exp.Literal.number(6))),
        expression=exp.Literal.number(7),
    )


_STRFTIME_PARTS = {
    DatePart.YEAR: "%Y", DatePart.MONTH: "%m", DatePart.DAY: "%d", DatePart.DAY_OF_YEAR: "%j",
    DatePart.HOUR: "%H", DatePart.MINUTE: "%M", DatePart.SECOND: "%S",
}


def _to_anonymous(je: exp.JSONExtract) -> exp.Anonymous:
    return exp.Anonymous(
        this="JSON_EXTRACT",
        expressions=[je.this, je.expression],
    )


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
        # Smallest k with cume_dist (k+1)/n >= p.
        k = max(0, math.ceil(self._p * n) - 1)
        return s[k]


class _OneVarWelford:
    """Welford ``(n, mean, M2)`` state for the 1-arg stat aggregates; NULLs skipped."""

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
    """Pearson correlation. NULL when N < 2 or either side has zero variance."""

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


def _ln(x):
    if x is None:
        return None
    return math.log(x)


def _log10(x):
    if x is None:
        return None
    return math.log10(x)


def _log2(x):
    # Overrides SQLite >=3.35's built-in so domain errors propagate, as in Postgres.
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
    """``math.pow``, not ``**``: raises on a negative base's fractional power, no big-ints."""
    if x is None or n is None:
        return None
    return math.pow(x, n)


_SUB_DAY_DELTAS = {"second": "seconds", "minute": "minutes", "hour": "hours"}
_MONTH_STEPS = {"month": 1, "quarter": 3, "year": 12}
_DAY_STEPS = {"day": 1, "week": 7, "week_sunday": 7}


_SQLITE_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_SQLITE_TIME_RE = re.compile(r"[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?")


def _is_sqlite_temporal_text(value: str) -> bool:
    if not _SQLITE_DATE_RE.fullmatch(value, 0, 10):
        return False
    return len(value) == 10 or _SQLITE_TIME_RE.fullmatch(value, 10) is not None


def _parse_sqlite_temporal(value) -> Optional[datetime]:
    """Stored SQLite date text as a datetime; ``None`` when it is not ISO date/timestamp text."""
    if not isinstance(value, str) or not _is_sqlite_temporal_text(value):
        return None
    try:
        return datetime.fromisoformat(value) if len(value) > 10 else datetime.fromisoformat(value + " 00:00:00")
    except ValueError:
        return None


def _slayer_date_add(ts, n, unit):
    """``ts`` moved by ``n`` ``unit``s, month-based moves clamped at month-end; NULL on NULL / malformed input."""
    moment = _parse_sqlite_temporal(ts)
    if moment is None or not isinstance(n, (int, float)) or int(n) != n:
        return None
    count = int(n)
    try:
        if unit in _SUB_DAY_DELTAS:
            moved = moment + timedelta(**{_SUB_DAY_DELTAS[unit]: count})
        elif unit in _DAY_STEPS:
            moved = moment + timedelta(days=count * _DAY_STEPS[unit])
        else:
            total = moment.month - 1 + count * _MONTH_STEPS[unit]
            year, month = moment.year + total // 12, total % 12 + 1
            moved = moment.replace(year=year, month=month, day=min(moment.day, monthrange(year, month)[1]))
    except (OverflowError, ValueError):
        return None
    if len(ts) == 10 and unit not in _SUB_DAY_DELTAS:
        return moved.date().isoformat()
    return moved.isoformat(sep=" ")


_NUMBER_TEXT_RE = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")


_INT64_BOUND = 2 ** 63


def _try_number(text: str, kind: DataType):
    stripped = text.strip()
    if not _NUMBER_TEXT_RE.fullmatch(stripped):
        return None
    if kind is DataType.DOUBLE:
        number = float(stripped)
        return number if math.isfinite(number) else None
    exact = Decimal(stripped)
    return int(exact) if -_INT64_BOUND <= exact < _INT64_BOUND else None


def _try_boolean(text: str):
    word = text.strip().lower()
    if word in TRUE_WORDS:
        return 1
    if word in FALSE_WORDS:
        return 0
    return None


def _try_temporal(text: str, kind: DataType):
    moment = parse_temporal_value(text.strip())
    if moment is None:
        return None
    if kind is DataType.DATE:
        return (moment.date() if isinstance(moment, datetime) else moment).isoformat()
    stamp = moment if isinstance(moment, datetime) else datetime(moment.year, moment.month, moment.day)
    return stamp.isoformat(sep=" ")


def _slayer_try_cast(text, target):
    """``TRY_CAST(text AS target)``: the converted value, or NULL where the cast cannot convert ``text``."""
    if text is None:
        return None
    kind = cast_data_type(target)
    if kind in (DataType.INT, DataType.DOUBLE):
        return _try_number(text, kind)
    if kind is DataType.BOOLEAN:
        return _try_boolean(text)
    if kind in (DataType.DATE, DataType.TIMESTAMP):
        return _try_temporal(text, kind)
    return text


def _nulls(fn: Callable) -> Callable:
    def wrapped(*args):
        return None if any(a is None for a in args) else fn(*args)
    return wrapped


@lru_cache(maxsize=1024)
def _compiled(pattern: str, flags: str) -> "re.Pattern[str]":
    return re.compile(pattern, re.DOTALL | (re.IGNORECASE if "i" in flags else 0))


def _regexp_like(value, pattern, flags):
    if value is None or pattern is None:
        return None
    return int(_compiled(pattern, flags or "").search(str(value)) is not None)


def _regexp_extract(value, pattern, group):
    if value is None or pattern is None or group is None:
        return None
    match = _compiled(pattern, "").search(str(value))
    return None if match is None else match.group(int(group)) or ""


def _regexp_replace(value, pattern, replacement, flags):
    """``replacement`` is portable text: ``\\1``-``\\9`` insert a group, ``\\\\`` a backslash."""
    if value is None or pattern is None or replacement is None:
        return None
    compiled = _compiled(pattern, flags or "")
    parts = parse_replacement(replacement, groups=compiled.groups).parts

    def substitute(match: "re.Match[str]") -> str:
        return "".join(p if isinstance(p, str) else (match.group(p.index) or "") for p in parts)
    return compiled.sub(substitute, str(value))


_GLOB_SPECIALS = {"*": "[*]", "?": "[?]", "[": "[[]"}


def like_to_glob(pattern: Optional[str]) -> Optional[str]:
    """A LIKE pattern as the GLOB pattern with its meaning (case-sensitive, collation-free)."""
    if pattern is None:
        return None
    out = []
    for part in parse_like(pattern).parts:
        if isinstance(part, LikeWildcard):
            out.append("*" if part.kind == "any" else "?")
        else:
            out.append("".join(_GLOB_SPECIALS.get(ch, ch) for ch in part))
    return "".join(out)


def _pad(s: str, n, fill: str, *, left: bool) -> str:
    n = int(n)
    if n <= 0:
        return ""
    if len(s) >= n or not fill:
        return s[:n]
    padding = (fill * n)[:n - len(s)]
    return padding + s if left else s + padding


def _split_part(s: str, delimiter: str, index) -> str:
    fields = s.split(delimiter) if delimiter else [s]
    index = int(index)
    if index > 0:
        return fields[index - 1] if index <= len(fields) else ""
    return fields[index] if -index <= len(fields) else ""


def _fmod(a, b):
    if a is None or b is None or b == 0:
        return None
    return math.fmod(a, b) if isinstance(a, float) or isinstance(b, float) else int(math.fmod(a, b))


def _trunc(x):
    if x is None:
        return None
    return math.trunc(x) if isinstance(x, int) else float(math.trunc(x))


def _cbrt(x: float) -> float:
    return math.copysign(abs(x) ** (1 / 3), x)


_TEXT_UDFS: dict = {
    "slayer_lpad": (3, _nulls(lambda s, n, f: _pad(str(s), n, str(f), left=True))),
    "slayer_rpad": (3, _nulls(lambda s, n, f: _pad(str(s), n, str(f), left=False))),
    "slayer_reverse": (1, _nulls(lambda s: str(s)[::-1])),
    "slayer_repeat": (2, _nulls(lambda s, n: str(s) * max(int(n), 0))),
    "slayer_split_part": (3, _nulls(lambda s, d, k: _split_part(str(s), str(d), k))),
    "slayer_concat_ws": (-1, lambda sep, *args: None if sep is None else str(sep).join(
        str(a) for a in args if a is not None)),
    "slayer_like_glob": (1, like_to_glob),
    "slayer_regexp_like": (3, _regexp_like),
    "slayer_regexp_extract": (3, _regexp_extract),
    "slayer_regexp_replace": (4, _regexp_replace),
    # SQLAlchemy's pysqlite registers ``floor`` as ``math.floor``, which fails on NULL.
    "floor": (1, _nulls(lambda x: float(math.floor(x)) if isinstance(x, float) else math.floor(x))),
    "slayer_fmod": (2, _fmod),
    "slayer_trunc": (1, _trunc),
    "slayer_pi": (0, lambda: math.pi),
    "slayer_cbrt": (1, _nulls(_cbrt)),
    "slayer_atan2": (2, _nulls(math.atan2)),
    **{f"slayer_{name}": (1, _nulls(getattr(math, name))) for name in ("sin", "cos", "tan", "asin", "acos", "atan")},
}


def register_sqlite_udfs(dbapi_connection) -> None:
    """Register all SLayer SQLite UDFs on a fresh DBAPI connection (idempotent)."""
    for name, (narg, fn) in _TEXT_UDFS.items():
        dbapi_connection.create_function(name, narg, fn, deterministic=True)
    dbapi_connection.create_function("ln", 1, _ln)
    dbapi_connection.create_function("log10", 1, _log10)
    dbapi_connection.create_function("log2", 1, _log2)
    # SQLite >=3.35's built-in ``log(B, X)`` silently returns NULL on domain errors.
    dbapi_connection.create_function("log", 2, _log_base_x)
    dbapi_connection.create_function("exp", 1, _exp)
    dbapi_connection.create_function("sqrt", 1, _sqrt)
    dbapi_connection.create_function("pow", 2, _pow)
    dbapi_connection.create_function("power", 2, _pow)
    dbapi_connection.create_function("slayer_date_add", 3, _slayer_date_add)
    dbapi_connection.create_function("slayer_try_cast", 2, _slayer_try_cast)

    dbapi_connection.create_aggregate("median", 1, _MedianAgg)
    dbapi_connection.create_aggregate("percentile_cont", 2, _PercentileContAgg)
    dbapi_connection.create_aggregate("percentile_disc", 2, _PercentileDiscAgg)

    # Also under the names sqlglot rewrites them to on SQLite.
    dbapi_connection.create_aggregate("stddev_samp", 1, _StddevSampAgg)
    dbapi_connection.create_aggregate("stddev_pop", 1, _StddevPopAgg)
    dbapi_connection.create_aggregate("var_samp", 1, _VarSampAgg)
    dbapi_connection.create_aggregate("variance", 1, _VarSampAgg)
    dbapi_connection.create_aggregate("var_pop", 1, _VarPopAgg)
    dbapi_connection.create_aggregate("variance_pop", 1, _VarPopAgg)
    dbapi_connection.create_aggregate("corr", 2, _CorrAgg)
    dbapi_connection.create_aggregate("covar_samp", 2, _CovarSampAgg)
    dbapi_connection.create_aggregate("covar_pop", 2, _CovarPopAgg)


def _like_source(pattern: LikePattern) -> str:
    """``pattern`` back as LIKE text (the GLOB conversion runs in ``slayer_like_glob``)."""
    return like_text(pattern)


class SqliteDialect(SqlDialect):
    sqlglot_name: str = "sqlite"
    native_lists: ClassVar[bool] = False
    database_name: ClassVar[str] = "SQLite"
    # Any type name is a column affinity on SQLite.
    vendor_cast_types: ClassVar[frozenset[DType]] = VENDOR_ONLY_CAST_TYPES
    ds_type_aliases: frozenset[str] = frozenset({"sqlite"})
    explain_prefix: str | None = "EXPLAIN QUERY PLAN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # Numeric affinity stores INTEGER/REAL — nothing exact to preserve.
    exact_decimal_native: bool = False
    max_identifier_bytes: int | None = None  # unbounded
    url_scheme: str | None = "sqlite"

    regex_flavour: ClassVar[RegexFlavour] = PYTHON
    json_db_types: ClassVar[frozenset[str]] = frozenset()

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        """SQLite lacks the padding, reversing, repeating and separator functions: registered ones stand in."""
        if fn in ("lpad", "rpad"):
            return anon(f"slayer_{fn}", *args, *([] if len(args) > 2 else [lit(" ")]))
        if fn in ("reverse", "repeat", "concat_ws"):
            return anon(f"slayer_{fn}", *args)
        return super().build_text_call(fn, args)

    def code_point_char(self, n: Expression) -> Expression:
        return anon("CHAR", n)

    def first_code_point(self, s: Expression) -> Expression:
        return case((exp.EQ(this=s.copy(), expression=lit("")), lit(0)), default=anon("UNICODE", s))

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        return anon("slayer_split_part", value, delimiter, lit(index))

    def build_math_call(self, fn: MathFn, args: tuple[Expression, ...]) -> Expression:
        """Math functions are registered (SQLite builds may lack them); the domain guards stay."""
        if fn in ("asin", "acos"):
            inside = exp.Between(this=args[0].copy(), low=lit(-1), high=lit(1))
            return case((inside, anon(f"slayer_{fn}", args[0])))
        if fn == "div":
            return super().build_math_call(fn, args)
        return anon(f"slayer_{fn}", *args)

    def truncate(self, value: Expression) -> Expression:
        return anon("slayer_trunc", value)

    def build_extremum(self, fn: ExtremumFn, args: tuple[Expression, ...]) -> Expression:
        """Scalar ``MAX`` / ``MIN`` propagate NULL: each term coalesces to a present argument."""
        return self.coalesced_extremum(fn, args, spelling="min_max")

    def build_modulo(self, left: Expression, right: Expression, integer: bool) -> Expression:
        """``%`` truncates real operands to integers first; non-integers use ``fmod``."""
        if integer:
            return super().build_modulo(left, right, integer)
        return anon("slayer_fmod", left, right)

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        return anon("slayer_regexp_like", value, self.regex_text(pattern), lit("i" if case_insensitive else ""))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        return anon("slayer_regexp_extract", value, self.regex_text(pattern), lit(group))

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        portable = "".join(
            f"\\{p.index}" if isinstance(p, RegexGroupRef) else p.replace("\\", "\\\\") for p in replacement.parts
        )
        return anon("slayer_regexp_replace", value, self.regex_text(pattern), lit(portable),
                    lit("i" if case_insensitive else ""))

    def build_like(
        self, value: Expression, pattern: LikePattern | Expression, case_insensitive: bool,
    ) -> Expression:
        """``LIKE`` is ``GLOB`` over the converted pattern (case-sensitive whatever the collation);
        ``ILIKE`` is SQLite's own case-insensitive ``LIKE``."""
        if case_insensitive:
            return super().build_like(value, pattern, case_insensitive)
        text = lit(_like_source(pattern)) if isinstance(pattern, LikePattern) else pattern.copy()
        return exp.Glob(this=value.copy(), expression=anon("slayer_like_glob", text))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        kind, read = self._json_parts(value, path)
        text = case(
            (exp.In(this=kind, expressions=[lit("true"), lit("false")]), kind.copy()),
            (exp.In(this=kind.copy(), expressions=[lit("text"), lit("integer"), lit("real")]),
             exp.Cast(this=read, to=exp.DataType.build("TEXT"))),
        )
        return case((anon("JSON_VALID", value), text))

    def build_json_value_as(
        self, value: Expression, path: JsonPath, native: bool, to: exp.DataType, target: DataType,
    ) -> Expression:
        """A JSON number or boolean converts directly (never through its text); a JSON string by ``TRY_CAST``."""
        kind, read = self._json_parts(value, path)
        from_text = (exp.EQ(this=kind.copy(), expression=lit("text")), self.build_try_cast(read.copy(), to))
        direct: list = []
        if target is DataType.INT:
            direct = [(exp.EQ(this=kind.copy(), expression=lit("integer")), read.copy())]
        elif target is DataType.DOUBLE:
            direct = [(exp.In(this=kind.copy(), expressions=[lit("integer"), lit("real")]),
                       exp.Cast(this=read.copy(), to=exp.DataType.build("REAL")))]
        elif target is DataType.BOOLEAN:
            direct = [(exp.EQ(this=kind.copy(), expression=lit("true")), lit(1)),
                      (exp.EQ(this=kind.copy(), expression=lit("false")), lit(0))]
        elif target is DataType.TEXT:
            return self.build_json_value(value, path, native)
        return case((anon("JSON_VALID", value), case(*direct, from_text)))

    def _json_parts(self, value: Expression, path: JsonPath) -> tuple[Expression, Expression]:
        text = lit(path.text)
        return anon("JSON_TYPE", value, text), anon("JSON_EXTRACT", value, text)

    def declared_cast_type(self, dt: Optional[DataType]) -> Optional[DataType]:
        """No DATE / TIMESTAMP cast: under numeric affinity it collapses a text date to its year."""
        if dt in (DataType.DATE, DataType.TIMESTAMP):
            return None
        return dt

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        return exp.Anonymous(this="slayer_try_cast", expressions=[
            as_text(value), exp.Literal.string(to.sql(dialect="postgres")),
        ])

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """Bare ``IS``: ``IS NOT DISTINCT FROM`` needs SQLite ≥ 3.39."""
        return exp.Is(this=left, expression=right)

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """SQLite has no DATE_TRUNC: STRFTIME, a CASE for quarter, a weekday modifier for week."""
        if granularity == TimeGranularity.WEEK_SUNDAY:
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

    def promote_to_timestamp(self, expr: Expression) -> Expression:
        return exp.Anonymous(this="DATETIME", expressions=[expr.copy()])

    def build_temporal_literal(self, *, value: date, dt: DataType) -> Expression:
        """SQLite stores dates as text: a plain ISO literal compares and parses like a stored value."""
        return exp.Literal.string(iso_text(value))

    def build_current_timestamp(self) -> Expression:
        return exp.CurrentTimestamp()

    def build_temporal_comparison(
        self, *, op: TemporalComparisonOp, operand: Expression, value: date,
    ) -> Expression:
        """A midnight ``>=`` / ``<`` bound compares as date text (sargable); anything else
        normalises both sides to millisecond ISO text, independent of the stored spelling."""
        day = value.date() if isinstance(value, datetime) else value
        midnight = value == day or value == datetime(day.year, day.month, day.day)
        if op in (">=", "<") and midnight:
            return COMPARISON_NODES[op](this=operand, expression=exp.Literal.string(day.isoformat()))
        return COMPARISON_NODES[op](
            this=_normalised_timestamp(operand),
            expression=_normalised_timestamp(exp.Literal.string(iso_text(value))),
        )

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """STRFTIME components; ISO week/year via the week's Thursday (no ``%V``/``%G`` before 3.46)."""
        if part is DatePart.QUARTER:
            return exp.IntDiv(this=exp.Paren(this=exp.Add(
                this=_strftime_int("%m", expr), expression=exp.Literal.number(2),
            )), expression=exp.Literal.number(3))
        if part is DatePart.DAY_OF_WEEK:
            return exp.Add(this=exp.Paren(this=_days_since_monday(expr)), expression=exp.Literal.number(1))
        if part in (DatePart.WEEK, DatePart.ISO_YEAR):
            # Thursday of the ISO week: its year is the ISO year, its day-of-year fixes the week.
            thursday = exp.Anonymous(this="DATE", expressions=[expr, exp.DPipe(
                this=exp.Paren(this=exp.Sub(
                    this=exp.Literal.number(3), expression=exp.Paren(this=_days_since_monday(expr)),
                )),
                expression=exp.Literal.string(" days"),
            )])
            if part is DatePart.ISO_YEAR:
                return _strftime_int("%Y", thursday)
            return exp.Add(
                this=exp.IntDiv(this=exp.Paren(this=exp.Sub(
                    this=_strftime_int("%j", thursday), expression=exp.Literal.number(1),
                )), expression=exp.Literal.number(7)),
                expression=exp.Literal.number(1),
            )
        return _strftime_int(_STRFTIME_PARTS[part], expr)

    def _day_gap(self, *, start: Expression, end: Expression) -> Expression:
        def julian(e: Expression) -> Expression:
            return exp.Anonymous(this="JULIANDAY", expressions=[exp.Anonymous(this="DATE", expressions=[e])])
        return exp.Cast(this=exp.Sub(this=julian(end), expression=julian(start)), to=exp.DataType.build("INTEGER"))

    def _second_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.Sub(this=_strftime_int("%s", end), expression=_strftime_int("%s", start))

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """The ``slayer_date_add`` UDF: SQLite's own modifiers overflow past month-end."""
        return exp.Anonymous(this="slayer_date_add", expressions=[
            expr.copy(), count.copy(), exp.Literal.string(unit.value),
        ])

    def frame_time_operand(self, expr: Expression) -> Expression:
        """DATETIME-wrap: a bare-date text sorts before the same day's DATETIME frame bound."""
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
        """Emit JSON extraction in the ``json_extract(...)`` function-call form."""
        return rewrite_sqlite_json_extract(tree)

    def register_udfs(self, dbapi_connection) -> None:
        """Register the Python aggregate / scalar UDFs on the connection."""
        register_sqlite_udfs(dbapi_connection)


