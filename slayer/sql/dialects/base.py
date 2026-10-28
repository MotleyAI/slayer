"""SqlDialect strategy base class: Postgres-shaped defaults that dialects override."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, TypeGuard, get_args
from collections.abc import Callable, Iterable, Sequence

from pydantic import BaseModel, ConfigDict
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expression
from sqlglot.dialects.dialect import Dialect as _SqlglotDialect
from sqlglot.errors import SqlglotError

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.core.errors import IdentifierCollisionError, IdentifierLengthError, ServerVersionError
from slayer.core.granularity import CustomGranularity, Granularity, granularity_parts, nests_into
from slayer.core.json_path import JsonPath
from slayer.core.like_pattern import LikePattern
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.patterns import POSTGRES, RegexFlavour, emit_regex, emit_replacement, like_expression, like_text
from slayer.sql._identifier_fit import (
    SqlLexis,
    fit_identifier,
    overlimit_tokens,
    quoted_identifiers,
    substitute_quoted,
)
from slayer.sql.dialects.reflection import (
    CatalogEngine,
    ReflectedColumn,
    TableKeys,
    TableLocation,
    all_of,
    best_effort,
    clean_comment,
    col,
    column_from_type_string,
    eq_literal,
    fetch,
    inspector_column_names,
    inspector_foreign_keys,
    inspector_unique_constraints,
    inspector_unique_indexes,
    relation,
    schema_token,
    without_primary_key,
)
from slayer.sql.naming_bijection import decode_alias, encode_alias

if TYPE_CHECKING:
    import sqlalchemy as sa

    from slayer.core.models import DatasourceConfig


_GRANULARITY_TO_DATE_TRUNC = {
    TimeGranularity.SECOND: "second",
    TimeGranularity.MINUTE: "minute",
    TimeGranularity.HOUR: "hour",
    TimeGranularity.DAY: "day",
    TimeGranularity.WEEK: "week",
    TimeGranularity.MONTH: "month",
    TimeGranularity.QUARTER: "quarter",
    TimeGranularity.YEAR: "year",
}

_INTERVAL_UNITS: dict[TimeGranularity, tuple[str, int]] = {
    TimeGranularity.SECOND: ("SECOND", 1), TimeGranularity.MINUTE: ("MINUTE", 1),
    TimeGranularity.HOUR: ("HOUR", 1), TimeGranularity.DAY: ("DAY", 1),
    TimeGranularity.WEEK: ("WEEK", 1), TimeGranularity.WEEK_SUNDAY: ("WEEK", 1),
    TimeGranularity.MONTH: ("MONTH", 1), TimeGranularity.QUARTER: ("MONTH", 3),
    TimeGranularity.YEAR: ("YEAR", 1),
}
_UNIT_SECONDS = {TimeGranularity.MINUTE: 60, TimeGranularity.HOUR: 3600}
SUB_DAY_PARTS = frozenset({DatePart.HOUR, DatePart.MINUTE, DatePart.SECOND})


def literal_int(node: Expression) -> Optional[int]:
    """The value of an integer literal node (``-2`` included), else ``None``."""
    negative = isinstance(node, exp.Neg)
    inner = node.this if negative else node
    if not (isinstance(inner, exp.Literal) and not inner.is_string):
        return None
    try:
        value = int(inner.name)
    except ValueError:
        return None
    return -value if negative else value


StatAgg1Name = Literal["stddev_samp", "stddev_pop", "var_samp", "var_pop"]
StatAgg2Name = Literal["corr", "covar_samp", "covar_pop"]

def is_stat_agg1(name: str) -> TypeGuard[StatAgg1Name]:
    return name in get_args(StatAgg1Name)


def is_stat_agg2(name: str) -> TypeGuard[StatAgg2Name]:
    return name in get_args(StatAgg2Name)


TextFn = Literal[
    "left", "right", "starts_with", "ends_with", "contains", "lpad", "rpad", "reverse", "repeat", "concat_ws",
    "chr", "ascii", "trim", "ltrim", "rtrim", "strpos", "instr", "replace",
]
TrimFn = Literal["trim", "ltrim", "rtrim"]
MathFn = Literal["div", "pi", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "cbrt"]
ExtremumFn = Literal["greatest", "least"]


def is_text_fn(name: str) -> TypeGuard[TextFn]:
    return name in get_args(TextFn)


def is_math_fn(name: str) -> TypeGuard[MathFn]:
    return name in get_args(MathFn)


def is_extremum_fn(name: str) -> TypeGuard[ExtremumFn]:
    return name in get_args(ExtremumFn)


def anon(name: str, *args: Expression) -> exp.Anonymous:
    """``NAME(args)`` emitted as written on every dialect (no transpilation)."""
    return exp.Anonymous(this=name, expressions=[a.copy() for a in args])


def case(*branches: tuple[Expression, Expression], default: Optional[Expression] = None) -> exp.Case:
    return exp.Case(ifs=[exp.If(this=c, true=v) for c, v in branches], default=default)


def lit(value: int | str) -> Expression:
    return exp.Literal.number(value) if isinstance(value, int) else exp.Literal.string(value)


def is_null(value: Expression) -> Expression:
    return exp.Is(this=operand_copy(value), expression=exp.Null())


def either(*conditions: Expression) -> Expression:
    """``c1 OR c2 OR …``."""
    out = conditions[0]
    for condition in conditions[1:]:
        out = exp.Or(this=out, expression=condition)
    return out


def any_null(*values: Expression) -> Expression:
    return either(*(is_null(v) for v in values))


def func(name: str, *args: Expression) -> Expression:
    """``exp.func``: the named sqlglot function, transpiled per dialect."""
    node = exp.func(name, *(a.copy() for a in args))
    assert isinstance(node, Expression)  # concrete Func classes and Anonymous all are
    return node


def pad_fill(args: Sequence[Expression]) -> Expression:
    """``lpad`` / ``rpad``'s fill: the third argument, else one space."""
    return args[2] if len(args) > 2 else lit(" ")


def literal_value(node: Expression) -> Optional[int]:
    """An integer literal's value (``-2`` included), else ``None``."""
    return literal_int(node)


#: Code points ``chr`` takes: 1 to 0x10FFFF without the surrogates.
CHR_RANGES = ((1, 0xD7FF), (0xE000, 0x10FFFF))


_OPERATOR_SHAPED = (exp.Binary, exp.Unary, exp.Between, exp.In)


def is_operator(node: object) -> bool:
    """Whether ``node`` is an operator expression (needs parens as an operand)."""
    return isinstance(node, _OPERATOR_SHAPED) and not isinstance(node, exp.Paren)


def operand_copy(value: Expression) -> Expression:
    """A copy of ``value`` safe to place as an operator's operand."""
    return exp.Paren(this=value.copy()) if is_operator(value) else value.copy()


def iso_text(value: date) -> str:
    """``YYYY-MM-DD``, or ``YYYY-MM-DD HH:MM:SS[.ffffff]`` for a datetime."""
    return str(value)


TemporalComparisonOp = Literal["=", "!=", "<", "<=", ">", ">="]
COMPARISON_NODES: dict[str, Any] = {
    "=": exp.EQ, "!=": exp.NEQ, "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE,
}


#: The validity grammar where a database cannot check a cast itself: a signed decimal with an optional
#: fraction and exponent, surrounding spaces ignored (POSIX classes, so no backslash meets a string literal).
NUMBER_TEXT_PATTERN = r"^[[:space:]]*[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?[[:space:]]*$"
TRUE_WORDS = ("true", "t", "yes", "1")
FALSE_WORDS = ("false", "f", "no", "0")


#: ``meta`` tag on the cast ``read_column`` wraps around a zoned column.
ZONED_READ = "slayer_zoned_read"


def bare_comparands(left: Expression, right: Expression) -> tuple[Expression, Expression]:
    """A zoned column read stays bare where the other side is already in the zoned type (a ``TIMESTAMP``
    cast, ISO text, another zoned column), so the comparison keeps partition pruning."""
    def zoned(node: Expression) -> bool:
        return isinstance(node, exp.Cast) and bool(node.meta.get(ZONED_READ))

    def in_zoned_type(node: Expression) -> bool:
        return zoned(node) or (isinstance(node, exp.Literal) and node.is_string) or (
            isinstance(node, exp.Cast) and node.to.this is exp.DataType.Type.TIMESTAMPTZ)

    new_left = left.this if zoned(left) and in_zoned_type(right) else left
    new_right = right.this if zoned(right) and in_zoned_type(left) else right
    return new_left, new_right


def as_text(value: Expression) -> Expression:
    """``CAST(value AS TEXT)`` — the input a validity check reads."""
    return exp.Cast(this=value.copy(), to=exp.DataType.build("TEXT"))


def cast_target(type_text: str, *, dialect: "SqlDialect") -> exp.DataType:
    """The author's cast type as ``dialect``'s ``DataType``; ``ValueError`` when it cannot express it."""
    sqlglot_name = dialect.sqlglot_name
    built: Optional[exp.DataType] = None
    for reader in (sqlglot_name, "postgres"):
        try:
            built = exp.DataType.build(type_text, dialect=reader)
            break
        except (SqlglotError, ValueError):
            continue
    if built is None or built.this == exp.DataType.Type.USERDEFINED or not dialect.cast_type_supported(built):
        raise ValueError(f"CAST to {type_text} cannot be expressed on {sqlglot_name}.")
    return built


#: Types only some databases have; a dialect without one rejects a CAST to it at plan time.
VENDOR_ONLY_CAST_TYPES: frozenset[DType] = frozenset({
    exp.DataType.Type.MONEY, exp.DataType.Type.SMALLMONEY,
})


def temporal_type_of(value: date) -> DataType:
    return DataType.TIMESTAMP if isinstance(value, datetime) else DataType.DATE


def _build_covar_decomposition(
    *,
    col_expr: Expression,
    other_expr: Expression,
    agg: StatAgg2Name,
    var_fn_samp: str,
    var_fn_pop: str,
    stddev_fn: str,
) -> Expression:
    """corr / covar via ``cov(x, y) = (Var(x+y) - Var(x) - Var(y)) / 2`` for dialects without them.

    Each leg is NULL-guarded by the other; ``exp.Anonymous`` calls dodge sqlglot's
    MySQL VAR_SAMP → VARIANCE (= VAR_POP) rewrite.
    """
    var_fn = var_fn_samp if agg in ("covar_samp", "corr") else var_fn_pop

    def _guarded(value: Expression, guard: Expression) -> Expression:
        return exp.Case(ifs=[exp.If(
            this=exp.Not(this=exp.Is(this=operand_copy(guard), expression=exp.Null())),
            true=value.copy(),
        )])

    x_guarded = _guarded(col_expr, other_expr)
    y_guarded = _guarded(other_expr, col_expr)

    def _call(fn: str, *args: Expression) -> exp.Anonymous:
        return exp.Anonymous(this=fn, expressions=[a.copy() for a in args])

    xy_sum = exp.Add(this=x_guarded.copy(), expression=y_guarded.copy())
    covar = exp.Div(
        this=exp.Paren(this=exp.Sub(
            this=exp.Sub(this=_call(var_fn, xy_sum), expression=_call(var_fn, x_guarded)),
            expression=_call(var_fn, y_guarded),
        )),
        expression=exp.Literal.number(2),
    )

    if agg != "corr":
        return covar

    raw_denom = exp.Paren(this=exp.Mul(
        this=_call(stddev_fn, x_guarded), expression=_call(stddev_fn, y_guarded),
    ))
    denom = exp.Anonymous(
        this="NULLIF", expressions=[raw_denom, exp.Literal.number(0)]
    )
    return exp.Div(this=covar, expression=denom)


@lru_cache(maxsize=None)
def _sqlglot_backslash_escapes(sqlglot_name: str) -> bool:
    """Whether sqlglot's tokenizer treats backslash as a string escape."""
    tokenizer = _SqlglotDialect.get_or_raise(sqlglot_name).tokenizer_class
    escapes = getattr(tokenizer, "STRING_ESCAPES", None)
    if not isinstance(escapes, (list, tuple, set, frozenset)):
        raise RuntimeError(
            f"Cannot derive the backslash-escaping regime for sqlglot dialect "
            f"{sqlglot_name!r}: its tokenizer's STRING_ESCAPES is "
            f"{type(escapes).__name__}, expected a collection of strings. A "
            f"sqlglot upgrade may have changed this internal API."
        )
    return "\\" in escapes


@lru_cache(maxsize=None)
def _sqlglot_masking_lexis(sqlglot_name: str) -> SqlLexis:
    """Lexical rules the identifier masker needs, read from sqlglot's tokenizer."""
    tokenizer = _SqlglotDialect.get_or_raise(sqlglot_name).tokenizer_class
    nested = getattr(tokenizer, "NESTED_COMMENTS", None)
    heredoc = getattr(tokenizer, "HEREDOC_STRINGS", None)
    raw = getattr(tokenizer, "RAW_STRINGS", None)
    if not isinstance(nested, bool) or not isinstance(
        heredoc, (list, tuple, set, frozenset)
    ) or not isinstance(raw, (list, tuple, set, frozenset)):
        raise RuntimeError(
            f"Cannot derive the masking lexis for sqlglot dialect {sqlglot_name!r}: "
            f"tokenizer NESTED_COMMENTS/HEREDOC_STRINGS/RAW_STRINGS shape changed. A "
            f"sqlglot upgrade may have changed these internal APIs."
        )
    return SqlLexis(
        backslash_escapes=_sqlglot_backslash_escapes(sqlglot_name),
        nested_comments=nested,
        dollar_quotes="$" in heredoc or any("$" in str(r) for r in raw),
    )


def _scheme_driver(url_scheme: str | None) -> str | None:
    return (url_scheme or "").partition("+")[2] or None


def _digest(secret: str | None) -> str:
    """Non-reversible 16-hex id for secret material in cache keys."""
    if not secret:
        return ""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]


class ServerProfile(BaseModel):
    """What the server reports about itself and the connected user; ``None`` = unknown."""

    model_config = ConfigDict(frozen=True)

    version: tuple[int, int] | None = None
    readonly: int | None = None
    correlated_subqueries: bool | None = None


class DriverFacts(BaseModel):
    """The URL scheme, drivers and pip extra one datasource type connects with."""

    model_config = ConfigDict(frozen=True)

    url_scheme: str | None = None
    url_backend_aliases: frozenset[str] = frozenset()
    async_driver: str | None = None
    install_extra: str | None = None

    @property
    def url_backend(self) -> str:
        """The SQLAlchemy backend name of ``url_scheme``."""
        return (self.url_scheme or "").partition("+")[0]

    @property
    def url_backends(self) -> frozenset[str]:
        """Every SQLAlchemy backend this type serves; none without a scheme."""
        if self.url_scheme is None:
            return frozenset()
        return frozenset({self.url_backend}) | self.url_backend_aliases

    @property
    def sync_driver(self) -> str | None:
        """The default driver: ``url_scheme``'s ``+driver`` part."""
        return _scheme_driver(self.url_scheme)

    def pin_default_driver(self, url: str) -> str:
        """``url`` with the default driver named when it names none on a served backend; else unchanged."""
        if self.sync_driver is None:
            return url
        try:
            parsed = make_url(url)
        except (ArgumentError, ValueError):
            return url
        backend = parsed.get_backend_name()
        if "+" in parsed.drivername or backend not in self.url_backends:
            return url
        return parsed.set(drivername=f"{backend}+{self.sync_driver}").render_as_string(hide_password=False)


class SqlDialect(BaseModel):
    """One database's quirks; the base IS the Postgres-shaped default, subclasses override what differs."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    sqlglot_name: str = "postgres"
    ds_type_aliases: frozenset[str] = frozenset()
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    # Whether NUMERIC/DECIMAL is stored exactly (False: SQLite's numeric affinity).
    exact_decimal_native: bool = True

    # ``AVG`` over integers returns a truncated integer, so a non-DOUBLE input is read as a float.
    integer_avg: bool = False

    # Identifier budget in bytes (``None`` = unbounded); defaults to Postgres, the tightest.
    max_identifier_bytes: int | None = 63

    # Oracle/T-SQL name the call: sqlglot mis-spells theirs.
    approx_count_distinct_native: bool = False
    approx_count_distinct_anonymous_name: str | None = None

    # A rejected ``statement_timeout_sql`` is rolled back and reported instead of raised.
    statement_timeout_best_effort: bool = False

    # Correlated subqueries need a server setting this user may be unable to set.
    correlated_subqueries_gated: bool = False

    # Whether setting the statement timeout depends on the server profile (probed only then).
    timeout_reads_profile: bool = False

    # The server's profile, when SQL generation reads it (``profile_needed``); unknown otherwise.
    server_profile: ServerProfile = ServerProfile()

    # An ``IN`` subquery and its joins run once on the initiator (``GLOBAL``): exact over shards.
    global_in_subqueries: bool = False

    # URL scheme (``None`` = datasource type; its ``+driver`` is the default driver), extra URL
    # backends served, async DBAPI driver, and their pip extra; read through ``driver_facts``.
    url_scheme: str | None = None
    url_backend_aliases: frozenset[str] = frozenset()
    async_driver: str | None = None
    install_extra: str | None = None

    # SQLAlchemy backend names (``engine.dialect.name``) whose structure this dialect reflects.
    sa_backend_names: frozenset[str] = frozenset()

    # Declared-INT columns may store non-integers (SQLite affinity): ingestion probes them.
    probes_integer_affinity: bool = False

    # The Inspector's PK is final: an empty one means "no PK", never an INFORMATION_SCHEMA retry.
    primary_key_inspector_only: bool = False

    @property
    def sync_driver(self) -> str | None:
        """The default driver: ``url_scheme``'s ``+driver`` part."""
        return _scheme_driver(self.url_scheme)

    def driver_facts(self, ds_type: str | None) -> DriverFacts:
        """``ds_type``'s driver facts: this dialect's when it serves ``ds_type``, else only ``ds_type`` as scheme."""
        if ds_type is None or ds_type not in self.ds_type_aliases:
            return DriverFacts(url_scheme=ds_type)
        return DriverFacts(
            url_scheme=self.url_scheme or ds_type,
            url_backend_aliases=self.url_backend_aliases,
            async_driver=self.async_driver,
            install_extra=self.install_extra,
        )

    @property
    def backslash_escapes_strings(self) -> bool:
        """Whether string literals treat backslash as an escape; feeds Mode-A ``{var}`` escaping."""
        return _sqlglot_backslash_escapes(self.sqlglot_name)

    @property
    def identifier_masking_lexis(self) -> SqlLexis:
        """Lexical rules for the identifier masker, including this dialect's identifier quotes."""
        return _sqlglot_masking_lexis(self.sqlglot_name).model_copy(
            update={"identifier_quote": self._identifier_quote_anchors()},
        )

    def declared_cast_type(self, dt: Optional[DataType]) -> Optional[DataType]:
        """The CAST target for a value of type ``dt`` (``None`` skips the cast)."""
        return dt

    # Vendor-only types sqlglot parses for every dialect; a dialect lists the ones it has.
    vendor_cast_types: ClassVar[frozenset[DType]] = frozenset()

    def cast_type_supported(self, data_type: exp.DataType) -> bool:
        """Whether this dialect can express a CAST to ``data_type``."""
        return data_type.this not in VENDOR_ONLY_CAST_TYPES or data_type.this in self.vendor_cast_types

    #: Column ``db_type``s holding a zoned instant where SLayer's TIMESTAMP is naive (BigQuery ``TIMESTAMP``).
    zoned_timestamp_db_types: ClassVar[frozenset[str]] = frozenset()

    def is_zoned_db_type(self, db_type: Optional[str]) -> bool:
        """Whether a column of ``db_type`` holds a zoned instant on this dialect."""
        return bool(db_type) and db_type.split("(")[0].strip().upper() in self.zoned_timestamp_db_types

    def read_column(self, column: Expression, *, zoned: bool) -> Expression:  # NOSONAR(S1172) — hook parameter read by dialect overrides
        """A column as SLayer reads it; default: as stored."""
        return column

    def try_cast_type(self, to: exp.DataType, *, data_type: DataType) -> exp.DataType:  # NOSONAR(S1172) — hook parameter read by dialect overrides
        """The portable ``TRY_CAST`` target of a cast to ``data_type``; default: the database's own type."""
        return to

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        """``TRY_CAST``: NULL for an input the cast cannot convert; the input is read as text first."""
        return exp.TryCast(this=as_text(value), to=to)

    # A native case-insensitive ``ILIKE``; elsewhere it is ``LOWER(x) LIKE LOWER(p)``.
    ilike_native: ClassVar[bool] = False

    def build_ilike(self, value: Expression, pattern: Expression) -> Expression:
        if self.ilike_native:
            return exp.ILike(this=value, expression=pattern)
        return exp.Like(this=exp.Lower(this=value), expression=exp.Lower(this=pattern))

    # -- The portable library (spec: queries/scalar-functions, regex, json, string-matching) -------

    #: How this engine spells the portable regex.
    regex_flavour: ClassVar[RegexFlavour] = POSTGRES
    #: This engine's JSON column types (a column of one is read natively by ``json_value``).
    json_db_types: ClassVar[frozenset[str]] = frozenset({"JSON"})
    #: Whether LIKE takes ``ESCAPE '\'`` (else backslash already escapes).
    like_escape_clause: ClassVar[bool] = True
    #: Whether ``[`` is a LIKE metacharacter (SQL Server).
    like_brackets: ClassVar[bool] = False

    def profile_needed(self, keys: Iterable[object]) -> bool:  # NOSONAR(S1172) — no-op hook default
        """Whether rendering ``keys`` reads ``server_profile``."""
        return False

    def require_version(self, *, minimum: tuple[int, int], what: str, release: str) -> None:
        """Fail closed when the known server version is below ``minimum``; an unknown version emits."""
        version = self.server_profile.version
        if version is not None and version < minimum:
            raise ServerVersionError(
                summary=f"{what} needs {release} or later; the server reports version {version[0]}.{version[1]}.",
                suggestion=f"Upgrade to {release}, or express the computation without {what}.",
            )

    def char_length(self, value: Expression) -> Expression:
        """The length of ``value`` in characters."""
        return self.rewrite_target_ast(exp.Length(this=value.copy()))

    def char_substring(self, value: Expression, start: Expression, length: Expression) -> Expression:
        """``length`` characters of ``value`` from the 1-based ``start``."""
        return self.rewrite_target_ast(exp.Substring(this=value.copy(), start=start.copy(), length=length.copy()))

    def text_repeat(self, value: Expression, count: Expression) -> Expression:
        """``value`` repeated ``count`` (≥ 1) times."""
        return anon("REPEAT", value, count)

    def truncate(self, value: Expression) -> Expression:
        """``value`` truncated toward zero."""
        return self.rewrite_target_ast(func("TRUNC", value))

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        """A library string function with its Postgres meaning (characters, never bytes)."""
        s = args[0]
        if fn in ("trim", "ltrim", "rtrim"):
            return self.trim_spaces(fn, s)
        if fn in ("left", "right"):
            return self._left_right(fn=fn, s=s, n=args[1])
        if fn == "starts_with":
            head = self.char_substring(s, lit(1), self.char_length(args[1]))
            return exp.EQ(this=self.binary_text(head), expression=self.binary_text(args[1]))
        if fn == "ends_with":
            p = args[1]
            start = case((exp.GTE(this=self.char_length(s), expression=self.char_length(p)),
                          exp.Add(this=exp.Sub(this=self.char_length(s), expression=self.char_length(p)),
                                  expression=lit(1))), default=lit(1))
            tail = self.char_substring(s, start, self.char_length(p))
            return exp.EQ(this=self.binary_text(tail), expression=self.binary_text(p))
        if fn == "contains":
            return exp.GT(this=self.text_position(s, args[1]), expression=lit(0))
        if fn in ("strpos", "instr"):
            return self.text_position(s, args[1])
        if fn == "replace":
            return self.rewrite_target_ast(func("REPLACE", *args))
        if fn in ("lpad", "rpad"):
            return self._pad(fn=fn, s=s, n=args[1], fill=pad_fill(args))
        if fn == "reverse":
            return anon("REVERSE", s)
        if fn == "repeat":
            return self._non_positive_empty(args[1], self.text_repeat(s, args[1]), source=s)
        if fn == "concat_ws":
            return anon("CONCAT_WS", *args)
        if fn == "chr":
            return self._code_point_guard(s, self.code_point_char(s))
        return self.first_code_point(s)

    def binary_text(self, value: Expression) -> Expression:
        """``value`` compared case- and accent-sensitively whatever its collation; default: as is."""
        return value.copy()

    def text_position(self, s: Expression, p: Expression) -> Expression:
        """The 1-based character position of the first ``p`` in ``s``, 0 when absent, compared binary."""
        return self.rewrite_target_ast(exp.StrPosition(this=self.binary_text(s), substr=self.binary_text(p)))

    def trim_spaces(self, fn: TrimFn, s: Expression) -> Expression:
        """Postgres ``trim`` / ``ltrim`` / ``rtrim``: spaces only, never other whitespace."""
        return self.rewrite_target_ast(func(fn.upper(), s))

    def _left_right(self, *, fn: TextFn, s: Expression, n: Expression) -> Expression:
        """Postgres ``left`` / ``right``: a negative count drops that many characters from the other end."""
        length = self.char_length(s)
        kept = exp.Add(this=length, expression=n.copy())
        if fn == "left":
            positive = self.char_substring(s, lit(1), n)
            negative = self.char_substring(s, lit(1), kept)
        else:
            positive = self.char_substring(s, exp.Add(this=exp.Sub(this=length.copy(), expression=n.copy()),
                                                      expression=lit(1)), n)
            negative = self.char_substring(s, exp.Sub(this=lit(1), expression=n.copy()), kept.copy())
        branches: list[tuple[Expression, Expression]] = [(any_null(s, n), exp.Null())]
        if fn == "right":
            branches.append((exp.GTE(this=n.copy(), expression=length.copy()), s.copy()))
        branches += [(exp.GTE(this=n.copy(), expression=lit(0)), positive),
                     (exp.GT(this=kept.copy(), expression=lit(0)), negative)]
        return case(*branches, default=lit(""))

    def _non_positive_empty(self, n: Expression, value: Expression, *, source: Expression) -> Expression:
        """``value``, or ``''`` for a count ≤ 0 (a NULL ``source`` stays NULL)."""
        count = literal_value(n)
        if count is not None:
            return value if count > 0 else case((exp.Not(this=any_null(source)), lit("")))
        return case((exp.LTE(this=n.copy(), expression=lit(0)), case((exp.Not(this=any_null(source)), lit("")))),
                    default=value)

    def _pad(self, *, fn: TextFn, s: Expression, n: Expression, fill: Expression) -> Expression:
        node = anon("LPAD" if fn == "lpad" else "RPAD", s, n, fill)
        return self._non_positive_empty(n, node, source=s)

    def portable_pad(self, *, fn: TextFn, s: Expression, n: Expression, fill: Expression) -> Expression:
        """``lpad`` / ``rpad`` built from substring and repeat, for an engine without them."""
        length = self.char_length(s)
        padding = self.char_substring(self.text_repeat(fill, n), lit(1), exp.Sub(this=n.copy(), expression=length))
        padded = self.concat_text(padding, s) if fn == "lpad" else self.concat_text(s, padding)
        return case(
            (any_null(s, n, fill), exp.Null()),
            (exp.LTE(this=n.copy(), expression=lit(0)), lit("")),
            (exp.GTE(this=length.copy(), expression=n.copy()), self.char_substring(s, lit(1), n)),
            (exp.EQ(this=self.char_length(fill), expression=lit(0)), s.copy()),
            default=padded,
        )

    def concat_text(self, left: Expression, right: Expression) -> Expression:
        return exp.DPipe(this=left.copy(), expression=right.copy())

    def field_count(self, value: Expression, delimiter: Expression) -> Expression:
        """How many ``delimiter``-separated fields ``value`` has."""
        removed = self.char_length(anon("REPLACE", value, delimiter, lit("")))
        gap = exp.Paren(this=exp.Sub(this=self.char_length(value), expression=removed))
        per_field = exp.Nullif(this=self.char_length(delimiter), expression=lit(0))
        return exp.Add(this=exp.IntDiv(this=gap, expression=per_field), expression=lit(1))

    def code_point_char(self, n: Expression) -> Expression:
        """The character of Unicode code point ``n`` (in range)."""
        return anon("CHR", n)

    def _code_point_guard(self, n: Expression, value: Expression) -> Expression:
        """``value`` where ``n`` is a Unicode scalar value, else NULL."""
        inside = either(*(exp.Between(this=n.copy(), low=lit(lo), high=lit(hi)) for lo, hi in CHR_RANGES))
        return case((inside, value))

    def first_code_point(self, s: Expression) -> Expression:
        """The code point of ``s``'s first character; 0 for ``''``."""
        return anon("ASCII", s)

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        """The ``index``-th field (negative: from the end); ``''`` beyond the last."""
        return anon("SPLIT_PART", value, delimiter, lit(index))

    def build_math_call(self, fn: MathFn, args: tuple[Expression, ...]) -> Expression:
        """A library math function; an out-of-domain argument yields NULL."""
        if fn == "div":
            divisor = exp.Nullif(this=operand_copy(args[1]), expression=lit(0))
            return self.truncate(exp.Div(this=operand_copy(args[0]), expression=divisor))
        if fn == "pi":
            return anon("PI")
        if fn in ("asin", "acos"):
            inside = exp.Between(this=args[0].copy(), low=lit(-1), high=lit(1))
            return case((inside, self.rewrite_target_ast(func(fn.upper(), args[0]))))
        if fn == "cbrt":
            return self.cube_root(args[0])
        return self.rewrite_target_ast(func(fn.upper(), *args))

    def cube_root(self, value: Expression) -> Expression:
        return anon("CBRT", value)

    def build_extremum(self, fn: ExtremumFn, args: tuple[Expression, ...]) -> Expression:
        """``greatest`` / ``least`` of the non-NULL arguments; NULL only when all are NULL."""
        return anon(fn.upper(), *args)

    def coalesced_extremum(
        self, fn: ExtremumFn, args: tuple[Expression, ...], *, spelling: Literal["upper", "lower", "min_max"],
    ) -> Expression:
        """``GREATEST(COALESCE(a, b, c), COALESCE(b, a, c), …)`` for an engine whose function propagates NULL
        (``min_max``: the scalar ``MAX`` / ``MIN``)."""
        terms = [exp.Coalesce(this=a.copy(), expressions=[o.copy() for j, o in enumerate(args) if j != i])
                 for i, a in enumerate(args)]
        name = {"upper": fn.upper(), "lower": fn, "min_max": "MAX" if fn == "greatest" else "MIN"}[spelling]
        return anon(name, *terms)

    def build_modulo(self, left: Expression, right: Expression, integer: bool) -> Expression:  # NOSONAR(S1172) — hook parameter read by dialect overrides
        """``left % right`` truncated (the result takes the dividend's sign); ``integer``: both are integers."""
        return exp.Mod(this=operand_copy(left), expression=operand_copy(right))

    # Regex

    def regex_text(
        self, pattern: RegexPattern, *, case_insensitive: bool = False, capture: Optional[int] = None,
        wrap: bool = False, anchored: bool = False,
    ) -> Expression:
        return lit(emit_regex(pattern, self.regex_flavour, case_insensitive=case_insensitive, capture=capture,
                              wrap=wrap, anchored=anchored))

    def replacement_text(self, replacement: RegexReplacement) -> Expression:
        return lit(emit_replacement(replacement, self.regex_flavour))

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        """Whether ``pattern`` matches anywhere in ``value``."""
        node = exp.RegexpILike if case_insensitive else exp.RegexpLike
        return node(this=value.copy(), expression=self.regex_text(pattern))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        """The first match (``group`` 0) or its ``group``; NULL on no match, ``''`` for a group that did not
        take part."""
        match = anon("REGEXP_MATCH", value, self.regex_text(pattern, wrap=True),
                     *([lit("i")] if case_insensitive else []))
        picked = exp.Bracket(this=exp.Paren(this=match), expressions=[lit(group)])
        return self.skipped_group_empty(value, pattern, group, picked)

    def skipped_group_empty(
        self, value: Expression, pattern: RegexPattern, group: int, extracted: Expression,
    ) -> Expression:
        """``extracted`` with a skipped group's NULL read as ``''`` (NULL stays for no match)."""
        if group == 0:
            return extracted
        matched = self.build_regexp_like(value, pattern, False)
        return case((matched, exp.Coalesce(this=extracted, expressions=[lit("")])))

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        """``value`` with every match replaced."""
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern), self.replacement_text(replacement),
                    lit("gi" if case_insensitive else "g"))

    # LIKE

    def like_pattern(self, pattern: LikePattern | Expression) -> Expression:
        """The pattern as this engine's backslash-escaped LIKE text."""
        if isinstance(pattern, LikePattern):
            return lit(like_text(pattern, brackets=self.like_brackets))
        return like_expression(pattern.copy(), brackets=self.like_brackets)

    def build_like(
        self, value: Expression, pattern: LikePattern | Expression, case_insensitive: bool,
    ) -> Expression:
        """``LIKE`` / ``ILIKE`` in the one LIKE pattern language."""
        text = self.like_pattern(pattern)
        if not case_insensitive:
            node: Expression = exp.Like(this=value.copy(), expression=text)
        elif self.ilike_native:
            node = exp.ILike(this=value.copy(), expression=text)
        else:
            node = exp.Like(this=exp.Lower(this=value.copy()), expression=exp.Lower(this=text))
        return exp.Escape(this=node, expression=lit("\\")) if self.like_escape_clause else node

    # JSON

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        """The scalar at ``path`` as TEXT; NULL when missing, JSON null, an object or an array."""
        jsonb = exp.DataType.build("JSONB")
        already = isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.JSONB)
        doc = value.copy() if already else exp.Cast(this=value.copy(), to=jsonb)
        keys = [lit(str(step)) for step in path.steps]
        found = anon("JSONB_EXTRACT_PATH", doc, *keys)
        scalar = exp.In(this=anon("JSONB_TYPEOF", found), expressions=[lit("string"), lit("number"), lit("boolean")])
        read = case((scalar, anon("JSONB_EXTRACT_PATH_TEXT", doc, *keys)))
        if native or not self.validates_json_input():
            return read
        valid = anon("PG_INPUT_IS_VALID", as_text(value), lit("jsonb"))
        return case((valid, read))

    def validates_json_input(self) -> bool:
        """Whether malformed JSON text can be detected (Postgres 16+)."""
        version = self.server_profile.version
        return version is not None and version >= (16, 0)

    def build_json_value_as(
        self, value: Expression, path: JsonPath, native: bool, to: exp.DataType, target: DataType,  # NOSONAR(S1172) — hook parameter read by dialect overrides
    ) -> Expression:
        """``json_value(… RETURNING target)``: the scalar converted, NULL where it cannot convert."""
        return self.build_try_cast(self.build_json_value(value, path, native), to)

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """Null-safe equality (``IS NOT DISTINCT FROM``) for grain join-backs."""
        return exp.NullSafeEQ(this=left, expression=right)

    def build_ordered(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """The one builder of ``ORDER BY`` terms; ``"default"`` renders nulls last."""
        kwargs: dict = {"this": order_col, "desc": descending}
        if nulls == "first":
            kwargs["nulls_first"] = True
        elif nulls == "last":
            kwargs["nulls_first"] = False
        return exp.Ordered(**kwargs)

    def build_order_terms(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
        resolved: Optional[Expression] = None,  # NOSONAR(S1172) — read by the T-SQL emulation
    ) -> list[exp.Ordered]:
        """The ``ORDER BY`` terms of one entry; ``resolved`` is the expression ``order_col`` names, when it is
        an alias of the SELECT being ordered."""
        return [self.build_ordered(order_col, descending=descending, nulls=nulls)]

    def native_nulls_first(self, *, descending: bool) -> bool:
        """Where NULLs natively sort for ``descending``, read from sqlglot's emitter."""
        ordering = getattr(
            _SqlglotDialect.get_or_raise(self.sqlglot_name),
            "NULL_ORDERING", None,
        )
        if ordering == "nulls_are_last":
            return False
        if ordering == "nulls_are_small":
            return not descending
        if ordering == "nulls_are_large":
            return descending
        raise RuntimeError(
            f"Cannot derive the native null ordering for sqlglot dialect "
            f"{self.sqlglot_name!r}: NULL_ORDERING is {ordering!r}. A sqlglot "
            f"upgrade may have changed this API.",
        )

    @staticmethod
    def _expanded_null_safe_eq(
        left: Expression, right: Expression,
    ) -> Expression:
        """Two-valued ``(l = r AND l IS NOT NULL AND r IS NOT NULL) OR (l IS NULL AND r IS NULL)``, safe to negate."""
        def is_null(side: Expression) -> Expression:
            return exp.Is(this=side.copy(), expression=exp.Null())

        both_known = exp.and_(
            exp.EQ(this=left.copy(), expression=right.copy()),
            exp.Not(this=is_null(left)),
            exp.Not(this=is_null(right)),
        )
        both_null = exp.And(this=is_null(left), expression=is_null(right))
        return exp.paren(exp.Or(this=exp.paren(both_known), expression=exp.paren(both_null)))

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """``DATE_TRUNC``; other operands are cast to TIMESTAMP so Postgres picks an overload."""
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # Sunday week = Monday week of (col + 1 day), shifted back a day.
            shifted = self.build_date_add(
                expr=col_expr, count=exp.Literal.number(1), unit=TimeGranularity.DAY,
                operand=DataType.TIMESTAMP,
            )
            monday = self.build_date_trunc(
                col_expr=shifted, granularity=TimeGranularity.WEEK,
            )
            return self.build_date_add(
                expr=monday, count=exp.Literal.number(-1), unit=TimeGranularity.DAY,
                operand=DataType.TIMESTAMP,
            )
        gran_str = _GRANULARITY_TO_DATE_TRUNC.get(granularity, granularity.value)
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        return exp.DateTrunc(this=col_expr, unit=exp.Literal.string(gran_str))

    def build_bucket(self, *, col_expr: Expression, granularity: Granularity) -> Expression:
        """Bucket start of ``col_expr``; custom: last ``origin + k·multiple·base`` at or before it."""
        if not isinstance(granularity, CustomGranularity):
            return self.build_date_trunc(col_expr=col_expr, granularity=granularity)
        base, multiple, origin = granularity_parts(granularity)
        ts = self.promote_to_timestamp(col_expr)
        anchor = self.build_temporal_literal(value=origin, dt=DataType.TIMESTAMP)

        def at(count: Expression) -> Expression:
            return self.build_date_add(expr=anchor, count=count, unit=base, operand=DataType.TIMESTAMP)

        crossed = self.build_date_diff(unit=base, start=anchor, end=ts, operand=DataType.TIMESTAMP)
        # Boundaries crossed equal whole periods unless the origin is off the base's own boundaries.
        whole = crossed if nests_into(base, granularity) else exp.Case(ifs=[exp.If(
            this=exp.GT(this=at(crossed), expression=ts.copy()),
            true=exp.Sub(this=exp.Paren(this=crossed.copy()), expression=exp.Literal.number(1)),
        )], default=crossed.copy())
        if multiple == 1:
            return at(whole)
        m = exp.Literal.number(multiple)

        def natural_div(value: Expression) -> Expression:
            return self.natural_div(exp.Paren(this=value), divisor=m.copy())

        floored = exp.Case(ifs=[exp.If(
            this=exp.GTE(this=exp.Paren(this=whole.copy()), expression=exp.Literal.number(0)),
            true=natural_div(whole.copy()),
        )], default=exp.Neg(this=exp.Paren(this=natural_div(exp.Add(
            this=exp.Neg(this=exp.Paren(this=whole.copy())), expression=exp.Literal.number(multiple - 1),
        )))))
        return at(exp.Mul(this=exp.Paren(this=floored), expression=m))

    def natural_div(self, value: Expression, *, divisor: Expression) -> Expression:
        """``value // divisor`` for non-negative integers."""
        return exp.IntDiv(this=value, expression=divisor)

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        """``SELECT i`` over the integers ``0 .. size - 1``; default: a recursive CTE."""
        seq = exp.to_identifier("_seq")
        step = exp.select(exp.Add(this=exp.column("i"), expression=exp.Literal.number(1))).from_(
            exp.Table(this=seq.copy()),
        ).where(exp.LT(this=exp.column("i"), expression=exp.Literal.number(size - 1)))
        cte = exp.CTE(
            this=exp.union(exp.select(exp.Literal.number(0).as_("i")), step, distinct=False),
            alias=exp.TableAlias(this=seq.copy(), columns=[exp.to_identifier("i")]),
        )
        out = exp.select(exp.column("i")).from_(exp.Table(this=seq.copy()))
        out.set("with_", exp.With(expressions=[cte], recursive=True))
        return out

    def attach_sequence_setting(self, statement: Expression, *, size: int) -> None:  # NOSONAR(S1172) — no-op hook default
        """Statement-level setting a generated integer sequence of ``size`` rows needs."""

    def bucket_step(self, *, bucket: Expression, count: int, unit: Granularity) -> Expression:
        """A time bucket moved by ``count`` ``unit`` steps (a custom unit steps ``multiple`` of its base)."""
        base, multiple, _ = granularity_parts(unit)
        return self.bucket_offset(bucket=bucket, count=count * multiple, unit=base)

    def promote_to_timestamp(self, expr: Expression) -> Expression:
        """A DATE value as the dialect's naive timestamp (midnight)."""
        return exp.Cast(this=expr.copy(), to=exp.DataType.build("TIMESTAMP"))

    def build_temporal_literal(self, *, value: date, dt: DataType) -> Expression:
        return exp.Cast(this=exp.Literal.string(iso_text(value)), to=exp.DataType.build(dt.value))

    def build_temporal_comparison(
        self, *, op: TemporalComparisonOp, operand: Expression, value: date,
    ) -> Expression:
        """``operand <op> value`` for a DATE / TIMESTAMP ``value`` (the operand's type)."""
        literal = self.build_temporal_literal(value=value, dt=temporal_type_of(value))
        return COMPARISON_NODES[op](this=operand, expression=literal)

    def build_current_date(self) -> Expression:
        return exp.CurrentDate()

    def build_current_timestamp(self) -> Expression:
        return exp.Cast(this=exp.CurrentTimestamp(), to=exp.DataType.build("TIMESTAMP"))

    def build_date_part(self, *, part: DatePart, expr: Expression, operand: DataType) -> Expression:
        """``date_part``: an integer; sub-day parts of a DATE are 0."""
        if part in SUB_DAY_PARTS and operand is DataType.DATE:
            return self._date_part(part, self.promote_to_timestamp(expr))
        return self._date_part(part, expr.copy())

    _EXTRACT_FIELDS: ClassVar[dict[DatePart, str]] = {
        DatePart.YEAR: "YEAR", DatePart.ISO_YEAR: "ISOYEAR", DatePart.QUARTER: "QUARTER",
        DatePart.MONTH: "MONTH", DatePart.WEEK: "WEEK", DatePart.DAY: "DAY",
        DatePart.DAY_OF_WEEK: "ISODOW", DatePart.DAY_OF_YEAR: "DOY",
        DatePart.HOUR: "HOUR", DatePart.MINUTE: "MINUTE", DatePart.SECOND: "SECOND",
    }

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """``part`` of ``expr`` (owned by the caller); timestamp parts only for a timestamp."""
        node: Expression = exp.Extract(this=exp.var(self._EXTRACT_FIELDS[part]), expression=expr)
        if part is DatePart.SECOND:
            node = exp.Floor(this=node)
        return exp.Cast(this=node, to=exp.DataType.build("INT"))

    def build_date_diff(
        self, *, unit: TimeGranularity, start: Expression, end: Expression, operand: DataType,
    ) -> Expression:
        """``unit`` boundaries crossed from ``start`` to ``end`` (both of type ``operand``)."""
        if unit in (TimeGranularity.YEAR, TimeGranularity.QUARTER, TimeGranularity.MONTH):
            years = self._part_gap(DatePart.YEAR, start=start, end=end, operand=operand)
            if unit is TimeGranularity.YEAR:
                return years
            sub, per_year = (
                (DatePart.QUARTER, 4) if unit is TimeGranularity.QUARTER else (DatePart.MONTH, 12)
            )
            return exp.Add(
                this=exp.Mul(this=exp.Literal.number(per_year), expression=exp.Paren(this=years)),
                expression=exp.Paren(this=self._part_gap(sub, start=start, end=end, operand=operand)),
            )
        if unit in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY):
            gap = self._day_gap(
                start=self.build_date_trunc(col_expr=start.copy(), granularity=unit),
                end=self.build_date_trunc(col_expr=end.copy(), granularity=unit),
            )
            return self._exact_div(gap, 7)
        if unit is TimeGranularity.DAY:
            return self._day_gap(start=start.copy(), end=end.copy())
        if operand is DataType.DATE:
            start, end = self.promote_to_timestamp(start), self.promote_to_timestamp(end)
        gap = self._second_gap(
            start=self.build_date_trunc(col_expr=start.copy(), granularity=unit),
            end=self.build_date_trunc(col_expr=end.copy(), granularity=unit),
        )
        return gap if unit is TimeGranularity.SECOND else self._exact_div(gap, _UNIT_SECONDS[unit])

    def _part_gap(self, part: DatePart, *, start: Expression, end: Expression, operand: DataType) -> Expression:
        return exp.Sub(
            this=self.build_date_part(part=part, expr=end, operand=operand),
            expression=self.build_date_part(part=part, expr=start, operand=operand),
        )

    def _day_gap(self, *, start: Expression, end: Expression) -> Expression:
        """Whole days from ``start``'s date to ``end``'s date; operands are owned by the caller."""
        return exp.Sub(
            this=exp.Cast(this=end, to=exp.DataType.build("DATE")),
            expression=exp.Cast(this=start, to=exp.DataType.build("DATE")),
        )

    def _second_gap(self, *, start: Expression, end: Expression) -> Expression:
        """Whole seconds between two second-aligned timestamps; operands are owned by the caller."""
        return exp.Cast(
            this=exp.Extract(this=exp.var("EPOCH"), expression=exp.Paren(this=exp.Sub(this=end, expression=start))),
            to=exp.DataType.build("BIGINT"),
        )

    def _exact_div(self, value: Expression, divisor: int) -> Expression:
        """``value / divisor`` for an exact multiple, as an integer."""
        return exp.IntDiv(this=exp.Paren(this=value), expression=exp.Literal.number(divisor))

    def bucket_offset(self, *, bucket: Expression, count: int, unit: TimeGranularity) -> Expression:
        """A time bucket moved by ``count`` ``unit``s, comparable with ``bucket_comparand`` of a bucket."""
        return self.build_date_add(
            expr=self.bucket_comparand(bucket), count=exp.Literal.number(count), unit=unit,
            operand=DataType.TIMESTAMP,
        )

    def bucket_comparand(self, bucket: Expression) -> Expression:
        """A time bucket as compared with a ``bucket_offset``."""
        return bucket

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``expr`` moved by ``count`` ``unit``s; months clamp at month-end.

        A DATE stays a DATE for day-or-coarser units."""
        interval_unit, per_count = _INTERVAL_UNITS[unit]
        literal = literal_int(count)
        if literal is not None:
            amount = literal * per_count
            op = exp.Add if amount >= 0 else exp.Sub
            moved: Expression = op(this=expr.copy(), expression=self._interval(abs(amount), exp.var(interval_unit)))
        else:
            moved = exp.Add(this=expr.copy(), expression=exp.Mul(
                this=exp.Paren(this=count.copy()), expression=self._interval(per_count, exp.var(interval_unit)),
            ))
        if operand is DataType.DATE and unit not in SUB_DAY_GRANULARITIES:
            return exp.Cast(this=moved, to=exp.DataType.build("DATE"))
        return moved

    def _interval(self, amount: int, unit: exp.Var) -> exp.Interval:
        """``INTERVAL amount unit`` for a non-negative ``amount``."""
        return exp.Interval(this=exp.Literal.number(amount), unit=unit)

    def frame_time_operand(self, expr: Expression) -> Expression:
        """The source time column as compared in a trailing-window frame."""
        return expr

    def build_median(self, inner: Expression) -> Expression:
        """``PERCENTILE_CONT(0.5)``."""
        return self.build_percentile(p=exp.Literal.number("0.5"), col_expr=inner)

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col)``."""
        return exp.WithinGroup(
            this=exp.PercentileCont(this=p.copy()),
            expression=exp.Order(expressions=[exp.Ordered(
                this=col_expr.copy(),
                nulls_first=self.native_nulls_first(descending=False),
            )]),
        )

    def build_approx_count_distinct(self, col_expr: Expression) -> Expression:
        """Approximate distinct; the exact ``COUNT(DISTINCT col)`` unless the dialect has a native one."""
        if self.approx_count_distinct_anonymous_name is not None:
            return exp.Anonymous(
                this=self.approx_count_distinct_anonymous_name,
                expressions=[col_expr.copy()],
            )
        if self.approx_count_distinct_native:
            return exp.ApproxDistinct(this=col_expr.copy())
        return exp.Count(this=exp.Distinct(expressions=[col_expr.copy()]))

    def build_stat_agg_1arg(
        self, agg_name: StatAgg1Name, col_expr: Expression,
    ) -> Expression:
        """Canonical name; sqlglot transpiles per dialect."""
        return self._named_call(agg_name, col_expr)

    def build_covar_2arg(
        self,
        agg_name: StatAgg2Name,
        col_expr: Expression,
        other_expr: Expression,
    ) -> Expression:
        """Native ``CORR`` / ``COVAR_SAMP`` / ``COVAR_POP``."""
        return self._named_call(agg_name, col_expr, other_expr)

    def _named_call(
        self, agg_name: StatAgg1Name | StatAgg2Name, *args: Expression,
    ) -> Expression:
        """``AGG_NAME(args)`` as this dialect's parser would build it, spelling kept."""
        name = agg_name.upper()
        node = exp.func(name, *(a.copy() for a in args), dialect=self.sqlglot_name)
        assert isinstance(node, Expression)  # concrete Func classes and Anonymous all are
        node.meta["name"] = name
        return node

    def should_use_native_log(self, base: int) -> bool:
        """Whether ``log{N}(x)`` uses a native single-arg function rather than ``LOG(N, x)``."""
        if base == 10:
            return self.log10_native
        if base == 2:
            return self.log2_native
        return False

    def rewrite_parsed_ast(self, tree: Expression) -> Expression:
        """Hook: input-side AST rewrite keyed on the dialect the text was parsed with."""
        return tree

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """Hook: output-shaping AST rewrite keyed on the target dialect."""
        return tree

    def rewrite_statement(self, statement: Expression) -> Expression:
        """Rewrite the assembled statement once, just before its single render. Default: identity."""
        return statement

    def apply_pagination(
        self,
        select: exp.Select,
        *,
        limit: Optional[int],
        offset: Optional[int],
    ) -> exp.Select:
        """The single place LIMIT/OFFSET is applied to a completed ``SELECT``.

        Set on the ``Select``: sqlglot transpiles bounds only there, never from a free ``Limit``.
        """
        out = select
        if limit is not None:
            out = out.limit(limit)
        if offset is not None:
            out = out.offset(offset)
        return out

    # Aliases stay canonical inside SLayer; fitted on emission, restored on result keys.

    def quote_identifier(self, name: str) -> str:
        """``name`` wrapped in this dialect's identifier quotes."""
        return exp.Identifier(this=name, quoted=True).sql(dialect=self.sqlglot_name)

    def fit_alias(self, name: str) -> str:
        """Length-only fitting; identity when ``name`` already fits."""
        return fit_identifier(name=name, limit=self.max_identifier_bytes)

    def emit_alias(self, alias: str) -> str:
        """The final identifier a canonical alias reaches the SQL as."""
        return self.fit_alias(alias)

    def alias_rewrite_map(self, aliases: Sequence[str]) -> dict[str, str]:
        """``{canonical: fitted}`` where they differ; collisions checked over identities too."""
        if self.max_identifier_bytes is None:
            return {}
        allocation: dict[str, str] = {}
        owner: dict[str, str] = {}
        for alias in aliases:
            if alias in allocation:
                continue
            fitted = self.fit_alias(alias)
            prior = owner.get(fitted)
            if prior is not None and prior != alias:
                raise IdentifierCollisionError(
                    first=prior, second=alias, emitted=fitted,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="projection alias",
                )
            owner[fitted] = alias
            allocation[alias] = fitted
        return {k: v for k, v in allocation.items() if k != v}

    def decode_alias_map(self, aliases: Sequence[str]) -> dict[str, str]:
        """``{emitted: canonical}`` read-side inverse; raises if two aliases emit alike."""
        out: dict[str, str] = {}
        owner: dict[str, str] = {}
        for alias in aliases:
            emitted = self.emit_alias(alias)
            prior = owner.get(emitted)
            if prior is not None and prior != alias:
                raise IdentifierCollisionError(
                    first=prior, second=alias, emitted=emitted,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="result key",
                )
            owner[emitted] = alias
            if emitted != alias:
                out[emitted] = alias
        return out

    def _rekey_row(
        self,
        row: dict[str, Any],
        mapping: dict[str, str],
        *,
        fallback: Callable[[str], str] | None = None,
    ) -> dict[str, Any]:
        """Rekey a row via ``mapping`` (else ``fallback``); raises if two keys collapse."""
        out: dict[str, Any] = {}
        for key, value in row.items():
            if key in mapping:
                decoded = mapping[key]
            elif fallback is not None:
                decoded = fallback(key)
            else:
                decoded = key
            if decoded in out:
                raise IdentifierCollisionError(
                    first=key, second=decoded, emitted=decoded,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="result key",
                )
            out[decoded] = value
        return out

    def _identifier_quote_anchors(self) -> tuple[str, str]:
        """This dialect's identifier open/close quote chars, read off ``quote_identifier``."""
        probe = self.quote_identifier("x")
        return probe[0], probe[-1]

    def _emission_fit_map(
        self, *, sql: str, aliases: Sequence[str], exempt: frozenset[str],
    ) -> dict[str, str]:
        """``{canonical: fitted}`` for over-limit aliases and scanned CTE columns; exempt pass through."""
        limit = self.max_identifier_bytes
        if limit is None:
            return {}
        quote_open, quote_close = self._identifier_quote_anchors()
        present = quoted_identifiers(
            sql, quote_open=quote_open, quote_close=quote_close,
            lexis=self.identifier_masking_lexis,
        )
        # Post-mangle sizing also catches dotted aliases that mangling pushes over.
        scanned = sorted(n for n in present if self.fit_alias(n) != n)
        allocation: dict[str, str] = {}
        owner: dict[str, str] = {}
        for name in (*aliases, *scanned):
            if name in exempt or name in allocation:
                continue
            fitted = self.fit_alias(name)
            prior = owner.get(fitted)
            if prior is not None and prior != name:
                raise IdentifierCollisionError(
                    first=prior, second=name, emitted=fitted,
                    dialect=self.sqlglot_name, limit=limit,
                )
            owner[fitted] = name
            allocation[name] = fitted
        for name, fitted in allocation.items():
            if fitted != name and fitted in present and fitted not in allocation:
                raise IdentifierCollisionError(
                    first=name, second=fitted, emitted=fitted,
                    dialect=self.sqlglot_name, limit=limit,
                )
        return {k: v for k, v in allocation.items() if k != v}

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (), exempt: frozenset[str] = frozenset(),
    ) -> str:
        """Fit over-limit SLayer-minted identifiers in the final SQL (literals/comments excepted)."""
        mapping = self._emission_fit_map(sql=sql, aliases=aliases, exempt=exempt)
        if not mapping:
            return sql
        return substitute_quoted(
            sql=sql, mapping=mapping, quote=self.quote_identifier,
            lexis=self.identifier_masking_lexis,
        )

    def assert_no_overlimit_identifiers(
        self, sql: str, *, exempt: frozenset[str] = frozenset(),
    ) -> None:
        """Raise if the final SQL still carries a non-exempt over-limit identifier."""
        limit = self.max_identifier_bytes
        if limit is None:
            return
        survivors = [
            t for t in overlimit_tokens(
                sql, limit=limit, quote_styles=[self._identifier_quote_anchors()],
                lexis=self.identifier_masking_lexis,
            )
            if t not in exempt
        ]
        if survivors:
            raise IdentifierLengthError(
                tokens=survivors, dialect=self.sqlglot_name, limit=limit,
            )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the write-side rewrite on result-row keys."""
        mapping = self.decode_alias_map(aliases)
        if not mapping:
            return rows
        return [self._rekey_row(row=row, mapping=mapping) for row in rows]

    def register_udfs(self, dbapi_connection) -> None:  # NOSONAR(S1172) — no-op hook default; overrides use it
        """Hook: register UDFs on every fresh connection."""
        return None

    def build_explain_sql(self, sql: str) -> str:
        """Wrap ``sql`` in the dialect's EXPLAIN prefix/postfix pair."""
        if self.explain_prefix is None:
            raise ValueError(
                f"EXPLAIN is not supported for dialect '{self.sqlglot_name}'. "
                "Use dry_run=True to inspect the generated SQL instead."
            )
        return f"{self.explain_prefix} {sql}{self.explain_postfix}"

    # Runtime hooks: keep dialect conditionals out of engine_factory / client.

    def build_connection_url(
        self,
        datasource: "DatasourceConfig",  # NOSONAR(S1172) — no-op hook default; overrides use it
    ) -> str | None:
        """Hook: connection string, or ``None`` to use ``DatasourceConfig.get_connection_string()``."""
        return None

    def build_engine(
        self,
        datasource: "DatasourceConfig",  # NOSONAR(S1172) — no-op hook default; overrides use it
        *,
        connection_string: str,  # NOSONAR(S1172) — no-op hook default; overrides use it
    ) -> "sa.Engine | None":
        """Hook: a dialect-built engine, or ``None`` for ``engine_factory``'s default ``create_engine``."""
        return None

    def deferred_driver_modules(
        self,
        connection_string: str,  # NOSONAR(S1172) — no-op hook default; overrides use it
    ) -> tuple[str, ...]:
        """Hook: driver modules the engine first imports at connect time, checked up front instead."""
        return ()

    def credential_fingerprint(self, datasource: "DatasourceConfig") -> str:
        """Digest of credentials not in the connection string; part of the engine cache key."""
        return _digest(datasource.credentials_json)

    def apply_session_overrides(
        self,
        dbapi_connection: Any,  # NOSONAR(S1172) — no-op hook default; overrides use it
        datasource: "DatasourceConfig",  # NOSONAR(S1172) — no-op hook default; overrides use it
    ) -> None:
        """Hook: session setup on every new pooled connection (e.g. ``USE WAREHOUSE``)."""
        return None

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:  # NOSONAR(S1172) — no-op hook default
        """Hook: statement that sets the timeout before the query, or ``None``."""
        return None

    def set_connection_timeout(self, dbapi_connection: Any, timeout_seconds: int) -> object:  # NOSONAR(S1172) — hook
        """Hook: put the timeout on the DBAPI connection; returns the prior state for restore."""
        return None

    def restore_connection_timeout(self, dbapi_connection: Any, prior: object) -> None:  # NOSONAR(S1172) — hook
        """Hook: undo ``set_connection_timeout`` with the state it returned."""
        return None

    def server_profile_sql(self) -> str | None:
        """Hook: probe whose one row ``parse_server_profile`` reads as ``base_row``, or ``None``."""
        return None

    def correlated_setting_sql(self, profile: ServerProfile) -> str | None:  # NOSONAR(S1172) — no-op hook default
        """Hook: follow-up probe for the correlated-subquery setting (its row is ``correlated_row``), or ``None``."""
        return None

    def parse_server_profile(
        self,
        *,
        base_row: Sequence[Any] | None,  # NOSONAR(S1172) — no-op hook default; overrides use it
        correlated_row: Sequence[Any] | None = None,  # NOSONAR(S1172) — no-op hook default
    ) -> ServerProfile:
        """Hook: probe rows → profile; tolerant, never raises."""
        return ServerProfile()

    def timeout_permitted(self, profile: ServerProfile) -> bool:  # NOSONAR(S1172) — no-op hook default
        """Hook: whether this user may set the statement timeout."""
        return True

    def correlated_subquery_refusal(self, profile: ServerProfile) -> str | None:  # NOSONAR(S1172) — no-op hook default
        """Hook: why this user cannot run a correlated subquery, or ``None``."""
        return None

    def attach_correlated_setting(self, statement: Expression) -> None:  # NOSONAR(S1172) — no-op hook default
        """Hook: enable correlated subqueries on ``statement`` in place."""
        return None

    def in_subquery_key(self, key: Expression) -> Expression:
        """Hook: the key an uncorrelated ``IN`` subquery projects; default unchanged."""
        return key

    def map_cursor_type_code(self, type_code: int) -> str | None:  # NOSONAR(S1172) — no-op hook default
        """Hook: cursor type code → SLayer category, or ``None`` for the Postgres OID map."""
        return None

    # Structure reflection: the one reader of a live table's columns and keys.

    def reflect_columns(
        self, *, sa_engine: CatalogEngine | None, inspector: Any, table: str, schema: str | None, catalog: str | None,
    ) -> list[ReflectedColumn]:
        """Inspector columns; when it raises, ``columns_query`` plus ``column_comments_query``."""
        try:
            return [
                ReflectedColumn(name=c["name"], type=c["type"], comment=clean_comment(c.get("comment")))
                for c in inspector.get_columns(table, schema=schema_token(schema=schema, catalog=catalog))
            ]
        except Exception:  # noqa: BLE001 — catalog fallback below
            pass
        location = TableLocation(name=table, schema_name=schema, catalog=catalog)
        rows = fetch(sa_engine, self.columns_query(location), dialect=self.sqlglot_name)
        comments = self._column_comments(sa_engine=sa_engine, location=location)
        return [
            column_from_type_string(
                name=name, data_type=dtype, comment=comments.get(name), zoned_types=self.zoned_timestamp_db_types,
                json_types=self.json_db_types,
            )
            for name, dtype in rows
        ]

    def columns_query(self, location: TableLocation) -> exp.Select:
        """``(column_name, data_type)`` of one table from INFORMATION_SCHEMA, in column order."""
        conditions = [eq_literal(col("table_name"), location.name)]
        if location.schema_name:
            conditions.append(eq_literal(col("table_schema"), location.schema_name))
        if location.catalog:
            conditions.append(eq_literal(col("table_catalog"), location.catalog))
        return (
            exp.select(col("column_name"), col("data_type"))
            .from_(relation("columns", db="information_schema"))
            .where(all_of(conditions))
            .order_by(col("ordinal_position"))
        )

    def column_comments_query(self, location: TableLocation) -> exp.Select | None:  # NOSONAR(S1172) — no-op hook default
        """Hook: ``(column_name, comment)`` of one table, or ``None`` when comments have no catalog source."""
        return None

    def _column_comments(self, *, sa_engine: CatalogEngine | None, location: TableLocation) -> dict[str, str]:
        query = self.column_comments_query(location)
        if query is None:
            return {}
        try:
            rows = fetch(sa_engine, query, dialect=self.sqlglot_name)
        except Exception:  # noqa: BLE001 — comments are optional
            return {}
        return {name: comment for name, raw in rows if (comment := clean_comment(raw))}

    def reflect_table_keys(
        self, *, sa_engine: CatalogEngine | None, inspector: Any, table: str, schema: str | None, catalog: str | None,
    ) -> TableKeys:
        """PK, FKs and whole-table unique key sets; each read best-effort, a failure logged at WARNING."""
        token = schema_token(schema=schema, catalog=catalog)
        pk = best_effort(
            lambda: self._primary_key(
                sa_engine=sa_engine, inspector=inspector, table=table, schema=schema, catalog=catalog,
            ),
            default=[], table=table, what="primary key",
        )
        fks = best_effort(
            lambda: inspector_foreign_keys(inspector, table=table, token=token),
            default=[], table=table, what="foreign keys",
        )
        uniques = best_effort(
            lambda: inspector_unique_constraints(inspector, table=table, token=token),
            default=[], table=table, what="unique constraints",
        )
        indexes = best_effort(
            lambda: inspector_unique_indexes(inspector, table=table, token=token),
            default=[], table=table, what="unique indexes",
        )
        return TableKeys(
            primary_key=pk, foreign_keys=fks, unique_key_sets=without_primary_key(pk, uniques + indexes),
        )

    def _primary_key(
        self, *, sa_engine: CatalogEngine | None, inspector: Any, table: str, schema: str | None, catalog: str | None,
    ) -> list[str]:
        """The Inspector's PK; when it yields none, ``primary_key_query`` (unless the Inspector is final)."""
        try:
            result = inspector.get_pk_constraint(table, schema=schema_token(schema=schema, catalog=catalog))
            found = inspector_column_names(result.get("constrained_columns")) if isinstance(result, dict) else None
        except Exception:
            if self.primary_key_inspector_only:
                raise
            found = None
        if found or self.primary_key_inspector_only:
            return found or []
        query = self.primary_key_query(TableLocation(name=table, schema_name=schema, catalog=catalog))
        return [row[0] for row in fetch(sa_engine, query, dialect=self.sqlglot_name)]

    def primary_key_query(self, location: TableLocation) -> exp.Select:
        """PK column names from INFORMATION_SCHEMA; joined on the catalog too (DuckDB reuses constraint names)."""
        on = [
            exp.EQ(this=col("constraint_name", "tc"), expression=col("constraint_name", "kcu")),
            exp.EQ(this=col("table_schema", "tc"), expression=col("table_schema", "kcu")),
        ]
        where = [
            eq_literal(col("table_name", "tc"), location.name),
            eq_literal(col("constraint_type", "tc"), "PRIMARY KEY"),
        ]
        if location.catalog:
            on.append(exp.EQ(this=col("table_catalog", "tc"), expression=col("table_catalog", "kcu")))
            where.append(eq_literal(col("table_catalog", "tc"), location.catalog))
        if location.schema_name:
            where.append(eq_literal(col("table_schema", "tc"), location.schema_name))
        return (
            exp.select(col("column_name", "kcu"))
            .from_(relation("table_constraints", db="information_schema", alias="tc"))
            .join(relation("key_column_usage", db="information_schema", alias="kcu"), on=all_of(on))
            .where(all_of(where))
        )

    def reflect_schema_description(
        self, *, sa_engine: "sa.Engine", schema: str | None, configured_schema: str | None,  # NOSONAR(S1172) — no-op hook default
    ) -> str | None:
        """Hook: the schema's own description (``schema``, else the connection's, else ``configured_schema``)."""
        return None


class DottedAliasManglingMixin:
    """Shared ``.``-to-``___`` alias mangling for BigQuery / T-SQL.

    Mix in before ``SqlDialect`` so the base length pass runs first.
    """

    dotted_alias_re: ClassVar[re.Pattern[str]]
    alias_quote_open: ClassVar[str]
    alias_quote_close: ClassVar[str]

    def fit_alias(self, name: str) -> str:
        """Fit against the post-mangle length; the result stays dotted."""
        return fit_identifier(
            name=name, limit=self.max_identifier_bytes, expand=encode_alias,
        )

    def emit_alias(self, alias: str) -> str:
        """The final identifier: length-fitted, then dot-mangled."""
        return encode_alias(self.fit_alias(alias))

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (), exempt: frozenset[str] = frozenset(),
    ) -> str:
        """Base LENGTH pass, then ``.`` -> ``___`` inside quoted identifiers."""
        sql = super().rewrite_emitted_sql(sql=sql, aliases=aliases, exempt=exempt)
        return self.dotted_alias_re.sub(
            lambda m: (
                f"{self.alias_quote_open}{encode_alias(m.group(1))}"
                f"{self.alias_quote_close}"
            ),
            sql,
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the mangling on result-row keys."""
        mapping = self.decode_alias_map(aliases)
        return [
            self._rekey_row(row=row, mapping=mapping, fallback=decode_alias)
            for row in rows
        ]
