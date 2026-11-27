"""MysqlDialect.

MySQL has no ``PERCENTILE_CONT`` (it is interpolated over ``ROW_NUMBER`` / ``COUNT``
windows) and no native ``CORR`` / ``COVAR_SAMP`` / ``COVAR_POP`` (uses the
variance-decomposition formula).

``var_samp`` / ``var_pop`` need the ``exp.Anonymous`` workaround because
sqlglot's MySQL transpiler rewrites them to ``VARIANCE`` (which on MySQL
is actually ``VAR_POP`` — silently wrong sample variance).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import Field
from sqlalchemy.engine.url import URL
from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import DataType, DatePart, NUMERIC_DATATYPES, TimeGranularity
from slayer.core.granularity import bucket_type
from slayer.core.errors import ReadOnlyConnectionError, TimeZoneError, UnsupportedServerFunctionError
from slayer.core.json_path import JsonPath, json_path_text
from slayer.core.keys import CastKey, ScalarCallKey
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    trimmed_fraction,
    NativeFn,
    TransactionMode,
    set_statement,
    FALSE_WORDS,
    INT64_BOUNDS,
    NARROW_INTEGER_BOUNDS,
    NUMBER_TEXT_PATTERN,
    PORTABLE_LIBRARY,
    REGEX_LIBRARY,
    TRUE_WORDS,
    UNSIGNED_CAST_TYPES,
    MathFn,
    ReadOnlyEngineArgs,
    ServerProfile,
    SqlDialect,
    TextFn,
    anon,
    any_null,
    as_text,
    case,
    lit,
    StatAgg1Name,
    StatAgg2Name,
    _build_covar_decomposition,
    iso_text,
    zone_name,
)
from slayer.sql.dialects._ordered_set_split import ranked_percentiles, split_percentile_selects
from slayer.sql.dialects.patterns import ICU, RegexFlavour
from slayer.sql.dialects.reflection import (
    ResultColumnType,
    TableLocation,
    call,
    info_schema_comments_query,
    result_column_type_of_name,
)

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig

#: MySQL 8.0 brought the ICU regex functions; 8.0.21 ``JSON_VALUE``.
_MYSQL_8_0 = (8, 0, 0)
_MYSQL_8_0_21 = (8, 0, 21)
_MYSQL_MAX_SCALE = 30
_VERSION = re.compile(r"\s*(\d+)\.(\d+)(?:\.(\d+))?")
_NUMBERS = NUMERIC_DATATYPES
_GATED_FUNCTIONS = REGEX_LIBRARY | {"json_value"}

# The session read-only from connect, through the drivers' one init statement.
_INIT_COMMAND = "init_command"
_READ_ONLY_SESSION = "SET SESSION TRANSACTION READ ONLY"
_UTC_OFFSET = "+00:00"


def _numeric_try_cast(key: object) -> bool:
    return isinstance(key, CastKey) and key.try_ and key.data_type in _NUMBERS


def _match_type(case_insensitive: bool) -> Expression:
    """ICU match type: always explicit, so a case-insensitive collation never decides."""
    return lit("i" if case_insensitive else "c")


_SUB_DAY_TRUNC_FORMATS = {
    TimeGranularity.HOUR: "%Y-%m-%d %H:00:00",
    TimeGranularity.MINUTE: "%Y-%m-%d %H:%i:00",
    TimeGranularity.SECOND: "%Y-%m-%d %H:%i:%s",
}
# Wire-protocol field types → type names; TINY, BIT and the decimals are read by length / scale.
_FIELD_TYPE_NAMES: dict[int, str] = {
    2: "SMALLINT", 3: "INT", 4: "FLOAT", 5: "DOUBLE", 7: "TIMESTAMP", 8: "BIGINT", 9: "MEDIUMINT",
    10: "DATE", 11: "TIME", 12: "DATETIME", 13: "YEAR", 14: "DATE", 15: "VARCHAR", 245: "JSON",
    247: "ENUM", 248: "SET", 249: "TINYBLOB", 250: "MEDIUMBLOB", 251: "LONGBLOB", 252: "BLOB",
    253: "VARCHAR", 254: "CHAR", 255: "GEOMETRY",
}
_TINY, _BIT, _DECIMALS = 1, 16, frozenset({0, 246})

_MYSQL_PART_FUNCTIONS = {
    DatePart.YEAR: "YEAR", DatePart.QUARTER: "QUARTER", DatePart.MONTH: "MONTH",
    DatePart.DAY: "DAYOFMONTH", DatePart.DAY_OF_YEAR: "DAYOFYEAR",
    DatePart.HOUR: "HOUR", DatePart.MINUTE: "MINUTE", DatePart.SECOND: "SECOND",
}


def _native_null_keys(term: exp.Ordered) -> list[exp.Ordered]:
    """``term`` as sort keys in MySQL's native NULL order (NULLs low), led by an ``IS NULL`` key when it asks otherwise."""
    descending = bool(term.args.get("desc"))
    nulls_first = bool(term.args.get("nulls_first"))
    if nulls_first != descending or term.this.is_int or isinstance(term.this, exp.Rand):
        return [term]
    is_null = exp.Case(
        ifs=[exp.If(this=exp.Is(this=term.this.copy(), expression=exp.Null()), true=exp.Literal.number(1))],
        default=exp.Literal.number(0),
    )
    return [
        exp.Ordered(this=is_null, desc=nulls_first, nulls_first=not nulls_first),
        exp.Ordered(this=term.this, desc=descending, nulls_first=not descending),
    ]


#: sqlglot writes these as VARIANCE / VARIANCE_POP / STDDEV: population, no function, population on MySQL.
_MYSQL_STATISTIC_NAMES: dict[type[Expression], str] = {
    exp.Variance: "VAR_SAMP", exp.VariancePop: "VAR_POP", exp.Stddev: "STDDEV_SAMP",
}


def _is_double(node: Expression) -> bool:
    return isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.DOUBLE)


class MysqlDialect(SqlDialect):
    enforces_unique_keys: ClassVar[bool] = True
    sqlglot_name: str = "mysql"
    varchar_cast_truncates: ClassVar[bool] = True
    unsigned_cast_types: ClassVar[frozenset[DType]] = frozenset({
        exp.DataType.Type.UTINYINT, exp.DataType.Type.USMALLINT, exp.DataType.Type.UMEDIUMINT, exp.DataType.Type.UINT,
        exp.DataType.Type.UBIGINT, exp.DataType.Type.UDECIMAL,
    })
    database_name: ClassVar[str] = "MySQL"
    nulls_clause: bool = False
    # ``AVG`` over integers keeps only ``div_precision_increment`` decimals.
    integer_avg: bool = True
    avg_input_type: str = "DOUBLE"
    ds_type_aliases: frozenset[str] = frozenset({"mysql"})
    transaction_mode: ClassVar[TransactionMode] = "driver_autocommit"
    sa_backend_names: frozenset[str] = frozenset({"mysql"})
    regex_flavour: ClassVar[Optional[RegexFlavour]] = ICU
    library_functions: ClassVar[frozenset[str]] = PORTABLE_LIBRARY - {"regexp_count"}
    time_of_day_parts: ClassVar[bool] = True
    # A bare ``TIME`` keeps no fraction here.
    time_of_day_precision: ClassVar[Optional[int]] = 6

    def build_time_cast(
        self, value: Expression, to: exp.DataType, *, source: Optional[DataType], try_: bool,
    ) -> Expression:
        """``TIME`` also holds durations here: a text ``TRY_CAST`` keeps only a clock time inside the day."""
        cast = super().build_time_cast(value, to, source=source, try_=False)
        if source is not None or not try_:
            return cast
        day = exp.And(
            this=exp.GTE(this=cast.copy(), expression=self.build_time_literal(time(0, 0))),
            expression=exp.LT(this=cast.copy(), expression=exp.Cast(this=lit("24:00:00"), to=self.time_of_day_type())),
        )
        # The text is the cast's own ``HH:MM[:SS[.f…]]`` spelling: MySQL casts a valid prefix of any text.
        text = as_text(value)
        length = exp.Length(this=text.copy())
        spelled = anon("LEFT", anon("TIME_FORMAT", cast.copy(), lit("%H:%i:%s.%f")), length.copy())
        shaped = exp.or_(exp.In(this=length.copy(), expressions=[lit(5), lit(8)]),
                         exp.Between(this=length, low=lit(10), high=lit(15)))
        clock = exp.and_(shaped, exp.EQ(this=text, expression=spelled))
        return case((exp.And(this=clock, expression=day), cast))

    def build_time_text(self, value: Expression) -> Expression:
        return trimmed_fraction(as_text(exp.Cast(this=value, to=self.time_of_day_type())))
    native_functions: ClassVar[frozenset[NativeFn]] = frozenset({
        "REPEAT", "LPAD", "RPAD", "REVERSE", "CONCAT_WS", "GREATEST", "LEAST",
    })
    extremum_propagates_null: ClassVar[bool] = True
    zoned_timestamp_db_types: ClassVar[frozenset[str]] = frozenset({"TIMESTAMP"})

    def convert_zone(self, value: Expression, *, source: Optional[ZoneInfo], target: ZoneInfo) -> Expression:
        """``CONVERT_TZ(v, S, Q)``; an instant (a ``TIMESTAMP``) from its epoch seconds, independent of the session
        zone."""
        if source is None:
            epoch = exp.Anonymous(this="UNIX_TIMESTAMP", expressions=[value])
            value = exp.DateAdd(this=exp.Literal.string("1970-01-01"), expression=epoch, unit=exp.var("SECOND"))
            source_tz: Expression = exp.Literal.string(_UTC_OFFSET)
        else:
            source_tz = zone_name(source)
        return exp.ConvertTimezone(source_tz=source_tz, target_tz=zone_name(target), timestamp=value)

    def instant_literal(self, value: datetime) -> Optional[Expression]:
        return exp.Anonymous(this="CONVERT_TZ", expressions=[
            exp.Literal.string(iso_text(value)), exp.Literal.string(_UTC_OFFSET), exp.var("@@session.time_zone")])

    def converted_zones(self, statement: Expr, *, zones: Iterable[str]) -> list[str]:
        named = {zone.name for node in statement.find_all(exp.ConvertTimezone)
                 for zone in (node.args.get("source_tz"), node.args.get("target_tz")) if isinstance(zone, exp.Literal)}
        return sorted(named.intersection(zones))

    def zone_probe(self, zone: ZoneInfo) -> Optional[exp.Select]:
        """``CONVERT_TZ`` is NULL for a named zone the server's time zone tables do not hold."""
        return exp.select(exp.ConvertTimezone(
            source_tz=exp.Literal.string(_UTC_OFFSET), target_tz=zone_name(zone),
            timestamp=exp.Literal.string("2000-01-01 00:00:00"),
        ).as_("z"))

    def unresolved_zone_error(self, zone: ZoneInfo) -> TimeZoneError:
        return TimeZoneError(
            f"{self.database_name} cannot resolve time zone {zone.key!r}: its time zone tables are not loaded. Load "
            f"them with mysql_tzinfo_to_sql (see the MySQL manual, 'MySQL Server Time Zone Support'), then retry.",
            zone=zone.key,
        )

    def profile_needed(self, keys: Iterable[object]) -> bool:
        """Regex and numeric ``TRY_CAST`` need MySQL 8.0, ``json_value`` 8.0.21: the version decides."""
        return any(
            (isinstance(k, ScalarCallKey) and k.name in _GATED_FUNCTIONS) or _numeric_try_cast(k) for k in keys
        )

    def server_profile_sql(self) -> str | None:
        return "SELECT VERSION()"

    def parse_server_profile(
        self, *, base_row: Sequence[Any] | None, correlated_row: Sequence[Any] | None = None,
        time_row: Sequence[Any] | None = None,
    ) -> ServerProfile:
        match = _VERSION.match(str(base_row[0])) if base_row and base_row[0] is not None else None
        if match is None:
            return ServerProfile()
        return ServerProfile(version=tuple(int(part or 0) for part in match.groups()))

    def _require_mysql(self, minimum: tuple[int, int, int], what: str) -> None:
        release = "MySQL " + ".".join(map(str, minimum[:2] if minimum[2] == 0 else minimum))
        self.require_version(minimum=minimum, what=what, release=release)

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        if fn == "chr":
            utf32 = exp.Chr(expressions=[args[0].copy()], charset=exp.var("utf32"))
            text = exp.Cast(this=utf32, to=exp.DataType(this=exp.DataType.Type.CHARACTER_SET, kind=exp.var("utf8mb4")))
            return self._code_point_guard(args[0], text)
        if fn == "ascii":
            return exp.Unicode(this=args[0].copy())
        return super().build_text_call(fn, args)

    def binary_text(self, value: Expression) -> Expression:
        """Read as ``utf8mb4`` (any column charset) under its binary collation."""
        utf8 = exp.Cast(this=value.copy(), to=exp.DataType(this=exp.DataType.Type.CHARACTER_SET, kind=exp.var("utf8mb4")))
        return exp.Collate(this=utf8, expression=exp.var("utf8mb4_bin"))

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        """``SUBSTRING_INDEX`` twice; ``''`` past the last field."""
        inner = anon("SUBSTRING_INDEX", value, delimiter, lit(index))
        field = anon("SUBSTRING_INDEX", inner, delimiter, lit(-1 if index > 0 else 1))
        beyond = exp.GT(this=lit(abs(index)), expression=self.field_count(value, delimiter))
        return case((any_null(value, delimiter), exp.Null()), (beyond, lit("")), default=field)

    def build_math_call(self, fn: MathFn, args: tuple[Expression, ...]) -> Expression:
        if fn == "pi":
            return anon("ACOS", lit(-1))  # PI() reaches the client rounded to 6 places
        return super().build_math_call(fn, args)

    def cube_root(self, value: Expression) -> Expression:
        # A DOUBLE exponent: 1 / 3.0 is a DECIMAL cut to div_precision_increment places.
        third = exp.Div(this=lit(1), expression=exp.Literal.number("3E0"))
        return exp.Mul(this=exp.Sign(this=value.copy()), expression=exp.Pow(this=exp.Abs(this=value.copy()), expression=third))

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        self._require_mysql(_MYSQL_8_0, "regexp_like()")
        return anon("REGEXP_LIKE", value, self.regex_text(pattern), _match_type(case_insensitive))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        """The first match, then (for a group) that match re-read whole by the pattern."""
        self._require_mysql(_MYSQL_8_0, "regexp_extract()")
        match = anon("REGEXP_SUBSTR", value, self.regex_text(pattern), lit(1), lit(1), _match_type(False))
        if group == 0:
            return match
        return anon("REGEXP_REPLACE", match, self.regex_text(pattern, anchored=True), lit(f"${group}"), lit(1),
                    lit(0), _match_type(False))

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        self._require_mysql(_MYSQL_8_0, "regexp_replace()")
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern), self.replacement_text(replacement), lit(1),
                    lit(0), _match_type(case_insensitive))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        self._require_mysql(_MYSQL_8_0_21, "json_value()")

        def json_type(steps: tuple[str | int, ...]) -> Expression:
            return anon("JSON_TYPE", anon("JSON_EXTRACT", value.copy(), lit(json_path_text(steps))))

        # MySQL reads ``[n]`` on a non-array as the value itself; an index needs an array parent.
        arrays = [exp.EQ(this=json_type(path.steps[:i]), expression=lit("ARRAY"))
                  for i, step in enumerate(path.steps) if isinstance(step, int)]
        readable: Expression = exp.Not(this=exp.In(this=json_type(path.steps),
                                                   expressions=[lit("OBJECT"), lit("ARRAY"), lit("NULL")]))
        for array in reversed(arrays):
            readable = exp.And(this=array, expression=readable)
        read = case((readable, anon("JSON_VALUE", value.copy(), lit(path.text))))
        return case((anon("JSON_VALID", value), read))  # a nested CASE: JSON_EXTRACT raises on invalid text

    def json_fraction(self, value: Expression, path: JsonPath, native: bool) -> Optional[Expression]:
        found = anon("JSON_TYPE", anon("JSON_EXTRACT", value.copy(), lit(path.text)))
        fraction = exp.In(this=found, expressions=[lit("DOUBLE"), lit("DECIMAL")])
        exact = exp.DataType.build(f"DECIMAL(65, {_MYSQL_MAX_SCALE})")  # a DOUBLE's ROUND breaks a tie to even
        number = exp.Cast(this=self.build_json_value(value, path, native), to=exact)
        return case((anon("JSON_VALID", value), case((fraction, number))))

    def build_decimal_cast(
        self, value: Expression, to: exp.DataType, *, precision: int, scale: int, source: Optional[DataType],
        try_: bool,
    ) -> Expression:
        """A value past ``precision`` saturates here: ``TRY_CAST`` checks it."""
        cast = super().build_decimal_cast(value, to, precision=precision, scale=scale, source=source, try_=try_)
        if not try_:
            return cast
        exact = exp.Cast(this=value.copy(), to=exp.DataType.build(f"DECIMAL(65, {min(scale, _MYSQL_MAX_SCALE)})"))
        return case((exp.LT(this=exp.Abs(this=exact), expression=lit(10 ** (precision - scale))), cast))

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        """Numeric and boolean targets are guarded by the validity grammar; MySQL's date casts already yield NULL."""
        text = as_text(value)
        if to.is_type(*exp.DataType.NUMERIC_TYPES):
            self._require_mysql(_MYSQL_8_0, "TRY_CAST to a number")
            guard = exp.RegexpLike(this=text, expression=exp.Literal.string(NUMBER_TEXT_PATTERN))
            return exp.Case(ifs=[exp.If(this=guard, true=exp.Cast(this=value.copy(), to=to))])
        if to.is_type(exp.DataType.Type.BOOLEAN):
            word = exp.Lower(this=exp.Trim(this=text))
            return exp.Case(ifs=[
                exp.If(this=exp.In(this=word, expressions=[exp.Literal.string(w) for w in TRUE_WORDS]), true=exp.true()),
                exp.If(this=exp.In(this=word.copy(), expressions=[exp.Literal.string(w) for w in FALSE_WORDS]),
                       true=exp.false()),
            ])
        return exp.Cast(this=value, to=to)

    def build_integer_text_try_cast(self, text: Expression, to: exp.DataType) -> Expression:
        """An integer ``CAST`` saturates here: the text's exact value is range-checked first."""
        trimmed = exp.Trim(this=text.copy())
        unsigned = (0, 2 ** 64 - 1) if to.this in UNSIGNED_CAST_TYPES else INT64_BOUNDS
        low, high = NARROW_INTEGER_BOUNDS.get(to.this, unsigned)
        exact = exp.Cast(this=trimmed.copy(), to=exp.DataType.build("DECIMAL(65, 0)"))
        valid = exp.And(
            this=self.integer_text(text),
            expression=exp.Between(this=exact, low=lit(low), high=lit(high)),
        )
        return case((valid, self.build_try_cast(trimmed, to)))
    explain_prefix: str | None = "EXPLAIN FORMAT=JSON"
    log10_native: bool = True
    log2_native: bool = True
    # Conservative: MySQL allows 256 for column aliases but errors (not truncates).
    max_identifier_bytes: int | None = 64
    url_scheme: str | None = "mysql+pymysql"
    url_backend_aliases: frozenset[str] = frozenset({"mariadb"})
    install_extra: str | None = "mysql"
    extra_drivers: dict[str, str] = Field(default_factory=lambda: {"pymysql": "pymysql"})

    def statement_timeout(self, timeout_seconds: int) -> Expression | None:
        return set_statement(name="max_execution_time", value=timeout_seconds * 1000)

    def read_only_engine_args(
        self, *, url: URL, datasource: DatasourceConfig, connect_args: Mapping[str, Any],
    ) -> ReadOnlyEngineArgs:
        """The session read-only from connect; a URL with its own init statement is refused (two would need the
        multi-statement client flag)."""
        if _INIT_COMMAND in url.query or _INIT_COMMAND in connect_args:
            raise ReadOnlyConnectionError(
                datasource_name=datasource.name,
                reason=f"its connection sets {_INIT_COMMAND}, which SLayer uses to make the session read-only",
                hint=f"remove {_INIT_COMMAND}; set the session state it carried on the database user instead",
            )
        return ReadOnlyEngineArgs(connect_args={_INIT_COMMAND: _READ_ONLY_SESSION})

    def describe_cursor_column(self, description_row: Sequence[Any]) -> ResultColumnType | None:
        """PyMySQL / aiomysql field types: length 1 makes ``TINY`` and ``BIT`` boolean; decimals type by scale."""
        code, length, scale = description_row[1], description_row[3], description_row[5]
        if not isinstance(code, int):
            return None
        if code == _TINY:
            return ResultColumnType(type=DataType.BOOLEAN if length == 1 else DataType.INTEGER, db_type=f"TINYINT({length})")
        if code == _BIT:
            return ResultColumnType(type=DataType.BOOLEAN if length == 1 else DataType.STRING, db_type=f"BIT({length})")
        if code in _DECIMALS:
            # Length counts the sign (assumed: the row has no UNSIGNED flag) and, at a positive scale, the point.
            precision = max(1, (length or 0) - 1 - (1 if scale else 0))
            return result_column_type_of_name(f"DECIMAL({precision},{scale or 0})")
        name = _FIELD_TYPE_NAMES.get(code)
        return None if name is None else result_column_type_of_name(name)

    def column_comments_query(self, location: TableLocation) -> exp.Select:
        return info_schema_comments_query(
            comment_column="column_comment", default_schema=call("DATABASE"), location=location,
        )

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """MySQL's ``TRUNCATE`` has no single-argument form — ``TRUNCATE(x)`` is
        a syntax error. A 1-arg ``trunc(x)`` becomes ``TRUNCATE(x, 0)``; the sample and population statistics keep
        MySQL's names."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Trunc) and node.args.get("decimals") is None:
                node.set("decimals", exp.Literal.number(0))
            name = _MYSQL_STATISTIC_NAMES.get(type(node))
            if name is not None:
                # ``transform`` skips a replaced node's children, so they are rewritten here.
                return exp.Anonymous(this=name, expressions=[node.this.transform(_fix)])
            return node
        return tree.transform(_fix)

    def true_division(self, quotient: Expression) -> Expression:
        """The dividend as a ``DOUBLE``: MySQL's ``/`` keeps only ``div_precision_increment`` extra decimal digits."""
        dividend = quotient
        while isinstance(dividend, exp.Div) and isinstance(dividend.this, exp.Div):
            dividend = dividend.this
        if isinstance(dividend, exp.Div) and not _is_double(dividend.this):
            dividend.set("this", exp.Cast(this=dividend.this, to=exp.DataType.build("DOUBLE")))
        return quotient

    def rewrite_statement(self, statement: Expression) -> Expression:
        """Emulate NULL placement on each ORDER BY term as written, so an alias stays an alias.

        sqlglot's emulation inlines an aliased projection, which ``ONLY_FULL_GROUP_BY`` can fail to match.
        """
        statement = split_percentile_selects(statement.copy(), columns=ranked_percentiles)
        for select in statement.find_all(exp.Select):
            order = select.args.get("order")
            if order is not None:
                order.set("expressions", [key for term in order.expressions for key in _native_null_keys(term)])
        return statement

    def natural_div(self, value: Expression, *, divisor: Expression) -> Expression:
        """``FLOOR``: an integer ``CAST`` rounds here."""
        return exp.Floor(this=exp.Div(this=value, expression=divisor))

    def attach_sequence_setting(self, statement: Expression, *, size: int) -> None:
        """Lift the recursion cap for the recursive integer sequence (default 1000)."""
        statement.set("hint", exp.Hint(expressions=[exp.Anonymous(this="SET_VAR", expressions=[
            exp.EQ(this=exp.var("cte_max_recursion_depth"), expression=exp.Literal.number(size + 1)),
        ])]))

    def build_date_trunc(
        self, *, col_expr: Expression, granularity: TimeGranularity, operand: DataType,
    ) -> Expression:
        """Sub-day units via ``DATE_FORMAT`` (sqlglot renders them all as ``DATE(t)``); a TIMESTAMP bucket is a
        ``DATETIME`` (a coarser truncation returns a DATE)."""
        fmt = _SUB_DAY_TRUNC_FORMATS.get(granularity)
        if fmt is not None:
            formatted = exp.Anonymous(this="DATE_FORMAT", expressions=[col_expr.copy(), exp.Literal.string(fmt)])
            return exp.Cast(this=formatted, to=exp.DataType.build("DATETIME"))
        trunc = super().build_date_trunc(col_expr=col_expr, granularity=granularity, operand=operand)
        if bucket_type(operand, granularity) is DataType.DATE:
            return trunc
        return exp.Cast(this=trunc, to=exp.DataType.build("DATETIME"))

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """Named functions: ``WEEK(x, 3)`` is ISO, ``WEEKDAY`` counts from Monday=0."""
        if part is DatePart.WEEK:
            return exp.Anonymous(this="WEEK", expressions=[expr, exp.Literal.number(3)])
        if part is DatePart.ISO_YEAR:
            year_week = exp.Anonymous(this="YEARWEEK", expressions=[expr, exp.Literal.number(3)])
            return exp.Cast(
                this=exp.Floor(this=exp.Div(this=year_week, expression=exp.Literal.number(100))),
                to=exp.DataType.build("INT"),
            )
        if part is DatePart.DAY_OF_WEEK:
            return exp.Add(this=exp.Anonymous(this="WEEKDAY", expressions=[expr]), expression=exp.Literal.number(1))
        if part is DatePart.MILLISECOND:
            return exp.Floor(this=exp.Div(this=exp.Anonymous(this="MICROSECOND", expressions=[expr]), expression=lit(1000)))
        return exp.Anonymous(this=_MYSQL_PART_FUNCTIONS[part], expressions=[expr])

    def _day_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.Anonymous(this="DATEDIFF", expressions=[end, start])

    def _second_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.Anonymous(this="TIMESTAMPDIFF", expressions=[exp.var("SECOND"), start, end])

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATE_ADD`` clamps at month-end and keeps a DATE a DATE for day-or-coarser units."""
        word = "WEEK" if unit is TimeGranularity.WEEK_SUNDAY else unit.value.upper()
        return exp.DateAdd(this=expr.copy(), expression=count.copy(), unit=exp.var(word))

    def build_stat_agg_1arg(
        self, agg_name: StatAgg1Name, col_expr: Expression,
    ) -> Expression:
        """MySQL override for ``var_samp`` / ``var_pop``.

        sqlglot's MySQL transpiler rewrites ``VAR_SAMP`` → ``VARIANCE``
        (which on MySQL is ``VAR_POP``) — silently wrong. Emit the
        canonical MySQL names via ``exp.Anonymous`` to bypass sqlglot.
        """
        if agg_name in {"var_samp", "var_pop"}:
            return exp.Anonymous(
                this=agg_name.upper(),
                expressions=[col_expr.copy()],
            )
        return super().build_stat_agg_1arg(agg_name=agg_name, col_expr=col_expr)

    def build_covar_2arg(
        self,
        agg_name: StatAgg2Name,
        col_expr: Expression,
        other_expr: Expression,
    ) -> Expression:
        """MySQL has no native CORR / COVAR_* — use the
        variance-decomposition formula with MySQL-native VAR_SAMP /
        VAR_POP / STDDEV_SAMP names."""
        return _build_covar_decomposition(
            col_expr=col_expr,
            other_expr=other_expr,
            agg=agg_name,
            var_fn_samp="VAR_SAMP",
            var_fn_pop="VAR_POP",
            stddev_fn="STDDEV_SAMP",
        )


class MariadbDialect(MysqlDialect):
    """MariaDB renders as MySQL; it names its statement timeout differently. Library regex, numeric
    ``TRY_CAST`` (so text-to-number coercion) and ``json_value`` are refused: MariaDB has no
    ``REGEXP_LIKE``, shorter ``REGEXP_SUBSTR`` / ``REGEXP_REPLACE``, and reads JSON booleans as 1 / 0
    (lowerings: DEV-1975)."""
    enforces_unique_keys: ClassVar[bool] = True

    ds_type_aliases: frozenset[str] = frozenset({"mariadb"})
    database_name: ClassVar[str] = "MariaDB"
    sa_backend_names: frozenset[str] = frozenset({"mariadb"})
    library_functions: ClassVar[frozenset[str]] = PORTABLE_LIBRARY - REGEX_LIBRARY - {"json_value"}

    def statement_timeout(self, timeout_seconds: int) -> Expression | None:
        return set_statement(name="max_statement_time", value=timeout_seconds)

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        if to.is_type(*exp.DataType.NUMERIC_TYPES):
            raise UnsupportedServerFunctionError(
                summary="TRY_CAST to a number (and comparing or computing text with numbers) is not supported on "
                "MariaDB yet.",
                suggestion="Run it on MySQL, or express it in the field's own SQL (DatasetField.sql).",
            )
        return super().build_try_cast(value, to)
