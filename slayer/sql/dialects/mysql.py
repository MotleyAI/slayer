"""MysqlDialect.

MySQL has no native ``PERCENTILE_CONT`` (``build_median`` / ``build_percentile``
raise ``NotImplementedError``) and no native ``CORR`` / ``COVAR_SAMP`` /
``COVAR_POP`` (uses the variance-decomposition formula).

``var_samp`` / ``var_pop`` need the ``exp.Anonymous`` workaround because
sqlglot's MySQL transpiler rewrites them to ``VARIANCE`` (which on MySQL
is actually ``VAR_POP`` — silently wrong sample variance).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar, Optional

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.core.errors import UnsupportedServerFunctionError
from slayer.core.json_path import JsonPath, json_path_text
from slayer.core.keys import CastKey, ScalarCallKey
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    NativeFn,
    FALSE_WORDS,
    NUMBER_TEXT_PATTERN,
    PORTABLE_LIBRARY,
    REGEX_LIBRARY,
    TRUE_WORDS,
    MathFn,
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
)
from slayer.sql.dialects.patterns import ICU, RegexFlavour
from slayer.sql.dialects.reflection import TableLocation, call, info_schema_comments_query


#: MySQL 8.0 brought the ICU regex functions; 8.0.21 ``JSON_VALUE``.
_MYSQL_8_0 = (8, 0, 0)
_MYSQL_8_0_21 = (8, 0, 21)
_VERSION = re.compile(r"\s*(\d+)\.(\d+)(?:\.(\d+))?")
_NUMBERS = frozenset({DataType.INT, DataType.DOUBLE})
_GATED_FUNCTIONS = REGEX_LIBRARY | {"json_value"}


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


class MysqlDialect(SqlDialect):
    sqlglot_name: str = "mysql"
    database_name: ClassVar[str] = "MySQL"
    ds_type_aliases: frozenset[str] = frozenset({"mysql"})
    sa_backend_names: frozenset[str] = frozenset({"mysql"})
    regex_flavour: ClassVar[Optional[RegexFlavour]] = ICU
    library_functions: ClassVar[frozenset[str]] = PORTABLE_LIBRARY
    native_functions: ClassVar[frozenset[NativeFn]] = frozenset({
        "REPEAT", "LPAD", "RPAD", "REVERSE", "CONCAT_WS", "GREATEST", "LEAST",
    })
    extremum_propagates_null: ClassVar[bool] = True

    def profile_needed(self, keys: Iterable[object]) -> bool:
        """Regex and numeric ``TRY_CAST`` need MySQL 8.0, ``json_value`` 8.0.21: the version decides."""
        return any(
            (isinstance(k, ScalarCallKey) and k.name in _GATED_FUNCTIONS) or _numeric_try_cast(k) for k in keys
        )

    def server_profile_sql(self) -> str | None:
        return "SELECT VERSION()"

    def parse_server_profile(
        self, *, base_row: Sequence[Any] | None, correlated_row: Sequence[Any] | None = None,
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
    explain_prefix: str | None = "EXPLAIN FORMAT=JSON"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # Conservative: MySQL allows 256 for column aliases but errors (not truncates).
    max_identifier_bytes: int | None = 64
    url_scheme: str | None = "mysql+pymysql"
    url_backend_aliases: frozenset[str] = frozenset({"mariadb"})
    async_driver: str | None = "aiomysql"
    install_extra: str | None = "mysql"

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        return f"SET max_execution_time = {timeout_seconds * 1000}"

    def column_comments_query(self, location: TableLocation) -> exp.Select:
        return info_schema_comments_query(
            comment_column="column_comment", default_schema=call("DATABASE"), location=location,
        )

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """MySQL's ``TRUNCATE`` has no single-argument form — ``TRUNCATE(x)`` is
        a syntax error. A 1-arg ``trunc(x)`` becomes ``TRUNCATE(x, 0)``."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Trunc) and node.args.get("decimals") is None:
                node.set("decimals", exp.Literal.number(0))
            return node
        return tree.transform(_fix)

    def rewrite_statement(self, statement: Expression) -> Expression:
        """Emulate NULL placement on each ORDER BY term as written, so an alias stays an alias.

        sqlglot's emulation inlines an aliased projection, which ``ONLY_FULL_GROUP_BY`` can fail to match.
        """
        statement = statement.copy()
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

    def build_date_trunc(self, col_expr: Expression, granularity: TimeGranularity) -> Expression:
        """Sub-day units via ``DATE_FORMAT``: sqlglot renders them all as ``DATE(t)``."""
        fmt = _SUB_DAY_TRUNC_FORMATS.get(granularity)
        if fmt is None:
            return super().build_date_trunc(col_expr=col_expr, granularity=granularity)
        formatted = exp.Anonymous(this="DATE_FORMAT", expressions=[col_expr.copy(), exp.Literal.string(fmt)])
        return exp.Cast(this=formatted, to=exp.DataType.build("DATETIME"))

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

    def build_median(self, inner: Expression) -> Expression:
        # ``MariadbDialect`` inherits this, so never suggest "use MariaDB".
        raise NotImplementedError(
            "Aggregation 'median' is not supported on MySQL: MySQL has no native "
            "MEDIAN/PERCENTILE_CONT function and no Python UDF mechanism. "
            "Use a datasource with native percentile support (Postgres, DuckDB, "
            "ClickHouse, SQLite via UDF) or compute the value client-side."
        )

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on MySQL: "
            "MySQL has no native PERCENTILE_CONT. "
            "Use a datasource with native percentile support (Postgres, DuckDB, "
            "ClickHouse, SQLite via UDF) or compute the value client-side."
        )

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

    ds_type_aliases: frozenset[str] = frozenset({"mariadb"})
    database_name: ClassVar[str] = "MariaDB"
    sa_backend_names: frozenset[str] = frozenset({"mariadb"})
    library_functions: ClassVar[frozenset[str]] = PORTABLE_LIBRARY - REGEX_LIBRARY - {"json_value"}

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        return f"SET max_statement_time = {timeout_seconds}"

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        if to.is_type(*exp.DataType.NUMERIC_TYPES):
            raise UnsupportedServerFunctionError(
                summary="TRY_CAST to a number (and comparing or computing text with numbers) is not supported on "
                "MariaDB yet.",
                suggestion="Run it on MySQL, or express it in the model's own SQL (Column.sql).",
            )
        return super().build_try_cast(value, to)
