"""TsqlDialect (SQL Server 2022+).

Dotted aliases are mangled to ``[a___b]``: T-SQL's ``ORDER BY`` resolves ``[a.b]``
against the FROM scope, not the SELECT list.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar, Literal, Optional

from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.core.json_path import JsonPath
from slayer.core.keys import ScalarCallKey
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    DottedAliasManglingMixin,
    ExtremumFn,
    ServerProfile,
    SqlDialect,
    TextFn,
    anon,
    any_null,
    case,
    either,
    lit,
    operand_copy,
    pad_fill,
    StatAgg1Name,
    StatAgg2Name,
    _build_covar_decomposition,
)
from slayer.sql.dialects.patterns import RE2, RegexFlavour

#: SQL Server major versions: 16 = 2022 (``GREATEST`` / ``LEAST``), 17 = 2025 (regex functions).
_SQL_SERVER_2022 = (16, 0)
_SQL_SERVER_2025 = (17, 0)
_REGEX_FUNCTIONS = frozenset({"regexp_like", "regexp_extract", "regexp_replace"})
_GATED_FUNCTIONS = _REGEX_FUNCTIONS | {"greatest", "least"}
_HIGH_SURROGATES = (55296, 56319)


# sqlglot's tsql transpiler emits names (VAR_SAMP, VARIANCE_POP) T-SQL lacks.
_TSQL_STAT_NAMES: dict[str, str] = {
    "stddev_samp": "STDEV",
    "stddev_pop": "STDEVP",
    "var_samp": "VAR",
    "var_pop": "VARP",
}


# Caveat: a single-bracketed word-only path like ``[schema.tbl]`` false-positive mangles.
_TSQL_DOTTED_ALIAS_RE = re.compile(r"\[(\w+(?:\.\w+)+)\]", re.ASCII)


_DATEPART_UNITS = {
    DatePart.YEAR: "YEAR", DatePart.QUARTER: "QUARTER", DatePart.MONTH: "MONTH",
    DatePart.WEEK: "ISO_WEEK", DatePart.DAY: "DAY", DatePart.DAY_OF_YEAR: "DAYOFYEAR",
    DatePart.HOUR: "HOUR", DatePart.MINUTE: "MINUTE", DatePart.SECOND: "SECOND",
}
_DATEADD_UNITS = {
    TimeGranularity.SECOND: "SECOND", TimeGranularity.MINUTE: "MINUTE", TimeGranularity.HOUR: "HOUR",
    TimeGranularity.DAY: "DAY", TimeGranularity.WEEK: "WEEK", TimeGranularity.WEEK_SUNDAY: "WEEK",
    TimeGranularity.MONTH: "MONTH", TimeGranularity.QUARTER: "QUARTER", TimeGranularity.YEAR: "YEAR",
}


def _datepart(unit: str, expr: Expression) -> Expression:
    return exp.Anonymous(this="DATEPART", expressions=[exp.var(unit), expr])


def _dateadd(unit: str, count: Expression, expr: Expression) -> Expression:
    return exp.Anonymous(this="DATEADD", expressions=[exp.var(unit), count, expr])


def _days_since_monday(expr: Expression) -> Expression:
    """0 for Monday … 6 for Sunday, counted from 1900-01-01 (a Monday)."""
    days = exp.Anonymous(this="DATEDIFF", expressions=[
        exp.var("DAY"), exp.Cast(this=exp.Literal.string("1900-01-01"), to=exp.DataType.build("DATE")),
        expr.copy(),
    ])
    inner = exp.Mod(this=days, expression=exp.Literal.number(7))
    return exp.Mod(
        this=exp.Paren(this=exp.Add(this=exp.Paren(this=inner), expression=exp.Literal.number(7))),
        expression=exp.Literal.number(7),
    )


def _is_predicate(node: Expr) -> bool:
    """A predicate node; ``LIKE`` under its ``ESCAPE`` clause is part of that predicate."""
    if isinstance(node.parent, exp.Escape) and node.arg_key == "this":
        return False
    return isinstance(node, (exp.Predicate, exp.Connector, exp.Not, exp.Escape))


def _in_condition_position(node: Expr) -> bool:
    """WHERE / HAVING / JOIN ON / CASE WHEN condition, or an operand of a connective."""
    child, parent = node, node.parent
    while isinstance(parent, exp.Paren):
        child, parent = parent, parent.parent
    if isinstance(parent, (exp.Where, exp.Having, exp.Connector, exp.Not)):
        return True
    if isinstance(parent, exp.Join):
        return child.arg_key == "on"
    return isinstance(parent, exp.If) and child.arg_key == "this"


def _bit_value(predicate: Expr) -> Expression:
    """``CAST(CASE WHEN p THEN 1 WHEN NOT p THEN 0 END AS BIT)`` — NULL when ``p`` is unknown."""
    case = exp.Case(ifs=[
        exp.If(this=predicate, true=exp.Literal.number(1)),
        exp.If(this=exp.Not(this=exp.Paren(this=predicate.copy())), true=exp.Literal.number(0)),
    ])
    return exp.Cast(this=case, to=exp.DataType.build("BIT", dialect="tsql"))


class TsqlDialect(DottedAliasManglingMixin, SqlDialect):
    sqlglot_name: str = "tsql"
    ds_type_aliases: frozenset[str] = frozenset({"mssql", "sqlserver", "tsql"})
    vendor_cast_types: ClassVar[frozenset[DType]] = frozenset({
        exp.DataType.Type.MONEY, exp.DataType.Type.SMALLMONEY,
    })
    explain_prefix: str | None = "SET SHOWPLAN_ALL ON;"
    explain_postfix: str = "; SET SHOWPLAN_ALL OFF"
    log10_native: bool = True
    log2_native: bool = False
    integer_avg: bool = True
    max_identifier_bytes: int | None = 128  # sysname is nvarchar(128)
    # sqlglot would re-emit APPROX_DISTINCT, which T-SQL lacks.
    approx_count_distinct_anonymous_name: str | None = "APPROX_COUNT_DISTINCT"
    url_scheme: str | None = "mssql+pyodbc"
    install_extra: str | None = "sqlserver"
    dotted_alias_re: ClassVar[re.Pattern[str]] = _TSQL_DOTTED_ALIAS_RE
    alias_quote_open: ClassVar[str] = "["
    alias_quote_close: ClassVar[str] = "]"

    regex_flavour: ClassVar[RegexFlavour] = RE2.model_copy(update={"replacement_ampersand": True})
    json_db_types: ClassVar[frozenset[str]] = frozenset()
    like_brackets: ClassVar[bool] = True

    def profile_needed(self, keys: Iterable[object]) -> bool:
        """Regex needs SQL Server 2025 and ``greatest`` / ``least`` 2022: the version decides."""
        return any(isinstance(k, ScalarCallKey) and k.name in _GATED_FUNCTIONS for k in keys)

    def server_profile_sql(self) -> str | None:
        return "SELECT SERVERPROPERTY('ProductMajorVersion')"

    def parse_server_profile(
        self, *, base_row: Sequence[Any] | None, correlated_row: Sequence[Any] | None = None,
    ) -> ServerProfile:
        major = base_row[0] if base_row else None
        try:
            version = None if major is None else (int(str(major).strip()), 0)
        except ValueError:
            version = None
        return ServerProfile(version=version)

    def _require_regex(self, fn: str) -> None:
        self.require_version(minimum=_SQL_SERVER_2025, what=f"{fn}()", release="SQL Server 2025")

    def text_repeat(self, value: Expression, count: Expression) -> Expression:
        return anon("REPLICATE", value, count)

    def concat_text(self, left: Expression, right: Expression) -> Expression:
        return anon("CONCAT", left, right)

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        if fn in ("lpad", "rpad"):
            return self.portable_pad(fn=fn, s=args[0], n=args[1], fill=pad_fill(args))
        if fn == "contains":
            found = exp.GT(this=anon("CHARINDEX", args[1], args[0]), expression=lit(0))
            empty = exp.And(this=exp.EQ(this=anon("DATALENGTH", args[1]), expression=lit(0)),
                            expression=exp.Not(this=any_null(args[0])))
            return either(found, empty)
        return super().build_text_call(fn, args)

    def code_point_char(self, n: Expression) -> Expression:
        """``NCHAR``, astral code points as their UTF-16 surrogate pair (no supplementary collation needed)."""
        offset = exp.Paren(this=exp.Sub(this=n.copy(), expression=lit(65536)))
        high = anon("NCHAR", exp.Add(this=lit(55296), expression=exp.IntDiv(this=offset, expression=lit(1024))))
        low = anon("NCHAR", exp.Add(this=lit(56320), expression=exp.Mod(this=offset.copy(), expression=lit(1024))))
        return case((exp.LT(this=n.copy(), expression=lit(65536)), anon("NCHAR", n)), default=anon("CONCAT", high, low))

    def first_code_point(self, s: Expression) -> Expression:
        """``UNICODE`` reads one UTF-16 unit: a surrogate pair is combined; ``''`` is 0."""
        first = anon("UNICODE", s)
        second = anon("UNICODE", self.char_substring(s, lit(2), lit(1)))
        astral = exp.Add(this=exp.Add(this=exp.Mul(this=exp.Paren(this=exp.Sub(this=first.copy(), expression=lit(55296))),
                                                   expression=lit(1024)),
                                      expression=exp.Sub(this=second, expression=lit(56320))), expression=lit(65536))
        return case(
            (exp.EQ(this=anon("DATALENGTH", s), expression=lit(0)), lit(0)),
            (exp.Between(this=first.copy(), low=lit(_HIGH_SURROGATES[0]), high=lit(_HIGH_SURROGATES[1])), astral),
            default=first,
        )

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        """The fields as a JSON array (``STRING_ESCAPE``d), indexed by ``JSON_VALUE``; ``''`` past the end."""
        escaped = anon("REPLACE", anon("STRING_ESCAPE", value, lit("json")), anon("STRING_ESCAPE", delimiter, lit("json")),
                       lit('","'))
        array = anon("CONCAT", lit('["'), escaped, lit('"]'))
        if index > 0:
            path: Expression = lit(f"$[{index - 1}]")
        else:
            position = exp.Add(this=self.field_count(value, delimiter), expression=lit(index))
            path = anon("CONCAT", lit("$["), position, lit("]"))
        field = exp.Coalesce(this=anon("JSON_VALUE", array, path), expressions=[lit("")])
        return case((any_null(value, delimiter), exp.Null()), default=field)

    def cube_root(self, value: Expression) -> Expression:
        magnitude = exp.Cast(this=exp.Abs(this=value.copy()), to=exp.DataType.build("FLOAT"))
        root = anon("POWER", magnitude, exp.Div(this=lit(1), expression=exp.Literal.number("3.0")))
        return exp.Mul(this=exp.Sign(this=value.copy()), expression=root)

    def build_modulo(self, left: Expression, right: Expression, integer: bool) -> Expression:
        """``%`` rejects FLOAT operands: ``a - b * trunc(a / b)``."""
        if integer:
            return super().build_modulo(left, right, integer)
        quotient = self.truncate(exp.Div(this=operand_copy(left), expression=exp.Nullif(this=operand_copy(right),
                                                                                    expression=lit(0))))
        return exp.Paren(this=exp.Sub(this=operand_copy(left), expression=exp.Mul(this=operand_copy(right),
                                                                                 expression=quotient)))

    def build_extremum(self, fn: ExtremumFn, args: tuple[Expression, ...]) -> Expression:
        self.require_version(minimum=_SQL_SERVER_2022, what=f"{fn}()", release="SQL Server 2022")
        return super().build_extremum(fn, args)

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        """``REGEXP_INSTR(…) > 0``: ``REGEXP_LIKE`` needs compatibility level 170, this does not."""
        self._require_regex("regexp_like")
        position = anon("REGEXP_INSTR", value, self.regex_text(pattern, case_insensitive=case_insensitive))
        return exp.GT(this=position, expression=lit(0))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        self._require_regex("regexp_extract")
        extracted = anon("REGEXP_SUBSTR", value, self.regex_text(pattern), lit(1), lit(1), lit("c"), lit(group))
        return self.skipped_group_empty(value, pattern, group, extracted)

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        self._require_regex("regexp_replace")
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern, case_insensitive=case_insensitive),
                    self.replacement_text(replacement), lit(1), lit(0), lit("c"))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        valid = exp.EQ(this=anon("ISJSON", value), expression=lit(1))
        return case((valid, anon("JSON_VALUE", value, lit(path.text))))

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """T-SQL has no ``IS NOT DISTINCT FROM`` / ``<=>``."""
        return self._expanded_null_safe_eq(left, right)

    def build_ordered(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """Pin the default to T-SQL's native null order; an explicit policy is honoured.

        sqlglot's nulls-last ``CASE WHEN <alias> IS NULL`` emulation mis-resolves the bracketed alias.
        """
        if nulls == "default":
            return exp.Ordered(
                this=order_col, desc=descending, nulls_first=not descending,
            )
        return super().build_ordered(
            order_col, descending=descending, nulls=nulls,
        )

    def build_order_terms(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
        resolved: Optional[Expression] = None,
    ) -> list[exp.Ordered]:
        """An explicit NULL placement is a leading ``CASE WHEN <expr> IS NULL`` key over the resolved
        expression (an alias is not visible inside an ORDER BY expression), then the native term."""
        if nulls == "default":
            return [self.build_ordered(order_col, descending=descending)]
        null_rank = exp.Case(
            ifs=[exp.If(
                this=exp.Is(this=operand_copy(resolved if resolved is not None else order_col), expression=exp.Null()),
                true=exp.Literal.number(0 if nulls == "first" else 1),
            )],
            default=exp.Literal.number(1 if nulls == "first" else 0),
        )
        return [
            self.build_ordered(null_rank, descending=False),
            self.build_ordered(order_col, descending=descending),
        ]

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """``DATETRUNC`` (2022+); ``iso_week`` keeps weeks ``@@DATEFIRST``-independent."""
        if granularity == TimeGranularity.WEEK_SUNDAY:
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity,
            )
        gran_str = granularity.value
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        tsql_gran = "iso_week" if gran_str == "week" else gran_str
        return exp.Anonymous(
            this="DATETRUNC",
            expressions=[exp.Var(this=tsql_gran), col_expr],
        )

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """``LEN`` drops trailing spaces: ``length`` counts an ``NVARCHAR(MAX)`` copy through a
        sentinel, and a 2-arg ``SUBSTRING`` (which requires a length) reads to the end via ``DATALENGTH``."""
        def _fix(node: Expression) -> Expression:
            if isinstance(node, exp.Substring) and node.args.get("length") is None:
                node.set("length", exp.Anonymous(this="DATALENGTH", expressions=[node.this.copy()]))
            if isinstance(node, exp.Length):
                text = exp.Cast(this=node.this.copy(), to=exp.DataType.build("NVARCHAR(MAX)", dialect="tsql"))
                padded = exp.Add(this=text, expression=exp.Literal.string("x"))
                return exp.Paren(this=exp.Sub(
                    this=exp.Anonymous(this="LEN", expressions=[padded]), expression=exp.Literal.number(1),
                ))
            return node
        return tree.transform(_fix)

    def rewrite_statement(self, statement: Expression) -> Expression:
        """T-SQL predicates are conditions only: one in a value position becomes its BIT value."""
        statement = statement.copy()
        for node in reversed(list(statement.dfs())):
            if _is_predicate(node) and not _in_condition_position(node):
                node.replace(_bit_value(node.copy()))
        return statement

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("value").as_("i")).from_(exp.Table(
            this=exp.Anonymous(this="GENERATE_SERIES", expressions=[
                exp.Literal.number(0), exp.Literal.number(size - 1),
            ]),
            alias=exp.TableAlias(this=exp.to_identifier("_seq")),
        ))

    def build_current_date(self) -> Expression:
        return exp.Cast(this=exp.Anonymous(this="GETDATE"), to=exp.DataType.build("DATE"))

    def build_current_timestamp(self) -> Expression:
        return exp.Anonymous(this="SYSDATETIME")

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """``DATEPART``; weekday from a fixed Monday epoch so ``DATEFIRST`` never matters."""
        if part is DatePart.DAY_OF_WEEK:
            return exp.Add(this=exp.Paren(this=_days_since_monday(expr)), expression=exp.Literal.number(1))
        if part is DatePart.ISO_YEAR:
            thursday = _dateadd("DAY", exp.Sub(
                this=exp.Literal.number(3), expression=exp.Paren(this=_days_since_monday(expr)),
            ), expr.copy())
            return _datepart("YEAR", thursday)
        return _datepart(_DATEPART_UNITS[part], expr)

    def build_date_diff(
        self, *, unit: TimeGranularity, start: Expression, end: Expression, operand: DataType,
    ) -> Expression:
        """``DATEDIFF_BIG`` counts boundaries; its ``WEEK`` is Sunday-based regardless of ``DATEFIRST``."""
        if unit in SUB_DAY_GRANULARITIES and operand is DataType.DATE:
            start, end = self.promote_to_timestamp(start), self.promote_to_timestamp(end)
        else:
            start, end = start.copy(), end.copy()
        if unit is TimeGranularity.WEEK:
            # Monday boundaries = Sunday boundaries of the day before.
            start = _dateadd("DAY", exp.Literal.number(-1), start)
            end = _dateadd("DAY", exp.Literal.number(-1), end)
        return exp.Anonymous(this="DATEDIFF_BIG", expressions=[exp.var(_DATEADD_UNITS[unit]), start, end])

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATEADD`` (clamps at month-end); a DATE is promoted to DATETIME2 for sub-day units."""
        base = (
            self.promote_to_timestamp(expr)
            if unit in SUB_DAY_GRANULARITIES and operand is DataType.DATE else expr.copy()
        )
        return _dateadd(_DATEADD_UNITS[unit], count.copy(), base)

    def build_median(self, inner: Expression) -> Expression:
        raise NotImplementedError(
            "Aggregation 'median' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT in T-SQL is a window function (requires OVER clause) "
            "and cannot be used as a GROUP BY aggregate. "
            "Use a window subquery or compute the value client-side."
        )

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        raise NotImplementedError(
            "Aggregation 'percentile' is not supported on T-SQL (SQL Server): "
            "PERCENTILE_CONT requires a window function OVER clause in T-SQL "
            "and is not valid as a GROUP BY aggregate. "
            "Compute the value client-side or restructure as a window query."
        )

    def build_stat_agg_1arg(
        self, agg_name: StatAgg1Name, col_expr: Expression,
    ) -> Expression:
        """Emit the T-SQL names (``STDEV`` / ``STDEVP`` / ``VAR`` / ``VARP``)."""
        if agg_name in _TSQL_STAT_NAMES:
            return exp.Anonymous(
                this=_TSQL_STAT_NAMES[agg_name],
                expressions=[col_expr.copy()],
            )
        return super().build_stat_agg_1arg(agg_name=agg_name, col_expr=col_expr)

    def build_covar_2arg(
        self,
        agg_name: StatAgg2Name,
        col_expr: Expression,
        other_expr: Expression,
    ) -> Expression:
        """T-SQL has no CORR / COVAR_*: variance decomposition with T-SQL names."""
        return _build_covar_decomposition(
            col_expr=col_expr,
            other_expr=other_expr,
            agg=agg_name,
            var_fn_samp="VAR",
            var_fn_pop="VARP",
            stddev_fn="STDEV",
        )

    def apply_pagination(
        self,
        select: exp.Select,
        *,
        limit: "int | None",
        offset: "int | None",
    ) -> exp.Select:
        """SQL Server rejects ``OFFSET`` without ``ORDER BY``: add ``ORDER BY (SELECT NULL)``."""
        if offset is not None and select.args.get("order") is None:
            select = select.order_by(
                exp.Subquery(this=exp.Select().select(exp.Null())),
            )
        return super().apply_pagination(select, limit=limit, offset=offset)
