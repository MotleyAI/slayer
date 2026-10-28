"""ClickhouseDialect.

ClickHouse uses native ``median(x)`` directly and the parametric
``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP / COVAR_POP
are native. log10 and log2 are native.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Sequence
from datetime import date
from typing import Any, ClassVar, Optional

from sqlalchemy.types import NullType
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.core.json_path import JsonPath
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    LIST_ELEMENT,
    LIST_VALUE,
    ExtremumFn,
    ListRelation,
    ServerProfile,
    SqlDialect,
    TextFn,
    anon,
    as_text,
    case,
    element_lambda,
    is_not_null,
    is_null,
    iso_text,
    lit,
    pad_fill,
)
from slayer.sql.dialects._clickhouse_cells import array_reader, read_bool
from slayer.sql.dialects.patterns import RE2, RegexFlavour
from slayer.sql.dialects.reflection import (
    CatalogEngine,
    ReflectedColumn,
    TableLocation,
    all_of,
    call,
    col,
    column_from_type_string,
    eq_literal,
    fetch,
    relation,
)

#: Byte-based string functions and their character (UTF-8) counterparts.
_UTF8_FUNCTIONS = {exp.Substring: "substringUTF8", exp.Lower: "lowerUTF8", exp.Upper: "upperUTF8"}
_JSON_SCALAR_TYPES = ("Int64", "UInt64", "Double", "Bool")
#: Types ClickHouse refuses inside ``Nullable``.
_NEVER_NULLABLE = (*exp.DataType.NESTED_TYPES, exp.DataType.Type.LOWCARDINALITY, exp.DataType.Type.JSON)


def _compact(values: Expression) -> Expression:
    """``arrayFilter(e -> e IS NOT NULL, values)`` (the lambda comes first here)."""
    element = exp.to_identifier(LIST_ELEMENT)
    keep = exp.Lambda(this=is_not_null(exp.Column(this=element.copy())), expressions=[element])
    return exp.Anonymous(this="arrayFilter", expressions=[keep, values.copy()])


def _utf8(node: Expression) -> Expression:
    """Character semantics for the byte-based string functions."""
    for cls, name in _UTF8_FUNCTIONS.items():
        if type(node) is cls:
            args = [node.this, *(node.args[k] for k in ("start", "length") if node.args.get(k) is not None)]
            return exp.Anonymous(this=name, expressions=args)
    if isinstance(node, exp.StrPosition):
        return exp.Anonymous(this="positionUTF8", expressions=[node.this, node.args["substr"]])
    return node


def _byte(value: Expression, position: int) -> Expression:
    return exp.Anonymous(this="ascii", expressions=[exp.Anonymous(this="substring", expressions=[
        value.copy(), exp.Literal.number(position), exp.Literal.number(1)])])


def _bool_as_int(cast: Expression, to: exp.DataType) -> Expression:
    """A boolean as 0 / 1, like ClickHouse's own predicates: the HTTP driver returns ``Bool`` as text."""
    return anon("toUInt8", cast) if to.is_type(exp.DataType.Type.BOOLEAN) else cast

_CH_CORRELATED_SETTING = "allow_experimental_correlated_subqueries"
_CORRELATED_MIN_VERSION = (25, 4)
_CORRELATED_DEFAULT_ON_VERSION = (25, 8)
_TIMEOUT_SETTING = "max_execution_time"
_ABSENT = object()
_BOOL_SETTING_VALUES = {"1": True, "true": True, "0": False, "false": False}
_BOOL_TYPES = frozenset({"Bool", "Nullable(Bool)"})


def _parse_clickhouse_version(raw: Any) -> tuple[int, int] | None:
    """Parse a ClickHouse ``version()`` string to ``(major, minor)``; ``None`` if unparseable."""
    if not isinstance(raw, str):
        return None
    match = re.match(r"\s*v?(\d+)\.(\d+)", raw)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)))


def _parse_readonly(raw: Any) -> int | None:
    if isinstance(raw, int) and not isinstance(raw, bool):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw)
    return None


def _parse_bool_setting(raw: Any) -> bool | None:
    """``getSetting`` of a Bool setting: the driver may yield a bool, an int, or its text."""
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return {0: False, 1: True}.get(raw)
    if isinstance(raw, str):
        return _BOOL_SETTING_VALUES.get(raw.strip().lower())
    return None


def _attach_ch_correlated_setting(ast: Expression) -> None:
    """Force ``allow_experimental_correlated_subqueries = 1``, preserving other settings.

    Any prior entry for this setting is dropped so a caller-supplied ``= 0`` can't leave
    the correlated subquery emitted with the setting disabled.
    """
    holder = _settings_holder(ast)
    kept = [
        s
        for s in (holder.args.get("settings") or [])
        if getattr(s.this, "name", None) != _CH_CORRELATED_SETTING
    ]
    holder.set("settings", [*kept, exp.var(_CH_CORRELATED_SETTING).eq(1)])


def _settings_holder(ast: Expression) -> Expression:
    """The node carrying (or that should carry) this statement's ClickHouse ``SETTINGS``.

    sqlglot parks a trailing ``SETTINGS`` on the last branch of an unparenthesised
    ``UNION`` rather than the root, so honour that placement to avoid emitting two
    clauses. Only the set-operation's own right spine is followed — a nested ``FROM``
    subquery's ``SETTINGS`` is local to that rowset.
    """
    if ast.args.get("settings"):
        return ast
    if isinstance(ast, exp.SetOperation):
        node: Expression = ast
        while isinstance(node, exp.SetOperation):
            node = node.expression  # right branch owns the trailing SETTINGS
        if node.args.get("settings"):
            return node
    return ast


_CH_PART_FUNCTIONS = {
    DatePart.YEAR: "toYear", DatePart.ISO_YEAR: "toISOYear", DatePart.QUARTER: "toQuarter",
    DatePart.MONTH: "toMonth", DatePart.WEEK: "toISOWeek", DatePart.DAY: "toDayOfMonth",
    DatePart.DAY_OF_WEEK: "toDayOfWeek", DatePart.DAY_OF_YEAR: "toDayOfYear",
    DatePart.HOUR: "toHour", DatePart.MINUTE: "toMinute", DatePart.SECOND: "toSecond",
}
_CH_ADD_FUNCTIONS = {
    TimeGranularity.SECOND: "addSeconds", TimeGranularity.MINUTE: "addMinutes",
    TimeGranularity.HOUR: "addHours", TimeGranularity.DAY: "addDays",
    TimeGranularity.WEEK: "addWeeks", TimeGranularity.WEEK_SUNDAY: "addWeeks",
    TimeGranularity.MONTH: "addMonths", TimeGranularity.QUARTER: "addQuarters",
    TimeGranularity.YEAR: "addYears",
}


class ClickhouseDialect(SqlDialect):
    sqlglot_name: str = "clickhouse"
    ilike_native: ClassVar[bool] = True
    regex_flavour: ClassVar[RegexFlavour] = RE2
    json_db_types: ClassVar[frozenset[str]] = frozenset({"JSON", "OBJECT"})
    like_escape_clause: ClassVar[bool] = False
    database_name: ClassVar[str] = "ClickHouse"

    def list_length(self, values: Expression) -> Expression:
        return anon("length", _compact(values))

    def list_contains(self, values: Expression, value: Expression) -> Expression:
        return anon("has", _compact(values), value)

    def list_overlaps(self, values: Expression, other: Expression) -> Expression:
        return anon("hasAny", _compact(values), _compact(other))

    def list_contains_all(self, values: Expression, other: Expression) -> Expression:
        return anon("hasAll", values, _compact(other))

    def empty_list(self, element: Optional[DataType]) -> Expression:
        return exp.Array(expressions=[])

    def result_readers(self, description: Sequence[Any]) -> dict[int, Callable[[Any], Any]]:
        """The HTTP driver returns ``Bool`` and ``Array(T)`` cells as their text."""
        readers: dict[int, Callable[[Any], Any]] = {}
        for i, d in enumerate(description):
            type_code = d[1] if isinstance(d[1], str) else ""
            reader = read_bool if type_code in _BOOL_TYPES else array_reader(type_code)
            if reader is not None:
                readers[i] = reader
        return readers

    def list_exists(
        self, values: Expression, *, alias: exp.Identifier, element: Optional[DataType], predicate: Expr,
    ) -> Expression:
        """``arrayExists(e -> predicate(e), values)``: no correlated subquery reads the outer row's list."""
        return anon("arrayExists", element_lambda(predicate, alias=alias), _compact(values))

    def list_relation(
        self, values: Expression, *, alias: exp.Identifier, element: Optional[DataType], leading: bool,
        distinct: bool = False,
    ) -> ListRelation:
        """``ARRAY JOIN`` of one-field named tuples, so the element reads as ``alias.value``."""
        element_name = exp.to_identifier(LIST_ELEMENT)
        row_type = exp.DataType.build(f"STRUCT<{LIST_VALUE} {(element or DataType.UNKNOWN).value}>")
        as_row = exp.Lambda(this=exp.Cast(this=anon("tuple", exp.Column(this=element_name.copy())), to=row_type),
                            expressions=[element_name])
        kept = anon("arrayDistinct", _compact(values)) if distinct else _compact(values)
        rows = exp.Anonymous(this="arrayMap", expressions=[as_row, kept])
        return ListRelation(relation=exp.Alias(this=rows, alias=alias.copy()), join_kind="ARRAY")

    def build_collect(self, value: Expression, element: Optional[DataType]) -> Expression:
        """``groupUniqArray`` skips NULLs and returns ``[]`` over none."""
        return anon("arraySort", anon("groupUniqArray", value))

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """Character, not byte, semantics for substring / lower / upper / position."""
        return tree.transform(_utf8)

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        if fn == "left":
            return anon("leftUTF8", *args)
        if fn == "right":
            # rightUTF8 ignores a negative constant length over a column: drop the first |n| as a kept length.
            s, n = args
            kept = anon("greatest", exp.Add(this=anon("lengthUTF8", s), expression=n.copy()), lit(0))
            return anon("rightUTF8", s, case((exp.LT(this=n.copy(), expression=lit(0)), kept), default=n.copy()))
        if fn in ("lpad", "rpad"):
            node = anon("leftPadUTF8" if fn == "lpad" else "rightPadUTF8", args[0], args[1], pad_fill(args))
            return self._non_positive_empty(args[1], node, source=args[0])
        if fn == "reverse":
            # reverseUTF8 garbles four-byte characters; RE2 splits by code point.
            chars = anon("extractAll", anon("assumeNotNull", args[0]), lit("(?s)."))
            return case((is_null(args[0]), exp.Null()),
                        default=anon("arrayStringConcat", anon("arrayReverse", chars)))
        if fn == "concat_ws":
            item = exp.to_identifier("_v")
            present = exp.Lambda(this=anon("isNotNull", exp.column(item.copy())), expressions=[item])
            values = exp.Array(expressions=[a.copy() for a in args[1:]])
            return case((anon("isNull", args[0]), exp.Null()),
                        default=anon("arrayStringConcat", anon("arrayFilter", present, values), args[0]))
        return super().build_text_call(fn, args)

    def code_point_char(self, n: Expression) -> Expression:
        """The UTF-8 bytes of code point ``n``."""
        def part(shift: int, mask_base: int) -> Expression:
            shifted = n.copy() if shift == 0 else anon("intDiv", n, lit(1 << shift))
            return exp.Add(this=lit(mask_base), expression=anon("modulo", shifted, lit(64)) if mask_base == 128
                           else shifted)
        two = anon("char", part(6, 192), part(0, 128))
        three = anon("char", part(12, 224), part(6, 128), part(0, 128))
        four = anon("char", part(18, 240), part(12, 128), part(6, 128), part(0, 128))
        return case((exp.LT(this=n.copy(), expression=lit(128)), anon("char", n)),
                    (exp.LT(this=n.copy(), expression=lit(2048)), two),
                    (exp.LT(this=n.copy(), expression=lit(65536)), three), default=four)

    def first_code_point(self, s: Expression) -> Expression:
        """Decoded from the first character's UTF-8 bytes; ``''`` is 0."""
        b1 = _byte(s, 1)

        def tail(position: int) -> Expression:
            return exp.Paren(this=exp.Sub(this=_byte(s, position), expression=lit(128)))

        def scaled(value: Expression, factor: int) -> Expression:
            return exp.Mul(this=value, expression=lit(factor))
        two = exp.Add(this=scaled(exp.Paren(this=exp.Sub(this=b1.copy(), expression=lit(192))), 64), expression=tail(2))
        three = exp.Add(this=exp.Add(this=scaled(exp.Paren(this=exp.Sub(this=b1.copy(), expression=lit(224))), 4096),
                                     expression=scaled(tail(2), 64)), expression=tail(3))
        four = exp.Add(this=exp.Add(this=exp.Add(
            this=scaled(exp.Paren(this=exp.Sub(this=b1.copy(), expression=lit(240))), 262144),
            expression=scaled(tail(2), 4096)), expression=scaled(tail(3), 64)), expression=tail(4))
        return case((exp.LT(this=b1.copy(), expression=lit(128)), b1.copy()),
                    (exp.LT(this=b1.copy(), expression=lit(224)), two),
                    (exp.LT(this=b1.copy(), expression=lit(240)), three), default=four)

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        """``splitByString`` indexes from either end; past the end is the empty default. An array cannot be
        ``Nullable``, so NULL operands are tested first."""
        fields = anon("splitByString", anon("assumeNotNull", delimiter), anon("assumeNotNull", value))
        return case((exp.Or(this=is_null(value), expression=is_null(delimiter)), exp.Null()),
                    default=anon("arrayElement", fields, lit(index)))

    def cube_root(self, value: Expression) -> Expression:
        return anon("cbrt", value)

    def natural_log(self, value: Expression) -> Expression:
        """``ln`` is approximate (~1e-9); ``log2`` is exact."""
        return exp.Mul(this=anon("log2", value), expression=exp.Literal.number(repr(math.log(2))))

    def build_is_not_distinct_from(self, left: Expression, right: Expression) -> Expression:
        """``isNotDistinctFrom`` is only allowed in ``JOIN ON``."""
        return self._expanded_null_safe_eq(left, right)

    def build_cast(self, value: Expression, to: exp.DataType) -> Expression:
        """A ``Nullable`` target (where ClickHouse allows one), so NULL stays NULL."""
        target = to.copy()
        if not target.is_type(*_NEVER_NULLABLE):
            target.set("nullable", True)
        return _bool_as_int(exp.Cast(this=value, to=target), to)

    def text_repeat(self, value: Expression, count: Expression) -> Expression:
        return anon("repeat", value, count)

    def build_extremum(self, fn: ExtremumFn, args: tuple[Expression, ...]) -> Expression:
        """``greatest`` / ``least`` propagate NULL before 24.12."""
        return self.coalesced_extremum(fn, args, spelling="lower")

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        return anon("match", value, self.regex_text(pattern, case_insensitive=case_insensitive))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        """``regexpExtract`` returns ``''`` without a match: a match is tested first."""
        text = self.regex_text(pattern)
        return case((anon("match", value, text), anon("regexpExtract", value, text.copy(), lit(group))))

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        return anon("replaceRegexpAll", value, self.regex_text(pattern, case_insensitive=case_insensitive),
                    self.replacement_text(replacement))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        """A native input is serialised from the cast's operand: a ``CAST(… AS JSON)`` needs an experimental
        setting before 25.3."""
        document = anon("toString", value.this if isinstance(value, exp.Cast) else value) if native else value.copy()
        keys = [lit(step + 1) if isinstance(step, int) else lit(step) for step in path.steps]
        kind = anon("JSONType", document, *keys)
        return case(
            (exp.EQ(this=kind, expression=lit("String")), anon("JSONExtractString", document, *keys)),
            (exp.In(this=kind.copy(), expressions=[lit(t) for t in _JSON_SCALAR_TYPES]),
             anon("JSONExtractRaw", document, *keys)),
        )
    ds_type_aliases: frozenset[str] = frozenset({"clickhouse"})
    sa_backend_names: frozenset[str] = frozenset({"clickhouse"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True
    correlated_subqueries_gated: bool = True
    timeout_reads_profile: bool = True
    global_in_subqueries: bool = True
    url_scheme: str | None = "clickhouse+http"
    install_extra: str | None = "clickhouse"

    def column_comments_query(self, location: TableLocation) -> exp.Select:
        return self._system_columns(location, field=col("comment"))

    def _system_columns(self, location: TableLocation, *, field: exp.Column) -> exp.Select:
        """``(name, field)`` of one table's columns from ``system.columns``."""
        database = exp.Literal.string(location.schema_name) if location.schema_name else call("currentDatabase")
        return (
            exp.select(col("name"), field)
            .from_(relation("columns", db="system"))
            .where(all_of([eq_literal(col("table"), location.name), exp.EQ(this=col("database"), expression=database)]))
        )

    def reflect_columns(
        self, *, sa_engine: CatalogEngine | None, inspector: Any, table: str, schema: str | None, catalog: str | None,
    ) -> list[ReflectedColumn]:
        """A column the driver cannot type (``JSON``) is read from its ``system.columns`` type."""
        columns = super().reflect_columns(
            sa_engine=sa_engine, inspector=inspector, table=table, schema=schema, catalog=catalog)
        if not any(isinstance(c.type, NullType) for c in columns):
            return columns
        location = TableLocation(name=table, schema_name=schema, catalog=catalog)
        declared = dict(fetch(sa_engine, self._system_columns(location, field=col("type")), dialect=self.sqlglot_name))
        return [
            column_from_type_string(
                name=c.name, data_type=declared.get(c.name), comment=c.comment,
                zoned_types=self.zoned_timestamp_db_types, json_types=self.json_db_types,
            ) if isinstance(c.type, NullType) else c
            for c in columns
        ]

    def set_connection_timeout(self, dbapi_connection: Any, timeout_seconds: int) -> object:
        """Per-request HTTP setting: the driver keeps no session, so a ``SET`` never reaches the query."""
        settings = dbapi_connection.transport.ch_settings
        prior = settings.get(_TIMEOUT_SETTING, _ABSENT)
        settings[_TIMEOUT_SETTING] = timeout_seconds
        return prior

    def restore_connection_timeout(self, dbapi_connection: Any, prior: object) -> None:
        settings = dbapi_connection.transport.ch_settings
        if prior is _ABSENT:
            settings.pop(_TIMEOUT_SETTING, None)
        else:
            settings[_TIMEOUT_SETTING] = prior

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        spelled = to.sql(dialect=self.sqlglot_name)
        if spelled.startswith("Nullable(") and spelled.endswith(")"):
            spelled = spelled[len("Nullable("):-1]
        cast = exp.Anonymous(this="accurateCastOrNull", expressions=[as_text(value), exp.Literal.string(spelled)])
        return _bool_as_int(cast, to)

    def server_profile_sql(self) -> str | None:
        return "SELECT version(), getSetting('readonly')"

    def correlated_setting_sql(self, profile: ServerProfile) -> str | None:
        """``getSetting`` of a setting the server lacks errors, so ask only where it exists."""
        if profile.version is None or profile.version < _CORRELATED_MIN_VERSION:
            return None
        return f"SELECT getSetting('{_CH_CORRELATED_SETTING}')"

    def parse_server_profile(
        self, *, base_row: Sequence[Any] | None, correlated_row: Sequence[Any] | None = None,
    ) -> ServerProfile:
        base = tuple(base_row or ())
        return ServerProfile(
            version=_parse_clickhouse_version(base[0]) if len(base) > 0 else None,
            readonly=_parse_readonly(base[1]) if len(base) > 1 else None,
            correlated_subqueries=_parse_bool_setting(correlated_row[0]) if correlated_row else None,
        )

    def timeout_permitted(self, profile: ServerProfile) -> bool:
        """``readonly = 1`` forbids every setting change; ``0`` and ``2`` allow it."""
        return profile.readonly != 1

    def correlated_subquery_refusal(self, profile: ServerProfile) -> str | None:
        version = profile.version
        if version is None or version < _CORRELATED_MIN_VERSION:
            detected = (
                "the server version could not be determined." if version is None
                else f"detected {version[0]}.{version[1]}. Upgrade the server."
            )
            return f"ClickHouse supports correlated subqueries only from server 25.4, but {detected}"
        if profile.correlated_subqueries or profile.readonly not in (None, 1):
            return None
        setting_state = (
            "could not be read" if profile.correlated_subqueries is None else "is 0"
        )
        level = (
            "the user's read-only level could not be determined" if profile.readonly is None
            else "the user is readonly = 1, which may not change it"
        )
        remedies = [
            f"enable {_CH_CORRELATED_SETTING} in the user's settings profile",
            "use readonly = 2",
        ]
        if version < _CORRELATED_DEFAULT_ON_VERSION:
            remedies.append("upgrade the server to 25.8 or later, where it is on by default")
        return (
            f"The user's {_CH_CORRELATED_SETTING} setting {setting_state}, and {level}. "
            f"Remedies: {'; or '.join(remedies)}."
        )

    def attach_correlated_setting(self, statement: Expression) -> None:
        _attach_ch_correlated_setting(statement)

    def rewrite_statement(self, statement: Expression) -> Expression:
        """ORDER BY a projection's expression, not its alias: a dotted alias such as
        ``orders.created_at`` resolves to the source column of that name.

        A windowed projection keeps its alias.
        """
        statement = statement.copy()
        for select in statement.find_all(exp.Select):
            order = select.args.get("order")
            if order is None:
                continue
            projections = {p.alias: p.this for p in select.expressions if isinstance(p, exp.Alias)}
            for term in order.expressions:
                ref = term.this
                projected = projections.get(ref.name) if isinstance(ref, exp.Column) and not ref.table else None
                if projected is not None and projected.find(exp.Window) is None:
                    ref.replace(projected.copy())
        return statement

    def in_subquery_key(self, key: Expression) -> Expression:
        """``toNullable(key)``: pre-25 ``nullIn`` errors probing a non-Nullable set with a Nullable key."""
        return exp.Anonymous(this="toNullable", expressions=[key.copy()])

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("number").as_("i")).from_(exp.Table(
            this=exp.Anonymous(this="numbers", expressions=[exp.Literal.number(size)]),
        ))

    def promote_to_timestamp(self, expr: Expression) -> Expression:
        return exp.Anonymous(this="toDateTime64", expressions=[expr.copy(), exp.Literal.number(6)])

    def build_temporal_literal(self, *, value: date, dt: DataType) -> Expression:
        if dt is DataType.DATE:
            return exp.Anonymous(this="toDate", expressions=[exp.Literal.string(value.isoformat())])
        return self.promote_to_timestamp(exp.Literal.string(iso_text(value)))

    def build_current_date(self) -> Expression:
        return exp.Anonymous(this="today")

    def build_current_timestamp(self) -> Expression:
        return exp.Anonymous(this="now")

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """``to<Part>`` functions; ``toDayOfWeek`` is ISO (Monday=1) in its default mode."""
        return exp.Anonymous(this=_CH_PART_FUNCTIONS[part], expressions=[expr])

    def build_date_diff(
        self, *, unit: TimeGranularity, start: Expression, end: Expression, operand: DataType,
    ) -> Expression:
        """``dateDiff`` counts unit boundaries crossed; weeks compose from truncation."""
        if unit in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY):
            return super().build_date_diff(unit=unit, start=start, end=end, operand=operand)
        if unit in SUB_DAY_GRANULARITIES and operand is DataType.DATE:
            start, end = self.promote_to_timestamp(start), self.promote_to_timestamp(end)
        else:
            start, end = start.copy(), end.copy()
        return exp.Anonymous(this="dateDiff", expressions=[exp.Literal.string(unit.value), start, end])

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``add<Unit>s`` (month-based ones clamp at month-end)."""
        base = (
            self.promote_to_timestamp(expr)
            if unit in SUB_DAY_GRANULARITIES and operand is DataType.DATE else expr.copy()
        )
        return exp.Anonymous(this=_CH_ADD_FUNCTIONS[unit], expressions=[base, count.copy()])

    def build_median(self, inner: Expression) -> Expression:
        """ClickHouse: ``quantile(0.5)(x)``."""
        return self.build_percentile(p=exp.Literal.number("0.5"), col_expr=inner)

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """ClickHouse: parametric ``quantile(p)(x)`` syntax."""
        return exp.Quantile(this=col_expr.copy(), quantile=p.copy())
