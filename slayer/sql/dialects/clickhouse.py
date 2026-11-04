"""ClickhouseDialect.

ClickHouse uses native ``median(x)`` directly and the parametric
``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP / COVAR_POP
are native. log10 and log2 are native.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from datetime import date
from typing import Any, ClassVar, Optional

import sqlglot

from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.sql.dialects._clickhouse_cells import array_reader, read_bool
from slayer.sql.dialects.base import (
    LIST_ELEMENT, LIST_VALUE, ListRelation, ServerProfile, SqlDialect, anon, element_lambda, is_not_null, iso_text,
)
from slayer.sql.dialects.reflection import TableLocation, all_of, call, col, eq_literal, relation

_CH_CORRELATED_SETTING = "allow_experimental_correlated_subqueries"
_CORRELATED_MIN_VERSION = (25, 4)
_CORRELATED_DEFAULT_ON_VERSION = (25, 8)
_TIMEOUT_SETTING = "max_execution_time"
_ABSENT = object()
_BOOL_SETTING_VALUES = {"1": True, "true": True, "0": False, "false": False}
_BOOL_TYPES = frozenset({"Bool", "Nullable(Bool)"})


def _compact(values: Expression) -> Expression:
    """``arrayFilter(e -> e IS NOT NULL, values)`` (the lambda comes first here)."""
    element = exp.to_identifier(LIST_ELEMENT)
    keep = exp.Lambda(this=is_not_null(exp.Column(this=element.copy())), expressions=[element])
    return exp.Anonymous(this="arrayFilter", expressions=[keep, values.copy()])


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


def _exists_beside_array_join(sql: str) -> bool:
    """Whether a SELECT of ``sql`` both ``ARRAY JOIN``s and filters by ``EXISTS`` (ClickHouse < 25.8 cannot plan it)."""
    if "ARRAY JOIN" not in sql.upper():
        return False
    return any(
        any(j.args.get("kind") == "ARRAY" for j in select.args.get("joins") or [])
        and select.args.get("where") is not None and select.args["where"].find(exp.Exists) is not None
        for select in sqlglot.parse_one(sql, read="clickhouse").find_all(exp.Select)
    )


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
    ds_type_aliases: frozenset[str] = frozenset({"clickhouse"})
    sa_backend_names: frozenset[str] = frozenset({"clickhouse"})
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True
    correlated_subqueries_gated: bool = True
    global_in_subqueries: bool = True
    url_scheme: str | None = "clickhouse+http"
    install_extra: str | None = "clickhouse"
    database_name: ClassVar[str] = "ClickHouse"
    native_lists: ClassVar[bool] = True

    def list_length(self, values: Expression) -> Expression:
        return anon("length", _compact(values))

    def list_contains(self, values: Expression, value: Expression) -> Expression:
        return anon("has", _compact(values), value)

    def list_overlaps(self, values: Expression, other: Expression) -> Expression:
        return anon("hasAny", _compact(values), _compact(other))

    def list_contains_all(self, values: Expression, other: Expression) -> Expression:
        return anon("hasAll", values, _compact(other))

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

    def column_comments_query(self, location: TableLocation) -> exp.Select:
        database = exp.Literal.string(location.schema_name) if location.schema_name else call("currentDatabase")
        return (
            exp.select(col("name"), col("comment"))
            .from_(relation("columns", db="system"))
            .where(all_of([eq_literal(col("table"), location.name), exp.EQ(this=col("database"), expression=database)]))
        )

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

    def correlated_subquery_refusal(self, profile: ServerProfile, *, sql: str = "") -> str | None:
        version = profile.version
        if version is None or version < _CORRELATED_MIN_VERSION:
            detected = (
                "the server version could not be determined." if version is None
                else f"detected {version[0]}.{version[1]}. Upgrade the server."
            )
            return f"ClickHouse supports correlated subqueries only from server 25.4, but {detected}"
        if version < _CORRELATED_DEFAULT_ON_VERSION and _exists_beside_array_join(sql):
            return (
                "ClickHouse runs a correlated subquery beside a list's ARRAY JOIN only from server 25.8, "
                f"but detected {version[0]}.{version[1]}. Upgrade the server."
            )
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
