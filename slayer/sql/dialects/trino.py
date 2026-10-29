"""TrinoDialect over the Presto-family grammar it shares with Presto / Athena."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Optional

import sqlalchemy as sa
from sqlalchemy.engine.url import URL, make_url
from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.json_path import JsonPath, json_path_text
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    PORTABLE_LIBRARY,
    NativeFn, ListRelation, SqlDialect, TrimFn, anon, any_null, case, element_lambda, is_not_null, lit,
    non_null_elements, text_try_cast, unnest_elements,
)
from slayer.sql.dialects.patterns import JAVA, RegexFlavour

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig


_TIMEOUT_PROPERTY = "query_max_run_time"
_ABSENT = object()
_HTTP_SCHEME = "http_scheme"
_HTTP_SCHEMES = frozenset({"http", "https"})
_SEQUENCE_MAX = 10_000  # entries one SEQUENCE call may return


def _unnest_sequence(*, size: int, name: str) -> exp.Unnest:
    """``UNNEST(SEQUENCE(0, size - 1)) AS name(i)``."""
    return exp.Unnest(
        expressions=[exp.Anonymous(this="SEQUENCE", expressions=[
            exp.Literal.number(0), exp.Literal.number(size - 1),
        ])],
        alias=exp.TableAlias(this=exp.to_identifier(name), columns=[exp.to_identifier("i")]),
    )


def _carries_credentials(url: URL) -> bool:
    """Whether the driver would configure authentication for ``url`` (its own predicates)."""
    query = url.query
    return bool(
        url.password
        or "access_token" in query
        or ("cert" in query and "key" in query)
        or "externalAuthentication" in query
    )


def _presto_path(path: JsonPath) -> str:
    """The path in Presto-family JSONPath: a key that is not a plain identifier goes in ``["…"]``."""
    out = ["$"]
    for step in path.steps:
        if isinstance(step, int):
            out.append(f"[{step}]")
        elif json_path_text((step,)) == f"$.{step}":
            out.append(f".{step}")
        else:
            out.append(f'["{step}"]')
    return "".join(out)


class PrestoFamilyDialect(SqlDialect):
    """Grammar and portable library shared by Trino, Presto and Athena."""

    sqlglot_name: str = "presto"
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True
    native_lists: ClassVar[bool] = True
    regex_flavour: ClassVar[Optional[RegexFlavour]] = JAVA
    native_functions: ClassVar[frozenset[NativeFn]] = frozenset({
        "LPAD", "RPAD", "REVERSE", "CONCAT_WS", "CHR", "PI", "CBRT", "GREATEST", "LEAST",
    })
    extremum_propagates_null: ClassVar[bool] = True

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        return text_try_cast(value, to)

    def text_repeat(self, value: Expression, count: Expression) -> Expression:
        """``repeat`` builds an array here."""
        return anon("ARRAY_JOIN", anon("REPEAT", value, count), lit(""))

    def first_code_point(self, s: Expression) -> Expression:
        first = exp.Cast(this=anon("SUBSTR", s, lit(1), lit(1)), to=exp.DataType.build("VARCHAR(1)"))
        return case((exp.EQ(this=s.copy(), expression=lit("")), lit(0)), default=anon("CODEPOINT", first))

    def trim_spaces(self, fn: TrimFn, s: Expression) -> Expression:
        """The bare form strips every whitespace character."""
        return anon(fn.upper(), s, lit(" "))

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        """``element_at`` indexes from either end and is NULL past it."""
        field = exp.Coalesce(this=anon("ELEMENT_AT", anon("SPLIT", value, delimiter), lit(index)),
                             expressions=[lit("")])
        return case((any_null(value, delimiter), exp.Null()), default=field)

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        return anon("REGEXP_LIKE", value, self.regex_text(pattern, case_insensitive=case_insensitive))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        extracted = anon("REGEXP_EXTRACT", value, self.regex_text(pattern), lit(group))
        return self.skipped_group_empty(value, pattern, group, extracted)

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern, case_insensitive=case_insensitive),
                    self.replacement_text(replacement))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        return anon("JSON_EXTRACT_SCALAR", value, lit(_presto_path(path)))

    def list_length(self, values: Expression) -> Expression:
        return anon("CARDINALITY", non_null_elements(values))

    def list_relation(
        self, values: Expression, *, alias: exp.Identifier, element: Optional[DataType], leading: bool,
        distinct: bool = False,
    ) -> ListRelation:
        kept = non_null_elements(values)
        return ListRelation(relation=unnest_elements(anon("ARRAY_DISTINCT", kept) if distinct else kept, alias=alias))

    def list_contains(self, values: Expression, value: Expression) -> Expression:
        return anon("CONTAINS", non_null_elements(values), value)

    def list_exists(
        self, values: Expression, *, alias: exp.Identifier, element: Optional[DataType], predicate: Expr,
    ) -> Expression:
        """``any_match(values, e -> predicate(e))``: Trino rejects a correlated subquery over ``UNNEST``."""
        matched = anon("any_match", non_null_elements(values), element_lambda(predicate, alias=alias))
        return exp.Coalesce(this=matched, expressions=[exp.false()])

    def list_overlaps(self, values: Expression, other: Expression) -> Expression:
        return anon("ARRAYS_OVERLAP", non_null_elements(values), non_null_elements(other))

    def list_contains_all(self, values: Expression, other: Expression) -> Expression:
        missing = anon("ARRAY_EXCEPT", non_null_elements(other), values)
        return exp.EQ(this=anon("CARDINALITY", missing), expression=lit(0))

    def build_collect(self, value: Expression, element: Optional[DataType]) -> Expression:
        """The untyped ``ARRAY[]`` fallback takes the aggregate's own array type."""
        kept = exp.Filter(this=exp.ArrayAgg(this=exp.Distinct(expressions=[value.copy()])),
                          expression=exp.Where(this=is_not_null(value)))
        return exp.Coalesce(this=anon("ARRAY_SORT", kept), expressions=[exp.Array(expressions=[])])

    _EXTRACT_FIELDS: ClassVar[dict[DatePart, str]] = {
        **SqlDialect._EXTRACT_FIELDS,
        DatePart.ISO_YEAR: "YEAR_OF_WEEK", DatePart.DAY_OF_WEEK: "DAY_OF_WEEK",
        DatePart.DAY_OF_YEAR: "DAY_OF_YEAR",
    }

    def _interval(self, amount: int, unit: exp.Var) -> exp.Interval:
        """The count is a quoted string: ``INTERVAL '1' DAY``."""
        return exp.Interval(this=exp.Literal.string(str(amount)), unit=unit)

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """A DATE takes no sub-day interval, so it moves as a TIMESTAMP."""
        if operand is DataType.DATE and unit in SUB_DAY_GRANULARITIES:
            expr, operand = self.promote_to_timestamp(expr), DataType.TIMESTAMP
        return super().build_date_add(expr=expr, count=count, unit=unit, operand=operand)

    def _day_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.DateDiff(
            this=exp.Cast(this=end, to=exp.DataType.build("DATE")),
            expression=exp.Cast(this=start, to=exp.DataType.build("DATE")),
            unit=exp.var("DAY"),
        )

    def _second_gap(self, *, start: Expression, end: Expression) -> Expression:
        return exp.DateDiff(this=end, expression=start, unit=exp.var("SECOND"))

    def _exact_div(self, value: Expression, divisor: int) -> Expression:
        return self.natural_div(exp.Paren(this=value), divisor=exp.Literal.number(divisor))

    def natural_div(self, value: Expression, *, divisor: Expression) -> Expression:
        """Integer ``/`` truncates; sqlglot's ``IntDiv`` would round through DOUBLE."""
        return exp.Div(this=value, expression=divisor, typed=True)

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        """``UNNEST(SEQUENCE(..))``; past ``SEQUENCE``'s entry cap, a filtered product of base-cap digits."""
        if size <= _SEQUENCE_MAX:
            return exp.select(exp.column("i")).from_(_unnest_sequence(size=size, name="_seq"))
        places = [1]
        while places[-1] * _SEQUENCE_MAX < size:
            places.append(places[-1] * _SEQUENCE_MAX)
        places.reverse()  # most significant digit first
        names = ["_hi", *(f"_d{j}" for j in range(len(places) - 2, 0, -1)), "_lo"]

        def value() -> Expression:
            total: Expression = exp.Mul(this=exp.column("i", table="_hi"), expression=exp.Literal.number(places[0]))
            for name, place in zip(names[1:], places[1:]):
                digit = exp.column("i", table=name)
                total = exp.Add(
                    this=total, expression=exp.Mul(this=digit, expression=exp.Literal.number(place)) if place > 1 else digit,
                )
            return total

        select = exp.select(value().as_("i")).from_(_unnest_sequence(size=-(-size // places[0]), name="_hi"))
        for name in names[1:]:
            select = select.join(_unnest_sequence(size=_SEQUENCE_MAX, name=name), join_type="cross")
        return select.where(exp.LT(this=value(), expression=exp.Literal.number(size)))


class TrinoDialect(PrestoFamilyDialect):
    sqlglot_name: str = "trino"
    ds_type_aliases: frozenset[str] = frozenset({"trino"})
    sa_backend_names: frozenset[str] = frozenset({"trino"})
    url_scheme: str | None = "trino"
    install_extra: str | None = "trino"
    database_name: ClassVar[str] = "Trino"
    library_functions: ClassVar[frozenset[str]] = PORTABLE_LIBRARY

    def set_connection_timeout(self, dbapi_connection: Any, timeout_seconds: int) -> object:
        """``query_max_run_time`` as a client-session property: sent with the next statement only."""
        properties = dbapi_connection._client_session.properties
        prior = properties.get(_TIMEOUT_PROPERTY, _ABSENT)
        properties[_TIMEOUT_PROPERTY] = f"{timeout_seconds}s"
        return prior

    def restore_connection_timeout(self, dbapi_connection: Any, prior: object) -> None:
        properties = dbapi_connection._client_session.properties
        if prior is _ABSENT:
            properties.pop(_TIMEOUT_PROPERTY, None)
        else:
            properties[_TIMEOUT_PROPERTY] = prior

    def build_engine(
        self,
        datasource: DatasourceConfig,
        *,
        connection_string: str,
    ) -> sa.Engine | None:
        """HTTPS when the URL carries credentials; ``?http_scheme=`` overrides."""
        url = make_url(connection_string)
        scheme = url.query.get(_HTTP_SCHEME)
        if scheme is None:
            if not _carries_credentials(url):
                return None
            scheme = "https"
        elif scheme not in _HTTP_SCHEMES:
            raise SlayerError(
                f"Datasource '{datasource.name}': http_scheme must be 'http' or 'https', got {scheme!r}."
            )
        return sa.create_engine(
            url.difference_update_query([_HTTP_SCHEME]),
            pool_pre_ping=True,
            connect_args={_HTTP_SCHEME: scheme},
        )
