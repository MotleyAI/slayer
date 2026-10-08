"""TrinoDialect over the Presto-family grammar it shares with Presto / Athena."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy as sa
from sqlalchemy.engine.url import URL, make_url
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.sql.dialects.base import SqlDialect

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


class PrestoFamilyDialect(SqlDialect):
    """Grammar shared by Trino, Presto and Athena."""

    sqlglot_name: str = "presto"
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = None  # unbounded
    approx_count_distinct_native: bool = True

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
        """``UNNEST(SEQUENCE(..))``; past ``SEQUENCE``'s entry cap, a filtered product of two."""
        if size <= _SEQUENCE_MAX:
            return exp.select(exp.column("i")).from_(_unnest_sequence(size=size, name="_seq"))

        def value() -> Expression:
            return exp.Add(
                this=exp.Mul(this=exp.column("i", table="_hi"), expression=exp.Literal.number(_SEQUENCE_MAX)),
                expression=exp.column("i", table="_lo"),
            )

        return exp.select(value().as_("i")).from_(
            _unnest_sequence(size=-(-size // _SEQUENCE_MAX), name="_hi"),
        ).join(
            _unnest_sequence(size=_SEQUENCE_MAX, name="_lo"), join_type="cross",
        ).where(exp.LT(this=value(), expression=exp.Literal.number(size)))


class TrinoDialect(PrestoFamilyDialect):
    sqlglot_name: str = "trino"
    ds_type_aliases: frozenset[str] = frozenset({"trino"})
    url_scheme: str | None = "trino"
    install_extra: str | None = "trino"

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
