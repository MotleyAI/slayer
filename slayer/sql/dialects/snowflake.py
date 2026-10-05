"""SnowflakeDialect: connection-name auth, session overrides, statement timeout, cursor types."""

from __future__ import annotations

import re as _re
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote

import sqlalchemy as sa
import sqlalchemy.engine.url as _sa_url

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.drivers import import_driver

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig


# ``snowflake.connector.constants.FieldType`` codes → SLayer category.
_SNOWFLAKE_TYPE_MAP: dict[int, str] = {
    0: "number",   # FIXED (NUMBER / INT / DECIMAL)
    1: "number",   # REAL (FLOAT / DOUBLE)
    2: "string",   # TEXT (VARCHAR / STRING)
    3: "time",     # DATE
    4: "time",     # TIMESTAMP (legacy alias)
    5: "string",   # VARIANT (semi-structured JSON / object)
    6: "time",     # TIMESTAMP_LTZ
    7: "time",     # TIMESTAMP_TZ
    8: "time",     # TIMESTAMP_NTZ
    9: "string",   # OBJECT
    10: "string",  # ARRAY
    11: "string",  # BINARY
    12: "time",    # TIME
    13: "boolean", # BOOLEAN
}


# snowflake-sqlalchemy has no ``connection_name=`` knob, so this sentinel routes via ``creator=``.
_CONNECTION_NAME_PREFIX = "snowflake://?connection_name="

_CONNECTOR_MODULE = "snowflake.connector"


_SAFE_SNOWFLAKE_IDENT = _re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _validate_unquoted_identifier(*, field: str, value: str) -> str:
    """Reject values unsafe to emit unquoted in ``USE ...`` statements.

    Unquoted so Snowflake case-folding applies (``compute_wh`` matches ``COMPUTE_WH``).
    """
    if not _SAFE_SNOWFLAKE_IDENT.match(value):
        raise ValueError(
            f"Invalid Snowflake identifier for DatasourceConfig.{field}: "
            f"{value!r}. Only letters, digits, underscores, and '$' are allowed; "
            f"the first character must be a letter or underscore. (If you have "
            f"a quoted/mixed-case Snowflake object, create the datasource with "
            f"the uppercase or canonical form.)"
        )
    return value


def _is_connection_name_sentinel(connection_string: str) -> bool:
    """True iff the URL is the sentinel with ``connection_name`` as its only non-empty param."""
    if not connection_string.startswith("snowflake://"):
        return False
    try:
        url = _sa_url.make_url(connection_string)
    except sa.exc.ArgumentError:
        return False
    name = url.query.get("connection_name")
    if not name:
        return False
    extra = {k for k, v in url.query.items() if k != "connection_name" and v != ""}
    return not extra


def _extract_connection_name(connection_string: str) -> str:
    """Parse ``connection_name=`` from the sentinel URL (``make_url`` already URL-decodes it)."""
    try:
        url = _sa_url.make_url(connection_string)
    except sa.exc.ArgumentError as exc:
        raise ValueError(
            f"Could not parse Snowflake sentinel URL: {connection_string!r}"
        ) from exc
    name = url.query.get("connection_name")
    if not name:
        raise ValueError(
            f"Snowflake URL is missing the 'connection_name' query parameter: "
            f"{connection_string!r}"
        )
    return name


class SnowflakeDialect(SqlDialect):
    """Snowflake dialect."""

    sqlglot_name: str = "snowflake"
    ds_type_aliases: frozenset[str] = frozenset({"snowflake"})
    explain_prefix: str | None = "EXPLAIN USING JSON"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False
    max_identifier_bytes: int | None = 255
    approx_count_distinct_native: bool = True
    url_scheme: str | None = "snowflake"
    install_extra: str | None = "snowflake"

    _EXTRACT_FIELDS: ClassVar[dict[DatePart, str]] = {
        **SqlDialect._EXTRACT_FIELDS,
        DatePart.ISO_YEAR: "YEAROFWEEKISO", DatePart.WEEK: "WEEKISO",
        DatePart.DAY_OF_WEEK: "DAYOFWEEKISO", DatePart.DAY_OF_YEAR: "DAYOFYEAR",
    }

    def _second_gap(self, *, start: Expression, end: Expression) -> Expression:
        """No interval type: ``DATEDIFF`` of second-aligned timestamps is exact."""
        return exp.Anonymous(this="DATEDIFF", expressions=[exp.var("SECOND"), start, end])

    def natural_div(self, value: Expression, *, divisor: Expression) -> Expression:
        """``FLOOR``: an integer ``CAST`` rounds here."""
        return exp.Floor(this=exp.Div(this=value, expression=divisor))

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        generator = exp.Anonymous(this="GENERATOR", expressions=[
            exp.Kwarg(this=exp.var("ROWCOUNT"), expression=exp.Literal.number(size)),
        ])
        row = exp.Window(
            this=exp.RowNumber(),
            order=exp.Order(expressions=[exp.Ordered(this=exp.Anonymous(this="SEQ4", expressions=[]))]),
        )
        return exp.select(exp.Sub(this=row, expression=exp.Literal.number(1)).as_("i")).from_(
            exp.Table(this=exp.Anonymous(this="TABLE", expressions=[generator])),
        )

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATEADD`` clamps at month-end, keeps a DATE a DATE for day-or-coarser units."""
        word = "WEEK" if unit is TimeGranularity.WEEK_SUNDAY else unit.value.upper()
        return exp.Anonymous(this="DATEADD", expressions=[exp.var(word), count.copy(), expr.copy()])

    def build_connection_url(
        self,
        datasource: DatasourceConfig,
    ) -> str | None:
        """Sentinel URL when ``connection_name`` is set, else a snowflake-sqlalchemy URL."""
        if datasource.connection_name:
            return f"{_CONNECTION_NAME_PREFIX}{quote(datasource.connection_name, safe='')}"
        if not datasource.host:
            raise ValueError(
                "Snowflake DatasourceConfig requires either 'connection_name' "
                "(profile from ~/.snowflake/connections.toml) or inline credentials. "
                "Set 'host' to the Snowflake account identifier (e.g. 'jp13593' or "
                "'xy12345.us-east-1'), plus username/password — and optionally "
                "database/schema_name/warehouse/role."
            )
        URL = import_driver("snowflake.sqlalchemy", datasource=datasource, dialect=self).URL
        kwargs: dict[str, str] = {"account": datasource.host}
        if datasource.username:
            kwargs["user"] = datasource.username
        if datasource.password:
            kwargs["password"] = datasource.password
        if datasource.database:
            kwargs["database"] = datasource.database
        if datasource.schema_name:
            kwargs["schema"] = datasource.schema_name
        if datasource.warehouse:
            kwargs["warehouse"] = datasource.warehouse
        if datasource.role:
            kwargs["role"] = datasource.role
        return str(URL(**kwargs))

    def build_engine(
        self,
        datasource: DatasourceConfig,
        *,
        connection_string: str,
    ) -> sa.Engine | None:
        """Route the sentinel URL through ``creator=``; None for the default engine path."""
        if connection_string.startswith("snowflake://"):
            try:
                parsed = _sa_url.make_url(connection_string)
            except sa.exc.ArgumentError:
                parsed = None
            if parsed is not None and parsed.query.get("connection_name"):
                extras = {
                    k for k, v in parsed.query.items()
                    if k != "connection_name" and v
                }
                if extras:
                    raise ValueError(
                        f"Snowflake sentinel URL must contain only the "
                        f"``connection_name`` query parameter — extra params "
                        f"{sorted(extras)!r} would be silently dropped by the "
                        f"snowflake-connector bridge. Set ``warehouse`` / "
                        f"``role`` / ``database`` / ``schema_name`` on the "
                        f"typed ``DatasourceConfig`` fields instead — those "
                        f"fire via ``apply_session_overrides`` on every "
                        f"pool checkout."
                    )
        if not _is_connection_name_sentinel(connection_string):
            return None
        name = _extract_connection_name(connection_string)

        def _create_snowflake_connection():
            sf = import_driver(_CONNECTOR_MODULE, datasource=datasource, dialect=self)
            return sf.connect(connection_name=name)

        return sa.create_engine(
            "snowflake://",
            creator=_create_snowflake_connection,
            pool_pre_ping=True,
        )

    def deferred_driver_modules(self, connection_string: str) -> tuple[str, ...]:
        """The sentinel's connector is imported only by the connect-time ``creator``."""
        return (_CONNECTOR_MODULE,) if _is_connection_name_sentinel(connection_string) else ()

    def apply_session_overrides(
        self,
        dbapi_connection: Any,
        datasource: DatasourceConfig,
    ) -> None:
        """Issue ``USE ROLE / WAREHOUSE / DATABASE / SCHEMA`` on a fresh DBAPI connection."""
        if not any((
            datasource.warehouse,
            datasource.role,
            datasource.database,
            datasource.schema_name,
        )):
            return
        warehouse = (
            _validate_unquoted_identifier(field="warehouse", value=datasource.warehouse)
            if datasource.warehouse else None
        )
        role = (
            _validate_unquoted_identifier(field="role", value=datasource.role)
            if datasource.role else None
        )
        database = (
            _validate_unquoted_identifier(field="database", value=datasource.database)
            if datasource.database else None
        )
        schema_name = (
            _validate_unquoted_identifier(field="schema_name", value=datasource.schema_name)
            if datasource.schema_name else None
        )
        cur = dbapi_connection.cursor()
        try:
            # Role first: it gates warehouse access; database before schema for bare names.
            if role:
                cur.execute(f"USE ROLE {role}")
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            if database:
                cur.execute(f"USE DATABASE {database}")
            if schema_name:
                cur.execute(f"USE SCHEMA {schema_name}")
        finally:
            cur.close()

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        """Session-wide ``STATEMENT_TIMEOUT_IN_SECONDS``."""
        return f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout_seconds}"

    def map_cursor_type_code(self, type_code: int) -> str | None:
        """Map a connector ``FieldType`` code to a SLayer category; None if unknown."""
        return _SNOWFLAKE_TYPE_MAP.get(type_code)
