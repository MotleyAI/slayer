"""DEV-1551: SnowflakeDialect — Tier 1 promotion.

Promoted from ``_tier2.py`` to its own file because Snowflake now carries
runtime quirks beyond the data-shaped Tier-2 set:

* Connection-name URL form (``snowflake://?connection_name=<name>``) +
  ``sa.create_engine(..., creator=...)`` bridge to delegate to
  ``snowflake.connector.connect(connection_name=...)`` for TOML-driven auth.
* Per-connection session overrides (``USE WAREHOUSE / USE ROLE /
  USE DATABASE / USE SCHEMA``) from the typed ``DatasourceConfig`` fields.
* Per-statement timeout via ``ALTER SESSION SET
  STATEMENT_TIMEOUT_IN_SECONDS``.
* Cursor type-code mapping for ``snowflake-connector-python``'s
  ``FieldType`` enum (FIXED/REAL/TEXT/DATE/TIMESTAMP/VARIANT/...).

SQL generation quirks (DATE_TRUNC, DATEADD, MEDIAN, PERCENTILE_CONT,
STDDEV_*, VAR_*, CORR, COVAR_*) all match the Postgres-shaped base —
sqlglot's snowflake dialect transpiles correctly. The only divergence
from the base is ``log2_native=False`` (Snowflake has no native LOG2).
"""

from __future__ import annotations

import re as _re
from typing import TYPE_CHECKING, Any, Callable, Optional
from urllib.parse import quote

import sqlalchemy as sa
import sqlalchemy.engine.url as _sa_url
from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig


# snowflake-connector-python's ``FieldType`` integer codes → SLayer
# category. Codes from ``snowflake.connector.constants.FieldType``;
# kept in sync with the consumer-facing categories used by
# ``slayer.sql.client._map_type_code``.
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


# Sentinel-URL prefix used by ``build_connection_url`` when
# ``DatasourceConfig.connection_name`` is set. ``engine_factory``
# bridges this URL to ``snowflake.connector.connect(connection_name=...)``
# via ``sa.create_engine(..., creator=...)`` because snowflake-sqlalchemy
# has no ``connection_name=`` URL knob.
_CONNECTION_NAME_PREFIX = "snowflake://?connection_name="


def _import_snowflake_connector():
    """Lazy import with an actionable install hint."""
    try:
        import snowflake.connector  # noqa: PLC0415
        return snowflake.connector
    except ImportError as exc:
        raise ImportError(
            "Snowflake support requires the 'snowflake' extra: "
            "pip install 'motley-slayer[snowflake]'"
        ) from exc


def _import_snowflake_sqlalchemy_url():
    """Lazy import for the inline-URL form. Same install hint."""
    try:
        from snowflake.sqlalchemy import URL  # noqa: PLC0415
        return URL
    except ImportError as exc:
        raise ImportError(
            "Snowflake support requires the 'snowflake' extra: "
            "pip install 'motley-slayer[snowflake]'"
        ) from exc


# Snowflake identifier characters allowed unquoted: letters, digits,
# underscores, dollar signs. Anything else (whitespace, semicolons,
# quotes, parentheses) is rejected up front rather than emitted into a
# ``USE WAREHOUSE/ROLE/DATABASE/SCHEMA`` statement.
_SAFE_SNOWFLAKE_IDENT = _re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _validate_unquoted_identifier(*, field: str, value: str) -> str:
    """Reject Snowflake identifier values that aren't safe to emit unquoted.

    The values that flow into ``USE WAREHOUSE / ROLE / DATABASE / SCHEMA``
    statements come from typed ``DatasourceConfig`` fields. We deliberately
    emit them **unquoted** so Snowflake's case-folding rules apply (the
    common ``warehouse: compute_wh`` config matches the uppercase storage
    ``COMPUTE_WH``); always-quoting would silently break those configs.
    To keep that path safe we reject any character that could change the
    statement's meaning (whitespace, semicolons, quotes, parens, dots).
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
    """True iff ``connection_string`` is the
    ``snowflake://?connection_name=<name>`` sentinel.

    Cross-source-of-truth: the user can land on this URL either via
    ``DatasourceConfig.connection_name`` (typed field) or by typing the
    URL into the ``connection_string`` field (e.g. the CLI form
    ``slayer datasources create snowflake://?connection_name=default``).
    Both paths must route through ``creator=``.

    The recognition is **strict**: the URL must contain exactly one
    non-empty query parameter, ``connection_name``. Extra params like
    ``warehouse=WH`` are rejected up front because ``build_engine`` only
    forwards ``connection_name`` to ``snowflake.connector.connect`` —
    silently accepting other params would route the user to the profile
    defaults instead of the requested session context. To override
    warehouse / role / database / schema, use the typed
    ``DatasourceConfig`` fields; those fire via ``apply_session_overrides``.
    """
    if not connection_string.startswith("snowflake://"):
        return False
    try:
        url = _sa_url.make_url(connection_string)
    except sa.exc.ArgumentError:
        return False
    name = url.query.get("connection_name")
    if not name:
        return False
    # Reject sentinel URLs with extra query params — they would silently
    # be ignored by build_engine's creator= bridge.
    extra = {k for k, v in url.query.items() if k != "connection_name" and v != ""}
    return not extra


def _extract_connection_name(connection_string: str) -> str:
    """Parse the ``connection_name=`` value from the sentinel URL.

    ``sa.engine.url.make_url`` already URL-decodes query-string values,
    so the value is returned as-is. (Calling ``unquote`` again would
    double-decode literal percent-encoded text such as ``%2F`` in
    profile names.)
    """
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
    """Snowflake dialect — Tier 1.

    Inherits Postgres-shaped SQL-generation defaults; sqlglot handles the
    DATE_TRUNC / DATEADD / native-aggregate transpilation. Runtime
    behavior (connection URL, engine creation, per-connection session
    overrides, statement timeout, cursor type map) is encoded on the
    class so ``engine_factory`` / ``client`` stay dialect-agnostic.
    """

    sqlglot_name: str = "snowflake"
    ds_type_aliases: frozenset[str] = frozenset({"snowflake"})
    explain_prefix: Optional[str] = "EXPLAIN USING JSON"
    explain_postfix: str = ""
    log10_native: bool = True
    # No native LOG2 — falls through to canonical ``LOG(2, x)`` form.
    log2_native: bool = False

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """Snowflake: native ``APPROX_COUNT_DISTINCT(x)`` aggregate."""
        return parse(f"APPROX_COUNT_DISTINCT({col_sql})")

    # ------------------------------------------------------------------
    # Connection URL / engine
    # ------------------------------------------------------------------

    def build_connection_url(
        self,
        datasource: "DatasourceConfig",
    ) -> Optional[str]:
        """Emit the sentinel URL when ``connection_name`` is set, otherwise
        build the full snowflake-sqlalchemy URL from inline fields.

        Inline form requires ``host`` (the Snowflake account identifier).
        ``warehouse`` and ``role`` populate the URL's query string.
        """
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
        URL = _import_snowflake_sqlalchemy_url()
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
        # ``snowflake.sqlalchemy.URL`` returns a ``URL`` object; cast to
        # ``str`` so the ``Optional[str]`` return-type annotation is honest.
        return str(URL(**kwargs))

    def build_engine(
        self,
        datasource: "DatasourceConfig",
        *,
        connection_string: str,
    ) -> Optional["sa.Engine"]:
        """When the sentinel URL is in play, route through ``creator=``
        so ``snowflake.connector.connect(connection_name=...)`` drives
        the auth path. Otherwise return None to let ``engine_factory``
        use the default ``sa.create_engine(connection_string)`` path
        (snowflake-sqlalchemy understands the inline URL form natively).
        """
        # Defence in depth: if the URL has the snowflake scheme AND a
        # ``connection_name=`` query param BUT extra params, the strict
        # sentinel check rejects it. Falling through to ``sa.create_engine``
        # would then either (a) trigger a confusing snowflake-sqlalchemy
        # parse error or (b) silently connect to the profile defaults.
        # Raise an actionable error pointing at the typed DatasourceConfig
        # fields instead.
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
            sf = _import_snowflake_connector()
            return sf.connect(connection_name=name)

        return sa.create_engine(
            "snowflake://",
            creator=_create_snowflake_connection,
            pool_pre_ping=True,
        )

    def apply_session_overrides(
        self,
        dbapi_connection: Any,
        datasource: "DatasourceConfig",
    ) -> None:
        """Issue ``USE WAREHOUSE / USE ROLE / USE DATABASE / USE SCHEMA``
        in order on a fresh DBAPI connection.

        Order matters:
          * USE WAREHOUSE first — some accounts require an active
            warehouse before USE SCHEMA can resolve.
          * USE ROLE second — the role can scope what databases/schemas
            are visible.
          * USE DATABASE before USE SCHEMA — bare schema names resolve
            against the current database.
        """
        if not any((
            datasource.warehouse,
            datasource.role,
            datasource.database,
            datasource.schema_name,
        )):
            return
        # Validate every value up front; reject anything that isn't a
        # safe-to-emit-unquoted Snowflake identifier (catches embedded
        # semicolons, quotes, whitespace).
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
            # Order: USE ROLE first — role determines warehouse / database
            # privileges. A role granted via ``DatasourceConfig.role`` that
            # has access to a warehouse the profile's default role doesn't
            # see would otherwise fail at USE WAREHOUSE.
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

    # ------------------------------------------------------------------
    # Runtime statement hooks
    # ------------------------------------------------------------------

    def statement_timeout_sql(self, timeout_seconds: int) -> Optional[str]:
        """``ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = N``.

        Per-session setting; takes effect for every subsequent statement
        on the same connection until the connection is closed or the
        setting is reset.
        """
        return f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout_seconds}"

    def map_cursor_type_code(self, type_code: int) -> Optional[str]:
        """Map a snowflake-connector ``FieldType`` integer code to a
        SLayer category. Returns ``None`` for unknown codes so the caller
        can fall through to a default rather than mis-classify."""
        return _SNOWFLAKE_TYPE_MAP.get(type_code)
