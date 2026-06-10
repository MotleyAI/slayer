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

from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import quote

import sqlalchemy as sa

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


def _is_connection_name_sentinel(connection_string: str) -> bool:
    """True iff ``connection_string`` is the
    ``snowflake://?connection_name=<name>`` sentinel.

    Cross-source-of-truth: the user can land on this URL either via
    ``DatasourceConfig.connection_name`` (typed field) or by typing the
    URL into the ``connection_string`` field (e.g. the CLI form
    ``slayer datasources create snowflake://?connection_name=default``).
    Both paths must route through ``creator=``.
    """
    if not connection_string.startswith(_CONNECTION_NAME_PREFIX):
        return False
    # Reject empty connection_name values up front so the failure surfaces
    # at engine-build time, not deep inside the connector.
    return bool(connection_string[len(_CONNECTION_NAME_PREFIX):].strip("&"))


def _extract_connection_name(connection_string: str) -> str:
    """Parse + URL-decode the ``connection_name=`` value from the sentinel URL."""
    import sqlalchemy.engine.url as sa_url  # noqa: PLC0415
    try:
        url = sa_url.make_url(connection_string)
    except Exception as exc:
        raise ValueError(
            f"Could not parse Snowflake sentinel URL: {connection_string!r}"
        ) from exc
    name = url.query.get("connection_name")
    if not name:
        raise ValueError(
            f"Snowflake URL is missing the 'connection_name' query parameter: "
            f"{connection_string!r}"
        )
    from urllib.parse import unquote  # noqa: PLC0415
    return unquote(name)


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
        return URL(**kwargs)

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
        cur = dbapi_connection.cursor()
        try:
            if datasource.warehouse:
                cur.execute(f"USE WAREHOUSE {datasource.warehouse}")
            if datasource.role:
                cur.execute(f"USE ROLE {datasource.role}")
            if datasource.database:
                cur.execute(f"USE DATABASE {datasource.database}")
            if datasource.schema_name:
                cur.execute(f"USE SCHEMA {datasource.schema_name}")
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
