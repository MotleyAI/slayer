"""Shared SQLAlchemy engine factory (DEV-1551).

Single source of truth for building ``sa.Engine`` instances from a
``DatasourceConfig``. Every production code path that creates engines —
ingestion, schema_drift, type_refinement, the CLI's datasources-test
command, the MCP server's connectivity probes, and ``SlayerSQLClient`` —
funnels through ``get_engine(datasource)``.

The factory itself is dialect-agnostic. Each dialect's ``SqlDialect``
strategy class carries its own runtime hooks
(``build_engine``, ``apply_session_overrides``) under
``slayer/sql/dialects/<name>.py``; this module just calls them and falls
back to a vanilla ``sa.create_engine`` when the dialect declines to
customise.

Engine caching is keyed on ``DatasourceConfig.get_connection_string()``
plus a fingerprint of the dialect-relevant runtime fields, so two
datasources that differ only in (e.g.) warehouse get different cached
engines.
"""

from __future__ import annotations

import logging
from typing import Dict, Tuple

import sqlalchemy as sa
import sqlalchemy.event as sa_event

from slayer.core.models import DatasourceConfig
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import SqlDialect

logger = logging.getLogger(__name__)


# Engine cache. Key = (connection_string, runtime_fingerprint).
_engine_cache: Dict[Tuple[str, str], sa.Engine] = {}


def _runtime_fingerprint(datasource: DatasourceConfig) -> str:
    """Stable fingerprint of dialect-relevant runtime fields for the
    cache key. Two datasources differing only in (e.g.) warehouse or
    role must NOT share a cached engine — the session-overrides listener
    would otherwise apply the wrong USE statements.

    Currently only Snowflake uses any of these fields; for other
    dialects the fingerprint collapses to an empty string and the
    cache key reduces to the connection_string alone.
    """
    if datasource.type != "snowflake":
        return ""
    parts = (
        ("wh", datasource.warehouse or ""),
        ("rl", datasource.role or ""),
        ("db", datasource.database or ""),
        ("sc", datasource.schema_name or ""),
    )
    return "|".join(f"{k}={v}" for k, v in parts)


def _attach_session_overrides_listener(
    *,
    engine: sa.Engine,
    datasource: DatasourceConfig,
) -> None:
    """Register a ``checkout`` event listener that calls the dialect's
    ``apply_session_overrides`` hook every time a connection is taken
    from the pool.

    The ``checkout`` event is used (not ``connect``) so the session
    state is re-applied on every query — not just on the first physical
    connection creation. Without this, anything that mutates Snowflake
    session state mid-flight (an inspector probe issuing its own ``USE``,
    or a user-issued ``client.execute("USE SCHEMA other")``) would
    silently persist on the pooled connection and leak into the next
    query. Cost: ~1-4 ``USE`` round-trips per query, dominated by
    network latency to Snowflake. Acceptable trade-off for correctness.

    The listener's name is ``_slayer_session_overrides`` so tests can
    verify registration without coupling to a private API.

    Skipped when the dialect's hook is the base-class no-op; detection
    is by class identity so the no-op default doesn't trigger a
    ``checkout`` listener that does nothing.
    """
    dialect = dialect_for_ds_type(datasource.type)
    base_method = SqlDialect.apply_session_overrides
    dialect_method = type(dialect).apply_session_overrides
    if dialect_method is base_method:
        return

    @sa_event.listens_for(engine, "checkout")
    def _slayer_session_overrides(dbapi_connection, _connection_record, _connection_proxy):
        dialect.apply_session_overrides(
            dbapi_connection=dbapi_connection,
            datasource=datasource,
        )


def _attach_register_udfs_listener(
    *,
    engine: sa.Engine,
    datasource: DatasourceConfig,
) -> None:
    """Register a ``connect`` event listener that calls the dialect's
    ``register_udfs`` hook on every new pooled connection.

    Skipped when the dialect's hook is the base-class no-op (every
    dialect except SQLite). SQLite needs this to register the median /
    percentile_cont / stddev / corr / log10 / log2 / ... UDFs without
    which generated SQL like ``STDDEV_SAMP(x)`` fails with
    ``sqlite3.OperationalError: no such function``.
    """
    dialect = dialect_for_ds_type(datasource.type)
    base_method = SqlDialect.register_udfs
    dialect_method = type(dialect).register_udfs
    if dialect_method is base_method:
        return

    @sa_event.listens_for(engine, "connect")
    def _slayer_register_udfs(dbapi_connection, _connection_record):
        dialect.register_udfs(dbapi_connection)


def _build_engine(*, datasource: DatasourceConfig, connection_string: str) -> sa.Engine:
    """Construct a new SA engine for the datasource without consulting
    the cache. Delegates engine-build to the dialect's ``build_engine``
    hook; falls back to vanilla ``sa.create_engine`` when the dialect
    declines (returns ``None``).
    """
    dialect = dialect_for_ds_type(datasource.type)
    engine = dialect.build_engine(datasource, connection_string=connection_string)
    if engine is None:
        engine = sa.create_engine(connection_string, pool_pre_ping=True)
    _attach_register_udfs_listener(engine=engine, datasource=datasource)
    _attach_session_overrides_listener(engine=engine, datasource=datasource)
    return engine


def get_engine(datasource: DatasourceConfig) -> sa.Engine:
    """Return a cached ``sa.Engine`` for the given datasource. Builds one
    if the cache misses.

    The cache key includes a fingerprint of dialect runtime fields so
    that two datasources differing in (e.g.) warehouse get different
    cached engines — otherwise the connect listener would silently
    apply the wrong USE statements.
    """
    connection_string = datasource.get_connection_string()
    cache_key = (connection_string, _runtime_fingerprint(datasource))
    if cache_key not in _engine_cache:
        _engine_cache[cache_key] = _build_engine(
            datasource=datasource, connection_string=connection_string,
        )
    return _engine_cache[cache_key]


def reset_cache() -> None:
    """Discard every cached engine. Used by tests that need fresh pools;
    not called by production code."""
    _engine_cache.clear()
