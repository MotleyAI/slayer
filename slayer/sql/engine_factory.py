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
import os
import threading
from collections import OrderedDict

import sqlalchemy as sa
import sqlalchemy.event as sa_event

from slayer.core.models import DatasourceConfig
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import SqlDialect, _digest

logger = logging.getLogger(__name__)


#: (connection_string, runtime_fingerprint, credential_fingerprint). Exported so
#: callers keying their own caches by engine identity don't hard-code the arity.
EngineCacheKey = tuple[str, str, str]

# LRU-ordered, bounded. The credential leg is a security boundary: where the
# secret isn't in the URL (BigQuery), two identities would otherwise share one
# engine. Including it also makes cardinality track users, hence the cap.
_engine_cache: "OrderedDict[EngineCacheKey, sa.Engine]" = OrderedDict()

# Engines are reached from worker threads as well as the event loop, so the
# lookup/move_to_end pair needs guarding: an interleaved invalidate raises
# KeyError, and simultaneous misses orphan a pool. Never held across engine
# construction or dispose() — both do I/O.
_cache_lock = threading.Lock()

#: Sized for "datasources x working set of active users". A miss costs one
#: engine build, so over-eviction hurts latency, not correctness.
DEFAULT_MAX_CACHED_ENGINES = 64

#: Env override for :data:`DEFAULT_MAX_CACHED_ENGINES`. ``0`` disables caching.
MAX_CACHED_ENGINES_ENV = "SLAYER_MAX_CACHED_ENGINES"


def _max_cached_engines() -> int:
    """Cache cap, defaulting on junk input. Read per call so it stays retunable."""
    raw = os.environ.get(MAX_CACHED_ENGINES_ENV)
    if raw is None:
        return DEFAULT_MAX_CACHED_ENGINES
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "%s=%r is not an integer; using default %d.",
            MAX_CACHED_ENGINES_ENV, raw, DEFAULT_MAX_CACHED_ENGINES,
        )
        return DEFAULT_MAX_CACHED_ENGINES
    if value < 0:
        logger.warning(
            "%s=%d is negative; using default %d.",
            MAX_CACHED_ENGINES_ENV, value, DEFAULT_MAX_CACHED_ENGINES,
        )
        return DEFAULT_MAX_CACHED_ENGINES
    return value


def loggable_key(key: EngineCacheKey) -> str:
    """Short, stable, log-safe id for a cache key.

    ``key[0]`` is the connection string, rendered with ``hide_password=False``
    — so the raw key must never reach a log line. The digest stays correlatable
    across lines without being reversible.
    """
    return _digest("\x00".join(key))


def _dispose_quietly(*, engine: sa.Engine, reason: str) -> None:
    """Release an engine's pooled connections, logging rather than raising.

    Safe on an engine someone still holds: ``dispose()`` swaps in a fresh pool
    and lets in-flight connections close on return. ``reason`` is logged, so
    keep secrets out of it — see :func:`loggable_key`.
    """
    try:
        engine.dispose()
    except Exception:
        logger.warning("Failed to dispose engine (%s).", reason, exc_info=True)


def _take_evictions_over_limit() -> list[sa.Engine]:
    """Pop LRU entries until the cache fits its cap.

    Returns them instead of disposing: callers hold ``_cache_lock``, and
    ``dispose()`` does I/O. Dispose after releasing.
    """
    limit = _max_cached_engines()
    evicted: list[sa.Engine] = []
    while len(_engine_cache) > limit:
        _, engine = _engine_cache.popitem(last=False)
        evicted.append(engine)
    return evicted


def _cache_key(datasource: DatasourceConfig, connection_string: str) -> EngineCacheKey:
    """Cache identity for ``datasource``: URL + runtime fields + credentials.

    Kept in one place because ``query_engine._sql_client_cache_key`` must agree
    with it; a divergence between the two caches means a caller can get a client
    whose engine was built for different credentials.

    Only ``get_engine`` snapshots ``datasource`` first — it is the one caller
    with a slow build between the key and the credentials it describes. Callers
    that use the key immediately just miss and rebuild.
    """
    return (
        connection_string,
        _runtime_fingerprint(datasource),
        dialect_for_ds_type(datasource.type).credential_fingerprint(datasource),
    )


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

    The cap is re-applied on hits as well as inserts, so lowering
    ``SLAYER_MAX_CACHED_ENGINES`` takes effect on the next call, not the next
    miss.
    """
    # DatasourceConfig is mutable and the build sits between the key and the
    # dialect's second read of the credentials; a rotation in that window would
    # cache an engine under a fingerprint that misdescribes it. Shallow is
    # enough — every field is a scalar.
    snapshot = datasource.model_copy()
    connection_string = snapshot.get_connection_string()
    cache_key = _cache_key(datasource=snapshot, connection_string=connection_string)
    with _cache_lock:
        cached = _engine_cache.get(cache_key)
        if cached is None:
            trimmed, reuse = [], False
        else:
            _engine_cache.move_to_end(cache_key)
            # Trim here too, else a lowered cap waits for the next miss.
            trimmed = _take_evictions_over_limit()
            # A cap of 0 trims the entry we just touched: fall through and
            # build rather than hand back an engine we are about to dispose.
            reuse = cache_key in _engine_cache
    for stale in trimmed:
        _dispose_quietly(engine=stale, reason="cache limit lowered")
    if reuse:
        return cached
    # Built outside the lock: construction does I/O, and every other datasource
    # would queue behind it.
    engine = _build_engine(
        datasource=snapshot, connection_string=connection_string,
    )
    with _cache_lock:
        winner = _engine_cache.get(cache_key)
        if winner is not None:
            # Someone else built it first; converge so one key never backs
            # two live pools.
            _engine_cache.move_to_end(cache_key)
            loser, engine = engine, winner
        else:
            loser = None
            _engine_cache[cache_key] = engine
        evicted = _take_evictions_over_limit()
    if loser is not None:
        _dispose_quietly(engine=loser, reason="lost a concurrent build race")
    for stale in evicted:
        _dispose_quietly(engine=stale, reason="evicted from engine cache")
    return engine


def invalidate_engine(datasource: DatasourceConfig) -> bool:
    """Drop and dispose the cached engine for ``datasource``. Returns whether
    one was cached.

    For credentials that have stopped working — revoked grant, rotated key.
    Such engines are poisoned permanently (the credentials are baked in at
    construction), so retrying through the cache fails identically until one is
    thrown away; a transient blip, by contrast, wants the pool kept.
    ``get_engine`` then rebuilds from whatever the datasource now carries.
    """
    connection_string = datasource.get_connection_string()
    cache_key = _cache_key(datasource=datasource, connection_string=connection_string)
    with _cache_lock:
        evicted = _engine_cache.pop(cache_key, None)
    if evicted is None:
        return False
    _dispose_quietly(engine=evicted, reason=f"credentials rejected for '{datasource.name}'")
    return True


def reset_cache(*, dispose: bool = False) -> None:
    """Discard every cached engine.

    ``dispose`` defaults to False, preserving the test-fixture behaviour of
    dropping references only. Pass True when tearing a process down, so
    server-side connections close promptly rather than at GC.
    """
    with _cache_lock:
        dropped = list(_engine_cache.items())
        _engine_cache.clear()
    if dispose:
        for key, engine in dropped:
            _dispose_quietly(engine=engine, reason=f"cache reset ({loggable_key(key)})")
