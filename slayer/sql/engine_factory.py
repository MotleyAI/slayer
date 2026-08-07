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


#: Identity of a cached engine: URL + runtime fields + credentials. Exported so
#: callers that key their own caches by engine identity stay in lockstep with
#: this module instead of hard-coding the arity.
EngineCacheKey = tuple[str, str, str]

# Engine cache. Key = (connection_string, runtime_fingerprint, credential_fingerprint).
# The credential leg is load-bearing for security: a dialect whose secret is not
# in the URL (BigQuery's service-account / OAuth credentials) would otherwise
# have two differently-authenticated callers share one engine, silently running
# one identity's queries under the other's credentials.
#
# Ordered least- to most-recently-used, and bounded: once credentials are part
# of the key, cardinality follows the number of distinct *identities*, not
# datasources. A deployment handing SLayer per-end-user credentials would
# otherwise accumulate one engine — and one connection pool — per user who ever
# ran a query, for the life of the process.
_engine_cache: "OrderedDict[EngineCacheKey, sa.Engine]" = OrderedDict()

# Guards every read, recency bump, insert, eviction and reset of the cache
# above. Engines are reached from worker threads (``_run_sync_in_thread`` in
# slayer.sql.client) as well as from the event loop, so the lookup/move_to_end
# pair is not safe unsynchronised: an ``invalidate_engine`` landing between the
# two raises KeyError, and two threads missing at once build two pools for one
# key and silently orphan the loser.
#
# Never held across engine construction or ``dispose()`` — both do real I/O,
# and serialising unrelated datasources behind them would cost more than the
# race it prevents.
_cache_lock = threading.Lock()

#: Cap on simultaneously cached engines. Sized for "every datasource, times a
#: working set of active users" rather than for a whole user base; a miss costs
#: one engine construction, so over-eviction degrades latency, not correctness.
DEFAULT_MAX_CACHED_ENGINES = 64

#: Env override for :data:`DEFAULT_MAX_CACHED_ENGINES`. ``0`` disables caching.
MAX_CACHED_ENGINES_ENV = "SLAYER_MAX_CACHED_ENGINES"


def _max_cached_engines() -> int:
    """Resolve the cache cap, falling back to the default on junk input.

    Read per call rather than at import so tests and hosts can retune it
    without reloading the module.
    """
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
    """A short, stable id for a cache key that is safe to put in a log line.

    ``key[0]`` is the connection string, and for username/password dialects
    ``DatasourceConfig.get_connection_string`` renders it with
    ``hide_password=False`` — so the raw key carries a plaintext password and
    must never be logged. Digesting the whole key keeps entries correlatable
    across log lines while reducing every leg to non-reversible hex.
    """
    return _digest("\x00".join(key))


def _dispose_quietly(*, engine: sa.Engine, reason: str) -> None:
    """Release an engine's pooled connections, logging rather than raising.

    ``Engine.dispose()`` closes checked-in connections and swaps in a fresh
    pool; connections checked out by an in-flight query are detached and
    closed when returned. So this is safe to call on an engine another caller
    still holds a reference to — it reclaims sockets without breaking them.

    ``reason`` reaches a log line, so callers must keep secrets out of it —
    see :func:`loggable_key` for cache keys.
    """
    try:
        engine.dispose()
    except Exception:
        logger.warning("Failed to dispose engine (%s).", reason, exc_info=True)


def _take_evictions_over_limit() -> list[sa.Engine]:
    """Pop least-recently-used entries until the cache fits its cap.

    Returns the evicted engines rather than disposing them, because callers
    run this holding ``_cache_lock`` and ``dispose()`` closes sockets. Dispose
    the result after releasing the lock.
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

    ``datasource`` is read, not retained. Only ``get_engine`` snapshots it
    first, because only there does a slow engine build sit between the key and
    the credentials it claims to describe. Callers that compute a key and use
    it immediately are safe: a config mutated underneath them yields a key that
    simply misses and rebuilds.
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
    ``SLAYER_MAX_CACHED_ENGINES`` — or setting it to ``0`` to turn caching off
    — takes effect on the next call instead of waiting for a miss.
    """
    # Snapshot first: DatasourceConfig is mutable, and the key is computed long
    # before _build_engine re-reads the credentials — engine construction sits
    # between them. A rotation landing in that window would cache an engine
    # under a fingerprint that doesn't describe what it authenticates as, which
    # is the exact confusion the credential leg of the key exists to prevent.
    # Shallow copy suffices: every field is a scalar, so this is already
    # detached from later writes to the caller's object.
    snapshot = datasource.model_copy()
    connection_string = snapshot.get_connection_string()
    cache_key = _cache_key(datasource=snapshot, connection_string=connection_string)
    with _cache_lock:
        cached = _engine_cache.get(cache_key)
        if cached is None:
            trimmed, reuse = [], False
        else:
            _engine_cache.move_to_end(cache_key)
            # Re-apply the cap on the hit path too. A cache that only trims on
            # insert stays oversized until the next miss, and a cap of 0 would
            # keep serving entries admitted before it was set.
            trimmed = _take_evictions_over_limit()
            # A cap of 0 trims the entry we just touched, which is what "no
            # caching" has to mean: fall through and build instead of handing
            # back an engine we are about to dispose.
            reuse = cache_key in _engine_cache
    for stale in trimmed:
        _dispose_quietly(engine=stale, reason="cache limit lowered")
    if reuse:
        return cached
    # Built outside the lock — construction does real work (BigQuery mints an
    # API client), and every other datasource would queue behind it.
    engine = _build_engine(
        datasource=snapshot, connection_string=connection_string,
    )
    with _cache_lock:
        winner = _engine_cache.get(cache_key)
        if winner is not None:
            # Another caller built the same engine while we were constructing.
            # Converge on theirs so one key never backs two live pools.
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

    Meant for the case where the *credentials* an engine was built with have
    stopped working — a revoked OAuth grant, a rotated service-account key.
    Those engines are poisoned for good: the credential object is baked in at
    construction, so every retry through the cache fails identically until
    someone throws the engine away. Retrying a transient network blip, by
    contrast, wants the pool kept.

    ``get_engine`` rebuilds on the next call, picking up whatever credentials
    the datasource now carries.
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

    ``dispose`` defaults to False to preserve the long-standing test-fixture
    behaviour of dropping references without touching pools. Hosts tearing a
    process down want ``dispose=True`` so server-side connections close
    promptly instead of waiting on garbage collection.
    """
    with _cache_lock:
        dropped = list(_engine_cache.items())
        _engine_cache.clear()
    if dispose:
        for key, engine in dropped:
            _dispose_quietly(engine=engine, reason=f"cache reset ({loggable_key(key)})")
