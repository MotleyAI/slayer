"""Shared engine factory: the single ``get_engine(datasource)`` every engine-creating
path funnels through. Dialect-agnostic; cached by connection string + runtime fingerprint."""

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


EngineCacheKey = tuple[str, str, str]

# LRU-ordered, bounded. Credential leg is a security boundary: with the secret out of the URL (BigQuery), two identities would otherwise share one engine.
_engine_cache: "OrderedDict[EngineCacheKey, sa.Engine]" = OrderedDict()

# Guards the lookup/move_to_end pair (reached from worker threads); never held across construction or dispose() (both do I/O).
_cache_lock = threading.Lock()

DEFAULT_MAX_CACHED_ENGINES = 64

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
    """Log-safe (irreversible) digest — ``key[0]`` embeds a plaintext secret, never log it raw."""
    return _digest("\x00".join(key))


def _dispose_quietly(*, engine: sa.Engine, reason: str) -> None:
    """Dispose an engine's pool, logging rather than raising (safe while others hold it). Keep secrets out of ``reason`` — it's logged."""
    try:
        engine.dispose()
    except Exception:
        logger.warning("Failed to dispose engine (%s).", reason, exc_info=True)


def _take_evictions_over_limit() -> list[sa.Engine]:
    """Pop LRU entries over the cap and RETURN them (callers hold ``_cache_lock``; dispose after releasing)."""
    limit = _max_cached_engines()
    evicted: list[sa.Engine] = []
    while len(_engine_cache) > limit:
        _, engine = _engine_cache.popitem(last=False)
        evicted.append(engine)
    return evicted


def _cache_key(datasource: DatasourceConfig, connection_string: str) -> EngineCacheKey:
    """Cache identity: URL + runtime fields + credentials (shared with ``_sql_client_cache_key``)."""
    return (
        connection_string,
        _runtime_fingerprint(datasource),
        dialect_for_ds_type(datasource.type).credential_fingerprint(datasource),
    )


def _sql_client_cache_key(datasource: DatasourceConfig) -> EngineCacheKey:
    """Cache key for ``SlayerQueryEngine._sql_clients`` — delegates to :func:`_cache_key` so a memoized client can't disagree with the engine's creds."""
    return _cache_key(
        datasource=datasource, connection_string=datasource.get_connection_string(),
    )


def _runtime_fingerprint(datasource: DatasourceConfig) -> str:
    """Fingerprint of runtime fields so datasources differing only in (e.g.) warehouse don't share an engine. Only Snowflake uses these; others collapse to ``""``."""
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
    """Apply ``apply_session_overrides`` on every pool ``checkout`` (not ``connect``) so a stray ``USE`` can't leak into the next query. Skipped when the hook is the no-op."""
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
    """Call ``register_udfs`` on every new connection. Skipped unless SQLite, which needs median / percentile_cont / stddev / ... UDFs or generated SQL fails with ``no such function``."""
    dialect = dialect_for_ds_type(datasource.type)
    base_method = SqlDialect.register_udfs
    dialect_method = type(dialect).register_udfs
    if dialect_method is base_method:
        return

    @sa_event.listens_for(engine, "connect")
    def _slayer_register_udfs(dbapi_connection, _connection_record):
        dialect.register_udfs(dbapi_connection)


def _build_engine(*, datasource: DatasourceConfig, connection_string: str) -> sa.Engine:
    """Build a new engine (no cache) via ``build_engine``, falling back to ``sa.create_engine`` when the dialect declines."""
    dialect = dialect_for_ds_type(datasource.type)
    engine = dialect.build_engine(datasource, connection_string=connection_string)
    if engine is None:
        engine = sa.create_engine(connection_string, pool_pre_ping=True)
    _attach_register_udfs_listener(engine=engine, datasource=datasource)
    _attach_session_overrides_listener(engine=engine, datasource=datasource)
    return engine


def get_engine(datasource: DatasourceConfig) -> sa.Engine:
    """Return a cached engine, building on a miss. The cap is re-applied on hits too, so a lowered ``SLAYER_MAX_CACHED_ENGINES`` takes effect next call."""
    # Snapshot first: a credential rotation mid-build would cache an engine under a wrong fingerprint.
    snapshot = datasource.model_copy()
    connection_string = snapshot.get_connection_string()
    cache_key = _cache_key(datasource=snapshot, connection_string=connection_string)
    with _cache_lock:
        cached = _engine_cache.get(cache_key)
        if cached is None:
            trimmed, reuse = [], False
        else:
            _engine_cache.move_to_end(cache_key)
            trimmed = _take_evictions_over_limit()
            # A cap of 0 trims the entry we just touched: fall through and rebuild.
            reuse = cache_key in _engine_cache
    for stale in trimmed:
        _dispose_quietly(engine=stale, reason="cache limit lowered")
    if reuse:
        return cached
    # Built outside the lock: construction does I/O and would queue everyone else behind it.
    engine = _build_engine(
        datasource=snapshot, connection_string=connection_string,
    )
    with _cache_lock:
        winner = _engine_cache.get(cache_key)
        if winner is not None:
            # Someone built it first; converge so one key never backs two live pools.
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
    """Drop and dispose the cached engine (returns whether one was cached) — creds are baked in at construction, so only eviction lets ``get_engine`` rebuild after a rotation/revocation."""
    snapshot = datasource.model_copy()
    connection_string = snapshot.get_connection_string()
    cache_key = _cache_key(datasource=snapshot, connection_string=connection_string)
    with _cache_lock:
        evicted = _engine_cache.pop(cache_key, None)
    if evicted is None:
        return False
    _dispose_quietly(engine=evicted, reason=f"credentials rejected for '{datasource.name}'")
    return True


def reset_cache(*, dispose: bool = False) -> None:
    """Discard every cached engine; ``dispose=True`` (for teardown) also closes their server-side connections promptly."""
    with _cache_lock:
        dropped = list(_engine_cache.items())
        _engine_cache.clear()
    if dispose:
        for key, engine in dropped:
            _dispose_quietly(engine=engine, reason=f"cache reset ({loggable_key(key)})")
