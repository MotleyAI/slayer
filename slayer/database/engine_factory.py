"""Shared engine factory: the single ``get_engine(datasource)`` every engine-creating
path funnels through. Dialect-agnostic; cached by connection string + runtime fingerprint."""

from __future__ import annotations

import atexit
import logging
import os
import threading
from collections import OrderedDict
from typing import Any

import sqlalchemy as sa
import sqlalchemy.event as sa_event
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import ArgumentError, InvalidRequestError, NoSuchModuleError
from sqlalchemy.pool import ConnectionPoolEntry, QueuePool

from slayer.core.models import DatasourceConfig
from slayer.database import server_profiles
from slayer.database.errors import DatabaseUnavailable
from slayer.sql import connection_policy
from slayer.sql.dialects import dialect_for_ds_type, dialect_for_sa_backend
from slayer.sql.dialects.base import LocalDatabase, ServerProfile, SqlDialect, _digest
from slayer.sql.dialects.drivers import load_driver

logger = logging.getLogger(__name__)


EngineCacheKey = tuple[str, str, str]

# SQLite in-memory sentinel (bare value or path of ``sqlite:///:memory:``).
_MEMORY_DB_NAME = ":memory:"

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
    """A datasource's connection identity — delegates to :func:`_cache_key` so a memoized client can't disagree with the engine's creds."""
    return _cache_key(
        datasource=datasource, connection_string=datasource.get_connection_string(),
    )


ClientCacheKey = tuple[str, EngineCacheKey]


def client_cache_key(datasource: DatasourceConfig) -> ClientCacheKey:
    """A query engine's client-cache key: datasource name plus connection identity."""
    return (datasource.name, _sql_client_cache_key(datasource))


def _runtime_fingerprint(datasource: DatasourceConfig) -> str:
    """Fingerprint of runtime fields so datasources differing only in (e.g.) warehouse or ``transaction_pooler`` don't share an engine."""
    pooler = "pooler=1" if datasource.transaction_pooler else ""
    return "|".join(p for p in (_dialect_fingerprint(datasource), pooler) if p)


def _dialect_fingerprint(datasource: DatasourceConfig) -> str:
    """Snowflake's session fields and an in-memory SQLite's name; ``""`` elsewhere."""
    if datasource.type == "snowflake":
        parts = (
            ("wh", datasource.warehouse or ""),
            ("rl", datasource.role or ""),
            ("db", datasource.database or ""),
            ("sc", datasource.schema_name or ""),
        )
        return "|".join(f"{k}={v}" for k, v in parts)
    # An in-memory database is unique to its datasource: two ``:memory:`` sources
    # share one connection string, so the name must key them apart (D2).
    if datasource.type == "sqlite" and _is_in_memory_sqlite(
        datasource.get_connection_string()
    ):
        return f"mem={datasource.name}"
    return ""


def _attach_session_overrides_listener(
    *,
    engine: sa.Engine,
    datasource: DatasourceConfig,
) -> None:
    """Apply ``apply_session_overrides`` once per new connection (the client refuses a stray ``USE``). Skipped when the hook is the no-op."""
    dialect = dialect_for_ds_type(datasource.type)
    base_method = SqlDialect.apply_session_overrides
    dialect_method = type(dialect).apply_session_overrides
    if dialect_method is base_method:
        return

    @sa_event.listens_for(engine, "connect")
    def _slayer_session_overrides(dbapi_connection, _connection_record):
        dialect.apply_session_overrides(
            dbapi_connection=dbapi_connection,
            datasource=datasource,
        )


def _attach_register_udfs_for_dialect(*, engine: sa.Engine, dialect: SqlDialect) -> None:
    """Call ``register_udfs`` on every new connection, unless the dialect keeps the no-op base (SQLite needs median / percentile_cont / stddev / ... or generated SQL fails with ``no such function``)."""
    if type(dialect).register_udfs is SqlDialect.register_udfs:
        return

    @sa_event.listens_for(engine, "connect")
    def _slayer_register_udfs(dbapi_connection, _connection_record):
        dialect.register_udfs(dbapi_connection)


def _attach_register_udfs_listener(
    *,
    engine: sa.Engine,
    datasource: DatasourceConfig,
) -> None:
    """``_attach_register_udfs_for_dialect`` for a datasource's dialect."""
    _attach_register_udfs_for_dialect(
        engine=engine, dialect=dialect_for_ds_type(datasource.type),
    )


def _names_async_driver(url: URL) -> bool:
    """Whether ``url`` names an async driver (an unknown one is judged at engine build)."""
    try:
        return url.get_dialect().is_async
    except NoSuchModuleError:
        return False


def local_database(connection_string: str | URL) -> LocalDatabase:
    """Where ``connection_string`` keeps its database, as its backend's dialect reads the URL."""
    if connection_string == _MEMORY_DB_NAME:
        return LocalDatabase(kind="memory")
    try:
        url = make_url(connection_string)
    except ArgumentError:
        return LocalDatabase()
    return dialect_for_sa_backend(url.get_backend_name()).local_database(url)


def _is_in_memory_sqlite(connection_string: str) -> bool:
    """True iff the connection string is a SQLite in-memory database (URI forms included) on a sync driver;
    an async one goes to the engine build, which refuses it."""
    if connection_string == _MEMORY_DB_NAME:
        return True
    try:
        url = make_url(connection_string)
    except ArgumentError:
        return False
    if url.get_backend_name() != "sqlite" or _names_async_driver(url):
        return False
    return local_database(url).kind == "memory"


def refuse_missing_local_file(datasource: DatasourceConfig, connection_string: str) -> None:
    """Refuse a local database file that does not exist, before anything connects (and creates it)."""
    local = local_database(connection_string)
    if local.kind == "file" and local.path is not None and not os.path.exists(local.path):
        raise _missing_file(datasource, local.path)


def _missing_file(datasource: DatasourceConfig, path: str) -> DatabaseUnavailable:
    return DatabaseUnavailable(f"Datasource {datasource.name!r}: its database file {path!r} does not exist.")


class _OneThreadPool(QueuePool):
    """One connection, held by one thread at a time: a concurrent checkout waits; a nested one fails at once."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._holder: int | None = None

    def _do_get(self) -> ConnectionPoolEntry:
        if self._holder == threading.get_ident():
            raise InvalidRequestError(
                "nested checkout of the in-memory SQLite connection: this thread already holds it",
            )
        record = super()._do_get()
        self._holder = threading.get_ident()
        return record

    def _do_return_conn(self, record: ConnectionPoolEntry) -> None:
        self._holder = None
        super()._do_return_conn(record)


def build_in_memory_sqlite_engine(connection_string: str) -> sa.Engine:
    """The one in-memory SQLite engine builder: its one connection (the database) serves one thread at a time and
    lives until invalidated or disposed. Registers the SQLite UDFs on connect."""
    # make_url rejects bare ":memory:" — normalize to the scheme form first.
    if connection_string == _MEMORY_DB_NAME:
        connection_string = f"sqlite:///{_MEMORY_DB_NAME}"
    engine = sa.create_engine(
        connection_string,
        poolclass=_OneThreadPool,
        pool_size=1,
        max_overflow=0,
        connect_args={"check_same_thread": False},
    )
    _attach_register_udfs_for_dialect(
        engine=engine, dialect=dialect_for_ds_type("sqlite"),
    )
    return engine


def _dbapi_first_row(dbapi_connection: Any, sql: str) -> server_profiles.FirstRow:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
    finally:
        cursor.close()
    return None if row is None else tuple(row)


def _attach_read_only_listeners(
    *, engine: sa.Engine, datasource: DatasourceConfig, connection_string: str, dialect: SqlDialect,
) -> None:
    """Make every new connection read-only (its server profile read first where the dialect needs it), start
    each transaction read-only where the dialect does, and type a local database file vanishing before connect."""
    local = local_database(connection_string)
    if local.kind != "memory" and type(dialect).make_connection_read_only is not SqlDialect.make_connection_read_only:

        @sa_event.listens_for(engine, "connect")
        def _slayer_read_only(dbapi_connection, _connection_record):
            def _apply(profile: ServerProfile) -> None:
                dialect.make_connection_read_only(dbapi_connection, profile=profile)

            profile = ServerProfile()
            if dialect.read_only_reads_profile:
                profile = server_profiles.cached_profile(
                    engine=engine, dialect=dialect, on_base=_apply,
                    first_row=lambda sql: _dbapi_first_row(dbapi_connection, sql),
                )
            _apply(profile)

    if dialect.begins_read_only(url=make_url(connection_string), datasource=datasource):

        @sa_event.listens_for(engine, "begin")
        def _slayer_begin_read_only(conn):
            dialect.begin_read_only(conn.connection.dbapi_connection)

    if local.kind == "file" and local.path is not None:
        path = local.path

        @sa_event.listens_for(engine, "handle_error")
        def _slayer_missing_database(context):
            if dialect.missing_database_error(context.original_exception):
                return _missing_file(datasource, path)
            return None


def _build_engine(*, datasource: DatasourceConfig, connection_string: str) -> sa.Engine:
    """Build a new engine (no cache). In-memory SQLite goes through its one builder; otherwise ``build_engine``, falling back to ``sa.create_engine`` when the dialect declines."""
    connection_policy.check_transaction_pooler_backend(datasource, url=connection_string)
    if _is_in_memory_sqlite(connection_string):
        return build_in_memory_sqlite_engine(connection_string)
    dialect = dialect_for_ds_type(datasource.type)
    load_driver(datasource, connection_string, dialect=dialect, is_async=False)
    refuse_missing_local_file(datasource, connection_string)
    engine = dialect.build_engine(datasource, connection_string=connection_string)
    if engine is None:
        engine = sa.create_engine(connection_string, **connection_policy.engine_kwargs(
            url=connection_string, dialect=dialect, datasource=datasource,
        ))
    _attach_register_udfs_listener(engine=engine, datasource=datasource)
    _attach_session_overrides_listener(engine=engine, datasource=datasource)
    _attach_read_only_listeners(
        engine=engine, datasource=datasource, connection_string=connection_string, dialect=dialect,
    )
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
    if reuse and cached is not None:
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


def holds_local_file(engine: sa.Engine) -> bool:
    """Whether ``engine``'s idle connections hold a local database file: a file-backed SQLite or DuckDB URL on a
    QueuePool (an in-memory database closes its live data when drained)."""
    return isinstance(engine.pool, QueuePool) and local_database(engine.url).kind == "file"


def release_idle(engine: sa.Engine) -> None:
    """Close a cached engine's idle local-file connections (freeing file locks) without disposing it: checked-out
    connections return to the same, still-owned pool; networked connections stay open."""
    try:
        if holds_local_file(engine):
            engine.pool.dispose()
    except Exception:
        logger.warning("Failed to release idle connections.", exc_info=True)


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


def reset_cache() -> None:
    """Discard and dispose every cached engine (teardown / test isolation).

    Always disposes: a reset that leaves live pools behind is the leak (D5).
    Dispose closes checked-in connections and lets a checked-out one finish and
    close on return.
    """
    with _cache_lock:
        dropped = list(_engine_cache.items())
        _engine_cache.clear()
    for key, engine in dropped:
        _dispose_quietly(engine=engine, reason=f"cache reset ({loggable_key(key)})")


def _forget_parent_pools() -> None:
    """In a forked child: a fresh cache lock (the parent's may be held), and inherited pools dropped without
    closing the parent's connections."""
    global _cache_lock
    _cache_lock = threading.Lock()
    for engine in _engine_cache.values():
        engine.dispose(close=False)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_forget_parent_pools)

atexit.register(reset_cache)
