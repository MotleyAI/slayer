"""SQL client for executing queries against databases."""

import asyncio
import concurrent.futures
import functools
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

import sqlalchemy as sa
import sqlalchemy.engine.url
import sqlalchemy.event as sa_event
import sqlalchemy.exc
from sqlalchemy.pool import StaticPool

from slayer.core.models import DatasourceConfig
from slayer.sql.sqlite_udfs import register_sqlite_udfs

logger = logging.getLogger(__name__)

# Async-capable drivers: db_type → SQLAlchemy async scheme.
# Databases not listed here fall back to sync execution in a thread pool.
_ASYNC_DRIVERS = {
    "postgres": "postgresql+asyncpg",
    "postgresql": "postgresql+asyncpg",
    "mysql": "mysql+aiomysql",
    "mariadb": "mysql+aiomysql",
}

# ---------------------------------------------------------------------------
# Engine caches — reuse connection pools across queries
# ---------------------------------------------------------------------------

# DBAPI sentinel for SQLite in-memory databases. Appears as either the bare
# value or the path component of `sqlite:///:memory:` connection strings.
_MEMORY_DB_NAME = ":memory:"

_sync_engines: Dict[str, sa.Engine] = {}


def _get_sync_engine(connection_string: str) -> sa.Engine:
    """Get or create a cached sync engine (with connection pool).

    Sync engines are safe to cache globally — they're not tied to an event loop.
    For SQLite, we attach a ``connect`` event listener that registers Python
    aggregate UDFs (median/percentile_cont/percentile_disc) on every new
    connection — SQLite has no native equivalents.

    In-memory SQLite is NOT routed through this cache: each
    ``SlayerSQLClient`` owns its own per-instance engine (see
    ``_create_in_memory_sqlite_engine``) so two clients on
    ``sqlite:///:memory:`` get isolated databases.
    """
    if connection_string not in _sync_engines:
        engine = sa.create_engine(connection_string, pool_pre_ping=True)
        if engine.dialect.name == "sqlite":
            @sa_event.listens_for(engine, "connect")
            def _register_udfs(dbapi_connection, _connection_record):
                register_sqlite_udfs(dbapi_connection)
        _sync_engines[connection_string] = engine
    return _sync_engines[connection_string]


def _is_in_memory_sqlite(connection_string: str) -> bool:
    """Return True iff ``connection_string`` refers to a SQLite in-memory database.

    Uses ``sqlalchemy.engine.url.make_url`` to handle URI-form variants
    (``file::memory:?cache=shared&uri=true``, ``mode=memory`` query param)
    in addition to the bare ``:memory:`` and ``sqlite:///:memory:`` forms.
    """
    if connection_string == _MEMORY_DB_NAME:
        return True
    try:
        url = sqlalchemy.engine.url.make_url(connection_string)
    except sqlalchemy.exc.ArgumentError:
        return False
    if not url.drivername.startswith("sqlite"):
        return False
    database = url.database
    if not database or database == _MEMORY_DB_NAME:
        return True
    query: Dict[str, Any] = dict(url.query) if url.query else {}
    # SQLite honors `mode=memory` and the `file::memory:` URI form ONLY when
    # the connection is opened with URI handling enabled (`uri=true`).
    # Without `uri=true`, SQLite treats the database part as a literal
    # filename — `sqlite:///file:foo?mode=memory` actually creates a file
    # called "file:foo" on disk. Misclassifying those as in-memory would
    # break per-client isolation: two clients on the same string would each
    # build a StaticPool engine but both back onto the same on-disk file.
    is_uri = str(query.get("uri", "")).lower() == "true"
    if is_uri and database.startswith("file:") and (
        query.get("mode") == "memory" or _MEMORY_DB_NAME in database
    ):
        return True
    return False


def _create_in_memory_sqlite_engine(connection_string: str) -> sa.Engine:
    """Create a fresh sync engine for an in-memory SQLite connection string.

    ``StaticPool`` keeps a single connection pinned for the engine's lifetime,
    and ``check_same_thread=False`` allows that connection to be reused across
    asyncio worker threads. Together they make ``sqlite:///:memory:`` usable
    across multiple async calls — without them every ``asyncio.to_thread``
    call would land on a thread with its own private in-memory database.

    The same ``connect`` event registers Python UDFs (median/percentile_cont/
    percentile_disc) as ``_get_sync_engine`` does. With StaticPool the event
    fires exactly once.
    """
    # SQLAlchemy's make_url rejects a bare ":memory:" — normalize to the
    # standard scheme form before create_engine. The detector accepts the
    # bare form for caller convenience; this is where it gets canonicalized.
    if connection_string == _MEMORY_DB_NAME:
        connection_string = f"sqlite:///{_MEMORY_DB_NAME}"
    engine = sa.create_engine(
        connection_string,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    @sa_event.listens_for(engine, "connect")
    def _register_udfs(dbapi_connection, _connection_record):
        register_sqlite_udfs(dbapi_connection)
    return engine


def _resolve_sync_engine(
    connection_string: str,
    override_engine: Optional[sa.Engine] = None,
) -> sa.Engine:
    """Choose the engine for a sync DB call.

    If ``override_engine`` is provided (per-client engine for in-memory
    SQLite, supplied by ``SlayerSQLClient``), use it. Otherwise return the
    module-cached engine from ``_get_sync_engine``.
    """
    if override_engine is not None:
        return override_engine
    return _get_sync_engine(connection_string)


def _get_async_engine(connection_string: str):
    """Create an async engine for the current event loop.

    NOT cached globally — async engines bind to the event loop that created them.
    Callers should cache per-loop if needed (e.g., in a web app's lifespan).
    For query-per-request patterns, the overhead of engine creation is negligible
    compared to the query itself, and the connection pool handles reuse within
    a single engine's lifetime.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(connection_string, pool_pre_ping=True)


def _async_connection_string(connection_string: str, db_type: Optional[str]) -> Optional[str]:
    """Convert a sync connection string to its async equivalent, or None if no async driver."""
    async_scheme = _ASYNC_DRIVERS.get(db_type)
    if async_scheme is None:
        return None
    if "://" in connection_string:
        _, _, remainder = connection_string.partition("://")
        return f"{async_scheme}://{remainder}"
    return None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


def _map_type_code(type_code, db_type: Optional[str] = None) -> str:
    """Map a DB-API type_code to a SLayer type category.

    Handles DuckDB (string type names), SQLite (Python types),
    asyncpg (Postgres OID integers), and aiomysql (MySQL field-type codes).
    When ``db_type`` is provided, the correct OID/field-type map is selected.
    """
    if isinstance(type_code, str):
        # DuckDB returns type name strings like 'INTEGER', 'VARCHAR', etc.
        tc = type_code.upper()
        if any(t in tc for t in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")):
            return "number"
        if any(t in tc for t in ("VARCHAR", "TEXT", "CHAR", "STRING", "BLOB", "ENUM")):
            return "string"
        if any(t in tc for t in ("TIMESTAMP", "DATE", "TIME", "INTERVAL")):
            return "time"
        if "BOOL" in tc:
            return "boolean"
        return "string"
    if isinstance(type_code, type):
        # SQLite/some drivers return Python types
        # Check bool before int — bool is a subclass of int in Python
        if issubclass(type_code, bool):
            return "boolean"
        if issubclass(type_code, (int, float)):
            return "number"
        if issubclass(type_code, str):
            return "string"
        return "string"
    if isinstance(type_code, int):
        # Select the correct map by database type
        if db_type and "mysql" in db_type.lower():
            return _MYSQL_TYPE_MAP.get(type_code, "string")
        return _PG_OID_MAP.get(type_code, "string")
    return "string"


# Postgres OIDs (from pg_type)
_PG_OID_MAP: Dict[int, str] = {
    16: "boolean",   # bool
    20: "number",    # int8 (bigint)
    21: "number",    # int2 (smallint)
    23: "number",    # int4 (integer)
    26: "number",    # oid
    700: "number",   # float4
    701: "number",   # float8
    1700: "number",  # numeric
    790: "number",   # money
    18: "string",    # char
    25: "string",    # text
    1042: "string",  # bpchar
    1043: "string",  # varchar
    1082: "time",    # date
    1083: "time",    # time
    1114: "time",    # timestamp
    1184: "time",    # timestamptz
    1186: "time",    # interval
}

# MySQL field-type codes (aiomysql wire protocol)
_MYSQL_TYPE_MAP: Dict[int, str] = {
    0: "number",     # MYSQL_TYPE_DECIMAL
    1: "boolean",    # MYSQL_TYPE_TINY (TINYINT/BOOL)
    2: "number",     # MYSQL_TYPE_SHORT
    3: "number",     # MYSQL_TYPE_LONG (INT)
    4: "number",     # MYSQL_TYPE_FLOAT
    5: "number",     # MYSQL_TYPE_DOUBLE
    8: "number",     # MYSQL_TYPE_LONGLONG (BIGINT)
    9: "number",     # MYSQL_TYPE_INT24
    16: "number",    # MYSQL_TYPE_BIT
    246: "number",   # MYSQL_TYPE_NEWDECIMAL
    7: "time",       # MYSQL_TYPE_TIMESTAMP
    10: "time",      # MYSQL_TYPE_DATE
    11: "time",      # MYSQL_TYPE_TIME
    12: "time",      # MYSQL_TYPE_DATETIME
    13: "time",      # MYSQL_TYPE_YEAR
    14: "time",      # MYSQL_TYPE_NEWDATE
    15: "string",    # MYSQL_TYPE_VARCHAR
    253: "string",   # MYSQL_TYPE_VAR_STRING
    254: "string",   # MYSQL_TYPE_STRING
}


def _extract_types_from_cursor(result, db_type: Optional[str] = None) -> Dict[str, str]:
    """Extract {column_name: type_category} from a SQLAlchemy CursorResult.

    Uses cursor.description type_code when available (DuckDB, Postgres).
    Falls back to checking Python value types from the first row when
    type_codes are all None (SQLite, some drivers).
    """
    columns = list(result.keys())
    cursor_desc = result.cursor.description

    # Try cursor.description type_codes first
    if cursor_desc is not None:
        type_codes = [desc[1] for desc in cursor_desc]
        if any(tc is not None for tc in type_codes):
            return {col: _map_type_code(tc, db_type=db_type) for col, tc in zip(columns, type_codes)}

    # Fallback: check Python value types from the first fetched row
    rows = result.fetchall()
    if not rows:
        return {col: "string" for col in columns}  # empty table — safe default
    row = rows[0]
    types = {}
    for col, val in zip(columns, row):
        if val is None:
            types[col] = "string"  # can't infer from NULL
        elif isinstance(val, bool):
            types[col] = "boolean"
        elif isinstance(val, (int, float)):
            types[col] = "number"
        elif isinstance(val, str):
            types[col] = "string"
        elif hasattr(val, "isoformat"):
            types[col] = "time"
        else:
            types[col] = "string"
    return types


# Databases that return all-None cursor.description type codes need a real row
_NEEDS_ROW_FOR_TYPES = {"sqlite"}
# DBs that should call _execute_with_retry_sync inline from async coroutines.
# Empty: every dispatch goes through _run_sync_in_thread / _execute_with_retry_threaded
# so the event loop is never blocked on DB work or on time.sleep retry backoff.
_INLINE_SYNC_DB_TYPES: set[str] = set()


async def _run_sync_in_thread(func, *args, **kwargs):
    """Run one blocking DB call in a short-lived worker thread.

    Avoid using the event loop's default executor here. pytest-asyncio can wait
    indefinitely for default-executor threads after SQLite integration tests,
    while a scoped executor is shut down immediately after the call completes.
    """
    loop = asyncio.get_running_loop()
    call = functools.partial(func, *args, **kwargs)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return await loop.run_in_executor(executor, call)


def _get_column_types_sync(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    engine: Optional[sa.Engine] = None,
) -> Dict[str, str]:
    """Infer column types. Uses LIMIT 0 for cursor metadata, LIMIT 1 for SQLite."""
    engine = _resolve_sync_engine(connection_string, override_engine=engine)
    limit = 1 if db_type in _NEEDS_ROW_FOR_TYPES else 0
    limit_sql = f"SELECT * FROM ({sql}) AS _types LIMIT {limit}"
    with engine.connect() as conn:
        result = conn.execute(sa.text(limit_sql))
        return _extract_types_from_cursor(result, db_type=db_type)


async def _get_column_types_async(
    sql: str,
    engine,
    db_type: Optional[str],
) -> Dict[str, str]:
    """Async version of column type inference. Uses LIMIT 0; LIMIT 1 for SQLite."""
    limit = 1 if db_type in _NEEDS_ROW_FOR_TYPES else 0
    limit_sql = f"SELECT * FROM ({sql}) AS _types LIMIT {limit}"
    async with engine.connect() as conn:
        result = await conn.execute(sa.text(limit_sql))
        return _extract_types_from_cursor(result, db_type=db_type)


class SlayerSQLClient:
    """Executes SQL against databases via SQLAlchemy.

    Async path uses native async drivers (asyncpg, aiomysql) when available,
    with pooled connections. Falls back to sync-in-thread for databases without
    async drivers (SQLite, DuckDB, ClickHouse, etc.).

    The async engine is cached per client instance (tied to the current event loop).
    For web apps, keep the client alive across requests to reuse the pool.
    """

    def __init__(self, datasource: DatasourceConfig):
        self.datasource = datasource
        self._async_engine = None
        self._sync_engine: Optional[sa.Engine] = None

    def _get_async_engine(self):
        """Get or create the async engine for this client (cached per instance)."""
        if self._async_engine is None:
            conn_str = self.datasource.get_connection_string()
            async_conn_str = _async_connection_string(
                connection_string=conn_str, db_type=self.datasource.type,
            )
            if async_conn_str:
                self._async_engine = _get_async_engine(async_conn_str)
        return self._async_engine

    def _get_sync_engine_for_client(self) -> Optional[sa.Engine]:
        """Return a per-client sync engine for in-memory SQLite, else None.

        For ``sqlite:///:memory:`` (and equivalent URI-form variants) every
        ``SlayerSQLClient`` instance owns its own ``StaticPool`` engine so
        the single pinned connection is shared across all sync/async paths
        on this client — but isolated from other clients. For every other
        connection string this returns ``None`` and the helpers fall back
        to the module-level engine cache via ``_resolve_sync_engine``.
        """
        if self._sync_engine is not None:
            return self._sync_engine
        conn_str = self.datasource.get_connection_string()
        if _is_in_memory_sqlite(conn_str):
            self._sync_engine = _create_in_memory_sqlite_engine(conn_str)
            return self._sync_engine
        return None

    async def execute(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> List[Dict[str, Any]]:
        """Execute SQL asynchronously."""
        async_engine = self._get_async_engine()
        db_type = self.datasource.type
        if async_engine is not None:
            return await _execute_with_retry_async(
                sql=sql,
                engine=async_engine,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
            )
        if db_type in _INLINE_SYNC_DB_TYPES:
            return _execute_with_retry_sync(
                sql=sql,
                connection_string=self.datasource.get_connection_string(),
                db_type=db_type,
                timeout_seconds=timeout_seconds,
            )
        # No async driver — fall back to sync in thread pool
        return await _execute_with_retry_threaded(
            sql=sql,
            connection_string=self.datasource.get_connection_string(),
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=self._get_sync_engine_for_client(),
        )

    async def get_column_types(self, sql: str) -> Dict[str, str]:
        """Infer column types by executing SQL with LIMIT 0.

        Returns {column_name: type_category} where type_category is
        "number", "string", "time", or "boolean".
        """
        async_engine = self._get_async_engine()
        if async_engine is not None:
            return await _get_column_types_async(
                sql=sql, engine=async_engine, db_type=self.datasource.type,
            )
        if self.datasource.type in _INLINE_SYNC_DB_TYPES:
            return _get_column_types_sync(
                sql=sql,
                connection_string=self.datasource.get_connection_string(),
                db_type=self.datasource.type,
            )
        return await _run_sync_in_thread(
            _get_column_types_sync,
            sql=sql,
            connection_string=self.datasource.get_connection_string(),
            db_type=self.datasource.type,
            engine=self._get_sync_engine_for_client(),
        )

    def execute_sync(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> List[Dict[str, Any]]:
        """Execute SQL synchronously (for CLI, notebooks, tests)."""
        return _execute_with_retry_sync(
            sql=sql,
            connection_string=self.datasource.get_connection_string(),
            db_type=self.datasource.type,
            timeout_seconds=timeout_seconds,
            engine=self._get_sync_engine_for_client(),
        )


# ---------------------------------------------------------------------------
# Native async execution (asyncpg, aiomysql — pooled connections)
# ---------------------------------------------------------------------------


# Substituted into the retry-warning when the SQL is empty/whitespace, so the
# log line still has a recognisable "what was running" field.
_EMPTY_SQL_PLACEHOLDER = "<empty sql>"

# Format string for the warning logged on each retry attempt. Args are:
# attempt index (1-based), delay seconds, underlying DBAPI exception, SQL excerpt.
_TRANSIENT_RETRY_LOG_FORMAT = (
    "Transient DB error on attempt %d, retrying in %.1fs: %s | sql: %s"
)


async def _retry_with_backoff(
    *,
    sql: str,
    do_call: Callable[[], Awaitable[List[Dict[str, Any]]]],
    max_attempts: int,
    initial_delay: float,
    max_delay: float,
) -> List[Dict[str, Any]]:
    """Retry an async DB call with exponential backoff on transient errors.

    `sql` is used only for the warning's excerpt so users can correlate
    retries with the offending query. The underlying DBAPI message comes
    from `exc.orig` (e.g. sqlite3.OperationalError("database is locked"));
    without it the warning would be uninformative.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
    delay = initial_delay
    for attempt in range(max_attempts):
        try:
            return await do_call()
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError) as exc:
            if attempt == max_attempts - 1:
                raise
            sql_lines = (sql or "").strip().splitlines()
            sql_excerpt = sql_lines[0][:120] if sql_lines else _EMPTY_SQL_PLACEHOLDER
            logger.warning(
                _TRANSIENT_RETRY_LOG_FORMAT,
                attempt + 1, delay, getattr(exc, "orig", exc), sql_excerpt,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)


async def _execute_with_retry_async(
    sql: str,
    engine,
    db_type: Optional[str],
    timeout_seconds: int = 120,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> List[Dict[str, Any]]:
    return await _retry_with_backoff(
        sql=sql,
        do_call=lambda: _execute_sql_async(
            sql=sql, engine=engine, db_type=db_type, timeout_seconds=timeout_seconds,
        ),
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
    )


async def _execute_sql_async(
    sql: str,
    engine,
    db_type: Optional[str],
    timeout_seconds: int = 120,
) -> List[Dict[str, Any]]:
    async with engine.connect() as conn:
        timeout_ms = timeout_seconds * 1000
        if db_type in ("mysql", "mariadb"):
            await conn.execute(sa.text(f"SET max_execution_time = {timeout_ms}"))
        elif db_type in ("postgres", "postgresql", None):
            try:
                await conn.execute(sa.text(f"SET statement_timeout = {timeout_ms}"))
            except Exception:
                pass
        result = await conn.execute(sa.text(sql))
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]


# ---------------------------------------------------------------------------
# Thread-pool fallback (for DBs without async drivers: SQLite, DuckDB, etc.)
# ---------------------------------------------------------------------------


async def _execute_with_retry_threaded(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    timeout_seconds: int = 120,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    engine: Optional[sa.Engine] = None,
) -> List[Dict[str, Any]]:
    return await _retry_with_backoff(
        sql=sql,
        do_call=lambda: _run_sync_in_thread(
            _execute_sql_sync,
            sql=sql,
            connection_string=connection_string,
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=engine,
        ),
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
    )


# ---------------------------------------------------------------------------
# Sync execution (pooled connections, for CLI/notebooks and thread fallback)
# ---------------------------------------------------------------------------


def _execute_with_retry_sync(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    timeout_seconds: int = 120,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    engine: Optional[sa.Engine] = None,
) -> List[Dict[str, Any]]:
    delay = initial_delay
    for attempt in range(max_attempts):
        try:
            return _execute_sql_sync(
                sql=sql,
                connection_string=connection_string,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
                engine=engine,
            )
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError) as exc:
            if attempt == max_attempts - 1:
                raise
            sql_lines = (sql or "").strip().splitlines()
            sql_excerpt = sql_lines[0][:120] if sql_lines else _EMPTY_SQL_PLACEHOLDER
            logger.warning(
                _TRANSIENT_RETRY_LOG_FORMAT,
                attempt + 1, delay, getattr(exc, "orig", exc), sql_excerpt,
            )
            time.sleep(delay)
            delay = min(delay * 2, max_delay)


def _execute_sql_sync(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    timeout_seconds: int = 120,
    engine: Optional[sa.Engine] = None,
) -> List[Dict[str, Any]]:
    engine = _resolve_sync_engine(connection_string, override_engine=engine)
    with engine.connect() as conn:
        timeout_ms = timeout_seconds * 1000
        if db_type in ("mysql", "mariadb"):
            conn.execute(sa.text(f"SET max_execution_time = {timeout_ms}"))
        elif db_type == "clickhouse":
            conn.execute(sa.text(f"SET max_execution_time = {timeout_seconds}"))
        elif db_type in ("postgres", "postgresql", None):
            try:
                conn.execute(sa.text(f"SET statement_timeout = {timeout_ms}"))
            except Exception:
                pass
        result = conn.execute(sa.text(sql))
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]
