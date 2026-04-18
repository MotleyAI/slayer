"""SQL client for executing queries against databases."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig

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

_sync_engines: Dict[str, sa.Engine] = {}


def _get_sync_engine(connection_string: str) -> sa.Engine:
    """Get or create a cached sync engine (with connection pool).

    Sync engines are safe to cache globally — they're not tied to an event loop.
    """
    if connection_string not in _sync_engines:
        _sync_engines[connection_string] = sa.create_engine(
            connection_string, pool_pre_ping=True,
        )
    return _sync_engines[connection_string]


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


def _map_type_code(type_code) -> str:
    """Map a DB-API type_code to a SLayer type category.

    Handles DuckDB (string type names), SQLite (Python types), and
    asyncpg (OID integers) cursor description formats.
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
        if issubclass(type_code, (int, float)):
            return "number"
        if issubclass(type_code, str):
            return "string"
        return "string"
    # asyncpg OIDs, others — default to string
    return "string"


def _extract_types_from_cursor(result) -> Dict[str, str]:
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
            return {col: _map_type_code(tc) for col, tc in zip(columns, type_codes)}

    # Fallback: check Python value types from the first fetched row
    rows = result.fetchall()
    if not rows:
        return {col: "string" for col in columns}  # empty table — safe default
    row = rows[0]
    types = {}
    for col, val in zip(columns, row):
        if val is None:
            types[col] = "string"  # can't infer from NULL
        elif isinstance(val, (int, float)):
            types[col] = "number"
        elif isinstance(val, str):
            types[col] = "string"
        elif hasattr(val, "isoformat"):
            types[col] = "time"
        elif isinstance(val, bool):
            types[col] = "boolean"
        else:
            types[col] = "string"
    return types


def _get_column_types_sync(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
) -> Dict[str, str]:
    """Infer column types. Uses LIMIT 0 for cursor metadata, LIMIT 1 as fallback."""
    engine = _get_sync_engine(connection_string)
    # Try LIMIT 0 first (schema only, no data scan)
    limit_sql = f"SELECT * FROM ({sql}) AS _types LIMIT 1"
    with engine.connect() as conn:
        result = conn.execute(sa.text(limit_sql))
        return _extract_types_from_cursor(result)


async def _get_column_types_async(
    sql: str,
    engine,
    db_type: Optional[str],
) -> Dict[str, str]:
    """Async version of column type inference."""
    limit_sql = f"SELECT * FROM ({sql}) AS _types LIMIT 1"
    async with engine.connect() as conn:
        result = await conn.execute(sa.text(limit_sql))
        return _extract_types_from_cursor(result)


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
        # No async driver — fall back to sync in thread pool
        return await _execute_with_retry_threaded(
            sql=sql,
            connection_string=self.datasource.get_connection_string(),
            db_type=db_type,
            timeout_seconds=timeout_seconds,
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
        return await asyncio.to_thread(
            _get_column_types_sync, sql,
            self.datasource.get_connection_string(), self.datasource.type,
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
        )


# ---------------------------------------------------------------------------
# Native async execution (asyncpg, aiomysql — pooled connections)
# ---------------------------------------------------------------------------


async def _execute_with_retry_async(
    sql: str,
    engine,
    db_type: Optional[str],
    timeout_seconds: int = 120,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> List[Dict[str, Any]]:
    delay = initial_delay
    for attempt in range(max_attempts):
        try:
            return await _execute_sql_async(
                sql=sql,
                engine=engine,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
            )
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError):
            if attempt == max_attempts - 1:
                raise
            logger.warning("Transient DB error on attempt %d, retrying in %.1fs", attempt + 1, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)


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
) -> List[Dict[str, Any]]:
    delay = initial_delay
    for attempt in range(max_attempts):
        try:
            return await asyncio.to_thread(
                _execute_sql_sync,
                sql,
                connection_string,
                db_type,
                timeout_seconds,
            )
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError):
            if attempt == max_attempts - 1:
                raise
            logger.warning("Transient DB error on attempt %d, retrying in %.1fs", attempt + 1, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)


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
) -> List[Dict[str, Any]]:
    delay = initial_delay
    for attempt in range(max_attempts):
        try:
            return _execute_sql_sync(
                sql=sql,
                connection_string=connection_string,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
            )
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError):
            if attempt == max_attempts - 1:
                raise
            logger.warning("Transient DB error on attempt %d, retrying in %.1fs", attempt + 1, delay)
            time.sleep(delay)
            delay = min(delay * 2, max_delay)


def _execute_sql_sync(
    sql: str,
    connection_string: str,
    db_type: Optional[str],
    timeout_seconds: int = 120,
) -> List[Dict[str, Any]]:
    engine = _get_sync_engine(connection_string)
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
