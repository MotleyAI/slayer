"""SQL client for executing queries against databases."""

import asyncio
import concurrent.futures
import contextlib
import functools
import logging
import threading
import time
import warnings as _warnings_module
import weakref
from typing import Any
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

import sqlalchemy as sa
import sqlalchemy.exc
import sqlglot
from pydantic import BaseModel, Field
from sqlglot import expressions as exp
from sqlalchemy.ext.asyncio import create_async_engine

from slayer.core.models import DatasourceConfig
from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning, StatementTimeoutSkippedWarning
from slayer.sql import engine_factory
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.core import timing

logger = logging.getLogger(__name__)

# db_type → async SQLAlchemy scheme; unlisted types fall back to sync-in-thread.
_ASYNC_DRIVERS = {
    "postgres": "postgresql+asyncpg",
    "postgresql": "postgresql+asyncpg",
    "mysql": "mysql+aiomysql",
    "mariadb": "mysql+aiomysql",
}

def _dispose_engine_quietly(engine: sa.Engine) -> None:
    """Dispose a private engine, logging rather than raising (finalizer / close backstop)."""
    try:
        engine.dispose()
    except Exception:  # pragma: no cover - dispose failures are best-effort
        logger.warning("Failed to dispose a private in-memory engine.", exc_info=True)


def _get_async_engine(connection_string: str):
    """Create an async engine (not cached — async engines bind to their event loop)."""
    return create_async_engine(connection_string, pool_pre_ping=True)


def _async_connection_string(connection_string: str, db_type: str | None) -> str | None:
    """Convert a sync connection string to its async equivalent, or None if no async driver."""
    async_scheme = _ASYNC_DRIVERS.get(db_type)
    if async_scheme is None:
        return None
    if "://" in connection_string:
        _, _, remainder = connection_string.partition("://")
        return f"{async_scheme}://{remainder}"
    return None


def _map_type_code(type_code, db_type: str | None = None) -> str:
    """Map a DB-API type_code to a SLayer type category.

    Handles DuckDB (str names), SQLite (Python types), Postgres OIDs, MySQL
    field-types. Dialect map_cursor_type_code (Snowflake) is consulted first.
    """
    if isinstance(type_code, int) and db_type:
        dialect_category = dialect_for_ds_type(db_type).map_cursor_type_code(type_code)
        if dialect_category is not None:
            return dialect_category
    if db_type and "duckdb" in db_type.lower():
        # DuckDB returns DuckDBPyType objects; str() is the type name.
        type_code = str(type_code)
    if isinstance(type_code, str):
        tc = type_code.upper()
        # Check temporal names before numeric ones: INTERVAL contains "INT".
        if any(t in tc for t in ("TIMESTAMP", "DATE", "TIME", "INTERVAL")):
            return "time"
        if any(t in tc for t in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")):
            return "number"
        if any(t in tc for t in ("VARCHAR", "TEXT", "CHAR", "STRING", "BLOB", "ENUM")):
            return "string"
        if "BOOL" in tc:
            return "boolean"
        return "string"
    if isinstance(type_code, type):
        # Check bool before int — bool subclasses int.
        if issubclass(type_code, bool):
            return "boolean"
        if issubclass(type_code, (int, float)):
            return "number"
        if issubclass(type_code, str):
            return "string"
        return "string"
    if isinstance(type_code, int):
        if db_type and "mysql" in db_type.lower():
            return _MYSQL_TYPE_MAP.get(type_code, "string")
        if db_type and any(t in db_type.lower() for t in ("mssql", "sqlserver", "tsql")):
            return _ODBC_SQL_TYPE_MAP.get(type_code, "string")
        # Snowflake already tried above; unrecognised codes default to string
        # rather than misclassifying through the Postgres OID map.
        if db_type and "snowflake" in db_type.lower():
            return "string"
        return _PG_OID_MAP.get(type_code, "string")
    return "string"


# Postgres OIDs (from pg_type)
_PG_OID_MAP: dict[int, str] = {
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
_MYSQL_TYPE_MAP: dict[int, str] = {
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

# ODBC SQL type codes (pyodbc / SQL Server); negatives are SQL_SS_* extensions.
_ODBC_SQL_TYPE_MAP: dict[int, str] = {
    4: "number",      # SQL_INTEGER
    5: "number",      # SQL_SMALLINT
    -6: "number",     # SQL_TINYINT
    -5: "number",     # SQL_BIGINT
    2: "number",      # SQL_NUMERIC
    3: "number",      # SQL_DECIMAL
    6: "number",      # SQL_FLOAT
    7: "number",      # SQL_REAL
    8: "number",      # SQL_DOUBLE
    1: "string",      # SQL_CHAR
    12: "string",     # SQL_VARCHAR
    -1: "string",     # SQL_LONGVARCHAR
    -8: "string",     # SQL_WCHAR
    -9: "string",     # SQL_WVARCHAR
    -10: "string",    # SQL_WLONGVARCHAR
    -152: "string",   # SQL_SS_XML
    -11: "string",    # SQL_GUID (uniqueidentifier)
    -7: "boolean",    # SQL_BIT
    -2: "string",     # SQL_BINARY
    -3: "string",     # SQL_VARBINARY
    -4: "string",     # SQL_LONGVARBINARY
    91: "time",       # SQL_TYPE_DATE
    92: "time",       # SQL_TYPE_TIME
    93: "time",       # SQL_TYPE_TIMESTAMP
    -154: "time",     # SQL_SS_TIMESTAMPOFFSET (datetimeoffset)
    -155: "time",     # SQL_SS_TIME2 (time with fractional seconds)
}


def _extract_types_from_cursor(result, db_type: str | None = None) -> dict[str, str]:
    """Extract {column: type_category} from a CursorResult.

    Uses cursor.description type_codes; falls back to first-row value types.
    """
    columns = list(result.keys())
    cursor_desc = result.cursor.description

    if cursor_desc is not None:
        type_codes = [desc[1] for desc in cursor_desc]
        if any(tc is not None for tc in type_codes):
            return {col: _map_type_code(tc, db_type=db_type) for col, tc in zip(columns, type_codes)}

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
# T-SQL (SQL Server) does not support LIMIT; use SELECT TOP N instead.
_TSQL_DB_TYPES = frozenset({"mssql", "sqlserver", "tsql"})


async def _run_sync_in_thread(func, *args, **kwargs):
    """Run one blocking DB call in a short-lived worker thread.

    Scoped executor, not the default one: pytest-asyncio can hang on default-
    executor threads after SQLite tests; this one shuts down immediately.
    """
    loop = asyncio.get_running_loop()
    call = functools.partial(func, *args, **kwargs)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return await loop.run_in_executor(executor, call)


def _build_type_probe_sql(sql: str, db_type: str | None) -> str:
    """Build a row-limiting probe query appropriate for the target dialect."""
    limit = 1 if db_type in _NEEDS_ROW_FOR_TYPES else 0
    if db_type in _TSQL_DB_TYPES:
        return f"SELECT TOP {limit} * FROM ({sql}) AS _types"
    return f"SELECT * FROM ({sql}) AS _types LIMIT {limit}"


def build_sql_model_trial_query(inner_sql: str) -> str:
    """Wrap raw model SQL in the zero-row trial-execute probe.

    Strips a trailing ``;`` (invalid in a subquery) and puts the inner SQL on
    its own line so a trailing ``-- comment`` can't swallow the closing paren.
    """
    inner = inner_sql.rstrip()
    if inner.endswith(";"):
        inner = inner[:-1].rstrip()
    return f"SELECT * FROM (\n{inner}\n) AS _sd_validate WHERE 1=0"


# A read-only source parses to a query root with no DML/DDL nested (a
# data-modifying CTE hides DML under a SELECT root; COPY/GRANT/CALL/etc. are
# non-query roots the allowlist rejects on its own).
_READ_ONLY_ROOTS = (
    exp.Select, exp.Union, exp.Except, exp.Intersect, exp.Subquery, exp.Values,
)
_DATA_MODIFYING_NODES = (
    exp.Insert, exp.Update, exp.Delete, exp.Merge,
    exp.Create, exp.Drop, exp.Alter, exp.TruncateTable,
    exp.Into,  # SELECT ... INTO new_table creates a table under a SELECT root
    exp.Lock,  # SELECT ... FOR UPDATE / FOR SHARE takes write-intent row locks
    exp.NextValueFor,  # SELECT NEXT VALUE FOR seq advances sequence state
)


def classify_model_sql(sql: str, *, dialect: str | None = None) -> str:
    """'unparseable', 'modifying' (anything not a proven read-only query), or
    'read_only'. Parses like the generator (prequote + parse_one), so SQL it
    rejects is non-functional there too — rejected with no DB round-trip."""
    d = dialect or "postgres"
    try:
        tree = sqlglot.parse_one(prequote_reserved_identifiers(sql, dialect=d), dialect=d)
    except Exception:
        return "unparseable"
    if tree is None:
        return "unparseable"
    if not isinstance(tree, _READ_ONLY_ROOTS):
        return "modifying"
    return "modifying" if tree.find(*_DATA_MODIFYING_NODES) is not None else "read_only"


def _exec_verbatim(conn, sql: str) -> Any:
    """Run rendered SQL verbatim: no bind-param or ``%`` reinterpretation."""
    return conn.exec_driver_sql(sql, execution_options={"no_parameters": True})


async def _exec_verbatim_async(conn, sql: str) -> Any:
    """Async sibling: the one verbatim door for async execution paths."""
    return await conn.exec_driver_sql(sql, execution_options={"no_parameters": True})


# Per-engine answer of the dialect's timeout-permission check, and the lock guarding it.
_timeout_permitted: "weakref.WeakKeyDictionary[Any, bool]" = weakref.WeakKeyDictionary()
_permission_locks: "weakref.WeakKeyDictionary[Any, Any]" = weakref.WeakKeyDictionary()
_permission_locks_guard = threading.Lock()


def _permission_lock(engine: Any, factory: Callable[[], Any]) -> Any:
    with _permission_locks_guard:
        lock = _permission_locks.get(engine)
        if lock is None:
            lock = _permission_locks[engine] = factory()
        return lock


def _skip(datasource_name: str, timeout_seconds: int, reason: str) -> StatementTimeoutSkippedWarning:
    return StatementTimeoutSkippedWarning.model_validate(
        {"datasource": datasource_name, "timeout_seconds": timeout_seconds, "reason": reason},
    )


def _warn_skipped(skipped: list[StatementTimeoutSkippedWarning]) -> None:
    for payload in skipped:
        _warnings_module.warn(SlayerStatementTimeoutSkippedWarning(payload), stacklevel=3)


def _timeout_is_permitted(conn, *, engine: Any, dialect: SqlDialect) -> bool:
    permission_sql = dialect.timeout_permission_sql()
    if permission_sql is None:
        return True
    with _permission_lock(engine, threading.Lock):
        if engine not in _timeout_permitted:
            value = _exec_verbatim(conn, permission_sql).scalar()
            _timeout_permitted[engine] = dialect.timeout_permitted(value)
        return _timeout_permitted[engine]


async def _timeout_is_permitted_async(conn, *, engine: Any, dialect: SqlDialect) -> bool:
    permission_sql = dialect.timeout_permission_sql()
    if permission_sql is None:
        return True
    async with _permission_lock(engine, asyncio.Lock):
        if engine not in _timeout_permitted:
            value = (await _exec_verbatim_async(conn, permission_sql)).scalar()
            _timeout_permitted[engine] = dialect.timeout_permitted(value)
        return _timeout_permitted[engine]


@contextlib.contextmanager
def _statement_timeout(
    conn, *, engine: Any, dialect: SqlDialect, datasource_name: str, timeout_seconds: int,
) -> Iterator[list[StatementTimeoutSkippedWarning]]:
    """Put the dialect's timeout on ``conn`` for the block; yields the skips to report."""
    if not _timeout_is_permitted(conn, engine=engine, dialect=dialect):
        yield [_skip(datasource_name, timeout_seconds, "readonly_user")]
        return
    dbapi_connection = conn.connection.dbapi_connection
    prior = dialect.set_connection_timeout(dbapi_connection, timeout_seconds)
    try:
        skipped: list[StatementTimeoutSkippedWarning] = []
        timeout_sql = dialect.statement_timeout_sql(timeout_seconds)
        if timeout_sql is not None:
            try:
                _exec_verbatim(conn, timeout_sql)
            except sqlalchemy.exc.DBAPIError:
                if not dialect.statement_timeout_best_effort:
                    raise
                conn.rollback()
                skipped.append(_skip(datasource_name, timeout_seconds, "timeout_rejected"))
        yield skipped
    finally:
        dialect.restore_connection_timeout(dbapi_connection, prior)


@contextlib.asynccontextmanager
async def _statement_timeout_async(
    conn, *, engine: Any, dialect: SqlDialect, datasource_name: str, timeout_seconds: int,
) -> AsyncIterator[list[StatementTimeoutSkippedWarning]]:
    """Async sibling of ``_statement_timeout``."""
    if not await _timeout_is_permitted_async(conn, engine=engine, dialect=dialect):
        yield [_skip(datasource_name, timeout_seconds, "readonly_user")]
        return
    dbapi_connection = (await conn.get_raw_connection()).dbapi_connection
    prior = dialect.set_connection_timeout(dbapi_connection, timeout_seconds)
    try:
        skipped: list[StatementTimeoutSkippedWarning] = []
        timeout_sql = dialect.statement_timeout_sql(timeout_seconds)
        if timeout_sql is not None:
            try:
                await _exec_verbatim_async(conn, timeout_sql)
            except sqlalchemy.exc.DBAPIError:
                if not dialect.statement_timeout_best_effort:
                    raise
                await conn.rollback()
                skipped.append(_skip(datasource_name, timeout_seconds, "timeout_rejected"))
        yield skipped
    finally:
        dialect.restore_connection_timeout(dbapi_connection, prior)


# Type probes only compile (LIMIT 0/1); 60s is generous.
_TYPE_PROBE_TIMEOUT_SECONDS = 60


# Dialects whose SET TRANSACTION READ ONLY binds the current txn.
_READ_ONLY_TXN_DIALECTS = frozenset({"postgres", "redshift", "oracle"})


def _read_only_transaction_sql(db_type: str | None) -> str | None:
    if dialect_for_ds_type(db_type).sqlglot_name in _READ_ONLY_TXN_DIALECTS:
        return "SET TRANSACTION READ ONLY"
    return None


def _get_column_types_sync(
    sql: str,
    *,
    db_type: str | None,
    engine: sa.Engine,
    datasource_name: str | None = None,
) -> dict[str, str]:
    """Infer column types read-only (rolled back) so a trial probe can't mutate."""
    limit_sql = _build_type_probe_sql(sql, db_type)
    with engine.connect() as conn, _statement_timeout(
        conn, engine=engine, dialect=dialect_for_ds_type(db_type),
        datasource_name=datasource_name or str(engine.url), timeout_seconds=_TYPE_PROBE_TIMEOUT_SECONDS,
    ) as skipped:
        ro_sql = _read_only_transaction_sql(db_type)
        if ro_sql:
            _exec_verbatim(conn, ro_sql)
        result = _exec_verbatim(conn=conn, sql=limit_sql)
        types = _extract_types_from_cursor(result, db_type=db_type)
        conn.rollback()
    _warn_skipped(skipped)
    return types


def get_column_types_sync(
    sql: str, *, engine: sa.Engine, db_type: str | None = None, datasource_name: str | None = None,
) -> dict[str, str]:
    """Public sync column-type inference over an existing engine (stable entry point)."""
    return _get_column_types_sync(sql, db_type=db_type, engine=engine, datasource_name=datasource_name)


async def _get_column_types_async(
    sql: str,
    engine,
    db_type: str | None,
    datasource_name: str | None = None,
) -> dict[str, str]:
    """Async type inference read-only (rolled back) so a trial probe can't mutate."""
    limit_sql = _build_type_probe_sql(sql, db_type)
    async with engine.connect() as conn, _statement_timeout_async(
        conn, engine=engine, dialect=dialect_for_ds_type(db_type),
        datasource_name=datasource_name or str(engine.url), timeout_seconds=_TYPE_PROBE_TIMEOUT_SECONDS,
    ) as skipped:
        ro_sql = _read_only_transaction_sql(db_type)
        if ro_sql:
            await _exec_verbatim_async(conn, ro_sql)
        result = await _exec_verbatim_async(conn, limit_sql)
        types = _extract_types_from_cursor(result, db_type=db_type)
        await conn.rollback()
    _warn_skipped(skipped)
    return types


class ExecutionResult(BaseModel):
    """Rows of one executed statement, plus the timeout skips it ran under."""

    rows: list[dict[str, Any]]
    warnings: list[StatementTimeoutSkippedWarning] = Field(default_factory=list)


class SlayerSQLClient:
    """Executes SQL against databases via SQLAlchemy.

    Native async drivers (asyncpg, aiomysql) when available, else sync-in-thread.
    The async engine is cached per instance (bound to the current event loop).
    """

    def __init__(self, datasource: DatasourceConfig):
        self.datasource = datasource
        self._async_engine = None
        self._sync_engine: sa.Engine | None = None
        # Set only when ``_sync_engine`` is this client's PRIVATE in-memory
        # engine (built off the factory's StaticPool builder). A factory-owned
        # engine is disposed by the factory, never here.
        self._owns_sync_engine = False
        # weakref backstop: disposes the private engine if the client is
        # collected without ``close()`` (D4). Detached whenever we dispose it.
        self._sync_engine_finalizer: weakref.finalize | None = None

    def close(self) -> None:
        """Dispose this client's private in-memory engine (idempotent).

        The synchronous teardown: a factory-owned engine is left to the factory.
        An async engine is event-loop-bound and can only be disposed by
        ``aclose()`` inside its loop — if one is still live here, warn rather
        than drop it silently (a sync ``close()`` cannot dispose it).
        """
        self._dispose_private_sync_engine()
        if self._async_engine is not None:
            logger.warning(
                "SlayerSQLClient.close() cannot dispose the loop-bound async "
                "engine for datasource %r; await aclose() in its event loop.",
                self.datasource.name,
            )

    def _dispose_private_sync_engine(self) -> bool:
        """Dispose the private in-memory engine (detaching its finalizer) and
        forget it; return whether there was one. Idempotent; a factory-owned
        engine is left untouched."""
        finalizer = self._sync_engine_finalizer
        if self._owns_sync_engine and self._sync_engine is not None:
            engine = self._sync_engine
            self._sync_engine = None
            self._owns_sync_engine = False
            self._sync_engine_finalizer = None
            if finalizer is not None:
                finalizer.detach()
            _dispose_engine_quietly(engine)
            return True
        return False

    async def aclose(self) -> None:
        """Dispose the cached async engine inside the current event loop."""
        engine = self._async_engine
        if engine is None:
            return
        # Null first so a failed dispose can't leave a half-torn engine cached.
        self._async_engine = None
        try:
            await engine.dispose()
        except Exception as exc:  # pragma: no cover
            import logging
            logging.getLogger(__name__).warning(
                "Async engine dispose failed for datasource %r: %s",
                self.datasource.name, exc,
            )

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

    def _get_sync_engine_for_client(self) -> sa.Engine:
        """Return a per-client sync engine.

        In-memory SQLite gets a private StaticPool engine (isolated per client),
        owned and disposed here with a finalizer backstop; everything else
        delegates to engine_factory, which owns and disposes it.
        """
        if self._sync_engine is not None:
            return self._sync_engine
        conn_str = self.datasource.get_connection_string()
        if engine_factory._is_in_memory_sqlite(conn_str):
            engine = engine_factory.build_in_memory_sqlite_engine(conn_str)
            self._sync_engine = engine
            self._owns_sync_engine = True
            self._sync_engine_finalizer = weakref.finalize(
                self, _dispose_engine_quietly, engine,
            )
            return engine
        self._sync_engine = engine_factory.get_engine(self.datasource)
        return self._sync_engine

    def _discard_sync_engine_on_auth_failure(self, exc: BaseException) -> bool:
        """Drop the sync engine on a credential rejection; return whether exc was one.

        Credentials are fixed at construction, so a revoked grant poisons the
        engine permanently — dispose a private one, evict a factory one.
        Best-effort; never displace exc.
        """
        if not _is_auth_failure(exc):
            return False
        if not self._dispose_private_sync_engine():
            self._sync_engine = None
            try:
                engine_factory.invalidate_engine(self.datasource)
            except Exception:
                logger.warning(
                    "Failed to invalidate engine for datasource %r after an "
                    "authentication failure.", self.datasource.name, exc_info=True,
                )
        return True

    async def _discard_engines_on_auth_failure(self, exc: BaseException) -> None:
        """Async-path cleanup: the sync engine plus this client's async pool.

        Disposing the async pool needs a loop — hence the split from execute_sync.
        """
        if not self._discard_sync_engine_on_auth_failure(exc):
            return
        await self.aclose()

    async def execute(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> ExecutionResult:
        """Execute SQL asynchronously."""
        try:
            return await self._execute(sql=sql, timeout_seconds=timeout_seconds)
        except Exception as exc:
            await self._discard_engines_on_auth_failure(exc)
            raise

    async def _execute(
        self,
        *,
        sql: str,
        timeout_seconds: int,
    ) -> ExecutionResult:
        async_engine = self._get_async_engine()
        db_type = self.datasource.type
        if async_engine is not None:
            return await _execute_with_retry_async(
                sql=sql,
                engine=async_engine,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
                datasource_name=self.datasource.name,
            )
        # No async driver — fall back to sync in thread pool
        return await _execute_with_retry_threaded(
            sql=sql,
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=self._get_sync_engine_for_client(),
            datasource_name=self.datasource.name,
        )

    async def get_column_types(self, sql: str) -> dict[str, str]:
        """Infer column types via LIMIT 0; returns {column: number|string|time|boolean}."""
        try:
            return await self._get_column_types(sql=sql)
        except Exception as exc:
            await self._discard_engines_on_auth_failure(exc)
            raise

    async def _get_column_types(self, *, sql: str) -> dict[str, str]:
        async_engine = self._get_async_engine()
        if async_engine is not None:
            return await _get_column_types_async(
                sql=sql, engine=async_engine, db_type=self.datasource.type,
                datasource_name=self.datasource.name,
            )
        return await _run_sync_in_thread(
            _get_column_types_sync,
            sql=sql,
            db_type=self.datasource.type,
            engine=self._get_sync_engine_for_client(),
            datasource_name=self.datasource.name,
        )

    def execute_sync(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> ExecutionResult:
        """Execute SQL synchronously (CLI, notebooks, tests).

        Discards only the sync engine on auth failure — no loop for the async pool.
        """
        try:
            return _execute_with_retry_sync(
                sql=sql,
                db_type=self.datasource.type,
                timeout_seconds=timeout_seconds,
                engine=self._get_sync_engine_for_client(),
                datasource_name=self.datasource.name,
            )
        except Exception as exc:
            self._discard_sync_engine_on_auth_failure(exc)
            raise


# Placeholder in the retry warning when the SQL is empty/whitespace.
_EMPTY_SQL_PLACEHOLDER = "<empty sql>"

# Retry-warning format: attempt (1-based), delay, DBAPI exception, SQL excerpt.
_TRANSIENT_RETRY_LOG_FORMAT = (
    "Transient DB error on attempt %d, retrying in %.1fs: %s | sql: %s"
)

# Lower-cased DBAPI-message substrings marking a transient failure worth a
# retry. Deterministic schema errors are excluded — sleeping wouldn't help.
_TRANSIENT_DB_ERROR_SIGNALS = (
    "database is locked",     # SQLite under contention
    "deadlock",               # Postgres / MySQL deadlock_detected
    "lost connection",        # MySQL "Lost connection to MySQL server"
    "broken pipe",            # connection mid-query
    "could not connect",      # libpq / psycopg
    "server closed",          # Postgres "server closed the connection unexpectedly"
    "connection refused",
    "connection reset",
    "connection was killed",  # MySQL admin kill
)


def _is_transient_db_error(exc: BaseException) -> bool:
    """True only for DB errors worth retrying.

    OperationalError is too broad (spans schema errors); match transient signals
    in exc.orig. DisconnectionError is always transient.
    """
    if isinstance(exc, sqlalchemy.exc.DisconnectionError):
        return True
    msg = str(getattr(exc, "orig", exc)).lower()
    return any(sig in msg for sig in _TRANSIENT_DB_ERROR_SIGNALS)


# Credential-rejection signals (distinct from transient): retrying is pointless,
# so evict the engine. Narrow — table-level "permission denied" excluded.
_AUTH_ERROR_SIGNALS = (
    "invalid_grant",                      # OAuth refresh token revoked / expired
    "invalid_client",
    "unauthorized_client",
    "token has been expired or revoked",
    "could not refresh access token",
    "reauthentication is needed",
    "authentication failed",
    "password authentication failed",     # libpq
    "invalid credentials",
    "invalid username or password",
)

# Matched by class name (dependency-free — google-auth is an optional extra).
_AUTH_ERROR_TYPE_NAMES = frozenset({
    "RefreshError",             # google.auth.exceptions
    "DefaultCredentialsError",  # google.auth.exceptions
    "Unauthorized",             # google.api_core.exceptions — HTTP 401
})


def _is_auth_failure(exc: BaseException) -> bool:
    """True when the server rejected the credentials themselves (walks orig/cause/context)."""
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if type(current).__name__ in _AUTH_ERROR_TYPE_NAMES:
            return True
        text = str(current).lower()
        if any(signal in text for signal in _AUTH_ERROR_SIGNALS):
            return True
        pending.extend(
            nested for nested in (
                getattr(current, "orig", None),
                current.__cause__,
                current.__context__,
            )
            if isinstance(nested, BaseException)
        )
    return False


# Connect-phase failure signals — narrow "couldn't reach the backend at all",
# so reached-backend rejections stay rejectable for save-time validation.
_UNREACHABLE_DB_ERROR_SIGNALS = (
    "could not connect",              # libpq / psycopg connect phase
    "can't connect",                  # MySQL 2002/2003 connect phase (incl. "(timed out)")
    "connection refused",
    "connection to server at",        # libpq host:port dial failure
    "getaddrinfo",                    # host-name resolution failure
    "unable to open database file",   # SQLite path unreachable
    "login timeout",                  # ODBC / SQL Server connect-phase timeout
)

# Matched by class name (dependency-free). Transport/availability failures where
# the backend was never reached; blanket InterfaceError excluded on purpose.
_UNREACHABLE_DB_ERROR_TYPE_NAMES = frozenset({
    "ServiceUnavailable",   # google.api_core / neo4j — HTTP 503
    "GatewayTimeout",       # HTTP 504
    "TooManyRequests",      # HTTP 429 — backend refusing new work
    "DeadlineExceeded",     # google.api_core — deadline before a reply
    "ConnectionError",      # transport-layer dial failure
})


def _is_unreachable_db_error(exc: BaseException) -> bool:
    """True when the datasource could not be reached at all (walks orig/cause/context)."""
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, sqlalchemy.exc.DisconnectionError):
            return True
        if type(current).__name__ in _UNREACHABLE_DB_ERROR_TYPE_NAMES:
            return True
        text = str(current).lower()
        if any(signal in text for signal in _UNREACHABLE_DB_ERROR_SIGNALS):
            return True
        pending.extend(
            nested for nested in (
                getattr(current, "orig", None),
                current.__cause__,
                current.__context__,
            )
            if isinstance(nested, BaseException)
        )
    return False


async def _retry_with_backoff(
    *,
    sql: str,
    do_call: Callable[[], Awaitable[ExecutionResult]],
    max_attempts: int,
    initial_delay: float,
    max_delay: float,
) -> ExecutionResult:
    """Retry an async DB call with exponential backoff on transient errors.

    `sql` is only the warning excerpt; the DBAPI message comes from exc.orig.
    """
    _check_max_attempts(max_attempts)
    delay = initial_delay
    for attempt in range(max_attempts - 1):
        try:
            return await do_call()
        except _RETRYABLE_ERRORS as exc:
            _log_transient_or_raise(exc=exc, sql=sql, attempt=attempt, delay=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
    return await do_call()


def _check_max_attempts(max_attempts: int) -> None:
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")


_RETRYABLE_ERRORS = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError)


def _log_transient_or_raise(*, exc: Exception, sql: str, attempt: int, delay: float) -> None:
    """Re-raise a non-transient error; log the retry of a transient one."""
    if not _is_transient_db_error(exc):
        raise exc
    sql_lines = (sql or "").strip().splitlines()
    sql_excerpt = sql_lines[0][:120] if sql_lines else _EMPTY_SQL_PLACEHOLDER
    logger.warning(
        _TRANSIENT_RETRY_LOG_FORMAT,
        attempt + 1, delay, getattr(exc, "orig", exc), sql_excerpt,
    )


async def _execute_with_retry_async(
    sql: str,
    engine,
    db_type: str | None,
    timeout_seconds: int = 120,
    datasource_name: str | None = None,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> ExecutionResult:
    return await _retry_with_backoff(
        sql=sql,
        do_call=lambda: _execute_sql_async(
            sql=sql, engine=engine, db_type=db_type, timeout_seconds=timeout_seconds,
            datasource_name=datasource_name,
        ),
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
    )


async def _execute_sql_async(
    sql: str,
    engine,
    db_type: str | None,
    timeout_seconds: int = 120,
    datasource_name: str | None = None,
) -> ExecutionResult:
    _t = timing.start()
    async with engine.connect() as conn:
        timing.record("connect", _t)
        _t = timing.start()
        async with _statement_timeout_async(
            conn, engine=engine, dialect=dialect_for_ds_type(db_type),
            datasource_name=datasource_name or str(engine.url), timeout_seconds=timeout_seconds,
        ) as skipped:
            timing.record("set_timeout", _t)
            _t = timing.start()
            result = await _exec_verbatim_async(conn, sql)
            rows = _fetch_rows(result)
            timing.record("query", _t)
    _warn_skipped(skipped)
    return ExecutionResult(rows=rows, warnings=skipped)


async def _execute_with_retry_threaded(
    sql: str,
    db_type: str | None,
    *,
    engine: sa.Engine,
    timeout_seconds: int = 120,
    datasource_name: str | None = None,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> ExecutionResult:
    return await _retry_with_backoff(
        sql=sql,
        do_call=lambda: _run_sync_in_thread(
            _execute_sql_sync,
            sql=sql,
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=engine,
            datasource_name=datasource_name,
        ),
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
    )


def _execute_with_retry_sync(
    sql: str,
    db_type: str | None,
    *,
    engine: sa.Engine,
    timeout_seconds: int = 120,
    datasource_name: str | None = None,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> ExecutionResult:
    _check_max_attempts(max_attempts)

    def _call() -> ExecutionResult:
        return _execute_sql_sync(
            sql=sql,
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=engine,
            datasource_name=datasource_name,
        )

    delay = initial_delay
    for attempt in range(max_attempts - 1):
        try:
            return _call()
        except _RETRYABLE_ERRORS as exc:
            _log_transient_or_raise(exc=exc, sql=sql, attempt=attempt, delay=delay)
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return _call()


def _execute_sql_sync(
    sql: str,
    db_type: str | None,
    *,
    engine: sa.Engine,
    timeout_seconds: int = 120,
    datasource_name: str | None = None,
) -> ExecutionResult:
    with engine.connect() as conn, _statement_timeout(
        conn, engine=engine, dialect=dialect_for_ds_type(db_type),
        datasource_name=datasource_name or str(engine.url), timeout_seconds=timeout_seconds,
    ) as skipped:
        rows = _fetch_rows(_exec_verbatim(conn=conn, sql=sql))
    _warn_skipped(skipped)
    return ExecutionResult(rows=rows, warnings=skipped)


def _fetch_rows(result) -> list[dict[str, Any]]:
    columns = list(result.keys())
    return [dict(zip(columns, row)) for row in result.fetchall()]
