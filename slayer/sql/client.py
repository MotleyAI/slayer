"""SQL clients: the ``SqlClient`` contract, its default implementation, and a delegating base."""

import asyncio
import concurrent.futures
import contextlib
import functools
import logging
import os
import re
import threading
import time
import warnings as _warnings_module
import weakref
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol, TypeVar, runtime_checkable
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

import sqlalchemy as sa
import sqlalchemy.exc
import sqlglot
from pydantic import BaseModel, Field
from sqlglot import expressions as exp
from sqlglot.dialects.dialect import Dialect as SqlglotDialect
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expr
from sqlglot.tokenizer_core import TokenType
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from slayer import async_utils
from slayer.async_utils import on_caller_thread, run_sync
from slayer.core.deprecation import warn_deprecated
from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning, StatementTimeoutSkippedWarning
from slayer.sql import connection_policy, engine_factory
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.dialects.base import ServerProfile, describe_type_name_code
from slayer.sql.dialects.drivers import load_driver, plugin_errors
from slayer.sql.dialects.reflection import (
    LiveName,
    ReflectedColumn,
    ResultColumnType,
    TableKeys,
    TableLocation,
    clean_comment,
    recover,
    schema_token,
)
from slayer.sql.render.parse import is_read_only_query, parse_query
from slayer.sql.reserved_keywords import prequote_reserved_identifiers
from slayer.sql.type_probe import TypeProbe, mint_type_probe
from slayer.core import timing

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_ENV_REF = re.compile(r"\$\{\w+\}")

# Who disposes a client's sync engine: the caller who passed it, the client (private in-memory), or the factory.
EngineOwner = Literal["caller", "client", "factory"]


class SqlClientError(Exception):
    """Base of the typed client failures."""


class DatabaseUnavailable(SqlClientError):
    """The database could not be reached."""


class AuthenticationRefused(SqlClientError):
    """The database rejected the client's credentials."""


class ObjectNotFound(SqlClientError):
    """A metadata read named a schema or table that does not exist."""


class SessionStatementRefused(SqlClientError):
    """``execute`` was given a statement that changes session state or controls transactions."""


# Leading keywords of a statement the default client refuses.
_SESSION_LEADS = (
    ("ALTER", "SESSION"), ("START", "TRANSACTION"),
    ("USE",), ("SET",), ("RESET",), ("DISCARD",), ("BEGIN",), ("COMMIT",), ("ROLLBACK",),
    ("END",), ("ABORT",), ("SAVEPOINT",), ("RELEASE",), ("SAVE",), ("XA",),
)

# Leading keywords of a statement that only reads.
_READ_LEADS = frozenset({"SELECT", "WITH", "VALUES", "TABLE", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "("})

# Tokens that are never keywords: literals and quoted names.
_LITERAL_TOKENS = frozenset({
    TokenType.STRING, TokenType.NATIONAL_STRING, TokenType.RAW_STRING, TokenType.BIT_STRING,
    TokenType.HEX_STRING, TokenType.BYTE_STRING, TokenType.HEREDOC_STRING, TokenType.UNICODE_STRING,
    TokenType.IDENTIFIER,
})


# Keywords that make a statement leading with a read keyword (a ``WITH``) write.
_WRITE_TOKENS = frozenset({TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE, TokenType.MERGE})


def _statement_leads(sql: str, *, dialect: SqlDialect) -> list[tuple[tuple[str, ...], bool]]:
    """Each ``;``-separated statement's first two words, upper-cased (a literal or quoted name reads as ``""``),
    and whether it holds a write keyword."""
    statements: list[tuple[list[str], list[bool]]] = [([], [False])]
    for token in SqlglotDialect.get_or_raise(dialect.sqlglot_name).tokenize(sql):
        if token.token_type is TokenType.SEMICOLON:
            statements.append(([], [False]))
            continue
        words, writes = statements[-1]
        if len(words) < 2:
            words.append("" if token.token_type in _LITERAL_TOKENS else token.text.upper())
        if token.token_type in _WRITE_TOKENS:
            writes[0] = True
    return [(tuple(words), writes[0]) for words, writes in statements if words]


def admit_statement(sql: str, *, dialect: SqlDialect) -> bool:
    """Refuse session and transaction statements in ``sql``; return whether it does anything but read."""
    try:
        leads = _statement_leads(sql, dialect=dialect)
    except SqlglotError as exc:
        raise SessionStatementRefused(f"The SQL client cannot split this text into statements: {exc}") from exc
    for lead, _writes in leads:
        refused = next((words for words in _SESSION_LEADS if lead[:len(words)] == words), None)
        if refused is not None:
            raise SessionStatementRefused(
                f"The SQL client refuses {' '.join(refused)} statements: session state and transactions "
                "are managed by SLayer.",
            )
    return any(writes or lead[0] not in _READ_LEADS for lead, writes in leads)


class UnparseableTypeProbeError(ValueError):
    """The SQL whose result columns were asked for could not be parsed, so no type probe was built."""

    def __init__(self, *, sql: str, dialect: str) -> None:
        super().__init__(f"Cannot type the result of this {dialect} statement; it does not parse: {sql!r}")
        self.sql = sql


class ObjectListing(BaseModel):
    """The relations of one schema as the database lists them by kind (a name may repeat across kinds), in
    SQLAlchemy's spelling."""

    tables: list[LiveName] = Field(default_factory=list)
    views: list[LiveName] = Field(default_factory=list)
    materialized_views: list[LiveName] = Field(default_factory=list)


def _dispose_engine_quietly(engine: sa.Engine) -> None:
    """Dispose a private engine, logging rather than raising (finalizer / close backstop)."""
    try:
        engine.dispose()
    except Exception:  # pragma: no cover - dispose failures are best-effort
        logger.warning("Failed to dispose a private in-memory engine.", exc_info=True)


def _release_bridge_engine(engine: AsyncEngine, token: int) -> None:
    """Dispose a bridge-loop async engine on that loop, unless bridge shutdown already did."""
    if async_utils.unregister_shutdown(token):
        async_utils.call_on_bridge(engine.dispose)


def _forget_bridge_release(release: weakref.finalize) -> None:
    """Cancel a pending ``_release_bridge_engine``: its engine is being disposed on the bridge loop now."""
    detached = release.detach()
    if detached is not None:
        _engine, token = detached[2]
        async_utils.unregister_shutdown(token)


def _abandon(engines: list[AsyncEngine]) -> None:
    """Drop engines whose event loop has closed: their pools are dereferenced, never touched from this loop."""
    for engine in engines:
        try:
            engine.sync_engine.dispose(close=False)
        except Exception:  # pragma: no cover - best-effort
            logger.warning("Failed to abandon a closed loop's async engine.", exc_info=True)


_TEXT = ResultColumnType(type=DataType.TEXT)


def _first_value_type(value: Any) -> ResultColumnType:
    """A column typed off one value; a number is uncertified, so DOUBLE."""
    if isinstance(value, bool):
        return ResultColumnType(type=DataType.BOOLEAN)
    if isinstance(value, (list, tuple)):
        return ResultColumnType(type=DataType.ARRAY)
    if isinstance(value, (int, float, Decimal)):
        return ResultColumnType(type=DataType.DOUBLE)
    if isinstance(value, datetime):
        return ResultColumnType(type=DataType.TIMESTAMP)
    if isinstance(value, date):
        return ResultColumnType(type=DataType.DATE)
    return _TEXT


def _types_from_cursor(result: Any, *, dialect: SqlDialect) -> dict[str, ResultColumnType]:
    """Per column: the dialect's reading of its description row, the type-name reading, then TEXT;
    a column the description gives no type code is typed off the first row."""
    columns = list(result.keys())
    description = result.cursor.description or [None] * len(columns)
    types: dict[str, ResultColumnType | None] = {}
    for name, row in zip(columns, description):
        if row is None or row[1] is None:
            types[name] = None
        else:
            types[name] = dialect.describe_cursor_column(row) or describe_type_name_code(row) or _TEXT
    if any(t is None for t in types.values()):
        rows = result.fetchall()
        first = dict(zip(columns, rows[0])) if rows else {}
        types = {name: t or _first_value_type(first.get(name)) for name, t in types.items()}
    return {name: t or _TEXT for name, t in types.items()}


def type_categories(types: dict[str, ResultColumnType], *, dialect: SqlDialect) -> dict[str, str]:
    """The 1.6.0 categories (``number`` / ``string`` / ``time`` / ``boolean``) of typed probe results."""
    return {name: dialect.legacy_type_categories.category(column) for name, column in types.items()}


async def _run_sync_in_thread(func, *args, **kwargs):
    """Run one blocking DB call in a short-lived worker thread.

    Scoped executor, not the default one: pytest-asyncio can hang on default-
    executor threads after SQLite tests; this one shuts down immediately.
    Cancellation never waits: the worker is abandoned and finishes on its own.
    """
    loop = asyncio.get_running_loop()
    call = functools.partial(func, *args, **kwargs)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="slayer-blocking")
    cancelled = False
    try:
        return await loop.run_in_executor(executor, call)
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        executor.shutdown(wait=not cancelled, cancel_futures=cancelled)


def render_type_probe(query: Expr, *, dialect: SqlDialect) -> TypeProbe:
    """``query`` bounded by ``dialect.type_probe`` and rendered once."""
    return mint_type_probe(dialect.type_probe(query), dialect=dialect,
                           render=lambda tree: tree.sql(dialect=dialect.sqlglot_name))


def _rows_as_dicts(result: Any) -> list[dict[str, Any]]:
    keys = list(result.keys())
    return [dict(zip(keys, row)) for row in result.fetchall()]


def sql_model_trial_query(sql: str, *, dialect: SqlDialect) -> exp.Select:
    """The zero-row trial of read-only model ``sql``; anything else raises."""
    inner = parse_query(sql=sql, target_dialect=dialect)
    return exp.select("*").from_(inner.subquery("_sd_validate")).where(
        exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(0)))


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
    return "read_only" if is_read_only_query(tree) else "modifying"


def _exec_verbatim(conn, sql: str) -> Any:
    """Run rendered SQL verbatim: no bind-param or ``%`` reinterpretation."""
    return conn.exec_driver_sql(sql, execution_options={"no_parameters": True})


async def _exec_verbatim_async(conn, sql: str) -> Any:
    """Async sibling: the one verbatim door for async execution paths."""
    return await conn.exec_driver_sql(sql, execution_options={"no_parameters": True})


# Per-engine server profile, and the lock guarding its fill. A failed fill is never cached.
_server_profiles: "weakref.WeakKeyDictionary[Any, ServerProfile]" = weakref.WeakKeyDictionary()
_profile_locks: "weakref.WeakKeyDictionary[Any, Any]" = weakref.WeakKeyDictionary()
_profile_locks_guard = threading.Lock()


def _fresh_profile_locks() -> None:
    """In a forked child: the parent's profile locks may be held, so start over."""
    global _profile_locks_guard
    _profile_locks_guard = threading.Lock()
    _profile_locks.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_fresh_profile_locks)


def _profile_lock(engine: Any, factory: Callable[[], Any]) -> Any:
    with _profile_locks_guard:
        lock = _profile_locks.get(engine)
        if lock is None:
            lock = _profile_locks[engine] = factory()
        return lock


def _skip(datasource_name: str, timeout_seconds: int, reason: str) -> StatementTimeoutSkippedWarning:
    """Build the skip payload and emit its Python warning now, before the statement runs."""
    payload = StatementTimeoutSkippedWarning.model_validate(
        {"datasource": datasource_name, "timeout_seconds": timeout_seconds, "reason": reason},
    )
    _warnings_module.warn(SlayerStatementTimeoutSkippedWarning(payload), stacklevel=2)
    return payload


def _first_row(result: Any) -> tuple | None:
    row = result.first()
    return None if row is None else tuple(row)


def _correlated_probe_failed(exc: Exception) -> None:
    logger.warning("Correlated-subquery setting probe failed; treating it as unknown: %s", exc)


def _server_profile(conn, *, engine: Any, dialect: SqlDialect) -> ServerProfile:
    """The engine's cached profile, filled over ``conn`` on first use (verbatim, never via ``execute``)."""
    profile_sql = dialect.server_profile_sql()
    if profile_sql is None:
        return ServerProfile()
    with _profile_lock(engine, threading.Lock):
        cached = _server_profiles.get(engine)
        if cached is not None:
            return cached
        base_row = _first_row(_exec_verbatim(conn, profile_sql))
        profile = dialect.parse_server_profile(base_row=base_row)
        correlated = dialect.correlated_setting_probe(profile)
        if correlated is not None:
            correlated_sql = correlated.sql(dialect=dialect.sqlglot_name)
            try:
                correlated_row = _first_row(_exec_verbatim(conn, correlated_sql))
            except Exception as exc:  # the driver's own error types need not be DBAPI errors
                _correlated_probe_failed(exc)
                return profile
            profile = dialect.parse_server_profile(base_row=base_row, correlated_row=correlated_row)
        _server_profiles[engine] = profile
        return profile


async def _server_profile_async(conn, *, engine: Any, dialect: SqlDialect) -> ServerProfile:
    """Async sibling of ``_server_profile``."""
    profile_sql = dialect.server_profile_sql()
    if profile_sql is None:
        return ServerProfile()
    async with _profile_lock(engine, asyncio.Lock):
        cached = _server_profiles.get(engine)
        if cached is not None:
            return cached
        base_row = _first_row(await _exec_verbatim_async(conn, profile_sql))
        profile = dialect.parse_server_profile(base_row=base_row)
        correlated = dialect.correlated_setting_probe(profile)
        if correlated is not None:
            correlated_sql = correlated.sql(dialect=dialect.sqlglot_name)
            try:
                correlated_row = _first_row(await _exec_verbatim_async(conn, correlated_sql))
            except Exception as exc:  # the driver's own error types need not be DBAPI errors
                _correlated_probe_failed(exc)
                return profile
            profile = dialect.parse_server_profile(base_row=base_row, correlated_row=correlated_row)
        _server_profiles[engine] = profile
        return profile


def _server_profile_sync(*, engine: sa.Engine, dialect: SqlDialect) -> ServerProfile:
    with engine.connect() as conn:
        return _server_profile(conn, engine=engine, dialect=dialect)


def _timeout_sql(dialect: SqlDialect, timeout_seconds: int, *, transaction_local: bool) -> str | None:
    tree = (dialect.transaction_local_statement_timeout(timeout_seconds) if transaction_local
            else dialect.statement_timeout(timeout_seconds))
    return None if tree is None else tree.sql(dialect=dialect.sqlglot_name)


def _timeout_in_force(info: Any, timeout_seconds: int) -> bool | None:
    """Whether the session's timeout is ``timeout_seconds`` (``True``: rejected at that value), or ``None``."""
    record = info.get(connection_policy.TIMEOUT_IN_FORCE)
    if isinstance(record, tuple) and record[0] == timeout_seconds:
        return record[1]
    return None


def _timeout_plan(
    conn, *, dialect: SqlDialect, timeout_seconds: int, transaction_local: bool,
) -> tuple[str | None, bool]:
    """The timeout statement to send (``None`` when the session already has the value), and whether the
    session rejected this value before."""
    sql = _timeout_sql(dialect, timeout_seconds, transaction_local=transaction_local)
    if sql is None or transaction_local:
        return sql, False
    rejected = _timeout_in_force(conn.info, timeout_seconds)
    if rejected is None:
        conn.info.pop(connection_policy.TIMEOUT_IN_FORCE, None)
        return sql, False
    return None, rejected


def _timeout_outcome(
    conn, *, sent: bool, rejected: bool, transaction_local: bool, timeout_seconds: int, datasource_name: str,
) -> list[StatementTimeoutSkippedWarning]:
    """Record a sent session timeout on the connection; return the skips to report."""
    if sent and not transaction_local:
        conn.info[connection_policy.TIMEOUT_IN_FORCE] = (timeout_seconds, rejected)
    return [_skip(datasource_name, timeout_seconds, "timeout_rejected")] if rejected else []


def _set_timeout(
    conn, *, dialect: SqlDialect, datasource_name: str, timeout_seconds: int, transaction_local: bool,
) -> list[StatementTimeoutSkippedWarning]:
    """Send the timeout statement unless the session already has it; return the skips to report."""
    sql, rejected = _timeout_plan(
        conn, dialect=dialect, timeout_seconds=timeout_seconds, transaction_local=transaction_local,
    )
    if sql is not None:
        try:
            _exec_verbatim(conn, sql)
        except sqlalchemy.exc.DBAPIError as exc:
            if exc.connection_invalidated or not dialect.statement_timeout_best_effort:
                raise
            rejected = True
            if transaction_local:
                conn.rollback()
    return _timeout_outcome(
        conn, sent=sql is not None, rejected=rejected, transaction_local=transaction_local,
        timeout_seconds=timeout_seconds, datasource_name=datasource_name,
    )


async def _set_timeout_async(
    conn, *, dialect: SqlDialect, datasource_name: str, timeout_seconds: int, transaction_local: bool,
) -> list[StatementTimeoutSkippedWarning]:
    """Async sibling of ``_set_timeout``."""
    sql, rejected = _timeout_plan(
        conn, dialect=dialect, timeout_seconds=timeout_seconds, transaction_local=transaction_local,
    )
    if sql is not None:
        try:
            await _exec_verbatim_async(conn, sql)
        except sqlalchemy.exc.DBAPIError as exc:
            if exc.connection_invalidated or not dialect.statement_timeout_best_effort:
                raise
            rejected = True
            if transaction_local:
                await conn.rollback()
    return _timeout_outcome(
        conn, sent=sql is not None, rejected=rejected, transaction_local=transaction_local,
        timeout_seconds=timeout_seconds, datasource_name=datasource_name,
    )


@contextlib.contextmanager
def _statement_timeout(
    conn, *, engine: Any, dialect: SqlDialect, datasource_name: str, timeout_seconds: int,
    transaction_local: bool = False,
) -> Iterator[list[StatementTimeoutSkippedWarning]]:
    """Put the dialect's timeout on ``conn`` for the block (a session statement once per value); yields the skips."""
    if dialect.timeout_reads_profile and not dialect.timeout_permitted(_server_profile(conn, engine=engine, dialect=dialect)):
        yield [_skip(datasource_name, timeout_seconds, "readonly_user")]
        return
    dbapi_connection = conn.connection.dbapi_connection
    prior = dialect.set_connection_timeout(dbapi_connection, timeout_seconds)
    try:
        yield _set_timeout(
            conn, dialect=dialect, datasource_name=datasource_name, timeout_seconds=timeout_seconds,
            transaction_local=transaction_local,
        )
    finally:
        dialect.restore_connection_timeout(dbapi_connection, prior)


@contextlib.asynccontextmanager
async def _statement_timeout_async(
    conn, *, engine: Any, dialect: SqlDialect, datasource_name: str, timeout_seconds: int,
    transaction_local: bool = False,
) -> AsyncIterator[list[StatementTimeoutSkippedWarning]]:
    """Async sibling of ``_statement_timeout``."""
    if dialect.timeout_reads_profile and not dialect.timeout_permitted(
        await _server_profile_async(conn, engine=engine, dialect=dialect),
    ):
        yield [_skip(datasource_name, timeout_seconds, "readonly_user")]
        return
    dbapi_connection = (await conn.get_raw_connection()).dbapi_connection
    prior = dialect.set_connection_timeout(dbapi_connection, timeout_seconds)
    try:
        yield await _set_timeout_async(
            conn, dialect=dialect, datasource_name=datasource_name, timeout_seconds=timeout_seconds,
            transaction_local=transaction_local,
        )
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


def type_probe_of(sql: str, *, db_type: str | None) -> TypeProbe:
    """``sql`` as a type probe: a ``TypeProbe`` as is, other text parsed in the datasource's dialect and bounded."""
    if isinstance(sql, TypeProbe):
        return sql
    dialect = dialect_for_ds_type(db_type)
    try:
        query = parse_query(sql=sql, target_dialect=dialect)
    except SqlglotError as exc:
        raise UnparseableTypeProbeError(sql=sql, dialect=dialect.sqlglot_name) from exc
    return render_type_probe(query, dialect=dialect)


def _probe_sync(conn: Any, *, probe: TypeProbe, db_type: str | None) -> dict[str, ResultColumnType]:
    """Type ``probe``'s result on ``conn``: the dialect's describe statement, else the probe's cursor or census."""
    dialect = dialect_for_ds_type(db_type)
    describe = dialect.describe_result_statement(exp.National(this=probe.sql))
    if describe is not None:
        try:
            described = _exec_verbatim(conn, describe.sql(dialect=dialect.sqlglot_name))
            return dialect.read_result_description(_rows_as_dicts(described))
        except sqlalchemy.exc.DBAPIError as exc:
            if not dialect.result_undescribable(exc):
                raise
    result = _exec_verbatim(conn, probe.sql)
    columns = list(result.keys())
    census_sql = probe.census_sql(columns, dialect=dialect)
    if census_sql is None:
        return _types_from_cursor(result, dialect=dialect)
    result.fetchall()
    return dialect.result_census_types(columns=columns, row=_first_row(_exec_verbatim(conn, census_sql)))


async def _probe_async(conn: Any, *, probe: TypeProbe, db_type: str | None) -> dict[str, ResultColumnType]:
    """Async sibling of ``_probe_sync``."""
    dialect = dialect_for_ds_type(db_type)
    describe = dialect.describe_result_statement(exp.National(this=probe.sql))
    if describe is not None:
        try:
            described = await _exec_verbatim_async(conn, describe.sql(dialect=dialect.sqlglot_name))
            return dialect.read_result_description(_rows_as_dicts(described))
        except sqlalchemy.exc.DBAPIError as exc:
            if not dialect.result_undescribable(exc):
                raise
    result = await _exec_verbatim_async(conn, probe.sql)
    columns = list(result.keys())
    census_sql = probe.census_sql(columns, dialect=dialect)
    if census_sql is None:
        return _types_from_cursor(result, dialect=dialect)
    result.fetchall()
    return dialect.result_census_types(
        columns=columns, row=_first_row(await _exec_verbatim_async(conn, census_sql)),
    )


def _open_probe_transaction(conn: sa.Connection) -> bool:
    """Leave autocommit so the driver opens the probe's transaction; return whether the connection was in it."""
    dbapi_connection: Any = conn.connection.dbapi_connection
    try:
        autocommit = bool(conn.dialect.detect_autocommit_setting(dbapi_connection))
    except NotImplementedError:
        return False
    if autocommit:
        dbapi_connection.autocommit = False
    return autocommit


def _close_probe_transaction(conn: sa.Connection, *, autocommit: bool) -> None:
    """Roll the probe's transaction back and restore autocommit; a connection that cannot roll back is discarded."""
    try:
        conn.rollback()
    except Exception:
        if not conn.invalidated:
            conn.invalidate()
        raise
    if autocommit:
        dbapi_connection: Any = conn.connection.dbapi_connection
        dbapi_connection.autocommit = True


def get_result_column_types_sync(
    sql: str, *, engine: sa.Engine, db_type: str | None = None, datasource_name: str | None = None,
    transaction_local: bool = False,
) -> dict[str, ResultColumnType]:
    """Column types of ``sql``'s result over an existing engine, read-only (rolled back) so a probe can't mutate."""
    probe = type_probe_of(sql, db_type=db_type)
    with engine.connect() as conn, _statement_timeout(
        conn, engine=engine, dialect=dialect_for_ds_type(db_type),
        datasource_name=datasource_name or str(engine.url), timeout_seconds=_TYPE_PROBE_TIMEOUT_SECONDS,
        transaction_local=transaction_local,
    ):
        ro_sql = _read_only_transaction_sql(db_type)
        if ro_sql is None:
            types = _probe_sync(conn, probe=probe, db_type=db_type)
            conn.rollback()
            return types
        autocommit = _open_probe_transaction(conn)
        try:
            _exec_verbatim(conn, ro_sql)
            return _probe_sync(conn, probe=probe, db_type=db_type)
        finally:
            _close_probe_transaction(conn, autocommit=autocommit)


def get_column_types_sync(
    sql: str, *, engine: sa.Engine, db_type: str | None = None, datasource_name: str | None = None,
) -> dict[str, str]:
    """Deprecated: the 1.5.0 category probe over an existing engine; use ``get_result_column_types_sync``."""
    warn_deprecated(
        "get_column_types_sync() returns 1.5.0 type categories and is removed in SLayer 2.0; "
        "use get_result_column_types_sync(), which returns SLayer types and database type names.",
        removal="2.0",
    )
    return type_categories(get_result_column_types_sync(
        sql, engine=engine, db_type=db_type, datasource_name=datasource_name,
    ), dialect=dialect_for_ds_type(db_type))


async def _get_result_column_types_async(
    probe: TypeProbe,
    *,
    engine,
    db_type: str | None,
    datasource_name: str | None = None,
    transaction_local: bool = False,
) -> dict[str, ResultColumnType]:
    """Async sibling of ``get_result_column_types_sync``."""
    async with engine.connect() as conn, _statement_timeout_async(
        conn, engine=engine, dialect=dialect_for_ds_type(db_type),
        datasource_name=datasource_name or str(engine.url), timeout_seconds=_TYPE_PROBE_TIMEOUT_SECONDS,
        transaction_local=transaction_local,
    ):
        ro_sql = _read_only_transaction_sql(db_type)
        if ro_sql is None:
            types = await _probe_async(conn, probe=probe, db_type=db_type)
            await conn.rollback()
            return types
        autocommit = await conn.run_sync(_open_probe_transaction)
        try:
            await _exec_verbatim_async(conn, ro_sql)
            return await _probe_async(conn, probe=probe, db_type=db_type)
        finally:
            await conn.run_sync(functools.partial(_close_probe_transaction, autocommit=autocommit))


class ExecutionResult(BaseModel):
    """Rows of one executed statement, plus the timeout skips it ran under."""

    rows: list[dict[str, Any]]
    warnings: list[StatementTimeoutSkippedWarning] = Field(default_factory=list)


@runtime_checkable
class SqlClient(Protocol):
    """Everything SLayer asks of one datasource's database: statements, live metadata, lifecycle.

    The contract (semantics, typed failures, lifecycle) is ``docs/configuration/sql-clients.md``;
    ``slayer.sql.conformance.run_conformance`` checks an implementation against it.
    """

    @property
    def datasource(self) -> DatasourceConfig: ...

    async def execute(self, sql: str) -> ExecutionResult: ...

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]: ...

    async def server_profile(self) -> ServerProfile: ...

    async def ping(self) -> None: ...

    async def list_schemas(self) -> list[str]: ...

    async def default_schema(self) -> str | None: ...

    async def current_catalog(self) -> str | None: ...

    async def list_objects(self, schema: str | None) -> ObjectListing: ...

    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]: ...

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys: ...

    async def table_comment(self, location: TableLocation) -> str | None: ...

    async def schema_description(self, schema: str | None, configured_schema: str | None) -> str | None: ...

    async def aclose(self) -> None: ...

    def close(self) -> None: ...


class SlayerSQLClient:
    """Executes SQL against databases via SQLAlchemy, through one driver.

    The driver is the caller's ``sa_engine``'s, else the URL's (the one named, else the type's
    default). An async-capable driver serves everything on one async engine per event loop; a
    sync-only driver, or a loop the driver's async mode refuses, runs on its sync engine in a worker.
    A caller's ``sa_engine`` serves every call on the caller's thread and is never disposed here.
    """

    def __init__(self, datasource: DatasourceConfig, *, sa_engine: sa.Engine | None = None):
        self.datasource = datasource
        self._async_engines: dict[asyncio.AbstractEventLoop, AsyncEngine] = {}
        # Guards the per-loop registry; never held across engine construction or disposal.
        self._async_lock = threading.Lock()
        self._sync_engine: sa.Engine | None = sa_engine
        self._sync_engine_owner: EngineOwner | None = "caller" if sa_engine is not None else None
        # Backstop disposing a client-owned engine when the client is collected unclosed.
        self._sync_engine_finalizer: weakref.finalize | None = None
        # Releases the bridge loop's async engine: on ``close()``, or when the client is collected unclosed.
        self._bridge_release: weakref.finalize | None = None

    def close(self) -> None:
        """Dispose this client's private in-memory engine and its bridge-loop async engine (idempotent).

        A factory-owned engine is left to the factory. Other loops' async engines are released only by
        ``aclose()`` on their own loop: while a live one remains, this warns.
        """
        self._dispose_private_sync_engine()
        with self._async_lock:
            for loop in [loop for loop in self._async_engines if async_utils.is_bridge_loop(loop)]:
                del self._async_engines[loop]
            closed = self._take_closed_loop_engines()
            live = bool(self._async_engines)
            release, self._bridge_release = self._bridge_release, None
        if release is not None:
            release()
        _abandon(closed)
        if live:
            logger.warning(
                "SlayerSQLClient.close() cannot dispose the loop-bound async engines for datasource %r; "
                "await aclose() on each engine's event loop.",
                self.datasource.name,
            )

    def _dispose_private_sync_engine(self) -> bool:
        """Dispose the private in-memory engine (detaching its finalizer) and
        forget it; return whether there was one. Idempotent; a factory- or
        caller-owned engine is left untouched."""
        finalizer = self._sync_engine_finalizer
        if self._sync_engine_owner == "client" and self._sync_engine is not None:
            engine = self._sync_engine
            self._sync_engine = None
            self._sync_engine_owner = None
            self._sync_engine_finalizer = None
            if finalizer is not None:
                finalizer.detach()
            _dispose_engine_quietly(engine)
            return True
        return False

    async def aclose(self) -> None:
        """Release the running loop's resources (its async engine, idle pooled connections); the client stays usable."""
        if self._sync_engine is not None and self._sync_engine_owner == "factory":
            engine_factory.release_idle(self._sync_engine)
        loop = asyncio.get_running_loop()
        with self._async_lock:
            engine = self._async_engines.pop(loop, None)
            closed = self._take_closed_loop_engines()
            release = None
            if engine is not None and async_utils.is_bridge_loop(loop):
                release, self._bridge_release = self._bridge_release, None
        _abandon(closed)
        if release is not None:
            _forget_bridge_release(release)
        if engine is None:
            return
        try:
            await engine.dispose()
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "Async engine dispose failed for datasource %r: %s",
                self.datasource.name, exc,
            )

    def _take_closed_loop_engines(self) -> list[AsyncEngine]:
        """Pop the engines whose loop has closed (callers hold ``_async_lock``)."""
        closed = [loop for loop in self._async_engines if loop.is_closed()]
        return [self._async_engines.pop(loop) for loop in closed]

    def _connection_datasource(self) -> DatasourceConfig:
        """``datasource`` with its ``${ENV_VAR}`` references resolved (itself when it has none)."""
        ds = self.datasource
        if any(isinstance(v, str) and _ENV_REF.search(v) for v in ds.model_dump().values()):
            return ds.resolve_env_vars()
        return ds

    @property
    def _dialect(self) -> SqlDialect:
        return dialect_for_ds_type(self.datasource.type)

    def _async_url(self) -> URL | None:
        """The connection URL when its driver runs async; ``None`` for a caller's engine or a sync-only driver."""
        if self._sync_engine_owner is not None and self._sync_engine_owner != "factory":
            return None
        ds = self._connection_datasource()
        conn_str = ds.get_connection_string()
        if engine_factory._is_in_memory_sqlite(conn_str):
            return None
        try:
            url = make_url(conn_str)
        except ArgumentError:
            return None
        with plugin_errors(ds, conn_str, dialect=self._dialect):
            is_async = url.get_dialect(_is_async=True).is_async
        return url if is_async else None

    def _async_engine_here(self) -> AsyncEngine | None:
        """The running loop's async engine, or ``None`` when this client runs its driver synchronously here."""
        url = self._async_url()
        if url is None:
            return None
        facts = self._dialect.driver_facts(self.datasource.type)
        if facts.refuses_loop(url.get_driver_name(), asyncio.get_running_loop()):
            return None
        return self._get_async_engine()

    def _warn_bundled_driver(self, url: URL) -> None:
        facts = self._dialect.driver_facts(self.datasource.type)
        if facts.bundled_non_default(url):
            warn_deprecated(
                f"motley-slayer[{facts.install_extra}] stops installing {url.get_driver_name()} in 2.0; "
                "depend on it directly.",
                removal="2.0",
                stacklevel=3,
            )

    def _get_async_engine(self) -> AsyncEngine:
        """The running loop's async engine, built on first use there."""
        loop = asyncio.get_running_loop()
        with self._async_lock:
            engine = self._async_engines.get(loop)
            closed = self._take_closed_loop_engines()
        _abandon(closed)
        if engine is not None:
            return engine
        ds = self._connection_datasource()
        conn_str = ds.get_connection_string()
        connection_policy.check_transaction_pooler_backend(ds, url=conn_str)
        load_driver(ds, conn_str, dialect=self._dialect, is_async=True)
        self._warn_bundled_driver(make_url(conn_str))
        built = create_async_engine(conn_str, **connection_policy.engine_kwargs(
            url=conn_str, dialect=self._dialect, datasource=ds, is_async=True,
        ))
        with self._async_lock:
            engine = self._async_engines.setdefault(loop, built)
            if engine is built and async_utils.is_bridge_loop(loop):
                token = async_utils.register_shutdown(engine.dispose)
                if token is not None:
                    self._bridge_release = weakref.finalize(self, _release_bridge_engine, engine, token)
        if engine is not built:
            _abandon([built])  # a concurrent first use built the loop's engine; this one never connected
        return engine

    def _get_sync_engine_for_client(self) -> sa.Engine:
        """Return a per-client sync engine.

        In-memory SQLite gets a private StaticPool engine (isolated per client),
        owned and disposed here with a finalizer backstop; everything else
        delegates to engine_factory, which owns and disposes it.
        """
        if self._sync_engine is not None:
            return self._sync_engine
        ds = self._connection_datasource()
        conn_str = ds.get_connection_string()
        connection_policy.check_transaction_pooler_backend(ds, url=conn_str)
        if engine_factory._is_in_memory_sqlite(conn_str):
            engine = engine_factory.build_in_memory_sqlite_engine(conn_str)
            self._sync_engine = engine
            self._sync_engine_owner = "client"
            self._sync_engine_finalizer = weakref.finalize(
                self, _dispose_engine_quietly, engine,
            )
            return engine
        self._sync_engine = engine_factory.get_engine(ds)
        self._sync_engine_owner = "factory"
        with contextlib.suppress(ArgumentError):
            self._warn_bundled_driver(make_url(conn_str))
        return self._sync_engine

    async def _blocking(self, func: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:
        """Run blocking ``func``: on the caller's thread over a caller's engine (pools may be thread-bound), else in a worker."""
        if self._sync_engine_owner == "caller":
            return await on_caller_thread(functools.partial(func, *args, **kwargs))
        return await _run_sync_in_thread(func, *args, **kwargs)

    def _discard_sync_engine_on_auth_failure(self, exc: BaseException) -> bool:
        """Drop the sync engine on a credential rejection; return whether exc was one.

        Credentials are fixed at construction, so a revoked grant poisons the
        engine permanently — dispose a private one, evict a factory one, leave
        a caller's alone. Best-effort; never displace exc.
        """
        if not _is_auth_failure(exc):
            return False
        if self._sync_engine_owner == "caller":
            return True
        if not self._dispose_private_sync_engine():
            self._sync_engine = None
            self._sync_engine_owner = None
            try:
                engine_factory.invalidate_engine(self._connection_datasource())
            except Exception:
                logger.warning(
                    "Failed to invalidate engine for datasource %r after an "
                    "authentication failure.", self.datasource.name, exc_info=True,
                )
        return True

    async def _discard_engines_on_auth_failure(self, exc: BaseException) -> None:
        """Async-path cleanup: the sync engine plus the running loop's async engine."""
        if not self._discard_sync_engine_on_auth_failure(exc):
            return
        await self.aclose()

    async def _typed(self, exc: Exception) -> None:
        """Discard engines on a credential rejection, then raise ``exc``'s typed failure, if any."""
        await self._discard_engines_on_auth_failure(exc)
        typed = _typed_failure(exc)
        if typed is not None:
            raise typed from exc

    async def execute(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> ExecutionResult:
        """Execute SQL asynchronously; a session or transaction statement is refused."""
        writes = admit_statement(sql, dialect=self._dialect)
        return await self._typed_execute(sql=sql, timeout_seconds=timeout_seconds, writes=writes)

    async def _typed_execute(self, *, sql: str, timeout_seconds: int, writes: bool) -> ExecutionResult:
        try:
            return await self._execute(sql=sql, timeout_seconds=timeout_seconds, writes=writes)
        except Exception as exc:
            await self._typed(exc)
            raise

    async def _execute(
        self,
        *,
        sql: str,
        timeout_seconds: int,
        writes: bool,
    ) -> ExecutionResult:
        async_engine = self._async_engine_here()
        db_type = self.datasource.type
        if async_engine is not None:
            return await _execute_with_retry_async(
                sql=sql,
                engine=async_engine,
                db_type=db_type,
                timeout_seconds=timeout_seconds,
                datasource_name=self.datasource.name,
                transaction_local=self.datasource.transaction_pooler,
                writes=writes,
            )
        return await _execute_with_retry_threaded(
            sql=sql,
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=self._get_sync_engine_for_client(),
            datasource_name=self.datasource.name,
            transaction_local=self.datasource.transaction_pooler,
            writes=writes,
            run=self._blocking,
        )

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        """The SLayer type and database type name of each of ``sql``'s result columns."""
        probe = type_probe_of(sql, db_type=self.datasource.type)
        try:
            return await self._get_result_column_types(probe=probe)
        except Exception as exc:
            await self._typed(exc)
            raise

    async def _get_result_column_types(self, *, probe: TypeProbe) -> dict[str, ResultColumnType]:
        db_type = self.datasource.type
        async_engine = self._async_engine_here()
        if async_engine is not None:
            return await _get_result_column_types_async(
                probe, engine=async_engine, db_type=db_type, datasource_name=self.datasource.name,
                transaction_local=self.datasource.transaction_pooler,
            )
        return await self._blocking(
            get_result_column_types_sync,
            probe,
            db_type=db_type,
            engine=self._get_sync_engine_for_client(),
            datasource_name=self.datasource.name,
            transaction_local=self.datasource.transaction_pooler,
        )

    async def get_column_types(self, sql: str) -> dict[str, str]:
        """Deprecated: the 1.5.0 category probe; use ``get_result_column_types``."""
        warn_deprecated(
            "SlayerSQLClient.get_column_types() returns 1.5.0 type categories and is removed in SLayer 2.0; "
            "use get_result_column_types(), which returns SLayer types and database type names.",
            removal="2.0",
        )
        return type_categories(await self.get_result_column_types(sql), dialect=self._dialect)

    async def server_profile(self) -> ServerProfile:
        """The server's profile for this client's engine, probed once per engine."""
        dialect = self._dialect
        if dialect.server_profile_sql() is None:
            return ServerProfile()
        try:
            async_engine = self._async_engine_here()
            if async_engine is not None:
                async with async_engine.connect() as conn:
                    return await _server_profile_async(conn, engine=async_engine, dialect=dialect)
            return await self._blocking(
                _server_profile_sync, engine=self._get_sync_engine_for_client(), dialect=dialect,
            )
        except Exception as exc:
            await self._typed(exc)
            raise

    # Live metadata: one read on one open sync connection — the async engine's, else the sync engine's off the loop.

    async def _metadata(self, read: Callable[[sa.Connection], _T]) -> _T:
        try:
            async_engine = self._async_engine_here()
            if async_engine is not None:
                async with async_engine.connect() as conn:
                    return await conn.run_sync(read)
            return await self._blocking(_read_on_connection, self._get_sync_engine_for_client(), read)
        except Exception as exc:
            await self._typed(exc)
            raise

    async def ping(self) -> None:
        """Raise unless the database answers a trivial statement."""
        await self._metadata(lambda conn: _exec_verbatim(conn, "SELECT 1"))

    async def list_schemas(self) -> list[str]:
        return await self._metadata(lambda conn: list(sa.inspect(conn).get_schema_names() or []))

    async def default_schema(self) -> str | None:
        try:
            url = make_url(self._connection_datasource().get_connection_string())
        except ArgumentError:
            url = None
        fixed = None if url is None else self._dialect.url_default_schema(url=url)
        return fixed if fixed is not None else await self._metadata(self._default_schema_sync)

    async def current_catalog(self) -> str | None:
        return await self._metadata(self._current_catalog_sync)

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return await self._metadata(functools.partial(self._list_objects_sync, schema=schema))

    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]:
        return await self._metadata(functools.partial(self._reflect_columns_sync, location=location))

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        return await self._metadata(functools.partial(self._reflect_table_keys_sync, location=location))

    async def table_comment(self, location: TableLocation) -> str | None:
        return await self._metadata(functools.partial(self._table_comment_sync, location=location))

    async def schema_description(self, schema: str | None, configured_schema: str | None) -> str | None:
        return await self._metadata(lambda conn: self._dialect.reflect_schema_description(
            connection=conn, schema=schema, configured_schema=configured_schema,
        ))

    def _default_schema_sync(self, conn: sa.Connection) -> str | None:
        return self._dialect.reflect_default_schema(connection=conn, inspector=_LazyInspector(conn))

    def _current_catalog_sync(self, conn: sa.Connection) -> str | None:
        sql = self._dialect.current_catalog_sql()
        if sql is None:
            return None
        try:
            row = _exec_verbatim(conn, sql).fetchone()
        except Exception as exc:  # noqa: BLE001 — an unknown catalog is None
            if _typed_failure(exc) is not None:
                raise
            recover(conn)
            return None
        value = row[0] if row else None
        return value if isinstance(value, str) and value else None

    def _default_token(self, conn: sa.Connection) -> str | None:
        """The connection default as an Inspector token: catalog-qualified where tokens are."""
        if not self._dialect.catalog_qualified_schema_tokens:
            return None
        return schema_token(schema=self._default_schema_sync(conn), catalog=self._current_catalog_sync(conn))

    def _location_token(self, conn: sa.Connection, location: TableLocation) -> str | None:
        """``location``'s Inspector token; unset parts are the connection's current ones where tokens qualify."""
        if location.catalog is None and self._dialect.catalog_qualified_schema_tokens:
            return schema_token(
                schema=location.schema_name or self._default_schema_sync(conn),
                catalog=self._current_catalog_sync(conn),
            )
        return schema_token(schema=location.schema_name, catalog=location.catalog)

    def _list_objects_sync(self, conn: sa.Connection, *, schema: str | None) -> ObjectListing:
        inspector = sa.inspect(conn)
        if schema is not None and not _schema_listed(inspector, schema, conn=conn):
            raise ObjectNotFound(f"schema {schema!r} not found")
        token = schema if schema is not None else self._default_token(conn)
        return ObjectListing(
            tables=list(inspector.get_table_names(schema=token) or []),
            views=_object_names(inspector, accessor="get_view_names", token=token),
            materialized_views=_object_names(inspector, accessor="get_materialized_view_names", token=token),
        )

    def _reflect_columns_sync(self, conn: sa.Connection, *, location: TableLocation) -> list[ReflectedColumn]:
        try:
            columns = self._dialect.reflect_columns(
                connection=conn, inspector=_LazyInspector(conn), table=location.name,
                schema=location.schema_name, catalog=location.catalog,
            )
        except Exception as exc:
            if _typed_failure(exc) is None:
                recover(conn)
                if self._absent(conn, location):
                    raise ObjectNotFound(f"table {location.name!r} not found") from exc
            raise
        if not columns:
            raise ObjectNotFound(f"table {location.name!r} not found")
        return columns

    def _absent(self, conn: sa.Connection, location: TableLocation) -> bool:
        """Whether the Inspector confirms ``location`` does not exist (unconfirmable → ``False``)."""
        try:
            return not sa.inspect(conn).has_table(location.name, schema=self._location_token(conn, location))
        except Exception:  # noqa: BLE001 — unconfirmable
            recover(conn)
            return False

    def _reflect_table_keys_sync(self, conn: sa.Connection, *, location: TableLocation) -> TableKeys:
        keys = self._dialect.reflect_table_keys(
            connection=conn, inspector=_LazyInspector(conn), table=location.name,
            schema=location.schema_name, catalog=location.catalog,
        )
        if not (keys.primary_key or keys.foreign_keys or keys.unique_key_sets):
            self._reflect_columns_sync(conn, location=location)  # a keyless table must exist
        return keys

    def _table_comment_sync(self, conn: sa.Connection, *, location: TableLocation) -> str | None:
        try:
            comment = sa.inspect(conn).get_table_comment(
                location.name, schema=self._location_token(conn, location),
            )
        except NotImplementedError:
            return None
        return clean_comment(comment.get("text"))

    def execute_sync(
        self,
        sql: str,
        timeout_seconds: int = 120,
    ) -> ExecutionResult:
        """Execute SQL synchronously (CLI, notebooks, tests); a session or transaction statement is refused.

        An async driver runs on the bridge loop, whose connections serve later calls; a sync one
        runs here, discarding only the sync engine on auth failure.
        """
        writes = admit_statement(sql, dialect=self._dialect)
        if self._async_url() is not None:
            return run_sync(self._execute_bridged(sql=sql, timeout_seconds=timeout_seconds, writes=writes))
        try:
            return _execute_with_retry_sync(
                sql=sql,
                db_type=self.datasource.type,
                timeout_seconds=timeout_seconds,
                engine=self._get_sync_engine_for_client(),
                datasource_name=self.datasource.name,
                transaction_local=self.datasource.transaction_pooler,
                writes=writes,
            )
        except Exception as exc:
            self._discard_sync_engine_on_auth_failure(exc)
            raise

    async def _execute_bridged(self, *, sql: str, timeout_seconds: int, writes: bool) -> ExecutionResult:
        try:
            return await self._typed_execute(sql=sql, timeout_seconds=timeout_seconds, writes=writes)
        finally:
            await asyncio.shield(self.release_local_connections())

    async def release_local_connections(self) -> None:
        """End of a synchronous workflow: free idle connections holding a local database file; networked
        connections stay open for later calls. A private loop (a nested call) ends here, so it is fully released."""
        if not async_utils.is_bridge_loop(asyncio.get_running_loop()):
            await self.aclose()
            return
        if self._sync_engine is not None and self._sync_engine_owner == "factory":
            engine_factory.release_idle(self._sync_engine)
        with self._async_lock:
            engine = self._async_engines.get(asyncio.get_running_loop())
        if engine is not None and engine_factory.holds_local_file(engine.sync_engine):
            await engine.dispose()


def _read_on_connection(engine: sa.Engine, read: Callable[[sa.Connection], _T]) -> _T:
    with engine.connect() as conn:
        return read(conn)


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


def _is_transient_db_error(exc: BaseException, *, writes: bool = False) -> bool:
    """True only for DB errors worth retrying.

    OperationalError is too broad (spans schema errors); match transient signals
    in exc.orig. DisconnectionError is always transient; so is an error SQLAlchemy read as a lost connection,
    unless the statement ``writes`` (it may have been applied).
    """
    if isinstance(exc, sqlalchemy.exc.DisconnectionError):
        return True
    if isinstance(exc, sqlalchemy.exc.DBAPIError) and exc.connection_invalidated and not writes:
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
    writes: bool = False,
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
            _log_transient_or_raise(exc=exc, sql=sql, attempt=attempt, delay=delay, writes=writes)
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
    return await do_call()


def _check_max_attempts(max_attempts: int) -> None:
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")


_RETRYABLE_ERRORS = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DisconnectionError)


def _log_transient_or_raise(*, exc: Exception, sql: str, attempt: int, delay: float, writes: bool = False) -> None:
    """Re-raise a non-transient error; log the retry of a transient one."""
    if not _is_transient_db_error(exc, writes=writes):
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
    transaction_local: bool = False,
    writes: bool = False,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> ExecutionResult:
    return await _retry_with_backoff(
        sql=sql,
        do_call=lambda: _execute_sql_async(
            sql=sql, engine=engine, db_type=db_type, timeout_seconds=timeout_seconds,
            datasource_name=datasource_name, transaction_local=transaction_local, writes=writes,
        ),
        writes=writes,
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
    transaction_local: bool = False,
    writes: bool = False,
) -> ExecutionResult:
    _t = timing.start()
    async with engine.connect() as conn:
        timing.record("connect", _t)
        _t = timing.start()
        async with _statement_timeout_async(
            conn, engine=engine, dialect=dialect_for_ds_type(db_type),
            datasource_name=datasource_name or str(engine.url), timeout_seconds=timeout_seconds,
            transaction_local=transaction_local,
        ) as skipped:
            timing.record("set_timeout", _t)
            _t = timing.start()
            result = await _exec_verbatim_async(conn, sql)
            rows = _fetch_rows(result, dialect=dialect_for_ds_type(db_type))
            if writes or not result.returns_rows:
                await conn.commit()
            timing.record("query", _t)
    return ExecutionResult(rows=rows, warnings=skipped)


async def _execute_with_retry_threaded(
    sql: str,
    db_type: str | None,
    *,
    engine: sa.Engine,
    timeout_seconds: int = 120,
    datasource_name: str | None = None,
    transaction_local: bool = False,
    writes: bool = False,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    run: Callable[..., Awaitable[Any]] = _run_sync_in_thread,
) -> ExecutionResult:
    return await _retry_with_backoff(
        sql=sql,
        do_call=lambda: run(
            _execute_sql_sync,
            sql=sql,
            db_type=db_type,
            timeout_seconds=timeout_seconds,
            engine=engine,
            datasource_name=datasource_name,
            transaction_local=transaction_local,
            writes=writes,
        ),
        writes=writes,
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
    transaction_local: bool = False,
    writes: bool = False,
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
            transaction_local=transaction_local,
            writes=writes,
        )

    delay = initial_delay
    for attempt in range(max_attempts - 1):
        try:
            return _call()
        except _RETRYABLE_ERRORS as exc:
            _log_transient_or_raise(exc=exc, sql=sql, attempt=attempt, delay=delay, writes=writes)
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
    transaction_local: bool = False,
    writes: bool = False,
) -> ExecutionResult:
    with engine.connect() as conn, _statement_timeout(
        conn, engine=engine, dialect=dialect_for_ds_type(db_type),
        datasource_name=datasource_name or str(engine.url), timeout_seconds=timeout_seconds,
        transaction_local=transaction_local,
    ) as skipped:
        result = _exec_verbatim(conn=conn, sql=sql)
        rows = _fetch_rows(result, dialect=dialect_for_ds_type(db_type))
        if writes or not result.returns_rows:
            conn.commit()
    return ExecutionResult(rows=rows, warnings=skipped)


def _fetch_rows(result, *, dialect: SqlDialect) -> list[dict[str, Any]]:
    """A row-returning result's rows, each cell read by its dialect reader and each driver tuple as a Python
    list; a statement returning none (DDL) yields ``[]``."""
    if not result.returns_rows:
        return []
    columns = list(result.keys())
    description = getattr(getattr(result, "cursor", None), "description", None)
    readers = dialect.result_readers(description) if description else {}

    def cell(i: int, v: Any) -> Any:
        if v is not None and i in readers:
            v = readers[i](v)
        return list(v) if isinstance(v, tuple) else v

    return [{c: cell(i, v) for i, (c, v) in enumerate(zip(columns, row))} for row in result.fetchall()]


def _typed_failure(exc: BaseException) -> SqlClientError | None:
    """The typed failure ``exc`` stands for: rejected credentials, an unreachable database, a missing table."""
    if isinstance(exc, SqlClientError):
        return None
    if _is_auth_failure(exc):
        return AuthenticationRefused(str(exc))
    if _is_unreachable_db_error(exc):
        return DatabaseUnavailable(str(exc))
    if isinstance(exc, sqlalchemy.exc.NoSuchTableError):
        return ObjectNotFound(str(exc))
    return None


class _LazyInspector:
    """An Inspector built on first use, so a hook that never reads it never builds one."""

    def __init__(self, conn: sa.Connection) -> None:
        self._conn = conn
        self._inspector: Any = None

    def __getattr__(self, name: str) -> Any:
        if self._inspector is None:
            self._inspector = sa.inspect(self._conn)
        return getattr(self._inspector, name)


def _schema_listed(inspector: Any, schema: str, *, conn: sa.Connection) -> bool:
    """Whether ``schema`` (bare or catalog-qualified) is among the listed schemas; unlistable → ``True``."""
    try:
        names = [n.casefold() for n in inspector.get_schema_names() or []]
    except Exception:  # noqa: BLE001 — unknown listing never claims absence
        recover(conn)
        return True
    wanted = schema.casefold()
    return any(n == wanted or ("." not in wanted and n.rpartition(".")[2] == wanted) for n in names)


def _object_names(inspector: Any, *, accessor: str, token: str | None) -> list[str]:
    """An optional ``get_*_names`` accessor's names; ``[]`` when the dialect lacks the kind, raises when reading it fails."""
    read = getattr(inspector, accessor, None)
    if read is None:
        return []
    try:
        return list(read(schema=token) or [])
    except NotImplementedError:
        return []


class DelegatingSqlClient:
    """Forwards every operation, closing included, to ``inner``; subclass it and override what a gateway serves.

    No operation calls another, so the wrapped client's own work never routes through an override.
    """

    def __init__(self, inner: SqlClient) -> None:
        self.inner = inner

    @property
    def datasource(self) -> DatasourceConfig:
        return self.inner.datasource

    async def execute(self, sql: str) -> ExecutionResult:
        return await self.inner.execute(sql)

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        return await self.inner.get_result_column_types(sql)

    async def get_column_types(self, sql: str) -> dict[str, str]:
        """Deprecated: the 1.6.0 category probe; use ``get_result_column_types``."""
        warn_deprecated(
            "DelegatingSqlClient.get_column_types() returns 1.6.0 type categories and is removed in SLayer 2.0; "
            "use get_result_column_types(), which returns SLayer types and database type names.",
            removal="2.0",
        )
        return type_categories(
            await self.inner.get_result_column_types(sql), dialect=dialect_for_ds_type(self.datasource.type),
        )

    async def server_profile(self) -> ServerProfile:
        return await self.inner.server_profile()

    async def ping(self) -> None:
        await self.inner.ping()

    async def list_schemas(self) -> list[str]:
        return await self.inner.list_schemas()

    async def default_schema(self) -> str | None:
        return await self.inner.default_schema()

    async def current_catalog(self) -> str | None:
        return await self.inner.current_catalog()

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return await self.inner.list_objects(schema)

    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]:
        return await self.inner.reflect_columns(location)

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        return await self.inner.reflect_table_keys(location)

    async def table_comment(self, location: TableLocation) -> str | None:
        return await self.inner.table_comment(location)

    async def schema_description(self, schema: str | None, configured_schema: str | None) -> str | None:
        return await self.inner.schema_description(schema=schema, configured_schema=configured_schema)

    async def aclose(self) -> None:
        await self.inner.aclose()

    async def release_local_connections(self) -> None:
        """Forwarded to ``inner`` when it has the end-of-workflow release."""
        release = getattr(self.inner, "release_local_connections", None)
        if release is not None:
            await release()

    def close(self) -> None:
        self.inner.close()


class BorrowedSqlClient(DelegatingSqlClient):
    """``inner`` lent to another engine: closing it here leaves ``inner`` open."""

    async def aclose(self) -> None:
        """No-op: the lender closes ``inner``."""

    def close(self) -> None:
        """No-op: the lender closes ``inner``."""


_CATEGORY_TYPES = {
    "number": DataType.DOUBLE, "string": DataType.TEXT, "time": DataType.TIMESTAMP, "boolean": DataType.BOOLEAN,
}


class _CategoryProbeClient(DelegatingSqlClient):
    """A client written for 1.6.0, whose type probe is ``get_column_types``: its categories read as types."""

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        warn_deprecated(
            "A SQL client's get_column_types() is the 1.6.0 type probe and is no longer called in SLayer 2.0; "
            "implement get_result_column_types(), which returns SLayer types and database type names.",
            removal="2.0",
        )
        categories: dict[str, str] = await self.inner.get_column_types(sql)  # pyright: ignore[reportAttributeAccessIssue]
        return {name: ResultColumnType(type=_CATEGORY_TYPES.get(c, DataType.TEXT)) for name, c in categories.items()}


def adapt_client(client: SqlClient) -> SqlClient:
    """``client``, wrapped to probe through its 1.6.0 ``get_column_types`` when that is the only probe it serves."""
    if isinstance(client, DelegatingSqlClient):
        cls = type(client)
        legacy = (cls.get_result_column_types is DelegatingSqlClient.get_result_column_types
                  and cls.get_column_types is not DelegatingSqlClient.get_column_types)
    else:
        typed = getattr(client, "get_result_column_types", None)
        stub = typed is None or getattr(typed, "__func__", None) is SqlClient.get_result_column_types
        legacy = stub and hasattr(client, "get_column_types")
    return _CategoryProbeClient(client) if legacy else client
