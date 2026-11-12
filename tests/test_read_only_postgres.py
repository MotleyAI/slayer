"""Read-only Postgres connections on every driver, direct and behind a transaction pooler (spec: database/execution)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Generator
from typing import Any

import pytest
from sqlalchemy.exc import DBAPIError
from pytest_postgresql import factories

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from tests._pg_helpers import pg_database
from tests._pg_statement_log import LOGGED_POSTGRES_OPTIONS, LoggedEntry, ServerLog

postgresql_logged = factories.postgresql_proc(port=None, postgres_options=LOGGED_POSTGRES_OPTIONS)

_WRITE = "INSERT INTO t VALUES (2)"

_RO_ERROR = "read-only transaction"

_SYNC_SCHEMES = ["postgresql+psycopg", "postgresql+psycopg2", "postgresql+pg8000"]

_ASYNC_SCHEMES = ["postgresql+psycopg", "postgresql+asyncpg"]

_INIT = [
    "select pg_catalog.version()", "select current_schema()",
    "show transaction isolation level", "show standard_conforming_strings",
]

_MARKS = [f"SELECT 'mark-{n}' AS m" for n in (1, 2, 3)]

# What the server logged for three queries through the client before this change.
_DIRECT_BASELINE = {
    "postgresql+psycopg": [*_INIT, "BEGIN", "SELECT", "COMMIT", "SET statement_timeout = 120000", *_MARKS],
    "postgresql+psycopg2": [
        "BEGIN", "SELECT t.oid, typarray", "ROLLBACK", *_INIT, "SET statement_timeout = 120000", *_MARKS,
    ],
    "postgresql+pg8000": [*_INIT, "SET statement_timeout = 120000", *_MARKS],
    "postgresql+asyncpg": [*_INIT, "SET statement_timeout = 120000", *_MARKS],
}

# The pooler's per-query transactions before this change, with each transaction start as ``<begin>``.
_POOLER_QUERIES = [
    stmt for mark in _MARKS for stmt in ("<begin>", "SET LOCAL statement_timeout = 120000", mark, "<end>")
]

_BEGINS = ("begin", "start transaction")

_ENDS = ("rollback", "commit")


@contextlib.contextmanager
def _pg(info: Any, *, scheme: str, query: str = "", **extra: Any) -> Generator[tuple[DatasourceConfig, ServerLog]]:
    with pg_database(info, statements=["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1)"], name="ro") as plain:
        assert plain.database is not None
        ds = DatasourceConfig.model_validate({
            "name": plain.name, "type": "postgres",
            "connection_string": f"{scheme}://{plain.username}@{plain.host}:{plain.port}/{plain.database}{query}",
            **extra,
        })
        try:
            yield ds, ServerLog(logfile=info.logfile, database=plain.database)
        finally:
            engine_factory.invalidate_engine(ds)


def _three_queries(ds: DatasourceConfig) -> None:
    client = SlayerSQLClient(ds)

    async def _go() -> None:
        try:
            for sql in _MARKS:
                await client.execute(sql)
        finally:
            await client.aclose()

    try:
        asyncio.run(_go())
    finally:
        client.close()


def _normalised(sql: str) -> str:
    lowered = sql.lower().rstrip(";")
    if lowered.startswith(_BEGINS):
        return "<begin>"
    if lowered in _ENDS:
        return "<end>"
    return sql


def _transactions(entries: list[LoggedEntry]) -> list[list[str]]:
    """Each backend's logged statements grouped by transaction, each led by its start."""
    grouped: list[list[str]] = []
    open_on: dict[int, list[str]] = {}
    for entry in entries:
        if _normalised(entry.sql) == "<begin>":
            open_on[entry.pid] = [entry.sql]
            grouped.append(open_on[entry.pid])
        elif entry.pid in open_on:
            open_on[entry.pid].append(entry.sql)
            if _normalised(entry.sql) == "<end>":
                del open_on[entry.pid]
    return grouped


def _async_session(ds: DatasourceConfig, *statements: str) -> list[Any]:
    """Run ``statements`` on one connection of the client's async engine; return each scalar."""

    async def _go() -> list[Any]:
        client = SlayerSQLClient(ds)
        try:
            async with client._get_async_engine().connect() as conn:
                return [(await conn.exec_driver_sql(sql)).scalar() for sql in statements]
        finally:
            await client.aclose()

    return asyncio.run(_go())


# ---------------------------------------------------------------------------
# Direct connections: every transaction read-only from connection start
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scheme", _SYNC_SCHEMES)
def test_sync_engines_refuse_writes_in_autocommit(postgresql_logged: Any, scheme: str) -> None:
    with _pg(postgresql_logged, scheme=scheme) as (ds, _log):
        with engine_factory.get_engine(ds).connect() as conn:
            assert conn.exec_driver_sql("SHOW default_transaction_read_only").scalar() == "on"
            dbapi_connection = conn.connection.dbapi_connection
            assert dbapi_connection is not None
            assert dbapi_connection.autocommit is True
            with pytest.raises(DBAPIError, match=_RO_ERROR):
                conn.exec_driver_sql(_WRITE)


@pytest.mark.parametrize("scheme", _ASYNC_SCHEMES)
def test_async_engines_refuse_writes(postgresql_logged: Any, scheme: str) -> None:
    with _pg(postgresql_logged, scheme=scheme) as (ds, _log):
        assert _async_session(ds, "SHOW default_transaction_read_only") == ["on"]
        with pytest.raises(DBAPIError, match=_RO_ERROR):
            _async_session(ds, _WRITE)


@pytest.mark.parametrize("scheme", sorted(_DIRECT_BASELINE))
def test_read_only_costs_no_request(postgresql_logged: Any, scheme: str) -> None:
    with _pg(postgresql_logged, scheme=scheme) as (ds, log):
        _three_queries(ds)
        sent = [e.sql for e in log.statements(until="'mark-3'")]
    assert sent == _DIRECT_BASELINE[scheme]


@pytest.mark.parametrize(
    ("scheme", "path"),
    [("postgresql+psycopg", "sync"), ("postgresql+psycopg", "async"), ("postgresql+psycopg2", "sync")],
)
def test_a_urls_own_options_are_kept(postgresql_logged: Any, scheme: str, path: str) -> None:
    probe = ("SHOW search_path", "SHOW default_transaction_read_only")
    with _pg(postgresql_logged, scheme=scheme, query="?options=-c%20search_path%3Dother") as (ds, _log):
        if path == "async":
            seen = _async_session(ds, *probe)
        else:
            with engine_factory.get_engine(ds).connect() as conn:
                seen = [conn.exec_driver_sql(sql).scalar() for sql in probe]
    assert seen == ["other", "on"]


# ---------------------------------------------------------------------------
# Behind a transaction pooler: every transaction begins read-only, no session setting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("scheme", ["postgresql+psycopg", "postgresql+psycopg2", "postgresql+pg8000", "postgresql+asyncpg"])
def test_pooler_transactions_begin_read_only(postgresql_logged: Any, scheme: str) -> None:
    with _pg(postgresql_logged, scheme=scheme, transaction_pooler=True) as (ds, log):
        _three_queries(ds)
        if scheme.endswith("asyncpg"):
            with pytest.raises(DBAPIError, match=_RO_ERROR):
                _async_session(ds, _WRITE)
        else:
            with engine_factory.get_engine(ds).connect() as conn:
                with pytest.raises(DBAPIError, match=_RO_ERROR):
                    conn.exec_driver_sql(_WRITE)
        entries = log.statements(until="INSERT INTO t")
    sent = [e.sql for e in entries]
    first = next(i for i, sql in enumerate(sent) if "'mark-1'" in sql) - 2
    queries = sent[first:first + len(_POOLER_QUERIES)]
    begins = [txn[0] for txn in _transactions(entries) if any("'mark-" in s or "INSERT INTO t" in s for s in txn)]
    assert [_normalised(sql) for sql in queries] == _POOLER_QUERIES, queries
    assert len(begins) == len(_MARKS) + 1, sent
    assert all("read only" in sql.lower() for sql in begins), begins
    assert not [sql for sql in sent if "default_transaction_read_only" in sql or "CHARACTERISTICS" in sql.upper()]
