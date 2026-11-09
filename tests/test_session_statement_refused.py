"""The default client refuses session-changing and transaction-control statements (spec: sql/client-contract)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core.models import DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient, SqlClientError

_URLS = {
    "postgres": "postgresql+psycopg://u@db.invalid/d",
    "mysql": "mysql+pymysql://u@db.invalid/d",
    "snowflake": "snowflake://u@acct/d",
    None: "postgresql+psycopg://u@db.invalid/d",
}

# (datasource type, text, leading keywords the error names)
_REFUSED = [
    ("snowflake", "USE WAREHOUSE other_wh", "USE"),
    ("snowflake", "use role analyst", "USE"),
    ("snowflake", "ALTER SESSION SET QUERY_TAG = 'x'", "ALTER SESSION"),
    ("mysql", "USE other_db", "USE"),
    ("postgres", "SET statement_timeout = 0", "SET"),
    ("postgres", "reset statement_timeout", "RESET"),
    ("postgres", "SET ROLE analyst", "SET"),
    ("postgres", "DISCARD ALL", "DISCARD"),
    ("postgres", "BEGIN", "BEGIN"),
    ("postgres", "begin", "BEGIN"),
    ("postgres", "START TRANSACTION", "START TRANSACTION"),
    ("postgres", "Commit", "COMMIT"),
    ("postgres", "ROLLBACK", "ROLLBACK"),
    ("mysql", "COMMIT", "COMMIT"),
    (None, "SET search_path = other", "SET"),
    ("postgres", "SELECT 1; SET statement_timeout = 0", "SET"),
    ("postgres", "SELECT 1;\n  -- next\n  begin", "BEGIN"),
    ("postgres", "/* lead */ SET x = 1", "SET"),
]


@pytest.fixture
def nothing_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a refused text reached an engine")

    monkeypatch.setattr(engine_factory, "get_engine", _refuse)
    monkeypatch.setattr(sql_client, "create_async_engine", _refuse)


def _client(ds_type: str | None) -> SlayerSQLClient:
    return SlayerSQLClient(DatasourceConfig(name="ds", type=ds_type, connection_string=_URLS[ds_type]))


def _refused() -> type[Exception]:
    return sql_client.SessionStatementRefused


def test_refusal_is_a_typed_client_error() -> None:
    assert issubclass(_refused(), SqlClientError)


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize(("ds_type", "sql", "keywords"), _REFUSED)
def test_sync_refuses_and_sends_nothing(ds_type: str | None, sql: str, keywords: str) -> None:
    client = _client(ds_type)
    with pytest.raises(_refused(), match=keywords):
        client.execute_sync(sql)


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize(("ds_type", "sql", "keywords"), _REFUSED)
def test_async_refuses_and_sends_nothing(ds_type: str | None, sql: str, keywords: str) -> None:
    executing = _client(ds_type).execute(sql)
    with pytest.raises(_refused(), match=keywords):
        asyncio.run(executing)


@pytest.mark.usefixtures("nothing_sent")
def test_untokenisable_text_is_refused() -> None:
    client = _client("postgres")
    with pytest.raises(_refused()):
        client.execute_sync("SELECT 'unterminated")


# ---------------------------------------------------------------------------
# Queries, DDL and DML are unaffected
# ---------------------------------------------------------------------------

# DuckDB answers a write with its ``Count`` row.
_WRITE_ROWS = {"sqlite": [], "duckdb": [{"Count": 1}]}

_ORDINARY = [
    ("CREATE TABLE t (x INT)", []),
    ("INSERT INTO t VALUES (1)", _WRITE_ROWS),
    ("UPDATE t SET x = 2", _WRITE_ROWS),
    ("SELECT x FROM t", [{"x": 2}]),
    ("SELECT 'SET x' AS \"USE\"", [{"USE": "SET x"}]),
    ("-- BEGIN\nSELECT 1 AS one", [{"one": 1}]),
    ("SELECT 1 AS one /* ; COMMIT */", [{"one": 1}]),
]


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_ordinary_statements_run(tmp_path: Path, ds_type: str) -> None:
    client = SlayerSQLClient(DatasourceConfig(name="ds", type=ds_type, database=str(tmp_path / f"f.{ds_type}")))
    try:
        rows = [client.execute_sync(sql).rows for sql, _ in _ORDINARY]
    finally:
        client.close()
        engine_factory.reset_cache()
    assert rows == [expected[ds_type] if isinstance(expected, dict) else expected for _, expected in _ORDINARY]


@pytest.mark.parametrize("path", ["sync", "async"])
def test_duckdb_writes_are_committed(tmp_path: Path, path: str) -> None:
    db = tmp_path / "w.duckdb"
    client = SlayerSQLClient(DatasourceConfig(name="ds", type="duckdb", database=str(db)))
    try:
        for sql in ("CREATE TABLE t (x INT)", "INSERT INTO t VALUES (1), (2)"):
            if path == "sync":
                client.execute_sync(sql)
            else:
                asyncio.run(client.execute(sql))
    finally:
        client.close()
        engine_factory.reset_cache()
    with duckdb.connect(str(db), read_only=True) as conn:
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (2,)
