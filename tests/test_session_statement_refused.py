"""The default client refuses session-changing and transaction-control statements (spec: database/client-contract)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core.models import DatasourceConfig
from slayer.database import client as sql_client
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient, SqlClientError

_URLS = {
    "postgres": "postgresql+psycopg://u@db.invalid/d",
    "mysql": "mysql+pymysql://u@db.invalid/d",
    "snowflake": "snowflake://u@acct/d",
    "sqlserver": "mssql+pyodbc://u@db.invalid/d",
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
    ("postgres", "END", "END"),
    ("postgres", "abort", "ABORT"),
    ("postgres", "SAVEPOINT a", "SAVEPOINT"),
    ("postgres", "RELEASE SAVEPOINT a", "RELEASE"),
    ("sqlserver", "SAVE TRANSACTION a", "SAVE"),
    ("mysql", "XA START 'x'", "XA"),
    ("postgres", "SELECT 1; END", "END"),
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
# Queries are unaffected
# ---------------------------------------------------------------------------

_ORDINARY = [
    ("SELECT 'SET x' AS \"USE\"", [{"USE": "SET x"}]),
    ("-- BEGIN\nSELECT 1 AS one", [{"one": 1}]),
    ("SELECT 1 AS one /* ; COMMIT */", [{"one": 1}]),
    ("WITH a AS (SELECT 1 AS x) SELECT x FROM a", [{"x": 1}]),
]


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_ordinary_statements_run(tmp_path: Path, ds_type: str) -> None:
    path = tmp_path / f"f.{ds_type}"
    if ds_type == "duckdb":
        duckdb.connect(str(path)).close()
    else:
        path.touch()
    client = SlayerSQLClient(DatasourceConfig(name="ds", type=ds_type, database=str(path)))
    try:
        rows = [client.execute_sync(sql).rows for sql, _ in _ORDINARY]
    finally:
        client.close()
        engine_factory.reset_cache()
    assert rows == [expected for _, expected in _ORDINARY]
