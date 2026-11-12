"""The default client sends only reads, except into an in-memory database (spec: database/client-contract)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Generator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core.models import DatasourceConfig
from slayer.database import client as sql_client
from slayer.database import engine_factory
from slayer.database.client import SessionStatementRefused, SlayerSQLClient, SqlClientError
from slayer.sql.dialects import PostgresDialect, TsqlDialect, dialect_for_ds_type
from slayer.sql.dialects.base import ExplainStatements

_NETWORKED = {
    "postgres": "postgresql+psycopg://u@db.invalid/d",
    "sqlserver": "mssql+pyodbc://u@db.invalid/d",
}

# (text, leading keyword the error names)
_WRITES = [
    ("CREATE TABLE t (x INT)", "CREATE"),
    ("INSERT INTO t VALUES (1)", "INSERT"),
    ("UPDATE t SET x = 2", "UPDATE"),
    ("delete from t", "DELETE"),
    ("DROP TABLE t", "DROP"),
    ("TRUNCATE t", "TRUNCATE"),
    ("CALL p()", "CALL"),
    ("PRAGMA query_only = OFF", "PRAGMA"),
    ("SELECT 1 AS x INTO t2", "SELECT"),
    ("WITH d AS (DELETE FROM t RETURNING x) SELECT x FROM d", "WITH"),
    ("WITH n AS (SELECT 1 AS x) INSERT INTO t SELECT x FROM n", "WITH"),
    ("EXPLAIN SELECT 1", "EXPLAIN"),
    ("EXPLAIN ANALYZE CREATE TABLE t AS SELECT 1", "EXPLAIN"),
    ("SELECT 1; DELETE FROM t", "DELETE"),
]

_DATASOURCES = ["duckdb_file", "sqlite_file", "postgres"]


def _write_refused() -> type[Exception]:
    return sql_client.WriteStatementRefused


def _datasource(kind: str) -> DatasourceConfig:
    if kind == "postgres":
        return DatasourceConfig(name="ds", type="postgres", connection_string=_NETWORKED["postgres"])
    ds_type = kind.removesuffix("_file")
    return DatasourceConfig(name="ds", type=ds_type, database=f"/nonexistent/f.{ds_type}")


@pytest.fixture
def nothing_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    def _refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a refused text reached an engine")

    monkeypatch.setattr(engine_factory, "get_engine", _refuse)
    monkeypatch.setattr(sql_client, "create_async_engine", _refuse)


def test_write_refusal_is_a_typed_client_error() -> None:
    assert issubclass(_write_refused(), SqlClientError)


# ---------------------------------------------------------------------------
# Writes, EXPLAIN and a trailing write are refused unsent
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize("kind", _DATASOURCES)
@pytest.mark.parametrize(("sql", "keyword"), _WRITES)
def test_sync_refuses_and_sends_nothing(kind: str, sql: str, keyword: str) -> None:
    client, refused = SlayerSQLClient(_datasource(kind)), _write_refused()
    with pytest.raises(refused, match=f"(?i){keyword}"):
        client.execute_sync(sql)


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize("kind", _DATASOURCES)
@pytest.mark.parametrize(("sql", "keyword"), _WRITES)
def test_async_refuses_and_sends_nothing(kind: str, sql: str, keyword: str) -> None:
    executing = SlayerSQLClient(_datasource(kind)).execute(sql)
    with pytest.raises(_write_refused(), match=f"(?i){keyword}"):
        asyncio.run(executing)


# ---------------------------------------------------------------------------
# Write keywords in literals, quoted names and comments do not count
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _file_backed(tmp_path: Path, ds_type: str) -> Generator[SlayerSQLClient]:
    path = tmp_path / f"f.{ds_type}"
    if ds_type == "duckdb":
        duckdb.connect(str(path)).close()
    else:
        path.touch()
    ds = DatasourceConfig(name="ds", type=ds_type, database=str(path))
    client = SlayerSQLClient(ds)
    try:
        yield client
    finally:
        client.close()
        engine_factory.invalidate_engine(ds)


_READS = [
    ("SELECT 'DELETE' AS \"INSERT\"", [{"INSERT": "DELETE"}]),
    ("-- DROP TABLE t\nSELECT 1 AS one", [{"one": 1}]),
    ("SELECT 'SET x' AS \"USE\"", [{"USE": "SET x"}]),
    ("-- BEGIN\nSELECT 1 AS one", [{"one": 1}]),
    ("WITH a AS (SELECT 1 AS x) SELECT x FROM a", [{"x": 1}]),
]


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
@pytest.mark.parametrize(("sql", "rows"), _READS)
def test_reads_run(tmp_path: Path, ds_type: str, sql: str, rows: list[dict[str, Any]]) -> None:
    with _file_backed(tmp_path, ds_type) as client:
        assert client.execute_sync(sql).rows == rows


# ---------------------------------------------------------------------------
# Statements without separators are refused unsent
# ---------------------------------------------------------------------------

_UNSEPARATED = ["SELECT 1 AS x TRUNCATE TABLE t", "SELECT 1 AS x DROP TABLE t", "SELECT 1 AS x DBCC SHRINKDATABASE(0)"]


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize("sql", _UNSEPARATED)
def test_statements_without_separators_are_refused(sql: str) -> None:
    client = SlayerSQLClient(DatasourceConfig(name="ds", type="sqlserver", connection_string=_NETWORKED["sqlserver"]))
    with pytest.raises(_write_refused()):
        client.execute_sync(sql)
    executing = client.execute(sql)
    with pytest.raises(_write_refused()):
        asyncio.run(executing)


@pytest.mark.parametrize(
    ("ds_type", "sql"),
    [
        ("duckdb", "FROM t"),
        ("postgres", "SHOW statement_timeout"),
        ("clickhouse", "SHOW TABLES"),
        ("postgres", "VALUES (1)"),
        ("postgres", "DESCRIBE t"),
        ("postgres", "(SELECT 1) UNION ALL (SELECT 2)"),
        ("mysql", "DESCRIBE t"),
        ("snowflake", "DESC TABLE t"),
        ("mysql", "SELECT 1 /* a comment */"),
    ],
)
def test_parsed_reads_are_admitted(ds_type: str, sql: str) -> None:
    assert sql_client.admit_statement(sql, dialect=dialect_for_ds_type(ds_type)) is False


@pytest.mark.parametrize(
    ("ds_type", "sql"),
    [
        ("postgres", "TABLE t"), ("duckdb", "SUMMARIZE t"), ("postgres", "SELECT FROM WHERE"),
        ("mysql", "SHOW TABLES INTO OUTFILE '/tmp/x'"), ("clickhouse", "SHOW TABLES INTO OUTFILE 'x'"),
        ("mysql", "EXPLAIN SELECT 1"), ("mysql", "DESCRIBE SELECT 1"), ("mysql", "EXPLAIN t"),
        ("mysql", "/*! SET SESSION TRANSACTION READ WRITE */"), ("mysql", "/*! DELETE FROM t */"),
        ("mysql", "SELECT 1 /*!, (SELECT 2) */"), ("mysql", "/*M! DELETE FROM t */"),
        ("postgres", "-- nothing"), ("postgres", ""),
    ],
)
def test_texts_not_parsed_as_reads_are_refused(ds_type: str, sql: str) -> None:
    dialect, refused = dialect_for_ds_type(ds_type), _write_refused()
    with pytest.raises(refused):
        sql_client.admit_statement(sql, dialect=dialect)


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_explain_is_refused_on_an_in_memory_database(ds_type: str) -> None:
    dialect, refused = dialect_for_ds_type(ds_type), _write_refused()
    with pytest.raises(refused, match="EXPLAIN"):
        sql_client.admit_statement("EXPLAIN SELECT 1", dialect=dialect, writes_allowed=True)


# ---------------------------------------------------------------------------
# explain() refuses a plan of a write, sending nothing
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize(
    ("ds_type", "statements"),
    [
        ("postgres", ExplainStatements(statement="EXPLAIN ANALYZE INSERT INTO t VALUES (1)")),
        ("postgres", PostgresDialect().explain_statements("DELETE FROM t")),
        ("sqlserver", TsqlDialect().explain_statements("DELETE FROM t")),
        ("sqlserver", ExplainStatements(statement="SELECT 1; DELETE FROM t", session_on="SET SHOWPLAN_ALL ON",
                                        session_off="SET SHOWPLAN_ALL OFF")),
    ],
)
def test_explain_refuses_a_plan_of_a_write(ds_type: str, statements: ExplainStatements) -> None:
    client = SlayerSQLClient(DatasourceConfig(name="ds", type=ds_type, connection_string=_NETWORKED[ds_type]))
    explaining = client.explain(statements)
    with pytest.raises(SqlClientError):
        asyncio.run(explaining)


@pytest.mark.usefixtures("nothing_sent")
@pytest.mark.parametrize(
    ("ds_type", "statements"),
    [
        ("sqlserver", ExplainStatements(statement="SELECT 1", session_on="DROP TABLE t", session_off="SET SHOWPLAN_ALL OFF")),
        ("postgres", ExplainStatements(statement="SELECT 1", session_on="SET SHOWPLAN_ALL ON",
                                       session_off="SET SHOWPLAN_ALL OFF")),
    ],
)
def test_explain_switches_plan_mode_only_with_the_dialects_own_statements(
    ds_type: str, statements: ExplainStatements,
) -> None:
    client = SlayerSQLClient(DatasourceConfig(name="ds", type=ds_type, connection_string=_NETWORKED[ds_type]))
    explaining = client.explain(statements)
    with pytest.raises(SessionStatementRefused):
        asyncio.run(explaining)


# ---------------------------------------------------------------------------
# An in-memory database accepts writes, but still refuses session statements
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type", ["sqlite", "duckdb"])
def test_an_in_memory_database_accepts_writes(ds_type: str) -> None:
    ds = DatasourceConfig(name=f"mem_{ds_type}", type=ds_type, database=":memory:")
    client = SlayerSQLClient(ds)
    try:
        client.execute_sync("CREATE TABLE t (x INT)")
        client.execute_sync("INSERT INTO t VALUES (1)")
        rows = client.execute_sync("SELECT x FROM t").rows
        with pytest.raises(SessionStatementRefused):
            client.execute_sync("SET x = 1")
    finally:
        client.close()
        engine_factory.invalidate_engine(ds)
    assert rows == [{"x": 1}]
