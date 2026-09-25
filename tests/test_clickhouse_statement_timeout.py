"""ClickHouse per-statement timeout: ``SETTINGS max_execution_time`` on the query itself."""

from __future__ import annotations

from unittest.mock import MagicMock

import sqlglot

import pytest

from slayer.sql.client import (
    _TYPE_PROBE_TIMEOUT_SECONDS,
    _ch_readonly_engines,
    _get_column_types_sync,
    _with_ch_statement_timeout,
)


def _settings(sql: str) -> list[str]:
    """Every ``SETTINGS`` entry in the statement, in order, across all clauses."""
    ast = sqlglot.parse_one(sql, dialect="clickhouse")
    return [
        s.sql(dialect="clickhouse")
        for node in ast.walk()
        for s in (node.args.get("settings") or [])
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t",
        "WITH x AS (SELECT 1 AS c) SELECT * FROM x",
        "SELECT * FROM (SELECT a FROM t) AS q",
    ],
)
def test_adds_timeout_setting(sql: str) -> None:
    assert _settings(_with_ch_statement_timeout(sql=sql, timeout_seconds=30)) == ["max_execution_time = 30"]


def test_adds_timeout_setting_without_from() -> None:
    # sqlglot can't re-parse a FROM-less SETTINGS clause, so compare text.
    assert _with_ch_statement_timeout(sql="SELECT 1", timeout_seconds=30) == "SELECT 1 SETTINGS max_execution_time = 30"


def test_keeps_existing_settings_in_one_clause() -> None:
    sql = "SELECT a FROM t SETTINGS allow_experimental_correlated_subqueries = 1"
    out = _with_ch_statement_timeout(sql=sql, timeout_seconds=30)
    assert out.count("SETTINGS") == 1
    assert _settings(out) == [
        "allow_experimental_correlated_subqueries = 1",
        "max_execution_time = 30",
    ]


def test_union_gets_one_trailing_clause() -> None:
    sql = "SELECT a FROM t UNION ALL SELECT b FROM u SETTINGS max_threads = 2"
    out = _with_ch_statement_timeout(sql=sql, timeout_seconds=30)
    assert out.count("SETTINGS") == 1
    assert _settings(out) == ["max_threads = 2", "max_execution_time = 30"]


def test_timeout_already_in_sql_wins() -> None:
    sql = "SELECT a FROM t SETTINGS max_execution_time = 5"
    assert _with_ch_statement_timeout(sql=sql, timeout_seconds=30) == sql


@pytest.mark.parametrize("sql", ["SHOW TABLES", "SELECT FROM WHERE (((", "SELECT 'abc"])
def test_non_query_or_unparseable_sql_runs_unchanged(sql: str) -> None:
    assert _with_ch_statement_timeout(sql=sql, timeout_seconds=30) == sql


def _fake_engine(conn: MagicMock) -> MagicMock:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    result = MagicMock()
    result.keys.return_value = ["col"]
    result.cursor.description = [("col", "UInt8", None, None, None, None, None)]
    conn.exec_driver_sql.return_value = result
    return engine


def _executed_sql(conn: MagicMock) -> list[str]:
    return [c.args[0] for c in conn.exec_driver_sql.call_args_list]


def test_type_probe_carries_timeout_setting() -> None:
    conn = MagicMock()
    types = _get_column_types_sync("SELECT 1 AS col", db_type="clickhouse", engine=_fake_engine(conn))
    assert types == {"col": "number"}
    (probe,) = _executed_sql(conn)
    assert _settings(probe) == [f"max_execution_time = {_TYPE_PROBE_TIMEOUT_SECONDS}"]


def test_type_probe_readonly_user_falls_back_without_setting() -> None:
    conn = MagicMock()
    engine = _fake_engine(conn)
    ok = conn.exec_driver_sql.return_value
    conn.exec_driver_sql.side_effect = [Exception("Code: 164. DB::Exception: readonly"), ok]
    _get_column_types_sync("SELECT 1 AS col", db_type="clickhouse", engine=engine)
    timed, plain = _executed_sql(conn)
    assert "max_execution_time" in timed
    assert "max_execution_time" not in plain
    assert engine in _ch_readonly_engines
