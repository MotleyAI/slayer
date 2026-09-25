"""ClickHouse per-statement timeout: ``SETTINGS max_execution_time`` on the query itself."""

from __future__ import annotations

import sqlglot

import pytest

from slayer.sql.client import _with_ch_statement_timeout


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
