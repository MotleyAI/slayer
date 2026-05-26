"""Tests for slayer.flight.probe_queries — the connection-probe whitelist."""

from __future__ import annotations

import pyarrow as pa
import pytest
import sqlglot

import slayer
from slayer.flight.probe_queries import match_probe


def _parse(sql: str):
    return sqlglot.parse_one(sql)


def test_select_one_matches() -> None:
    table = match_probe(_parse("SELECT 1"))
    assert table is not None
    assert table.schema.field("1").type == pa.int64()
    assert table.to_pylist() == [{"1": 1}]


def test_select_one_case_insensitive() -> None:
    assert match_probe(_parse("select 1")) is not None
    assert match_probe(_parse("Select 1")) is not None


def test_select_one_with_alias_does_not_match() -> None:
    # `SELECT 1 AS foo` is a different probe (and not in the whitelist).
    # We don't match because the projection is an Alias wrapping the Literal.
    assert match_probe(_parse("SELECT 1 AS foo")) is None


def test_select_one_with_from_does_not_match() -> None:
    assert match_probe(_parse("SELECT 1 FROM orders")) is None


def test_select_null_where_false() -> None:
    table = match_probe(_parse("SELECT NULL WHERE 1=0"))
    assert table is not None
    assert table.num_rows == 0
    assert table.schema.field("NULL").type == pa.int64()


def test_select_null_where_false_reverse_operands() -> None:
    # Permissive on argument order: 0=1 is a valid restatement of 1=0.
    table = match_probe(_parse("SELECT NULL WHERE 0=1"))
    assert table is not None


def test_select_null_where_true_does_not_match() -> None:
    # WHERE 1=1 is NOT the no-rows probe; should not match.
    assert match_probe(_parse("SELECT NULL WHERE 1=1")) is None


def test_select_version_function() -> None:
    table = match_probe(_parse("SELECT version()"))
    assert table is not None
    assert table.schema.field("version").type == pa.utf8()
    rows = table.to_pylist()
    assert rows == [{"version": f"SLayer Flight SQL {slayer.__version__}"}]


def test_select_at_at_version() -> None:
    table = match_probe(_parse("SELECT @@version"))
    assert table is not None
    rows = table.to_pylist()
    assert rows[0]["version"].startswith("SLayer Flight SQL ")


def test_select_current_database() -> None:
    table = match_probe(_parse("SELECT current_database()"))
    assert table is not None
    assert table.schema.field("current_database").type == pa.utf8()
    assert table.to_pylist() == [{"current_database": "slayer"}]


def test_unmatched_select_returns_none() -> None:
    assert match_probe(_parse("SELECT * FROM orders")) is None
    assert match_probe(_parse("SELECT id, status FROM orders")) is None
    assert match_probe(_parse("SELECT 2")) is None
    assert match_probe(_parse("SELECT 'string-literal'")) is None
    assert match_probe(_parse("SELECT version() FROM orders")) is None


def test_non_select_statement_returns_none() -> None:
    assert match_probe(_parse("INSERT INTO orders VALUES (1)")) is None
    assert match_probe(_parse("DELETE FROM orders")) is None


def test_select_one_with_group_by_does_not_match() -> None:
    assert match_probe(_parse("SELECT 1 GROUP BY 1")) is None


def test_select_one_with_limit_does_not_match() -> None:
    assert match_probe(_parse("SELECT 1 LIMIT 1")) is None


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT NULL WHERE 1=0",
        "SELECT version()",
        "SELECT @@version",
        "SELECT current_database()",
    ],
)
def test_every_canned_table_is_well_formed(sql: str) -> None:
    table = match_probe(_parse(sql))
    assert isinstance(table, pa.Table)
    # Single-column responses across the board.
    assert len(table.schema) == 1
