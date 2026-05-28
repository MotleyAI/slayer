"""Tests for slayer.facade.probe_queries — the connection-probe whitelist.

The shared matcher returns a pyarrow-free ``RowBatch``.
"""

from __future__ import annotations

import pytest
import sqlglot

import slayer
from slayer.core.enums import DataType
from slayer.facade.probe_queries import match_probe
from slayer.facade.rows import RowBatch


def _parse(sql: str):
    return sqlglot.parse_one(sql)


def test_select_one_matches() -> None:
    batch = match_probe(_parse("SELECT 1"))
    assert batch is not None
    assert batch.columns[0].name == "1"
    assert batch.columns[0].type == DataType.INT
    assert batch.rows == [{"1": 1}]


def test_select_one_case_insensitive() -> None:
    assert match_probe(_parse("select 1")) is not None
    assert match_probe(_parse("Select 1")) is not None


def test_select_one_with_alias_does_not_match() -> None:
    assert match_probe(_parse("SELECT 1 AS foo")) is None


def test_select_one_with_from_does_not_match() -> None:
    assert match_probe(_parse("SELECT 1 FROM orders")) is None


def test_select_null_where_false() -> None:
    batch = match_probe(_parse("SELECT NULL WHERE 1=0"))
    assert batch is not None
    assert batch.rows == []
    assert batch.columns[0].name == "NULL"
    assert batch.columns[0].type == DataType.INT


def test_select_null_where_false_reverse_operands() -> None:
    assert match_probe(_parse("SELECT NULL WHERE 0=1")) is not None


def test_select_null_where_true_does_not_match() -> None:
    assert match_probe(_parse("SELECT NULL WHERE 1=1")) is None


def test_select_version_function() -> None:
    batch = match_probe(_parse("SELECT version()"))
    assert batch is not None
    assert batch.columns[0].name == "version"
    assert batch.columns[0].type == DataType.TEXT
    assert batch.rows == [{"version": f"SLayer Flight SQL {slayer.__version__}"}]


def test_select_at_at_version() -> None:
    batch = match_probe(_parse("SELECT @@version"))
    assert batch is not None
    assert batch.rows[0]["version"].startswith("SLayer Flight SQL ")


def test_select_current_database() -> None:
    batch = match_probe(_parse("SELECT current_database()"))
    assert batch is not None
    assert batch.columns[0].name == "current_database"
    assert batch.columns[0].type == DataType.TEXT
    assert batch.rows == [{"current_database": "slayer"}]


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
def test_every_canned_batch_is_well_formed(sql: str) -> None:
    batch = match_probe(_parse(sql))
    assert isinstance(batch, RowBatch)
    # Single-column responses across the board.
    assert len(batch.columns) == 1
