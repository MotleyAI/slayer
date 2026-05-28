"""Tests for slayer.pg_facade.probes — datasource-aware connection probes."""

from __future__ import annotations

import sqlglot

from slayer.pg_facade.probes import match_pg_probe


def _parse(sql: str):
    return sqlglot.parse_one(sql, dialect="postgres")


def _probe(sql: str, *, datasource="jaffle", version_str="PostgreSQL 14.0 (SLayer)"):
    return match_pg_probe(_parse(sql), datasource=datasource, version_str=version_str)


def test_version_returns_pg_version_string() -> None:
    batch = _probe("SELECT version()")
    assert batch is not None
    assert batch.columns[0].name == "version"
    assert batch.rows == [{"version": "PostgreSQL 14.0 (SLayer)"}]


def test_current_database_returns_datasource() -> None:
    batch = _probe("SELECT current_database()", datasource="analytics")
    assert batch is not None
    assert batch.rows == [{"current_database": "analytics"}]


def test_current_schema_returns_public() -> None:
    batch = _probe("SELECT current_schema()")
    assert batch is not None
    assert batch.rows == [{"current_schema": "public"}]


def test_show_search_path() -> None:
    batch = _probe("SHOW search_path")
    assert batch is not None
    assert batch.columns[0].name == "search_path"
    assert batch.rows[0]["search_path"]


def test_show_unknown_setting_returns_empty() -> None:
    batch = _probe("SHOW some_unknown_setting")
    assert batch is not None
    assert batch.rows == [{"some_unknown_setting": ""}]


def test_current_setting_jit_off() -> None:
    batch = _probe("SELECT current_setting('jit')")
    assert batch is not None
    assert batch.rows == [{"current_setting": "off"}]


def test_set_config_returns_value() -> None:
    batch = _probe("SELECT set_config('jit', 'off', false)")
    assert batch is not None
    assert batch.rows == [{"set_config": "off"}]


def test_non_probe_returns_none() -> None:
    assert _probe("SELECT revenue_sum FROM orders") is None
    assert _probe("SELECT 1") is None  # delegated to the shared matcher
