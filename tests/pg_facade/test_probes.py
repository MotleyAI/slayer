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


def test_show_transaction_isolation_level_multiword() -> None:
    # pgjdbc's getTransactionIsolation() spelling; c3p0 calls it at pool init.
    batch = _probe("SHOW TRANSACTION ISOLATION LEVEL")
    assert batch is not None
    assert batch.rows == [{"transaction_isolation": "read committed"}]


def test_show_time_zone_multiword() -> None:
    batch = _probe("SHOW TIME ZONE")
    assert batch is not None
    assert batch.rows == [{"timezone": "UTC"}]


def test_current_catalog_niladic_returns_datasource() -> None:
    # pgjdbc's PgConnection.getCatalog() — called by Metabase's c3p0 pool on
    # every new connection — issues the no-parens spelling.
    batch = _probe("SELECT current_catalog", datasource="analytics")
    assert batch is not None
    assert batch.rows == [{"current_catalog": "analytics"}]


def test_current_schema_niladic_returns_public() -> None:
    batch = _probe("SELECT current_schema")
    assert batch is not None
    assert batch.rows == [{"current_schema": "public"}]


def test_current_user_and_session_user_return_constant() -> None:
    for sql in ("SELECT current_user", "SELECT session_user"):
        batch = _probe(sql)
        assert batch is not None, sql
        assert batch.rows == [{"current_user": "slayer"}]


def test_show_search_path() -> None:
    batch = _probe("SHOW search_path")
    assert batch is not None
    assert batch.columns[0].name == "search_path"
    assert batch.rows[0]["search_path"]


def test_show_server_version_is_bare_version_not_full_string() -> None:
    # SHOW server_version must match ParameterStatus / pg_settings ("14.0"),
    # NOT the full "PostgreSQL 14.0 (SLayer ...)" version() string.
    from slayer.pg_facade.identity import PG_SERVER_VERSION

    batch = _probe("SHOW server_version", version_str="PostgreSQL 14.0 (SLayer)")
    assert batch is not None
    assert batch.rows == [{"server_version": PG_SERVER_VERSION}]


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
