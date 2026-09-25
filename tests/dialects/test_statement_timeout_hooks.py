"""Per-dialect statement-timeout hooks on ``SqlDialect``."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from slayer.sql.dialects import dialect_for_ds_type, get_dialect
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.clickhouse import ClickhouseDialect
from slayer.sql.dialects.duckdb import DuckdbDialect
from slayer.sql.dialects.mysql import MariadbDialect, MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.dialects.snowflake import SnowflakeDialect


class TestStatementTimeoutSql:
    def test_mysql_milliseconds(self) -> None:
        assert MysqlDialect().statement_timeout_sql(120) == "SET max_execution_time = 120000"

    def test_mariadb_seconds(self) -> None:
        assert MariadbDialect().statement_timeout_sql(120) == "SET max_statement_time = 120"

    def test_postgres_transaction_local(self) -> None:
        assert PostgresDialect().statement_timeout_sql(120) == "SET LOCAL statement_timeout = 120000"

    def test_snowflake_unchanged(self) -> None:
        assert SnowflakeDialect().statement_timeout_sql(120) == (
            "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120"
        )

    @pytest.mark.parametrize("ds_type", ["clickhouse", "duckdb", "sqlite", "bigquery", "mssql"])
    def test_no_statement(self, ds_type: str) -> None:
        assert dialect_for_ds_type(ds_type).statement_timeout_sql(120) is None

    @pytest.mark.parametrize("ds_type", [None, "", "totally_unknown", "postgresql"])
    def test_untyped_and_unknown_take_postgres(self, ds_type: str | None) -> None:
        dialect = dialect_for_ds_type(ds_type)
        assert dialect.statement_timeout_sql(120) == "SET LOCAL statement_timeout = 120000"
        assert dialect.statement_timeout_best_effort is True


class TestBestEffort:
    def test_default_fail_closed(self) -> None:
        assert SqlDialect().statement_timeout_best_effort is False

    @pytest.mark.parametrize("dialect", [MysqlDialect(), MariadbDialect(), SnowflakeDialect(), ClickhouseDialect()])
    def test_fail_closed_dialects(self, dialect: SqlDialect) -> None:
        assert dialect.statement_timeout_best_effort is False

    def test_postgres_best_effort(self) -> None:
        assert PostgresDialect().statement_timeout_best_effort is True


class TestMariadbRegistration:
    def test_ds_type_resolves_mariadb(self) -> None:
        assert type(dialect_for_ds_type("mariadb")) is MariadbDialect

    def test_mysql_ds_type_stays_mysql(self) -> None:
        assert type(dialect_for_ds_type("mysql")) is MysqlDialect

    def test_sqlglot_name_stays_mysql(self) -> None:
        assert type(get_dialect("mysql")) is MysqlDialect

    def test_mariadb_renders_as_mysql(self) -> None:
        assert MariadbDialect().sqlglot_name == "mysql"

    def test_alias_moves_to_mariadb(self) -> None:
        assert MariadbDialect().ds_type_aliases == frozenset({"mariadb"})
        assert MysqlDialect().ds_type_aliases == frozenset({"mysql"})


def _dbapi(settings: dict) -> SimpleNamespace:
    return SimpleNamespace(transport=SimpleNamespace(ch_settings=settings))


class TestPermissionHooks:
    def test_default_no_check(self) -> None:
        assert SqlDialect().timeout_permission_sql() is None

    def test_clickhouse_asks_readonly_level(self) -> None:
        assert ClickhouseDialect().timeout_permission_sql() == "SELECT getSetting('readonly')"

    @pytest.mark.parametrize(("level", "permitted"), [(0, True), (1, False), (2, True)])
    def test_clickhouse_readonly_levels(self, level: int, permitted: bool) -> None:
        assert ClickhouseDialect().timeout_permitted(level) is permitted


class TestConnectionTimeoutHooks:
    def test_default_leaves_connection_alone(self) -> None:
        settings = {"a": 1}
        conn = _dbapi(settings)
        prior = DuckdbDialect().set_connection_timeout(conn, 30)
        DuckdbDialect().restore_connection_timeout(conn, prior)
        assert settings == {"a": 1}

    def test_clickhouse_writes_request_setting(self) -> None:
        settings: dict = {"default_format": "TSV"}
        ClickhouseDialect().set_connection_timeout(_dbapi(settings), 30)
        assert settings == {"default_format": "TSV", "max_execution_time": 30}

    def test_clickhouse_restores_absence(self) -> None:
        settings: dict = {"default_format": "TSV"}
        conn = _dbapi(settings)
        prior = ClickhouseDialect().set_connection_timeout(conn, 30)
        ClickhouseDialect().restore_connection_timeout(conn, prior)
        assert settings == {"default_format": "TSV"}

    def test_clickhouse_restores_prior_value(self) -> None:
        settings: dict = {"max_execution_time": 77}
        conn = _dbapi(settings)
        prior = ClickhouseDialect().set_connection_timeout(conn, 30)
        assert settings == {"max_execution_time": 30}
        ClickhouseDialect().restore_connection_timeout(conn, prior)
        assert settings == {"max_execution_time": 77}

    def test_clickhouse_real_driver_connection(self) -> None:
        """Pins the undocumented ``transport.ch_settings`` on the real driver object."""
        connector = pytest.importorskip("clickhouse_sqlalchemy.drivers.http.connector")
        conn = connector.Connection("http://ch.invalid:8123/", "default", "u", "p", http_session=object())
        before = dict(conn.transport.ch_settings)
        prior = ClickhouseDialect().set_connection_timeout(conn, 30)
        assert conn.transport.ch_settings["max_execution_time"] == 30
        ClickhouseDialect().restore_connection_timeout(conn, prior)
        assert conn.transport.ch_settings == before
