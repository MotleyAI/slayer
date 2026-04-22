"""Tests for CLI helpers."""

import pytest

from slayer.cli import _parse_connection_string


class TestParseConnectionString:
    def test_postgres_url(self):
        assert _parse_connection_string("postgresql://user:pw@host:5432/my_db") == ("postgres", "my_db")

    def test_postgres_short_scheme(self):
        assert _parse_connection_string("postgres://host/analytics") == ("postgres", "analytics")

    def test_postgres_with_driver_suffix(self):
        assert _parse_connection_string("postgresql+psycopg2://host/warehouse") == (
            "postgres",
            "warehouse",
        )

    def test_mysql_with_driver_suffix(self):
        assert _parse_connection_string("mysql+pymysql://u:p@h/shop") == ("mysql", "shop")

    def test_clickhouse_http(self):
        assert _parse_connection_string("clickhouse+http://localhost:8123/events") == (
            "clickhouse",
            "events",
        )

    def test_sqlite_file_path(self):
        assert _parse_connection_string("sqlite:///var/data/app.db") == ("sqlite", "app")

    def test_sqlite_relative_path(self):
        # urlparse treats this as scheme + relative path; stem is "app".
        assert _parse_connection_string("sqlite:///app.db") == ("sqlite", "app")

    def test_duckdb_file_path(self):
        assert _parse_connection_string("duckdb:///tmp/warehouse.duckdb") == (
            "duckdb",
            "warehouse",
        )

    def test_missing_scheme_raises(self):
        with pytest.raises(ValueError, match="missing a scheme"):
            _parse_connection_string("localhost/mydb")

    def test_empty_db_path_raises(self):
        with pytest.raises(ValueError, match="Cannot derive a name"):
            _parse_connection_string("postgresql://host:5432")

    def test_sqlite_no_path_raises(self):
        with pytest.raises(ValueError, match="Cannot derive a name"):
            _parse_connection_string("sqlite://")
