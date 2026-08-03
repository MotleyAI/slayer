"""Tests for CLI helpers."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml

from slayer.cli import _parse_cli_variables, _parse_connection_string, _run_datasources, _run_query
from slayer.core.models import DatasourceConfig


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


class TestParseCliVariables:
    def test_invalid_json_exits_cleanly(self):
        """Malformed --variables-json must produce a clean SystemExit, not a
        bare ``json.JSONDecodeError`` traceback to the user.
        """
        args = SimpleNamespace(variables=None, variables_json="{not valid json")
        with pytest.raises(SystemExit) as exc_info:
            _parse_cli_variables(args)
        assert "invalid JSON" in str(exc_info.value)

    def test_valid_json_object_returned(self):
        args = SimpleNamespace(variables=None, variables_json='{"a": 1, "b": "x"}')
        assert _parse_cli_variables(args) == {"a": 1, "b": "x"}

    def test_json_non_object_exits(self):
        args = SimpleNamespace(variables=None, variables_json='[1, 2, 3]')
        with pytest.raises(SystemExit, match="must decode to a JSON object"):
            _parse_cli_variables(args)


class TestRunDatasources:
    def test_show_masks_all_credentials(self, monkeypatch, capsys):
        datasource = DatasourceConfig(
            name="mydb",
            type="bigquery",
            password="PASSWORD_SENTINEL",
            connection_string="bigquery://project",
            credentials_json='{"private_key": "PRIVATE_KEY_SENTINEL"}',
        )
        storage = SimpleNamespace(get_datasource=AsyncMock(return_value=datasource))
        monkeypatch.setattr("slayer.cli._resolve_storage", lambda _args: storage)

        _run_datasources(SimpleNamespace(datasources_command="show", name="mydb"))

        data = yaml.safe_load(capsys.readouterr().out)
        assert data["password"] == "********"
        assert data["connection_string"] == "********"
        assert data["credentials_json"] == "********"


class TestRunQueryFileLoading:
    """`slayer query @<path>` reads the JSON from disk; missing/unreadable
    files must produce a clean ``Error: ...`` + exit-1 rather than a Python
    traceback. Regression for the inconsistency CodeRabbit flagged on
    PR #70 (discussion r3177821627)."""

    def _args(self, query_input: str, tmp_path) -> SimpleNamespace:
        return SimpleNamespace(
            query_json=query_input,
            variables=None,
            variables_json=None,
            storage=str(tmp_path / "storage"),
            models_dir=None,
            dry_run=False,
            explain=False,
            format="table",
        )

    def test_missing_file_exits_cleanly(self, tmp_path):
        args = self._args(f"@{tmp_path}/does-not-exist.json", tmp_path)
        with pytest.raises(SystemExit, match="Query file not found"):
            _run_query(args)

    def test_unreadable_file_exits_cleanly(self, tmp_path):
        # A directory passed where a file is expected → IsADirectoryError,
        # which is an OSError subclass and should hit the OSError branch.
        somedir = tmp_path / "actually-a-dir"
        somedir.mkdir()
        args = self._args(f"@{somedir}", tmp_path)
        with pytest.raises(SystemExit, match="Error reading query file"):
            _run_query(args)
