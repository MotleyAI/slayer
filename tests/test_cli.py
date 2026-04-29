"""Tests for CLI helpers."""

import argparse
import json
from types import SimpleNamespace

import pytest

from slayer.cli import _parse_connection_string, _parse_variables, _run_queries


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


class TestParseVariables:
    def test_empty_returns_empty_dict(self):
        assert _parse_variables(None) == {}
        assert _parse_variables("") == {}

    def test_int_and_float_coercion(self):
        assert _parse_variables("a=1,b=2.5,c=hello") == {"a": 1, "b": 2.5, "c": "hello"}

    def test_skips_blank_pairs(self):
        # Trailing comma → empty pair, ignored.
        assert _parse_variables("a=1,") == {"a": 1}

    def test_strips_whitespace(self):
        assert _parse_variables("  a = 1 ,  b=hi  ") == {"a": 1, "b": "hi"}

    def test_missing_equals_raises(self):
        with pytest.raises(ValueError, match="key=value form"):
            _parse_variables("broken")

    def test_empty_key_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _parse_variables("=value")

    def test_only_whitespace_key_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _parse_variables("   =value")


def _queries_args(**overrides) -> argparse.Namespace:
    """Construct an argparse Namespace for the `slayer queries …` CLI path."""
    base = dict(
        command="queries",
        queries_command=None,
        storage=None,
        models_dir=None,
        name=None,
        file=None,
        variables=None,
        format="json",
        dry_run=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


class TestQueriesCliVariablesErrors:
    """`slayer queries run/inspect --variables <bad>` should exit cleanly, not traceback."""

    def test_run_bad_variables_exits_with_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            "slayer.cli._resolve_storage", lambda args: SimpleNamespace()
        )
        with pytest.raises(SystemExit) as exc_info:
            _run_queries(
                _queries_args(queries_command="run", name="some_query", variables="broken")
            )
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "Error:" in out
        assert "key=value form" in out

    def test_inspect_bad_variables_exits_with_error(self, monkeypatch, capsys):
        # storage.get_query must return *something* to reach _parse_variables —
        # the inspect path checks for the named query first.
        async def _fake_get_query(name):
            return SimpleNamespace(name=name)

        monkeypatch.setattr(
            "slayer.cli._resolve_storage",
            lambda args: SimpleNamespace(get_query=_fake_get_query),
        )
        with pytest.raises(SystemExit) as exc_info:
            _run_queries(
                _queries_args(queries_command="inspect", name="some_query", variables="=oops")
            )
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "Error:" in out
        assert "non-empty" in out


class TestQueriesInspectMetadata:
    """`slayer queries inspect` should preserve label/format on each column,
    matching the MCP `inspect_query` tool."""

    def test_inspect_includes_label_and_format(self, monkeypatch, capsys):
        from slayer.core.format import NumberFormat, NumberFormatType
        from slayer.core.query import SlayerQuery
        from slayer.engine.query_engine import (
            FieldMetadata,
            ResponseAttributes,
            SlayerResponse,
        )

        nq = SimpleNamespace(
            name="q",
            description="desc",
            stages=[SlayerQuery(source_model="orders")],
            variables={},
            unsupplied_variables=lambda: set(),
            model_copy=lambda update: SimpleNamespace(
                name="q",
                description="desc",
                stages=update["stages"],
                variables={},
            ),
        )

        async def _fake_get_query(name):
            return nq

        fake_storage = SimpleNamespace(get_query=_fake_get_query)

        attrs = ResponseAttributes(
            dimensions={"orders.month": FieldMetadata(label="Month")},
            measures={
                "orders.revenue_sum": FieldMetadata(
                    label="Revenue",
                    format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="$"),
                ),
                "orders.bare": FieldMetadata(),
            },
        )
        fake_response = SlayerResponse(
            data=[],
            columns=["orders.month", "orders.revenue_sum", "orders.bare"],
            attributes=attrs,
        )

        class _FakeEngine:
            def __init__(self, *args, **kwargs):
                pass

            def execute_sync(self, *args, **kwargs):
                return fake_response

        monkeypatch.setattr("slayer.cli._resolve_storage", lambda args: fake_storage)
        monkeypatch.setattr("slayer.engine.query_engine.SlayerQueryEngine", _FakeEngine)

        _run_queries(_queries_args(queries_command="inspect", name="q"))
        out = capsys.readouterr().out
        result = json.loads(out)

        cols = {c["name"]: c for c in result["columns"]}
        assert cols["orders.month"]["kind"] == "dimension"
        assert cols["orders.month"]["label"] == "Month"
        assert "format" not in cols["orders.month"]

        assert cols["orders.revenue_sum"]["kind"] == "measure"
        assert cols["orders.revenue_sum"]["label"] == "Revenue"
        assert cols["orders.revenue_sum"]["format"]["type"] == "currency"
        assert cols["orders.revenue_sum"]["format"]["symbol"] == "$"

        # No metadata → only name and kind, no label/format keys.
        assert cols["orders.bare"] == {"name": "orders.bare", "kind": "measure"}
