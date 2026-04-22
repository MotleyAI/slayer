"""Unit tests for the Jaffle Shop demo CLI affordances."""

import argparse
from pathlib import Path

import pytest

from slayer import cli
from slayer.demo import jaffle_shop


def _make_args(**overrides):
    defaults = dict(
        storage=None,
        models_dir=None,
        connection_string="demo",
        name=None,
        description=None,
        ingest=False,
        schema=None,
        include=None,
        exclude=None,
        years=1,
        yes=True,
        demo=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestDemoKeywordDispatch:
    def test_demo_keyword_routes_to_demo_handler(self, monkeypatch):
        called = {}

        def fake_handler(*args, **kwargs):
            called["args"] = kwargs.get("args", args[0] if args else None)
            called["storage"] = kwargs.get("storage", args[1] if len(args) > 1 else None)

        monkeypatch.setattr(cli, "_run_datasources_create_demo", fake_handler)

        cli._run_datasources_create(
            args=_make_args(connection_string="demo"), storage=object()
        )

        assert "args" in called, "expected demo handler to be invoked"

    def test_demo_keyword_is_case_insensitive(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            cli,
            "_run_datasources_create_demo",
            lambda *args, **kwargs: called.append(1),
        )

        cli._run_datasources_create(
            args=_make_args(connection_string="DEMO"), storage=object()
        )
        cli._run_datasources_create(
            args=_make_args(connection_string=" demo "), storage=object()
        )

        assert len(called) == 2

    def test_non_demo_connection_string_falls_through(self, monkeypatch):
        monkeypatch.setattr(
            cli,
            "_run_datasources_create_demo",
            lambda *args, **kwargs: pytest.fail("demo handler should not run for URLs"),
        )

        # Force the normal path to exit early without hitting storage.
        def fake_parse(_url):
            raise ValueError("stop here")

        monkeypatch.setattr(cli, "_parse_connection_string", fake_parse)
        with pytest.raises(SystemExit):
            cli._run_datasources_create(
                args=_make_args(connection_string="postgresql://host/db"),
                storage=object(),
            )


class TestMissingDependencyHints:
    def test_missing_duckdb_prints_install_hint(self, tmp_path, monkeypatch, capsys):
        def raise_missing():
            raise jaffle_shop.DemoDependencyError(
                jaffle_shop._missing_deps_message(["duckdb"])
            )

        monkeypatch.setattr(jaffle_shop, "check_dependencies", raise_missing)

        with pytest.raises(SystemExit) as exc:
            cli._run_datasources_create_demo(
                args=_make_args(storage=str(tmp_path)),
                storage=object(),
            )

        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "pip install duckdb" in out

    def test_missing_jafgen_prints_install_hint(self, tmp_path, monkeypatch, capsys):
        def raise_missing():
            raise jaffle_shop.DemoDependencyError(
                jaffle_shop._missing_deps_message(["jafgen"])
            )

        monkeypatch.setattr(jaffle_shop, "check_dependencies", raise_missing)

        with pytest.raises(SystemExit) as exc:
            cli._run_datasources_create_demo(
                args=_make_args(storage=str(tmp_path)),
                storage=object(),
            )

        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "jaffle-shop-generator" in out
        assert "pip install" in out


class TestServeMcpDemoHook:
    def test_serve_demo_flag_calls_prepare_demo(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            cli, "_prepare_demo", lambda *args, **kwargs: calls.append("prepare")
        )
        monkeypatch.setattr(cli, "_resolve_storage", lambda *args, **kwargs: "STORAGE")

        # Stub out create_app + uvicorn.run.
        import sys as _sys
        import types as _types

        fake_api = _types.ModuleType("slayer.api.server")
        fake_api.create_app = lambda *args, **kwargs: "APP"
        monkeypatch.setitem(_sys.modules, "slayer.api.server", fake_api)
        fake_uvicorn = _types.ModuleType("uvicorn")
        fake_uvicorn.run = lambda *args, **kwargs: calls.append(
            f"uvicorn:{args[0] if args else kwargs.get('app')}:{kwargs.get('host')}:{kwargs.get('port')}"
        )
        monkeypatch.setitem(_sys.modules, "uvicorn", fake_uvicorn)

        args = argparse.Namespace(host="h", port=1, storage=None, models_dir=None, demo=True)
        cli._run_serve(args=args)

        assert calls == ["prepare", "uvicorn:APP:h:1"]

    def test_mcp_demo_flag_calls_prepare_demo(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            cli, "_prepare_demo", lambda *args, **kwargs: calls.append("prepare")
        )
        monkeypatch.setattr(cli, "_resolve_storage", lambda *args, **kwargs: "STORAGE")

        import sys as _sys
        import types as _types

        class _FakeMCP:
            def run(self):
                calls.append("mcp.run")

        fake_mcp = _types.ModuleType("slayer.mcp.server")
        fake_mcp.create_mcp_server = lambda *args, **kwargs: _FakeMCP()
        monkeypatch.setitem(_sys.modules, "slayer.mcp.server", fake_mcp)

        args = argparse.Namespace(storage=None, models_dir=None, demo=True)
        cli._run_mcp(args=args)

        assert calls == ["prepare", "mcp.run"]


class TestResolveDemoDbPath:
    def test_yaml_directory_storage(self, tmp_path):
        storage_dir = tmp_path / "slayer_data"
        result = jaffle_shop.resolve_demo_db_path(str(storage_dir))
        assert Path(result) == storage_dir / "demo" / "jaffle_shop.duckdb"

    def test_sqlite_file_storage_uses_parent_dir(self, tmp_path):
        db = tmp_path / "slayer.db"
        db.touch()
        result = jaffle_shop.resolve_demo_db_path(str(db))
        assert Path(result) == tmp_path / "demo" / "jaffle_shop.duckdb"


class TestBuildJaffleShopIdempotency:
    def test_returns_false_and_reshifts_when_db_exists(self, tmp_path, monkeypatch):
        duckdb = pytest.importorskip("duckdb")

        db = tmp_path / "jaffle_shop.duckdb"
        # Create a real (empty) DuckDB file so the reuse path can open it.
        duckdb.connect(str(db)).close()

        def boom():
            raise AssertionError("check_dependencies should not be called on the reuse path")

        shift_called = []
        monkeypatch.setattr(jaffle_shop, "check_dependencies", boom)
        monkeypatch.setattr(
            jaffle_shop,
            "shift_dates_to_today",
            lambda conn: shift_called.append(conn),
        )

        assert jaffle_shop.build_jaffle_shop(db_path=str(db)) is False
        assert len(shift_called) == 1, "reuse path must refresh dates"
