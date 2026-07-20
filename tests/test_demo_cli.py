"""Unit tests for the Jaffle Shop demo CLI affordances."""

import argparse
import subprocess
import sys
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


class TestJafgenCmdResolution:
    def test_prefers_current_interpreter_when_package_importable(self, monkeypatch):
        monkeypatch.setattr(jaffle_shop, "find_spec", lambda name: object())
        monkeypatch.setattr(
            jaffle_shop.shutil,
            "which",
            lambda name: pytest.fail("PATH lookup should not run when jafgen is importable"),
        )

        cmd = jaffle_shop._jafgen_cmd(3)

        assert cmd[0] == sys.executable
        assert cmd[1] == "-c"
        assert "jafgen" in cmd[2]
        assert cmd[-1] == "3"

    def test_falls_back_to_path_lookup(self, monkeypatch):
        monkeypatch.setattr(jaffle_shop, "find_spec", lambda name: None)
        monkeypatch.setattr(jaffle_shop.shutil, "which", lambda name: "/usr/bin/jafgen")

        assert jaffle_shop._jafgen_cmd(2) == ["/usr/bin/jafgen", "2"]

    def test_missing_everywhere_raises_install_hint(self, monkeypatch):
        monkeypatch.setattr(jaffle_shop, "find_spec", lambda name: None)
        monkeypatch.setattr(jaffle_shop.shutil, "which", lambda name: None)

        with pytest.raises(RuntimeError, match="pip install jafgen"):
            jaffle_shop._jafgen_cmd(1)

    def test_years_clamped_to_minimum_one(self, monkeypatch):
        monkeypatch.setattr(jaffle_shop, "find_spec", lambda name: object())

        assert jaffle_shop._jafgen_cmd(0)[-1] == "1"
        assert jaffle_shop._jafgen_cmd(-5)[-1] == "1"

    def test_interpreter_entrypoint_import_path_is_valid(self):
        # Pins the `from jafgen.cli import app` import path used by the -c
        # fallback against future jafgen version bumps.
        result = subprocess.run(
            [sys.executable, "-c", "from jafgen.cli import app"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr


class TestEnsureDemoDatasourceScoping:
    def test_fast_path_with_models_in_multiple_datasources(self, tmp_path, monkeypatch):
        # Bare list_models()/get_model() raise on storages whose models span
        # several datasources (DEV-1330); the demo must scope every lookup to
        # its own datasource.
        from slayer.core.models import SlayerModel
        from slayer.storage.yaml_storage import YAMLStorage

        storage = YAMLStorage(base_dir=str(tmp_path))
        for table in jaffle_shop.TABLE_NAMES:
            jaffle_shop.run_sync(
                storage.save_model(
                    SlayerModel(name=table, sql_table=table, data_source="jaffle_shop")
                )
            )
        jaffle_shop.run_sync(
            storage.save_model(
                SlayerModel(name="unrelated", sql_table="unrelated", data_source="other_ds")
            )
        )
        monkeypatch.setattr(jaffle_shop, "build_jaffle_shop", lambda **kwargs: False)

        ds, models, db_built = jaffle_shop.ensure_demo_datasource(
            storage, storage_path=str(tmp_path)
        )

        assert db_built is False
        assert ds.name == "jaffle_shop"
        assert sorted(m.name for m in models) == sorted(jaffle_shop.TABLE_NAMES)


class TestDemoEnrichment:
    def _orders_model(self):
        from slayer.core.enums import DataType
        from slayer.core.models import Column, SlayerModel

        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="jaffle_shop",
            columns=[
                Column(name="id", type=DataType.TEXT, primary_key=True),
                Column(name="customer_id", type=DataType.TEXT),
                Column(name="ordered_at", type=DataType.DATE),
                Column(name="store_id", type=DataType.TEXT),
                Column(name="subtotal", type=DataType.DOUBLE),
                Column(name="tax_paid", type=DataType.DOUBLE),
                Column(name="order_total", type=DataType.DOUBLE),
            ],
        )

    def test_covers_every_demo_table(self):
        assert set(jaffle_shop.DEMO_ENRICHMENT) == set(jaffle_shop.TABLE_NAMES)

    def test_applies_labels_measures_and_aggregations(self):
        from slayer.core.format import NumberFormatType

        model = self._orders_model()
        assert jaffle_shop.apply_demo_enrichment(model) is True

        assert model.description is not None
        assert model.default_time_dimension == "ordered_at"
        by_name = {c.name: c for c in model.columns}
        assert by_name["order_total"].label == "Order Total"
        assert by_name["order_total"].format.type == NumberFormatType.CURRENCY
        assert "total_revenue" in {m.name for m in model.measures}
        assert "weighted_avg" in {a.name for a in model.aggregations}

    def test_idempotent_second_run_is_noop(self):
        model = self._orders_model()
        jaffle_shop.apply_demo_enrichment(model)
        snapshot = model.model_dump()
        assert jaffle_shop.apply_demo_enrichment(model) is False
        assert model.model_dump() == snapshot

    def test_preserves_user_edits(self):
        model = self._orders_model()
        by_name = {c.name: c for c in model.columns}
        by_name["order_total"].label = "My Label"
        model.description = "My description"
        jaffle_shop.apply_demo_enrichment(model)
        assert by_name["order_total"].label == "My Label"
        assert model.description == "My description"
        # Unset fields still get filled.
        assert by_name["subtotal"].label == "Net Sales"

    def test_auto_default_format_replaced_custom_format_preserved(self):
        from slayer.core.format import NumberFormat, NumberFormatType

        model = self._orders_model()
        by_name = {c.name: c for c in model.columns}
        # Auto-ingestion stamps a bare FLOAT format on numeric columns — the
        # curated currency format must win over it.
        by_name["order_total"].format = NumberFormat(type=NumberFormatType.FLOAT)
        # A user-tuned format must survive.
        by_name["subtotal"].format = NumberFormat(
            type=NumberFormatType.CURRENCY, symbol="€", precision=0
        )
        jaffle_shop.apply_demo_enrichment(model)
        assert by_name["order_total"].format.type == NumberFormatType.CURRENCY
        assert by_name["subtotal"].format.symbol == "€"

    def test_duplicate_measure_names_not_added(self):
        from slayer.core.models import ModelMeasure

        model = self._orders_model()
        model.measures = [ModelMeasure(name="total_revenue", formula="subtotal:sum")]
        jaffle_shop.apply_demo_enrichment(model)
        revenue = [m for m in model.measures if m.name == "total_revenue"]
        assert len(revenue) == 1
        assert revenue[0].formula == "subtotal:sum"

    def test_unknown_model_untouched(self):
        from slayer.core.models import SlayerModel

        model = SlayerModel(name="unrelated", sql_table="t", data_source="other")
        assert jaffle_shop.apply_demo_enrichment(model) is False

    def test_fast_path_saves_enriched_models(self, tmp_path, monkeypatch):
        from slayer.core.enums import DataType
        from slayer.core.models import Column, SlayerModel
        from slayer.storage.yaml_storage import YAMLStorage

        storage = YAMLStorage(base_dir=str(tmp_path))
        for table in jaffle_shop.TABLE_NAMES:
            jaffle_shop.run_sync(
                storage.save_model(
                    SlayerModel(
                        name=table,
                        sql_table=table,
                        data_source="jaffle_shop",
                        columns=[Column(name="id", type=DataType.TEXT, primary_key=True)],
                    )
                )
            )
        monkeypatch.setattr(jaffle_shop, "build_jaffle_shop", lambda **kwargs: False)

        _, models, _ = jaffle_shop.ensure_demo_datasource(
            storage, storage_path=str(tmp_path)
        )

        # Returned models are enriched, and the enrichment was persisted.
        assert all(m.description is not None for m in models)
        stored_orders = jaffle_shop.run_sync(
            storage.get_model(name="orders", data_source="jaffle_shop")
        )
        assert "order_count" in {m.name for m in stored_orders.measures}


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
        import duckdb

        db = tmp_path / "jaffle_shop.duckdb"
        # Create a real (empty) DuckDB file so the reuse path can open it.
        duckdb.connect(str(db)).close()

        shift_called = []
        monkeypatch.setattr(
            jaffle_shop,
            "shift_dates_to_today",
            lambda conn: shift_called.append(conn),
        )

        assert jaffle_shop.build_jaffle_shop(db_path=str(db)) is False
        assert len(shift_called) == 1, "reuse path must refresh dates"
