"""Every ingest caller, the demo included, prints an ingest result the same way (specs/cli/commands)."""

from __future__ import annotations

import argparse
import io
from pathlib import Path
from typing import Any

import pytest

from slayer import cli
from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.demo import jaffle_shop
from slayer.engine import ingestion
from slayer.engine.ingestion import SkippedTable, ingest_all_datasources_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import IdempotentIngestResult, IngestionError, ModelAddition, WholeModelDelete
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import CliResult, run_cli_in_process

DS = jaffle_shop.DEMO_NAME
FIXED = IdempotentIngestResult(
    additions=[
        ModelAddition(model_name="orders", data_source=DS, created=True, new_columns=["id", "amount"]),
        ModelAddition(model_name="customers", data_source=DS, new_columns=["email"]),
    ],
    to_delete=[WholeModelDelete(model_name="gone", data_source=DS)],
    skipped=[SkippedTable(table_name="odd", reason="not modellable")],
    errors=[IngestionError(model_name="widgets", data_source=DS, error="save failed")],
)


@pytest.fixture
def fixed_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real ingest runs (so the demo models get stored), but every caller is handed ``FIXED``."""
    real = ingestion.ingest_datasource_idempotent

    async def fake(**kwargs: Any) -> IdempotentIngestResult:
        await real(**kwargs)
        return FIXED

    monkeypatch.setattr(ingestion, "ingest_datasource_idempotent", fake)


def _sqlite(tmp_path: Path) -> str:
    db = str(tmp_path / "app.db")
    with transaction(db) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, amount REAL)")
    return db


def _storage_with_sqlite_ds(tmp_path: Path, name: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=str(tmp_path / name))
    run_sync(storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=_sqlite(tmp_path))))
    return storage


def _cli(args: list[str]) -> CliResult:
    return run_cli_in_process(args, uncaught_exit=True)


def _golden(tmp_path: Path) -> str:
    storage = _storage_with_sqlite_ds(tmp_path, "ingest_store")
    result = _cli(["ingest", "--storage", storage.base_dir, "--datasource", DS])
    assert result.stdout.strip(), result.stderr
    return result.stdout


class TestOneRenderer:
    def test_golden_has_every_part(self, fixed_result: None, tmp_path: Path) -> None:
        golden = _golden(tmp_path)
        for part in ("Created: orders", "Updated: customers", "Pending drift", "Skipped (1)", "Errors (1)"):
            assert part in golden, golden

    def test_create_url_prints_the_golden(self, fixed_result: None, tmp_path: Path) -> None:
        golden = _golden(tmp_path)
        result = _cli(["datasources", "--storage", str(tmp_path / "url_store"), "create",
                       f"sqlite:///{_sqlite(tmp_path)}", "--name", DS, "--ingest"])
        assert result.returncode == 0, result.stderr
        assert result.stdout == f"Created datasource '{DS}' (sqlite).\n{golden}"

    def test_create_demo_prints_the_golden(self, fixed_result: None, jaffle_generator, tmp_path: Path) -> None:
        golden = _golden(tmp_path)
        result = _cli(["datasources", "--storage", str(tmp_path / "demo_store"), "create", "demo", "--ingest"])
        assert result.returncode == 0, result.stderr
        head, sep, rest = result.stdout.partition(f"Created datasource '{DS}' (duckdb).\n")
        assert sep and head.startswith("Generated Jaffle Shop DuckDB at "), result.stdout
        assert rest == golden

    async def test_startup_orchestrator_prints_the_golden(self, fixed_result: None, tmp_path: Path) -> None:
        golden = _golden(tmp_path)
        storage = _storage_with_sqlite_ds(tmp_path, "startup_store")
        engine = SlayerQueryEngine(storage=storage)
        stream = io.StringIO()
        try:
            await ingest_all_datasources_idempotent(storage=engine.storage, client_for=engine.client_for, stream=stream)
        finally:
            await engine.aclose()
        assert f"Ingesting datasource '{DS}'…\n{golden}Datasource '{DS}': ingested\n" in stream.getvalue()

    def test_slayer_ingest_keeps_its_stricter_exit(self, fixed_result: None, jaffle_generator, tmp_path: Path) -> None:
        storage = _storage_with_sqlite_ds(tmp_path, "strict_store")
        assert _cli(["ingest", "--storage", storage.base_dir, "--datasource", DS]).returncode == 1
        demo = _cli(["datasources", "--storage", str(tmp_path / "demo_store"), "create", "demo", "--ingest"])
        assert demo.returncode == 0, demo.stderr


class TestDemoCreate:
    def test_prints_a_created_line_per_demo_model(self, jaffle_generator, tmp_path: Path) -> None:
        result = _cli(["datasources", "--storage", str(tmp_path / "store"), "create", "demo", "--ingest"])
        assert result.returncode == 0, result.stderr
        for table in jaffle_shop.TABLE_NAMES:
            assert f"Created: {table} " in result.stdout, result.stdout
        assert "Ingested:" not in result.stdout

    def test_an_ingest_that_raises_fails_once_and_keeps_the_datasource(
        self, jaffle_generator, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def boom(**_kwargs: Any) -> IdempotentIngestResult:
            raise RuntimeError("boom")

        monkeypatch.setattr(ingestion, "ingest_datasource_idempotent", boom)
        store = tmp_path / "store"
        result = _cli(["datasources", "--storage", str(store), "create", "demo", "--ingest"])
        assert result.returncode == 1
        failures = [line for line in result.stderr.splitlines() if "Ingestion failed:" in line]
        assert len(failures) == 1 and "boom" in failures[0], result.stderr
        assert run_sync(YAMLStorage(base_dir=str(store)).get_datasource(DS)) is not None


def test_prepare_demo_reports_the_ready_demo(jaffle_generator, tmp_path: Path) -> None:
    store = str(tmp_path / "store")
    out = io.StringIO()
    cli._prepare_demo(argparse.Namespace(storage=store, models_dir=None), YAMLStorage(base_dir=store), stream=out)
    assert "Demo ready: generated " in out.getvalue()
    assert f"datasource '{DS}', {len(jaffle_shop.TABLE_NAMES)} model(s) available." in out.getvalue()
