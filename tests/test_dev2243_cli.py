"""Spec: cli/commands (export-osi, semantic-models, ossie-roundtrip) and the import-osi exit status."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._dev2243_fixtures import CORPUS, DS, SALES, dataset, document_text, field, make_storage, store_document

M4 = CORPUS / "compliance" / "portability" / "m4" / "m4_cross_dataset.ossie.yaml"
SUPPORT = document_text(name="support", datasets=[
    dataset("returns", primary_key=["return_id"], fields=[field("return_id"), field("customer_id")]),
])
STAFF = document_text(name="staff", datasets=[
    dataset("managers", primary_key=["manager_id"], fields=[field("manager_id"), field("name")]),
])


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("database accessed")


@pytest.fixture
def offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any database client construction fails."""
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)


async def make_store(base: Path, *documents: tuple[str, str]) -> tuple[StorageBackend, Path]:
    """A YAML store under ``base`` holding ``(data_source, text)`` documents; datasources point at a missing db."""
    storage = make_storage("yaml", base)
    for data_source in sorted({ds for ds, _ in documents} | {DS}):
        await storage.save_datasource(DatasourceConfig(name=data_source, type="duckdb",
                                                       database=str(base / "absent" / f"{data_source}.duckdb")))
    for data_source, text in documents:
        await store_document(storage, text, data_source=data_source)
    return storage, base / "store"


def error_lines(result: CliResult) -> list[str]:
    return [line for line in result.stderr.splitlines() if line.startswith("Error: ")]


def snapshot(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


# --------------------------------------------------------------------------- #
# export-osi writes an Ossie document
# --------------------------------------------------------------------------- #
async def test_export_to_yaml(tmp_path: Path, offline: None) -> None:
    """Export to YAML."""
    _, store = await make_store(tmp_path, (DS, SALES))
    out = tmp_path / "shop.osi.yaml"
    result = run_cli_in_process(["export-osi", "--datasource", DS, "--out", str(out), "--storage", str(store)])
    assert result.returncode == 0, result.stderr
    assert out.read_text(encoding="utf-8") == SALES
    assert result.stdout.strip()


async def test_export_to_json(tmp_path: Path, offline: None) -> None:
    _, store = await make_store(tmp_path, (DS, SALES))
    out = tmp_path / "shop.osi.json"
    result = run_cli_in_process(["export-osi", "--datasource", DS, "--out", str(out), "--storage", str(store)])
    assert result.returncode == 0, result.stderr
    assert json.loads(out.read_text(encoding="utf-8")) == yaml.safe_load(SALES)


async def test_unknown_model(tmp_path: Path, offline: None) -> None:
    """Unknown model."""
    _, store = await make_store(tmp_path, (DS, SALES))
    out = tmp_path / "shop.osi.yaml"
    result = run_cli_in_process(["export-osi", "--datasource", DS, "--datasets", "nope", "--out", str(out),
                                 "--storage", str(store)])
    assert result.returncode == 1
    [line] = error_lines(result)
    assert "nope" in line
    assert not out.exists()


async def test_unknown_datasource_writes_no_file(tmp_path: Path, offline: None) -> None:
    _, store = await make_store(tmp_path, (DS, SALES))
    out = tmp_path / "ghost.osi.yaml"
    result = run_cli_in_process(["export-osi", "--datasource", "ghost", "--out", str(out), "--storage", str(store)])
    assert result.returncode == 1
    [line] = error_lines(result)
    assert "ghost" in line
    assert not out.exists()


# --------------------------------------------------------------------------- #
# semantic-models lists, shows and deletes documents
# --------------------------------------------------------------------------- #
async def test_list_prints_datasource_name_and_datasets(tmp_path: Path, offline: None) -> None:
    _, store = await make_store(tmp_path, (DS, SALES), (DS, SUPPORT), ("hr", STAFF))
    result = run_cli_in_process(["semantic-models", "list", "--storage", str(store)])
    assert result.returncode == 0, result.stderr
    for text in (DS, "sales", "orders", "customers", "support", "returns", "hr", "staff", "managers"):
        assert text in result.stdout, text
    filtered = run_cli_in_process(["semantic-models", "list", "--datasource", DS, "--storage", str(store)])
    assert filtered.returncode == 0, filtered.stderr
    assert "sales" in filtered.stdout and "support" in filtered.stdout
    assert "staff" not in filtered.stdout


async def test_show_prints_the_stored_text(tmp_path: Path, offline: None) -> None:
    """Show prints the stored text."""
    _, store = await make_store(tmp_path, (DS, SALES))
    result = run_cli_in_process(["semantic-models", "show", "sales", "--datasource", DS, "--storage", str(store)])
    assert result.returncode == 0, result.stderr
    assert SALES in result.stdout


async def test_show_unknown_document(tmp_path: Path, offline: None) -> None:
    _, store = await make_store(tmp_path)
    result = run_cli_in_process(["semantic-models", "show", "nope", "--datasource", DS, "--storage", str(store)])
    assert result.returncode == 1
    [line] = error_lines(result)
    assert "nope" in line


async def test_delete_with_yes_deletes_the_document_and_its_datasets(tmp_path: Path, offline: None) -> None:
    storage, store = await make_store(tmp_path, (DS, SALES), (DS, SUPPORT))
    result = run_cli_in_process(["semantic-models", "delete", "sales", "--datasource", DS, "--yes",
                                 "--storage", str(store)])
    assert result.returncode == 0, result.stderr
    assert await storage.list_semantic_models(DS) == [(DS, "support")]
    names = await storage.list_datasets(DS)
    assert "orders" not in names and "customers" not in names
    assert "returns" in names


async def test_delete_without_yes_asks_and_keeps_on_decline(tmp_path: Path, offline: None) -> None:
    storage, store = await make_store(tmp_path, (DS, SALES))
    result = run_cli_in_process(["semantic-models", "delete", "sales", "--datasource", DS, "--storage", str(store)])
    assert "--yes" in result.stdout + result.stderr
    assert await storage.get_semantic_model_text("sales", DS) == SALES


async def test_unknown_document(tmp_path: Path, offline: None) -> None:
    """Unknown document."""
    _, store = await make_store(tmp_path)
    result = run_cli_in_process(["semantic-models", "delete", "nope", "--datasource", DS, "--yes",
                                 "--storage", str(store)])
    assert result.returncode == 1
    [line] = error_lines(result)
    assert "nope" in line


# --------------------------------------------------------------------------- #
# ossie-roundtrip imports and exports one document
# --------------------------------------------------------------------------- #
async def _user_store(base: Path) -> tuple[Path, dict[str, str]]:
    _, store = await make_store(base)
    assert snapshot(store)
    return store, {**os.environ, "SLAYER_STORAGE": str(store)}


async def test_a_valid_document_round_trips_unchanged(tmp_path: Path) -> None:
    """A valid document round-trips unchanged."""
    store, env = await _user_store(tmp_path / "user")
    before = snapshot(store)
    out = tmp_path / "out.yaml"
    result = run_cli_in_process(["ossie-roundtrip", str(M4), str(out)], env=env)
    assert result.returncode == 0, result.stderr
    assert out.read_text(encoding="utf-8") == M4.read_text(encoding="utf-8")
    assert snapshot(store) == before


async def test_roundtrip_writes_json_for_a_json_output(tmp_path: Path) -> None:
    store, env = await _user_store(tmp_path / "user")
    out = tmp_path / "out.json"
    result = run_cli_in_process(["ossie-roundtrip", str(M4), str(out)], env=env)
    assert result.returncode == 0, result.stderr
    assert json.loads(out.read_text(encoding="utf-8")) == yaml.safe_load(M4.read_text(encoding="utf-8"))


async def test_roundtrip_of_a_refused_document_exits_non_zero_with_its_reasons(tmp_path: Path) -> None:
    store, env = await _user_store(tmp_path / "user")
    before = snapshot(store)
    bad = tmp_path / "bad.yaml"
    bad.write_text(SALES.replace("version: 0.3.0.dev0", "version: 0.4.0"), encoding="utf-8")
    result = run_cli_in_process(["ossie-roundtrip", str(bad), str(tmp_path / "out.yaml")], env=env)
    assert result.returncode != 0
    assert "0.4.0" in result.stdout + result.stderr
    assert snapshot(store) == before


# --------------------------------------------------------------------------- #
# import-osi exit status (importers/osi-import)
# --------------------------------------------------------------------------- #
async def test_import_osi_stores_a_valid_document(tmp_path: Path, offline: None) -> None:
    storage, store = await make_store(tmp_path)
    path = tmp_path / "in" / "sales.yaml"
    path.parent.mkdir()
    path.write_text(SALES, encoding="utf-8")
    result = run_cli_in_process(["import-osi", str(path), "--datasource", DS, "--storage", str(store)])
    assert result.returncode == 0, result.stdout + result.stderr
    assert await storage.get_semantic_model_text("sales", DS) == SALES


async def test_mixed_shape_exits_non_zero_and_stores_the_other_files(tmp_path: Path, offline: None) -> None:
    """Mixed shape (command exit status)."""
    storage, store = await make_store(tmp_path)
    folder = tmp_path / "in"
    folder.mkdir()
    mixed = {"semantic_model": [{"name": "a", "datasets": [dataset("orders", fields=[field("order_id")])]}],
             "datasets": [dataset("orders", fields=[field("order_id")])]}
    (folder / "mixed.yaml").write_text(yaml.safe_dump(mixed, sort_keys=False), encoding="utf-8")
    (folder / "support.yaml").write_text(SUPPORT, encoding="utf-8")
    result = run_cli_in_process(["import-osi", str(folder), "--datasource", DS, "--storage", str(store)])
    assert result.returncode != 0
    assert "mixed.yaml" in result.stdout + result.stderr
    assert await storage.get_semantic_model_text("support", DS) == SUPPORT
    assert await storage.list_semantic_models(DS) == [(DS, "support")]
