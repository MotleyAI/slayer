"""``export-osi --ontology`` / ``--mapping`` and the Python export API write stored documents as stored.

Spec: importers/osi-export › Ontologies and mappings export as stored.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from slayer.osi import exporter
from tests._cli_inprocess import run_cli_in_process
from tests._dev2243_fixtures import CORPUS, STORAGES, make_storage
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, empty_store, example, flights_store, load, offline,
    run_import, write,
)

SCHEMA_URLS = {
    "core-spec/ossie-schema.json": ("https://github.com/apache/ossie/core-spec/ossie-schema.json",
                                    "https://raw.githubusercontent.com/apache/ossie/main/core-spec/ossie-schema.json"),
    "ontology/ontology.json": ("https://github.com/apache/ossie/ontology/ontology.json",
                               "https://raw.githubusercontent.com/apache/ossie/main/ontology/ontology.json"),
    "ontology/mapping.json": ("https://github.com/apache/ossie/ontology/mapping.json",
                              "https://raw.githubusercontent.com/apache/ossie/main/ontology/mapping.json"),
}


def schema_errors(document: Any, schema_file: str) -> list[str]:
    """Errors of ``document`` against a vendored Ossie schema, every reference resolved from the corpus."""
    schemas = {path: json.loads((CORPUS / path).read_text(encoding="utf-8")) for path in SCHEMA_URLS}
    registry = Registry().with_resources([(url, Resource.from_contents(schemas[path]))
                                          for path, urls in SCHEMA_URLS.items() for url in urls])
    validator = Draft202012Validator(schemas[schema_file], registry=registry)
    return [error.message for error in validator.iter_errors(document)]


def export(store: Path, out: Path, *args: str) -> Any:
    return run_cli_in_process(["export-osi", *args, "--out", str(out), "--storage", str(store)])


@pytest.mark.parametrize("kind", STORAGES)
async def test_ontology_export_is_byte_identical(tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Ontology export is byte-identical."""
    offline(monkeypatch)
    storage = await empty_store(kind, tmp_path, FLIGHTS_DS)
    source = write(tmp_path / "flights.ontology.yaml", example("flights.ontology.yaml"))
    assert (await run_import(storage, source, data_source=FLIGHTS_DS)).refused == []
    store = tmp_path / ("store" if kind == "yaml" else "store.db")
    result = export(store, tmp_path / "out.yaml", "--ontology", FLIGHTS)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "out.yaml").read_bytes() == source.read_bytes()


async def test_upgraded_embedded_mapping_exports_standalone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Upgraded embedded mapping exports standalone."""
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    imported = await run_import(storage, write(tmp_path / "flights.yaml", example("flights.yaml")),
                                data_source=FLIGHTS_DS)
    assert imported.refused == []
    result = export(tmp_path / "store", tmp_path / "m.yaml", "--mapping", FLIGHTS_MAPPING, "--datasource", FLIGHTS_DS)
    assert result.returncode == 0, result.stderr
    document = load((tmp_path / "m.yaml").read_text(encoding="utf-8"))
    assert schema_errors(document, "ontology/mapping.json") == []
    assert document["ontology_ref"]["name"] == FLIGHTS
    assert document["semantic_model_ref"]["name"] == FLIGHTS_MODEL

    ontology = export(tmp_path / "store", tmp_path / "o.yaml", "--ontology", FLIGHTS)
    assert ontology.returncode == 0, ontology.stderr
    exported = load((tmp_path / "o.yaml").read_text(encoding="utf-8"))
    assert "ontology_mappings" not in exported
    assert schema_errors(exported, "ontology/ontology.json") == []


async def test_unknown_ontology(tmp_path: Path) -> None:
    """Spec scenario: Unknown ontology."""
    await empty_store("yaml", tmp_path)
    result = export(tmp_path / "store", tmp_path / "o.yaml", "--ontology", "Nope")
    assert result.returncode != 0
    assert any(line.startswith("Error:") and "Nope" in line for line in result.stderr.splitlines()), result.stderr
    assert not (tmp_path / "o.yaml").exists()


async def test_unknown_mapping(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path)
    result = export(tmp_path / "store", tmp_path / "m.yaml", "--mapping", "nope_mapping", "--datasource", FLIGHTS_DS)
    assert result.returncode != 0
    assert any(line.startswith("Error:") and "nope_mapping" in line for line in result.stderr.splitlines()), \
        result.stderr


@pytest.mark.parametrize("args", [
    pytest.param(["--ontology", FLIGHTS, "--datasource", FLIGHTS_DS], id="ontology-with-datasource"),
    pytest.param(["--ontology", FLIGHTS, "--datasets", "FLIGHT"], id="ontology-with-datasets"),
    pytest.param(["--mapping", FLIGHTS_MAPPING], id="mapping-without-datasource"),
])
async def test_flag_combinations_are_refused(tmp_path: Path, args: list[str]) -> None:
    await flights_store("yaml", tmp_path)
    result = export(tmp_path / "store", tmp_path / "x.yaml", *args)
    assert result.returncode != 0
    assert "unrecognized arguments" not in result.stderr
    assert not (tmp_path / "x.yaml").exists()


async def test_a_json_output_file_gets_json(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path)
    result = export(tmp_path / "store", tmp_path / "o.json", "--ontology", FLIGHTS)
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "o.json").read_text(encoding="utf-8")) == load(example("flights.ontology.yaml"))
    mapping = export(tmp_path / "store", tmp_path / "m.json", "--mapping", FLIGHTS_MAPPING, "--datasource", FLIGHTS_DS)
    assert mapping.returncode == 0, mapping.stderr
    assert json.loads((tmp_path / "m.json").read_text(encoding="utf-8")) == load(example("flights.mapping.yaml"))


async def test_the_datasource_export_is_unchanged_by_ontologies(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path)
    result = export(tmp_path / "store", tmp_path / "s.yaml", "--datasource", FLIGHTS_DS)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "s.yaml").read_text(encoding="utf-8") == example("flights.semantic_model.yaml")


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_python_export_api(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path)
    assert await exporter.export_ontology(storage=storage, name=FLIGHTS) == example("flights.ontology.yaml")
    assert await exporter.export_ontology_mapping(storage=storage, data_source=FLIGHTS_DS,
                                                  name=FLIGHTS_MAPPING) == example("flights.mapping.yaml")
    with pytest.raises(exporter.OsiExportError, match="Nope"):
        await exporter.export_ontology(storage=storage, name="Nope")
    reopened = make_storage(kind, tmp_path)
    with pytest.raises(exporter.OsiExportError, match="nope_mapping"):
        await exporter.export_ontology_mapping(storage=reopened, data_source=FLIGHTS_DS, name="nope_mapping")
