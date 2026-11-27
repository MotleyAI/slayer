"""Documents are kept as written: every upstream ontology and mapping example round-trips byte-identical.

Spec: ontologies/ontology-documents › Ontologies and mappings are stored verbatim (Byte round trip).
Enforces architecture/python.ontology.arc42.md §3.2.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.osi import exporter
from tests._cli_inprocess import run_cli_in_process
from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, SAP, SAP_DS, SAP_MAPPING, empty_store, example, offline, run_import, write,
)

#: (files imported together, datasource, ontology, mapping)
RUNS = {
    "flights": (("flights.ontology.yaml", "flights.semantic_model.yaml", "flights.mapping.yaml"), FLIGHTS_DS,
                FLIGHTS, FLIGHTS_MAPPING),
    "sap": (("sap_business_partner.ontology.yaml", "sap_business_partner.semantic_model.yaml",
             "sap_business_partner.mapping.yaml"), SAP_DS, SAP, SAP_MAPPING),
}


@pytest.mark.parametrize("run", sorted(RUNS))
@pytest.mark.parametrize("kind", STORAGES)
async def test_every_example_round_trips(tmp_path: Path, kind: str, run: str,
                                         monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    files, data_source, ontology, mapping = RUNS[run]
    storage = await empty_store(kind, tmp_path, data_source)
    for name in files:
        write(tmp_path / "in" / name, example(name))
    assert (await run_import(storage, tmp_path / "in", data_source=data_source)).refused == []
    ontology_file, _, mapping_file = files
    assert await exporter.export_ontology(storage=storage, name=ontology) == example(ontology_file)
    assert await exporter.export_ontology_mapping(storage=storage, data_source=data_source,
                                                  name=mapping) == example(mapping_file)


@pytest.mark.parametrize("kind", STORAGES)
async def test_byte_round_trip(tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Byte round trip."""
    offline(monkeypatch)
    await empty_store(kind, tmp_path, "anywhere")
    source = write(tmp_path / "foaf_owl_import.yaml", example("foaf_owl_import.yaml"))
    store = str(tmp_path / ("store" if kind == "yaml" else "store.db"))
    imported = run_cli_in_process(["import-osi", str(source), "--datasource", "anywhere", "--storage", store])
    assert imported.returncode == 0, imported.stderr
    exported = run_cli_in_process(["export-osi", "--ontology", "foaf", "--out", str(tmp_path / "out.yaml"),
                                   "--storage", store])
    assert exported.returncode == 0, exported.stderr
    assert (tmp_path / "out.yaml").read_bytes() == source.read_bytes()


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_saved_document_reads_back_as_its_text(tmp_path: Path, kind: str) -> None:
    storage = await empty_store(kind, tmp_path)
    text = example("foaf_owl_import.yaml")
    await storage.save_ontology_text(name="foaf", text=text)
    assert await storage.get_ontology_text("foaf") == text
