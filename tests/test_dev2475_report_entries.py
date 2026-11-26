"""Ontology findings are report-only ``validate_datasets`` entries on every surface, redacted by access.

Spec: ontologies/ontology-use › Ontology findings are report-only validation entries (Force-clean ignores ontology
findings).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import RelationshipCardinality
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import REPORT_ONLY_FINDINGS
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import make_storage
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, hide, view,
)
from tests._dev2475_fixtures import (
    aircraft_relationship, findings_module, flights_engine, flights_model, ontology_findings, text_of, validate,
    with_relationship,
)

CHECKS = {"identifier", "constraint", "required_link", "not_checked"}


def ontology_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in entries if e.get("finding") == "ontology"]


def test_ontology_findings_are_report_only() -> None:
    assert findings_module().OntologyFinding in REPORT_ONLY_FINDINGS


async def test_the_python_api_returns_typed_findings(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        found = ontology_findings(await validate(engine))
    assert {f.check for f in found} >= CHECKS
    assert all(isinstance(f, findings_module().OntologyFinding) for f in found)
    assert all((f.mapping, f.ontology, f.data_source) == (FLIGHTS_MAPPING, FLIGHTS, FLIGHTS_DS) for f in found)
    assert all(f.summary for f in found)


async def test_mcp_returns_findings_as_json(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, aircraft_relationship(RelationshipCardinality.ONE_TO_MANY))
        entries = json.loads(await mcp_text(engine.storage, "validate_datasets", data_source=FLIGHTS_DS))
    found = ontology_entries(entries)
    assert {e["check"] for e in found} >= {*CHECKS, "multiplicity"}
    assert [e["relationship"] for e in found if e["check"] == "multiplicity"] == ["Flight.aircraft"]
    assert all(e["mapping"] == FLIGHTS_MAPPING and e["ontology"] == FLIGHTS for e in found)


async def test_rest_returns_findings_as_json(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, aircraft_relationship(RelationshipCardinality.ONE_TO_MANY))
        response = TestClient(create_app(storage=engine.storage)).post("/validate-datasets",
                                                                      json={"data_source": FLIGHTS_DS})
    assert response.status_code == 200, response.text
    assert {e["check"] for e in ontology_entries(response.json())} >= {*CHECKS, "multiplicity"}


async def stored_texts(engine: SlayerQueryEngine) -> tuple[Any, ...]:
    storage = engine.storage
    return (await storage.get_ontology_text(FLIGHTS),
            await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS),
            await storage.get_semantic_model_text(FLIGHTS_MODEL, data_source=FLIGHTS_DS))


def cli(tmp_path: Path, *args: str) -> Any:
    return run_cli_in_process(["validate-datasets", "--datasource", FLIGHTS_DS, *args,
                               "--storage", str(tmp_path / "store")])


async def test_the_cli_prints_findings(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path):
        pass
    text = cli(tmp_path)
    assert text.returncode == 0, text.stderr
    assert all(s in text.stdout for s in ("DegreesLatitude <= 90", "COUNT[Airport] > 0", "Airport.latitude",
                                          "tail_nr")), text.stdout
    as_json = cli(tmp_path, "--format", "json")
    assert as_json.returncode == 0, as_json.stderr
    assert {e["check"] for e in ontology_entries(json.loads(as_json.stdout)["drift"])} >= CHECKS


async def test_force_clean_ignores_ontology_findings(tmp_path: Path) -> None:
    """Spec scenario: Force-clean ignores ontology findings."""
    async with flights_engine("yaml", tmp_path, model=flights_model(typed=True)) as engine:
        before = await stored_texts(engine)
        datasets = sorted(d.model_dump_json() for d in (await engine.storage.load_datasets(data_source=FLIGHTS_DS))[0])
    result = cli(tmp_path, "--force-clean", "--yes")
    assert result.returncode == 0, result.stderr
    assert "DegreesLatitude <= 90" in result.stdout
    storage = make_storage("yaml", tmp_path)
    assert (await storage.get_ontology_text(FLIGHTS), await storage.get_ontology_mapping_text(
        FLIGHTS_MAPPING, FLIGHTS_DS), await storage.get_semantic_model_text(
        FLIGHTS_MODEL, data_source=FLIGHTS_DS)) == before
    assert sorted(d.model_dump_json() for d in (await storage.load_datasets(data_source=FLIGHTS_DS))[0]) == datasets


async def test_a_caller_who_cannot_see_aircraft_gets_no_finding_naming_it(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        storage = make_storage("yaml", tmp_path)
        await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
        caller = SlayerQueryEngine(storage=view(storage))
        try:
            found = ontology_findings(await caller.validate_datasets(data_source=FLIGHTS_DS))
        finally:
            await caller.aclose()
        everything = ontology_findings(await validate(engine))
    assert found
    assert not [text_of(f) for f in found if "AIRCRAFT" in text_of(f)]
    assert [text_of(f) for f in everything if "AIRCRAFT" in text_of(f)]
