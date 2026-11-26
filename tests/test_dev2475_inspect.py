"""``inspect`` shows concept annotations and an ontology's concept graph, on every surface.

Spec: ontologies/ontology-use › Inspect shows annotations and the concept graph; ontologies/ontology-documents ›
Ontologies and mappings are reachable from every surface (Compact ontology inspect is the concept graph).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from slayer.api.server import create_app
from slayer.client import slayer_client as slayer_client_module
from slayer.client.slayer_client import SlayerClient
from slayer.inspect.service import InspectService
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import FLIGHTS, FLIGHTS_DS, example, flights_store, hide, view
from tests._dev2475_fixtures import FLIGHT_AIRCRAFT, flights_model, inspect_json, squashed

DEP_DELAY = f"{FLIGHTS_DS}.FLIGHT.dep_delay"
VERBALIZATION = "Flight `FLIGHT.id` has departure Delay `FLIGHT.dep_delay`"


async def markdown(storage: StorageBackend, reference: str, entity_type: str, **kwargs: Any) -> str:
    return squashed(await InspectService(storage=storage).inspect(reference=reference, entity_type=entity_type,
                                                                  **kwargs))


def concept_entry(payload: dict[str, Any], concept: str) -> dict[str, Any]:
    [hit] = [c for c in payload["concept_graph"] if c["concept"] == concept]
    return hit


def relationship_entry(entry: dict[str, Any], name: str) -> dict[str, Any]:
    [hit] = [r for r in entry["relationships"] if r["name"] == name]
    return hit


def assert_graph_markdown(text: str) -> None:
    assert all(s in text for s in ("NrMinutes", "departure_delay", "FLIGHT.dep_delay")), text


def assert_field_view(text: str) -> None:
    assert all(name in text for name in ("Delay", "NrMinutes", "Decimal")), text
    assert VERBALIZATION in text, text
    assert "Unit of measure for time in minutes" in text, text


# --------------------------------------------------------------------------- #
# Fields, datasets, relationships
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("compact", [True, False])
async def test_inspect_a_field(tmp_path: Path, compact: bool) -> None:
    """Spec scenario: Inspect a field."""
    storage = await flights_store("yaml", tmp_path)
    assert_field_view(await markdown(storage, DEP_DELAY, "field", compact=compact))
    assert_field_view(json.dumps(await inspect_json(storage, DEP_DELAY, "field", compact=compact)))


async def test_a_field_shows_its_domain(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert "1–10" in await markdown(storage, f"{FLIGHTS_DS}.ROUTE.dist_grp", "field")


@pytest.mark.parametrize("compact", [True, False])
async def test_inspect_a_dataset_shows_its_concepts(tmp_path: Path, compact: bool) -> None:
    storage = await flights_store("yaml", tmp_path)
    text = await markdown(storage, f"{FLIGHTS_DS}.AIRCRAFT", "dataset", compact=compact)
    assert all(name in text for name in ("Aircraft", "Model", "Manufacturer", "Carrier")), text
    payload = await inspect_json(storage, f"{FLIGHTS_DS}.AIRCRAFT", "dataset", compact=compact)
    assert [c["concept"] for c in payload["own_concepts"]] == ["Aircraft"]


async def test_inspect_a_dataset_shows_its_fields_concepts(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert VERBALIZATION in await markdown(storage, f"{FLIGHTS_DS}.FLIGHT", "dataset", compact=False)


async def test_inspect_shows_lined_up_relationships(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, model=flights_model(relationships=[FLIGHT_AIRCRAFT]))
    text = await markdown(storage, f"{FLIGHTS_DS}.FLIGHT", "dataset", compact=False, sections=["relationships"])
    assert "Flight.aircraft" in text
    assert "ManyToOne" in text
    assert "Flight `FLIGHT.id` uses Aircraft `FLIGHT.tail_nr`" in text


# --------------------------------------------------------------------------- #
# The ontology's concept graph
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_inspect_the_ontology(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Inspect the ontology."""
    storage = await flights_store(kind, tmp_path)
    payload = await inspect_json(storage, FLIGHTS, "ontology")
    delay = concept_entry(payload, "Delay")
    assert (delay["type"], delay["extends"]) == ("ValueType", ["NrMinutes"])
    assert {f"{FLIGHTS_DS}.FLIGHT.dep_delay", f"{FLIGHTS_DS}.FLIGHT.arr_delay"} <= set(delay["fields"])
    departure_delay = relationship_entry(concept_entry(payload, "Flight"), "departure_delay")
    assert departure_delay["multiplicity"] == "ManyToOne"
    assert departure_delay["verbalizes"] == ["{Flight} has departure- {Delay}"]
    assert departure_delay["roles"] == ["Delay"]

    text = await markdown(storage, FLIGHTS, "ontology")
    assert all(s in text for s in ("NrMinutes", "departure_delay", "ManyToOne", "{Flight} has departure- {Delay}",
                                   "FLIGHT.dep_delay", "FLIGHT.arr_delay")), text


async def test_the_concept_graph_carries_identifiers_requires_and_descriptions(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    payload = await inspect_json(storage, FLIGHTS, "ontology")
    airport = concept_entry(payload, "Airport")
    assert airport["identify_by"] == ["code"]
    assert airport["requires"] == ["Airport.latitude", "Airport.longitude"]
    assert concept_entry(payload, "NrMinutes")["description"] == "Unit of measure for time in minutes"
    assert concept_entry(payload, "DistanceGroup")["requires"] == ["1 <= DistanceGroup", "DistanceGroup <= 10"]
    route = concept_entry(payload, "Route")
    assert relationship_entry(route, "destination")["requires"] == ["NOT Route.departure(Airport)"]
    assert concept_entry(payload, "Polygon")["fields"] == []


async def test_the_concept_graph_reads_the_callers_mappings(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
    payload = await inspect_json(view(storage), FLIGHTS, "ontology")
    assert "AIRCRAFT" not in json.dumps([c["fields"] for c in payload["concept_graph"]])
    seen = await inspect_json(view(storage, "secret"), FLIGHTS, "ontology")
    assert f"{FLIGHTS_DS}.AIRCRAFT.tail_nr" in concept_entry(seen, "TailNr")["fields"]


async def test_compact_ontology_inspect_is_the_concept_graph(tmp_path: Path) -> None:
    """Spec scenario: Compact ontology inspect is the concept graph."""
    storage = await flights_store("yaml", tmp_path)
    compact = await inspect_json(storage, FLIGHTS, "ontology")
    assert concept_entry(compact, "Delay")["extends"] == ["NrMinutes"]
    assert "text" not in compact
    full = await inspect_json(storage, FLIGHTS, "ontology", compact=False)
    assert full["text"] == example("flights.ontology.yaml")
    assert full["concept_graph"] == compact["concept_graph"]
    assert example("flights.ontology.yaml") not in await markdown(storage, FLIGHTS, "ontology")


# --------------------------------------------------------------------------- #
# Surfaces
# --------------------------------------------------------------------------- #
async def test_mcp_inspect_shows_annotations_and_the_graph(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert_field_view(squashed(await mcp_text(storage, "inspect", entity_type="field", reference=DEP_DELAY)))
    assert_field_view(await mcp_text(storage, "inspect", entity_type="field", reference=DEP_DELAY, format="json"))
    graph = json.loads(await mcp_text(storage, "inspect", entity_type="ontology", reference=FLIGHTS, format="json"))
    assert concept_entry(graph, "Delay")["extends"] == ["NrMinutes"]
    assert_graph_markdown(await mcp_text(storage, "inspect", entity_type="ontology", reference=FLIGHTS))


async def test_rest_inspect_shows_annotations_and_the_graph(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    client = TestClient(create_app(storage=storage))
    field = client.post("/inspect", json={"reference": DEP_DELAY, "entity_type": "field"})
    assert field.status_code == 200, field.text
    assert_field_view(squashed(field.json()["result"]))
    field_json = client.post("/inspect", json={"reference": DEP_DELAY, "entity_type": "field", "format": "json"})
    assert_field_view(field_json.json()["result"])
    graph = client.post("/inspect", json={"reference": FLIGHTS, "entity_type": "ontology", "format": "json"})
    assert concept_entry(json.loads(graph.json()["result"]), "Delay")["extends"] == ["NrMinutes"]
    graph_markdown = client.post("/inspect", json={"reference": FLIGHTS, "entity_type": "ontology"})
    assert_graph_markdown(graph_markdown.json()["result"])


async def test_cli_inspect_shows_annotations_and_the_graph(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path)
    store = str(tmp_path / "store")
    field = run_cli_in_process(["inspect", DEP_DELAY, "--type", "field", "--storage", store])
    assert field.returncode == 0, field.stderr
    assert_field_view(squashed(field.stdout))
    field_json = run_cli_in_process(["inspect", DEP_DELAY, "--type", "field", "--format", "json", "--storage", store])
    assert_field_view(field_json.stdout)
    graph = run_cli_in_process(["inspect", FLIGHTS, "--type", "ontology", "--format", "json", "--storage", store])
    assert graph.returncode == 0, graph.stderr
    assert concept_entry(json.loads(graph.stdout), "Delay")["extends"] == ["NrMinutes"]
    assert_graph_markdown(run_cli_in_process(["inspect", FLIGHTS, "--type", "ontology", "--storage", store]).stdout)


async def test_the_python_client_inspect_shows_annotations(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    client = SlayerClient(storage=storage)
    assert_field_view(squashed(await client.inspect(reference=DEP_DELAY, entity_type="field")))
    assert_field_view(await client.inspect(reference=DEP_DELAY, entity_type="field", format="json"))
    graph = json.loads(await client.inspect(reference=FLIGHTS, entity_type="ontology", format="json"))
    assert concept_entry(graph, "Delay")["extends"] == ["NrMinutes"]
    assert_graph_markdown(await client.inspect(reference=FLIGHTS, entity_type="ontology"))


async def test_the_remote_python_client_inspect_shows_annotations(tmp_path: Path,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await flights_store("yaml", tmp_path)
    app = create_app(storage=storage)
    monkeypatch.setattr(slayer_client_module.httpx, "AsyncClient",
                        lambda **_: AsyncClient(transport=ASGITransport(app=app), base_url="http://test"))
    client = SlayerClient(url="http://test")
    assert_field_view(squashed(await client.inspect(reference=DEP_DELAY, entity_type="field")))
    assert_field_view(await client.inspect(reference=DEP_DELAY, entity_type="field", format="json"))
    assert_graph_markdown(await client.inspect(reference=FLIGHTS, entity_type="ontology"))
    graph = json.loads(await client.inspect(reference=FLIGHTS, entity_type="ontology", format="json"))
    assert concept_entry(graph, "Delay")["extends"] == ["NrMinutes"]
