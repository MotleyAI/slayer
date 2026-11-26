"""Saves and edits of mappings, ontologies, semantic models and datasets return the ontology mismatches they touch.

Spec: ontologies/ontology-use › Model checks warn on save and never refuse (A dataset save warns); ontologies/
ontology-documents › Ontologies and mappings are reachable from every surface (every surface returns the save's
warnings).
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
from slayer.core.enums import RelationshipCardinality
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, example, flights_store, hide, view, write,
)
from tests._dev2475_fixtures import aircraft_relationship, flights_engine, flights_model, with_relationship

MAPPING = example("flights.mapping.yaml")
ONTOLOGY = example("flights.ontology.yaml")
KEYED = flights_model(AIRCRAFT=["tail_nr"], AIRPORT=["code"], CARRIER=["code"], FLIGHT=["id"], ROUTE=["id"],
                      RUNWAY=["designator", "airport_code"])


def said(warning: Any) -> str:
    """Everything a save warning says."""
    if hasattr(warning, "human_message"):
        return f"{warning.human_message()} {warning.model_dump_json()}"
    return json.dumps(warning) if isinstance(warning, dict) else str(warning)


def aircraft_key(notes: list[Any]) -> list[Any]:
    """The warnings reporting ``Aircraft``'s identifier against ``AIRCRAFT``'s keys."""
    return [w for w in notes if all(s in said(w) for s in (FLIGHTS_MAPPING, "Aircraft", "AIRCRAFT", "tail_nr"))]


# --------------------------------------------------------------------------- #
# Storage and its access-filtered view
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_mapping_save_warns(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path, mapping=False)
    notes: list[Any] = []
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=MAPPING,
                                             warnings_out=notes)
    assert aircraft_key(notes)
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == MAPPING


async def test_a_mapping_edit_warns(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    notes: list[Any] = []
    await storage.edit_ontology_mapping(FLIGHTS_MAPPING, data_source=FLIGHTS_DS, description="Edited.",
                                        warnings_out=notes)
    assert aircraft_key(notes)


async def test_an_ontology_save_and_edit_warn(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    saved: list[Any] = []
    await storage.save_ontology_text(name=FLIGHTS, text=ONTOLOGY, warnings_out=saved)
    assert aircraft_key(saved)
    edited_notes: list[Any] = []
    await storage.edit_ontology(FLIGHTS, description="Edited.", warnings_out=edited_notes)
    assert aircraft_key(edited_notes)


async def test_a_semantic_model_save_and_edit_warn(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    saved: list[Any] = []
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL, text=flights_model(),
                                           warnings_out=saved)
    assert aircraft_key(saved)
    edited_notes: list[Any] = []
    await storage.edit_semantic_model(FLIGHTS_MODEL, data_source=FLIGHTS_DS, description="Edited.",
                                      warnings_out=edited_notes)
    assert aircraft_key(edited_notes)


async def test_a_save_with_nothing_to_report_returns_no_ontology_warning(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False, model=KEYED)
    notes: list[Any] = []
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=MAPPING,
                                             warnings_out=notes)
    assert not [w for w in notes if FLIGHTS_MAPPING in said(w)]


async def test_without_warnings_out_the_mismatch_is_a_python_warning(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    with pytest.warns(UserWarning) as caught:
        await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=MAPPING)
    assert [w for w in caught if "Aircraft" in str(w.message) and "AIRCRAFT" in str(w.message)]


async def test_the_access_filtered_view_returns_only_visible_mismatches(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
    everything: list[Any] = []
    await view(storage, "secret").save_ontology_text(name=FLIGHTS, text=ONTOLOGY, warnings_out=everything)
    assert aircraft_key(everything)
    visible: list[Any] = []
    await view(storage).save_ontology_text(name=FLIGHTS, text=ONTOLOGY, warnings_out=visible)
    assert [w for w in visible if "Airport" in said(w)]
    assert not [w for w in visible if "AIRCRAFT" in said(w)]


# --------------------------------------------------------------------------- #
# Datasets
# --------------------------------------------------------------------------- #
async def test_a_dataset_save_warns(tmp_path: Path) -> None:
    """Spec scenario: A dataset save warns."""
    async with flights_engine("yaml", tmp_path) as engine:
        notes: list[Any] = []
        await with_relationship(engine, aircraft_relationship(RelationshipCardinality.ONE_TO_MANY, name="uses"),
                                warnings_out=notes)
        stored = await engine.storage.get_dataset("FLIGHT", data_source=FLIGHTS_DS)
    assert stored is not None
    assert [(r.name, r.cardinality, r.trusted) for r in stored.relationships] == [
        ("uses", RelationshipCardinality.ONE_TO_MANY, False)]
    assert [w for w in notes if all(s in said(w) for s in ("Flight.aircraft", "ManyToOne", "uses"))]


# --------------------------------------------------------------------------- #
# Surfaces
# --------------------------------------------------------------------------- #
def mentions_aircraft_key(text: str) -> bool:
    return all(s in text for s in ("Aircraft", "AIRCRAFT", "tail_nr"))


async def test_mcp_saves_return_their_warnings(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    assert mentions_aircraft_key(await mcp_text(storage, "create_ontology_mapping", data_source=FLIGHTS_DS,
                                                content=MAPPING))
    assert mentions_aircraft_key(await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING,
                                                data_source=FLIGHTS_DS, description="Edited."))
    assert mentions_aircraft_key(await mcp_text(storage, "edit_ontology", name=FLIGHTS, description="Edited."))
    assert mentions_aircraft_key(await mcp_text(storage, "create_ontology", content=ONTOLOGY))
    assert mentions_aircraft_key(await mcp_text(storage, "edit_semantic_model", name=FLIGHTS_MODEL,
                                                data_source=FLIGHTS_DS, description="Edited."))


def _rest_warnings(response: Any) -> str:
    assert response.status_code == 200, response.text
    return json.dumps(response.json()["warnings"])


async def test_rest_saves_return_their_warnings(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    client = TestClient(create_app(storage=storage))
    assert mentions_aircraft_key(_rest_warnings(client.post("/ontology-mappings", json={
        "data_source": FLIGHTS_DS, "content": MAPPING})))
    assert mentions_aircraft_key(_rest_warnings(client.put(
        f"/ontology-mappings/{FLIGHTS_DS}/{FLIGHTS_MAPPING}", json={"content": MAPPING})))
    assert mentions_aircraft_key(_rest_warnings(client.put(f"/ontologies/{FLIGHTS}", json={"content": ONTOLOGY})))
    assert mentions_aircraft_key(_rest_warnings(client.put(f"/semantic-models/{FLIGHTS_DS}/{FLIGHTS_MODEL}",
                                                           json={"content": flights_model()})))


async def test_cli_saves_print_their_warnings(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path, mapping=False)
    store = str(tmp_path / "store")
    created = run_cli_in_process(["ontology-mappings", "create", str(write(tmp_path / "m.yaml", MAPPING)),
                                  "--datasource", FLIGHTS_DS, "--storage", store])
    assert created.returncode == 0, created.stderr
    assert mentions_aircraft_key(created.stdout + created.stderr)
    for args in (["ontology-mappings", "replace", FLIGHTS_MAPPING, str(write(tmp_path / "m2.yaml", MAPPING)),
                  "--datasource", FLIGHTS_DS],
                 ["ontologies", "replace", FLIGHTS, str(write(tmp_path / "o.yaml", ONTOLOGY))],
                 ["ontologies", "create", str(write(tmp_path / "o2.yaml", ONTOLOGY))]):
        result = run_cli_in_process([*args, "--storage", store])
        assert result.returncode == 0, result.stderr
        assert mentions_aircraft_key(result.stdout + result.stderr), args


async def _client_saves(client: SlayerClient) -> list[Any]:
    notes: list[Any] = []
    await client.create_ontology_mapping(data_source=FLIGHTS_DS, content=MAPPING, warnings_out=notes)
    await client.replace_ontology_mapping(FLIGHTS_MAPPING, data_source=FLIGHTS_DS, content=MAPPING, warnings_out=notes)
    await client.create_ontology(content=ONTOLOGY, warnings_out=notes)
    await client.replace_ontology(FLIGHTS, content=ONTOLOGY, warnings_out=notes)
    await client.replace_semantic_model(FLIGHTS_MODEL, data_source=FLIGHTS_DS, content=flights_model(),
                                        warnings_out=notes)
    return notes


async def test_the_python_client_returns_save_warnings(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    notes = await _client_saves(SlayerClient(storage=storage))
    assert len(aircraft_key(notes)) >= 5


async def test_the_remote_python_client_returns_save_warnings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    app = create_app(storage=storage)
    monkeypatch.setattr(slayer_client_module.httpx, "AsyncClient",
                        lambda **_: AsyncClient(transport=ASGITransport(app=app), base_url="http://test"))
    notes = await _client_saves(SlayerClient(url="http://test"))
    assert len(aircraft_key(notes)) >= 5
