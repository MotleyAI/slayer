"""Ontologies and mappings through the Python client (local and remote, with sync twins), REST and the CLI.

Spec: ontologies/ontology-documents › Ontologies and mappings are reachable from every surface (REST create is
refused with every problem; CLI round trip).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from slayer.api.server import create_app
from slayer.client import slayer_client as slayer_client_module
from slayer.client.slayer_client import SlayerClient
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import run_cli_in_process
from tests._dev2243_fixtures import STORAGES, make_storage
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, concept_mapping, edited, empty_store, example, flights_store,
    write,
)

ONTOLOGY = example("flights.ontology.yaml")
MAPPING = example("flights.mapping.yaml")
REPLACED_ONTOLOGY = ONTOLOGY.replace("Ontology of flights", "Ontology of all flights", 1)
REPLACED_MAPPING = MAPPING.replace("Maps the Flights", "Maps all the Flights", 1)


def two_unknown_fields() -> str:
    def change(document: dict[str, Any]) -> None:
        children = concept_mapping(document, "Flight")["link_mappings"][2]["children"]
        children[0]["object_mapping"]["expression"] = "FLIGHT.arrival_lateness"
        children[1]["object_mapping"]["expression"] = "FLIGHT.departure_lateness"
    return edited(MAPPING, change)


async def model_only(kind: str, base: Path) -> StorageBackend:
    storage = await empty_store(kind, base, FLIGHTS_DS)
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL,
                                           text=example("flights.semantic_model.yaml"))
    return storage


# --------------------------------------------------------------------------- #
# Python client
# --------------------------------------------------------------------------- #
async def _client_round_trip(client: SlayerClient, storage: StorageBackend) -> None:
    await client.create_ontology(content=ONTOLOGY)
    await client.create_ontology_mapping(data_source=FLIGHTS_DS, content=MAPPING)
    assert await client.list_ontologies() == [{"name": FLIGHTS}]
    assert await client.list_ontology_mappings() == [{"data_source": FLIGHTS_DS, "name": FLIGHTS_MAPPING}]
    assert await client.get_ontology(FLIGHTS) == ONTOLOGY
    assert await client.get_ontology_mapping(FLIGHTS_MAPPING, data_source=FLIGHTS_DS) == MAPPING
    await client.replace_ontology(FLIGHTS, content=REPLACED_ONTOLOGY)
    await client.replace_ontology_mapping(FLIGHTS_MAPPING, data_source=FLIGHTS_DS, content=REPLACED_MAPPING)
    assert await storage.get_ontology_text(FLIGHTS) == REPLACED_ONTOLOGY
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == REPLACED_MAPPING
    assert await client.delete_ontology_mapping(FLIGHTS_MAPPING, data_source=FLIGHTS_DS) is True
    assert await client.delete_ontology(FLIGHTS) is True
    assert await client.get_ontology(FLIGHTS) is None
    assert await storage.list_ontologies() == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_python_client_manages_ontologies(tmp_path: Path, kind: str) -> None:
    storage = await model_only(kind, tmp_path)
    await _client_round_trip(SlayerClient(storage=storage), storage)


async def test_the_remote_python_client_manages_ontologies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await model_only("yaml", tmp_path)
    app = create_app(storage=storage)
    monkeypatch.setattr(slayer_client_module.httpx, "AsyncClient",
                        lambda **_: AsyncClient(transport=ASGITransport(app=app), base_url="http://test"))
    await _client_round_trip(SlayerClient(url="http://test"), storage)


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_sync_twins(tmp_path: Path, kind: str) -> None:
    storage = await model_only(kind, tmp_path)
    client = SlayerClient(storage=storage)
    client.create_ontology_sync(content=ONTOLOGY)
    client.create_ontology_mapping_sync(data_source=FLIGHTS_DS, content=MAPPING)
    assert client.list_ontologies_sync() == [{"name": FLIGHTS}]
    assert client.list_ontology_mappings_sync(data_source=FLIGHTS_DS) == [
        {"data_source": FLIGHTS_DS, "name": FLIGHTS_MAPPING}]
    assert client.get_ontology_sync(FLIGHTS) == ONTOLOGY
    assert client.get_ontology_mapping_sync(FLIGHTS_MAPPING, data_source=FLIGHTS_DS) == MAPPING
    client.replace_ontology_sync(FLIGHTS, content=REPLACED_ONTOLOGY)
    client.replace_ontology_mapping_sync(FLIGHTS_MAPPING, data_source=FLIGHTS_DS, content=REPLACED_MAPPING)
    assert client.delete_ontology_mapping_sync(FLIGHTS_MAPPING, data_source=FLIGHTS_DS) is True
    assert client.delete_ontology_sync(FLIGHTS) is True


async def test_a_client_refusal_lists_every_problem(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    client, content = SlayerClient(storage=storage), two_unknown_fields()
    with pytest.raises(ValueError) as info:
        await client.create_ontology_mapping(data_source=FLIGHTS_DS, content=content)
    assert "arrival_lateness" in str(info.value)
    assert "departure_lateness" in str(info.value)


# --------------------------------------------------------------------------- #
# REST
# --------------------------------------------------------------------------- #
async def test_rest_manages_ontologies_and_mappings(tmp_path: Path) -> None:
    storage = await model_only("yaml", tmp_path)
    client = TestClient(create_app(storage=storage))
    assert client.post("/ontologies", json={"content": ONTOLOGY}).status_code == 200
    assert client.post("/ontology-mappings", json={"data_source": FLIGHTS_DS, "content": MAPPING}).status_code == 200
    assert client.get("/ontologies").json() == [{"name": FLIGHTS}]
    assert client.get("/ontology-mappings").json() == [{"data_source": FLIGHTS_DS, "name": FLIGHTS_MAPPING}]
    assert client.get("/ontology-mappings", params={"data_source": "other"}).json() == []
    assert client.get(f"/ontologies/{FLIGHTS}").json()["text"] == ONTOLOGY
    assert client.get(f"/ontology-mappings/{FLIGHTS_DS}/{FLIGHTS_MAPPING}").json()["text"] == MAPPING
    assert client.get("/ontologies/Nope").status_code == 404
    assert client.get(f"/ontology-mappings/{FLIGHTS_DS}/nope").status_code == 404

    assert client.put(f"/ontologies/{FLIGHTS}", json={"content": REPLACED_ONTOLOGY}).status_code == 200
    assert client.put(f"/ontology-mappings/{FLIGHTS_DS}/{FLIGHTS_MAPPING}",
                      json={"content": REPLACED_MAPPING}).status_code == 200
    assert await storage.get_ontology_text(FLIGHTS) == REPLACED_ONTOLOGY

    refused = client.delete(f"/ontologies/{FLIGHTS}")
    assert refused.status_code == 400
    assert FLIGHTS_MAPPING in refused.text
    assert client.delete(f"/ontology-mappings/{FLIGHTS_DS}/{FLIGHTS_MAPPING}").status_code == 200
    assert client.delete(f"/ontologies/{FLIGHTS}").status_code == 200
    assert client.delete(f"/ontologies/{FLIGHTS}").status_code == 404


async def test_rest_create_is_refused_with_every_problem(tmp_path: Path) -> None:
    """Spec scenario: REST create is refused with every problem."""
    storage = await flights_store("yaml", tmp_path, mapping=False)
    response = TestClient(create_app(storage=storage)).post(
        "/ontology-mappings", json={"data_source": FLIGHTS_DS, "content": two_unknown_fields()})
    assert response.status_code == 400
    assert "arrival_lateness" in response.text
    assert "departure_lateness" in response.text
    assert await storage.list_ontology_mappings() == []


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def cli(*args: str, store: Path) -> Any:
    return run_cli_in_process([*args, "--storage", str(store)])


def error_lines(result: Any) -> list[str]:
    return [line for line in result.stderr.splitlines() if line.startswith("Error:")]


@pytest.mark.parametrize("kind", STORAGES)
async def test_cli_round_trip(tmp_path: Path, kind: str) -> None:
    """Spec scenario: CLI round trip."""
    await empty_store(kind, tmp_path)
    store = tmp_path / ("store" if kind == "yaml" else "store.db")
    source = write(tmp_path / "flights.ontology.yaml", ONTOLOGY)
    created = cli("ontologies", "create", str(source), store=store)
    assert created.returncode == 0, created.stderr
    shown = cli("ontologies", "show", FLIGHTS, store=store)
    assert shown.returncode == 0, shown.stderr
    assert shown.stdout == ONTOLOGY


async def test_the_cli_manages_ontologies_and_mappings(tmp_path: Path) -> None:
    await model_only("yaml", tmp_path)
    store = tmp_path / "store"
    assert cli("ontologies", "create", str(write(tmp_path / "o.yaml", ONTOLOGY)), store=store).returncode == 0
    created = cli("ontology-mappings", "create", str(write(tmp_path / "m.yaml", MAPPING)), "--datasource", FLIGHTS_DS,
                  store=store)
    assert created.returncode == 0, created.stderr
    assert FLIGHTS in cli("ontologies", "list", store=store).stdout
    assert FLIGHTS_MAPPING in cli("ontology-mappings", "list", "--datasource", FLIGHTS_DS, store=store).stdout
    shown = cli("ontology-mappings", "show", FLIGHTS_MAPPING, "--datasource", FLIGHTS_DS, store=store)
    assert shown.stdout == MAPPING

    replaced = cli("ontologies", "replace", FLIGHTS, str(write(tmp_path / "o2.yaml", REPLACED_ONTOLOGY)), store=store)
    assert replaced.returncode == 0, replaced.stderr
    replaced = cli("ontology-mappings", "replace", FLIGHTS_MAPPING, str(write(tmp_path / "m2.yaml", REPLACED_MAPPING)),
                   "--datasource", FLIGHTS_DS, store=store)
    assert replaced.returncode == 0, replaced.stderr
    storage = make_storage("yaml", tmp_path)
    assert await storage.get_ontology_text(FLIGHTS) == REPLACED_ONTOLOGY
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == REPLACED_MAPPING

    refused = cli("ontologies", "delete", FLIGHTS, "--yes", store=store)
    assert refused.returncode != 0
    assert any(FLIGHTS_MAPPING in line for line in error_lines(refused)), refused.stderr
    assert cli("ontology-mappings", "delete", FLIGHTS_MAPPING, "--datasource", FLIGHTS_DS, "--yes",
               store=store).returncode == 0
    assert cli("ontologies", "delete", FLIGHTS, "--yes", store=store).returncode == 0
    assert await make_storage("yaml", tmp_path).list_ontologies() == []


async def test_a_cli_refusal_reports_every_problem(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path, mapping=False)
    result = cli("ontology-mappings", "create", str(write(tmp_path / "m.yaml", two_unknown_fields())),
                 "--datasource", FLIGHTS_DS, store=tmp_path / "store")
    assert result.returncode != 0
    assert "arrival_lateness" in result.stderr
    assert "departure_lateness" in result.stderr


async def test_the_cli_lists_an_unresolved_mapping_with_its_problems(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path)
    model_file = tmp_path / "store" / "semantic_models" / FLIGHTS_DS / f"{FLIGHTS_MODEL}.yaml"
    model_file.write_text(example("flights.semantic_model.yaml").replace(
        "  - name: dep_delay\n    expression:\n      sql: dep_delay\n", ""), encoding="utf-8")
    listed = cli("ontology-mappings", "list", "--datasource", FLIGHTS_DS, store=tmp_path / "store")
    assert listed.returncode == 0, listed.stderr
    assert FLIGHTS_MAPPING in listed.stdout
    assert "dep_delay" in listed.stdout


def test_starting_the_cli_does_not_import_jsonschema() -> None:
    """``jsonschema`` can cost ~1s to import; it loads on the first schema check, never on CLI/MCP start."""
    code = "import sys, slayer.cli, slayer.mcp.server; print(sorted(m for m in sys.modules if m.startswith('jsonschema')))"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "[]"
