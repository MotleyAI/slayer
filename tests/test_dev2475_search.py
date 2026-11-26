"""``search`` reads concept names, descriptions and verbalizations, and selects by ``concept:`` references.

Spec: ontologies/ontology-use › Search reads concepts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.embeddings import client as embedding_client
from slayer.search.retrievers.tantivy import TantivyRetriever
from slayer.search.service import SearchResponse, SearchService
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, component, concept_mapping, dump, edited, example, flights_store, hide,
    load, view, write,
)

DEP_DELAY = f"{FLIGHTS_DS}.FLIGHT.dep_delay"
ARR_DELAY = f"{FLIGHTS_DS}.FLIGHT.arr_delay"
TRAINS_MODEL = "Trains model"
LATE = f"{FLIGHTS_DS}.TRAINS.late"


async def search(storage: StorageBackend, *, retrievers: list[Any] | None = None, **kwargs: Any) -> SearchResponse:
    return await SearchService(storage=storage, retrievers=retrievers).search(datasource=FLIGHTS_DS, **kwargs)


def ids(response: SearchResponse) -> list[str]:
    return [h.id for h in response.results]


async def with_trains(storage: StorageBackend) -> None:
    """A second ontology ``Trains`` declaring its own ``Delay``, mapped onto ``TRAINS.late``."""
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=TRAINS_MODEL, text=dump({
        "version": "0.3.0.dev0", "name": TRAINS_MODEL, "datasets": [{"name": "TRAINS", "source": "TRAINS", "fields": [
            {"name": "id", "expression": {"sql": "id"}}, {"name": "late", "expression": {"sql": "late"}}]}]}))
    await storage.save_ontology_text(name="Trains", text=dump({"version": "0.3.0.dev0", "name": "Trains", "ontology": [
        {"concept": "TrainId", "type": "ValueType", "extends": ["String"]},
        {"concept": "Delay", "type": "ValueType", "extends": ["Decimal"], "description": "Minutes a train runs late"},
        {"concept": "Train", "type": "EntityType", "identify_by": ["id"], "relationships": [
            {"name": "id", "roles": [{"concept": "TrainId"}], "verbalizes": ["{Train} has {TrainId}"]},
            {"name": "delay", "roles": [{"concept": "Delay"}], "verbalizes": ["{Train} runs {Delay} late"]}]}]}))
    train = {"referent_mappings": [{"relationship": "id", "expression": "TRAINS.id"}]}
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name="trains_mapping", text=dump({
        "version": "0.3.0.dev0", "name": "trains_mapping", "ontology_ref": {"name": "Trains"},
        "semantic_model_ref": {"name": TRAINS_MODEL}, "concept_mappings": [
            {"concept": "Train", "object_mappings": [train], "link_mappings": [{"object_mapping": train, "children": [
                {"relationship": "delay", "object_mapping": {"concept": "Delay", "expression": "TRAINS.late"}}]}]}]}))


# --------------------------------------------------------------------------- #
# Concept references
# --------------------------------------------------------------------------- #
async def test_every_delay_field(tmp_path: Path) -> None:
    """Spec scenario: Every delay field."""
    storage = await flights_store("yaml", tmp_path)
    found = ids(await search(storage, entities=["concept:Delay"]))
    assert {DEP_DELAY, ARR_DELAY} <= set(found)
    assert f"{FLIGHTS_DS}.FLIGHT.distance" not in found


async def test_a_concept_reference_matches_subtypes(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert {DEP_DELAY, ARR_DELAY} <= set(ids(await search(storage, entities=["concept:NrMinutes"])))


async def test_a_concept_reference_selects_datasets(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert f"{FLIGHTS_DS}.AIRCRAFT" in ids(await search(storage, entities=["concept:Aircraft"]))


async def test_a_field_reached_through_two_mappings_is_listed_once(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    second = edited(example("flights.mapping.yaml"), lambda d: d.update(name="flights_mapping_2"))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name="flights_mapping_2", text=second)
    assert ids(await search(storage, entities=["concept:Delay"])).count(DEP_DELAY) == 1


async def test_unqualified_and_qualified_concept_references(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await with_trains(storage)
    both = await search(storage, entities=["concept:Delay"], compact=False)
    assert {DEP_DELAY, ARR_DELAY, LATE} <= set(ids(both))
    [late] = [h for h in both.results if h.id == LATE]
    assert "Trains" in late.text
    assert LATE not in ids(await search(storage, entities=[f"concept:{FLIGHTS}.Delay"]))
    trains = ids(await search(storage, entities=["concept:Trains.Delay"]))
    assert LATE in trains
    assert DEP_DELAY not in trains


async def test_no_visible_match(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    response = await search(storage, entities=["concept:Spaceship"])
    assert response.results == []
    assert any("Spaceship" in w for w in response.warnings)


async def test_deleting_a_mapping_removes_its_concepts(tmp_path: Path) -> None:
    """Spec scenario: Deleting a mapping removes its concepts."""
    storage = await flights_store("yaml", tmp_path)
    assert ids(await search(storage, entities=["concept:Delay"]))
    await storage.delete_ontology_mapping(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert ids(await search(storage, entities=["concept:Delay"])) == []


# --------------------------------------------------------------------------- #
# Questions
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("full_text_only", [False, True])
async def test_a_question_finds_a_concept(tmp_path: Path, full_text_only: bool) -> None:
    """Spec scenario: A question finds a concept."""
    storage = await flights_store("yaml", tmp_path)
    found = ids(await search(storage, question="unit of measure for time in minutes",
                             retrievers=[TantivyRetriever()] if full_text_only else None))
    assert {DEP_DELAY, ARR_DELAY} <= set(found)


async def test_a_question_finds_a_verbalization(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert f"{FLIGHTS_DS}.FLIGHT.tail_nr" in ids(await search(storage, question="flight uses aircraft"))


async def test_no_concept_text_from_a_hidden_dataset(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
    caller = view(storage)
    for response in (await search(caller, question="aircraft manufacturer model", compact=False),
                     await search(caller, entities=["concept:TailNr"], compact=False),
                     await search(caller, entities=["concept:Manufacturer"], compact=False)):
        assert not [h.id for h in response.results if "AIRCRAFT" in h.id]
        assert not [h.id for h in response.results if "AIRCRAFT." in h.text or "Manufacturer" in h.text]
    assert f"{FLIGHTS_DS}.FLIGHT.tail_nr" in ids(await search(caller, entities=["concept:Aircraft"]))


# --------------------------------------------------------------------------- #
# Embedding refresh
# --------------------------------------------------------------------------- #
class _Recorder:
    def __init__(self) -> None:
        self.texts: list[str] = []

    async def __call__(self, texts: list[str], *, model: str | None = None) -> list[list[float] | None]:
        self.texts.extend(texts)
        return [[1.0, 0.5, 0.25] for _ in texts]

    def take(self, needle: str) -> list[str]:
        """The recorded texts holding ``needle``, cleared after reading."""
        out = [t for t in self.texts if needle in t]
        self.texts.clear()
        return out


@pytest.fixture
def embedded(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    recorder = _Recorder()
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    monkeypatch.setattr("slayer.search.retrievers.embeddings.embed_batch", recorder)
    return recorder


async def test_embeddings_are_refreshed_on_mapping_save_edit_and_delete(tmp_path: Path, embedded: _Recorder) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    embedded.texts.clear()
    await mcp_text(storage, "create_ontology_mapping", data_source=FLIGHTS_DS, content=example("flights.mapping.yaml"))
    assert [t for t in embedded.take("dep_delay") if "Delay" in t and "NrMinutes" in t]

    await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                   remove_concept_mappings=["Flight"])
    refreshed = embedded.take("dep_delay")
    assert refreshed
    assert not [t for t in refreshed if "NrMinutes" in t]

    await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                   concept_mappings=[concept_mapping(load(example("flights.mapping.yaml")), "Flight")])
    assert [t for t in embedded.take("dep_delay") if "NrMinutes" in t]
    await mcp_text(storage, "delete_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS)
    refreshed = embedded.take("dep_delay")
    assert refreshed
    assert not [t for t in refreshed if "NrMinutes" in t]


async def test_embeddings_are_refreshed_on_ontology_edit(tmp_path: Path, embedded: _Recorder) -> None:
    storage = await flights_store("yaml", tmp_path)
    nr_minutes = component(load(example("flights.ontology.yaml")), "NrMinutes")
    nr_minutes["description"] = "Elapsed minutes on the clock"
    embedded.texts.clear()
    await mcp_text(storage, "edit_ontology", name=FLIGHTS, concepts=[nr_minutes])
    refreshed = embedded.take("dep_delay")
    assert [t for t in refreshed if "Elapsed minutes on the clock" in t]


async def test_embeddings_are_refreshed_on_ontology_save(tmp_path: Path, embedded: _Recorder) -> None:
    storage = await flights_store("yaml", tmp_path)
    text = example("flights.ontology.yaml").replace("Unit of measure for time in minutes", "Elapsed clock minutes")
    embedded.texts.clear()
    await mcp_text(storage, "create_ontology", content=text)
    assert [t for t in embedded.take("dep_delay") if "Elapsed clock minutes" in t]


RENAMED_ONTOLOGY = example("flights.ontology.yaml").replace("Unit of measure for time in minutes",
                                                            "Elapsed clock minutes")


async def _write_through(surface: str, storage: StorageBackend, tmp_path: Path, *, kind: str, content: str) -> None:
    """Save mapping or ontology ``content`` through REST, the local Python client or the CLI."""
    if surface == "rest":
        client = TestClient(create_app(storage=storage))
        response = client.post("/ontology-mappings", json={"data_source": FLIGHTS_DS, "content": content}) \
            if kind == "mapping" else client.put(f"/ontologies/{FLIGHTS}", json={"content": content})
        assert response.status_code == 200, response.text
    elif surface == "client":
        local = SlayerClient(storage=storage)
        if kind == "mapping":
            await local.create_ontology_mapping(data_source=FLIGHTS_DS, content=content, warnings_out=[])
        else:
            await local.replace_ontology(FLIGHTS, content=content, warnings_out=[])
    else:
        path = write(tmp_path / f"{kind}.yaml", content)
        args = ["ontology-mappings", "create", str(path), "--datasource", FLIGHTS_DS] if kind == "mapping" \
            else ["ontologies", "replace", FLIGHTS, str(path)]
        result = run_cli_in_process([*args, "--storage", str(tmp_path / "store")])
        assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("surface", ["rest", "client", "cli"])
async def test_embeddings_are_refreshed_on_every_surface(tmp_path: Path, embedded: _Recorder, surface: str) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    embedded.texts.clear()
    await _write_through(surface, storage, tmp_path, kind="mapping", content=example("flights.mapping.yaml"))
    assert [t for t in embedded.take("dep_delay") if "NrMinutes" in t]
    await _write_through(surface, storage, tmp_path, kind="ontology", content=RENAMED_ONTOLOGY)
    assert [t for t in embedded.take("dep_delay") if "Elapsed clock minutes" in t]


async def test_unchanged_text_is_not_re_embedded(tmp_path: Path, embedded: _Recorder) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    await mcp_text(storage, "create_ontology_mapping", data_source=FLIGHTS_DS, content=example("flights.mapping.yaml"))
    assert [t for t in embedded.take("dep_delay") if "NrMinutes" in t]
    await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                   description="Same concepts, new words.")
    assert embedded.take("dep_delay") == []
