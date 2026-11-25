"""Mapping entries that read a dataset hidden by access tags are redacted; ontologies are visible to every caller.

Spec: ontologies/ontology-documents › Mapping entries hidden by access tags are redacted; Explicit changes never break
a stored mapping (hidden-content conflict).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, component, concept_mapping, dump, edited, example, flights_store, hide,
    load,
    raises_refusal, refusal, view,
)

HIDDEN = ["Manufacturer", "Model", "Aircraft"]
ALL = [m["concept"] for m in load(example("flights.mapping.yaml"))["concept_mappings"]]
CONFLICT = "HiddenContentConflictError"


async def hidden_aircraft(kind: str, base: Path) -> Any:
    storage = await flights_store(kind, base)
    await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
    return storage


def concepts(text: str | None) -> list[str]:
    assert text is not None
    return [m["concept"] for m in load(text)["concept_mappings"]]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_hidden_datasets_entry_is_redacted(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A hidden dataset's entry is redacted."""
    storage = await hidden_aircraft(kind, tmp_path)
    seen = concepts(await view(storage).get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS))
    assert seen == [c for c in ALL if c not in HIDDEN]
    assert concepts(await view(storage, "secret").get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS)) == ALL


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_redacted_save_keeps_the_hidden_entries(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A redacted save keeps the hidden entries."""
    storage = await hidden_aircraft(kind, tmp_path)
    caller = view(storage)
    redacted = await caller.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert redacted is not None
    await caller.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                            text=edited(redacted, lambda d: d.update(description="Rewritten.")))
    stored = await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert concepts(stored) == ALL
    assert load(stored or "")["description"] == "Rewritten."
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


@pytest.mark.parametrize("entry", [
    pytest.param({"concept": "Aircraft", "object_mappings": [
        {"referent_mappings": [{"relationship": "tailnum", "expression": "AIRCRAFT.serial_nr"}]}]},
        id="an-entry-reading-a-hidden-dataset"),
    pytest.param({"concept": "Market", "object_mappings": [
        {"referent_mappings": [{"relationship": "name", "expression": "AIRCRAFT.name"}]}]},
        id="a-visible-concept-reading-a-hidden-dataset"),
])
async def test_an_edit_touching_hidden_content_is_refused(tmp_path: Path, entry: dict[str, Any]) -> None:
    storage = await hidden_aircraft("yaml", tmp_path)
    caller = view(storage)
    redacted = await caller.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert redacted is not None
    with raises_refusal(CONFLICT) as info:
        await caller.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                text=edited(redacted, lambda d: d["concept_mappings"].append(entry)))
    assert "AIRCRAFT" not in refusal(info.value)
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")


async def test_a_change_breaking_a_hidden_entry_is_a_hidden_content_conflict(tmp_path: Path) -> None:
    storage = await hidden_aircraft("yaml", tmp_path)
    def without_aircraft_carrier(document: dict[str, Any]) -> None:
        aircraft = component(document, "Aircraft")
        aircraft["relationships"] = [r for r in aircraft["relationships"] if r["name"] != "carrier"]

    with raises_refusal(CONFLICT) as info:
        await view(storage).save_ontology_text(name=FLIGHTS, text=edited(example("flights.ontology.yaml"),
                                                                           without_aircraft_carrier))
    assert "AIRCRAFT" not in refusal(info.value)
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")


def _predicate_only(document: dict[str, Any]) -> None:
    referent = concept_mapping(document, "Market")["object_mappings"][0]["referent_mappings"][0]
    referent["expression"] = "AIRPORT.market_nm WHERE ( AIRCRAFT.name IS NOT NULL )"


def _deep_only(document: dict[str, Any]) -> None:
    city = concept_mapping(document, "Airport")["link_mappings"][0]["children"][0]["object_mapping"]
    city["referent_mappings"][0]["expression"] = "AIRCRAFT.name"


@pytest.mark.parametrize(("concept", "change"), [
    pytest.param("Market", _predicate_only, id="read-only-in-a-predicate"),
    pytest.param("Airport", _deep_only, id="read-only-deep-in-a-link-child"),
])
async def test_every_read_of_an_entry_counts(tmp_path: Path, concept: str, change: Any) -> None:
    storage = await flights_store("yaml", tmp_path)
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                             text=edited(example("flights.mapping.yaml"), change))
    await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
    caller = view(storage)
    redacted = await caller.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert concepts(redacted) == [c for c in ALL if c not in [*HIDDEN, concept]]

    replacement = {"concept": concept, "object_mappings": [
        {"referent_mappings": [{"relationship": "code" if concept == "Airport" else "name",
                                "expression": "AIRPORT.code" if concept == "Airport" else "AIRPORT.market_nm"}]}]}
    with raises_refusal(CONFLICT) as info:
        await caller.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=edited(
            redacted or "", lambda d: d["concept_mappings"].append(replacement)))
    assert "AIRCRAFT" not in refusal(info.value)


@pytest.mark.parametrize("kind", STORAGES)
async def test_ontologies_are_visible_to_every_caller(tmp_path: Path, kind: str) -> None:
    storage = await hidden_aircraft(kind, tmp_path)
    caller = view(storage)
    assert await caller.list_ontologies() == [FLIGHTS]
    assert await caller.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")
    assert await caller.list_ontology_mappings(FLIGHTS_DS) == [(FLIGHTS_DS, FLIGHTS_MAPPING)]


async def test_a_caller_seeing_everything_saves_as_written(tmp_path: Path) -> None:
    storage = await hidden_aircraft("yaml", tmp_path)
    text = dump(load(example("flights.mapping.yaml")))
    await view(storage, "secret").save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=text)
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == text
