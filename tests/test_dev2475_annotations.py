"""Concept annotations of mapped fields, datasets and relationships, as inspect json shows them.

Spec: ontologies/ontology-use › Mapped elements are annotated with their concepts; Verbalizations are filled with mapped
names; Value-type constraints are shown as domains.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, component, concept_mapping, dump, edited, example,
    flights_store, hide, load, view,
)
from tests._dev2475_fixtures import (
    FLIGHT_AIRCRAFT, SEATING, SEATING_DS, SEATING_MAPPING, annotation, concepts_of, dataset_payload, field_annotations,
    flights_model, seat_relationship, seating_engine, seating_model,
)

MAPPING = example("flights.mapping.yaml")
ONTOLOGY = example("flights.ontology.yaml")


def lined_up(payload: dict[str, Any], to: str) -> list[dict[str, Any]]:
    """The ontology relationships the dataset relationship to ``to`` lines up with."""
    [hit] = [r for r in payload["relationships"] if r["to"] == to]
    return list(hit.get("concepts") or [])


# --------------------------------------------------------------------------- #
# Mapped elements are annotated with their concepts
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_value_field(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A value field."""
    storage = await flights_store(kind, tmp_path)
    found = annotation(await field_annotations(storage, "FLIGHT.dep_delay"), relationship="Flight.departure_delay")
    assert found["concept"] == "Delay"
    assert found["chain"] == ["Delay", "NrMinutes", "Decimal"]
    assert found["role"] == "value"
    assert (found["ontology"], found["mapping"]) == (FLIGHTS, FLIGHTS_MAPPING)
    assert found.get("expression") is None


async def test_identifying_and_referencing_fields(tmp_path: Path) -> None:
    """Spec scenario: Identifying and referencing fields."""
    storage = await flights_store("yaml", tmp_path)
    identifying = annotation(await field_annotations(storage, "AIRCRAFT.tail_nr"), role="identifies")
    assert (identifying["entity"], identifying["relationship"], identifying["concept"]) == (
        "Aircraft", "Aircraft.tailnum", "TailNr")
    referencing = annotation(await field_annotations(storage, "FLIGHT.tail_nr"), role="references")
    assert (referencing["entity"], referencing["relationship"]) == ("Aircraft", "Flight.aircraft")
    assert not [a for a in await field_annotations(storage, "FLIGHT.tail_nr") if a["role"] == "identifies"]


async def test_a_datasets_own_concept(tmp_path: Path) -> None:
    """Spec scenario: A dataset's own concept."""
    storage = await flights_store("yaml", tmp_path)
    aircraft = await dataset_payload(storage, "AIRCRAFT")
    assert concepts_of(aircraft["own_concepts"]) == ["Aircraft"]
    assert concepts_of(aircraft["also_populated"]) == ["Carrier", "Manufacturer", "Model"]
    airport = await dataset_payload(storage, "AIRPORT")
    assert concepts_of(airport["own_concepts"]) == ["Airport"]
    assert concepts_of(airport["also_populated"]) == ["City", "Market", "State"]
    route = await dataset_payload(storage, "ROUTE")
    assert concepts_of(route["own_concepts"]) == ["Route"]
    assert concepts_of(route["also_populated"]) == ["Airport"]


async def test_a_relationship_lines_up(tmp_path: Path) -> None:
    """Spec scenario: A relationship lines up."""
    storage = await flights_store("yaml", tmp_path, model=flights_model(relationships=[FLIGHT_AIRCRAFT]))
    [found] = lined_up(await dataset_payload(storage, "FLIGHT"), "AIRCRAFT")
    assert (found["relationship"], found["multiplicity"], found["mapping"]) == (
        "Flight.aircraft", "ManyToOne", FLIGHTS_MAPPING)


async def test_a_relationship_lines_up_in_either_orientation(tmp_path: Path) -> None:
    reverse = {**FLIGHT_AIRCRAFT, "from": "AIRCRAFT", "to": "FLIGHT"}
    storage = await flights_store("yaml", tmp_path, model=flights_model(relationships=[reverse]))
    assert [c["relationship"] for c in lined_up(await dataset_payload(storage, "AIRCRAFT"), "FLIGHT")] == [
        "Flight.aircraft"]


async def test_a_relationship_to_a_subtypes_dataset_lines_up(tmp_path: Path) -> None:
    """Spec scenario: A relationship to a subtype's dataset lines up."""
    storage = await flights_store("yaml", tmp_path, mapping=False,
                                  model=flights_model(relationships=[FLIGHT_AIRCRAFT]))
    await storage.save_ontology_text(name=FLIGHTS, text=edited(ONTOLOGY, lambda d: d["ontology"].append(
        {"concept": "JetAircraft", "type": "EntityType", "extends": ["Aircraft"]})))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, warnings_out=[],
                                             text=edited(MAPPING, lambda d: concept_mapping(d, "Aircraft").update(
                                                 concept="JetAircraft")))
    assert concepts_of((await dataset_payload(storage, "AIRCRAFT"))["own_concepts"]) == ["JetAircraft"]
    [found] = lined_up(await dataset_payload(storage, "FLIGHT"), "AIRCRAFT")
    assert (found["relationship"], found["multiplicity"]) == ("Flight.aircraft", "ManyToOne")


async def test_hidden_datasets_annotate_nothing(tmp_path: Path) -> None:
    """Spec scenario: Hidden datasets annotate nothing."""
    storage = await flights_store("yaml", tmp_path, model=flights_model(relationships=[FLIGHT_AIRCRAFT]))
    await hide(storage, "AIRCRAFT", data_source=FLIGHTS_DS)
    caller = view(storage)
    payload = await dataset_payload(caller, "FLIGHT")
    annotations = [a for f in payload["fields"] for a in f.get("concepts") or []]
    assert annotations, "FLIGHT's own entries stay visible"
    shown = json.dumps([annotations, payload.get("own_concepts"), payload.get("also_populated")])
    assert "AIRCRAFT" not in shown
    assert not {"Manufacturer", "Model"} & {a["concept"] for a in annotations}
    assert "Manufacturer" not in shown
    assert '"Model"' not in shown
    assert annotation(await field_annotations(view(storage, "secret"), "AIRCRAFT.tail_nr"), role="identifies")


# --------------------------------------------------------------------------- #
# Verbalizations and domains
# --------------------------------------------------------------------------- #
async def test_departure_delay_verbalization(tmp_path: Path) -> None:
    """Spec scenario: Departure delay."""
    storage = await flights_store("yaml", tmp_path)
    found = annotation(await field_annotations(storage, "FLIGHT.dep_delay"), relationship="Flight.departure_delay")
    assert found["verbalization"] == "Flight `FLIGHT.id` has departure Delay `FLIGHT.dep_delay`"


async def test_a_repeated_placeholder_is_filled_each_time(tmp_path: Path) -> None:
    def repeat(document: dict[str, Any]) -> None:
        [delay] = [r for r in component(document, "Flight")["relationships"] if r["name"] == "departure_delay"]
        delay["verbalizes"] = ["{Flight} has departure- {Delay}; {Flight} left late"]

    storage = await flights_store("yaml", tmp_path)
    await storage.save_ontology_text(name=FLIGHTS, text=edited(ONTOLOGY, repeat), warnings_out=[])
    found = annotation(await field_annotations(storage, "FLIGHT.dep_delay"), relationship="Flight.departure_delay")
    assert found["verbalization"] == ("Flight `FLIGHT.id` has departure Delay `FLIGHT.dep_delay`; "
                                      "Flight `FLIGHT.id` left late")


async def test_a_range_and_an_enumeration(tmp_path: Path) -> None:
    """Spec scenario: A range and an enumeration."""
    storage = await flights_store("yaml", tmp_path)
    assert "1–10" in annotation(await field_annotations(storage, "ROUTE.dist_grp"), concept="DistanceGroup")["domains"]
    code = annotation(await field_annotations(storage, "FLIGHT.cancel_code"), concept="CancelationCode")
    assert "one of A, B, C, D" in code["domains"]
    assert code["role"] == "references"
    assert code["entity"] == "CancelationReason"


async def test_a_two_sided_range_from_two_entries(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    assert "-90–90" in annotation(await field_annotations(storage, "AIRPORT.latitude"),
                                  concept="DegreesLatitude")["domains"]


async def test_constraints_are_inherited(tmp_path: Path) -> None:
    def precise(document: dict[str, Any]) -> None:
        document["ontology"].append({"concept": "PreciseLatitude", "type": "ValueType", "extends": ["DegreesLatitude"],
                                     "requires": ["PreciseLatitude != 0"]})

    def mapped_precise(document: dict[str, Any]) -> None:
        children = concept_mapping(document, "Airport")["link_mappings"][0]["children"]
        [latitude] = [c for c in children if c["relationship"] == "latitude"]
        latitude["object_mapping"]["concept"] = "PreciseLatitude"

    storage = await flights_store("yaml", tmp_path, mapping=False)
    await storage.save_ontology_text(name=FLIGHTS, text=edited(ONTOLOGY, precise))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                             text=edited(MAPPING, mapped_precise))
    found = annotation(await field_annotations(storage, "AIRPORT.latitude"), concept="PreciseLatitude")
    assert found["chain"] == ["PreciseLatitude", "DegreesLatitude", "Decimal"]
    assert "-90–90" in found["domains"]
    assert any("!= 0" in d for d in found["domains"])


async def test_a_constraint_outside_the_plain_subset_is_shown_as_written(tmp_path: Path) -> None:
    def not_plain(document: dict[str, Any]) -> None:
        component(document, "DistanceGroup")["requires"].append("DistanceGroup <= COUNT[Route]")

    storage = await flights_store("yaml", tmp_path, mapping=False)
    await storage.save_ontology_text(name=FLIGHTS, text=edited(ONTOLOGY, not_plain))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=MAPPING)
    found = annotation(await field_annotations(storage, "ROUTE.dist_grp"), concept="DistanceGroup")
    assert "1–10" in found["domains"]
    assert found["not_checked"] == ["DistanceGroup <= COUNT[Route]"]


async def test_a_qualified_relationship_through_extends(tmp_path: Path) -> None:
    def hub(document: dict[str, Any]) -> None:
        document["ontology"].append({"concept": "Hub", "type": "EntityType", "extends": ["Airport"]})

    def mapped_hub(document: dict[str, Any]) -> None:
        document["concept_mappings"].append({"concept": "Hub", "link_mappings": [{
            "object_mapping": {"referent_mappings": [{"relationship": "code", "expression": "AIRPORT.code"}]},
            "children": [{"relationship": "Airport.name",
                          "object_mapping": {"concept": "AirportName", "expression": "AIRPORT.city_nm"}}]}]})

    storage = await flights_store("yaml", tmp_path, mapping=False)
    await storage.save_ontology_text(name=FLIGHTS, text=edited(ONTOLOGY, hub))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                             text=edited(MAPPING, mapped_hub))
    found = annotation(await field_annotations(storage, "AIRPORT.city_nm"), concept="AirportName")
    assert (found["relationship"], found["role"]) == ("Airport.name", "value")


# --------------------------------------------------------------------------- #
# Expressions
# --------------------------------------------------------------------------- #
async def test_a_filtered_position_names_its_expression(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    found = annotation(await field_annotations(storage, "FLIGHT.id"), relationship="Flight.diverted")
    assert found["expression"] == "FLIGHT.id WHERE ( FLIGHT.diverted == TRUE )"
    assert found["role"] == "identifies"


def _route_child(document: dict[str, Any], relationship: str, expression: str) -> None:
    children = concept_mapping(document, "Route")["link_mappings"][0]["children"]
    [child] = [c for c in children if c["relationship"] == relationship]
    child["object_mapping"]["expression"] = expression


async def test_a_computed_single_dataset_expression(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=edited(
        MAPPING, lambda d: _route_child(d, "distance", "ROUTE.distance * 1.0")))
    found = annotation(await field_annotations(storage, "ROUTE.distance"), relationship="Route.distance")
    assert (found["concept"], found["expression"]) == ("Distance", "ROUTE.distance * 1.0")


async def test_a_multi_dataset_expression_annotates_each_field_read(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    expression = "concat(ROUTE.name, AIRPORT.name)"
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=edited(
        MAPPING, lambda d: _route_child(d, "route_name", expression)))
    for field in ("ROUTE.name", "AIRPORT.name"):
        assert annotation(await field_annotations(storage, field), relationship="Route.route_name")[
            "expression"] == expression


# --------------------------------------------------------------------------- #
# Which mappings annotate
# --------------------------------------------------------------------------- #
DELAYS_MAPPING = "delays_mapping"


def delays_mapping(*, semantic_model: str = FLIGHTS_MODEL) -> str:
    flight = concept_mapping(load(MAPPING), "Flight")
    link = flight["link_mappings"][2]
    link["children"] = [c for c in link["children"] if c["relationship"] == "departure_delay"]
    return dump({"version": "0.3.0.dev0", "name": DELAYS_MAPPING, "ontology_ref": {"name": FLIGHTS},
                 "semantic_model_ref": {"name": semantic_model},
                 "concept_mappings": [{"concept": "Flight", "object_mappings": flight["object_mappings"],
                                       "link_mappings": [link]}]})


async def test_two_mappings_annotate_one_field(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=DELAYS_MAPPING, text=delays_mapping())
    annotations = [a for a in await field_annotations(storage, "FLIGHT.dep_delay")
                   if a["relationship"] == "Flight.departure_delay"]
    assert sorted(a["mapping"] for a in annotations) == [DELAYS_MAPPING, FLIGHTS_MAPPING]


OTHER_MODEL = "Other model"
GATES = dump({"version": "0.3.0.dev0", "name": OTHER_MODEL, "datasets": [
    {"name": "GATES", "source": "GATES", "fields": [{"name": "airport", "expression": {"sql": "airport"}}]}]})


async def test_annotations_follow_the_semantic_model_reference(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=OTHER_MODEL, text=GATES)
    assert await field_annotations(storage, "GATES.airport") == []

    gates_mapping = dump({"version": "0.3.0.dev0", "name": "gates_mapping", "ontology_ref": {"name": FLIGHTS},
                          "semantic_model_ref": {"name": FLIGHTS_DS}, "concept_mappings": [
        {"concept": "Airport", "object_mappings": [{"referent_mappings": [
            {"relationship": "code", "expression": "GATES.airport"}]}]}]})
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name="gates_mapping", text=gates_mapping)
    found = annotation(await field_annotations(storage, "GATES.airport"), mapping="gates_mapping")
    assert (found["entity"], found["concept"]) == ("Airport", "AirportCode")


async def test_an_unresolved_mapping_annotates_nothing(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    model_file = tmp_path / "store" / "semantic_models" / FLIGHTS_DS / f"{FLIGHTS_MODEL}.yaml"
    model_file.write_text(example("flights.semantic_model.yaml").replace(
        "  - name: dep_delay\n    expression:\n      sql: dep_delay\n", ""), encoding="utf-8")
    flight_only = edited(delays_mapping(), lambda d: concept_mapping(d, "Flight").pop("link_mappings"))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=DELAYS_MAPPING, text=flight_only)
    assert await field_annotations(storage, "FLIGHT.arr_delay") == []
    assert (await dataset_payload(storage, "AIRCRAFT")).get("own_concepts") in (None, [])
    assert [a["mapping"] for a in await field_annotations(storage, "FLIGHT.id")] == [DELAYS_MAPPING]


# --------------------------------------------------------------------------- #
# Composite line-up
# --------------------------------------------------------------------------- #
async def test_a_composite_relationship_lines_up(tmp_path: Path) -> None:
    async with seating_engine("yaml", tmp_path, model=seating_model(relationships=[seat_relationship()])) as engine:
        payload = await dataset_payload(engine.storage, "BOOKINGS", SEATING_DS)
        [found] = lined_up(payload, "SEATS")
        assert (found["relationship"], found["mapping"], found["ontology"]) == (
            "Booking.seat", SEATING_MAPPING, SEATING)


async def test_swapped_composite_columns_do_not_line_up(tmp_path: Path) -> None:
    swapped = seat_relationship(to_columns=("letter", "row"))
    async with seating_engine("yaml", tmp_path, model=seating_model(relationships=[swapped])) as engine:
        payload = await dataset_payload(engine.storage, "BOOKINGS", SEATING_DS)
        assert lined_up(payload, "SEATS") == []
        assert [c for f in payload["fields"] for c in f.get("concepts") or [] if c["relationship"] == "Booking.seat"]


async def test_parallel_relationships_line_up_each_with_its_own(tmp_path: Path) -> None:
    original = seat_relationship(name="booking_original_seat", from_columns=("orig_row", "orig_letter"))
    async with seating_engine("yaml", tmp_path, model=seating_model(
            relationships=[seat_relationship(), original])) as engine:
        payload = await dataset_payload(engine.storage, "BOOKINGS", SEATING_DS)
        by_name = {r["name"]: [c["relationship"] for c in r.get("concepts") or []] for r in payload["relationships"]}
        assert by_name["booking_seat"] == ["Booking.seat"]
        assert by_name["booking_original_seat"] == ["Booking.original_seat"]
