"""Identifier and multiplicity checks of the semantic model against the ontology, reported by ``validate_datasets``.

Spec: ontologies/ontology-use › Identifiers are checked against dataset keys; Multiplicities are checked against
lined-up relationships.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import RelationshipCardinality
from slayer.core.models import DatasetRelationship
from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, dump, load
from tests._dev2475_fixtures import (
    SEATING, SEATING_DS, SEATING_MAPPING, aircraft_relationship, flights_engine, flights_model, none_naming,
    ontology_findings, seat_relationship, seating_engine, seating_mapping, seating_model, text_of, validate,
    with_relationship,
)

MANY_TO_ONE, ONE_TO_MANY = RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_MANY
ONE_TO_ONE = RelationshipCardinality.ONE_TO_ONE


def identifier(findings: list[Any], *, concept: str, dataset: str) -> list[Any]:
    return [f for f in ontology_findings(findings, "identifier") if (f.concept, f.dataset_name) == (concept, dataset)]


def multiplicity(findings: list[Any]) -> list[Any]:
    return ontology_findings(findings, "multiplicity")


async def ran(engine: Any, data_source: str = FLIGHTS_DS) -> list[Any]:
    """``validate_datasets``' entries, which hold ontology findings."""
    findings = await validate(engine, data_source)
    assert ontology_findings(findings)
    return findings


def with_unique_keys(model: str, dataset: str, keys: list[list[str]]) -> str:
    document = load(model)
    [target] = [d for d in document["datasets"] if d["name"] == dataset]
    target["unique_keys"] = keys
    return dump(document)


# --------------------------------------------------------------------------- #
# Identifiers
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_no_key_declared(tmp_path: Path, kind: str) -> None:
    """Spec scenario: No key declared."""
    async with flights_engine(kind, tmp_path) as engine:
        findings = await validate(engine)
    [aircraft] = identifier(findings, concept="Aircraft", dataset="AIRCRAFT")
    assert (aircraft.mapping, aircraft.ontology, aircraft.data_source) == (FLIGHTS_MAPPING, FLIGHTS, FLIGHTS_DS)
    assert aircraft.fields == ["tail_nr"]
    assert "primary key" in aircraft.suggestion
    assert "tail_nr" in aircraft.suggestion
    found = ontology_findings(findings, "identifier")
    assert none_naming(found, '"Model"')
    assert none_naming(found, '"Manufacturer"')
    own = {("Aircraft", "AIRCRAFT"), ("Airport", "AIRPORT"), ("Carrier", "CARRIER"), ("Route", "ROUTE"),
           ("Runway", "RUNWAY")}
    assert own <= {(f.concept, f.dataset_name) for f in found} <= {*own, ("Flight", "FLIGHT")}
    [runway] = identifier(findings, concept="Runway", dataset="RUNWAY")
    assert sorted(runway.fields) == ["airport_code", "designator"]
    assert "primary key" in runway.suggestion


async def test_a_composite_key_agrees(tmp_path: Path) -> None:
    """Spec scenario: A composite key agrees."""
    async with flights_engine("yaml", tmp_path, model=flights_model(RUNWAY=["designator", "airport_code"])) as engine:
        assert identifier(await ran(engine), concept="Runway", dataset="RUNWAY") == []


async def test_a_primary_key_agrees(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path, model=flights_model(AIRCRAFT=["tail_nr"])) as engine:
        assert identifier(await ran(engine), concept="Aircraft", dataset="AIRCRAFT") == []


async def test_a_single_field_beside_another_primary_key_is_suggested_unique(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path, model=flights_model(AIRCRAFT=["serial_nr"])) as engine:
        [found] = identifier(await validate(engine), concept="Aircraft", dataset="AIRCRAFT")
    assert "unique" in found.suggestion
    assert "tail_nr" in found.suggestion


async def test_a_unique_field_agrees(tmp_path: Path) -> None:
    model = with_unique_keys(flights_model(AIRCRAFT=["serial_nr"]), "AIRCRAFT", [["tail_nr"]])
    async with flights_engine("yaml", tmp_path, model=model) as engine:
        assert identifier(await ran(engine), concept="Aircraft", dataset="AIRCRAFT") == []


async def test_a_unique_singleton_inside_a_compound_identifier_agrees(tmp_path: Path) -> None:
    model = with_unique_keys(flights_model(), "RUNWAY", [["designator"]])
    async with flights_engine("yaml", tmp_path, model=model) as engine:
        assert identifier(await ran(engine), concept="Runway", dataset="RUNWAY") == []


async def test_a_compound_identifier_beside_another_primary_key_cannot_be_declared(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path, model=flights_model(RUNWAY=["shape"])) as engine:
        [found] = identifier(await validate(engine), concept="Runway", dataset="RUNWAY")
    assert "cannot" in found.suggestion


async def test_an_enforced_composite_key_agrees(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path, model=flights_model(RUNWAY=["designator", "airport_code"])) as engine:
        runway = await engine.storage.get_dataset("RUNWAY", data_source=FLIGHTS_DS)
        assert runway is not None
        await engine.save_dataset(runway.model_copy(update={"enforced_keys": [["designator", "airport_code"]]}))
        assert identifier(await ran(engine), concept="Runway", dataset="RUNWAY") == []


async def test_seat_identifiers(tmp_path: Path) -> None:
    async with seating_engine("yaml", tmp_path / "bare") as engine:
        [seat] = identifier(await validate(engine, SEATING_DS), concept="Seat", dataset="SEATS")
    assert (seat.mapping, seat.ontology) == (SEATING_MAPPING, SEATING)
    assert sorted(seat.fields) == ["letter", "row"]
    keyed = seating_model(primary_keys={"SEATS": ["letter", "row"], "TICKETS": ["nr"], "BOOKINGS": ["id"]})
    async with seating_engine("yaml", tmp_path / "keyed", model=keyed) as engine:
        assert ontology_findings(await ran(engine, SEATING_DS), "identifier") == []


async def test_validation_never_changes_the_stored_model(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        before = await engine.storage.get_semantic_model_text("Flights semantic model", data_source=FLIGHTS_DS)
        assert ontology_findings(await validate(engine), "identifier")
        assert await engine.storage.get_semantic_model_text("Flights semantic model", data_source=FLIGHTS_DS) == before


# --------------------------------------------------------------------------- #
# Multiplicities
# --------------------------------------------------------------------------- #
async def test_a_contradicting_cardinality(tmp_path: Path) -> None:
    """Spec scenario: A contradicting cardinality."""
    async with flights_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, aircraft_relationship(ONE_TO_MANY, name="uses"))
        [found] = multiplicity(await validate(engine))
    assert (found.mapping, found.ontology, found.relationship) == (FLIGHTS_MAPPING, FLIGHTS, "Flight.aircraft")
    assert "ManyToOne" in text_of(found)
    assert "uses" in text_of(found)
    assert "FLIGHT" in text_of(found)


async def test_an_agreeing_cardinality(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, aircraft_relationship(MANY_TO_ONE))
        assert multiplicity(await ran(engine)) == []


async def test_no_cardinality(tmp_path: Path) -> None:
    """Spec scenario: No cardinality."""
    query = {"population": "FLIGHT", "dimensions": ["AIRCRAFT.model"], "metrics": ["count(FLIGHT.*)"]}
    async with flights_engine("yaml", tmp_path, mapping="") as engine:
        await with_relationship(engine, aircraft_relationship(None))
        without = await engine.execute(query=query, data_source=FLIGHTS_DS)
    async with flights_engine("yaml", tmp_path / "mapped") as engine:
        await with_relationship(engine, aircraft_relationship(None))
        [found] = multiplicity(await validate(engine))
        mapped = await engine.execute(query=query, data_source=FLIGHTS_DS)
        stored = await engine.storage.get_dataset("FLIGHT", data_source=FLIGHTS_DS)
    assert found.relationship == "Flight.aircraft"
    assert "many_to_one" in found.suggestion
    assert "trusted" not in found.suggestion.lower()
    assert (mapped.sql, mapped.data) == (without.sql, without.data)
    assert stored is not None
    assert [(r.cardinality, r.trusted) for r in stored.relationships if r.to == "AIRCRAFT"] == [(None, False)]


@pytest.mark.parametrize(("cardinality", "mismatch"), [(ONE_TO_MANY, False), (MANY_TO_ONE, True)])
async def test_the_other_orientation(tmp_path: Path, cardinality: RelationshipCardinality, mismatch: bool) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, aircraft_relationship(cardinality, reverse=True), on="AIRCRAFT")
        found = multiplicity(await ran(engine))
    assert [f.relationship for f in found] == (["Flight.aircraft"] if mismatch else [])


def ticket_relationship(cardinality: RelationshipCardinality, *, reverse: bool = False) -> DatasetRelationship:
    if reverse:
        return DatasetRelationship(to="BOOKINGS", from_columns=["nr"], to_columns=["ticket_nr"],
                                   cardinality=cardinality)
    return DatasetRelationship(to="TICKETS", from_columns=["ticket_nr"], to_columns=["nr"], cardinality=cardinality)


@pytest.mark.parametrize(("cardinality", "reverse", "mismatch"), [
    pytest.param(ONE_TO_ONE, False, False, id="one-to-one"),
    pytest.param(MANY_TO_ONE, False, True, id="many-to-one"),
    pytest.param(ONE_TO_ONE, True, False, id="reversed-one-to-one"),
    pytest.param(ONE_TO_MANY, True, True, id="reversed-one-to-many"),
])
async def test_one_to_one_in_both_orientations(tmp_path: Path, cardinality: RelationshipCardinality, reverse: bool,
                                               mismatch: bool) -> None:
    async with seating_engine("yaml", tmp_path, mapping=seating_mapping(ticket="BOOKINGS.ticket_nr")) as engine:
        await with_relationship(engine, ticket_relationship(cardinality, reverse=reverse),
                                on="TICKETS" if reverse else "BOOKINGS", data_source=SEATING_DS)
        found = multiplicity(await ran(engine, SEATING_DS))
    assert [f.relationship for f in found] == (["Booking.ticket"] if mismatch else [])
    assert all("OneToOne" in text_of(f) for f in found)


async def test_a_position_with_a_predicate_does_not_line_up(tmp_path: Path) -> None:
    async with seating_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, ticket_relationship(MANY_TO_ONE), on="BOOKINGS", data_source=SEATING_DS)
        assert multiplicity(await ran(engine, SEATING_DS)) == []


async def test_parallel_relationships_are_compared_each_with_its_own(tmp_path: Path) -> None:
    async with seating_engine("yaml", tmp_path) as engine:
        for name, columns, cardinality in (("booking_seat", ("seat_row", "seat_letter"), MANY_TO_ONE),
                                           ("booking_original_seat", ("orig_row", "orig_letter"), ONE_TO_MANY)):
            await with_relationship(engine, DatasetRelationship(
                name=name, to="SEATS", from_columns=list(columns), to_columns=["row", "letter"],
                cardinality=cardinality), on="BOOKINGS", data_source=SEATING_DS)
        [found] = multiplicity(await validate(engine, SEATING_DS))
    assert found.relationship == "Booking.original_seat"
    assert "booking_original_seat" in text_of(found)


async def test_swapped_composite_columns_are_not_compared(tmp_path: Path) -> None:
    swapped = seat_relationship(to_columns=("letter", "row"))
    async with seating_engine("yaml", tmp_path, model=seating_model(relationships=[swapped])) as engine:
        assert multiplicity(await ran(engine, SEATING_DS)) == []


async def test_unary_and_ternary_relationships_are_not_compared(tmp_path: Path) -> None:
    async with seating_engine("yaml", tmp_path) as engine:
        found = ontology_findings(await ran(engine, SEATING_DS))
    assert none_naming(found, "Booking.cancelled")
    assert none_naming(found, "Booking.fare_for")
