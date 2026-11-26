"""Value-type constraints and required links checked against the data by ``validate_datasets``.

Spec: ontologies/ontology-use › Value constraints are checked against the data; Required links are checked against
the data.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import FLIGHTS, FLIGHTS_MAPPING, component, concept_mapping, dump, edited, example, load
from tests._dev2475_fixtures import (
    SEATING, SEATING_DS, SEATING_MAPPING, flights_engine, flights_model, ontology_findings, seating_engine,
    seating_mapping, seating_ontology, validate, violating,
)


def constraint_findings(findings: list[Any], concept: str) -> list[Any]:
    return [f for f in ontology_findings(findings, "constraint") if f.concept == concept]


def broken(findings: list[Any], concept: str) -> list[Any]:
    """The constraint findings of ``concept`` counting a violation."""
    return violating(constraint_findings(findings, concept))


def links(findings: list[Any], relationship: str, position: str) -> list[Any]:
    """The required-link findings of ``relationship`` at the position whose expression names ``position``."""
    return [f for f in ontology_findings(findings, "required_link")
            if f.relationship == relationship and position in (f.position or "")]


def samples(finding: Any) -> str:
    return json.dumps(finding.samples, default=str)


@pytest.fixture
async def flights_findings(tmp_path: Path) -> list[Any]:
    async with flights_engine("yaml", tmp_path) as engine:
        return await validate(engine)


@pytest.fixture
async def seating_findings(tmp_path: Path) -> list[Any]:
    async with seating_engine("yaml", tmp_path) as engine:
        return await validate(engine, SEATING_DS)


# --------------------------------------------------------------------------- #
# Value constraints
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_an_out_of_range_latitude(tmp_path: Path, kind: str) -> None:
    """Spec scenario: An out-of-range latitude."""
    async with flights_engine(kind, tmp_path) as engine:
        [found] = broken(await validate(engine), "DegreesLatitude")
    assert (found.mapping, found.ontology, found.constraint) == (FLIGHTS_MAPPING, FLIGHTS, "DegreesLatitude <= 90")
    assert "AIRPORT.latitude" in found.position
    assert (found.violations, found.samples) == (1, [95])


def test_null_is_not_a_violation(flights_findings: list[Any]) -> None:
    """Spec scenario: NULL is not a violation."""
    [found] = broken(flights_findings, "CancelationCode")
    assert "FLIGHT.cancel_code" in found.position
    assert (found.violations, found.samples) == (1, ["X"])


def test_reversed_comparisons(flights_findings: list[Any]) -> None:
    [found] = broken(flights_findings, "DistanceGroup")
    assert found.constraint == "DistanceGroup <= 10"
    assert "ROUTE.dist_grp" in found.position
    assert (found.violations, found.samples) == (1, [12])


def test_a_requires_list_is_checked_entry_by_entry(flights_findings: list[Any]) -> None:
    assert [f.constraint for f in broken(flights_findings, "DegreesLatitude")] == ["DegreesLatitude <= 90"]
    assert broken(flights_findings, "DegreesLongitude") == []


def test_unary_minus_is_checked(flights_findings: list[Any]) -> None:
    assert broken(flights_findings, "DegreesLatitude")
    unchecked = [f.constraint for f in ontology_findings(flights_findings, "not_checked")]
    assert "DegreesLatitude >= -90" not in unchecked
    assert "DegreesLongitude >= -180" not in unchecked


def test_not(seating_findings: list[Any]) -> None:
    [found] = broken(seating_findings, "RowNr")
    assert (found.mapping, found.ontology) == (SEATING_MAPPING, SEATING)
    assert found.constraint == "NOT (RowNr > 50)"
    assert "SEATS.row" in found.position
    assert (found.violations, found.samples) == (1, [51])


def test_in(seating_findings: list[Any]) -> None:
    [found] = broken(seating_findings, "SeatLetter")
    assert "SEATS.letter" in found.position
    assert (found.violations, found.samples) == (1, ["Z"])


def test_a_quoted_literal_holding_the_concept_name(seating_findings: list[Any]) -> None:
    [found] = broken(seating_findings, "Zone")
    assert found.constraint == "Zone != 'Zone'"
    assert (found.violations, found.samples) == (1, ["Zone"])


def test_a_filtered_position_checks_only_its_rows(seating_findings: list[Any]) -> None:
    assert broken(seating_findings, "Zone")
    assert broken(seating_findings, "TicketNr") == []


async def test_an_unfiltered_position_checks_every_row(tmp_path: Path) -> None:
    async with seating_engine("yaml", tmp_path, mapping=seating_mapping(ticket="BOOKINGS.ticket_nr")) as engine:
        [found] = broken(await validate(engine, SEATING_DS), "TicketNr")
    assert "BOOKINGS.ticket_nr" in found.position
    assert (found.violations, found.samples) == (1, ["VOID"])


async def test_at_most_five_samples(tmp_path: Path) -> None:
    ontology = edited(seating_ontology(), lambda d: component(d, "TicketNr").update(requires=["TicketNr == 'none'"]))
    async with seating_engine("yaml", tmp_path, ontology=ontology) as engine:
        [found] = [f for f in broken(await validate(engine, SEATING_DS), "TicketNr") if "TICKETS.nr" in f.position]
    assert found.violations == 7
    assert len(found.samples) == 5
    assert len(set(found.samples)) == 5


async def test_a_subtypes_position_is_checked_against_the_inherited_constraint(tmp_path: Path) -> None:
    def precise(document: dict[str, Any]) -> None:
        document["ontology"].append({"concept": "PreciseLatitude", "type": "ValueType",
                                     "extends": ["DegreesLatitude"]})

    def mapped_precise(document: dict[str, Any]) -> None:
        [airport] = [m for m in document["concept_mappings"] if m["concept"] == "Airport"]
        [latitude] = [c for c in airport["link_mappings"][0]["children"] if c["relationship"] == "latitude"]
        latitude["object_mapping"]["concept"] = "PreciseLatitude"

    async with flights_engine("yaml", tmp_path, ontology=edited(example("flights.ontology.yaml"), precise),
                              mapping=edited(example("flights.mapping.yaml"), mapped_precise)) as engine:
        [found] = [f for f in violating(ontology_findings(await validate(engine), "constraint"))
                   if f.constraint == "DegreesLatitude <= 90"]
    assert found.concept in ("PreciseLatitude", "DegreesLatitude")
    assert (found.violations, found.samples) == (1, [95])


async def test_a_computed_single_dataset_position(tmp_path: Path) -> None:
    def computed(document: dict[str, Any]) -> None:
        [route] = [m for m in document["concept_mappings"] if m["concept"] == "Route"]
        [group] = [c for c in route["link_mappings"][0]["children"] if c["relationship"] == "lies_in"]
        group["object_mapping"]["expression"] = "ROUTE.dist_grp + 1"

    async with flights_engine("yaml", tmp_path, mapping=edited(example("flights.mapping.yaml"), computed)) as engine:
        [found] = broken(await validate(engine), "DistanceGroup")
    assert "ROUTE.dist_grp + 1" in found.position
    assert (found.violations, found.samples) == (1, [13])


def _renamed_airport(node: Any) -> Any:
    """``node`` with dataset ``AIRPORT`` renamed ``AIRPORT DATA``, references quoted."""
    if isinstance(node, dict):
        return {k: _renamed_airport(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_renamed_airport(v) for v in node]
    if node == "AIRPORT":
        return "AIRPORT DATA"
    return re.sub(r"\bAIRPORT\.", '"AIRPORT DATA".', node) if isinstance(node, str) else node


async def test_a_dataset_name_needing_quotes_is_checked(tmp_path: Path) -> None:
    model = dump(_renamed_airport(load(flights_model())))
    mapping = dump(_renamed_airport(load(example("flights.mapping.yaml"))))
    async with flights_engine("yaml", tmp_path, model=model, mapping=mapping) as engine:
        [found] = broken(await validate(engine), "DegreesLatitude")
    assert (found.violations, found.samples) == (1, [95])


async def test_a_query_sourced_dataset_is_checked_through_its_query(tmp_path: Path) -> None:
    model = flights_model().replace("source: AIRPORTS", "source: SELECT * FROM AIRPORTS WHERE code <> 'DCA'")
    async with flights_engine("yaml", tmp_path, model=model) as engine:
        findings = await validate(engine)
    assert broken(findings, "DegreesLatitude") == []
    assert [f.violations for f in links(findings, "Airport.latitude", "AIRPORT.code")] != []
    assert all(f.violations == 1 for f in links(findings, "Airport.latitude", "AIRPORT.code"))


# --------------------------------------------------------------------------- #
# Required links
# --------------------------------------------------------------------------- #
def test_an_airport_with_no_latitude(flights_findings: list[Any]) -> None:
    """Spec scenario: An airport with no latitude."""
    found = links(flights_findings, "Airport.latitude", "AIRPORT.code")
    assert found
    assert all((f.mapping, f.ontology, f.concept) == (FLIGHTS_MAPPING, FLIGHTS, "Airport") for f in found)
    assert all(f.violations == 1 and "BOS" in samples(f) for f in found)
    assert not violating(links(flights_findings, "Airport.longitude", "AIRPORT.code"))


def test_an_airport_known_only_from_routes(flights_findings: list[Any]) -> None:
    """Spec scenario: An airport known only from routes."""
    for relationship in ("Airport.latitude", "Airport.longitude"):
        [found] = links(flights_findings, relationship, "ROUTE.dest_airport_code")
        assert found.violations == 1
        assert "ZZZ" in samples(found)
    assert not violating(links(flights_findings, "Airport.latitude", "ROUTE.orig_airport_code"))
    assert not violating(links(flights_findings, "Airport.latitude", "RUNWAY.airport_code"))


def _flat_runway_airport(document: dict[str, Any]) -> None:
    """The Runway object mapping's ``airport`` referent written as an ``expression`` instead of nested referents."""
    for node in (concept_mapping(document, "Runway")["object_mappings"][0],
                 concept_mapping(document, "Runway")["link_mappings"][0]["object_mapping"]):
        node["referent_mappings"][1] = {"relationship": "airport", "expression": "RUNWAY.airport_code"}


async def test_an_entity_referenced_by_an_expression_is_checked(tmp_path: Path) -> None:
    mapping = edited(example("flights.mapping.yaml"), _flat_runway_airport)
    async with flights_engine("yaml", tmp_path, mapping=mapping) as engine:
        findings = await validate(engine)
    assert links(findings, "Airport.latitude", "RUNWAY.airport_code")


def test_a_compound_identifier_with_a_null_part_makes_no_object(seating_findings: list[Any]) -> None:
    [found] = links(seating_findings, "Seat.zone", "SEATS.row")
    assert (found.mapping, found.concept) == (SEATING_MAPPING, "Seat")
    assert found.violations == 1
    assert "B" in samples(found)
    assert not violating(links(seating_findings, "Seat.zone", "BOOKINGS.seat_row"))
