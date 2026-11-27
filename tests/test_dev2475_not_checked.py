"""Every ``requires`` SLayer does not check, every multi-dataset position, every failed check query and every
unresolved mapping is listed as not checked.

Spec: ontologies/ontology-use › Every requires not checked is listed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, component, concept_mapping, edited, example,
)
from tests._dev2475_fixtures import flights_engine, flights_model, ontology_findings, validate, violating

FLIGHTS_UNCHECKED = {
    "COUNT[Airport] > 0", "COUNT[Carrier] > 0", "Flight.scheduled_departure < Flight.scheduled_arrival",
    "Flight.departs_at < Flight.arrives_at", "Flight.operated_by(Aircraft.carrier)", "NOT Route.departure(Airport)",
    "NOT Route.destination(Airport)",
}


def unchecked(findings: list[Any]) -> list[Any]:
    return ontology_findings(findings, "not_checked")


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_flights_rules_not_checked(tmp_path: Path, kind: str) -> None:
    """Spec scenario: The flights rules not checked."""
    async with flights_engine(kind, tmp_path) as engine:
        listed = unchecked(await validate(engine))
    assert {f.constraint for f in listed} == FLIGHTS_UNCHECKED
    assert len(listed) == len(FLIGHTS_UNCHECKED)
    assert all((f.mapping, f.ontology) == (FLIGHTS_MAPPING, FLIGHTS) and f.reason for f in listed)


async def test_derived_by_is_not_listed(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        listed = unchecked(await validate(engine))
    assert listed
    assert not [f for f in listed if "AVG[" in (f.constraint or "") + (f.reason or "")]


async def test_a_multi_dataset_position(tmp_path: Path) -> None:
    expression = "concat(ROUTE.name, AIRPORT.name)"

    def multi(document: dict[str, Any]) -> None:
        [child] = [c for c in concept_mapping(document, "Route")["link_mappings"][0]["children"]
                   if c["relationship"] == "route_name"]
        child["object_mapping"]["expression"] = expression

    async with flights_engine("yaml", tmp_path, mapping=edited(example("flights.mapping.yaml"), multi)) as engine:
        listed = unchecked(await validate(engine))
    [found] = [f for f in listed if expression in (f.position or "")]
    assert found.reason
    assert found.mapping == FLIGHTS_MAPPING


async def test_a_check_the_database_rejects(tmp_path: Path) -> None:
    def rejected(document: dict[str, Any]) -> None:
        component(document, "AirportName")["requires"] = ["AirportName > 5"]

    async with flights_engine("yaml", tmp_path, ontology=edited(example("flights.ontology.yaml"), rejected)) as engine:
        findings = await validate(engine)
    [found] = [f for f in unchecked(findings) if f.constraint == "AirportName > 5"]
    assert "AIRPORT.name" in found.position
    assert found.reason
    assert violating(ontology_findings(findings, "constraint")), "the other checks still run"


async def test_an_unresolved_mapping(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        model_file = tmp_path / "store" / "semantic_models" / FLIGHTS_DS / f"{FLIGHTS_MODEL}.yaml"
        model_file.write_text(flights_model().replace(
            "  - name: dep_delay\n    expression:\n      sql: dep_delay\n", ""), encoding="utf-8")
        found = ontology_findings(await validate(engine))
    assert [(f.check, f.mapping, f.ontology) for f in found] == [("not_checked", FLIGHTS_MAPPING, FLIGHTS)]
    assert "dep_delay" in found[0].reason


async def test_a_stored_mapping_of_the_wrong_shape_is_listed_not_checked(tmp_path: Path) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        mapping_file = tmp_path / "store" / "ontology_mappings" / FLIGHTS_DS / f"{FLIGHTS_MAPPING}.yaml"
        mapping_file.write_text(edited(example("flights.mapping.yaml"), lambda d: d.update(ontology_ref=FLIGHTS)),
                                encoding="utf-8")
        found = ontology_findings(await validate(engine))
    assert [(f.check, f.mapping, f.ontology) for f in found] == [("not_checked", FLIGHTS_MAPPING, None)]
    assert "ontology 'None'" not in found[0].summary
