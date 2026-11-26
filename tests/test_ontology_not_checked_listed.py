"""Every ``requires`` SLayer does not check is reported as not checked (ontology principle: unchecked is listed).

Spec: ontologies/ontology-use › Every requires not checked is listed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests._dev2474_fixtures import component, edited, example, load
from tests._dev2475_fixtures import flights_engine, ontology_findings, validate

#: One ``requires`` of each level outside the checked forms.
EXTRA = {
    "document": "COUNT[Flight] >= COUNT[Route]",
    "value type": "Delay < COUNT[Flight]",
    "entity": "Flight.departure_delay > 0",
    "two paths": "Flight.route.destination",
    "relationship": "Flight.arrival_delay >= 0",
}


def with_extra_requires(document: dict[str, Any]) -> None:
    document["requires"].append(EXTRA["document"])
    component(document, "Delay")["requires"] = [EXTRA["value type"]]
    component(document, "Flight")["requires"] = [EXTRA["entity"], EXTRA["two paths"]]
    [arrival] = [r for r in component(document, "Flight")["relationships"] if r["name"] == "arrival_delay"]
    arrival["requires"] = [EXTRA["relationship"]]


def every_requires(document: dict[str, Any]) -> set[str]:
    out = set(document.get("requires") or [])
    for concept in document["ontology"]:
        out |= set(concept.get("requires") or [])
        for relationship in concept.get("relationships") or []:
            out |= set(relationship.get("requires") or [])
    return out


async def test_every_requires_is_checked_or_listed(tmp_path: Path) -> None:
    text = edited(example("flights.ontology.yaml"), with_extra_requires)
    async with flights_engine("yaml", tmp_path, ontology=text) as engine:
        findings = ontology_findings(await validate(engine))
    listed = {f.constraint for f in findings if f.check == "not_checked"}
    assert set(EXTRA.values()) <= listed
    checked_forms = {"CancelationCode == 'A' OR CancelationCode == 'B' OR CancelationCode == 'C' OR "
                     "CancelationCode == 'D'", "DegreesLatitude <= 90", "DegreesLatitude >= -90",
                     "DegreesLongitude <= 180", "DegreesLongitude >= -180", "1 <= DistanceGroup",
                     "DistanceGroup <= 10", "Airport.latitude", "Airport.longitude"}
    assert every_requires(load(text)) - checked_forms == listed
