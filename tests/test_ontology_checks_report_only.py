"""No ontology check refuses a save or changes a stored model (ontology principle: checks report, never rewrite).

Spec: ontologies/ontology-use › Model checks warn on save and never refuse.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import RelationshipCardinality
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, example
from tests._dev2475_fixtures import (
    aircraft_relationship, flights_engine, flights_model, ontology_findings, validate, with_relationship,
)


async def snapshot(engine: SlayerQueryEngine) -> dict[str, Any]:
    """Every stored document's text and every dataset of the flights datasource."""
    storage = engine.storage
    datasets, errors = await storage.load_datasets(data_source=FLIGHTS_DS)
    assert errors == []
    return {
        "ontology": await storage.get_ontology_text(FLIGHTS),
        "mapping": await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS),
        "model": await storage.get_semantic_model_text(FLIGHTS_MODEL, data_source=FLIGHTS_DS),
        "datasets": sorted((d.model_dump_json() for d in datasets)),
    }


@pytest.mark.parametrize("kind", STORAGES)
async def test_saves_with_mismatches_are_stored_as_given(tmp_path: Path, kind: str) -> None:
    async with flights_engine(kind, tmp_path, mapping="") as engine:
        storage = engine.storage
        notes: list[Any] = []
        await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                 text=example("flights.mapping.yaml"), warnings_out=notes)
        await storage.save_ontology_text(name=FLIGHTS, text=example("flights.ontology.yaml"), warnings_out=notes)
        await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL, text=flights_model(),
                                               warnings_out=notes)
        await with_relationship(engine, aircraft_relationship(RelationshipCardinality.ONE_TO_MANY),
                                warnings_out=notes)
        assert notes
        assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")
        assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")
        flight = await storage.get_dataset("FLIGHT", data_source=FLIGHTS_DS)
        assert flight is not None
        assert [(r.cardinality, r.trusted) for r in flight.relationships] == [
            (RelationshipCardinality.ONE_TO_MANY, False)]


@pytest.mark.parametrize("cardinality", [None, RelationshipCardinality.ONE_TO_MANY])
async def test_validation_changes_nothing(tmp_path: Path, cardinality: RelationshipCardinality | None) -> None:
    async with flights_engine("yaml", tmp_path) as engine:
        await with_relationship(engine, aircraft_relationship(cardinality))
        before = await snapshot(engine)
        found = ontology_findings(await validate(engine))
        assert {"identifier", "multiplicity", "constraint", "required_link", "not_checked"} <= {f.check for f in found}
        assert await snapshot(engine) == before
