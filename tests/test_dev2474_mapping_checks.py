"""Mapping documents resolve against a stored ontology and one semantic model of their datasource.

Spec: ontologies/ontology-documents › Mapping references resolve; Mapping structure follows the ontology;
Mapping expressions are row-level Layer 1 over the referenced semantic model; Mapping expression types fit the concept.
"""

from __future__ import annotations

import re
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from slayer.storage.base import StorageBackend
from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    DEP_DELAY_PATH, FLIGHTS_DS, FLIGHTS_MAPPING, SAP_DS, SAP_MAPPING, SHOP_ONTOLOGY, concept, concept_mapping, dump, edited,
    empty_store, example, flights_store, load, mapping_text, ontology_text, order_mapping, raises_refusal, refusal,
    rel, sap_store, shop_model_text, shop_store,
)

INVALID = "OntologyMappingInvalidError"
FLIGHT = 8  # index of the Flight concept mapping in flights.mapping.yaml


async def refused(storage: StorageBackend, text: str, *, data_source: str, name: str = "m") -> str:
    """Save mapping ``text``; return the refusal and check nothing was stored."""
    with raises_refusal(INVALID) as info:
        await storage.save_ontology_mapping_text(data_source=data_source, name=name, text=text)
    assert await storage.get_ontology_mapping_text(name, data_source) is None
    return refusal(info.value)


async def flights_refused(tmp_path: Path, change: Any, *, model: str | None = None) -> str:
    storage = await flights_store("yaml", tmp_path, mapping=False, model=model)
    return await refused(storage, edited(example("flights.mapping.yaml"), change), data_source=FLIGHTS_DS,
                         name=FLIGHTS_MAPPING)


def flight_link(document: dict[str, Any], index: int) -> dict[str, Any]:
    return concept_mapping(document, "Flight")["link_mappings"][index]


# --------------------------------------------------------------------------- #
# Recognition
# --------------------------------------------------------------------------- #
async def test_a_mapping_document_is_recognised(tmp_path: Path) -> None:
    """Spec scenario: A mapping document is recognised."""
    storage = await flights_store("yaml", tmp_path)
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")
    assert await storage.list_ontology_mappings(FLIGHTS_DS) == [(FLIGHTS_DS, FLIGHTS_MAPPING)]


async def test_a_mapping_with_the_wrong_version_is_refused(tmp_path: Path) -> None:
    assert "0.2.0" in await flights_refused(tmp_path, lambda d: d.update(version="0.2.0"))


async def test_an_ontology_saved_as_a_mapping_is_refused(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    found = await refused(storage, example("flights.ontology.yaml"), data_source=FLIGHTS_DS)
    assert "mapping" in found.lower()


# --------------------------------------------------------------------------- #
# References resolve
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_document_is_referenced(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A document is referenced."""
    storage = await flights_store(kind, tmp_path)
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_datasource_is_referenced(tmp_path: Path, kind: str) -> None:
    """Spec scenario: The datasource is referenced."""
    storage = await shop_store(kind, tmp_path, standalone=True)
    await storage.save_ontology_mapping_text(data_source="shop", name="shop_mapping",
                                             text=order_mapping(semantic_model="shop"))
    assert await storage.ontology_mapping_problems("shop_mapping", "shop") == []


async def test_a_document_named_like_its_datasource_is_meant(tmp_path: Path) -> None:
    storage = await shop_store("yaml", tmp_path, standalone=True)
    assert await storage.delete_dataset("customers", "shop")
    customers_only = edited(shop_model_text(name="shop"), lambda d: d.update(datasets=d["datasets"][1:]))
    await storage.save_semantic_model_text(data_source="shop", name="shop", text=customers_only)
    found = await refused(storage, order_mapping(semantic_model="shop"), data_source="shop")
    assert "orders" in found


async def test_unknown_semantic_model(tmp_path: Path) -> None:
    """Spec scenario: Unknown semantic model."""
    found = await flights_refused(tmp_path, lambda d: d["semantic_model_ref"].update(name="Flights model"))
    assert "Flights model" in found
    assert "Flights semantic model" in found
    assert re.search(r"(?<![\w.])flights(?!\w)", found.replace(FLIGHTS_MAPPING, "")), found


async def test_unknown_ontology(tmp_path: Path) -> None:
    found = await flights_refused(tmp_path, lambda d: d["ontology_ref"].update(name="Flites"))
    assert "Flites" in found


async def test_a_reference_iri_is_kept_and_never_fetched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_network(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("network accessed")

    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    storage = await flights_store("yaml", tmp_path, mapping=False)
    text = edited(example("flights.mapping.yaml"),
                  lambda d: d["ontology_ref"].update(iri="https://example.invalid/flights.ontology.yaml"))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=text)
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == text


async def test_a_mapping_belongs_to_an_existing_datasource(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    text = example("flights.mapping.yaml")
    with pytest.raises(ValueError, match="nowhere"):
        await storage.save_ontology_mapping_text(data_source="nowhere", name=FLIGHTS_MAPPING, text=text)


# --------------------------------------------------------------------------- #
# Structure follows the ontology
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_every_upstream_mapping_example_resolves(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Every upstream mapping example resolves."""
    flights = await flights_store(kind, tmp_path / "flights")
    sap = await sap_store(kind, tmp_path / "sap")
    assert await flights.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []
    assert await sap.ontology_mapping_problems(SAP_MAPPING, SAP_DS) == []


async def test_wrong_link_level(tmp_path: Path) -> None:
    """Spec scenario: Wrong link level."""
    def route_at_depth_one(document: dict[str, Any]) -> None:
        concept_mapping(document, "Flight")["link_mappings"].append({
            "relationship": "route",
            "object_mapping": {"referent_mappings": [{"relationship": "id", "expression": "FLIGHT.id"}]}})

    found = await flights_refused(tmp_path, route_at_depth_one)
    assert f"concept_mappings -> {FLIGHT} -> link_mappings -> 3" in found
    assert "route" in found
    assert "2" in found
    assert "1" in found


async def test_missing_identifier_part(tmp_path: Path) -> None:
    """Spec scenario: Missing identifier part."""
    def designator_only(document: dict[str, Any]) -> None:
        runway = concept_mapping(document, "Runway")
        runway["object_mappings"] = [{"referent_mappings": [
            {"relationship": "designator", "expression": "RUNWAY.designator"}]}]

    found = await flights_refused(tmp_path, designator_only)
    assert "Runway" in found
    assert "airport" in found


@pytest.mark.parametrize(("change", "named"), [
    pytest.param(lambda d: d["concept_mappings"].append({"concept": "Ordr", "object_mappings": [
        {"expression": "FLIGHT.id"}]}), "Ordr", id="undeclared-concept"),
    pytest.param(lambda d: flight_link(d, 2)["children"][1]["object_mapping"].update(concept="Distance"), "Distance",
                 id="object-concept-not-the-expected-one"),
    pytest.param(lambda d: concept_mapping(d, "Runway")["object_mappings"][0]["referent_mappings"].append(
        {"relationship": "designator", "expression": "RUNWAY.designator"}), "designator",
                 id="identifier-named-twice"),
    pytest.param(lambda d: concept_mapping(d, "Runway")["object_mappings"].append(
        {"expression": "RUNWAY.designator"}), "Runway", id="expression-for-a-compound-identifier"),
    pytest.param(lambda d: flight_link(d, 2).update(relationship="number"), "number",
                 id="binary-relationship-at-depth-one"),
    pytest.param(lambda d: flight_link(d, 2)["children"][0].update(relationship="nonstop"), "nonstop",
                 id="undeclared-relationship"),
    pytest.param(lambda d: concept_mapping(d, "Flight")["object_mappings"][0]["referent_mappings"][0].update(
        relationship="Airport.code"), "Airport.code", id="qualified-relationship-of-another-concept"),
    pytest.param(lambda d: concept_mapping(d, "Flight")["object_mappings"][0]["referent_mappings"][0].update(
        relationship="Planet.id"), "Planet", id="qualified-relationship-of-an-undeclared-concept"),
    pytest.param(lambda d: d.pop("semantic_model_ref"), "semantic_model_ref", id="schema-missing-reference"),
    pytest.param(lambda d: concept_mapping(d, "Market").update(object_mappings=[]), "object_mappings",
                 id="schema-empty-list"),
])
async def test_each_structure_check_refuses(tmp_path: Path, change: Any, named: str) -> None:
    assert named in await flights_refused(tmp_path, change)


async def test_a_subtype_of_the_expected_concept_is_accepted(tmp_path: Path) -> None:
    ontology = SHOP_ONTOLOGY.replace(
        "ontology:\n", "ontology:\n- concept: NetAmount\n  type: ValueType\n  extends:\n  - Amount\n", 1)
    storage = await shop_store("yaml", tmp_path, ontology=ontology)
    text = edited(order_mapping(semantic_model="shop_model"), lambda d: d["concept_mappings"][0]["link_mappings"][0][
        "children"][0]["object_mapping"].update(concept="NetAmount"))
    assert await storage.save_ontology_mapping_text(data_source="shop", name="shop_mapping", text=text) == []


async def test_an_entity_with_a_simple_identifier_maps_by_expression(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)
    text = edited(example("flights.mapping.yaml"),
                  lambda d: concept_mapping(d, "Airport").update(object_mappings=[{"expression": "AIRPORT.code"}]))
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=text)
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


async def test_every_structure_problem_is_listed(tmp_path: Path) -> None:
    def two_problems(document: dict[str, Any]) -> None:
        flight_link(document, 2)["children"][0].update(relationship="nonstop")
        document["concept_mappings"].append({"concept": "Ordr", "object_mappings": [{"expression": "FLIGHT.id"}]})

    found = await flights_refused(tmp_path, two_problems)
    assert "nonstop" in found
    assert "Ordr" in found


# --------------------------------------------------------------------------- #
# Expressions are row-level Layer 1
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_filtered_expressions(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Filtered expressions."""
    flights = await flights_store(kind, tmp_path / "flights")
    sap = await sap_store(kind, tmp_path / "sap")
    assert "FLIGHT.id WHERE ( FLIGHT.diverted == TRUE )" in (
        await flights.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) or "")
    assert "CUSTOMER_SALES_AREA.SALES_ORG WHERE ( CUSTOMER_SALES_AREA.DELIVERY_BLOCK <> '' )" in (
        await sap.get_ontology_mapping_text(SAP_MAPPING, SAP_DS) or "")


def _departure_delay_reads(expression: str) -> Any:
    def change(document: dict[str, Any]) -> None:
        flight_link(document, 2)["children"][1]["object_mapping"]["expression"] = expression
    return change


async def test_unknown_field(tmp_path: Path) -> None:
    """Spec scenario: Unknown field."""
    found = await flights_refused(tmp_path, _departure_delay_reads("FLIGHT.departure_delay"))
    assert DEP_DELAY_PATH in found
    assert "departure_delay" in found


async def test_aggregate(tmp_path: Path) -> None:
    """Spec scenario: Aggregate."""
    assert DEP_DELAY_PATH in await flights_refused(tmp_path, _departure_delay_reads("AVG(FLIGHT.dep_delay)"))


@pytest.mark.parametrize("expression", [
    pytest.param("FLIGHTS.dep_delay", id="unknown-dataset"),
    pytest.param("rank() OVER (ORDER BY FLIGHT.dep_delay)", id="window"),
    pytest.param("FLIGHT.dep_delay WHERE ( FLIGHT.nr )", id="non-boolean-predicate"),
    pytest.param("FLIGHT.dep_delay WHERE ( FLIGHT.nope = 1 )", id="unknown-field-in-predicate"),
    pytest.param("FLIGHT.dep_delay WHERE ( COUNT(*) > 1 )", id="aggregate-in-predicate"),
    pytest.param("FLIGHT.dep_delay +", id="unparseable"),
    pytest.param("dep_delay", id="unqualified-field"),
])
async def test_each_expression_check_refuses(tmp_path: Path, expression: str) -> None:
    assert DEP_DELAY_PATH in await flights_refused(tmp_path, _departure_delay_reads(expression))


async def test_an_expression_in_a_nested_referent_is_checked(tmp_path: Path) -> None:
    def bad_nested(document: dict[str, Any]) -> None:
        runway = concept_mapping(document, "Runway")["object_mappings"][0]
        runway["referent_mappings"][1]["referent_mappings"][0]["expression"] = "RUNWAY.airport"

    found = await flights_refused(tmp_path, bad_nested)
    assert "concept_mappings -> 0 -> object_mappings -> 0 -> referent_mappings -> 1 -> referent_mappings -> 0" in found


# --------------------------------------------------------------------------- #
# Expression types fit the concept
# --------------------------------------------------------------------------- #
def _typed_dep_delay(datatype: str) -> str:
    def change(document: dict[str, Any]) -> None:
        [flight] = [d for d in document["datasets"] if d["name"] == "FLIGHT"]
        [field] = [f for f in flight["fields"] if f["name"] == "dep_delay"]
        field["datatype"] = datatype
    return edited(example("flights.semantic_model.yaml"), change)


async def test_incompatible_type(tmp_path: Path) -> None:
    """Spec scenario: Incompatible type."""
    storage = await flights_store("yaml", tmp_path, mapping=False, model=_typed_dep_delay("String"))
    found = await refused(storage, example("flights.mapping.yaml"), data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING)
    assert DEP_DELAY_PATH in found
    assert "String" in found
    assert "Delay" in found


@pytest.mark.parametrize("kind", STORAGES)
async def test_untyped_fields(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Untyped fields."""
    storage = await flights_store(kind, tmp_path, mapping=False)
    notes = await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                     text=example("flights.mapping.yaml"))
    assert len(notes) == 1, notes
    assert DEP_DELAY_PATH in str(notes[0])
    assert "concept_mappings -> 0" in str(notes[0])


async def test_a_typed_compatible_mapping_reports_nothing(tmp_path: Path) -> None:
    storage = await shop_store("yaml", tmp_path)
    notes = await storage.save_ontology_mapping_text(data_source="shop", name="shop_mapping",
                                                     text=order_mapping(semantic_model="shop_model"))
    assert notes == []


async def test_a_typed_mismatch_in_a_small_model(tmp_path: Path) -> None:
    storage = await shop_store("yaml", tmp_path, amount_type="String")
    found = await refused(storage, order_mapping(semantic_model="shop_model"), data_source="shop")
    assert "Amount" in found
    assert "String" in found


async def test_an_entity_mapped_by_expression_takes_its_identifiers_type(tmp_path: Path) -> None:
    storage = await shop_store("yaml", tmp_path)
    text = mapping_text({"concept": "Customer", "object_mappings": [{"expression": "orders.order_id"}]},
                        semantic_model="shop_model")
    found = await refused(storage, text, data_source="shop")
    assert "Customer" in found
    assert "Integer" in found


@pytest.mark.parametrize(("built_in", "datatype", "fits"), [
    ("Integer", "Float", True), ("Decimal", "Integer", True), ("Float", "Decimal", True),
    ("Boolean", "Boolean", True), ("Boolean", "String", False), ("String", "String", True),
    ("String", "Integer", False), ("Date", "Date", True), ("Date", "DateTime", False),
    ("DateTime", "DateTime", True), ("DateTime", "Date", True), ("Any", "String", True), ("Any", "Boolean", True),
    ("Integer", "String", False), ("Decimal", "Date", False),
])
async def test_type_families(tmp_path: Path, built_in: str, datatype: str, fits: bool) -> None:
    ontology = ontology_text(
        concept("OrderNr", "ValueType", extends=["Integer"]),
        concept("Value", "ValueType", extends=[built_in]),
        concept("Order", identify_by=["nr"], relationships=[rel("nr", "OrderNr"), rel("value", "Value")]),
    )
    model = dump({"version": "0.3.0.dev0", "name": "shop_model", "datasets": [{
        "name": "orders", "source": "orders", "primary_key": ["order_id"],
        "fields": [{"name": "order_id", "expression": {"sql": "order_id"}, "datatype": "Integer"},
                   {"name": "value", "expression": {"sql": "value"}, "datatype": datatype}]}]})
    storage = await empty_store("yaml", tmp_path, "shop")
    await storage.save_ontology_text(name="shop", text=ontology)
    await storage.save_semantic_model_text(data_source="shop", name="shop_model", text=model)
    text = mapping_text({"concept": "Order", "link_mappings": [{
        "object_mapping": {"referent_mappings": [{"relationship": "nr", "expression": "orders.order_id"}]},
        "children": [{"relationship": "value", "object_mapping": {"concept": "Value",
                                                                  "expression": "orders.value"}}]}]},
        semantic_model="shop_model")
    if fits:
        assert await storage.save_ontology_mapping_text(data_source="shop", name="m", text=text) == []
    else:
        found = await refused(storage, text, data_source="shop")
        assert datatype in found
        assert "Value" in found


async def test_a_mapping_stays_stored_as_written(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    stored = await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert stored is not None
    assert load(stored) == load(example("flights.mapping.yaml"))
