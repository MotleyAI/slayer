"""Ontology documents are recognised by Ossie's keys and checked when saved; rule expressions are kept, not interpreted.

Spec: ontologies/ontology-documents › Ontology and mapping documents are recognised by Ossie's keys;
Ontologies are checked when saved; Rule expressions are kept, not interpreted.
"""

from __future__ import annotations

import socket
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from tests._dev2243_fixtures import STORAGES
from tests._dev2474_fixtures import (
    FLIGHTS_DS, FLIGHTS_MAPPING, SAP, concept, dump, empty_store, example, flights_store, ontology_text,
    raises_refusal, refusal, rel,
)

INVALID = "OntologyDocumentInvalidError"


async def refused(tmp_path: Path, text: str, *, kind: str = "yaml", name: str = "shop") -> str:
    """Save ``text`` as ontology ``name``; return the refusal and check nothing was stored."""
    storage = await empty_store(kind, tmp_path)
    with raises_refusal(INVALID) as info:
        await storage.save_ontology_text(name=name, text=text)
    assert await storage.list_ontologies() == []
    assert await storage.get_ontology_text(name) is None
    return refusal(info.value)


async def accepted(tmp_path: Path, text: str, *, name: str) -> None:
    storage = await empty_store("yaml", tmp_path)
    await storage.save_ontology_text(name=name, text=text)
    assert await storage.get_ontology_text(name) == text


# --------------------------------------------------------------------------- #
# Recognised by Ossie's keys
# --------------------------------------------------------------------------- #
async def test_wrong_version(tmp_path: Path) -> None:
    """Spec scenario: Wrong version."""
    text = example("flights.ontology.yaml").replace("version: 0.3.0.dev0", "version: 0.2.0", 1)
    assert "0.2.0" in await refused(tmp_path, text, name="Flights")


@pytest.mark.parametrize("text", [
    pytest.param(dump({"version": "0.3.0.dev0", "name": "shop", "datasets": [{"name": "a", "source": "a"}]}),
                 id="semantic-model"),
    pytest.param(dump({"version": "0.3.0.dev0", "name": "shop", "ontology_ref": {"name": "o"},
                       "semantic_model_ref": {"name": "s"}, "concept_mappings": []}), id="mapping"),
])
async def test_a_document_without_root_ontology_is_not_an_ontology(tmp_path: Path, text: str) -> None:
    assert "ontology" in (await refused(tmp_path, text)).lower()


# --------------------------------------------------------------------------- #
# Checked when saved
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_every_problem_is_listed(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Every problem is listed."""
    text = ontology_text(
        concept("Order", extends=["Thing"]),
        concept("Line", identify_by=["order"], relationships=[rel("order", "Order", "Integer")]),
    )
    found = await refused(tmp_path, text, kind=kind)
    assert "Thing" in found
    assert "Line" in found
    assert "order" in found


async def test_inherited_identifier(tmp_path: Path) -> None:
    """Spec scenario: Inherited identifier."""
    await accepted(tmp_path, example("sap_business_partner.ontology.yaml"), name=SAP)


async def test_ambiguous_roles(tmp_path: Path) -> None:
    """Spec scenario: Ambiguous roles."""
    text = ontology_text(concept("Store", relationships=[rel("ships_to", "Store")]))
    found = await refused(tmp_path, text)
    assert "Store.ships_to" in found
    assert "Store" in found.replace("Store.ships_to", "")


async def test_named_roles_make_a_reflexive_relationship_unambiguous(tmp_path: Path) -> None:
    text = ontology_text(concept("Store", relationships=[rel("ships_to", {"concept": "Store", "name": "target"})]))
    await accepted(tmp_path, text, name="shop")


async def test_full_iris_and_qnames(tmp_path: Path) -> None:
    """Spec scenario: Full IRIs and QNames."""
    text = ontology_text(
        concept("Person", iri="foaf:Person"),
        concept("Human", iri="http://schema.org/Person"),
        concept("Book", iri="urn:isbn:0451450523"),
        concept("Thing", iri="ex:Thing"),
        prefixes={"foaf": "http://xmlns.com/foaf/0.1/"},
    )
    storage = await empty_store("yaml", tmp_path)
    with raises_refusal(INVALID) as info:
        await storage.save_ontology_text(name="shop", text=text)
    reasons = getattr(info.value, "reasons", [])
    assert len(reasons) == 1, reasons
    assert "'ex'" in reasons[0] or "ex:" in reasons[0]
    assert "foaf" not in reasons[0]


async def test_value_type_cycle(tmp_path: Path) -> None:
    """Spec scenario: Value type cycle."""
    text = ontology_text(concept("A", "ValueType", extends=["B"]), concept("B", "ValueType", extends=["A"]))
    found = await refused(tmp_path, text)
    assert "A" in found
    assert "B" in found
    assert "cycle" in found.lower() or "extends itself" in found.lower()


@pytest.mark.parametrize(("components", "named"), [
    pytest.param([concept("Order"), concept("Order")], "Order", id="duplicate-concept"),
    pytest.param([concept("Order"), concept("ORDER")], "ORDER", id="duplicate-concept-as-ossie-compares"),
    pytest.param([concept("Order", relationships=[rel("nr", "Integer"), rel("nr", "String")])], "nr",
                 id="duplicate-relationship"),
    pytest.param([concept("Order", relationships=[rel("customer", "Customer")])], "Customer",
                 id="undeclared-role-concept"),
    pytest.param([concept("Code", "ValueType", extends=["Order"]), concept("Order")], "Code",
                 id="value-type-extending-entity-type"),
    pytest.param([concept("Code", "ValueType")], "Code", id="value-type-reaching-no-built-in"),
    pytest.param([concept("Order", extends=["Code"]), concept("Code", "ValueType", extends=["String"])], "Order",
                 id="entity-type-extending-value-type"),
    pytest.param([concept("Order", extends=["String"])], "Order", id="entity-type-extending-built-in"),
    pytest.param([concept("Order", identify_by=["nr"])], "nr", id="identifier-undeclared"),
    pytest.param([concept("Order", identify_by=["shipped"], relationships=[rel("shipped")])], "shipped",
                 id="identifier-unary"),
    pytest.param([concept("Order", relationships=[rel("nr", "Integer")]),
                  concept("Line", identify_by=["nr"])], "nr", id="identifier-on-another-concept"),
    pytest.param([concept("Order", relationships=[rel("nr", "Integer", iri="ex:nr")])], "Order.nr",
                 id="relationship-qname-undeclared"),
    pytest.param([{"concept": "Order"}], "type", id="schema-missing-type"),
    pytest.param([concept("Order", relationships=[rel("nr", "Integer", multiplicity="ManyToMany")])], "ManyToMany",
                 id="schema-bad-multiplicity"),
])
async def test_each_check_refuses(tmp_path: Path, components: list[dict[str, Any]], named: str) -> None:
    assert named in await refused(tmp_path, ontology_text(*components))


async def test_an_identifier_declared_on_a_supertype_resolves(tmp_path: Path) -> None:
    text = ontology_text(
        concept("Party", relationships=[rel("nr", "Integer")]),
        concept("Customer", extends=["Party"], identify_by=["nr"]),
    )
    await accepted(tmp_path, text, name="shop")


async def test_an_undeclared_prefix_value_is_refused(tmp_path: Path) -> None:
    text = ontology_text(concept("Order"), prefixes={"ex": "not an iri"})
    assert "ex" in await refused(tmp_path, text)


async def test_schema_validation_never_touches_the_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_network(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("network accessed")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    storage = await flights_store("yaml", tmp_path)
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []
    with raises_refusal(INVALID):
        await storage.save_ontology_text(name="shop", text=ontology_text(concept("Order", type="Thing")))


# --------------------------------------------------------------------------- #
# Rule expressions are kept, not interpreted
# --------------------------------------------------------------------------- #
async def test_flights_rules_are_kept(tmp_path: Path) -> None:
    """Spec scenario: Flights rules are kept."""
    text = example("flights.ontology.yaml")
    storage = await empty_store("yaml", tmp_path)
    await storage.save_ontology_text(name="Flights", text=text)
    stored = await storage.get_ontology_text("Flights")
    assert stored is not None
    assert stored == text
    assert "Delay == AVG[Flight.departure_delay WHERE Airport == Flight.route.departure GROUP BY Airport]" in stored
    assert "COUNT[Airport] > 0" in stored


async def test_rules_outside_any_grammar_are_accepted(tmp_path: Path) -> None:
    text = ontology_text(
        concept("Order", requires=["%%% not an expression %%%"],
                relationships=[rel("nr", "Integer", derived_by=["Order.nr == RECURSE(Order)"])]),
        requires=["EXISTS ( Order )"],
    )
    await accepted(tmp_path, text, name="shop")


@pytest.mark.parametrize("requires", [
    pytest.param(["COUNT[Order] > 0", "COUNT[Order] > 0"], id="listed-twice"),
    pytest.param([""], id="empty"),
])
async def test_rules_obey_the_schema(tmp_path: Path, requires: list[str]) -> None:
    await refused(tmp_path, ontology_text(concept("Order"), requires=requires))
