"""MCP tools and ``inspect`` kinds for ontologies and mappings, within the surface rules.

Spec: ontologies/ontology-documents › Ontologies and mappings are reachable from every surface (An agent edits one
concept); mcp/tool-surface › The ontology tools fit the surface rules.
"""

from __future__ import annotations

import asyncio
from functools import cache
from pathlib import Path
from typing import Any

import pytest

import slayer.mcp.server as server_mod
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._dataset_access_fixtures import mcp_text
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, component, concept_mapping, edited, empty_store, example,
    flights_store, load, rel,
)

NEW_TOOLS = {
    "create_ontology": {"content"},
    "edit_ontology": {"name", "description", "ai_context", "prefixes", "requires", "concepts", "remove_concepts",
                      "relationships", "remove_relationships"},
    "delete_ontology": {"name"},
    "create_ontology_mapping": {"data_source", "content"},
    "edit_ontology_mapping": {"name", "data_source", "description", "ontology_ref", "semantic_model_ref",
                              "concept_mappings", "remove_concept_mappings"},
    "delete_ontology_mapping": {"name", "data_source"},
}


@cache
def _tools(base: str) -> dict[str, Any]:
    server = create_mcp_server(storage=YAMLStorage(base_dir=base), _seed_help=False)
    return {t.name: t for t in asyncio.run(server.list_tools())}


@pytest.fixture(scope="module")
def tools(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return _tools(str(tmp_path_factory.mktemp("mcp_surface")))


def carrier_with_country() -> dict[str, Any]:
    carrier = component(load(example("flights.ontology.yaml")), "Carrier")
    carrier["relationships"].append(rel("country", "String", verbalizes=["{Carrier} is based in {String}"],
                                        multiplicity="ManyToOne"))
    return carrier


# --------------------------------------------------------------------------- #
# The ontology tools fit the surface rules
# --------------------------------------------------------------------------- #
class TestTheOntologyToolsFitTheBudget:
    """Spec scenario: The ontology tools fit the budget."""

    @pytest.mark.parametrize("name", sorted(NEW_TOOLS))
    def test_each_description_fits_the_budget(self, tools: dict[str, Any], name: str) -> None:
        assert name in tools, sorted(tools)
        assert len(tools[name].description or "") <= server_mod.MAX_TOOL_DESCRIPTION_CHARS

    @pytest.mark.parametrize("name", sorted(NEW_TOOLS))
    def test_every_parameter_is_described(self, tools: dict[str, Any], name: str) -> None:
        assert name in tools, sorted(tools)
        props = tools[name].input_schema.get("properties", {})
        assert NEW_TOOLS[name] <= set(props), sorted(props)
        undescribed = [p for p, schema in props.items() if not (schema.get("description") or "").strip()]
        assert undescribed == []

    def test_inspect_names_the_new_kinds(self, tools: dict[str, Any]) -> None:
        entity_type = tools["inspect"].input_schema["properties"]["entity_type"]
        description = entity_type.get("description") or ""
        assert "ontology_mapping" in description
        assert "ontology" in description.replace("ontology_mapping", "")


# --------------------------------------------------------------------------- #
# Create and delete
# --------------------------------------------------------------------------- #
async def test_create_ontology_and_mapping(tmp_path: Path) -> None:
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL,
                                           text=example("flights.semantic_model.yaml"))
    reply = await mcp_text(storage, "create_ontology", content=example("flights.ontology.yaml"))
    assert FLIGHTS in reply
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")
    reply = await mcp_text(storage, "create_ontology_mapping", data_source=FLIGHTS_DS,
                           content=example("flights.mapping.yaml"))
    assert FLIGHTS_MAPPING in reply
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")


async def test_a_refusal_reports_every_problem(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path, mapping=False)

    def two_unknown_fields(document: dict[str, Any]) -> None:
        children = concept_mapping(document, "Flight")["link_mappings"][2]["children"]
        children[0]["object_mapping"]["expression"] = "FLIGHT.arrival_lateness"
        children[1]["object_mapping"]["expression"] = "FLIGHT.departure_lateness"

    reply = await mcp_text(storage, "create_ontology_mapping", data_source=FLIGHTS_DS,
                           content=edited(example("flights.mapping.yaml"), two_unknown_fields))
    assert "arrival_lateness" in reply and "departure_lateness" in reply
    assert await storage.list_ontology_mappings() == []


async def test_delete_ontology_refused_while_mapped(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    reply = await mcp_text(storage, "delete_ontology", name=FLIGHTS)
    assert FLIGHTS_MAPPING in reply
    assert await storage.list_ontologies() == [FLIGHTS]
    await mcp_text(storage, "delete_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS)
    assert await storage.list_ontology_mappings() == []
    await mcp_text(storage, "delete_ontology", name=FLIGHTS)
    assert await storage.list_ontologies() == []


# --------------------------------------------------------------------------- #
# Structured edits
# --------------------------------------------------------------------------- #
async def test_an_agent_edits_one_concept(tmp_path: Path) -> None:
    """Spec scenario: An agent edits one concept."""
    storage = await flights_store("yaml", tmp_path)
    before = example("flights.ontology.yaml")
    await mcp_text(storage, "edit_ontology", name=FLIGHTS, concepts=[carrier_with_country()])
    after = await storage.get_ontology_text(FLIGHTS)
    assert after is not None
    head, tail = before.split("\n- concept: Carrier\n")[0], before.split("\n- concept: RouteId\n")[1]
    assert after.startswith(head)
    assert after.endswith(tail)
    expected = load(before)
    expected["ontology"] = [carrier_with_country() if c["concept"] == "Carrier" else c for c in expected["ontology"]]
    assert load(after) == expected
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


async def test_edit_ontology_adds_and_removes_concepts_and_relationships(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    gate = {"concept": "Gate", "type": "EntityType", "description": "A boarding gate."}
    await mcp_text(storage, "edit_ontology", name=FLIGHTS, description="Flights, edited.", concepts=[gate],
                   relationships=[{"concept": "Carrier", "relationship": rel("country", "String")}])
    edited_doc = load(await storage.get_ontology_text(FLIGHTS) or "")
    assert edited_doc["description"] == "Flights, edited."
    assert component(edited_doc, "Gate") == gate
    assert "country" in [r["name"] for r in component(edited_doc, "Carrier")["relationships"]]

    await mcp_text(storage, "edit_ontology", name=FLIGHTS, remove_concepts=["Gate"],
                   remove_relationships=["Carrier.country"])
    restored = load(await storage.get_ontology_text(FLIGHTS) or "")
    assert [c["concept"] for c in restored["ontology"]] == [c["concept"] for c in load(example(
        "flights.ontology.yaml"))["ontology"]]
    assert "country" not in [r["name"] for r in component(restored, "Carrier")["relationships"]]


async def test_edit_ontology_document_level_fields(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await mcp_text(storage, "edit_ontology", name=FLIGHTS, ai_context="Use for flight delay analysis.",
                   prefixes={"ex": "http://example.org/"}, requires=["COUNT[Flight] > 0"])
    document = load(await storage.get_ontology_text(FLIGHTS) or "")
    assert document["ai_context"] == "Use for flight delay analysis."
    assert document["prefixes"] == {"ex": "http://example.org/"}
    assert document["requires"] == ["COUNT[Flight] > 0"]
    assert document["ontology"] == load(example("flights.ontology.yaml"))["ontology"]


async def test_edit_ontology_mapping_references(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                   semantic_model_ref={"name": FLIGHTS_DS})
    document = load(await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) or "")
    assert document["semantic_model_ref"] == {"name": FLIGHTS_DS}
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []

    reply = await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                           ontology_ref={"name": "Nope"})
    assert "Nope" in reply
    after = load(await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) or "")
    assert after["ontology_ref"]["name"] == FLIGHTS


async def test_an_edit_breaking_the_mapping_is_refused(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    reply = await mcp_text(storage, "edit_ontology", name=FLIGHTS, remove_relationships=["Flight.departure_delay"])
    assert FLIGHTS_MAPPING in reply
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")


async def test_edit_ontology_mapping(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    market = {"concept": "Market", "object_mappings": [{"referent_mappings": [
        {"relationship": "name", "expression": "AIRPORT.market_nm WHERE ( AIRPORT.market_nm IS NOT NULL )"}]}]}
    await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                   description="Edited.", concept_mappings=[market], remove_concept_mappings=["State"])
    document = load(await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) or "")
    assert document["description"] == "Edited."
    assert concept_mapping(document, "Market") == market
    assert "State" not in [m["concept"] for m in document["concept_mappings"]]
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


async def test_edit_ontology_mapping_refuses_a_broken_entry(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    reply = await mcp_text(storage, "edit_ontology_mapping", name=FLIGHTS_MAPPING, data_source=FLIGHTS_DS,
                           concept_mappings=[{"concept": "Market", "object_mappings": [
                               {"referent_mappings": [{"relationship": "name", "expression": "AIRPORT.nope"}]}]}])
    assert "nope" in reply
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")


# --------------------------------------------------------------------------- #
# inspect
# --------------------------------------------------------------------------- #
async def test_inspect_an_ontology(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    compact = await mcp_text(storage, "inspect", entity_type="ontology", reference=FLIGHTS)
    assert "Ontology of flights into and out of airports." in compact
    assert all(name in compact for name in ("Carrier", "Delay", "Runway"))
    full = await mcp_text(storage, "inspect", entity_type="ontology", reference=FLIGHTS, compact=False)
    assert example("flights.ontology.yaml") in full


async def test_inspect_a_mapping(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    reference = f"{FLIGHTS_DS}.{FLIGHTS_MAPPING}"
    compact = await mcp_text(storage, "inspect", entity_type="ontology_mapping", reference=reference)
    assert "Maps the Flights ontology onto the Flights semantic model" in compact
    assert all(name in compact for name in ("Runway", "Flight", "Route"))
    assert "resolves" in compact.lower()
    full = await mcp_text(storage, "inspect", entity_type="ontology_mapping", reference=reference, compact=False)
    assert example("flights.mapping.yaml") in full


async def test_inspect_an_unresolved_mapping_lists_its_problems(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    model_file = tmp_path / "store" / "semantic_models" / FLIGHTS_DS / f"{FLIGHTS_MODEL}.yaml"
    model_file.write_text(example("flights.semantic_model.yaml").replace(
        "  - name: dep_delay\n    expression:\n      sql: dep_delay\n", ""), encoding="utf-8")
    compact = await mcp_text(storage, "inspect", entity_type="ontology_mapping",
                             reference=f"{FLIGHTS_DS}.{FLIGHTS_MAPPING}")
    assert "dep_delay" in compact
    assert "unresolved" in compact.lower()
