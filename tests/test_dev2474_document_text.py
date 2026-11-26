"""Round-trip editing reads back as the edit asked, and edits one list item without rewriting the others; the packaged
Ossie schemas are the vendored ones."""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.core.document_text import edit_list_items, edit_text, parse_document_text
from tests._dev2243_fixtures import CORPUS
from tests._dev2474_fixtures import component, example, load

PACKAGED = Path(__file__).parent.parent / "slayer" / "ontology" / "schemas"


def test_an_edit_whose_splice_misreads_falls_back_to_the_re_emitted_text() -> None:
    text = example("flights.yaml")
    edited = edit_text(text, lambda doc: {k: v for k, v in doc.items() if k != "ontology_mappings"})
    expected = {k: v for k, v in load(text).items() if k != "ontology_mappings"}
    assert parse_document_text(edited) == expected


def _carrier_with(document: dict, relationship: dict) -> list[dict]:
    carrier = component(document, "Carrier")
    carrier["relationships"].append(relationship)
    return document["ontology"]


def test_editing_one_item_keeps_every_other_item_as_written() -> None:
    text = example("flights.ontology.yaml")
    desired = _carrier_with(load(text), {"name": "country", "roles": [{"concept": "String"}],
                                         "verbalizes": ["{Carrier} is based in {String}"]})
    edited = edit_list_items(text, key="ontology", item_key="concept", desired=desired)
    assert load(edited)["ontology"] == desired
    assert edited.split("\n- concept: Carrier\n")[0] == text.split("\n- concept: Carrier\n")[0]
    assert edited.split("\n- concept: RouteId\n")[1] == text.split("\n- concept: RouteId\n")[1]


@pytest.mark.parametrize("change", ["add", "remove", "reorder"])
def test_adding_removing_and_reordering_items(change: str) -> None:
    text = example("flights.ontology.yaml")
    items = load(text)["ontology"]
    desired = {"add": [*items, {"concept": "Gate", "type": "EntityType"}], "remove": items[1:],
               "reorder": [items[1], items[0], *items[2:]]}[change]
    edited = edit_list_items(text, key="ontology", item_key="concept", desired=desired)
    assert load(edited) == {**load(text), "ontology": desired}
    assert "# Each Airport has at most one average departure Delay" in edited


@pytest.mark.parametrize("name", ["ontology.json", "mapping.json"])
def test_the_packaged_schemas_are_the_vendored_ones(name: str) -> None:
    assert (PACKAGED / name).read_bytes() == (CORPUS / "ontology" / name).read_bytes()


def test_the_packaged_core_schema_is_the_vendored_one() -> None:
    assert (PACKAGED / "ossie-schema.json").read_bytes() == (CORPUS / "core-spec" / "ossie-schema.json").read_bytes()
