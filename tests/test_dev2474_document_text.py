"""Round-trip editing reads back as the edit asked, and edits one list item without rewriting the others; the packaged
Ossie schemas are the vendored ones."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from slayer.core.document_text import edit_list_items, edit_text, parse_document_text
from slayer.core.errors import UnreadableDocumentError
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


def _deep(depth: int) -> str:
    """A model whose ``ai_context`` nests ``depth`` mappings: past the round-trip loader's recursion at 300."""
    nest = [f"{'  ' * (i + 1)}a:" for i in range(depth)]
    return "\n".join(["# kept", "version: 0.3.0.dev0", "name: deep", "ai_context:", *nest,
                      f"{'  ' * (depth + 1)}leaf: x", "datasets:", "- name: orders", "  source: db.orders",
                      "  fields:", "  - name: id", "    expression:", "      sql: id"]) + "\n"


@pytest.fixture(autouse=True)
def _default_recursion_limit() -> Iterator[None]:
    """Python's default limit, which the depths below are chosen against; an imported library may raise it."""
    before = sys.getrecursionlimit()
    sys.setrecursionlimit(1000)
    yield
    sys.setrecursionlimit(before)


@pytest.mark.parametrize("depth", [300, 400])
def test_a_deeply_nested_document_saved_unchanged_keeps_its_text(depth: int) -> None:
    text = _deep(depth)
    assert edit_text(text, lambda doc: doc) == text


@pytest.mark.parametrize("depth", [300, 400])
def test_a_deeply_nested_document_reads_back_as_edited(depth: int) -> None:
    text = _deep(depth)
    desired = {**parse_document_text(text), "description": "edited"}
    assert parse_document_text(edit_text(text, lambda _doc: desired)) == desired


def test_a_deeply_nested_documents_list_items_read_back_as_edited() -> None:
    text = _deep(300)
    held = parse_document_text(text)
    desired = [*held["datasets"], {"name": "returns", "source": "db.returns"}]
    edited = edit_list_items(text, key="datasets", item_key="name", desired=desired)
    assert parse_document_text(edited) == {**held, "datasets": desired}


def test_an_edit_too_deep_for_yaml_that_json_cannot_hold_is_refused() -> None:
    text = _deep(400).replace("leaf: x", "leaf: 2026-11-26")
    desired = {**parse_document_text(text), "description": "edited"}
    with pytest.raises(UnreadableDocumentError, match="too deeply nested"):
        edit_text(text, lambda _doc: desired)


@pytest.mark.parametrize("depth", [10, 300])
def test_an_edit_too_deep_for_any_emitter_is_refused(depth: int) -> None:
    text = _deep(depth)
    nest: dict = {"leaf": "x"}
    for _ in range(sys.getrecursionlimit() * 2):
        nest = {"a": nest}
    desired = {**parse_document_text(text), "ai_context": nest}
    with pytest.raises(UnreadableDocumentError, match="too deeply nested"):
        edit_text(text, lambda _doc: desired)


@pytest.mark.parametrize("as_json", [False, True])
def test_an_edit_deeper_than_the_round_trip_holds_reads_back_as_edited(as_json: bool) -> None:
    text = _deep(10)
    held = parse_document_text(text)
    if as_json:
        text = json.dumps(held, indent=2) + "\n"
    desired = {**held, "ai_context": parse_document_text(_deep(400))["ai_context"]}
    assert parse_document_text(edit_text(text, lambda _doc: desired)) == desired


def test_a_json_edit_too_deep_for_json_is_refused() -> None:
    text = json.dumps(parse_document_text(_deep(10)), indent=2) + "\n"
    nest: dict = {"leaf": "x"}
    for _ in range(sys.getrecursionlimit() * 2):
        nest = {"a": nest}
    desired = {**parse_document_text(text), "ai_context": nest}
    with pytest.raises(UnreadableDocumentError, match="too deeply nested"):
        edit_text(text, lambda _doc: desired)


def test_a_document_too_deep_to_read_is_refused_as_unreadable() -> None:
    text = _deep(1000)
    with pytest.raises(UnreadableDocumentError, match="too deeply nested"):
        parse_document_text(text)


@pytest.mark.parametrize("name", ["ontology.json", "mapping.json"])
def test_the_packaged_schemas_are_the_vendored_ones(name: str) -> None:
    assert (PACKAGED / name).read_bytes() == (CORPUS / "ontology" / name).read_bytes()


def test_the_packaged_core_schema_is_the_vendored_one() -> None:
    assert (PACKAGED / "ossie-schema.json").read_bytes() == (CORPUS / "core-spec" / "ossie-schema.json").read_bytes()
