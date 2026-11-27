"""An edit never writes a string the document reader takes for another type (YAML 1.1: ``no``, ``on``, ``1.0``, ``~``)."""

from __future__ import annotations

import pytest

from slayer.core.document_text import edit_text, parse_document_text

AMBIGUOUS = ["no", "yes", "on", "off", "null", "~", "1.0", "12", "2024-01-02", "true"]


@pytest.mark.parametrize("value", AMBIGUOUS)
def test_a_new_string_value_reads_back_as_that_string(value: str) -> None:
    edited = edit_text("name: x\n", lambda doc: {**doc, "key": value})
    assert parse_document_text(edited)["key"] == value


def test_strings_inside_new_lists_and_mappings_read_back() -> None:
    desired = {"to_columns": ["co", "no"], "nested": {"flag": "off", "items": [{"name": "yes"}]}}
    edited = edit_text("name: x\nother: 1\n", lambda doc: {**doc, **desired})
    data = parse_document_text(edited)
    assert data["to_columns"] == ["co", "no"]
    assert data["nested"] == {"flag": "off", "items": [{"name": "yes"}]}


def test_a_replaced_list_item_reads_back() -> None:
    edited = edit_text("cols:\n- co\n- n\n", lambda doc: {"cols": ["co", "no"]})
    assert parse_document_text(edited)["cols"] == ["co", "no"]


def test_plain_strings_stay_unquoted() -> None:
    edited = edit_text("name: x\n", lambda doc: {**doc, "source": "orders"})
    assert "source: orders\n" in edited
