"""Ossie export/import edges: description inversion, nothing to export, duplicate payloads."""

import copy
from pathlib import Path

import pytest

from slayer.osi.exporter import OsiExportError
from slayer.osi.models import OSIAIContextObject, explicit_description, render_description
from tests._osi_export_fixtures import BASIC_MEASURES, customers, orders
from tests._osi_export_helpers import convert_document, dataset, export, export_models, make_env, mentions, slayer_entries
from tests._osi_helpers import by_name, ds, f, flat


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


@pytest.mark.parametrize(("explicit", "context"), [
    ("Order amount", {"instructions": "Gross value.", "synonyms": ["revenue"]}),
    (None, {"instructions": "Gross value."}),
    ("Two\nlines", {"synonyms": ["a", "b"]}),
    ("Same", {"instructions": "Same"}),
])
def test_explicit_description_inverts_render(explicit: str | None, context: dict) -> None:
    ctx = OSIAIContextObject.model_validate(context)
    stored = render_description(explicit, ctx)
    assert render_description(explicit_description(stored, context), ctx) == stored


def test_explicit_description_without_context_is_unchanged() -> None:
    assert explicit_description("Kept", None) == "Kept"


async def test_nothing_to_export_raises(env) -> None:
    with pytest.raises(OsiExportError, match="Nothing to export"):
        await export(env)


async def test_unknown_model_raises(env) -> None:
    await export_models(env, customers())
    with pytest.raises(OsiExportError, match="nope"):
        await export(env, models=["nope"])


async def test_duplicate_slayer_payloads_are_reported_and_ignored(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES, hidden=True))
    edited = copy.deepcopy(result.document)
    orders_ds = dataset(edited, "orders")
    orders_ds["custom_extensions"] = slayer_entries(orders_ds) * 2
    imported = convert_document(env, edited)
    assert not by_name(imported)["orders"].hidden
    assert mentions(imported, "2 SLAYER extensions")


async def test_single_field_unique_key_marks_the_column_unique(env) -> None:
    doc = flat([ds("customers", fields=[f("id", "c_custkey"), f("email"), f("segment")], primary_key=["id"],
                   unique_keys=[["email"], ["segment", "email"]])])
    imported = by_name(convert_document(env, doc))["customers"]
    assert {c.name: c.unique for c in imported.columns} == {"id": False, "email": True, "segment": False}
