"""Ossie export/import edges: description inversion, nothing to export, duplicate payloads."""

import copy
import re
from pathlib import Path

import pytest

from slayer.osi.exporter import OsiExportError
from slayer.core.ossie_content import explicit_description
from slayer.core.ossie_content import render_description
from slayer.core.ossie_content import OSIAIContextObject
from slayer.storage.yaml_storage import YAMLStorage
from tests._dataset_access_fixtures import mcp_text
from tests._osi_export_fixtures import BASIC_METRICS, customers, orders
from tests._osi_export_helpers import dataset, export, export_datasets, make_env, reimport, slayer_entries, stored
from tests._osi_helpers import ds, f, flat


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
    await export_datasets(env, customers())
    with pytest.raises(OsiExportError, match="nope"):
        await export(env, datasets=["nope"])


async def test_duplicate_slayer_payloads_are_reported_and_ignored(env) -> None:
    result = await export_datasets(env, customers(), orders(metrics=BASIC_METRICS, hidden=True))
    edited = copy.deepcopy(result.document)
    orders_ds = dataset(edited, "orders")
    orders_ds["nonportable_extensions"] = slayer_entries(orders_ds) * 2
    imported, _ = await reimport(env, edited)
    assert not (await stored(imported, "orders")).hidden
    text = await mcp_text(YAMLStorage(base_dir=str(imported.store)), "inspect",
                          reference=f"{imported.datasource}.orders", entity_type="dataset", compact=False)
    assert "slayer" in text.lower()
    assert re.search(r"several|duplicate|more than one|\b2\b", text, re.IGNORECASE), text


async def test_single_field_unique_key_marks_the_column_unique(env) -> None:
    doc = flat([ds("customers", fields=[f("id", "c_custkey"), f("email"), f("segment")], primary_key=["id"],
                   unique_keys=[["email"], ["segment", "email"]])])
    imported, _ = await reimport(env, doc)
    customers_ds = await stored(imported, "customers")
    assert {c.name: c.unique for c in customers_ds.fields} == {"id": False, "email": True, "segment": False}
