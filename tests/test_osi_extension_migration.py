"""SlayerModel 21→22: stored Ossie foreign extensions move to ``osi_nonportable_extensions`` with object data."""

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import SlayerModel
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.yaml_storage import YAMLStorage

OLD, NEW = "osi_custom_extensions", "osi_nonportable_extensions"
LOCATIONS = ["model", "column", "measure", "join", "semantic_model"]


def _doc(meta: dict[str, Any] | None = None, *, location: str = "model", version: int = 21) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "version": version, "name": "orders", "sql_table": "orders", "data_source": "osids",
        "columns": [{"name": "id", "sql": "o_orderkey", "type": "INT", "primary_key": True},
                    {"name": "customer_id", "sql": "o_custkey", "type": "INT"},
                    {"name": "amount", "type": "DOUBLE"}],
        "measures": [{"name": "revenue", "formula": "sum(amount)"}],
        "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
    }
    if meta is not None:
        _owner(doc, location)["meta"] = meta
    return doc


def _owner(doc: dict[str, Any], location: str) -> dict[str, Any]:
    return {"model": doc, "column": doc["columns"][2], "measure": doc["measures"][0], "join": doc["joins"][0],
            "semantic_model": doc}[location]


def _meta(model: SlayerModel, location: str) -> dict[str, Any]:
    owner: Any = {"model": model, "semantic_model": model, "column": model.get_column("amount"),
                  "measure": model.get_measure("revenue"), "join": model.joins[0]}[location]
    assert owner is not None
    return owner.meta or {}


def _entries_meta(location: str, entries: Any) -> dict[str, Any]:
    if location == "semantic_model":
        return {"osi_semantic_model": {"name": "shop", "custom_extensions": entries}}
    return {OLD: entries}


def _migrated(location: str, entries: Any) -> Any:
    model = SlayerModel.model_validate(_doc(_entries_meta(location, entries), location=location))
    meta = _meta(model, location)
    if location == "semantic_model":
        semantic = meta["osi_semantic_model"]
        assert "custom_extensions" not in semantic
        return semantic.get("nonportable_extensions")
    assert OLD not in meta
    return meta.get(NEW)


def test_version_is_22() -> None:
    assert CURRENT_VERSIONS["SlayerModel"] == 22


@pytest.mark.parametrize("location", LOCATIONS)
def test_every_location_converted(location: str) -> None:
    assert _migrated(location, [{"vendor_name": "ACME", "data": '{"k": 1, "nested": {"flag": true}}'}]) == [
        {"vendor_name": "ACME", "data": {"k": 1, "nested": {"flag": True}}}]


@pytest.mark.parametrize("location", LOCATIONS)
def test_object_data_passes_unchanged(location: str) -> None:
    entries = [{"vendor_name": "ACME", "data": {"k": 1}}]
    assert _migrated(location, copy.deepcopy(entries)) == entries


def test_order_preserved() -> None:
    entries = [{"vendor_name": v, "data": f'{{"i": {i}}}'} for i, v in enumerate(["C", "A", "B"])]
    assert _migrated("column", entries) == [{"vendor_name": v, "data": {"i": i}} for i, v in enumerate(["C", "A", "B"])]


def test_mixed_entries_handled_one_by_one() -> None:
    entries = [{"vendor_name": "A", "data": '{"k": 1}'}, {"vendor_name": "B", "data": "not json"},
               {"vendor_name": "C", "data": "[1, 2]"}, {"vendor_name": "D", "data": {"x": 1}},
               {"vendor_name": "E"}]
    assert _migrated("column", entries) == [
        {"vendor_name": "A", "data": {"k": 1}}, {"vendor_name": "B", "data": "not json"},
        {"vendor_name": "C", "data": "[1, 2]"}, {"vendor_name": "D", "data": {"x": 1}}, {"vendor_name": "E"}]


@pytest.mark.parametrize("value", ["garbage", {"vendor_name": "ACME"}, 3])
def test_non_list_value_untouched(value: Any) -> None:
    model = SlayerModel.model_validate(_doc({OLD: value, "owner": "x"}, location="column"))
    assert _meta(model, "column") == {OLD: value, "owner": "x"}


def test_other_meta_kept() -> None:
    model = SlayerModel.model_validate(_doc({OLD: [{"vendor_name": "A", "data": "{}"}], "osi_ai_context": {"x": 1},
                                             "owner": "finance"}, location="measure"))
    assert _meta(model, "measure") == {NEW: [{"vendor_name": "A", "data": {}}], "osi_ai_context": {"x": 1},
                                       "owner": "finance"}


def test_current_version_document_untouched() -> None:
    meta = {OLD: [{"vendor_name": "A", "data": '{"k": 1}'}]}
    model = SlayerModel.model_validate(_doc(copy.deepcopy(meta), location="model", version=22))
    assert model.meta == meta


async def test_stored_model_upgraded_on_load(tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path))
    path = tmp_path / "models" / "osids" / "orders.yaml"
    path.parent.mkdir(parents=True)
    doc = _doc({OLD: [{"vendor_name": "ACME", "data": '{"k": 1}'}]}, location="column")
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    model = await storage.get_model("orders", data_source="osids")
    assert model is not None
    assert model.version == 22
    assert _meta(model, "column") == {NEW: [{"vendor_name": "ACME", "data": {"k": 1}}]}
