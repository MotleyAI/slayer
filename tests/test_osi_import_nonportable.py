"""Ossie import: nonportable_extensions with object data, the legacy custom_extensions key, invalid data
(spec: importers/osi-import)."""

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from slayer.core.enums import JoinType
from slayer.core.models import SlayerModel
from slayer.ingest_report import ConversionResult
from slayer.osi.models import OSIDataset
from tests._osi_export_helpers import convert_document, make_env
from tests._osi_helpers import FIXTURES, by_name, ds, f, flat, m, rel

LEGACY_FIXTURE = FIXTURES / "legacy_custom_extensions.yaml"
LEVELS = ["document", "dataset", "field", "metric", "relationship"]
ACME = {"vendor_name": "ACME", "data": {"k": 1}}
LEGACY_ACME = {"vendor_name": "ACME", "data": '{"k": 1}'}


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _shop() -> dict[str, Any]:
    orders = ds("orders", fields=[f("id", "o_orderkey"), f("customer_id", "o_custkey"), f("amount")],
                primary_key=["id"], metrics=[m("revenue", "SUM(amount)")])
    customers = ds("customers", fields=[f("id", "c_custkey"), f("segment")], primary_key=["id"])
    o2c = rel("o2c", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])
    return flat([orders, customers], relationships=[o2c])


def _entity(doc: dict[str, Any], level: str) -> dict[str, Any]:
    orders = doc["datasets"][0]
    return {
        "document": doc, "dataset": orders, "field": orders["fields"][2], "metric": orders["metrics"][0],
        "relationship": doc["relationships"][0],
    }[level]


def _stored(models: dict[str, SlayerModel], level: str) -> list[Any]:
    """The foreign entries recorded for ``level``'s entity of ``_shop``."""
    orders = models["orders"]
    if level == "document":
        return (orders.meta or {})["osi_semantic_model"]["nonportable_extensions"]
    owner: Any = {
        "dataset": orders, "field": orders.get_column("amount"), "metric": orders.get_measure("revenue"),
        "relationship": orders.joins[0] if orders.joins else None,
    }[level]
    assert owner is not None, level
    return (owner.meta or {}).get("osi_nonportable_extensions", [])


def _legacy_notes(result: ConversionResult) -> list:
    """Informational entries naming the document ``shop`` with a workaround."""
    return [e for e in result.entries if e.severity == "info" and "shop" in e.message and e.suggestion]


def _reports(result: ConversionResult, *, entity: str, vendor: str) -> list:
    return [e for e in result.entries
            if (entity in (e.model_name, e.metric_name) or entity in e.message) and vendor in e.message]


# ─────────────────────────── current documents ─────────────────────────────

async def test_document_level_extensions_kept(env) -> None:
    doc = _shop()
    doc["nonportable_extensions"] = [{"vendor_name": "DBT", "data": {"project_name": "tpcds_analytics"}}]
    models = by_name(convert_document(env, doc))
    for model in models.values():
        assert (model.meta or {})["osi_semantic_model"]["nonportable_extensions"] == [
            {"vendor_name": "DBT", "data": {"project_name": "tpcds_analytics"}}]


@pytest.mark.parametrize("level", LEVELS)
async def test_foreign_entry_stored_as_object_at_every_level(env, level: str) -> None:
    doc = _shop()
    _entity(doc, level)["nonportable_extensions"] = [copy.deepcopy(ACME)]
    result = convert_document(env, doc)
    assert _stored(by_name(result), level) == [ACME]
    assert not _legacy_notes(result)


# ───────────────────────────── legacy key ──────────────────────────────────

@pytest.mark.parametrize("level", LEVELS)
async def test_legacy_key_read_at_every_level(env, level: str) -> None:
    doc = _shop()
    _entity(doc, level)["custom_extensions"] = [dict(LEGACY_ACME)]
    result = convert_document(env, doc)
    assert _stored(by_name(result), level) == [ACME]
    assert len(_legacy_notes(result)) == 1


@pytest.mark.parametrize("empty", [[], None])
@pytest.mark.parametrize("level", LEVELS)
async def test_empty_legacy_key_still_reports(env, level: str, empty: Any) -> None:
    doc = _shop()
    _entity(doc, level)["custom_extensions"] = empty
    assert len(_legacy_notes(convert_document(env, doc))) == 1


async def test_legacy_key_on_every_level_reports_once(env) -> None:
    doc = _shop()
    for level in LEVELS:
        _entity(doc, level)["custom_extensions"] = [dict(LEGACY_ACME)]
    result = convert_document(env, doc)
    models = by_name(result)
    for level in LEVELS:
        assert _stored(models, level) == [ACME], level
    assert len(_legacy_notes(result)) == 1


async def test_legacy_key_on_any_version(env) -> None:
    doc = {**_shop(), "version": "0.2.0.dev0"}
    doc["datasets"][0]["custom_extensions"] = [dict(LEGACY_ACME)]
    result = convert_document(env, doc)
    assert _stored(by_name(result), "dataset") == [ACME]
    assert len(_legacy_notes(result)) == 1


async def test_both_keys_on_one_entity(env) -> None:
    doc = _shop()
    amount = _entity(doc, "field")
    amount["nonportable_extensions"] = [{"vendor_name": "ACME", "data": {"a": 1}}]
    amount["custom_extensions"] = [{"vendor_name": "OTHER", "data": '{"b": 2}'}]
    stored = _stored(by_name(convert_document(env, doc)), "field")
    assert sorted(stored, key=lambda e: e["vendor_name"]) == [
        {"vendor_name": "ACME", "data": {"a": 1}}, {"vendor_name": "OTHER", "data": {"b": 2}}]


async def test_legacy_marker_in_the_input_is_ignored(env) -> None:
    doc = _shop()
    _entity(doc, "field")["nonportable_extensions"] = [{**LEGACY_ACME, "legacy": True}]
    result = convert_document(env, doc)
    assert _stored(by_name(result), "field") == []
    assert _reports(result, entity="amount", vendor="ACME")
    assert not _legacy_notes(result)


@pytest.mark.parametrize("current", [{}, "", 0])
def test_malformed_current_key_refused_beside_legacy_key(current: Any) -> None:
    with pytest.raises(ValidationError):
        OSIDataset.model_validate({"name": "d", "source": "t", "nonportable_extensions": current,
                                   "custom_extensions": []})


@pytest.mark.parametrize("data", ["[1, 2]", "not json"])
async def test_legacy_string_that_is_not_an_object(env, data: str) -> None:
    doc = _shop()
    _entity(doc, "field")["custom_extensions"] = [{"vendor_name": "ACME", "data": data}]
    result = convert_document(env, doc)
    models = by_name(result)
    assert _stored(models, "field") == []
    assert _reports(result, entity="amount", vendor="ACME")
    assert set(models) == {"orders", "customers"}
    assert models["orders"].get_measure("revenue") is not None


# ────────────────────────────── invalid data ───────────────────────────────

_BAD_DATA = [
    pytest.param("{released: 2024-01-01}", id="unquoted-date"),
    pytest.param("{log: {at: 2024-01-01T00:00:00Z}}", id="nested-timestamp"),
    pytest.param("{stats: {ratio: .inf}}", id="nested-inf"),
    pytest.param("{stats: {ratio: -.inf}}", id="nested-negative-inf"),
    pytest.param("{stats: {ratio: .nan}}", id="nested-nan"),
    pytest.param("{codes: {1: one}}", id="nested-non-string-key"),
    pytest.param("[1, 2]", id="list"),
    pytest.param("text", id="scalar"),
    pytest.param("&a {self: *a}", id="recursive-alias"),
]


def _with_bad_entry(level: str, data_yaml: str) -> dict[str, Any]:
    """``_shop`` whose ``level`` entity carries a valid ``ACME`` entry and a ``BAD`` entry read from YAML."""
    doc = _shop()
    _entity(doc, level)["nonportable_extensions"] = [
        {"vendor_name": "BAD", "data": yaml.safe_load(data_yaml)}, copy.deepcopy(ACME)]
    return doc


@pytest.mark.parametrize("data_yaml", _BAD_DATA)
async def test_invalid_data_drops_only_its_entry(env, data_yaml: str) -> None:
    result = convert_document(env, _with_bad_entry("dataset", data_yaml))
    models = by_name(result)
    assert _stored(models, "dataset") == [ACME]
    assert _reports(result, entity="orders", vendor="BAD")
    assert {"id", "customer_id", "amount"} <= {c.name for c in models["orders"].columns}


_DEEP = 20000


def _deep() -> dict[str, Any]:
    root: dict[str, Any] = {}
    node = root
    for _ in range(_DEEP):
        node["a"] = {}
        node = node["a"]
    return root


@pytest.mark.parametrize("key", ["nonportable_extensions", "custom_extensions"])
async def test_too_deep_data_drops_only_its_entry(env, key: str) -> None:
    data: Any = _deep() if key == "nonportable_extensions" else '{"a":' * _DEEP + "1" + "}" * _DEEP
    doc = _shop()
    _entity(doc, "field")[key] = [{"vendor_name": "DEEP", "data": data}]
    result = convert_document(env, doc)
    assert _stored(by_name(result), "field") == []
    assert _reports(result, entity="amount", vendor="DEEP")


async def test_shared_alias_data_kept(env) -> None:
    doc = _shop()
    _entity(doc, "dataset")["nonportable_extensions"] = [
        {"vendor_name": "ACME", "data": yaml.safe_load("{x: &a {k: [1]}, y: *a}")}]
    assert _stored(by_name(convert_document(env, doc)), "dataset") == [
        {"vendor_name": "ACME", "data": {"x": {"k": [1]}, "y": {"k": [1]}}}]


@pytest.mark.parametrize("level", LEVELS)
async def test_invalid_data_dropped_at_every_level(env, level: str) -> None:
    result = convert_document(env, _with_bad_entry(level, "[1, 2]"))
    assert _stored(by_name(result), level) == [ACME]
    assert [e for e in result.entries if "BAD" in e.message]


# ──────────────────────────── SLayer 1.8 export ────────────────────────────

def _current_twin(node: Any) -> Any:
    """``node`` with every legacy ``custom_extensions`` list rewritten as ``nonportable_extensions`` objects."""
    if isinstance(node, list):
        return [_current_twin(x) for x in node]
    if not isinstance(node, dict):
        return node
    out = {k: _current_twin(v) for k, v in node.items() if k != "custom_extensions"}
    if "custom_extensions" in node:
        out["nonportable_extensions"] = [
            {"vendor_name": e["vendor_name"], "data": json.loads(e["data"])} for e in node["custom_extensions"]]
    return out


def _legacy_document() -> dict[str, Any]:
    return yaml.safe_load(LEGACY_FIXTURE.read_text(encoding="utf-8"))


def test_legacy_fixture_is_a_legacy_export() -> None:
    text = LEGACY_FIXTURE.read_text(encoding="utf-8")
    assert "nonportable_extensions" not in text
    assert '"fingerprint"' in text


async def test_slayer_18_export_restores(env) -> None:
    legacy = convert_document(env, _legacy_document())
    current = convert_document(env, _current_twin(_legacy_document()))
    assert [x.model_dump() for x in legacy.models] == [x.model_dump() for x in current.models]
    assert not _legacy_notes(current)
    [note] = _legacy_notes(legacy)
    assert [e for e in legacy.entries if e is not note] == current.entries
    orders = by_name(legacy)["orders"]
    assert orders.hidden
    [join] = orders.joins
    assert (join.name, join.join_type) == ("buyer", JoinType.INNER)
    aov = orders.get_measure("aov")
    assert aov is not None
    assert aov.formula == "revenue / n"
    assert orders.get_measure("revenue_growth") is not None
