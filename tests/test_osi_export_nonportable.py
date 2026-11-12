"""Ossie export: nonportable_extensions with object data, a 1.8-stored model, YAML quoting, round trips
(spec: importers/osi-export)."""

import io
import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from ruamel.yaml import YAML

from slayer.core.enums import DataType
from slayer.core.models import SlayerModel
from slayer.osi import slayer_extension as ext
from tests._cli_inprocess import run_cli_in_process
from tests._osi_export_fixtures import BASIC_MEASURES, col, customers, orders, orders_columns, rich_orders
from tests._osi_export_helpers import (
    SLAYER, dataset, entities, export, export_models, field, make_env, mentions, payload,
    reimport, save, slayer_entries, validate,
)
from tests._osi_helpers import FIXTURES, ds, f, flat

LEGACY_FIXTURE = FIXTURES / "legacy_custom_extensions.yaml"
ACME = {"vendor_name": "ACME", "data": {"k": 1}}


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _keys(node: Any) -> set[str]:
    """Every mapping key anywhere in ``node``."""
    if isinstance(node, dict):
        return set(node) | {k for v in node.values() for k in _keys(v)}
    if isinstance(node, list):
        return {k for v in node for k in _keys(v)}
    return set()


def _foreign(entity: dict[str, Any]) -> list[dict[str, Any]]:
    return [e for e in entity.get("nonportable_extensions") or [] if e["vendor_name"] != SLAYER]


# ────────────────────────────── export shape ───────────────────────────────

async def test_slayer_payload_is_an_object_under_nonportable_extensions(env) -> None:
    result = await export_models(env, customers(), rich_orders())
    for entity in entities(result.document):
        [entry] = slayer_entries(entity)
        assert isinstance(entry["data"], dict), entity["name"]
        assert entry["data"]["version"] == ext.PAYLOAD_VERSION
    assert "custom_extensions" not in _keys(result.document)
    validate(result.document)


async def test_reexport_of_a_legacy_import_writes_objects(env) -> None:
    imported, _ = await reimport(env, yaml.safe_load(LEGACY_FIXTURE.read_text(encoding="utf-8")))
    document = (await export(imported)).document
    assert "custom_extensions" not in _keys(document)
    validate(document)
    assert _foreign(document) == [ACME]
    carriers = [e for e in entities(document) if _foreign(e)]
    assert {e["name"] for e in carriers} == {"orders", "amount", "orders_customer_discount", "buyer"}
    assert all(_foreign(e) == [ACME] for e in carriers)


# ───────────────────────── document-level entries ──────────────────────────

@pytest.mark.parametrize("extensions", [True, False])
async def test_document_level_foreign_extension(env, extensions: bool) -> None:
    dbt = {"vendor_name": "DBT", "data": {"project_name": "shop"}}
    doc = flat([ds("orders", fields=[f("id", "o_orderkey"), f("amount")], primary_key=["id"])])
    doc["nonportable_extensions"] = [dbt]
    imported, _ = await reimport(env, doc)
    document = (await export(imported, extensions=extensions)).document
    assert document["nonportable_extensions"] == [dbt]
    validate(document)


def _with_semantic(model: SlayerModel, extensions: list[Any]) -> SlayerModel:
    semantic = {"name": "shop", "description": None, "ai_context": None, "nonportable_extensions": extensions}
    return model.model_copy(update={"meta": {**(model.meta or {}), "osi_semantic_model": semantic}})


@pytest.mark.parametrize("extensions", [True, False])
@pytest.mark.parametrize("malformed", [
    pytest.param({"vendor_name": "DBT"}, id="no-data"),
    pytest.param({"vendor_name": "DBT", "data": "not json"}, id="string-data"),
    pytest.param({"vendor_name": "DBT", "data": [1, 2]}, id="list-data"),
    pytest.param({"vendor_name": "", "data": {"k": 1}}, id="blank-vendor"),
])
async def test_malformed_document_level_entry_is_reported_and_omitted(env, extensions: bool,
                                                                      malformed: dict[str, Any]) -> None:
    def models(entries: list[Any]) -> list[SlayerModel]:
        return [_with_semantic(customers(), entries), _with_semantic(orders(measures=BASIC_MEASURES), entries)]
    clean = await export_models(env, *models([ACME]), extensions=extensions)
    result = await export_models(env, *models([malformed, ACME]), extensions=extensions)
    assert result.document == clean.document
    assert len(result.entries) == len(clean.entries) + 1
    validate(result.document)


# ──────────────────────── model stored by SLayer 1.8 ───────────────────────

async def test_model_stored_before_the_shape_change(env) -> None:
    await save(env, customers(), orders(measures=BASIC_MEASURES))
    path = env.store / "models" / env.datasource / "orders.yaml"
    stored = yaml.safe_load(path.read_text(encoding="utf-8"))
    stored["version"] = 21
    [amount] = [c for c in stored["columns"] if c["name"] == "amount"]
    amount["meta"] = {"osi_custom_extensions": [{"vendor_name": "ACME", "data": '{"k": 1}'},
                                                {"vendor_name": "ACME", "data": "not json"}]}
    path.write_text(yaml.safe_dump(stored, sort_keys=False), encoding="utf-8")
    result = await export(env)
    exported = field(dataset(result.document, "orders"), "amount")
    assert exported is not None
    assert _foreign(exported) == [ACME]
    assert [e for e in mentions(result, "ACME") if "amount" in e.message]
    validate(result.document)


# ─────────────────────────────── YAML quoting ──────────────────────────────

_RETYPABLE = ["1e3", "1E3", "+1e3", "-1.5E+3", "0o17", "0x1F", ".Inf", "NULL", "yes", "010", "2024-01-01",
              "-.inf", "+.INF", ".NaN", ".nan", "1.", ".5", "1e-3", "-0x1f", "True", "~", "null", "Off",
              "2024-01-01T00:00:00Z", "12:30:00", "-0o77", "+0o17", "+0xFF", "0o7_7", "1_000e3"]


def _yaml12(text: str) -> Any:
    return YAML(typ="safe", pure=True).load(io.StringIO(text))


def test_ruamel_loader_reads_yaml_12_core() -> None:
    assert _yaml12("[1e3, 0o17, yes]") == [1000.0, 15, "yes"]


async def test_retypable_strings_round_trip(env, tmp_path: Path) -> None:
    meta = {f"v{i}": s for i, s in enumerate(_RETYPABLE)}
    columns = [col("amount", meta=meta) if c.name == "amount" else c for c in orders_columns()]
    await save(env, customers(), orders(columns=columns, measures=BASIC_MEASURES))
    out = tmp_path / "shop.osi.yaml"
    result = run_cli_in_process(["export-osi", "--datasource", env.datasource, "--out", str(out),
                                 "--storage", str(env.store)])
    assert result.returncode == 0, result.stderr
    text = out.read_text(encoding="utf-8")
    for loaded in (yaml.safe_load(text), _yaml12(text)):
        amount = field(dataset(loaded, "orders"), "amount")
        assert amount is not None
        assert payload(amount)["column"]["meta"] == meta
    assert yaml.safe_load(text) == _yaml12(text)


async def test_retypable_strings_quoted_outside_payloads(env, tmp_path: Path) -> None:
    columns = [*orders_columns(), col("x1e3", DataType.TEXT, "note", label="1e3", description="0o17")]
    await save(env, customers(), orders(columns=columns, measures=BASIC_MEASURES))
    out = tmp_path / "shop.osi.yaml"
    assert run_cli_in_process(["export-osi", "--datasource", env.datasource, "--out", str(out),
                               "--storage", str(env.store), "--no-extensions"]).returncode == 0
    text = out.read_text(encoding="utf-8")
    assert yaml.safe_load(text) == _yaml12(text)
    x = field(dataset(_yaml12(text), "orders"), "x1e3")
    assert x is not None
    assert (x["label"], x["description"]) == ("1e3", "0o17")


# ──────────────────────────────── round trips ──────────────────────────────

async def test_export_import_export_keeps_every_payload(env) -> None:
    first = (await export_models(env, customers(), rich_orders())).document
    imported, _ = await reimport(env, first)
    second = (await export(imported)).document
    assert [(e["name"], payload(e)) for e in entities(second)] == [(e["name"], payload(e)) for e in entities(first)]
    assert yaml.safe_dump(first, sort_keys=False) == yaml.safe_dump(second, sort_keys=False)


def _legacy_payload(entity: dict[str, Any]) -> dict[str, Any]:
    [entry] = [e for e in entity.get("custom_extensions") or [] if e["vendor_name"] == SLAYER]
    return json.loads(entry["data"])


async def test_legacy_payloads_reexport_unchanged(env) -> None:
    legacy = yaml.safe_load(LEGACY_FIXTURE.read_text(encoding="utf-8"))
    imported, _ = await reimport(env, legacy)
    document = (await export(imported)).document
    assert [(e["name"], payload(e)) for e in entities(document)] == [
        (e["name"], _legacy_payload(e)) for e in entities(legacy)]
