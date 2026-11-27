"""Spec: models/stored-format › Each Ossie construct is stored as Ossie defines it.

Each construct runs through import → YAML / SQLite storage → load → unchanged save and an unrelated edit → export,
in a named document and in the datasource model (reached by re-importing a merged export whose other datasets belong
to no recorded document).
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml
from pydantic import BaseModel, ConfigDict

from slayer.core.models import DatasetMetric, DatasourceConfig
from slayer.osi.exporter import export_osi
from slayer.osi.importer import import_osi
from tests._engine_helpers import owned_query_engine
from tests._stored_format_fixtures import (
    DS, STORAGES, dataset_documents, link, make_storage, model_documents, recorded, reopen, slayer_entry, snapshot,
    stored_dataset, table_dataset, texts, upgrade_warning_names,
)

SALES = "sales"
SUPPORT = "support"
HOMES = ("document", "datasource")
SLAYER = "SLAYER"

SEED_SQL = (
    "CREATE TABLE orders (order_id VARCHAR, line INTEGER, code VARCHAR, region VARCHAR, period INTEGER, "
    "ordered_on DATE, shipped_on DATE, amount DECIMAL(10,2), at_time TIME, at_tz TIMESTAMPTZ, fx DOUBLE, "
    "status VARCHAR)",
    "INSERT INTO orders VALUES ('O1', 1, 'c1', 'n', 202401, DATE '2024-01-02', DATE '2024-01-03', 10.5, "
    "TIME '10:00:00', TIMESTAMPTZ '2024-01-02 10:00:00+00', 1.0, 'open')",
    "CREATE TABLE customers (code VARCHAR, segment VARCHAR)",
    "INSERT INTO customers VALUES ('c1', 'b2b')",
    "CREATE TABLE tickets (ticket_id VARCHAR)",
    "INSERT INTO tickets VALUES ('T1')",
)


def seed(db: Path) -> None:
    with contextlib.closing(duckdb.connect(str(db))) as con:
        for statement in SEED_SQL:
            con.execute(statement)


def _f(name: str, sql: str | None = None, *, datatype: str | None = "String", **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "expression": {"sql": sql or name}}
    if datatype is not None:
        out["datatype"] = datatype
    return {**out, **extra}


ORDERS: dict[str, Any] = {
    "name": "orders",
    "source": "seed.main.orders",
    "primary_key": ["order_id", "line"],
    "unique_keys": [["order_id", "line"], ["code", "region"]],
    "description": "Order lines.",
    "ai_context": {"instructions": "One row per order line.", "synonyms": ["sales orders"]},
    "nonportable_extensions": [{"vendor_name": "SNOWFLAKE", "data": {"cluster_by": ["order_id"]}}],
    "fields": [
        _f("order_id", label="Order", ai_context="The order id.",
           nonportable_extensions=[{"vendor_name": "SNOWFLAKE", "data": {"masking_policy": "pii"}}]),
        _f("line", datatype="Integer"),
        _f("code"),
        _f("region", ai_context={"synonyms": ["area"]}),
        _f("period", datatype="Integer", dimension={"is_time": True}),
        _f("ordered_on", datatype="Date", dimension={"is_time": False}),
        _f("shipped_on", datatype="Date"),
        {"name": "amount", "datatype": "Decimal", "expression": {"dialects": [
            {"dialect": "ANSI_SQL", "expression": "amount"},
            {"dialect": "BIGQUERY", "expression": "SAFE_CAST(amount AS NUMERIC)"}]}},
        _f("at_time", datatype="Time"),
        _f("at_tz", datatype="DateTimeTz"),
        {"name": "fx", "datatype": "Float", "expression": {"dialects": [
            {"dialect": "SNOWFLAKE", "expression": "TRY_TO_DOUBLE(fx)"}]}},
        _f("running_total", "SUM(amount) OVER (ORDER BY ordered_on)", datatype="Float"),
    ],
    "metrics": [{"name": "total", "expression": {"sql": "SUM(amount)"}, "ai_context": "Gross revenue.",
                 "nonportable_extensions": [{"vendor_name": "DBT", "data": {"meta": {"owner": "finance"}}}]}],
}
CUSTOMERS: dict[str, Any] = {
    "name": "customers", "source": "SELECT code, segment FROM customers", "primary_key": ["code"],
    "fields": [_f("code"), _f("segment")],
}
TICKETS: dict[str, Any] = {"name": "tickets", "source": "tickets", "fields": [_f("ticket_id")]}
RELATIONSHIP: dict[str, Any] = {
    "name": "orders_to_customers", "from": "orders", "to": "customers", "from_columns": ["code"],
    "to_columns": ["code"], "ai_context": "Each line's buyer.",
    "nonportable_extensions": [{"vendor_name": "DBT", "data": {"tests": ["relationships"]}}],
}
MODEL_METRIC: dict[str, Any] = {"name": "revenue_per_customer",
                                "expression": {"sql": "SUM(orders.amount) / COUNT(DISTINCT customers.code)"},
                                "ai_context": {"synonyms": ["ARPU"]}}

SALES_DOC: dict[str, Any] = {
    "version": "0.3.0.dev0", "name": SALES, "ai_context": "Sales model.",
    "nonportable_extensions": [{"vendor_name": "DBT", "data": {"project": "jaffle"}}],
    "datasets": [ORDERS, CUSTOMERS], "relationships": [RELATIONSHIP], "metrics": [MODEL_METRIC],
}
#: A merged export of datasource ``shop`` recording only ``support``: ``orders`` and ``customers`` belong to no recorded
#: document, so a re-import puts them in the datasource model.
MERGED_DOC: dict[str, Any] = {
    "version": "0.3.0.dev0", "name": DS,
    "datasets": [ORDERS, CUSTOMERS, TICKETS], "relationships": [RELATIONSHIP],
    "nonportable_extensions": [{"vendor_name": SLAYER, "data": {
        "version": 3, "kind": "semantic_model", "entity": DS, "documents": [{"name": SUPPORT, "datasets": ["tickets"]}],
    }}],
}


class Flow(BaseModel):
    """What one import → store → save → export run produced."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    imported: dict[str, Any]
    saved: dict[str, Any]
    exported: dict[str, Any]


def _view(kind: str, base: Path, *, model: str) -> dict[str, Any]:
    """Model ``model``'s stored content as one flat document: its datasets resolved from inline or dataset documents."""
    doc = model_documents(kind, base).get(model)
    assert doc is not None, f"model {model!r} is not stored; models: {sorted(model_documents(kind, base))}"
    names = [d["name"] for d in doc.get("datasets") or []]
    names += [d["dataset"]["name"] for f, d in dataset_documents(kind, base).items()
              if f in {r["iri"] for r in doc.get("dataset_refs") or []}]
    return {**{k: v for k, v in doc.items() if k not in ("datasets", "dataset_refs")},
            "datasets": [stored_dataset(kind, base, n) for n in names]}


async def _run_flow(*, home: str, kind: str, base: Path) -> Flow:
    db = base / "seed.duckdb"
    seed(db)
    storage = make_storage(kind, base)
    source = base / "import.yaml"
    source.write_text(yaml.safe_dump(SALES_DOC if home == "document" else MERGED_DOC, sort_keys=False),
                      encoding="utf-8")
    model = SALES if home == "document" else DS
    async with owned_query_engine(storage=storage, datasources=[
            DatasourceConfig(name=DS, type="duckdb", database=str(db))]) as engine:
        await import_osi(storage=storage, path=source, data_source=DS, engine=engine)
        imported = _view(kind, base, model=model)
        for name in ("orders", "customers"):
            loaded = await storage.get_dataset(name, data_source=DS)
            assert loaded is not None, name
            await storage.save_dataset(loaded)
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        await storage.save_dataset(orders.model_copy(update={"description": "Order lines, edited."}))
        saved = _view(kind, base, model=model)
        exported = (await export_osi(engine=engine, datasource=DS)).document
    return Flow(imported=imported, saved=saved, exported=exported)


_FLOWS: dict[tuple[str, str], Flow | BaseException] = {}


@pytest.fixture(scope="module")
def flow_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("construct_flows")


def flow(*, home: str, kind: str, root: Path) -> Flow:
    """The cached run for ``(home, kind)``; its failure re-raised in every test that reads it."""
    key = (home, kind)
    if key not in _FLOWS:
        base = root / f"{home}-{kind}"
        base.mkdir()
        try:
            _FLOWS[key] = asyncio.run(_run_flow(home=home, kind=kind, base=base))
        except Exception as exc:  # noqa: BLE001 — re-raised per test, so each fails rather than errors
            _FLOWS[key] = exc
    result = _FLOWS[key]
    if isinstance(result, BaseException):
        raise result
    return result


# --------------------------------------------------------------------------- #
# Construct accessors over a flat document view
# --------------------------------------------------------------------------- #
def _ds(doc: dict[str, Any], name: str) -> dict[str, Any]:
    hits = [d for d in doc["datasets"] if d["name"] == name]
    assert len(hits) == 1, f"dataset {name!r}: {[d['name'] for d in doc['datasets']]}"
    return hits[0]


def _field(doc: dict[str, Any], dataset: str, name: str) -> dict[str, Any]:
    hits = [f for f in _ds(doc, dataset).get("fields") or [] if f["name"] == name]
    assert len(hits) == 1, f"field {dataset}.{name}"
    return hits[0]


def _metric(doc: dict[str, Any], dataset: str, name: str) -> dict[str, Any]:
    hits = [m for m in _ds(doc, dataset).get("metrics") or [] if m["name"] == name]
    assert len(hits) == 1, f"metric {dataset}.{name}"
    return hits[0]


def _relationship(doc: dict[str, Any], name: str) -> dict[str, Any]:
    hits = [r for r in doc.get("relationships") or [] if r["name"] == name]
    assert len(hits) == 1, f"relationship {name!r}"
    return hits[0]


def _foreign(entity: dict[str, Any]) -> list[dict[str, Any]]:
    """``entity``'s non-portable extension entries of every vendor but SLayer's."""
    return [e for e in entity.get("nonportable_extensions") or [] if e.get("vendor_name") != SLAYER]


def _dimension(field: dict[str, Any]) -> Any:
    return field.get("dimension", "<absent>")


Getter = Callable[[dict[str, Any]], Any]

#: ``(id, accessor)``; the accessor reads the construct from a flat document holding ``orders`` and ``customers``.
DATASET_CONSTRUCTS: list[tuple[str, Getter]] = [
    ("source-table", lambda d: _ds(d, "orders")["source"]),
    ("source-query", lambda d: _ds(d, "customers")["source"]),
    ("composite-primary-key", lambda d: _ds(d, "orders")["primary_key"]),
    ("multi-field-unique-keys", lambda d: _ds(d, "orders").get("unique_keys")),
    ("dataset-extension", lambda d: _foreign(_ds(d, "orders"))),
    ("field-extension", lambda d: _foreign(_field(d, "orders", "order_id"))),
    ("metric-extension", lambda d: _foreign(_metric(d, "orders", "total"))),
    ("relationship-extension", lambda d: _foreign(_relationship(d, "orders_to_customers"))),
    ("dataset-ai-context", lambda d: _ds(d, "orders").get("ai_context")),
    ("field-ai-context-string", lambda d: _field(d, "orders", "order_id").get("ai_context")),
    ("field-ai-context-object", lambda d: _field(d, "orders", "region").get("ai_context")),
    ("metric-ai-context", lambda d: _metric(d, "orders", "total").get("ai_context")),
    ("relationship-ai-context", lambda d: _relationship(d, "orders_to_customers").get("ai_context")),
    ("is-time-true", lambda d: _dimension(_field(d, "orders", "period"))),
    ("is-time-false", lambda d: _dimension(_field(d, "orders", "ordered_on"))),
    ("is-time-unset", lambda d: _dimension(_field(d, "orders", "shipped_on"))),
    ("foreign-dialect-beside-a-runnable-one", lambda d: _field(d, "orders", "amount")["expression"]),
    ("foreign-dialect-only", lambda d: _field(d, "orders", "fx")["expression"]),
    ("decimal", lambda d: _field(d, "orders", "amount").get("datatype")),
    ("time", lambda d: _field(d, "orders", "at_time").get("datatype")),
    ("datetimetz", lambda d: _field(d, "orders", "at_tz").get("datatype")),
    ("field-label", lambda d: _field(d, "orders", "order_id").get("label")),
    ("unevaluable-field", lambda d: _field(d, "orders", "running_total")["expression"]),
]
_ORIGINAL = {"datasets": [ORDERS, CUSTOMERS], "relationships": [RELATIONSHIP]}


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("home", HOMES)
@pytest.mark.parametrize(("construct", "get"), DATASET_CONSTRUCTS, ids=[c for c, _ in DATASET_CONSTRUCTS])
def test_each_construct_is_stored_as_written(flow_root: Path, home: str, kind: str, construct: str,
                                             get: Getter) -> None:
    run = flow(home=home, kind=kind, root=flow_root)
    expected = get(_ORIGINAL)
    assert get(run.imported) == expected, construct
    assert get(run.saved) == expected, construct


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("home", HOMES)
@pytest.mark.parametrize(("construct", "get"), DATASET_CONSTRUCTS, ids=[c for c, _ in DATASET_CONSTRUCTS])
def test_each_construct_exports_as_imported(flow_root: Path, home: str, kind: str, construct: str,
                                            get: Getter) -> None:
    run = flow(home=home, kind=kind, root=flow_root)
    assert get(run.exported) == get(_ORIGINAL), construct


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("home", HOMES)
def test_constructs_survive_a_round_trip(flow_root: Path, home: str, kind: str) -> None:
    """Spec scenario: Constructs survive a round trip."""
    run = flow(home=home, kind=kind, root=flow_root)
    for construct, get in DATASET_CONSTRUCTS:
        assert get(run.exported) == get(_ORIGINAL), construct


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("key", "expected"), [
    ("ai_context", SALES_DOC["ai_context"]), ("nonportable_extensions", SALES_DOC["nonportable_extensions"])],
    ids=["ai-context", "extensions"])
def test_model_level_constructs_are_stored_as_written(flow_root: Path, kind: str, key: str, expected: Any) -> None:
    run = flow(home="document", kind=kind, root=flow_root)
    assert run.imported.get(key) == expected
    assert run.saved.get(key) == expected
    assert run.exported.get(key) == expected


@pytest.mark.parametrize("kind", STORAGES)
def test_a_model_scoped_metrics_constructs_are_stored_as_written(flow_root: Path, kind: str) -> None:
    run = flow(home="document", kind=kind, root=flow_root)
    for doc in (run.imported, run.saved, run.exported):
        [stored] = [m for m in doc.get("metrics") or [] if m["name"] == MODEL_METRIC["name"]]
        assert stored == MODEL_METRIC


@pytest.mark.parametrize("kind", STORAGES)
def test_datasource_model_datasets_are_dataset_documents_in_ossie_core(flow_root: Path, kind: str) -> None:
    run = flow(home="datasource", kind=kind, root=flow_root)
    orders = _ds(run.saved, "orders")
    assert orders["primary_key"] == ["order_id", "line"]
    assert orders["unique_keys"] == ORDERS["unique_keys"]
    assert orders["ai_context"] == ORDERS["ai_context"]
    assert slayer_entry(orders) is None or "unique_keys" not in str(slayer_entry(orders))
    assert all(not f.get("primary_key") for f in orders["fields"])


# --------------------------------------------------------------------------- #
# A metric's label lives in its SLAYER entry
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("home", HOMES)
async def test_a_metric_label_is_stored_in_its_slayer_entry(tmp_path: Path, home: str, kind: str) -> None:
    db = tmp_path / "seed.duckdb"
    seed(db)
    storage = make_storage(kind, tmp_path)
    source = tmp_path / "import.yaml"
    source.write_text(yaml.safe_dump(SALES_DOC if home == "document" else MERGED_DOC, sort_keys=False),
                      encoding="utf-8")
    async with owned_query_engine(storage=storage, datasources=[
            DatasourceConfig(name=DS, type="duckdb", database=str(db))]) as engine:
        await import_osi(storage=storage, path=source, data_source=DS, engine=engine)
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        metrics = [m.model_copy(update={"label": "Total revenue"}) if m.name == "total" else m for m in orders.metrics]
        await storage.save_dataset(orders.model_copy(update={"metrics": metrics}))
        [stored] = [m for m in stored_dataset(kind, tmp_path, "orders").get("metrics") or [] if m["name"] == "total"]
        assert "label" not in stored
        payload = slayer_entry(stored)
        assert payload is not None
        assert DatasetMetric.model_validate(payload["metric"]).label == "Total revenue"
        reloaded = await reopen(kind, tmp_path).get_dataset("orders", data_source=DS)
        assert reloaded is not None
        assert {m.name: m.label for m in reloaded.metrics}["total"] == "Total revenue"
        exported = (await export_osi(engine=engine, datasource=DS)).document
        [out] = [m for m in _ds(exported, "orders").get("metrics") or [] if m["name"] == "total"]
        assert "label" not in out
        out_payload = slayer_entry(out)
        assert out_payload is not None
        assert DatasetMetric.model_validate(out_payload["metric"]).label == "Total revenue"


# --------------------------------------------------------------------------- #
# A load's derived dialect entry is never stored
# --------------------------------------------------------------------------- #
SNOWFLAKE_ONLY = {"dialects": [{"dialect": "SNOWFLAKE", "expression": "TRY_TO_DOUBLE(fx)"}]}


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("home", HOMES)
async def test_a_loads_derived_dialect_entry_is_not_stored(tmp_path: Path, home: str, kind: str) -> None:
    """Spec scenario: A load's derived dialect entry is not stored."""
    db = tmp_path / "seed.duckdb"
    seed(db)
    storage = make_storage(kind, tmp_path)
    source = tmp_path / "import.yaml"
    source.write_text(yaml.safe_dump(SALES_DOC if home == "document" else MERGED_DOC, sort_keys=False),
                      encoding="utf-8")
    async with owned_query_engine(storage=storage, datasources=[
            DatasourceConfig(name=DS, type="duckdb", database=str(db))]):
        await import_osi(storage=storage, path=source, data_source=DS)
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        fields = [f.model_copy(update={"description": "FX rate."}) if f.name == "fx" else f for f in orders.fields]
        await storage.save_dataset(orders.model_copy(update={"fields": fields}))
    [stored] = [f for f in stored_dataset(kind, tmp_path, "orders")["fields"] if f["name"] == "fx"]
    assert stored["expression"] == SNOWFLAKE_ONLY
    assert stored.get("description") == "FX rate."
    payload = slayer_entry(stored) or {}
    content = payload.get("field") or {}
    assert content.get("expression") is None, payload
    assert content.get("sql") is None, payload


# --------------------------------------------------------------------------- #
# The SLAYER payload shape
# --------------------------------------------------------------------------- #
_COMMON = {"version", "kind", "entity", "fingerprint"}
_DATASET_KEYS = _COMMON | {"source", "hidden", "access_tags", "default_time_dimension", "meta", "aggregations",
                           "fields", "metrics", "relationships", "enforced_keys"}
_SOURCE_KEYS = {"sql_table", "source_kind", "sql", "filters", "source_queries", "query_variables"}


def _check_dataset_payload(payload: dict[str, Any] | None, *, entity: str) -> dict[str, Any]:
    assert payload is not None
    assert set(payload) <= _DATASET_KEYS, set(payload) - _DATASET_KEYS
    assert (payload["version"], payload["kind"], payload["entity"]) == (4, "dataset", entity)
    assert isinstance(payload["fingerprint"], str)
    assert payload["fingerprint"]
    assert set(payload.get("source") or {}) <= _SOURCE_KEYS
    return payload


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_datasource_model_datasets_slayer_entry_has_the_frozen_shape(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    await storage.save_dataset(table_dataset("orders", fields=["id", "status"], filters=["status <> 'test'"],
                                             hidden=True), _validate=False)
    stored = stored_dataset(kind, tmp_path, "orders")
    payload = _check_dataset_payload(slayer_entry(stored), entity="orders")
    assert payload["hidden"] is True
    assert payload["source"]["filters"] == ["status <> 'test'"]
    assert stored["source"] == "orders"


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_query_backed_datasets_core_source_is_its_name(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    await storage.save_dataset(table_dataset("orders", fields=["id", "status"]), _validate=False)
    backed = table_dataset("open_orders", fields=["id"]).model_copy(update={
        "sql_table": None, "source_queries": [{"population": "orders", "dimensions": ["orders.id"]}]})
    await storage.save_dataset(backed, _validate=False)
    stored = stored_dataset(kind, tmp_path, "open_orders")
    assert stored["source"] == "open_orders"
    payload = _check_dataset_payload(slayer_entry(stored), entity="open_orders")
    assert payload["source"]["source_queries"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_field_slayer_entry_has_the_frozen_shape(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    dataset = table_dataset("orders", fields=["id", "amount"])
    fields = [f.model_copy(update={"hidden": True}) if f.name == "amount" else f for f in dataset.fields]
    await storage.save_dataset(dataset.model_copy(update={"fields": fields}), _validate=False)
    [amount] = [f for f in stored_dataset(kind, tmp_path, "orders")["fields"] if f["name"] == "amount"]
    payload = slayer_entry(amount)
    assert payload is not None
    assert set(payload) <= _COMMON | {"field"}
    assert (payload["version"], payload["kind"], payload["entity"]) == (4, "field", "amount")
    assert payload["field"]["hidden"] is True


# --------------------------------------------------------------------------- #
# A document without SLAYER entries reads as current
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_datasource_model_without_slayer_entries_reads_as_current(tmp_path: Path, kind: str) -> None:
    db = tmp_path / "seed.duckdb"
    seed(db)
    storage = make_storage(kind, tmp_path)
    source = tmp_path / "import.yaml"
    plain = copy.deepcopy(MERGED_DOC)
    plain["datasets"] = [CUSTOMERS, TICKETS]
    plain["relationships"] = []
    source.write_text(yaml.safe_dump(plain, sort_keys=False), encoding="utf-8")
    async with owned_query_engine(storage=storage, datasources=[
            DatasourceConfig(name=DS, type="duckdb", database=str(db))]):
        await import_osi(storage=storage, path=source, data_source=DS)
    assert all(slayer_entry(d["dataset"]) is None for d in dataset_documents(kind, tmp_path).values())
    assert stored_dataset(kind, tmp_path, "customers")["source"] == CUSTOMERS["source"]
    before = texts(kind, tmp_path)
    files = snapshot(tmp_path)
    with recorded() as record:
        loaded, errs = await reopen(kind, tmp_path).load_datasets(data_source=DS)
    assert {d.name for d in loaded} >= {"customers", "tickets"}
    assert errs == []
    assert upgrade_warning_names(record) == []
    assert texts(kind, tmp_path) == before
    assert snapshot(tmp_path) == files


# --------------------------------------------------------------------------- #
# Core holds what the export renders
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_field_edited_into_slayer_only_content_leaves_core(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    await storage.save_dataset(table_dataset("customers", fields=["code", "segment"], primary_key=["code"]))
    orders = table_dataset("orders", fields=["id", "code", "seg"], primary_key=["id"]).model_copy(update={
        "relationships": [link("customers", from_columns=["code"], to_columns=["code"])]})
    await storage.save_dataset(orders)
    assert "seg" in [f["name"] for f in stored_dataset(kind, tmp_path, "orders")["fields"]]
    loaded = await storage.get_dataset("orders", data_source=DS)
    assert loaded is not None
    joined = [f.model_copy(update={"expression": "customers.segment"}) if f.name == "seg" else f for f in loaded.fields]
    await storage.save_dataset(loaded.model_copy(update={"fields": joined}))
    assert "seg" not in [f["name"] for f in stored_dataset(kind, tmp_path, "orders")["fields"]]
    reloaded = await reopen(kind, tmp_path).get_dataset("orders", data_source=DS)
    assert reloaded is not None
    assert (reloaded.get_field("seg") or loaded.fields[0]).expression == "customers.segment"
