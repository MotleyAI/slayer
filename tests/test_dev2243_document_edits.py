"""Spec: models/semantic-model-documents — namespace and quarantine, edit-through, text fidelity, concurrency,
fingerprints, delete cascades, access filtering, unknown types, surfaces and search."""

from __future__ import annotations

import asyncio
import contextlib
import difflib
import json
import re
import warnings
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from httpx import ASGITransport, AsyncClient

from slayer.api.server import create_app
from slayer.client import slayer_client as slayer_client_module
from slayer.client.slayer_client import SlayerClient
from slayer.core.document_text import edit_text
from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.errors import HiddenContentConflictError, UnknownMetricError
from slayer.core.field_expression import portable_text
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Aggregation, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding, EntityKind
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.core import slayer_extension as ext
from slayer.sql.ossie_dialects import expression_digest
from slayer.core.ossie_expression import OSIExpression
from slayer.search.service import SearchService
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import open_connection, transaction
from tests._cli_inprocess import run_cli_in_process
from tests._dev1737_fixtures import TableSpec
from tests._dev2243_fixtures import (
    DS, SALES, STORAGES, dataset, document_engine, document_path, document_text, error, field, is_untyped,
    make_storage, metric, place_document, relationship, store_document,
)
from tests._dev2277_fixtures import by, customers_dataset, orders_dataset, returns_dataset, scoped
from tests._dataset_access_fixtures import mcp_text


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
STORES_FIELD = "      - name: store_id\n        expression: {sql: store_id}\n        datatype: String\n"
STORES_DATASET = """\
  - name: stores
    source: stores
    primary_key: [store_id]
    fields:
      - name: store_id
        expression: {sql: store_id}
        datatype: String
      - name: customer_id
        expression: {sql: customer_id}
        datatype: String
"""
#: ``SALES`` plus ``orders.store_id`` and a ``stores`` dataset (no relationship to it).
SALES_STORES = SALES.replace("    metrics:\n      - name: total_revenue", STORES_FIELD + "    metrics:\n"
                             "      - name: total_revenue").replace("relationships:\n", STORES_DATASET + "relationships:\n")


async def text_of(storage: StorageBackend, name: str = "sales") -> str:
    text = await storage.get_semantic_model_text(name, data_source=DS)
    assert text is not None
    return text


async def doc_of(storage: StorageBackend, name: str = "sales") -> dict[str, Any]:
    return yaml.safe_load(await text_of(storage, name))


def entry(doc: dict[str, Any], name: str) -> dict[str, Any]:
    [found] = [d for d in doc["datasets"] if d["name"] == name]
    return found


def slayer_text(entity: dict[str, Any]) -> str:
    """The ``SLAYER`` extension payloads of ``entity`` as one JSON text ('' when none)."""
    data = [e.get("data") for e in entity.get("nonportable_extensions") or [] if e.get("vendor_name") == "SLAYER"]
    return json.dumps(data) if data else ""


def core(entity: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in entity.items() if k != "nonportable_extensions"}


def diff(before: str, after: str) -> tuple[list[str], list[str]]:
    """``(removed, added)`` lines between two texts."""
    lines = list(difflib.ndiff(before.splitlines(), after.splitlines()))
    return [ln[2:] for ln in lines if ln.startswith("- ")], [ln[2:] for ln in lines if ln.startswith("+ ")]


def standalone_exists(kind: str, base: Path, name: str) -> bool:
    """A standalone dataset ``name`` is stored outside every document."""
    if kind == "yaml":
        return (base / "store" / "models" / DS / f"{name}.yaml").exists()
    with open_connection(base / "store.db") as con:
        return bool(con.execute("SELECT 1 FROM models WHERE data_source = ? AND name = ?", (DS, name)).fetchall())


def shop(model: SlayerDataset) -> SlayerDataset:
    return model.model_copy(update={"data_source": DS})


async def without_rpc(storage: StorageBackend) -> None:
    """Remove ``revenue_per_customer``, so ``orders`` and ``customers`` can be deleted."""
    await storage.edit_semantic_model("sales", data_source=DS, remove_metrics=["revenue_per_customer"])


async def must_get(storage: StorageBackend, name: str) -> SlayerDataset:
    found = await storage.get_dataset(name, data_source=DS)
    assert found is not None, name
    return found


async def single(engine: Any, *, population: str, metric_expr: str) -> Any:
    resp = await engine.execute({"population": population, "metrics": [{"metric": metric_expr, "alias": "v"}]})
    assert len(resp.data) == 1
    return resp.data[0]["v"]


# --------------------------------------------------------------------------- #
# Namespace and quarantine
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_colliding_save_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A colliding save is refused."""
    storage = make_storage(kind, tmp_path)
    await storage.save_dataset(shop(orders_dataset()).model_copy(update={"relationships": []}), _validate=False)
    text = document_text(datasets=[dataset("ORDERS", source="orders", fields=[field("order_id")])])
    with pytest.raises(error("NamespaceCollisionError")) as info:
        await store_document(storage, text)
    assert "standalone dataset 'orders'" in str(info.value)
    assert "semantic model 'sales'" in str(info.value)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_case_only_collision_between_documents_is_refused(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    other = document_text(name="ledger", datasets=[dataset("ORDERS", source="orders", fields=[field("order_id")])])
    with pytest.raises(error("NamespaceCollisionError")) as info:
        await store_document(storage, other)
    assert "sales" in str(info.value)
    assert "ledger" in str(info.value)


@pytest.mark.parametrize("kind", STORAGES)
async def test_model_scoped_metric_names_are_unique_across_documents(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    other = document_text(name="ledger", datasets=[dataset("returns", fields=[field("amount", datatype="Float")])],
                          metrics=[metric("revenue_per_customer", "SUM(returns.amount)")])
    with pytest.raises(error("NamespaceCollisionError")) as info:
        await store_document(storage, other)
    assert "revenue_per_customer" in str(info.value)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_model_scoped_metric_named_like_a_dataset_is_refused(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    text = document_text(datasets=[dataset("orders", fields=[field("amount", datatype="Float")])],
                         metrics=[metric("Orders", "SUM(orders.amount)")])
    with pytest.raises((error("NamespaceCollisionError"), error("ModelMetricNameError"))):
        await store_document(storage, text)


async def test_mixed_case_names_resolve_case_insensitively_and_keep_their_spelling() -> None:
    text = document_text(datasets=[dataset("Orders", source="orders", fields=[field("Amount", "amount", datatype="Float")])],
                         metrics=[metric("RevenueTotal", "SUM(Orders.Amount)")])
    async with document_engine(documents=(text,)) as engine:
        stored = await engine.storage.get_dataset("Orders", data_source=DS)
        assert stored is not None
        assert (stored.name, [f.name for f in stored.fields]) == ("Orders", ["Amount"])
        assert await single(engine, population="orders", metric_expr="sum(orders.amount)") == 650.0
        resp = await engine.execute({"population": "orders", "metrics": ["revenuetotal"]})
        assert [float(v) for v in resp.data[0].values()] == [650.0]


QUARANTINE_A = document_text(
    datasets=[dataset("orders", fields=[field("customer_id"), field("amount", datatype="Float")]),
              dataset("customers", fields=[field("customer_id"), field("region")], primary_key=["customer_id"])],
    relationships=[relationship("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"],
                                to_columns=["customer_id"])],
    metrics=[metric("revenue", "SUM(orders.amount)"), metric("regions", "COUNT(DISTINCT customers.region)")],
)
QUARANTINE_B = document_text(name="ledger", datasets=[dataset("orders", fields=[field("order_id")])])


def _assert_quarantined(loaded: list[SlayerDataset], errs: list[Any]) -> None:
    assert {d.name for d in loaded} == {"customers"}
    owners = [e for e in errs if e.kind == "dataset" and e.name == "orders"]
    assert len(owners) == 2
    for kind, name in (("relationship", "orders_to_customers"), ("metric", "revenue")):
        [hit] = [e for e in errs if e.kind == kind and e.name == name]
        assert "orders" in hit.cause_text
    assert not [e for e in errs if e.name in {"customers", "regions"}]


async def test_hand_placed_collisions_quarantine_dependents(tmp_path: Path) -> None:
    """Spec scenario: Hand-placed collisions quarantine dependents."""
    place_document(tmp_path, QUARANTINE_A, name="sales")
    place_document(tmp_path, QUARANTINE_B, name="ledger")
    storage = make_storage("yaml", tmp_path)
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_quarantined(loaded, errs)
    assert {m.name for m in await storage.list_model_scoped_metrics(DS)} == {"regions"}


async def test_hand_placed_collisions_quarantine_dependents_in_sqlite(tmp_path: Path) -> None:
    storage = make_storage("sqlite", tmp_path)
    await store_document(storage, QUARANTINE_A)
    with transaction(tmp_path / "store.db") as con:
        con.execute("INSERT INTO semantic_models (data_source, name, text) VALUES (?, ?, ?)", (DS, "ledger", QUARANTINE_B))
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_quarantined(loaded, errs)


async def test_a_document_dataset_colliding_with_a_standalone_file_is_unloadable_in_both(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    await storage.save_dataset(shop(returns_dataset()).model_copy(update={"name": "orders", "relationships": []}),
                               _validate=False)
    place_document(tmp_path, QUARANTINE_A, name="sales")
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert "orders" not in {d.name for d in loaded}
    assert len([e for e in errs if e.kind == "dataset" and e.name == "orders"]) == 2


# --------------------------------------------------------------------------- #
# Edit-through
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_adding_a_slayer_only_filter_to_a_document_dataset(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Adding a SLayer-only filter to a document dataset."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    original = entry(await doc_of(storage), "orders")
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, add_filters=["status <> 'test'"],
                   hidden=True)
    orders_entry = entry(await doc_of(storage), "orders")
    assert "status <> 'test'" in slayer_text(orders_entry)
    assert "hidden" in slayer_text(orders_entry)
    assert core(orders_entry) == core(original)
    orders = await must_get(storage, "orders")
    assert (orders.filters, orders.hidden) == (["status <> 'test'"], True)
    assert not standalone_exists(kind, tmp_path, "orders")


async def test_a_slayer_only_filter_on_a_document_dataset_applies_to_queries() -> None:
    async with document_engine() as engine:
        await mcp_text(engine.storage, "edit_dataset", dataset_name="orders", data_source=DS,
                       add_filters=["status <> 'open'"])
        assert await single(engine, population="orders", metric_expr="sum(orders.amount)") == 150.0


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_new_relationship_lands_at_the_documents_top_level(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A new relationship lands at the document's top level."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES_STORES)
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, relationships=[
        {"name": "orders_to_stores", "to": "stores", "from_columns": ["store_id"], "to_columns": ["store_id"],
         "cardinality": "many_to_one"}])
    doc = await doc_of(storage)
    assert relationship("orders_to_stores", src="orders", to="stores", from_columns=["store_id"],
                        to_columns=["store_id"]) in doc["relationships"]
    orders_entry = entry(doc, "orders")
    assert "relationships" not in orders_entry
    assert "orders_to_stores" not in slayer_text(orders_entry)
    assert {r.name for r in (await must_get(storage, "orders")).relationships} == {"orders_to_customers",
                                                                                   "orders_to_stores"}


_SLAYER_ONLY: list[Any] = [
    pytest.param({"filters": ["amount > 0"]}, "filters", None, id="row-filter"),
    pytest.param({"hidden": True}, "hidden", None, id="hidden"),
    pytest.param({"access_tags": ["finance"]}, "access_tags", None, id="access-tags"),
    pytest.param({"aggregations": [Aggregation(name="sum_sq", expression="SUM({value} * {value})")]}, "aggregations",
                 None, id="custom-aggregation"),
    pytest.param({"format": NumberFormat(type=NumberFormatType.CURRENCY, precision=2, symbol="$")}, "format", "amount",
                 id="field-format"),
    pytest.param({"hidden": True}, "hidden", "amount", id="field-hidden"),
]


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("update", "attribute", "field_name"), _SLAYER_ONLY)
async def test_slayer_only_content_is_written_to_a_slayer_entry(
    tmp_path: Path, kind: str, update: dict[str, Any], attribute: str, field_name: str | None,
) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    before = entry(await doc_of(storage), "orders")
    orders = await must_get(storage, "orders")
    if field_name is None:
        edited = orders.model_copy(update=update)
    else:
        edited = orders.model_copy(update={"fields": [f.model_copy(update=update) if f.name == field_name else f
                                                      for f in orders.fields]})
    await storage.save_dataset(edited)
    after = entry(await doc_of(storage), "orders")
    reread = await must_get(storage, "orders")
    if field_name is None:
        assert core(after) == core(before)
        assert attribute in slayer_text(after)
        assert getattr(reread, attribute) == getattr(edited, attribute)
    else:
        [f_before] = [f for f in before["fields"] if f["name"] == field_name]
        [f_after] = [f for f in after["fields"] if f["name"] == field_name]
        assert core(f_after) == core(f_before)
        assert attribute in slayer_text(f_after)
        [f_reread] = [f for f in reread.fields if f.name == field_name]
        assert getattr(f_reread, attribute) == update[attribute]
    assert not standalone_exists(kind, tmp_path, "orders")


@pytest.mark.parametrize("kind", STORAGES)
async def test_reading_a_patched_document_yields_the_dataset_as_written(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, description="One row per order.",
                   fields=[{"name": "double_amount", "expression": "amount * 2", "datatype": "Float", "label": "x2"}],
                   metrics=[{"name": "order_count", "expression": "count(*)"}])
    orders = await must_get(storage, "orders")
    assert orders.description == "One row per order."
    [doubled] = [f for f in orders.fields if f.name == "double_amount"]
    assert doubled.label == "x2"
    assert {m.name for m in orders.metrics} == {"total_revenue", "order_count"}


# --------------------------------------------------------------------------- #
# Owner routing: every dataset write patches the owning document
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_create_dataset_with_a_semantic_model_adds_it_to_the_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "create_dataset", name="returns", sql_table="returns", data_source=DS,
                   fields=[{"name": "return_id", "datatype": "String"}, {"name": "amount", "datatype": "Float"}],
                   semantic_model="sales")
    assert "returns" in {d["name"] for d in (await doc_of(storage))["datasets"]}
    assert await storage.semantic_model_owner("returns", data_source=DS) == "sales"
    assert not standalone_exists(kind, tmp_path, "returns")


@pytest.mark.parametrize("kind", STORAGES)
async def test_create_dataset_without_a_semantic_model_is_standalone(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "create_dataset", name="returns", sql_table="returns", data_source=DS,
                   fields=[{"name": "return_id", "datatype": "String"}])
    assert await storage.semantic_model_owner("returns", data_source=DS) is None
    assert standalone_exists(kind, tmp_path, "returns")
    assert "returns" not in await text_of(storage)


@pytest.mark.parametrize("kind", STORAGES)
async def test_delete_dataset_removes_it_from_the_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await without_rpc(storage)
    await mcp_text(storage, "delete_dataset", name="orders", data_source=DS)
    assert [d["name"] for d in (await doc_of(storage))["datasets"]] == ["customers"]
    assert await storage.get_dataset("orders", data_source=DS) is None
    assert not standalone_exists(kind, tmp_path, "orders")


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_rest_dataset_write_patches_the_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    body = (await must_get(storage, "orders")).model_copy(update={"description": "Via REST."}).model_dump(mode="json")
    async with AsyncClient(transport=ASGITransport(app=create_app(storage=storage)), base_url="http://t") as client:
        resp = await client.put("/datasets/orders", json=body)
    assert resp.status_code == 200, resp.text
    assert entry(await doc_of(storage), "orders")["description"] == "Via REST."
    assert not standalone_exists(kind, tmp_path, "orders")


def test_a_cli_dataset_write_patches_the_document(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    asyncio.run(store_document(storage, SALES))
    orders = asyncio.run(must_get(storage, "orders"))
    spec = tmp_path / "orders.yaml"
    spec.write_text(yaml.safe_dump(orders.model_copy(update={"description": "Via CLI."}).model_dump(
        mode="json", exclude_none=True)), encoding="utf-8")
    result = run_cli_in_process(["datasets", "--storage", str(tmp_path / "store"), "create", str(spec)])
    assert result.returncode == 0, result.stderr
    assert entry(asyncio.run(doc_of(storage)), "orders")["description"] == "Via CLI."
    assert not standalone_exists("yaml", tmp_path, "orders")


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_field_sampling_update_patches_the_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.update_field_sampled(data_source=DS, dataset_name="customers", field_name="region",
                                       sampled="EAST, WEST", sampled_values=["EAST", "WEST"], distinct_count=3)
    [region] = [f for f in (await must_get(storage, "customers")).fields if f.name == "region"]
    assert (region.sampled_values, region.distinct_count) == (["EAST", "WEST"], 3)
    assert "EAST" in slayer_text(next(f for f in entry(await doc_of(storage), "customers")["fields"]
                                      if f["name"] == "region"))
    assert not standalone_exists(kind, tmp_path, "customers")


async def test_cardinality_persistence_patches_the_document(tmp_path: Path) -> None:
    text = SALES_STORES.replace("metrics:\n  - name: revenue_per_customer", "  - name: stores_to_customers\n"
                                "    from: stores\n    to: customers\n    from_columns: [customer_id]\n"
                                "    to_columns: [customer_id]\nmetrics:\n  - name: revenue_per_customer")
    async with document_engine(documents=(text,), base=tmp_path) as engine:
        await engine.detect_relationship_cardinality(data_source=DS, persist=True)
        stores = await must_get(engine.storage, "stores")
        assert [r.cardinality for r in stores.relationships] == [RelationshipCardinality.ONE_TO_ONE]
        orders = await must_get(engine.storage, "orders")
        assert {r.cardinality for r in orders.relationships} == {RelationshipCardinality.MANY_TO_ONE}
        assert "SLAYER" in await text_of(engine.storage)
    assert not standalone_exists("yaml", tmp_path, "stores")


async def test_an_ingestion_upsert_patches_the_document(tmp_path: Path) -> None:
    text = document_text(datasets=[dataset("orders", fields=[field("order_id"), field("amount", datatype="Float")],
                                           primary_key=["order_id"])])
    async with document_engine(documents=(text,), base=tmp_path) as engine:
        ds = await engine.storage.get_datasource(DS)
        assert ds is not None
        await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, client=engine.client_for(ds),
                                           include_tables=["orders"])
        names = [f["name"] for f in entry(await doc_of(engine.storage), "orders")["fields"]]
        assert names[:2] == ["order_id", "amount"]
        assert "status" in names
    assert not standalone_exists("yaml", tmp_path, "orders")


# --------------------------------------------------------------------------- #
# Text fidelity
# --------------------------------------------------------------------------- #
async def _edit_field(storage: StorageBackend) -> None:
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS,
                   fields=[{"name": "amount", "description": "Order amount."}])


async def _edit_metric(storage: StorageBackend) -> None:
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS,
                   metrics=[{"name": "total_revenue", "expression": "SUM(amount)", "description": "All revenue."}])


async def _edit_relationship(storage: StorageBackend) -> None:
    await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, relationships=[relationship(
        "orders_to_stores", src="orders", to="stores", from_columns=["store_id"], to_columns=["store_id"])])


async def _edit_extension(storage: StorageBackend) -> None:
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, hidden=True)


async def _edit_document(storage: StorageBackend) -> None:
    await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, description="Sales and customers.")


def _only_added(*expected: str) -> Callable[[list[str], list[str]], None]:
    def check(removed: list[str], added: list[str]) -> None:
        assert removed == []
        assert [ln.strip() for ln in added] == list(expected)
    return check


def _added_containing(token: str) -> Callable[[list[str], list[str]], None]:
    def check(removed: list[str], added: list[str]) -> None:
        assert removed == []
        assert added
        assert token in "\n".join(added)
    return check


def _document_description(removed: list[str], added: list[str]) -> None:
    assert removed == ["description: Orders and customers."]
    assert added == ["description: Sales and customers."]


_EDITS: list[Any] = [
    pytest.param(_edit_field, _only_added("description: Order amount."), id="field"),
    pytest.param(_edit_metric, _only_added("description: All revenue."), id="dataset-metric"),
    pytest.param(_edit_relationship, _added_containing("orders_to_stores"), id="relationship"),
    pytest.param(_edit_extension, _added_containing("SLAYER"), id="extension-entry"),
    pytest.param(_edit_document, _document_description, id="document-field"),
]


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("edit", "check"), _EDITS)
async def test_a_save_changes_only_the_edited_lines(
    tmp_path: Path, kind: str, edit: Callable[[StorageBackend], Awaitable[None]],
    check: Callable[[list[str], list[str]], None],
) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES_STORES)
    await edit(storage)
    after = await text_of(storage)
    check(*diff(SALES_STORES, after))
    assert "  # owned by finance" in after.splitlines()


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_comment_survives_an_unrelated_edit(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A comment survives an unrelated edit."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, description="One row per order.")
    removed, added = diff(SALES, await text_of(storage))
    assert removed == []
    assert [ln.strip() for ln in added] == ["description: One row per order."]


FORMATTED = """\
name: sales
datasets:
- name: orders
  primary_key: [ order_id ]
  description: "A long description folded by its author
    onto a second line."
- name: stores
  primary_key: [ store_id ]
"""


def _describe_stores(data: dict[str, Any]) -> dict[str, Any]:
    data["datasets"][1]["description"] = "Stores."
    return data


def test_an_edit_keeps_the_authors_formatting_of_unedited_nodes() -> None:
    """Folded scalars and flow-sequence spacing the emitter would rewrite survive an edit elsewhere."""
    edited = edit_text(FORMATTED, _describe_stores)
    assert edited == FORMATTED + "  description: Stores.\n"
    assert edit_text(FORMATTED, lambda data: data) == FORMATTED


def test_a_json_edit_keeps_unedited_inline_arrays() -> None:
    text = '{\n  "name": "sales",\n  "tags": ["a", "b"],\n  "description": "Old."\n}\n'
    edited = edit_text(text, lambda data: {**data, "description": "New."})
    assert edited == text.replace("Old.", "New.")


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_json_document_keeps_its_key_order(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    doc = {"name": "sales", "version": "0.3.0.dev0", "description": "JSON.",
           "datasets": [{"fields": [{"name": "amount", "datatype": "Float", "expression": {"sql": "amount"}}],
                         "source": "orders", "name": "orders"}]}
    text = json.dumps(doc, indent=2)
    await storage.save_semantic_model_text(data_source=DS, name="sales", text=text)
    await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, description="Edited.")
    await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, description="Orders.")
    saved = json.loads(await text_of(storage))
    assert list(saved) == ["name", "version", "description", "datasets"]
    assert list(saved["datasets"][0])[:3] == ["fields", "source", "name"]
    assert list(saved["datasets"][0]["fields"][0]) == ["name", "datatype", "expression"]
    assert (saved["description"], saved["datasets"][0]["description"]) == ("Edited.", "Orders.")


# --------------------------------------------------------------------------- #
# Concurrency
# --------------------------------------------------------------------------- #
async def _describe(storage: StorageBackend, name: str, description: str) -> None:
    current = await must_get(storage, name)
    await storage.save_dataset(current.model_copy(update={"description": description}))


@pytest.mark.parametrize("kind", STORAGES)
async def test_two_edits_to_different_datasets_of_one_document(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Two edits to different datasets of one document."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    for round_ in range(5):
        await asyncio.gather(
            mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, description=f"O{round_}"),
            mcp_text(storage, "edit_dataset", dataset_name="customers", data_source=DS, description=f"C{round_}"),
        )
        doc = await doc_of(storage)
        assert (entry(doc, "orders").get("description"), entry(doc, "customers").get("description")) == (
            f"O{round_}", f"C{round_}")


@pytest.mark.parametrize("kind", STORAGES)
async def test_concurrent_storage_writes_to_one_document_never_lose_an_update(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await asyncio.gather(
        _describe(storage, "orders", "Orders."), _describe(storage, "customers", "Customers."),
        *(storage.update_field_sampled(data_source=DS, dataset_name="orders", field_name=name, sampled=name,
                                       sampled_values=[name], distinct_count=i)
          for i, name in enumerate(("order_id", "customer_id", "amount", "status"))),
    )
    orders = await must_get(storage, "orders")
    assert {f.name: f.distinct_count for f in orders.fields} == {"order_id": 0, "customer_id": 1, "amount": 2,
                                                                 "status": 3}
    assert (orders.description, (await must_get(storage, "customers")).description) == ("Orders.", "Customers.")


# --------------------------------------------------------------------------- #
# Fingerprints
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_every_document_write_changes_the_graph_fingerprint(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    seen = [await storage.graph_fingerprint()]
    await store_document(storage, SALES)
    seen.append(await storage.graph_fingerprint())
    await _describe(storage, "orders", "Orders.")
    await without_rpc(storage)
    seen.append(await storage.graph_fingerprint())
    await storage.delete_dataset("customers", data_source=DS)
    seen.append(await storage.graph_fingerprint())
    await storage.delete_semantic_model("sales", data_source=DS)
    seen.append(await storage.graph_fingerprint())
    assert None not in seen
    assert all(a != b for a, b in zip(seen, seen[1:]))


STATUS_UPPER = "      - name: status_upper\n        expression: {sql: UPPER(status)}\n        datatype: String\n"
SALES_UPPER = SALES.replace("    metrics:\n      - name: total_revenue", STATUS_UPPER + "    metrics:\n"
                            "      - name: total_revenue")
UPPER_QUERY = {"population": "orders", "dimensions": ["orders.status_upper"],
               "metrics": [{"metric": "count(orders.*)", "alias": "n"}]}


async def test_an_external_edit_is_seen_on_the_next_query(tmp_path: Path) -> None:
    """Spec scenario: An external edit is seen on the next query."""
    async with document_engine(base=tmp_path) as engine:
        assert await single(engine, population="orders", metric_expr="sum(orders.amount)") == 650.0
        before = await engine.storage.graph_fingerprint()
        document_path(tmp_path, name="sales").write_text(SALES_UPPER, encoding="utf-8")
        assert await engine.storage.graph_fingerprint() != before
        assert by(await engine.execute(UPPER_QUERY), "orders.status_upper", "n") == {"COMPLETED": 2.0, "OPEN": 2.0}


async def test_an_external_sqlite_edit_is_seen_on_the_next_query(tmp_path: Path) -> None:
    async with document_engine(kind="sqlite", base=tmp_path) as engine:
        assert await single(engine, population="orders", metric_expr="sum(orders.amount)") == 650.0
        before = await engine.storage.graph_fingerprint()
        with transaction(tmp_path / "store.db") as con:
            con.execute("UPDATE semantic_models SET text = ? WHERE data_source = ? AND name = ?", (SALES_UPPER, DS, "sales"))
        assert await engine.storage.graph_fingerprint() != before
        assert by(await engine.execute(UPPER_QUERY), "orders.status_upper", "n") == {"COMPLETED": 2.0, "OPEN": 2.0}


# --------------------------------------------------------------------------- #
# SLAYER entries are read on every load
# --------------------------------------------------------------------------- #
AMOUNT_FORMAT = NumberFormat(type=NumberFormatType.CURRENCY, precision=2)


def metric_entry(*, core_sql: str, expression: str, entity: str = "total_revenue",
                 home: str = "orders") -> dict[str, Any]:
    """A ``SLAYER`` metric entry recording ``expression``, fingerprinted against the core ``sql`` ``core_sql``."""
    payload = ext.MetricPayload(entity=entity, dataset=home, metric=DatasetMetric(name=entity, expression=expression))
    return ext.encode(payload, ossie=expression_digest(OSIExpression(sql=core_sql)))


def amount_entry() -> dict[str, Any]:
    """A valid ``SLAYER`` entry giving ``orders.amount`` a currency format."""
    payload = ext.FieldPayload(entity="amount", field=ext.FieldContent(datatype=DataType.FLOAT, format=AMOUNT_FORMAT))
    return ext.encode(payload, ossie=expression_digest(OSIExpression(sql="amount")))


def relationship_entry(*, src: str, to: str, columns: list[str], declared: DatasetRelationship,
                       declaring: str | None = None) -> dict[str, Any]:
    payload = ext.RelationshipPayload(entity=declared.name or "", declarations=[
        ext.RelationshipDeclaration(dataset=declaring or src, relationship=declared)])
    return ext.encode(payload, ossie=ext.relationship_digest(from_dataset=src, to=to, from_columns=columns,
                                                             to_columns=columns))


def sales_with_entries(*, metrics: list[dict[str, Any]] | None = None, revenue: list[dict[str, Any]] | None = None,
                       relationships: list[dict[str, Any]] | None = None,
                       orders_to_customers: list[dict[str, Any]] | None = None) -> str:
    """``SALES`` with ``amount`` carrying ``amount_entry()``, extra ``orders`` metrics and entries attached."""
    doc = yaml.safe_load(SALES)
    orders = entry(doc, "orders")
    next(f for f in orders["fields"] if f["name"] == "amount")["nonportable_extensions"] = [amount_entry()]
    orders["metrics"].extend(metrics or [])
    if revenue:
        orders["metrics"][0]["nonportable_extensions"] = revenue
    doc["relationships"].extend(relationships or [])
    if orders_to_customers:
        doc["relationships"][0]["nonportable_extensions"] = orders_to_customers
    return yaml.safe_dump(doc, sort_keys=False)


@contextlib.asynccontextmanager
async def placed(base: Path, text: str) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine whose document ``sales`` was written by hand as ``text``."""
    async with document_engine(documents=(), base=base) as engine:
        place_document(base, text, name="sales")
        yield engine


AOV_RESTORED = "SUM(amount) / COUNT(*)"
AOV_METRICS = [metric("revenue", "SUM(amount)"), metric("n", "COUNT(*)")]


def aov(core_sql: str) -> dict[str, Any]:
    return {**metric("aov", core_sql),
            "nonportable_extensions": [metric_entry(entity="aov", core_sql=AOV_RESTORED, expression="revenue / n")]}


async def test_a_matching_slayer_entry_restores_its_formula(tmp_path: Path) -> None:
    async with placed(tmp_path, sales_with_entries(metrics=[*AOV_METRICS, aov(AOV_RESTORED)])) as engine:
        [restored] = [m for m in (await must_get(engine.storage, "orders")).metrics if m.name == "aov"]
        assert portable_text(restored.expression) == "revenue / n"
        assert await single(engine, population="orders", metric_expr="orders.aov") == 162.5


async def test_a_hand_edit_makes_its_payload_stale(tmp_path: Path) -> None:
    """Spec scenario: A hand edit makes its payload stale."""
    async with placed(tmp_path, sales_with_entries(metrics=[*AOV_METRICS, aov("SUM(amount) / 2")])) as engine:
        [edited] = [m for m in (await must_get(engine.storage, "orders")).metrics if m.name == "aov"]
        assert portable_text(edited.expression) != "revenue / n"
        assert await single(engine, population="orders", metric_expr="orders.aov") == 325.0
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.orders.aov", entity_type="metric",
                                   compact=False)
    assert "aov" in inspected
    assert "slayer" in inspected.lower()
    assert re.search(r"edited|stale|fingerprint", inspected, re.IGNORECASE), inspected


def _unknown_version() -> list[dict[str, Any]]:
    entry_ = metric_entry(core_sql="SUM(amount)", expression="sum(amount) * 2")
    entry_["data"]["version"] = 99
    return [entry_]


_IGNORED: list[Any] = [
    pytest.param(lambda: [{"vendor_name": "SLAYER", "data": {"version": 3, "kind": "metric", "entity": "total_revenue"}}],
                 r"malformed", id="malformed"),
    pytest.param(_unknown_version, r"version", id="unknown-version"),
    pytest.param(lambda: [metric_entry(entity="other_metric", core_sql="SUM(amount)", expression="sum(amount) * 2")],
                 r"describ|attach|other_metric", id="misattached"),
    pytest.param(lambda: [metric_entry(core_sql="SUM(amount)", expression="sum(amount) * 2")] * 2,
                 r"several|duplicate|more than one|multiple", id="duplicate-entries"),
    pytest.param(lambda: [metric_entry(core_sql="SUM(amount)", expression="sum(ghost_col)")], r"ghost_col",
                 id="makes-the-dataset-invalid"),
    pytest.param(lambda: [metric_entry(home="ghost_ds", core_sql="SUM(amount)", expression="sum(amount) * 2")],
                 r"ghost_ds", id="names-an-absent-entity"),
]


@pytest.mark.parametrize(("entries", "reason"), _IGNORED)
async def test_an_unusable_slayer_entry_is_ignored_with_a_load_warning(
    tmp_path: Path, entries: Callable[[], list[dict[str, Any]]], reason: str,
) -> None:
    async with placed(tmp_path, sales_with_entries(revenue=entries())) as engine:
        loaded, errs = await engine.storage.load_datasets(data_source=DS)
        assert errs == []
        assert {d.name for d in loaded} == {"orders", "customers"}
        assert await single(engine, population="orders", metric_expr="orders.total_revenue") == 650.0
        [amount] = [f for f in (await must_get(engine.storage, "orders")).fields if f.name == "amount"]
        assert amount.format == AMOUNT_FORMAT
        resp = await engine.execute({"metrics": ["revenue_per_customer"]})
        assert [float(r["revenue_per_customer"]) for r in resp.data] == [162.5]
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.orders.total_revenue",
                                   entity_type="metric", compact=False)
    assert "total_revenue" in inspected
    assert "slayer" in inspected.lower()
    assert re.search(reason, inspected, re.IGNORECASE), inspected


async def test_an_unusable_relationship_entry_keeps_the_default_relationship(tmp_path: Path) -> None:
    declared = DatasetRelationship(name="orders_to_customers", to="customers", from_columns=["customer_id"],
                                   to_columns=["customer_id"], cardinality=RelationshipCardinality.ONE_TO_ONE,
                                   join_type=JoinType.INNER)
    bad = relationship_entry(src="orders", to="customers", columns=["customer_id"], declared=declared,
                             declaring="ghost_ds")
    async with placed(tmp_path, sales_with_entries(orders_to_customers=[bad])) as engine:
        [rel] = (await must_get(engine.storage, "orders")).relationships
        assert (rel.cardinality, rel.join_type) == (RelationshipCardinality.MANY_TO_ONE, JoinType.LEFT)
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.orders", entity_type="dataset",
                                   compact=False)
    assert "orders_to_customers" in inspected
    assert "ghost_ds" in inspected


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_relationship_keeps_its_declared_cardinality(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A relationship keeps its declared cardinality."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "edit_dataset", dataset_name="customers", data_source=DS, relationships=[
        {"name": "customer_orders", "to": "orders", "from_columns": ["customer_id"], "to_columns": ["customer_id"],
         "cardinality": "one_to_many", "join_type": "inner"}])
    assert "customer_orders" in {r["name"] for r in (await doc_of(storage))["relationships"]}
    for reader in (storage, make_storage(kind, tmp_path)):
        [rel] = (await must_get(reader, "customers")).relationships
        assert (rel.name, rel.cardinality, rel.join_type) == (
            "customer_orders", RelationshipCardinality.ONE_TO_MANY, JoinType.INNER)
    orders_rels = {r.name: r.cardinality for r in (await must_get(storage, "orders")).relationships}
    assert orders_rels == {"orders_to_customers": RelationshipCardinality.MANY_TO_ONE}


async def test_a_hand_written_relationship_entry_overrides_the_default(tmp_path: Path) -> None:
    declared = DatasetRelationship(name="customer_orders", to="orders", from_columns=["customer_id"],
                                   to_columns=["customer_id"], cardinality=RelationshipCardinality.ONE_TO_MANY,
                                   join_type=JoinType.INNER)
    rel = {**relationship("customer_orders", src="customers", to="orders", from_columns=["customer_id"],
                          to_columns=["customer_id"]),
           "nonportable_extensions": [relationship_entry(src="customers", to="orders", columns=["customer_id"],
                                                         declared=declared)]}
    place_document(tmp_path, sales_with_entries(relationships=[rel]), name="sales")
    storage = make_storage("yaml", tmp_path)
    [read] = (await must_get(storage, "customers")).relationships
    assert (read.name, read.cardinality, read.join_type) == (
        "customer_orders", RelationshipCardinality.ONE_TO_MANY, JoinType.INNER)


# --------------------------------------------------------------------------- #
# Delete cascades
# --------------------------------------------------------------------------- #
_EMBEDDED: dict[str, EntityKind] = {
    f"{DS}.orders": "dataset", f"{DS}.orders.amount": "field", f"{DS}.customers": "dataset",
    f"{DS}.revenue_per_customer": "metric",
}


async def _save_embeddings(storage: StorageBackend) -> None:
    name = embedding_client.current_model()
    await storage.save_embeddings([Embedding(canonical_id=cid, embedding_model_name=name, entity_kind=kind,
                                             content_hash=cid, embedding=[1.0, 0.0]) for cid, kind in _EMBEDDED.items()])


async def _embedded(storage: StorageBackend) -> set[str]:
    return {e.canonical_id for e in await storage.list_embeddings(embedding_model_name=embedding_client.current_model())}


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_document_dataset_strips_memory_entities(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Deleting a document dataset strips memory entities."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_memory(id="m1", learning="Orders by region", entities=[f"{DS}.orders", f"{DS}.customers"])
    await without_rpc(storage)
    assert await storage.delete_dataset("orders", data_source=DS) is True
    assert (await storage.get_memory("m1")).entities == [f"{DS}.customers"]
    doc = await doc_of(storage)
    assert [d["name"] for d in doc["datasets"]] == ["customers"]
    assert not doc.get("relationships")


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_document_dataset_removes_its_embeddings(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await without_rpc(storage)
    await _save_embeddings(storage)
    await storage.delete_dataset("orders", data_source=DS)
    assert await _embedded(storage) == {f"{DS}.customers", f"{DS}.revenue_per_customer"}


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_document_dataset_drops_inbound_relationships(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(shop(returns_dataset()), _validate=False)
    await without_rpc(storage)
    await storage.delete_dataset("customers", data_source=DS)
    assert (await must_get(storage, "returns")).relationships == []
    assert (await must_get(storage, "orders")).relationships == []
    assert not (await doc_of(storage)).get("relationships")


BY_STATUS = SlayerDataset.model_validate({"name": "by_status", "data_source": DS, "source_queries": [
    {"population": "orders", "dimensions": ["orders.status"],
     "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue"}]}]})


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("owner", ["standalone", "document"])
async def test_deleting_a_dataset_a_stage_reads_cascades_alike_for_document_members(kind: str, owner: str) -> None:
    async with document_engine(kind=kind, documents=(SALES,) if owner == "document" else ()) as engine:
        storage = engine.storage
        if owner == "standalone":
            await storage.save_dataset(shop(customers_dataset()))
            await storage.save_dataset(shop(orders_dataset()))
        else:
            await without_rpc(storage)
        await engine.save_dataset(BY_STATUS)
        resp = await engine.execute({"population": "by_status", "dimensions": ["by_status.status"]})
        assert {r["by_status.status"] for r in resp.data} == {"completed", "open"}
        await storage.save_memory(id="m1", learning="Revenue by status", entities=[f"{DS}.orders", f"{DS}.by_status"])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert await storage.delete_dataset("orders", data_source=DS) is True
        assert not [w for w in caught if "by_status" in str(w.message)]
        loaded, errs = await storage.load_datasets(data_source=DS)
        assert {d.name for d in loaded} == {"customers", "by_status"}
        assert errs == []
        assert (await storage.get_memory("m1")).entities == [f"{DS}.by_status"]
        with pytest.raises(ValueError, match="Dataset 'orders' not found"):
            await engine.execute({"population": "by_status", "dimensions": ["by_status.status"]})


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_whole_document_cascades_per_member(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await _save_embeddings(storage)
    await storage.save_memory(id="m1", learning="Orders", entities=[f"{DS}.orders", f"{DS}.customers.region"])
    await storage.save_memory(id="m2", learning="Other", entities=[f"{DS}.returns"])
    assert await storage.delete_semantic_model("sales", data_source=DS) is True
    assert await storage.list_semantic_models(data_source=DS) == []
    assert await storage.load_datasets(data_source=DS) == ([], [])
    assert await storage.list_model_scoped_metrics(DS) == []
    assert await _embedded(storage) == set()
    assert (await storage.get_memory("m1")).entities == []
    assert (await storage.get_memory("m2")).entities == [f"{DS}.returns"]


# --------------------------------------------------------------------------- #
# Access filtering
# --------------------------------------------------------------------------- #
HR_TABLES = [
    TableSpec(name="employees", columns=[("employee_id", "TEXT"), ("dept", "TEXT")],
              rows=[("E1", "eng"), ("E2", "eng"), ("E3", "ops")]),
    TableSpec(name="payroll", columns=[("payroll_id", "TEXT"), ("employee_id", "TEXT"), ("salary", "DOUBLE")],
              rows=[("P1", "E1", 100.0), ("P2", "E2", 200.0), ("P3", "E3", 300.0)]),
    TableSpec(name="payslips", columns=[("payslip_id", "TEXT"), ("payroll_id", "TEXT"), ("amount", "DOUBLE")],
              rows=[("Q1", "P1", 10.0)]),
]
HR = document_text(
    name="hr",
    datasets=[
        dataset("employees", fields=[field("employee_id"), field("dept")], primary_key=["employee_id"]),
        dataset("payroll", fields=[field("payroll_id"), field("employee_id"), field("salary", datatype="Float")],
                primary_key=["payroll_id"]),
        dataset("payslips", fields=[field("payslip_id"), field("payroll_id"), field("amount", datatype="Float")],
                primary_key=["payslip_id"]),
    ],
    relationships=[
        relationship("payroll_to_employees", src="payroll", to="employees", from_columns=["employee_id"],
                     to_columns=["employee_id"]),
        relationship("payslips_to_payroll", src="payslips", to="payroll", from_columns=["payroll_id"],
                     to_columns=["payroll_id"]),
    ],
    metrics=[metric("headcount", "COUNT(DISTINCT employees.employee_id)"),
             metric("payroll_total", "SUM(payroll.salary)"),
             metric("cost_per_head", "SUM(payroll.salary) / COUNT(DISTINCT employees.employee_id)")],
)


@contextlib.asynccontextmanager
async def hr_engine(kind: str = "yaml") -> AsyncGenerator[SlayerQueryEngine]:
    """The HR document with ``payroll`` tagged ``secret`` and ``per_head = payroll_total / headcount`` authored."""
    async with document_engine(kind=kind, documents=(HR,), tables=HR_TABLES) as engine:
        payroll = await must_get(engine.storage, "payroll")
        await engine.storage.save_dataset(payroll.model_copy(update={"access_tags": ["secret"]}))
        await mcp_text(engine.storage, "edit_semantic_model", name="hr", data_source=DS,
                       metrics=[{"name": "per_head", "expression": "payroll_total / headcount"}])
        yield engine


async def test_a_metric_over_a_hidden_dataset_is_invisible() -> None:
    """Spec scenario: A metric over a hidden dataset is invisible."""
    async with hr_engine() as engine, scoped(engine) as view:
        with pytest.raises(UnknownMetricError) as info:
            await view.execute({"metrics": ["cost_per_head"]})
        assert not {"payroll", "salary", "payroll_total", "per_head"} & set(
            str(info.value).replace("'", " ").replace(".", " ").split())


@pytest.mark.parametrize("kind", STORAGES)
async def test_access_filtering_prunes_metrics_transitively(kind: str) -> None:
    async with hr_engine(kind) as engine, scoped(engine) as view:
        assert [float(r["per_head"]) for r in (await engine.execute({"metrics": ["per_head"]})).data] == [200.0]
        assert {m.name for m in await view.storage.list_model_scoped_metrics(DS)} == {"headcount"}
        for name in ("payroll_total", "cost_per_head", "per_head"):
            with pytest.raises(UnknownMetricError):
                await view.execute({"metrics": [name]})
        assert [float(r["headcount"]) for r in (await view.execute({"metrics": ["headcount"]})).data] == [3.0]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_hidden_document_dataset_reads_as_deleted(kind: str) -> None:
    async with hr_engine(kind) as engine:
        view = AccessFilteredStorage(engine.storage, tags=frozenset(), bypass=False)
        assert await view.get_dataset("payroll", data_source=DS) is None
        assert sorted(await view.list_datasets(data_source=DS)) == ["employees", "payslips"]
        assert (await must_get(view, "payslips")).relationships == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_write_through_the_view_merges_onto_the_full_document(kind: str) -> None:
    async with hr_engine(kind) as engine:
        view = AccessFilteredStorage(engine.storage, tags=frozenset(), bypass=False)
        employees = await must_get(view, "employees")
        await view.save_dataset(employees.model_copy(update={"description": "Staff."}))
        doc = await doc_of(engine.storage, "hr")
        assert [d["name"] for d in doc["datasets"]] == ["employees", "payroll", "payslips"]
        assert {r["name"] for r in doc["relationships"]} == {"payroll_to_employees", "payslips_to_payroll"}
        assert {"payroll_total", "cost_per_head"} <= {m["name"] for m in doc["metrics"]}
        assert "per_head" in json.dumps(doc)
        assert entry(doc, "employees")["description"] == "Staff."


def _hr_view(engine: SlayerQueryEngine) -> AccessFilteredStorage:
    return AccessFilteredStorage(engine.storage, tags=frozenset(), bypass=False)


async def _hr_full(engine: SlayerQueryEngine) -> tuple[list[str], set[str], set[str], bool]:
    doc = await doc_of(engine.storage, "hr")
    return ([d["name"] for d in doc["datasets"]], {r["name"] for r in doc["relationships"]},
            {m["name"] for m in doc["metrics"]}, "per_head" in json.dumps(doc))


#: ``per_head`` is SLayer-only, so it lives in the document's ``SLAYER`` entry.
HR_FULL = (["employees", "payroll", "payslips"], {"payroll_to_employees", "payslips_to_payroll"},
           {"headcount", "payroll_total", "cost_per_head"}, True)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_documents_text_through_the_view_omits_hidden_entities(kind: str) -> None:
    async with hr_engine(kind) as engine:
        view = _hr_view(engine)
        doc = yaml.safe_load(await view.get_semantic_model_text("hr", data_source=DS) or "")
        assert [d["name"] for d in doc["datasets"]] == ["employees", "payslips"]
        assert doc["relationships"] == []
        assert [m["name"] for m in doc["metrics"]] == ["headcount"]
        assert not {"payroll", "payroll_total", "per_head"} & set(re.findall(r"\w+", json.dumps(doc)))
        assert await view.semantic_model_owner("payroll", data_source=DS) is None


@pytest.mark.parametrize("kind", STORAGES)
async def test_replacing_a_document_through_the_view_keeps_its_hidden_entities(kind: str) -> None:
    async with hr_engine(kind) as engine:
        view = _hr_view(engine)
        text = await view.get_semantic_model_text("hr", data_source=DS) or ""
        await view.save_semantic_model_text(data_source=DS, name="hr",
                                            text=edit_text(text, lambda d: {**d, "description": "People."}))
        assert await _hr_full(engine) == HR_FULL
        assert (await doc_of(engine.storage, "hr"))["description"] == "People."


@pytest.mark.parametrize("kind", STORAGES)
async def test_editing_a_document_through_the_view_keeps_its_hidden_entities(kind: str) -> None:
    async with hr_engine(kind) as engine:
        view = _hr_view(engine)
        await view.edit_semantic_model("hr", data_source=DS, description="People.")
        assert await _hr_full(engine) == HR_FULL


@pytest.mark.parametrize(("edit", "kwargs"), [
    ("save", {"text": document_text(name="hr", datasets=[dataset("payroll", fields=[field("payroll_id")])])}),
    ("save", {"text": document_text(name="hr", datasets=[dataset("employees", fields=[field("employee_id")])],
                                    metrics=[metric("raw_pay", "SUM(payroll.salary)")])}),
    ("edit", {"remove_metrics": ["payroll_total"]}),
    ("edit", {"remove_relationships": ["payroll_to_employees"]}),
    ("edit", {"metrics": [{"name": "raw_pay", "expression": "SUM(payroll.salary)"}]}),
    ("edit", {"metrics": [{"name": "raw_pay", "expression": {"dialects": [
        {"dialect": "ANSI_SQL", "expression": "COUNT(employees.employee_id)"},
        {"dialect": "SNOWFLAKE", "expression": "SUM(payroll.salary)"}]}}]}),
    ("edit", {"metrics": [{"name": "pay_plus", "expression": "payroll_total + 1"}]}),
    ("save", {"text": document_text(name="hr", datasets=[dataset("employees", fields=[field("employee_id")])],
                                    metrics=[metric("pay_plus", "payroll_total + 1")])}),
    ("delete", {}),
])
async def test_a_document_write_touching_hidden_entities_is_refused(edit: str, kwargs: dict[str, Any]) -> None:
    async with hr_engine() as engine:
        view = _hr_view(engine)
        write = {
            "save": lambda: view.save_semantic_model_text(data_source=DS, name="hr", **kwargs),
            "edit": lambda: view.edit_semantic_model("hr", data_source=DS, **kwargs),
            "delete": lambda: view.delete_semantic_model("hr", data_source=DS),
        }[edit]
        with pytest.raises(HiddenContentConflictError):
            await write()
        assert await _hr_full(engine) == HR_FULL


# --------------------------------------------------------------------------- #
# Unknown datatypes
# --------------------------------------------------------------------------- #
DECIMAL_ORDERS = TableSpec(name="orders", columns=[("order_id", "TEXT"), ("amount", "DECIMAL")],
                           rows=[("O1", 100.0), ("O2", 200.0), ("O3", 50.0), ("O4", 300.0)])
UNTYPED = document_text(datasets=[dataset("orders", fields=[field("order_id"), field("amount", datatype=None)])])


async def test_an_untyped_numeric_field_aggregates(tmp_path: Path) -> None:
    """Spec scenario: An untyped numeric field aggregates."""
    async with document_engine(documents=(UNTYPED,), tables=[DECIMAL_ORDERS], base=tmp_path) as engine:
        [_, amount] = (await must_get(engine.storage, "orders")).fields
        assert is_untyped(amount)
        assert float(await single(engine, population="orders", metric_expr="sum(orders.amount)")) == 650.0
        assert is_untyped((await must_get(engine.storage, "orders")).fields[1])
    assert document_path(tmp_path, name="sales").read_text(encoding="utf-8") == UNTYPED


async def test_an_untyped_field_loads_without_a_probe(tmp_path: Path) -> None:
    place_document(tmp_path, UNTYPED, name="sales")
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    assert is_untyped((await must_get(storage, "orders")).fields[1])
    assert not (tmp_path / "absent.duckdb").exists()


@pytest.mark.parametrize("kind", STORAGES)
async def test_inspect_shows_an_untyped_field_as_unknown(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, UNTYPED)
    shown = await mcp_text(storage, "inspect", reference="orders.amount", entity_type="field", compact=False)
    assert "Type: unknown" in shown
    dataset_view = await mcp_text(storage, "inspect", reference="orders", entity_type="dataset", compact=False)
    assert "amount" in dataset_view


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_save_time_check_on_an_untyped_field_is_deferred(kind: str) -> None:
    async with document_engine(kind=kind, documents=(UNTYPED,), tables=[DECIMAL_ORDERS]) as engine:
        orders = await must_get(engine.storage, "orders")
        avg = DatasetMetric.model_validate({"name": "avg_amount", "expression": "avg(amount)"})
        await engine.storage.save_dataset(orders.model_copy(update={"metrics": [avg]}))
        assert float(await single(engine, population="orders", metric_expr="orders.avg_amount")) == 162.5


# --------------------------------------------------------------------------- #
# Surfaces
# --------------------------------------------------------------------------- #
REGION_QUERY = {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}


async def test_native_authoring_without_an_ossie_file(tmp_path: Path) -> None:
    """Spec scenario: Native authoring without an Ossie file."""
    async with document_engine(documents=(), base=tmp_path) as engine:
        storage = engine.storage
        for model in (customers_dataset(), orders_dataset(buyer=True)):
            await storage.save_dataset(shop(model))
        before = {n: (await must_get(storage, n)).model_dump(mode="json", exclude={"version"})
                  for n in ("orders", "customers")}
        revenue = by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue")
        await mcp_text(storage, "create_semantic_model", name="sales", data_source=DS, datasets=["orders", "customers"])
        await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, metrics=[{
            "name": "revenue_per_customer",
            "expression": "AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)"}])
        for name in ("orders", "customers"):
            assert await storage.semantic_model_owner(name, data_source=DS) == "sales"
            assert not standalone_exists("yaml", tmp_path, name)
            assert (await must_get(storage, name)).model_dump(mode="json", exclude={"version"}) == before[name]
        assert by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue") == revenue
        resp = await engine.execute({"metrics": ["revenue_per_customer"]})
        assert [float(r["revenue_per_customer"]) for r in resp.data] == [162.5]


@pytest.mark.parametrize("content", [pytest.param(SALES, id="text"), pytest.param(yaml.safe_load(SALES), id="object")])
async def test_create_semantic_model_from_ossie_content(tmp_path: Path, content: Any) -> None:
    storage = make_storage("yaml", tmp_path)
    await mcp_text(storage, "create_semantic_model", name="sales", data_source=DS, content=content)
    assert await storage.list_semantic_models(data_source=DS) == [(DS, "sales")]
    assert {d.name for d in (await storage.load_datasets(data_source=DS))[0]} == {"orders", "customers"}


async def test_inspect_and_summary_show_documents_and_their_metrics() -> None:
    async with document_engine() as engine:
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.sales", entity_type="semantic_model")
        for token in ("sales", "orders", "customers", "orders_to_customers", "revenue_per_customer"):
            assert token in inspected
        summary = await mcp_text(engine.storage, "datasets_summary", datasource_name=DS)
        assert "sales" in summary
        assert "revenue_per_customer" in summary


async def test_edit_semantic_model_edits_document_fields_metrics_relationships_and_members(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES_STORES)
    await storage.save_dataset(shop(returns_dataset()).model_copy(update={"relationships": []}), _validate=False)
    await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, description="Edited.",
                   ai_context="Sales data.", remove_metrics=["revenue_per_customer"],
                   metrics=[metric("order_total", "SUM(orders.amount)")], remove_relationships=["orders_to_customers"],
                   relationships=[relationship("orders_to_stores", src="orders", to="stores",
                                               from_columns=["store_id"], to_columns=["store_id"])],
                   add_datasets=["returns"])
    doc = await doc_of(storage)
    assert (doc["description"], doc["ai_context"]) == ("Edited.", "Sales data.")
    assert [m["name"] for m in doc["metrics"]] == ["order_total"]
    assert [r["name"] for r in doc["relationships"]] == ["orders_to_stores"]
    assert "returns" in {d["name"] for d in doc["datasets"]}
    assert not standalone_exists("yaml", tmp_path, "returns")


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_failed_move_into_a_document_leaves_both_as_they_were(
        tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(shop(returns_dataset()).model_copy(update={"relationships": []}), _validate=False)
    before = await storage.get_semantic_model_text("sales", data_source=DS)

    async def refused(**_: Any) -> bool:
        raise OSError("disk full")

    monkeypatch.setattr(storage, "_delete_dataset_row", refused)
    with pytest.raises(OSError, match="disk full"):
        await storage.edit_semantic_model("sales", data_source=DS, add_datasets=["returns"])
    assert await storage.get_semantic_model_text("sales", data_source=DS) == before
    assert standalone_exists(kind, tmp_path, "returns")


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_move_cancelled_mid_write_restores_the_standalone_dataset(
        tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(shop(returns_dataset()).model_copy(update={"relationships": []}), _validate=False)

    async def cancelled(**_: Any) -> str:
        raise asyncio.CancelledError

    monkeypatch.setattr(storage, "_patch_semantic_model_text", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await storage.edit_semantic_model("sales", data_source=DS, add_datasets=["returns"])
    assert standalone_exists(kind, tmp_path, "returns")


async def test_deleting_a_yaml_document_keeps_its_lock_file(tmp_path: Path) -> None:
    """Unlinking the lock would let a waiter on the old inode and a new locker run concurrently."""
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES)
    path = document_path(tmp_path, name="sales")
    await storage.delete_semantic_model("sales", data_source=DS)
    assert not path.exists()
    assert Path(f"{path}.lock").exists()


async def test_delete_semantic_model_tool_removes_the_document(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "delete_semantic_model", name="sales", data_source=DS)
    assert await storage.list_semantic_models() == []
    assert not document_path(tmp_path, name="sales").exists()


async def test_rest_exposes_semantic_models(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    replaced = SALES.replace("description: Orders and customers.", "description: Replaced.")
    async with AsyncClient(transport=ASGITransport(app=create_app(storage=storage)), base_url="http://t") as client:
        created = await client.post("/semantic-models", json={"data_source": DS, "content": SALES})
        assert created.status_code in (200, 201), created.text
        listed = await client.get("/semantic-models", params={"data_source": DS})
        assert listed.status_code == 200
        assert "sales" in listed.text
        got = await client.get(f"/semantic-models/{DS}/sales")
        assert got.json() == {"data_source": DS, "name": "sales", "text": SALES}
        put = await client.put(f"/semantic-models/{DS}/sales", json={"content": replaced})
        assert put.status_code == 200, put.text
        assert await text_of(storage) == replaced
        assert (await client.delete(f"/semantic-models/{DS}/sales")).status_code == 200
        assert (await client.get(f"/semantic-models/{DS}/sales")).status_code == 404


async def test_the_python_client_manages_semantic_models(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    client = SlayerClient(storage=storage)
    replaced = SALES.replace("description: Orders and customers.", "description: Replaced.")
    await client.create_semantic_model(data_source=DS, content=SALES)
    assert "sales" in str(await client.list_semantic_models(data_source=DS))
    assert await client.get_semantic_model("sales", data_source=DS) == SALES
    await client.replace_semantic_model("sales", data_source=DS, content=replaced)
    assert await client.get_semantic_model("sales", data_source=DS) == replaced
    await client.delete_semantic_model("sales", data_source=DS)
    assert await storage.list_semantic_models() == []


async def test_the_remote_python_client_reports_a_missing_semantic_model(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote mode: an absent document reads as ``None`` and deletes as ``False``, as in local mode."""
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES)
    app = create_app(storage=storage)
    monkeypatch.setattr(slayer_client_module.httpx, "AsyncClient",
                        lambda **_: AsyncClient(transport=ASGITransport(app=app), base_url="http://test"))
    client = SlayerClient(url="http://test")
    assert await client.get_semantic_model("sales", data_source=DS) == SALES
    assert await client.get_semantic_model("nope", data_source=DS) is None
    assert await client.delete_semantic_model("nope", data_source=DS) is False
    assert await client.delete_semantic_model("sales", data_source=DS) is True


async def test_the_remote_python_client_encodes_a_semantic_model_name(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    name = "q?a#b%c"
    storage = make_storage("yaml", tmp_path)
    app = create_app(storage=storage)
    monkeypatch.setattr(slayer_client_module.httpx, "AsyncClient",
                        lambda **_: AsyncClient(transport=ASGITransport(app=app), base_url="http://test"))
    client = SlayerClient(url="http://test")
    text = SALES.replace("name: sales", f"name: '{name}'", 1)
    await client.create_semantic_model(data_source=DS, content=text)
    assert await client.get_semantic_model(name, data_source=DS) == text
    await client.replace_semantic_model(name, data_source=DS, content=text.replace("Orders and", "All"))
    assert "All customers" in (await client.get_semantic_model(name, data_source=DS) or "")
    assert await client.delete_semantic_model(name, data_source=DS) is True


@pytest.mark.parametrize("kind", STORAGES)
def test_the_python_clients_sync_twins_manage_semantic_models(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    client = SlayerClient(storage=storage)
    replaced = SALES.replace("description: Orders and customers.", "description: Replaced.")
    client.create_semantic_model_sync(data_source=DS, content=SALES)
    assert asyncio.run(storage.get_semantic_model_text("sales", data_source=DS)) == SALES
    assert "sales" in str(client.list_semantic_models_sync(data_source=DS))
    assert client.get_semantic_model_sync("sales", data_source=DS) == SALES
    client.replace_semantic_model_sync("sales", data_source=DS, content=replaced)
    assert asyncio.run(storage.get_semantic_model_text("sales", data_source=DS)) == replaced
    assert client.get_semantic_model_sync("sales", data_source=DS) == replaced
    client.delete_semantic_model_sync("sales", data_source=DS)
    assert asyncio.run(storage.list_semantic_models()) == []
    assert asyncio.run(storage.get_dataset("orders", data_source=DS)) is None


def test_the_cli_lists_shows_and_deletes_semantic_models(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    asyncio.run(store_document(storage, SALES))
    where = ["--storage", str(tmp_path / "store")]
    listed = run_cli_in_process(["semantic-models", "list", "--datasource", DS, *where])
    assert listed.returncode == 0, listed.stderr
    assert "sales" in listed.stdout
    shown = run_cli_in_process(["semantic-models", "show", "sales", "--datasource", DS, *where])
    assert shown.returncode == 0
    assert "# owned by finance" in shown.stdout
    deleted = run_cli_in_process(["semantic-models", "delete", "sales", "--datasource", DS, "--yes", *where])
    assert deleted.returncode == 0, deleted.stderr
    assert not document_path(tmp_path, name="sales").exists()


# --------------------------------------------------------------------------- #
# Search
# --------------------------------------------------------------------------- #
async def test_a_model_scoped_metric_is_found_by_its_synonym(tmp_path: Path) -> None:
    """Spec scenario: A model-scoped metric is found by its synonym."""
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES)
    hits = await SearchService(storage=storage).search(question="ARPU", max_results=20)
    assert f"{DS}.revenue_per_customer" in {h.id for h in hits.results}


AI_AMOUNT = document_text(datasets=[dataset("orders", primary_key=["order_id"], fields=[
    field("order_id"),
    field("amount", datatype="Float", ai_context={"instructions": "Gross order value.", "synonyms": ["revenue", "gross"]},
          nonportable_extensions=[{"vendor_name": "SNOWFLAKE", "data": {"unit": "usd"}}]),
])])


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_fields_ai_context_reaches_inspect_and_search(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A field's ai_context reaches inspect and search."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, AI_AMOUNT)
    shown = await mcp_text(storage, "inspect", reference="orders.amount", entity_type="field", compact=False)
    assert "revenue" in shown
    assert "gross" in shown
    hits = await SearchService(storage=storage).search(question="gross", max_results=20)
    assert f"{DS}.orders.amount" in {h.id for h in hits.results}
    orders = await must_get(storage, "orders")
    amount = orders.get_field("amount")
    assert amount is not None
    assert amount.meta is not None
    assert amount.meta["osi_nonportable_extensions"] == [{"vendor_name": "SNOWFLAKE", "data": {"unit": "usd"}}]
    await storage.save_dataset(orders)
    assert await text_of(storage) == AI_AMOUNT
    await storage.save_dataset(orders.model_copy(update={"description": "One row per order."}))
    assert "osi_" not in (await text_of(storage) or "")


async def test_search_indexes_documents_and_refreshes_on_edits(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES)
    service = SearchService(storage=storage)
    documents = await service.search(question="Orders and customers", max_results=50)
    assert any(h.kind == "semantic_model" and "sales" in h.id for h in documents.results)
    await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, metrics=[metric(
        "revenue_per_customer", "AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)",
        ai_context={"synonyms": ["ARPC"]})])
    refreshed = await service.search(question="ARPC", max_results=20)
    assert f"{DS}.revenue_per_customer" in {h.id for h in refreshed.results}
