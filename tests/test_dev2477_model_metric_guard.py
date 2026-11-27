"""Explicit changes never break a model-scoped metric; database-driven ones warn and leave it unloadable.

Spec: models/semantic-model-documents › Explicit changes never break a model-scoped metric.
"""

from __future__ import annotations

import contextlib
import importlib
import warnings
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.field_expression import portable_text
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.semantic_model import ossie_validity_errors
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding
from slayer.live_schema.schema_drift import UntypedFinding
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import (
    DS, SALES, STORAGES, dataset, document_text, field, make_storage, metric, relationship, store_document,
)
from tests._dev2474_fixtures import (
    FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, SHOP_ONTOLOGY, concept, empty_store, flights_store, hide, mapping_text,
    ontology_text, order_mapping, raises_refusal, refusal, rel, view,
)
from tests._engine_helpers import owned_query_engine

BREAK = "StoredDependentBreakError"
CONFLICT = "HiddenContentConflictError"
RPC = "revenue_per_customer"
ORDERS_TO_CUSTOMERS = relationship("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"],
                                   to_columns=["customer_id"])

def amounts(*, status_type: str | None = "String") -> str:
    """``orders`` without a dataset metric, and ``customers``; ``amount_total`` reads ``orders.amount``."""
    return document_text(
        datasets=[
            dataset("orders", primary_key=["order_id"], fields=[
                field("order_id"), field("customer_id"), field("amount", datatype="Float"),
                field("status", datatype=status_type), field("discount", datatype="Float")]),
            dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
        ],
        relationships=[ORDERS_TO_CUSTOMERS],
        metrics=[metric("amount_total", "SUM(orders.amount)"), metric("regions", "COUNT(DISTINCT customers.region)")],
    )


AMOUNTS = amounts()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def dependents() -> Any:
    return importlib.import_module("slayer.storage.dependents")


def unresolved_warning() -> type[Warning]:
    return dependents().StoredDependentUnresolvedWarning


@contextlib.contextmanager
def database_driven() -> Generator[None]:
    with dependents().database_driven():
        yield


async def sales_store(kind: str, base: Path, *, text: str = SALES,
                      metrics: list[dict[str, Any]] | None = None) -> StorageBackend:
    """Datasource ``shop`` holding document ``sales`` (``text``) plus the authored ``metrics``."""
    storage = await empty_store(kind, base, DS)
    await store_document(storage, text)
    if metrics:
        await storage.edit_semantic_model("sales", data_source=DS, metrics=metrics)
    return storage


async def text_of(storage: StorageBackend, name: str = "sales") -> str:
    text = await storage.get_semantic_model_text(name, data_source=DS)
    assert text is not None, name
    return text


async def must_get(storage: StorageBackend, name: str, data_source: str = DS) -> SlayerDataset:
    found = await storage.get_dataset(name, data_source=data_source)
    assert found is not None, name
    return found


def without_fields(ds: SlayerDataset, *names: str) -> SlayerDataset:
    return ds.model_copy(update={"fields": [f for f in ds.fields if f.name not in names]})


def renamed_field(ds: SlayerDataset, old: str, new: str) -> SlayerDataset:
    return ds.model_copy(update={"fields": [f.model_copy(update={"name": new}) if f.name == old else f
                                            for f in ds.fields]})


def line_naming(text: str, name: str) -> str:
    """The one refusal line about model-scoped metric ``name``."""
    [found] = {ln.strip().removeprefix("- ") for ln in text.splitlines() if f"metric '{name}'" in ln}
    return found


def metric_homes(text: str) -> tuple[list[str], list[str]]:
    """``(core, SLAYER)`` model-scoped metric names of a stored document."""
    doc = yaml.safe_load(text)
    slayer = [m["name"] for e in doc.get("nonportable_extensions") or [] if e.get("vendor_name") == "SLAYER"
              for m in (e.get("data") or {}).get("metrics") or []]
    return [m["name"] for m in doc.get("metrics") or []], slayer


async def metric_failures(storage: StorageBackend) -> dict[str, str]:
    _, errs = await storage.load_datasets(data_source=DS)
    return {e.name: e.cause_text for e in errs if e.kind == "metric"}


async def loadable_metrics(storage: StorageBackend) -> set[str]:
    return {m.name for m in await storage.list_model_scoped_metrics(DS)}


@contextlib.contextmanager
def caught() -> Generator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        yield seen


def unresolved(seen: list[warnings.WarningMessage]) -> str:
    return "\n".join(str(w.message) for w in seen if issubclass(w.category, unresolved_warning()))


# --------------------------------------------------------------------------- #
# Explicit dataset changes
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_dataset_a_metric_reads_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Deleting a dataset a metric reads is refused."""
    storage = await sales_store(kind, tmp_path)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.delete_dataset("customers", data_source=DS)
    found = refusal(info.value)
    assert RPC in found
    assert "'sales'" in found
    assert "customers" in found.replace(RPC, "")
    assert await text_of(storage) == before
    await must_get(storage, "customers")


@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_a_field_a_metric_reads_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Removing a field a metric reads is refused."""
    storage = await sales_store(kind, tmp_path, text=AMOUNTS)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.save_dataset(without_fields(await must_get(storage, "orders"), "amount"))
    found = refusal(info.value)
    assert "amount_total" in found
    assert "orders.amount" in found
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_renaming_a_field_a_metric_reads_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Renaming a field a metric reads is refused."""
    storage = await sales_store(kind, tmp_path, text=AMOUNTS)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.save_dataset(renamed_field(await must_get(storage, "orders"), "amount", "amount_usd"))
    found = refusal(info.value)
    assert "amount_total" in found
    assert "orders.amount" in found
    assert await text_of(storage) == before


def _drop_total_revenue(ds: SlayerDataset) -> SlayerDataset:
    return ds.model_copy(update={"metrics": [m for m in ds.metrics if m.name != "total_revenue"]})


def _rename_total_revenue(ds: SlayerDataset) -> SlayerDataset:
    return ds.model_copy(update={"metrics": [m.model_copy(update={"name": "revenue"}) if m.name == "total_revenue"
                                             else m for m in ds.metrics]})


@pytest.mark.parametrize("change", [_drop_total_revenue, _rename_total_revenue], ids=["remove", "rename"])
@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_a_dataset_metric_a_metric_evaluates_is_refused(
        tmp_path: Path, kind: str, change: Callable[[SlayerDataset], SlayerDataset]) -> None:
    """Spec scenario: Removing a dataset metric a metric evaluates is refused."""
    storage = await sales_store(kind, tmp_path)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.save_dataset(change(await must_get(storage, "orders")))
    found = refusal(info.value)
    assert RPC in found
    assert "orders.total_revenue" in found
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_replacing_a_read_field_by_a_dataset_metric_is_refused(tmp_path: Path, kind: str) -> None:
    """A field read inside an aggregate does not resolve to a dataset metric of the same name."""
    storage = await sales_store(kind, tmp_path, text=AMOUNTS)
    before = await text_of(storage)
    orders = without_fields(await must_get(storage, "orders"), "amount")
    replaced = SlayerDataset.model_validate({**orders.model_dump(), "metrics": [
        *orders.model_dump()["metrics"], {"name": "amount", "expression": "SUM(discount)"}]})
    with raises_refusal(BREAK) as info:
        await storage.save_dataset(replaced)
    assert "orders.amount" in refusal(info.value)
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_unvalidated_save_is_guarded_too(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, text=AMOUNTS)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.save_dataset(without_fields(await must_get(storage, "orders"), "amount"), _validate=False)
    assert "amount_total" in refusal(info.value)
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_a_slayer_only_field_a_metric_reads_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Removing a SLayer-only field a metric reads is refused."""
    storage = await sales_store(kind, tmp_path)
    orders = await must_get(storage, "orders")
    await storage.save_dataset_extensions(orders.model_copy(update={"fields": [
        *orders.fields, DatasetField(name="net", expression="amount * 0.9", datatype=DataType.FLOAT)]}))
    await storage.edit_semantic_model("sales", data_source=DS, metrics=[{"name": "net_total",
                                                                         "expression": "SUM(orders.net)"}])
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.save_dataset_extensions(without_fields(await must_get(storage, "orders"), "net"))
    found = refusal(info.value)
    assert "net_total" in found
    assert "orders.net" in found
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_change_keeping_every_metric_resolving_applies(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, metrics=[{"name": "doubled", "expression": f"{RPC} * 2"}])
    await storage.save_dataset((await must_get(storage, "orders")).model_copy(update={"description": "Orders."}))
    assert (await must_get(storage, "orders")).description == "Orders."
    assert await loadable_metrics(storage) == {RPC, "doubled"}


@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_the_metric_first_lets_the_delete_through(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path)
    await storage.edit_semantic_model("sales", data_source=DS, remove_metrics=[RPC])
    assert await storage.delete_dataset("customers", data_source=DS) is True
    assert await storage.get_dataset("customers", data_source=DS) is None


# --------------------------------------------------------------------------- #
# Metrics reading metrics
# --------------------------------------------------------------------------- #
DOUBLED = [{"name": "doubled", "expression": f"{RPC} * 2"}]


@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_a_metric_another_metric_reads_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Removing a metric another metric reads is refused."""
    storage = await sales_store(kind, tmp_path, metrics=DOUBLED)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.edit_semantic_model("sales", data_source=DS, remove_metrics=[RPC])
    found = refusal(info.value)
    assert "doubled" in found
    assert RPC in found
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_replacing_a_metric_another_metric_reads_applies(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, metrics=DOUBLED)
    await storage.edit_semantic_model("sales", data_source=DS, metrics=[
        {"name": RPC, "expression": "AGG(orders.total_revenue)"}])
    [replaced] = [m for m in await storage.list_model_scoped_metrics(DS) if m.name == RPC]
    assert portable_text(replaced.expression) == "AGG(orders.total_revenue)"
    assert await loadable_metrics(storage) == {RPC, "doubled"}
    assert await metric_failures(storage) == {}


@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_a_metric_read_inside_an_aggregate_is_refused(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, text=AMOUNTS, metrics=[
        {"name": "factor", "expression": "1.1"}, {"name": "scaled", "expression": "SUM(orders.amount * factor)"}])
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.edit_semantic_model("sales", data_source=DS, remove_metrics=["factor"])
    assert "factor" in line_naming(refusal(info.value), "scaled")
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dependency_chain_is_named(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A dependency chain is named."""
    storage = await sales_store(kind, tmp_path, metrics=DOUBLED)
    with raises_refusal(BREAK) as info:
        await storage.delete_dataset("customers", data_source=DS)
    found = refusal(info.value)
    assert "customers" in line_naming(found, RPC).replace(RPC, "")
    chain = line_naming(found, "doubled")
    assert RPC in chain
    assert "customers" in chain.replace(RPC, "")


HEADS = document_text(name="heads", datasets=[
    dataset("staff", primary_key=["employee_id"], fields=[field("employee_id"), field("head_count", datatype="Integer")])],
    metrics=[metric("heads", "SUM(staff.head_count)")])


async def _cross_document(kind: str, base: Path) -> StorageBackend:
    """``sales`` plus document ``heads``; ``per_head`` of ``sales`` reads ``heads`` of ``heads``."""
    storage = await sales_store(kind, base)
    await store_document(storage, HEADS)
    await storage.edit_semantic_model("sales", data_source=DS, metrics=[
        {"name": "per_head", "expression": f"{RPC} / heads"}])
    return storage


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_document_whose_metric_another_reads_is_refused(tmp_path: Path, kind: str) -> None:
    storage = await _cross_document(kind, tmp_path)
    with raises_refusal(BREAK) as info:
        await storage.delete_semantic_model("heads", data_source=DS)
    found = refusal(info.value)
    assert "per_head" in found
    assert "'heads'" in found
    assert await storage.list_semantic_models(DS) == [(DS, "heads"), (DS, "sales")]


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_documents_last_dataset_judges_the_documents_deletion(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path)
    await store_document(storage, document_text(name="heads", datasets=[
        dataset("staff", primary_key=["employee_id"], fields=[field("employee_id")])]))
    await storage.edit_semantic_model("heads", data_source=DS, metrics=[{"name": "twice", "expression": f"{RPC} * 2"}])
    await storage.edit_semantic_model("sales", data_source=DS, metrics=[{"name": "thrice", "expression": "twice + 1"}])
    with raises_refusal(BREAK) as info:
        await storage.delete_dataset("staff", data_source=DS)
    assert "thrice" in refusal(info.value)
    assert await storage.list_semantic_models(DS) == [(DS, "heads"), (DS, "sales")]


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_document_removes_its_unloadable_metrics_embeddings(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, text=document_text(
        datasets=[dataset("orders", primary_key=["order_id"], fields=[field("order_id")])],
        metrics=[metric("ghost", "SUM(orders.nope)")]))
    assert "ghost" in await metric_failures(storage)
    model = embedding_client.current_model()
    await storage.save_embeddings([Embedding(canonical_id=f"{DS}.ghost", embedding_model_name=model,
                                             entity_kind="metric", content_hash="h", embedding=[1.0, 0.0])])
    assert await storage.delete_semantic_model("sales", data_source=DS) is True
    assert await storage.list_embeddings(embedding_model_name=model) == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_documents_last_dataset_removes_its_metrics_embeddings(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path)
    await store_document(storage, HEADS)
    model = embedding_client.current_model()
    await storage.save_embeddings([Embedding(canonical_id=f"{DS}.heads", embedding_model_name=model,
                                             entity_kind="metric", content_hash="h", embedding=[1.0, 0.0])])
    assert await storage.delete_dataset("staff", data_source=DS) is True
    assert await storage.list_semantic_models(DS) == [(DS, "sales")]
    assert await storage.list_embeddings(embedding_model_name=model) == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_metric_reading_a_metric_a_database_driven_change_removed_is_unloadable(
        tmp_path: Path, kind: str) -> None:
    storage = await _cross_document(kind, tmp_path)
    with caught() as seen, database_driven():
        await storage.delete_dataset("staff", data_source=DS)
    assert "per_head" in unresolved(seen)
    assert "per_head" not in await loadable_metrics(storage)
    assert "heads" in (await metric_failures(storage))["per_head"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_removing_a_metric_another_document_reads_is_refused(tmp_path: Path, kind: str) -> None:
    storage = await _cross_document(kind, tmp_path)
    before = await text_of(storage, "heads")
    with raises_refusal(BREAK) as info:
        await storage.edit_semantic_model("heads", data_source=DS, remove_metrics=["heads"])
    assert "per_head" in refusal(info.value)
    assert await text_of(storage, "heads") == before


# --------------------------------------------------------------------------- #
# Whole-document writes
# --------------------------------------------------------------------------- #
def _sales_without_customers() -> str:
    doc = yaml.safe_load(SALES)
    doc["datasets"] = [d for d in doc["datasets"] if d["name"] != "customers"]
    doc.pop("relationships")
    return yaml.safe_dump(doc, sort_keys=False)


async def _save_text(storage: StorageBackend, text: str) -> None:
    await storage.save_semantic_model_text(data_source=DS, name="sales", text=text)


async def _create(storage: StorageBackend, text: str) -> None:
    await storage.create_semantic_model(data_source=DS, name="sales", content=text)


async def _adopt(storage: StorageBackend, text: str) -> None:
    await storage.adopt_semantic_model(data_source=DS, name="sales", text=text, replacing=[])


@pytest.mark.parametrize("write", [_save_text, _create, _adopt], ids=["save-text", "create", "adopt"])
@pytest.mark.parametrize("kind", STORAGES)
async def test_replacing_a_documents_text_without_a_read_dataset_is_refused(
        tmp_path: Path, kind: str, write: Callable[[StorageBackend, str], Awaitable[None]]) -> None:
    """Spec scenario: Replacing a document's text without a read dataset is refused."""
    storage = await sales_store(kind, tmp_path)
    with raises_refusal(BREAK) as info:
        await write(storage, _sales_without_customers())
    assert RPC in refusal(info.value)
    assert await text_of(storage) == SALES


# --------------------------------------------------------------------------- #
# One refusal for every stored dependent
# --------------------------------------------------------------------------- #
CUSTOMER_ONTOLOGY = ontology_text(
    concept("CustomerId", "ValueType", extends=["String"]),
    concept("Region", "ValueType", extends=["String"]),
    concept("Customer", identify_by=["id"], relationships=[rel("id", "CustomerId"), rel("region", "Region")]),
)
CUSTOMER_MAPPING = mapping_text(
    {"concept": "Customer",
     "object_mappings": [{"referent_mappings": [{"relationship": "id", "expression": "customers.customer_id"}]}],
     "link_mappings": [{"object_mapping": {"referent_mappings": [
         {"relationship": "id", "expression": "customers.customer_id"}]},
         "children": [{"relationship": "region", "object_mapping": {"concept": "Region",
                                                                   "expression": "customers.region"}}]}]},
    semantic_model="sales")


@pytest.mark.parametrize("kind", STORAGES)
async def test_one_refusal_lists_a_mapping_and_a_metric(tmp_path: Path, kind: str) -> None:
    """Spec scenario: One refusal lists a mapping and a metric."""
    storage = await sales_store(kind, tmp_path)
    await storage.save_ontology_text(name="shop", text=CUSTOMER_ONTOLOGY)
    await storage.save_ontology_mapping_text(data_source=DS, name="shop_mapping", text=CUSTOMER_MAPPING)
    with raises_refusal(BREAK) as info:
        await storage.delete_dataset("customers", data_source=DS)
    found = refusal(info.value)
    assert "shop_mapping" in found
    assert RPC in found
    await must_get(storage, "customers")


# --------------------------------------------------------------------------- #
# References already unresolved
# --------------------------------------------------------------------------- #
DISCOUNTED = [{"name": "discounted", "expression": "SUM(orders.amount) - SUM(orders.discount)"}]


async def _partly_unresolved(kind: str, base: Path) -> StorageBackend:
    """``discounted`` reads ``orders.amount`` and ``orders.discount``, which a database-driven save dropped."""
    storage = await sales_store(kind, base, text=AMOUNTS, metrics=DISCOUNTED)
    with caught(), database_driven():
        await storage.save_dataset(without_fields(await must_get(storage, "orders"), "discount"))
    assert (await must_get(storage, "orders")).get_field("discount") is None
    return storage


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_metric_already_unresolved_blocks_nothing_new(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A metric already unresolved blocks nothing new."""
    storage = await _partly_unresolved(kind, tmp_path)
    orders = await must_get(storage, "orders")
    await storage.save_dataset(orders.model_copy(update={"fields": [
        f.model_copy(update={"description": "Order status."}) if f.name == "status" else f for f in orders.fields]}))
    status = (await must_get(storage, "orders")).get_field("status")
    assert status is not None
    assert status.description == "Order status."


@pytest.mark.parametrize("kind", STORAGES)
async def test_restoring_a_missing_dataset_without_a_read_field_applies(tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, text=AMOUNTS)
    customers = await must_get(storage, "customers")
    with caught(), database_driven():
        await storage.delete_dataset("customers", data_source=DS)
    await storage.save_dataset(without_fields(customers, "region"))
    assert (await must_get(storage, "customers")).get_field("region") is None
    assert "customers.region" in (await metric_failures(storage))["regions"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_making_a_metric_read_an_unresolved_metric_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Making a metric read an unresolved metric is refused."""
    storage = await _partly_unresolved(kind, tmp_path)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.edit_semantic_model("sales", data_source=DS, metrics=[
            {"name": "amount_total", "expression": "discounted * 2"}])
    chain = line_naming(refusal(info.value), "amount_total")
    assert "discounted → orders.discount" in chain
    assert await text_of(storage) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_switching_between_metrics_with_the_same_unresolved_reference_applies(
        tmp_path: Path, kind: str) -> None:
    storage = await sales_store(kind, tmp_path, text=AMOUNTS, metrics=[
        *DISCOUNTED, {"name": "discount_total", "expression": "SUM(orders.discount)"},
        {"name": "reader", "expression": "discounted * 2"}])
    with caught(), database_driven():
        await storage.save_dataset(without_fields(await must_get(storage, "orders"), "discount"))
    await storage.edit_semantic_model("sales", data_source=DS, metrics=[
        {"name": "reader", "expression": "discount_total * 2"}])
    assert "reader" not in await loadable_metrics(storage)
    assert "discount_total" in (await metric_failures(storage))["reader"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_further_break_of_a_partly_unresolved_metric_is_refused(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A further break of a partly unresolved metric is refused."""
    storage = await _partly_unresolved(kind, tmp_path)
    before = await text_of(storage)
    with raises_refusal(BREAK) as info:
        await storage.save_dataset(without_fields(await must_get(storage, "orders"), "amount"))
    found = refusal(info.value)
    assert "discounted" in found
    assert "orders.amount" in found
    assert "discount'" not in found.replace("discounted", "")
    assert "orders.discount" not in found
    assert await text_of(storage) == before


# --------------------------------------------------------------------------- #
# Moves
# --------------------------------------------------------------------------- #
async def test_a_refused_move_writes_nothing(tmp_path: Path) -> None:
    """Spec scenario: A refused move writes nothing."""
    storage = await sales_store("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name="other", type="duckdb", database=str(tmp_path / "o.duckdb")))
    before = await text_of(storage)
    reply = await mcp_text(storage, "edit_dataset", dataset_name="customers", data_source=DS, new_data_source="other")
    assert RPC in reply, reply
    assert '"success": true' not in reply, reply
    assert await storage.get_dataset("customers", data_source="other") is None
    await must_get(storage, "customers")
    assert await text_of(storage) == before


def _by_order(population: str) -> SlayerDataset:
    return SlayerDataset.model_validate({"name": "by_order", "data_source": DS, "source_queries": [
        {"population": population, "dimensions": [f"{population}.order_id"],
         "metrics": [{"metric": f"sum({population}.amount)", "alias": "revenue"}]}]})


async def test_a_refused_query_backed_move_writes_nothing(tmp_path: Path) -> None:
    """The engine moves a query-backed dataset whose source now reads another datasource; a stored dependent refuses."""
    database = tmp_path / "shop.duckdb"
    with contextlib.closing(duckdb.connect(str(database))) as con:
        con.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id VARCHAR, amount DOUBLE)")
        con.execute("CREATE TABLE customers (customer_id VARCHAR PRIMARY KEY)")
        con.execute("INSERT INTO orders VALUES (1, 'a', 10.0), (2, 'b', 20.0)")
    storage = make_storage("yaml", tmp_path)
    shop = DatasourceConfig(name=DS, type="duckdb", database=str(database))
    other = DatasourceConfig(name="other", type="duckdb", database=str(database))
    async with owned_query_engine(storage=storage, datasources=[shop, other]) as engine:
        await engine.ingest_idempotent(shop)
        await storage.save_dataset(SlayerDataset.model_validate({
            "name": "lines", "sql_table": "orders", "data_source": "other",
            "fields": [{"name": "order_id", "primary_key": True, "datatype": "Integer"},
                       {"name": "amount", "datatype": "Float"}]}))
        await engine.save_dataset(_by_order("orders"))
        await storage.save_ontology_text(name="shop", text=SHOP_ONTOLOGY)
        await storage.save_ontology_mapping_text(data_source=DS, name="shop_mapping",
                                                 text=order_mapping(semantic_model=DS, amount="by_order.revenue"))
        with raises_refusal(BREAK) as info:
            await engine.save_dataset(_by_order("lines"))
    assert "shop_mapping" in refusal(info.value)
    assert await storage.get_dataset("by_order", data_source="other") is None
    await must_get(storage, "by_order")


# --------------------------------------------------------------------------- #
# Hidden content
# --------------------------------------------------------------------------- #
HR = document_text(name="hr", datasets=[
    dataset("payroll", primary_key=["employee_id"], fields=[field("employee_id"), field("salary", datatype="Float")]),
    dataset("staff", primary_key=["employee_id"], fields=[field("employee_id"), field("head_count", datatype="Integer")]),
], metrics=[metric("cost_per_head", "SUM(payroll.salary) / SUM(staff.head_count)"),
            metric("heads", "SUM(staff.head_count)")])
HIDDEN_NAMES = ("cost_per_head", "salary_per_head", "payroll", "salary")


async def _hidden_payroll(kind: str, base: Path) -> StorageBackend:
    """Document ``hr``; ``payroll`` is hidden from a caller without ``secret``, and with it every metric reading it."""
    storage = await empty_store(kind, base, DS)
    await store_document(storage, HR)
    await storage.edit_semantic_model("hr", data_source=DS, metrics=[
        {"name": "salary_per_head", "expression": "SUM(payroll.salary) / heads"}])
    await hide(storage, "payroll", data_source=DS)
    return storage


async def _hidden_delete(storage: StorageBackend) -> None:
    await storage.delete_dataset("staff", data_source=DS)


async def _hidden_save(storage: StorageBackend) -> None:
    await storage.save_dataset(without_fields(await must_get(storage, "staff"), "head_count"))


async def _hidden_edit(storage: StorageBackend) -> None:
    await storage.edit_semantic_model("hr", data_source=DS, remove_metrics=["heads"])


async def _hidden_text_save(storage: StorageBackend) -> None:
    doc = yaml.safe_load(await text_of(storage, "hr"))
    doc["datasets"] = [d for d in doc["datasets"] if d["name"] != "staff"]
    doc["metrics"] = [m for m in doc.get("metrics") or [] if m["name"] != "heads"] or None
    await storage.save_semantic_model_text(data_source=DS, name="hr", text=yaml.safe_dump(
        {k: v for k, v in doc.items() if v is not None}, sort_keys=False))


@pytest.mark.parametrize("change", [_hidden_delete, _hidden_save, _hidden_edit, _hidden_text_save],
                         ids=["delete-dataset", "save-dataset", "edit-document", "save-document-text"])
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_break_of_a_hidden_metric_is_a_hidden_content_conflict(
        tmp_path: Path, kind: str, change: Callable[[StorageBackend], Awaitable[None]]) -> None:
    """Spec scenario: A break of a hidden metric is a hidden-content conflict."""
    storage = await _hidden_payroll(kind, tmp_path)
    before = await text_of(storage, "hr")
    with raises_refusal(CONFLICT) as info:
        await change(view(storage))
    found = refusal(info.value)
    assert [n for n in HIDDEN_NAMES if n in found] == []
    assert await text_of(storage, "hr") == before
    await must_get(storage, "staff")


async def test_a_hidden_metric_broken_by_a_document_delete_is_a_hidden_content_conflict(tmp_path: Path) -> None:
    storage = await empty_store("yaml", tmp_path, DS)
    await store_document(storage, HEADS)
    await store_document(storage, document_text(name="hr", datasets=[dataset(
        "payroll", primary_key=["employee_id"], fields=[field("employee_id"), field("salary", datatype="Float")])]))
    await storage.edit_semantic_model("hr", data_source=DS, metrics=[
        {"name": "salary_per_head", "expression": "SUM(payroll.salary) / heads"}])
    await hide(storage, "payroll", data_source=DS)
    with raises_refusal(CONFLICT) as info:
        await view(storage).delete_semantic_model("heads", data_source=DS)
    assert [n for n in HIDDEN_NAMES if n in refusal(info.value)] == []
    assert await storage.list_semantic_models(DS) == [(DS, "heads"), (DS, "hr")]


async def test_a_caller_who_sees_the_metric_gets_the_break_itself(tmp_path: Path) -> None:
    storage = await _hidden_payroll("yaml", tmp_path)
    with raises_refusal(BREAK) as info:
        await view(storage, "secret").delete_dataset("staff", data_source=DS)
    assert "cost_per_head" in refusal(info.value)


# --------------------------------------------------------------------------- #
# Database-driven changes
# --------------------------------------------------------------------------- #
def _live_shop(base: Path, *, without: str) -> Path:
    """A DuckDB holding the ``AMOUNTS`` tables, without column ``orders.amount`` or table ``customers``."""
    database = base / "shop.duckdb"
    with contextlib.closing(duckdb.connect(str(database))) as con:
        amount = "" if without == "amount" else ", amount DOUBLE"
        con.execute(f"CREATE TABLE orders (order_id VARCHAR, customer_id VARCHAR, status VARCHAR, "
                    f"discount DOUBLE{amount})")
        if without != "customers":
            con.execute("CREATE TABLE customers (customer_id VARCHAR, region VARCHAR)")
    return database


async def _drift_cleanup(storage: StorageBackend, database: Path) -> str:
    """Run the drift cleanup over datasource ``shop`` at ``database``; the unresolved-dependent warnings it gave."""
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(database)))
    async with owned_query_engine(storage=storage) as engine:
        entries = await engine.validate_datasets(data_source=DS)
        with caught() as seen:
            await engine.apply_drift_deletes(entries)
    return unresolved(seen)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_drift_cleanup_that_drops_a_field_applies_with_a_warning(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A drift cleanup that drops a field applies with a warning."""
    storage = await sales_store(kind, tmp_path, text=AMOUNTS,
                                metrics=[{"name": "twice", "expression": "amount_total * 2"}])
    assert metric_homes(await text_of(storage))[0] == ["amount_total", "regions"]
    warned = await _drift_cleanup(storage, _live_shop(tmp_path, without="amount"))
    assert "amount_total" in warned
    assert "twice" in warned
    assert "orders.amount" in warned

    reopened = make_storage(kind, tmp_path)
    assert (await must_get(reopened, "orders")).get_field("amount") is None
    text = await text_of(reopened)
    core, slayer = metric_homes(text)
    assert core == ["regions"]
    assert {"amount_total", "twice"} <= set(slayer)
    assert ossie_validity_errors(yaml.safe_load(text)) == []
    failures = await metric_failures(reopened)
    assert "orders.amount" in failures["amount_total"]
    assert "amount_total" in failures["twice"]
    assert "orders.amount" in failures["twice"]
    assert await loadable_metrics(reopened) == {"regions"}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_drift_cleanup_that_drops_a_dataset_applies_with_a_warning(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A drift cleanup that drops a dataset applies with a warning."""
    storage = await sales_store(kind, tmp_path)
    database = tmp_path / "shop.duckdb"
    with contextlib.closing(duckdb.connect(str(database))) as con:
        con.execute("CREATE TABLE orders (order_id VARCHAR, customer_id VARCHAR, amount DOUBLE, status VARCHAR)")
    warned = await _drift_cleanup(storage, database)
    assert RPC in warned
    assert "customers" in warned.replace(RPC, "")

    reopened = make_storage(kind, tmp_path)
    assert await reopened.get_dataset("customers", data_source=DS) is None
    assert "customers" in (await metric_failures(reopened))[RPC]
    loaded, _ = await reopened.load_datasets(data_source=DS)
    assert [d.name for d in loaded] == ["orders"]
    assert ossie_validity_errors(yaml.safe_load(await text_of(reopened))) == []


@pytest.mark.parametrize("change", [_drop_total_revenue, _rename_total_revenue], ids=["remove", "rename"])
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_database_driven_dataset_metric_removal_applies_with_a_warning(
        tmp_path: Path, kind: str, change: Callable[[SlayerDataset], SlayerDataset]) -> None:
    storage = await sales_store(kind, tmp_path, metrics=DOUBLED)
    with caught() as seen, database_driven():
        await storage.save_dataset(change(await must_get(storage, "orders")))
    warned = unresolved(seen)
    assert RPC in warned
    assert "doubled" in warned
    assert "orders.total_revenue" in warned
    text = await text_of(storage)
    assert metric_homes(text)[0] == []
    assert {RPC, "doubled"} <= set(metric_homes(text)[1])
    assert ossie_validity_errors(yaml.safe_load(text)) == []
    failures = await metric_failures(storage)
    assert "orders.total_revenue" in failures[RPC]
    assert RPC in failures["doubled"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_unloadable_metric_is_never_written_back_on_load(
        tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = await _partly_unresolved(kind, tmp_path)
    text = await text_of(storage)
    writes: list[str] = []

    async def spy(*_args: Any, **_kwargs: Any) -> None:
        writes.append("write")

    for hook in ("_save_semantic_model_text_impl", "_patch_semantic_model_text", "_delete_semantic_model_row"):
        monkeypatch.setattr(storage, hook, spy)
    assert "orders.discount" in (await metric_failures(storage))["discounted"]
    assert "discounted" not in await loadable_metrics(storage)
    assert writes == []
    assert await text_of(storage) == text


async def test_an_unloadable_metric_keeps_its_name(tmp_path: Path) -> None:
    """Another document cannot reuse the name of a stored metric that does not load."""
    storage = await _partly_unresolved("yaml", tmp_path)
    await store_document(storage, HEADS)
    with raises_refusal("NamespaceCollisionError"):
        await storage.edit_semantic_model("heads", data_source=DS, metrics=[
            {"name": "discounted", "expression": "SUM(staff.head_count)"}])


async def test_an_untyped_finding_applies_beside_an_unresolved_metric(tmp_path: Path) -> None:
    """An untyped-finding write only sets a datatype, so it never removes a reference a metric reads."""
    storage = await sales_store("yaml", tmp_path, text=amounts(status_type=None), metrics=DISCOUNTED)
    with caught(), database_driven():
        await storage.save_dataset(without_fields(await must_get(storage, "orders"), "discount"))
    finding = UntypedFinding(dataset_name="orders", data_source=DS, semantic_model="sales", field="status",
                             live_type=DataType.STRING)
    async with owned_query_engine(storage=storage) as engine:
        with caught() as seen:
            assert await engine.apply_untyped_findings([finding]) == []
    status = (await must_get(storage, "orders")).get_field("status")
    assert status is not None
    assert status.datatype == DataType.STRING
    assert "discounted" not in unresolved(seen)


async def test_a_reingest_leaves_metrics_resolving(tmp_path: Path) -> None:
    """Ingest only adds and widens, so it never removes a reference a metric reads."""
    database = tmp_path / "shop.duckdb"
    with contextlib.closing(duckdb.connect(str(database))) as con:
        con.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id VARCHAR, amount DOUBLE)")
    config = DatasourceConfig(name=DS, type="duckdb", database=str(database))
    storage = await empty_store("yaml", tmp_path)
    async with owned_query_engine(storage=storage, datasources=[config]) as engine:
        await engine.ingest_idempotent(config)
    await storage.create_semantic_model(data_source=DS, name="sales", datasets=["orders"])
    await storage.edit_semantic_model("sales", data_source=DS, metrics=[{"name": "amount_total",
                                                                         "expression": "SUM(orders.amount)"}])
    with contextlib.closing(duckdb.connect(str(database))) as con:
        con.execute("ALTER TABLE orders ALTER COLUMN amount TYPE VARCHAR")
    with caught() as seen:
        async with owned_query_engine(storage=storage, datasources=[config]) as engine:
            await engine.ingest_idempotent(config)
    assert not [w for w in seen if "amount_total" in str(w.message)]
    assert await loadable_metrics(storage) == {"amount_total"}


# --------------------------------------------------------------------------- #
# Surfaces
# --------------------------------------------------------------------------- #
def _rest(storage: StorageBackend) -> TestClient:
    return TestClient(create_app(storage=storage), raise_server_exceptions=False)


async def test_rest_reports_a_refused_delete(tmp_path: Path) -> None:
    """Spec scenario: REST reports a refused delete."""
    storage = await sales_store("yaml", tmp_path)
    with _rest(storage) as client:
        response = client.delete("/datasets/customers", params={"data_source": DS})
    assert response.status_code == 400, response.text
    assert RPC in response.text
    await must_get(storage, "customers")


async def test_rest_reports_a_refused_semantic_model_delete(tmp_path: Path) -> None:
    storage = await _cross_document("yaml", tmp_path)
    with _rest(storage) as client:
        response = client.delete(f"/semantic-models/{DS}/heads")
    assert response.status_code == 400, response.text
    assert "per_head" in response.text
    assert (DS, "heads") in await storage.list_semantic_models(DS)


async def test_rest_reports_a_mapping_break_on_delete(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    with _rest(storage) as client:
        dataset_delete = client.delete("/datasets/FLIGHT", params={"data_source": FLIGHTS_DS})
        model_delete = client.delete(f"/semantic-models/{FLIGHTS_DS}/{FLIGHTS_MODEL}")
    for response in (dataset_delete, model_delete):
        assert response.status_code == 400, response.text
        assert FLIGHTS_MAPPING in response.text


async def test_rest_reports_a_hidden_content_conflict_on_delete(tmp_path: Path) -> None:
    storage = await _hidden_payroll("yaml", tmp_path)
    with _rest(view(storage)) as client:
        response = client.delete("/datasets/staff", params={"data_source": DS})
    assert response.status_code == 400, response.text
    assert [n for n in HIDDEN_NAMES if n in response.text] == []
    await must_get(storage, "staff")


async def test_mcp_reports_refusals(tmp_path: Path) -> None:
    storage = await _cross_document("yaml", tmp_path)
    await storage.edit_semantic_model("sales", data_source=DS, metrics=DOUBLED)
    before = {name: await text_of(storage, name) for name in ("sales", "heads")}
    replies = {
        "delete_dataset": await mcp_text(storage, "delete_dataset", name="customers", data_source=DS),
        "delete_semantic_model": await mcp_text(storage, "delete_semantic_model", name="heads", data_source=DS),
        "edit_dataset": await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS,
                                       remove={"metrics": ["total_revenue"]}),
        "edit_semantic_model": await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS,
                                              remove_metrics=[RPC]),
    }
    expected = {"delete_dataset": (RPC, "doubled", "per_head"), "delete_semantic_model": ("per_head",),
                "edit_dataset": (RPC, "doubled", "per_head"), "edit_semantic_model": ("doubled", "per_head")}
    for tool, reply in replies.items():
        assert BREAK in reply, (tool, reply)
        assert [n for n in expected[tool] if n not in reply] == [], (tool, reply)
    assert {name: await text_of(storage, name) for name in ("sales", "heads")} == before


def _cli(*args: str) -> Any:
    return run_cli_in_process(list(args))


def _error_lines(result: Any) -> str:
    return "\n".join(line for line in result.stderr.splitlines() if line.startswith("Error:"))


async def test_cli_reports_refused_deletes(tmp_path: Path) -> None:
    storage = await _cross_document("yaml", tmp_path)
    store = str(tmp_path / "store")
    dataset_delete = _cli("datasets", "--storage", store, "delete", "customers")
    model_delete = _cli("semantic-models", "delete", "heads", "--datasource", DS, "--yes", "--storage", store)
    assert dataset_delete.returncode != 0
    assert RPC in _error_lines(dataset_delete), dataset_delete.stderr
    assert model_delete.returncode != 0
    assert "per_head" in _error_lines(model_delete), model_delete.stderr
    await must_get(storage, "customers")
    assert (DS, "heads") in await storage.list_semantic_models(DS)
