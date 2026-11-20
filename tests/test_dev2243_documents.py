"""Spec: models/semantic-model-documents — storage, shape, version, load, decomposition, relationships, metrics,
unevaluable entities and converter-construct parity."""

from __future__ import annotations

import contextlib
import json
import sqlite3
import statistics
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from slayer.core.enums import JoinType, RelationshipCardinality
from slayer.core.errors import UnknownReferenceError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from tests._dev1737_fixtures import TableSpec
from tests._dev2243_fixtures import (
    DS, SALES, STORAGES, dataset, document_engine, document_path, document_text, error, field, make_storage, metric,
    place_document, relationship, store_document,
)
from tests._dev2277_fixtures import by, managers_dataset, returns_dataset
from tests._engine_helpers import owned_query_engine
from tests._dataset_access_fixtures import mcp_text


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def snapshot(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` with its bytes."""
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


async def yaml_storage_with_datasource(base: Path) -> StorageBackend:
    """YAML storage at ``base`` with datasource ``shop`` registered (its database never created)."""
    storage = make_storage("yaml", base)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(base / "absent.duckdb")))
    return storage


def failed(errs: Sequence[Any], kind: str) -> dict[str, str]:
    """``{name: cause_text}`` of the load failures of ``kind``."""
    return {e.name: e.cause_text for e in errs if e.kind == kind}


async def single(engine: SlayerQueryEngine, *, population: str, metric_expr: str) -> Any:
    resp = await engine.execute({"population": population, "metrics": [{"metric": metric_expr, "alias": "v"}]})
    assert len(resp.data) == 1
    return resp.data[0]["v"]


def plain(value: Any) -> Any:
    return value.model_dump(mode="json", exclude_none=True) if hasattr(value, "model_dump") else value


ORDERS_ONLY = document_text(datasets=[dataset("orders", fields=[field("order_id"), field("amount", datatype="Float")])])


# --------------------------------------------------------------------------- #
# Storage binding
# --------------------------------------------------------------------------- #
async def test_a_document_file_binds_to_its_directorys_datasource(tmp_path: Path) -> None:
    """Spec scenario: A document file binds to its directory's datasource."""
    place_document(tmp_path, SALES, name="sales")
    storage = await yaml_storage_with_datasource(tmp_path)
    assert await storage.list_datasets(data_source=DS) == ["customers", "orders"]
    assert await storage.list_semantic_models(data_source=DS) == [(DS, "sales")]
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    assert orders.data_source == DS


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_stored_document_is_listable_and_readable_as_its_text(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    assert await storage.list_semantic_models() == [(DS, "sales")]
    assert await storage.get_semantic_model_text("sales", data_source=DS) == SALES
    assert {n for n in await storage.list_datasets(data_source=DS)} == {"orders", "customers"}


async def test_sqlite_stores_the_text_verbatim(tmp_path: Path) -> None:
    """Spec scenario: SQLite stores the text verbatim."""
    storage = make_storage("sqlite", tmp_path)
    await store_document(storage, SALES)
    assert "# owned by finance" in SALES
    assert await storage.get_semantic_model_text("sales", data_source=DS) == SALES
    with contextlib.closing(sqlite3.connect(tmp_path / "store.db")) as con:
        [(text,)] = con.execute("SELECT text FROM semantic_models WHERE data_source = ? AND name = ?", (DS, "sales"))
    assert text == SALES


async def test_yaml_storage_keeps_the_document_at_its_layout_path(tmp_path: Path) -> None:
    storage = make_storage("yaml", tmp_path)
    await store_document(storage, SALES)
    assert document_path(tmp_path, name="sales").read_text(encoding="utf-8") == SALES
    assert not (tmp_path / "store" / "models" / DS / "orders.yaml").exists()


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_documents_name_must_equal_its_storage_name(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises((error("OssieDocumentInvalidError"), ValueError)):
        await storage.save_semantic_model_text(data_source=DS, name="other", text=SALES)
    assert await storage.list_semantic_models() == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_document_is_replaceable_and_deletable(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    replaced = document_text(datasets=[dataset("orders", fields=[field("order_id"), field("status")])])
    await store_document(storage, replaced)
    assert await storage.get_semantic_model_text("sales", data_source=DS) == replaced
    assert await storage.get_dataset("customers", data_source=DS) is None
    assert await storage.delete_semantic_model("sales", data_source=DS) is True
    assert await storage.list_semantic_models() == []
    assert await storage.get_semantic_model_text("sales", data_source=DS) is None
    assert await storage.get_dataset("orders", data_source=DS) is None
    assert await storage.delete_semantic_model("sales", data_source=DS) is False


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("data_source", "name"), [("shop", "../evil"), ("a/b", "sales"), ("shop", "x.y"), ("", "s")])
async def test_document_path_components_obey_the_path_rules(tmp_path: Path, kind: str, data_source: str,
                                                            name: str) -> None:
    storage = make_storage(kind, tmp_path)
    text = document_text(name=name, datasets=[dataset("orders", fields=[field("order_id")])])
    with pytest.raises(ValueError):
        await storage.save_semantic_model_text(data_source=data_source, name=name, text=text)


# --------------------------------------------------------------------------- #
# Shape
# --------------------------------------------------------------------------- #
_UNKNOWN_KEYS = [
    pytest.param(document_text(datasets=[dataset("orders", fields=[field("order_id")], grain="day")]), "grain",
                 id="dataset"),
    pytest.param(document_text(datasets=[dataset("orders", fields=[field("order_id")])], owner="me"), "owner",
                 id="document"),
    pytest.param(document_text(datasets=[dataset("orders", fields=[field("order_id", format="0.00")])]), "format",
                 id="field"),
    pytest.param(document_text(
        datasets=[dataset("orders", fields=[field("order_id"), field("customer_id")]),
                  dataset("customers", fields=[field("customer_id")], primary_key=["customer_id"])],
        relationships=[{**relationship("o2c", src="orders", to="customers", from_columns=["customer_id"],
                                       to_columns=["customer_id"]), "cardinality": "one_to_one"}]),
        "cardinality", id="relationship"),
    pytest.param(document_text(datasets=[dataset("orders", fields=[field("amount")])],
                               metrics=[metric("total", "SUM(orders.amount)", hidden=True)]), "hidden", id="metric"),
]


async def test_an_unknown_key_refuses_the_document(tmp_path: Path) -> None:
    """Spec scenario: An unknown key refuses the document."""
    text = document_text(datasets=[dataset("orders", fields=[field("order_id")], grain="day"),
                                   dataset("customers", fields=[field("customer_id")])])
    place_document(tmp_path, text, name="sales")
    storage = await yaml_storage_with_datasource(tmp_path)
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert not {d.name for d in loaded} & {"orders", "customers"}
    assert "grain" in failed(errs, "semantic_model")["sales"]


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "key"), _UNKNOWN_KEYS)
async def test_a_key_ossie_does_not_define_is_refused_at_any_level(tmp_path: Path, kind: str, text: str,
                                                                   key: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await store_document(storage, text)
    assert key in str(info.value)
    assert "sales" in str(info.value)


LEGACY_WRAPPER = yaml.safe_dump({"version": "0.2.0.dev0", "semantic_model": [
    {"name": "sales", "datasets": [dataset("orders", fields=[field("order_id")])]}]}, sort_keys=False)


async def test_a_legacy_wrapper_names_the_upgrade_path(tmp_path: Path) -> None:
    """Spec scenario: A legacy wrapper names the upgrade path."""
    place_document(tmp_path, LEGACY_WRAPPER, name="sales")
    storage = await yaml_storage_with_datasource(tmp_path)
    _, errs = await storage.load_datasets(data_source=DS)
    assert "slayer import-osi" in failed(errs, "semantic_model")["sales"]


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "needle"), [
    pytest.param(LEGACY_WRAPPER, "slayer import-osi", id="wrapper"),
    pytest.param(document_text(datasets=[dataset("orders", fields=[field("order_id")],
                                                 custom_extensions=[{"vendor_name": "SLAYER", "data": "{}"}])]),
                 "slayer import-osi", id="custom-extensions"),
    pytest.param(document_text(datasets=[dataset("orders", fields=[field("order_id")])]).replace(
        "version: 0.3.0.dev0", "version: 9.9.9"), "version", id="unknown-version"),
    pytest.param(document_text(datasets=[]), "datasets", id="empty-datasets"),
])
async def test_an_earlier_or_unknown_shape_is_refused_on_save(tmp_path: Path, kind: str, text: str,
                                                              needle: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.save_semantic_model_text(data_source=DS, name="sales", text=text)
    assert needle in str(info.value)


async def test_an_unloadable_document_leaves_standalone_datasets_loading(tmp_path: Path) -> None:
    place_document(tmp_path, LEGACY_WRAPPER, name="sales")
    storage = await yaml_storage_with_datasource(tmp_path)
    returns = returns_dataset().model_copy(update={"data_source": DS, "relationships": []})
    await storage.save_dataset(returns, _validate=False)
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert {d.name for d in loaded} == {"returns"}
    assert set(failed(errs, "semantic_model")) == {"sales"}


# --------------------------------------------------------------------------- #
# Version
# --------------------------------------------------------------------------- #
async def test_a_hand_written_document_needs_no_slayer_entry(tmp_path: Path) -> None:
    """Spec scenario: A hand-written document needs no SLayer entry."""
    path = place_document(tmp_path, SALES, name="sales")
    before = path.stat().st_mtime_ns
    storage = await yaml_storage_with_datasource(tmp_path)
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert {d.name for d in loaded} == {"orders", "customers"}
    assert errs == []
    assert path.read_text(encoding="utf-8") == SALES
    assert path.stat().st_mtime_ns == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_slayer_write_never_touches_ossies_version(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    await storage.save_dataset(orders.model_copy(update={"hidden": True}))
    doc = yaml.safe_load(await storage.get_semantic_model_text("sales", data_source=DS))
    assert doc["version"] == "0.3.0.dev0"
    slayer_entries = [e for e in doc.get("nonportable_extensions", []) if e.get("vendor_name") == "SLAYER"]
    assert len(slayer_entries) <= 1


# --------------------------------------------------------------------------- #
# Loading never introspects and never writes
# --------------------------------------------------------------------------- #
async def test_loading_with_the_database_unreachable(tmp_path: Path) -> None:
    """Spec scenario: Loading with the database unreachable."""
    place_document(tmp_path, SALES, name="sales")
    storage = await yaml_storage_with_datasource(tmp_path)
    assert await storage.list_datasets(data_source=DS) == ["customers", "orders"]
    loaded, _ = await storage.load_datasets(data_source=DS)
    assert {d.name for d in loaded} == {"orders", "customers"}
    assert not (tmp_path / "absent.duckdb").exists()  # DuckDB creates the file on any connection


@pytest.mark.parametrize("kind", STORAGES)
async def test_loading_never_writes_to_storage(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    before = snapshot(tmp_path)
    fingerprint = await storage.graph_fingerprint()
    await storage.load_datasets(data_source=DS)
    await storage.get_dataset("orders", data_source=DS)
    await storage.list_model_scoped_metrics(DS)
    assert snapshot(tmp_path) == before
    assert await storage.graph_fingerprint() == fingerprint


# --------------------------------------------------------------------------- #
# Decomposition
# --------------------------------------------------------------------------- #
RAW_ORDERS = TableSpec(name="orders", columns=[("o_orderkey", "TEXT"), ("o_totalprice", "DOUBLE")],
                       rows=[("O1", 10.0), ("O2", 20.0)])


async def test_undeclared_source_columns_stay_invisible() -> None:
    """Spec scenario: Undeclared source columns stay invisible."""
    text = document_text(datasets=[dataset("orders", fields=[field("order_id", "o_orderkey")])])
    async with document_engine(documents=(text,), tables=[RAW_ORDERS]) as engine:
        orders = await engine.storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        assert [f.name for f in orders.fields] == ["order_id"]
        assert await single(engine, population="orders", metric_expr="count(orders.order_id)") == 2
        with pytest.raises(UnknownReferenceError, match="o_totalprice"):
            await engine.execute({"population": "orders", "metrics": ["sum(orders.o_totalprice)"]})


@pytest.mark.parametrize("kind", STORAGES)
async def test_document_datasets_are_visible_through_every_read(kind: str) -> None:
    async with document_engine(kind=kind) as engine:
        storage = engine.storage
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        assert [f.name for f in orders.fields] == ["order_id", "customer_id", "amount", "status"]
        assert [m.name for m in orders.metrics] == ["total_revenue"]
        loaded, _ = await storage.load_datasets(data_source=DS)
        assert {d.name for d in loaded} == {"orders", "customers"}
        assert sorted(await storage.list_datasets(data_source=DS)) == ["customers", "orders"]
        assert "customer_id" in await mcp_text(storage, "inspect", reference=f"{DS}.orders", entity_type="dataset")
        assert await single(engine, population="orders", metric_expr="orders.total_revenue") == 650.0
        hits = await SearchService(storage=storage).search(question="orders amount", max_results=50)
        assert f"{DS}.orders" in {h.id for h in hits.results}


async def test_a_query_source_reads_as_a_query() -> None:
    text = document_text(datasets=[dataset("open_orders", source="SELECT order_id, amount FROM orders WHERE "
                                           "status = 'open'", fields=[field("order_id"), field("amount", datatype="Float")])])
    async with document_engine(documents=(text,)) as engine:
        assert await single(engine, population="open_orders", metric_expr="sum(open_orders.amount)") == 500.0


async def test_a_field_expression_is_the_fields_expression() -> None:
    text = document_text(datasets=[dataset("orders", fields=[field("order_id"),
                                                             field("double_amount", "amount * 2", datatype="Float")])])
    async with document_engine(documents=(text,)) as engine:
        assert await single(engine, population="orders", metric_expr="sum(orders.double_amount)") == 1300.0


async def test_a_composite_primary_key_marks_every_member(tmp_path: Path) -> None:
    """Spec scenario: A composite primary key marks every member."""
    storage = make_storage("yaml", tmp_path)
    text = document_text(datasets=[dataset("lines", fields=[field("order_id"), field("line_number", datatype="Integer"),
                                                            field("sku")], primary_key=["order_id", "line_number"])])
    await store_document(storage, text)
    lines = await storage.get_dataset("lines", data_source=DS)
    assert lines is not None
    flags = {f.name: (f.primary_key, f.unique) for f in lines.fields}
    assert flags == {"order_id": (True, False), "line_number": (True, False), "sku": (False, False)}


LINES = TableSpec(name="lines", columns=[("order_id", "TEXT"), ("line_number", "INT"), ("sku", "TEXT")],
                  rows=[("O1", 1, "A"), ("O1", 2, "B"), ("O2", 1, "A")])
SHIPMENTS = TableSpec(name="shipments", columns=[("shipment_id", "TEXT"), ("s_order", "TEXT"), ("s_line", "INT"),
                                                 ("qty", "DOUBLE")],
                      rows=[("H1", "O1", 1, 5.0), ("H2", "O1", 2, 7.0), ("H3", "O2", 1, 11.0), ("H4", "O1", 1, 1.0)])


async def test_a_reordered_composite_key_joins_on_its_declared_pairs() -> None:
    text = document_text(
        datasets=[
            dataset("lines", fields=[field("order_id"), field("line_number", datatype="Integer"), field("sku")],
                    primary_key=["line_number", "order_id"]),
            dataset("shipments", fields=[field("shipment_id"), field("s_order"), field("s_line", datatype="Integer"),
                                         field("qty", datatype="Float")], primary_key=["shipment_id"]),
        ],
        relationships=[relationship("shipments_to_lines", src="shipments", to="lines", from_columns=["s_line", "s_order"],
                                    to_columns=["line_number", "order_id"])],
    )
    async with document_engine(documents=(text,), tables=[LINES, SHIPMENTS]) as engine:
        lines = await engine.storage.get_dataset("lines", data_source=DS)
        assert lines is not None
        assert {f.name for f in lines.fields if f.primary_key} == {"order_id", "line_number"}
        resp = await engine.execute({"dimensions": ["lines.sku"],
                                     "metrics": [{"metric": "sum(shipments.qty)", "alias": "q"}]})
        assert by(resp, "lines.sku", "q") == {"A": 17.0, "B": 7.0}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_one_field_unique_key_sets_unique(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    text = document_text(datasets=[dataset("customers", fields=[field("customer_id"), field("region")],
                                           unique_keys=[["customer_id"]])])
    await store_document(storage, text)
    customers = await storage.get_dataset("customers", data_source=DS)
    assert customers is not None
    assert {f.name: (f.unique, f.primary_key) for f in customers.fields} == {
        "customer_id": (True, False), "region": (False, False)}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_multi_field_unique_key_is_kept_and_not_a_proof(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    text = document_text(datasets=[dataset("customers", fields=[field("region"), field("manager_id")],
                                           unique_keys=[["region", "manager_id"]])])
    await store_document(storage, text)
    customers = await storage.get_dataset("customers", data_source=DS)
    assert customers is not None
    assert not any(f.unique or f.primary_key for f in customers.fields)
    stored = yaml.safe_load(await storage.get_semantic_model_text("sales", data_source=DS))
    assert stored["datasets"][0]["unique_keys"] == [["region", "manager_id"]]


SALES_TABLE = TableSpec(name="sales", columns=[("order_id", "TEXT"), ("amount", "DOUBLE"), ("order_date", "DATE")],
                        rows=[("S1", 10.0, "2026-01-05"), ("S2", 20.0, "2026-01-20"), ("S3", 5.0, "2026-02-03")])


async def test_a_temporal_datatype_gives_the_time_role() -> None:
    text = document_text(name="ledger", datasets=[dataset("sales", fields=[
        field("order_id"), field("amount", datatype="Float"), field("order_date", datatype="Date")])])
    async with document_engine(documents=(text,), tables=[SALES_TABLE]) as engine:
        resp = await engine.execute({
            "population": "sales", "time_dimensions": [{"field": "sales.order_date", "granularity": "month", "alias": "m"}],
            "metrics": [{"metric": "sum(sales.amount)", "alias": "v"}], "order": [{"field": "m"}]})
        assert [float(r["v"]) for r in resp.data] == [30.0, 5.0]


async def test_a_dataset_named_source() -> None:
    text = document_text(datasets=[dataset("source", source="orders", fields=[field("amount", datatype="Float")],
                                           metrics=[metric("total", "SUM(source.amount)")])],
                         metrics=[metric("grand_total", "SUM(source.amount)")])
    async with document_engine(documents=(text,)) as engine:
        assert await single(engine, population="source", metric_expr="source.total") == 650.0
        resp = await engine.execute({"population": "source", "metrics": ["grand_total"]})
        assert [float(r["grand_total"]) for r in resp.data] == [650.0]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_declaring_no_fields_exposes_nothing(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, document_text(datasets=[{"name": "orders", "source": "orders"}]))
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    assert orders.fields == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_descriptions_labels_ai_context_and_extensions_are_kept(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    acme = [{"vendor_name": "ACME", "data": {"colour": "blue"}}]
    text = document_text(datasets=[dataset(
        "orders", description="One row per order.", ai_context={"synonyms": ["purchases"]},
        nonportable_extensions=acme,
        fields=[field("amount", datatype="Float", label="Amount", description="Gross.", nonportable_extensions=acme)])])
    await store_document(storage, text)
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    assert orders.description == "One row per order."
    [amount] = orders.fields
    assert (amount.label, amount.description) == ("Amount", "Gross.")
    assert await storage.get_semantic_model_text("sales", data_source=DS) == text


# --------------------------------------------------------------------------- #
# Relationships
# --------------------------------------------------------------------------- #
async def test_a_declared_relationship_proves_determination() -> None:
    """Spec scenario: A declared relationship proves determination."""
    text = SALES.replace("    primary_key: [customer_id]\n", "")
    assert "primary_key: [customer_id]" not in text
    async with document_engine(documents=(text,)) as engine:
        resp = await engine.execute({"dimensions": ["customers.region"],
                                     "metrics": [{"metric": "sum(orders.amount)", "alias": "v"}]})
        assert by(resp, "customers.region", "v") == {"EAST": 350.0, "WEST": 300.0}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_relationship_decomposes_onto_its_from_dataset(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    orders = await storage.get_dataset("orders", data_source=DS)
    customers = await storage.get_dataset("customers", data_source=DS)
    assert orders is not None and customers is not None
    [rel] = orders.relationships
    assert (rel.name, rel.to, rel.from_columns, rel.to_columns) == (
        "orders_to_customers", "customers", ["customer_id"], ["customer_id"])
    assert rel.cardinality is RelationshipCardinality.MANY_TO_ONE
    assert rel.join_type is JoinType.LEFT
    assert customers.relationships == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_relationship_names_only_datasets_of_its_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    text = document_text(datasets=[dataset("orders", fields=[field("store_id")])],
                         relationships=[relationship("o2s", src="orders", to="stores", from_columns=["store_id"],
                                                     to_columns=["store_id"])])
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await store_document(storage, text)
    assert "stores" in str(info.value)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_standalone_relationship_keeps_its_declared_cardinality(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(managers_dataset().model_copy(update={"data_source": DS}), _validate=False)
    stored = await storage.get_dataset("managers", data_source=DS)
    assert stored is not None
    assert [r.cardinality for r in stored.relationships] == [RelationshipCardinality.ONE_TO_MANY]
    assert await storage.semantic_model_owner("managers", data_source=DS) is None


# --------------------------------------------------------------------------- #
# Model-scoped metrics
# --------------------------------------------------------------------------- #
async def test_a_document_metric_answers_by_bare_name() -> None:
    """Spec scenario: A document metric answers by bare name."""
    text = SALES.replace("AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)",
                         "SUM(orders.amount) / COUNT(DISTINCT customers.customer_id)")
    async with document_engine(documents=(text,)) as engine:
        resp = await engine.execute({"population": "customers", "metrics": ["revenue_per_customer"]})
        assert [float(r["revenue_per_customer"]) for r in resp.data] == [162.5]


async def test_an_agg_document_metric_answers_by_bare_name() -> None:
    async with document_engine() as engine:
        resp = await engine.execute({"population": "customers", "metrics": ["revenue_per_customer"]})
        assert [float(r["revenue_per_customer"]) for r in resp.data] == [162.5]


@pytest.mark.parametrize("kind", STORAGES)
async def test_top_level_metrics_become_model_scoped_metrics(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES.replace("    description: Revenue per customer.\n",
                                                "    description: Revenue per customer.\n    datatype: Float\n"))
    [m] = await storage.list_model_scoped_metrics(DS)
    assert (m.name, m.semantic_model, m.data_source) == ("revenue_per_customer", "sales", DS)
    assert "AGG(orders.total_revenue)" in m.expression
    assert m.description == "Revenue per customer."
    assert plain(m.ai_context) == {"synonyms": ["ARPU"]}
    assert str(plain(m.datatype)) in {"Float", "DataType.FLOAT"}
    assert m.load_warning is None


# --------------------------------------------------------------------------- #
# Entities SLayer cannot evaluate yet
# --------------------------------------------------------------------------- #
WINDOWED = document_text(
    name="ledger",
    datasets=[dataset("sales", fields=[field("order_id"), field("amount", datatype="Float"),
                                       field("order_date", datatype="Date"),
                                       field("running_total", "SUM(amount) OVER (ORDER BY order_date)",
                                             datatype="Float")])],
    metrics=[metric("total", "SUM(sales.amount)"), metric("running", "SUM(sales.amount) OVER ()")],
)


async def test_a_windowed_field_loads_and_fails_only_when_read() -> None:
    """Spec scenario: A windowed field loads and fails only when read."""
    async with document_engine(documents=(WINDOWED,), tables=[SALES_TABLE]) as engine:
        _, errs = await engine.storage.load_datasets(data_source=DS)
        assert errs == []
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.sales.running_total", entity_type="field")
        assert "window" in inspected.lower()
        assert await single(engine, population="sales", metric_expr="sum(sales.amount)") == 35.0
        with pytest.raises(error("UnevaluableEntityError"), match="running_total"):
            await engine.execute({"population": "sales", "dimensions": ["sales.running_total"]})


async def test_an_unevaluable_entitys_warning_shows_on_summary_and_search() -> None:
    async with document_engine(documents=(WINDOWED,), tables=[SALES_TABLE]) as engine:
        summary = await mcp_text(engine.storage, "datasets_summary", datasource_name=DS, compact=False)
        assert "running_total" in summary
        assert "window" in summary.lower()
        hits = await SearchService(storage=engine.storage).search(question="running_total", compact=False,
                                                                  max_results=20)
        [hit] = [h for h in hits.results if h.id == f"{DS}.sales.running_total"]
        assert "window" in f"{hit.text} {hit.description or ''}".lower()


async def test_an_unevaluable_model_scoped_metric_loads_and_fails_only_when_read() -> None:
    async with document_engine(documents=(WINDOWED,), tables=[SALES_TABLE]) as engine:
        metrics = {m.name: m for m in await engine.storage.list_model_scoped_metrics(DS)}
        assert metrics["total"].load_warning is None
        assert "window" in (metrics["running"].load_warning or "").lower()
        resp = await engine.execute({"population": "sales", "metrics": ["total"]})
        assert [float(r["total"]) for r in resp.data] == [35.0]
        with pytest.raises(error("UnevaluableEntityError"), match="running"):
            await engine.execute({"population": "sales", "metrics": ["running"]})


# --------------------------------------------------------------------------- #
# Converter-construct parity (data and expected values of the 1.x importer tests)
# --------------------------------------------------------------------------- #
PARITY_DDL = (
    "CREATE TABLE orders (o_orderkey INTEGER, o_custkey INTEGER, amount DOUBLE, quantity INTEGER, tip DOUBLE, "
    "status VARCHAR, region VARCHAR, is_paid BOOLEAN, first_name VARCHAR, last_name VARCHAR)",
    "INSERT INTO orders VALUES (1, 101, 10, 1, 1, 'completed', 'north', TRUE, 'Ann', 'Lee'), "
    "(2, 101, 5, 2, NULL, 'open', NULL, FALSE, 'Bo', NULL), (3, 102, 7, 3, 2, 'completed', 'south', TRUE, 'Cy', 'Kim')",
    "CREATE TABLE customers (c_custkey INTEGER, segment VARCHAR, discount DOUBLE)",
    "INSERT INTO customers VALUES (101, 'b2b', 1.0), (102, 'b2c', 0.5)",
    "CREATE TABLE visits (visit_id INTEGER, status VARCHAR, customer_id VARCHAR)",
    "INSERT INTO visits VALUES (1, 'completed', 'c1'), (2, 'completed', 'c1'), (3, 'open', 'c2'), (4, 'completed', 'c3')",
    "CREATE TABLE paid (id INTEGER, status VARCHAR, amount INTEGER, tags VARCHAR[], region VARCHAR, "
    "created_at TIMESTAMP)",
    "INSERT INTO paid VALUES (1, 'paid', 10, ['a', 'b'], 'n', '2026-01-01 10:00:00'), "
    "(2, 'paid', 20, ['b'], 'n', '2026-01-02 10:00:00'), (3, 'open', 5, NULL, 's', '2026-01-03 10:00:00')",
    "CREATE TABLE tagged (id INTEGER, tags VARCHAR[])",
    "INSERT INTO tagged VALUES (1, ['a', NULL]), (2, []), (3, NULL)",
    "CREATE TABLE src_orders (order_id INTEGER, amount DOUBLE, quantity INTEGER, price DOUBLE, status VARCHAR, "
    "state VARCHAR)",
    "INSERT INTO src_orders VALUES (1, 10, 2, 10, 'completed', 'open'), (2, 5, 3, 5, 'open', 'closed'), "
    "(3, 7, NULL, NULL, 'completed', 'open')",
    "CREATE TABLE readings (reading_id INTEGER, quantity INTEGER)",
    "INSERT INTO readings VALUES (1, 1), (2, 2), (3, 3)",
)
_AMOUNTS = [10, 5, 7]
_QUANTITIES = [1, 2, 3]
_INT, _FLOAT, _BOOL = "Integer", "Float", "Boolean"


def _parity_datasets(extra: dict[str, dict[str, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """The parity datasets; ``extra[dataset]`` adds ``fields`` / ``metrics``."""
    base: dict[str, dict[str, Any]] = {
        "orders": {"fields": [field("order_id", "o_orderkey", datatype=_INT), field("customer_id", "o_custkey",
                   datatype=_INT), field("amount", datatype=_FLOAT), field("quantity", datatype=_INT),
                   field("tip", datatype=_FLOAT), field("status"), field("region"), field("is_paid", datatype=_BOOL),
                   field("first_name"), field("last_name")], "primary_key": ["order_id"]},
        "customers": {"fields": [field("id", "c_custkey", datatype=_INT), field("segment"),
                                 field("discount", datatype=_FLOAT)], "primary_key": ["id"]},
        "visits": {"fields": [field("visit_id", datatype=_INT), field("status"), field("customer_id")]},
        "paid": {"fields": [field("id", datatype=_INT), field("status"), field("amount", datatype=_INT),
                            field("tags", datatype=None), field("region"), field("created_at", datatype="DateTime")],
                 "primary_key": ["id"]},
        "tagged": {"fields": [field("id", datatype=_INT), field("tags", datatype=None)], "primary_key": ["id"]},
        "lines": {"source": "src_orders", "fields": [field("order_id", datatype=_INT), field("amount", datatype=_FLOAT)]},
        "renamed": {"source": "src_orders", "fields": [field("order_id", datatype=_INT), field("status", "state")]},
        "readings": {"fields": [field("reading_id", datatype=_INT)]},
    }
    out = []
    for name, spec in base.items():
        more = extra.get(name, {})
        entry = dataset(name, source=spec.get("source"), fields=[*spec["fields"], *more.get("fields", [])],
                        **({"primary_key": spec["primary_key"]} if "primary_key" in spec else {}))
        if more.get("metrics"):
            entry["metrics"] = more["metrics"]
        out.append(entry)
    return out


def parity_doc(*, model_metrics: list[dict[str, Any]] | None = None,
               extra: dict[str, dict[str, list[dict[str, Any]]]] | None = None) -> str:
    return document_text(
        name="parity", datasets=_parity_datasets(extra or {}), metrics=model_metrics,
        relationships=[relationship("o2c", src="orders", to="customers", from_columns=["customer_id"],
                                    to_columns=["id"])])


@contextlib.asynccontextmanager
async def ddl_engine(base: Path, *documents: str, statements: Sequence[str] = PARITY_DDL,
                     kind: str = "yaml") -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over a DuckDB file seeded by ``statements``, ``documents`` stored in ``shop``."""
    db = base / "parity.duckdb"
    with contextlib.closing(duckdb.connect(str(db))) as con:
        for statement in statements:
            con.execute(statement)
    storage = make_storage(kind, base)
    async with owned_query_engine(storage=storage,
                                  datasources=[DatasourceConfig(name=DS, type="duckdb", database=str(db))]) as engine:
        for text in documents:
            await store_document(storage, text)
        yield engine


async def metric_value(base: Path, *, dataset_name: str, sql: str, **fields: list[dict[str, Any]]) -> Any:
    """The value of a dataset metric ``v = sql`` on ``dataset_name``."""
    extra: dict[str, dict[str, list[dict[str, Any]]]] = {dataset_name: {"metrics": [metric("v", sql)]}}
    for name, more in fields.items():
        extra.setdefault(name, {})["fields"] = more
    async with ddl_engine(base, parity_doc(extra=extra)) as engine:
        return await single(engine, population=dataset_name, metric_expr=f"{dataset_name}.v")


async def by_row(engine: SlayerQueryEngine, *, dataset_name: str, key: str, value: str) -> list[Any]:
    resp = await engine.execute({"population": dataset_name, "dimensions": [f"{dataset_name}.{key}",
                                 f"{dataset_name}.{value}"], "order": [{"field": f"{dataset_name}.{key}"}]})
    return [r[f"{dataset_name}.{value}"] for r in resp.data]


def _covariance(xs: Sequence[float], ys: Sequence[float], *, sample: bool) -> float:
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (len(xs) - (1 if sample else 0))


@pytest.mark.parametrize(("sql", "expected"), [
    pytest.param("SUM(amount) FILTER (WHERE status = 'completed')", 17, id="sum"),
    pytest.param("COUNT(*) FILTER (WHERE status = 'completed')", 2, id="count-star"),
    pytest.param("AVG(amount) FILTER (WHERE status = 'completed')", 8.5, id="avg"),
    pytest.param("MIN(amount) FILTER (WHERE status = 'completed')", 7, id="min"),
    pytest.param("MAX(amount) FILTER (WHERE status = 'completed')", 10, id="max"),
    pytest.param("COUNT(amount) FILTER (WHERE status = 'open')", 1, id="count-col"),
    pytest.param("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FILTER (WHERE status = 'completed')", 8.5,
                 id="median"),
    pytest.param("PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount) FILTER (WHERE status = 'completed')", 9.7,
                 id="percentile"),
    pytest.param("SUM(amount) FILTER (WHERE status = 'open' OR region = 'south')", 12, id="or"),
    pytest.param("SUM(amount) FILTER (WHERE region IS NULL)", 5, id="is-null"),
    pytest.param("SUM(amount) FILTER (WHERE status = 'completed') / COUNT(*)", 17 / 3, id="in-arithmetic"),
    pytest.param("STDDEV_SAMP(amount)", statistics.stdev(_AMOUNTS), id="stddev-samp"),
    pytest.param("STDDEV_POP(amount)", statistics.pstdev(_AMOUNTS), id="stddev-pop"),
    pytest.param("VAR_SAMP(amount)", statistics.variance(_AMOUNTS), id="var-samp"),
    pytest.param("VAR_POP(amount)", statistics.pvariance(_AMOUNTS), id="var-pop"),
    pytest.param("CORR(amount, quantity)", statistics.correlation(_AMOUNTS, _QUANTITIES), id="corr"),
    pytest.param("COVAR_SAMP(amount, quantity)", _covariance(_AMOUNTS, _QUANTITIES, sample=True), id="covar-samp"),
    pytest.param("COVAR_POP(amount, quantity)", _covariance(_AMOUNTS, _QUANTITIES, sample=False), id="covar-pop"),
    pytest.param("CORR(amount, quantity) FILTER (WHERE status = 'completed')",
                 statistics.correlation([10, 7], [1, 3]), id="filtered-corr"),
    pytest.param("STDDEV_SAMP(amount) FILTER (WHERE status <> 'x')", statistics.stdev(_AMOUNTS), id="filtered-stddev"),
    pytest.param("APPROX_COUNT_DISTINCT(customer_id)", 2, id="approx-count-distinct"),
    pytest.param("SUM(quantity * amount)", 41, id="operand-arithmetic"),
    pytest.param("SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END)", 17, id="operand-case"),
    pytest.param("SUM(IFF(amount > 6, amount, 0))", 17, id="operand-iff"),
    pytest.param("SUM(CASE WHEN NOT is_paid THEN amount END)", 5, id="operand-boolean-column"),
    pytest.param("SUM(CAST(quantity AS DOUBLE) / 2)", 3.0, id="operand-cast"),
    pytest.param("COUNT(first_name || last_name)", 2, id="operand-concat-null"),
    pytest.param("SUM(LENGTH(LPAD(status, 12, 'x')))", 36, id="operand-outside-the-formula-language"),
])
async def test_an_orders_metric_construct_keeps_its_meaning(tmp_path: Path, sql: str, expected: float) -> None:
    assert float(await metric_value(tmp_path, dataset_name="orders", sql=sql)) == pytest.approx(expected)


async def test_a_filtered_distinct_count_keeps_its_meaning(tmp_path: Path) -> None:
    sql = "COUNT(DISTINCT customer_id) FILTER (WHERE status = 'completed')"
    assert await metric_value(tmp_path, dataset_name="visits", sql=sql) == 2


async def test_a_filter_aggregate_keeps_every_group(tmp_path: Path) -> None:
    """Spec scenario: A FILTER aggregate keeps every group."""
    doc = parity_doc(extra={"paid": {"metrics": [metric("paid", "SUM(amount) FILTER (WHERE status = 'paid')")]}})
    async with ddl_engine(tmp_path, doc) as engine:
        resp = await engine.execute({"population": "paid", "dimensions": ["paid.region"], "metrics": ["paid.paid"]})
        assert by(resp, "paid.region", "paid.paid") == {"n": 30.0, "s": None}


async def test_an_array_function_maps_to_the_list_library(tmp_path: Path) -> None:
    """Spec scenario: An array function maps to the list library."""
    doc = parity_doc(extra={"tagged": {"fields": [field("tag_count", "CARDINALITY(tags)", datatype=_INT)]}})
    async with ddl_engine(tmp_path, doc) as engine:
        assert await by_row(engine, dataset_name="tagged", key="id", value="tag_count") == [1, 0, None]


@pytest.mark.parametrize(("sql", "expected"), [
    pytest.param("CARDINALITY(tags)", [2, 1, None], id="cardinality"),
    pytest.param("ARRAY_LENGTH(tags)", [2, 1, None], id="array-length"),
    pytest.param("ARRAY_CONTAINS(tags, 'a')", [True, False, None], id="array-contains"),
    pytest.param("'a' = ANY(tags)", [True, False, None], id="any"),
])
async def test_a_list_field_construct_keeps_its_meaning(tmp_path: Path, sql: str, expected: list[Any]) -> None:
    doc = parity_doc(extra={"paid": {"fields": [field("listed", sql, datatype=None)]}})
    async with ddl_engine(tmp_path, doc) as engine:
        assert await by_row(engine, dataset_name="paid", key="id", value="listed") == expected


@pytest.mark.parametrize(("sql", "expected"), [
    pytest.param("COUNT(tags)", 2, id="count-of-a-list"),
    pytest.param("COUNT(*) FILTER (WHERE 'b' = ANY(tags))", 2, id="any-in-filter"),
    pytest.param("COUNT(*) FILTER (WHERE ARRAY_CONTAINS(tags, 'b'))", 2, id="contains-in-filter"),
    pytest.param("COUNT(*) FILTER (WHERE created_at > '2026-01-02')", 2, id="date-only-after-midnight"),
    pytest.param("COUNT(*) FILTER (WHERE created_at = '2026-01-02')", 0, id="date-only-is-an-instant"),
    pytest.param("COUNT(*) FILTER (WHERE created_at >= DATE '2026-01-02')", 2, id="date-literal"),
])
async def test_a_paid_metric_construct_keeps_its_meaning(tmp_path: Path, sql: str, expected: int) -> None:
    assert await metric_value(tmp_path, dataset_name="paid", sql=sql) == expected


async def test_distinct_values_collect_as_a_list(tmp_path: Path) -> None:
    doc = parity_doc(model_metrics=[metric("statuses", "ARRAY_AGG(DISTINCT paid.status)")])
    async with ddl_engine(tmp_path, doc) as engine:
        resp = await engine.execute({"metrics": ["statuses"]})
        assert sorted(resp.data[0]["statuses"]) == ["open", "paid"]


async def test_a_cross_dataset_operand_keeps_its_meaning(tmp_path: Path) -> None:
    doc = parity_doc(model_metrics=[metric("net", "SUM(orders.amount - customers.discount)")])
    async with ddl_engine(tmp_path, doc) as engine:
        resp = await engine.execute({"population": "orders", "metrics": ["net"]})
        assert float(resp.data[0]["net"]) == pytest.approx(19.5)


@pytest.mark.parametrize(("dataset_name", "sql", "expected", "fields"), [
    pytest.param("lines", "SUM(source.quantity)", 5, [], id="undeclared-column"),
    pytest.param("lines", "SUM(source.amount)", 22, [], id="declared-column"),
    pytest.param("lines", "SUM(SOURCE.quantity)", 5, [], id="qualifier-case"),
    pytest.param("lines", "SUM(source.quantity * price)", 35, [field("price", datatype=_FLOAT)], id="in-operand"),
    pytest.param("lines", "SUM(amount) FILTER (WHERE source.status = 'completed')", 17, [], id="in-filter"),
    pytest.param("lines", "COUNT(*) FILTER (WHERE source.status = 'completed')", 2, [], id="filtered-count"),
    pytest.param("readings", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.quantity)", 2, [], id="percentile"),
    pytest.param("renamed", "COUNT(*) FILTER (WHERE source.status = 'open')", 1, [], id="behind-a-renamed-field"),
])
async def test_a_source_column_in_a_dataset_metric_keeps_its_meaning(
    tmp_path: Path, dataset_name: str, sql: str, expected: float, fields: list[dict[str, Any]],
) -> None:
    extra = {dataset_name: fields} if fields else {}
    assert float(await metric_value(tmp_path, dataset_name=dataset_name, sql=sql, **extra)) == pytest.approx(expected)


def _pick(*entries: tuple[str, str], sql: str | None = None) -> dict[str, Any]:
    expression: dict[str, Any] = {"dialects": [{"dialect": d, "expression": e} for d, e in entries]}
    if sql is not None:
        expression = {"sql": sql, **expression}
    return {"name": "pick", "expression": expression, "datatype": _FLOAT}


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("pick", "expected"), [
    pytest.param(_pick(("ANSI_SQL", "amount * 3"), ("OSSIE_SQL_2026", "amount * 2")), 44, id="ossie-before-ansi"),
    pytest.param(_pick(("SNOWFLAKE", "amount * 2"), ("BIGQUERY", "amount * 5")), 44, id="document-order"),
    pytest.param(_pick(("OSSIE_SQL_2026", "amount * 2"), sql="amount * 1"), 22, id="top-level-sql-first"),
    pytest.param(_pick(("BIGQUERY", "amount * 5"), ("DUCKDB", "amount*6")), 132, id="target-native"),
    pytest.param(_pick(("SNOWFLAKE", "IFF(amount > 6, amount, 0)")), 17, id="vendor-function-translated"),
])
async def test_an_expression_without_a_runnable_form_is_translated_in_memory(
    tmp_path: Path, kind: str, pick: dict[str, Any], expected: float,
) -> None:
    doc = parity_doc(extra={"orders": {"fields": [pick]}})
    async with ddl_engine(tmp_path, doc, kind=kind) as engine:
        assert float(await single(engine, population="orders", metric_expr="sum(orders.pick)")) == expected
        assert await engine.storage.get_semantic_model_text("parity", data_source=DS) == doc
    if kind == "yaml":
        assert document_path(tmp_path, name="parity").read_text(encoding="utf-8") == doc


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("pick", "expected"), [
    pytest.param(_pick(("ANSI_SQL", "SUM(amount)")), 22, id="ansi-only"),
    pytest.param(_pick(("ANSI_SQL", "SUM(amount) * 3"), ("OSSIE_SQL_2026", "SUM(amount) * 2")), 44,
                 id="ossie-before-ansi"),
    pytest.param(_pick(("SNOWFLAKE", "SUM(amount) * 2"), ("BIGQUERY", "SUM(amount) * 5")), 44, id="document-order"),
    pytest.param(_pick(("OSSIE_SQL_2026", "SUM(amount) * 2"), sql="SUM(amount)"), 22, id="top-level-sql-first"),
    pytest.param(_pick(("BIGQUERY", "SUM(amount) * 5"), ("DUCKDB", "SUM(amount)*6")), 132, id="target-native"),
    pytest.param(_pick(("SNOWFLAKE", "SUM(IFF(amount > 6, amount, 0))")), 17, id="vendor-function-translated"),
])
async def test_a_dataset_metric_without_a_runnable_form_is_translated_in_memory(
    tmp_path: Path, kind: str, pick: dict[str, Any], expected: float,
) -> None:
    doc = parity_doc(extra={"orders": {"metrics": [pick]}})
    async with ddl_engine(tmp_path, doc, kind=kind) as engine:
        assert float(await single(engine, population="orders", metric_expr="orders.pick")) == expected
        assert await engine.storage.get_semantic_model_text("parity", data_source=DS) == doc
    if kind == "yaml":
        assert document_path(tmp_path, name="parity").read_text(encoding="utf-8") == doc


GROSS_REVENUE = document_text(
    datasets=[dataset("orders", primary_key=["order_id"],
                      fields=[field("order_id"), field("total_amount", "amount", datatype="Float")])],
    metrics=[{"name": "gross_revenue",
              "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.total_amount)"}]}}],
)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_metric_written_only_as_a_dialect_entry_runs(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A metric written only as a dialect entry runs."""
    async with document_engine(kind=kind, documents=(GROSS_REVENUE,), base=tmp_path) as engine:
        resp = await engine.execute({"population": "orders", "metrics": ["gross_revenue"]})
        assert [float(r["gross_revenue"]) for r in resp.data] == [650.0]
        assert await engine.storage.get_semantic_model_text("sales", data_source=DS) == GROSS_REVENUE
    if kind == "yaml":
        assert document_path(tmp_path, name="sales").read_text(encoding="utf-8") == GROSS_REVENUE


async def test_a_model_scoped_metric_in_the_datasources_own_dialect_runs_unchanged(tmp_path: Path) -> None:
    reach = {"name": "reach", "expression": {"dialects": [
        {"dialect": "DUCKDB", "expression": "APPROX_COUNT_DISTINCT(orders.customer_id)"}]}}
    doc = parity_doc(model_metrics=[reach])
    async with ddl_engine(tmp_path, doc) as engine:
        resp = await engine.execute({"population": "orders", "metrics": ["reach"]})
        assert [r["reach"] for r in resp.data] == [2]
        assert await engine.storage.get_semantic_model_text("parity", data_source=DS) == doc
    assert document_path(tmp_path, name="parity").read_text(encoding="utf-8") == doc


@pytest.mark.parametrize("bad", [
    pytest.param(metric("bad", "SUM(amount) FILTER (WHERE ROW_NUMBER() OVER (ORDER BY amount) = 1)"), id="window"),
    pytest.param({"name": "bad", "expression": {"dialects": [{"dialect": "MDX", "expression": "[Measures].[Amount]"}]}},
                 id="no-sql-dialect"),
])
async def test_a_construct_1x_left_unconverted_loads_and_fails_when_read(tmp_path: Path, bad: dict[str, Any]) -> None:
    doc = parity_doc(extra={"orders": {"metrics": [bad, metric("ok", "SUM(amount)")]}})
    async with ddl_engine(tmp_path, doc) as engine:
        _, errs = await engine.storage.load_datasets(data_source=DS)
        assert errs == []
        assert await single(engine, population="orders", metric_expr="orders.ok") == 22.0
        with pytest.raises(error("UnevaluableEntityError"), match="bad"):
            await engine.execute({"population": "orders", "metrics": ["orders.bad"]})


def test_parity_seed_matches_the_1x_expectations(tmp_path: Path) -> None:
    """The parity seed reproduces the 1.x seeds' inputs (guard)."""
    db = tmp_path / "seed.duckdb"
    with contextlib.closing(duckdb.connect(str(db))) as con:
        for statement in PARITY_DDL:
            con.execute(statement)
        assert [r[0] for r in con.execute("SELECT amount FROM orders ORDER BY o_orderkey").fetchall()] == _AMOUNTS
        assert json.dumps(con.execute("SELECT tags FROM paid ORDER BY id").fetchall()) == json.dumps(
            [[["a", "b"]], [["b"]], [None]])
