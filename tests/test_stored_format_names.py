"""Spec: models/stored-format › Names are stored as written; models/semantic-model-documents › A document name that is
not a file name."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig
from slayer.osi.exporter import export_osi
from slayer.osi.importer import import_osi
from slayer.search.service import SearchService
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import (
    SALES, dataset, document_engine, document_text, field, metric, relationship, store_document,
)
from tests._engine_helpers import owned_query_engine
from tests._stored_format_fixtures import (
    DS, STORAGES, folder, make_storage, messages, recorded, reopen, stored_dataset, table_dataset,
)

TOTAL = 650.0  # sum(orders.amount) on the seed
LONG = "f" * 300

_ORDERS_FIELDS = [field("order_id"), field("customer_id"), field("amount", datatype="Float"), field("status")]
_CUSTOMERS = dataset("customers", fields=[field("customer_id"), field("region")], primary_key=["customer_id"])


def _doc(*, orders_fields: list[dict[str, Any]] | None = None, orders_metrics: list[dict[str, Any]] | None = None,
         extra_datasets: list[dict[str, Any]] | None = None, relationships: list[dict[str, Any]] | None = None,
         metrics: list[dict[str, Any]] | None = None) -> str:
    orders = dataset("orders", fields=[*_ORDERS_FIELDS, *(orders_fields or [])], primary_key=["order_id"])
    if orders_metrics:
        orders["metrics"] = orders_metrics
    return document_text(datasets=[orders, _CUSTOMERS, *(extra_datasets or [])], relationships=relationships,
                         metrics=metrics)


#: ``(id, name, document)``: a document holding one entity whose name SLayer cannot reference yet.
INERT = [
    ("dotted-field", "amount.usd", _doc(orders_fields=[field("amount.usd", "amount * 1.1", datatype="Float")])),
    ("non-identifier-dataset-metric", "revenue %", _doc(orders_metrics=[metric("revenue %", "SUM(amount)")])),
    ("dataset-with-colon", "a:b", _doc(extra_datasets=[dataset("a:b", source="customers",
                                                               fields=[field("customer_id")])])),
    ("dotted-relationship", "orders.to.customers", _doc(relationships=[relationship(
        "orders.to.customers", src="orders", to="customers", from_columns=["customer_id"],
        to_columns=["customer_id"])])),
]


def _declares(text: str, name: str) -> bool:
    """Whether the document ``text`` declares an entity named exactly ``name``, at any level."""
    def walk(node: Any) -> bool:
        if isinstance(node, dict):
            return node.get("name") == name or any(walk(v) for v in node.values())
        if isinstance(node, list):
            return any(walk(v) for v in node)
        return False

    return walk(yaml.safe_load(text))


async def _total(engine: Any) -> float:
    resp = await engine.execute({"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "v"}]})
    return float(resp.data[0]["v"])


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dotted_field_name_round_trips(kind: str) -> None:
    """Spec scenario: A dotted field name round-trips."""
    text = INERT[0][2]
    with recorded() as record:
        async with document_engine(kind=kind, documents=(text,)) as engine:
            loaded, errs = await engine.storage.load_datasets(data_source=DS)
            assert errs == []
            assert {d.name for d in loaded} >= {"orders", "customers"}
            assert await _total(engine) == TOTAL
            exported = (await export_osi(engine=engine, datasource=DS)).document
    assert any("amount.usd" in m for m in messages(record))
    [orders] = [d for d in exported["datasets"] if d["name"] == "orders"]
    [dotted] = [f for f in orders["fields"] if f["name"] == "amount.usd"]
    assert dotted["expression"] == {"sql": "amount * 1.1"}


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("name", "text"), [(n, t) for _, n, t in INERT], ids=[i for i, _, _ in INERT])
async def test_a_name_slayer_cannot_reference_loads_inert(kind: str, name: str, text: str) -> None:
    with recorded() as record:
        async with document_engine(kind=kind, documents=(text,)) as engine:
            storage = engine.storage
            _, errs = await storage.load_datasets(data_source=DS)
            assert errs == []
            assert await _total(engine) == TOTAL
            assert name in await mcp_text(storage, "datasets_summary", datasource_name=DS, compact=False)
            hits = await SearchService(storage=storage).search(question=name, max_results=50)
            assert not [h.id for h in hits.results if name in h.id]
    assert any(name in m for m in messages(record))


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_inert_field_shows_its_warning_on_inspect(kind: str) -> None:
    async with document_engine(kind=kind, documents=(INERT[0][2],)) as engine:
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.orders", entity_type="dataset")
    assert "amount.usd" in inspected


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("name", "text"), [(n, t) for _, n, t in INERT], ids=[i for i, _, _ in INERT])
async def test_an_inert_names_text_survives_an_unrelated_edit_and_export(kind: str, name: str, text: str) -> None:
    async with document_engine(kind=kind, documents=(text,)) as engine:
        storage = engine.storage
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        await storage.save_dataset(orders.model_copy(update={"description": "Edited."}))
        stored = await storage.get_semantic_model_text("sales", data_source=DS)
        assert stored is not None
        assert _declares(stored, name)
        assert yaml.safe_load(stored)["datasets"][0]["description"] == "Edited."
        exported = (await export_osi(engine=engine, datasource=DS)).document
    assert _declares(yaml.safe_dump(exported), name)


# --------------------------------------------------------------------------- #
# Authoring such a name is refused
# --------------------------------------------------------------------------- #
async def _refused(coro_factory: Any, *, name: str) -> None:
    with pytest.raises(Exception) as info:  # noqa: PT011 — the typed error is new; its text must name the entity
        await coro_factory()
    assert name in str(info.value)


@pytest.mark.parametrize("kind", STORAGES)
async def test_authoring_a_field_slayer_cannot_reference_is_refused(kind: str) -> None:
    async with document_engine(kind=kind, documents=(SALES,)) as engine:
        storage = engine.storage
        before = await storage.get_semantic_model_text("sales", data_source=DS)

        async def add() -> None:
            orders = await storage.get_dataset("orders", data_source=DS)
            assert orders is not None
            await storage.save_dataset(orders.model_copy(update={
                "fields": [*orders.fields, DatasetField(name="amount.usd", expression="amount")]}))

        await _refused(add, name="amount.usd")
        assert await storage.get_semantic_model_text("sales", data_source=DS) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_authoring_a_dataset_metric_slayer_cannot_reference_is_refused(kind: str) -> None:
    async with document_engine(kind=kind, documents=(SALES,)) as engine:
        storage = engine.storage
        before = await storage.get_semantic_model_text("sales", data_source=DS)

        async def add() -> None:
            orders = await storage.get_dataset("orders", data_source=DS)
            assert orders is not None
            await storage.save_dataset(orders.model_copy(update={
                "metrics": [*orders.metrics, DatasetMetric(name="revenue %", expression="SUM(amount)")]}))

        await _refused(add, name="revenue %")
        assert await storage.get_semantic_model_text("sales", data_source=DS) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_model_metric_slayer_references_quoted_stays_live(kind: str) -> None:
    async with document_engine(kind=kind, documents=(SALES,)) as engine:
        await engine.storage.edit_semantic_model(
            "sales", data_source=DS, metrics=[{"name": "revenue %", "expression": "SUM(orders.amount)"}])
        resp = await engine.execute({"metrics": ['"revenue %"']})
    assert resp.data == [{"revenue %": TOTAL}]


@pytest.mark.parametrize("kind", STORAGES)
async def test_authoring_a_dataset_slayer_cannot_reference_is_refused(kind: str, tmp_path: Path) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    await _refused(lambda: storage.save_dataset(table_dataset("a:b", sql_table="customers")), name="a:b")
    assert await storage.list_datasets(data_source=DS) == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_edit_of_a_dataset_carrying_an_inert_field_keeps_refusing_new_ones(kind: str) -> None:
    async with document_engine(kind=kind, documents=(INERT[0][2],)) as engine:
        storage = engine.storage

        async def add() -> None:
            orders = await storage.get_dataset("orders", data_source=DS)
            assert orders is not None
            await storage.save_dataset(orders.model_copy(update={
                "fields": [*orders.fields, DatasetField(name="tax.usd", expression="amount")]}))

        await _refused(add, name="tax.usd")
        stored = await storage.get_semantic_model_text("sales", data_source=DS)
        assert stored is not None
        assert _declares(stored, "amount.usd") and not _declares(stored, "tax.usd")


# --------------------------------------------------------------------------- #
# Length and model names
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_300_character_field_name_is_stored_as_written(kind: str) -> None:
    text = _doc(orders_fields=[field(LONG, "amount", datatype="Float")])
    async with document_engine(kind=kind, documents=(text,)) as engine:
        storage = engine.storage
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        assert LONG in {f.name for f in orders.fields}
        await storage.save_dataset(orders.model_copy(update={"description": "Edited."}))
        stored = await storage.get_semantic_model_text("sales", data_source=DS)
    assert stored is not None and _declares(stored, LONG)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_300_character_dataset_name_is_stored_as_written(kind: str, tmp_path: Path) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    name = "d" * 300
    await storage.save_dataset(table_dataset(name, sql_table="orders"), _validate=False)
    assert stored_dataset(kind, tmp_path, name)["name"] == name
    assert await reopen(kind, tmp_path).get_dataset(name, data_source=DS) is not None


MODEL_NAMES = ["Sales / EMEA", "x.y", "../evil", "Ümsatz 2024"]


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("name", MODEL_NAMES)
async def test_a_document_name_that_is_not_a_file_name(tmp_path: Path, kind: str, name: str) -> None:
    """Spec scenario: A document name that is not a file name."""
    storage = make_storage(kind, tmp_path)
    text = document_text(name=name, datasets=[dataset("orders", fields=[field("order_id")])])
    await store_document(storage, text)
    reopened = reopen(kind, tmp_path)
    assert await reopened.list_semantic_models(data_source=DS) == [(DS, name)]
    assert await reopened.get_semantic_model_text(name, data_source=DS) == text
    if kind == "yaml":
        root = folder(tmp_path)
        assert all(p.resolve().is_relative_to(root.resolve()) for p in (tmp_path / "store").rglob("*.yaml")
                   if "semantic_models" in p.parts)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_model_named_with_a_slash_exports_and_reimports(tmp_path: Path, kind: str) -> None:
    name = "Sales / EMEA"
    text = SALES.replace("name: sales\n", f"name: {name}\n", 1)
    for part in ("first", "second"):
        (tmp_path / part).mkdir()
    async with document_engine(kind=kind, documents=(text,), base=tmp_path / "first") as engine:
        exported = await export_osi(engine=engine, datasource=DS)
    assert exported.document["name"] == name
    path = tmp_path / "export.yaml"
    path.write_text(exported.text or yaml.safe_dump(exported.document, sort_keys=False), encoding="utf-8")
    target = make_storage(kind, tmp_path / "second")
    async with owned_query_engine(storage=target, datasources=[
            DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb"))]) as engine:
        await import_osi(storage=target, path=path, data_source=DS, engine=engine)
        assert await target.list_semantic_models(data_source=DS) == [(DS, name)]
        assert sorted(await target.list_datasets(data_source=DS)) == ["customers", "orders"]
