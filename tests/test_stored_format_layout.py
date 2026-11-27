"""Spec: models/stored-format (layout, datasource model, moves, file names) and models/semantic-model-documents
(layout-dependent scenarios)."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
import yaml
from httpx import ASGITransport, AsyncClient

from slayer.api.server import create_app
from slayer.core.models import SlayerDataset
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding, EntityKind
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._dev2243_fixtures import SALES, dataset, document_engine, document_text, error, field, store_document
from tests._dev2277_fixtures import by, customers_dataset, orders_dataset, returns_dataset, stores_dataset
from tests._stored_format_fixtures import (
    DS, STORAGES, dataset_documents, dataset_file, folder, legacy_folder, legacy_rows, make_storage, model_documents,
    link, model_file, parse, refs, slayer_entry, store_with_datasource, stored_dataset, table_dataset, texts,
    validate_dataset,
)

REGION_QUERY = {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}
RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def shop(model: SlayerDataset) -> SlayerDataset:
    return model.model_copy(update={"data_source": DS})


def readable_stem(name: str) -> str:
    """The stem before any hash suffix: the name, or its percent-encoding."""
    return "".join(chr(b) if re.fullmatch(r"[A-Za-z0-9_-]", chr(b)) else f"%{b:02X}" for b in name.encode("utf-8"))


def hash_suffix(name: str) -> str:
    return "-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]


def iri_of(file_name: str) -> str:
    """A file name percent-encoded once more as one path segment."""
    return quote(file_name, safe="")


async def doc_of(storage: StorageBackend, name: str) -> dict[str, Any]:
    text = await storage.get_semantic_model_text(name, data_source=DS)
    assert text is not None, name
    return yaml.safe_load(text)


async def must_get(storage: StorageBackend, name: str) -> SlayerDataset:
    found = await storage.get_dataset(name, data_source=DS)
    assert found is not None, name
    return found


async def single(engine: SlayerQueryEngine, metric_expr: str) -> float:
    resp = await engine.execute({"metrics": [{"metric": metric_expr, "alias": "v"}]})
    assert len(resp.data) == 1
    return float(resp.data[0]["v"])


def place(base: Path, file_name: str, content: dict[str, Any]) -> Path:
    path = folder(base) / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content, sort_keys=False), encoding="utf-8")
    return path


def cross_model_relationships(kind: str, base: Path, model: str) -> list[dict[str, Any]]:
    """The relationships model ``model`` holds in its ``SLAYER`` entry."""
    entry = slayer_entry(model_documents(kind, base)[model]) or {}
    return list(entry.get("relationships") or [])


def mentions(relationship: dict[str, Any], *names: str) -> bool:
    text = json.dumps(relationship)
    return all(f'"{n}"' in text for n in names)


def _no_database(*_: Any, **__: Any) -> Any:
    raise RuntimeError("no database access in this test")


def dataset_doc(entry: dict[str, Any]) -> dict[str, Any]:
    return {"version": "0.3.0.dev0", "dataset": entry}


_EMBEDDED: dict[str, EntityKind] = {f"{DS}.orders": "dataset", f"{DS}.orders.amount": "field",
                                    f"{DS}.customers": "dataset"}


async def save_embeddings(storage: StorageBackend) -> None:
    name = embedding_client.current_model()
    await storage.save_embeddings([Embedding(canonical_id=cid, embedding_model_name=name, entity_kind=kind,
                                             content_hash=cid, embedding=[1.0, 0.0]) for cid, kind in _EMBEDDED.items()])


async def embedded(storage: StorageBackend) -> set[str]:
    return {e.canonical_id for e in await storage.list_embeddings(embedding_model_name=embedding_client.current_model())}


# --------------------------------------------------------------------------- #
# Layout and document kinds
# --------------------------------------------------------------------------- #
async def test_a_standalone_dataset_is_a_dataset_document(tmp_path: Path) -> None:
    """Spec scenario: A standalone dataset is a dataset document."""
    storage = make_storage("yaml", tmp_path)
    await mcp_text(storage, "create_dataset", name="orders", sql_table="orders", data_source=DS,
                   fields=[{"name": "order_id", "datatype": "String"}, {"name": "amount", "datatype": "Float"}])
    path = folder(tmp_path) / "orders.dataset.yaml"
    assert path.is_file()
    doc = parse(path.read_text(encoding="utf-8"))
    assert set(doc) == {"version", "dataset"}
    assert doc["dataset"]["name"] == "orders"
    assert "data_source" not in doc["dataset"]
    validate_dataset(doc["dataset"])
    assert refs(model_documents("yaml", tmp_path)[DS]) == ["orders.dataset.yaml"]
    assert not legacy_folder(tmp_path).exists()


async def test_sqlite_holds_the_same_texts(tmp_path: Path) -> None:
    """Spec scenario: SQLite holds the same texts."""
    written: dict[str, dict[str, str]] = {}
    for kind in STORAGES:
        (tmp_path / kind).mkdir()
        storage = make_storage(kind, tmp_path / kind)
        await mcp_text(storage, "create_dataset", name="orders", sql_table="orders", data_source=DS,
                       fields=[{"name": "order_id", "datatype": "String"}])
        written[kind] = texts(kind, tmp_path / kind)
    assert written["sqlite"] == written["yaml"]
    assert "orders.dataset.yaml" in written["sqlite"]
    assert model_file("sqlite", tmp_path / "sqlite", DS) in written["sqlite"]
    assert legacy_rows(tmp_path / "sqlite", table="models") == []
    assert legacy_rows(tmp_path / "sqlite", table="semantic_models") == []


async def test_sqlite_stores_a_document_text_verbatim_under_its_file_name(tmp_path: Path) -> None:
    """Spec scenario: SQLite stores the text verbatim."""
    storage = make_storage("sqlite", tmp_path)
    await store_document(storage, SALES)
    assert await storage.get_semantic_model_text("sales", data_source=DS) == SALES
    assert texts("sqlite", tmp_path) == {"sales.semantic_model.yaml": SALES}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_new_model_document_is_a_semantic_model_file(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    assert texts(kind, tmp_path) == {"sales.semantic_model.yaml": SALES}


async def test_a_split_model_document_loads(tmp_path: Path) -> None:
    """Spec scenario: A split model document loads."""
    place(tmp_path, "orders.dataset.yaml", dataset_doc(dataset("orders", fields=[field("order_id")])))
    place(tmp_path, "customers.dataset.yaml", dataset_doc(dataset("customers", fields=[field("customer_id")])))
    place(tmp_path, "sales.semantic_model.yaml", {"version": "0.3.0.dev0", "name": "sales", "dataset_refs": [
        {"iri": "orders.dataset.yaml"}, {"iri": "customers.dataset.yaml"}]})
    storage = await store_with_datasource("yaml", tmp_path)
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert errs == []
    assert {d.name for d in loaded} == {"orders", "customers"}
    assert await storage.list_semantic_models(data_source=DS) == [(DS, "sales")]
    for name in ("orders", "customers"):
        assert await storage.semantic_model_owner(name, data_source=DS) == "sales"


# --------------------------------------------------------------------------- #
# The datasource model
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_the_datasource_model_is_created_by_its_first_dataset(tmp_path: Path, kind: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    assert await storage.list_semantic_models(data_source=DS) == []
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    assert await storage.list_semantic_models(data_source=DS) == [(DS, DS)]
    doc = await doc_of(storage, DS)
    assert doc["name"] == DS
    assert refs(doc) == [dataset_file(kind, tmp_path, "orders")]
    assert await storage.semantic_model_owner("orders", data_source=DS) == DS


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_without_a_document_joins_the_datasource_model(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A dataset without a document joins the datasource model."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await mcp_text(storage, "create_dataset", name="returns", sql_table="returns", data_source=DS,
                   fields=[{"name": "return_id", "datatype": "String"}])
    assert await storage.semantic_model_owner("returns", data_source=DS) == DS
    assert await storage.get_semantic_model_text("sales", data_source=DS) == SALES
    assert refs(await doc_of(storage, DS)) == [dataset_file(kind, tmp_path, "returns")]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_model_scoped_metric_over_standalone_datasets(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A model-scoped metric over standalone datasets."""
    async with document_engine(kind=kind, documents=(), base=tmp_path) as engine:
        storage = engine.storage
        for model in (customers_dataset(), orders_dataset()):
            await storage.save_dataset(shop(model))
        await storage.edit_semantic_model(DS, data_source=DS, metrics=[{
            "name": "revenue_per_customer", "expression": "SUM(orders.amount) / COUNT(DISTINCT customers.customer_id)"}])
        assert await single(engine, "revenue_per_customer") == 162.5
        assert (DS, DS) in await storage.list_semantic_models(data_source=DS)


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_last_dataset_leaving_deletes_the_datasource_model(tmp_path: Path, kind: str) -> None:
    """Spec scenario: The last dataset leaving deletes the datasource model."""
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    assert await storage.list_semantic_models(data_source=DS) == [(DS, DS)]
    assert await storage.delete_dataset("orders", data_source=DS) is True
    assert await storage.list_semantic_models(data_source=DS) == []
    assert await storage.get_semantic_model_text(DS, data_source=DS) is None
    assert texts(kind, tmp_path) == {}


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_the_datasource_model_cascades_per_member(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await storage.save_dataset(shop(customers_dataset()), _validate=False)
    await storage.save_dataset(shop(orders_dataset()), _validate=False)
    await save_embeddings(storage)
    await storage.save_memory(id="m1", learning="Orders", entities=[f"{DS}.orders", f"{DS}.customers.region"])
    await storage.save_memory(id="m2", learning="Other", entities=[f"{DS}.returns"])
    assert await storage.delete_semantic_model(DS, data_source=DS) is True
    assert await storage.list_semantic_models(data_source=DS) == []
    assert await storage.load_datasets(data_source=DS) == ([], [])
    assert texts(kind, tmp_path) == {}
    assert await embedded(storage) == set()
    assert (await storage.get_memory("m1")).entities == []
    assert (await storage.get_memory("m2")).entities == [f"{DS}.returns"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_creating_a_document_named_like_the_datasource_model_is_refused(tmp_path: Path, kind: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    before = texts(kind, tmp_path)
    text = document_text(name=DS, datasets=[dataset("returns", fields=[field("return_id")])])
    with pytest.raises(error("NamespaceCollisionError")) as info:
        await storage.create_semantic_model(data_source=DS, content=text)
    assert DS in str(info.value)
    assert texts(kind, tmp_path) == before


async def test_importing_a_document_named_like_the_datasource_model_is_refused(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)
    storage = await store_with_datasource("yaml", tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    before = texts("yaml", tmp_path)
    path = tmp_path / "in" / f"{DS}.yaml"
    path.parent.mkdir()
    path.write_text(document_text(name=DS, datasets=[dataset("returns", fields=[field("return_id")])]), encoding="utf-8")
    result = run_cli_in_process(["import-osi", str(path), "--datasource", DS, "--storage", str(tmp_path / "store")])
    assert result.returncode != 0, result.stdout + result.stderr
    assert DS in result.stderr
    assert texts("yaml", tmp_path) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_replacing_the_datasource_models_text_is_a_replace(tmp_path: Path, kind: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    stored = await storage.get_semantic_model_text(DS, data_source=DS)
    assert stored is not None
    replaced = stored + "description: Replaced.\n"
    await storage.save_semantic_model_text(data_source=DS, name=DS, text=replaced)
    assert await storage.get_semantic_model_text(DS, data_source=DS) == replaced
    assert await storage.semantic_model_owner("orders", data_source=DS) == DS
    with pytest.raises(error("OssieDocumentInvalidError")):
        await storage.save_semantic_model_text(data_source=DS, name=DS, text=stored + "owner: me\n")
    assert await storage.get_semantic_model_text(DS, data_source=DS) == replaced


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_document_named_like_the_datasource_creates_its_datasource_model(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, document_text(name=DS, datasets=[dataset("customers", fields=[field("customer_id")])]))
    assert await storage.list_semantic_models(data_source=DS) == [(DS, DS)]
    await mcp_text(storage, "create_dataset", name="orders", sql_table="orders", data_source=DS,
                   fields=[{"name": "order_id", "datatype": "String"}])
    assert await storage.semantic_model_owner("orders", data_source=DS) == DS
    doc = model_documents(kind, tmp_path)[DS]
    assert [d["name"] for d in doc["datasets"]] == ["customers"]
    assert refs(doc) == [dataset_file(kind, tmp_path, "orders")]


async def test_a_same_named_2_0_document_is_the_datasource_model(tmp_path: Path) -> None:
    place(tmp_path, f"{DS}.yaml", yaml.safe_load(document_text(
        name=DS, datasets=[dataset("customers", fields=[field("customer_id")])])))
    storage = await store_with_datasource("yaml", tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    assert await storage.semantic_model_owner("orders", data_source=DS) == DS
    assert await storage.list_semantic_models(data_source=DS) == [(DS, DS)]
    assert model_file("yaml", tmp_path, DS) == f"{DS}.yaml"
    doc = model_documents("yaml", tmp_path)[DS]
    assert [d["name"] for d in doc["datasets"]] == ["customers"]
    assert refs(doc) == ["orders.dataset.yaml"]


# --------------------------------------------------------------------------- #
# Moves
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_move_keeps_the_dataset_file(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A move keeps the dataset file."""
    async with document_engine(kind=kind, documents=(), base=tmp_path) as engine:
        storage = engine.storage
        for model in (customers_dataset(), orders_dataset()):
            await storage.save_dataset(shop(model))
        file = dataset_file(kind, tmp_path, "orders")
        before = texts(kind, tmp_path)[file]
        revenue = await single(engine, "sum(orders.amount)")
        await storage.create_semantic_model(name="sales", data_source=DS, datasets=["orders"])
        assert texts(kind, tmp_path)[file] == before
        models = model_documents(kind, tmp_path)
        assert refs(models["sales"]) == [file]
        assert file not in refs(models[DS])
        assert await storage.semantic_model_owner("orders", data_source=DS) == "sales"
        assert await single(engine, "sum(orders.amount)") == revenue


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_move_takes_the_relationships_of_its_datasets(tmp_path: Path, kind: str) -> None:
    async with document_engine(kind=kind, documents=(), base=tmp_path) as engine:
        storage = engine.storage
        for model in (customers_dataset(), orders_dataset()):
            await storage.save_dataset(shop(model))
        before = {n: texts(kind, tmp_path)[dataset_file(kind, tmp_path, n)] for n in ("orders", "customers")}
        assert [r["from"] for r in model_documents(kind, tmp_path)[DS]["relationships"]] == ["orders"]
        revenue = by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue")
        await storage.create_semantic_model(name="sales", data_source=DS, datasets=["orders", "customers"])
        after = texts(kind, tmp_path)
        assert {n: after[dataset_file(kind, tmp_path, n)] for n in ("orders", "customers")} == before
        models = model_documents(kind, tmp_path)
        assert DS not in models
        assert [(r["from"], r["to"]) for r in models["sales"]["relationships"]] == [("orders", "customers")]
        assert by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue") == revenue


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_cross_model_relationship_is_held_by_the_model_of_its_from_dataset(tmp_path: Path, kind: str) -> None:
    async with document_engine(kind=kind, documents=(), base=tmp_path) as engine:
        storage = engine.storage
        for model in (customers_dataset(), orders_dataset()):
            await storage.save_dataset(shop(model))
        file = dataset_file(kind, tmp_path, "orders")
        before = texts(kind, tmp_path)[file]
        revenue = by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue")
        await storage.create_semantic_model(name="sales", data_source=DS, datasets=["orders"])
        assert texts(kind, tmp_path)[file] == before
        models = model_documents(kind, tmp_path)
        assert not models[DS].get("relationships")
        assert not models["sales"].get("relationships")
        held = cross_model_relationships(kind, tmp_path, "sales")
        assert len(held) == 1
        assert mentions(held[0], "orders", "customers")
        assert cross_model_relationships(kind, tmp_path, DS) == []
        assert slayer_entry(stored_dataset(kind, tmp_path, "orders")) is None
        assert by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue") == revenue


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_relationship_saved_before_its_target_lands_in_core_with_it(tmp_path: Path, kind: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset("orders", fields=["id", "customer_id"], relationships=[
        link("customers", from_columns=["customer_id"], to_columns=["id"])]), _validate=False)
    await storage.save_dataset(table_dataset("customers", fields=["id"]), _validate=False)
    held = model_documents(kind, tmp_path)[DS]
    assert [(r["from"], r["to"]) for r in held["relationships"]] == [("orders", "customers")]
    assert not (slayer_entry(held) or {}).get("relationships")
    assert [r.to for r in (await must_get(reopen_store(kind, tmp_path), "orders")).relationships] == ["customers"]


def reopen_store(kind: str, base: Path) -> StorageBackend:
    return make_storage(kind, base)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_reverse_cross_model_relationship_stays_with_the_datasource_model(tmp_path: Path, kind: str) -> None:
    async with document_engine(kind=kind, documents=(), base=tmp_path) as engine:
        storage = engine.storage
        customers = shop(customers_dataset()).model_copy(update={"relationships": [
            link("orders", from_columns=["customer_id"], to_columns=["customer_id"])]})
        await storage.save_dataset(customers)
        await storage.save_dataset(shop(orders_dataset()).model_copy(update={"relationships": []}))
        query = {"population": "customers", "dimensions": ["customers.region"], "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]}
        before_rows = by(await engine.execute(query), "customers.region", "rev")
        files = {n: texts(kind, tmp_path)[dataset_file(kind, tmp_path, n)] for n in ("orders", "customers")}
        await storage.create_semantic_model(name="sales", data_source=DS, datasets=["orders"])
        assert {n: texts(kind, tmp_path)[dataset_file(kind, tmp_path, n)] for n in ("orders", "customers")} == files
        models = model_documents(kind, tmp_path)
        assert not models[DS].get("relationships")
        held = cross_model_relationships(kind, tmp_path, DS)
        assert len(held) == 1
        assert mentions(held[0], "customers", "orders")
        assert cross_model_relationships(kind, tmp_path, "sales") == []
        assert by(await engine.execute(query), "customers.region", "rev") == before_rows


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_move_back_into_the_datasource_model(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A move back into the datasource model."""
    storage = make_storage(kind, tmp_path)
    for model in (customers_dataset(), orders_dataset(), stores_dataset()):
        await storage.save_dataset(shop(model), _validate=False)
    await storage.create_semantic_model(name="sales", data_source=DS, datasets=["orders", "stores"])
    file = dataset_file(kind, tmp_path, "orders")
    before = texts(kind, tmp_path)[file]
    await storage.edit_semantic_model(DS, data_source=DS, add_datasets=["orders"])
    assert texts(kind, tmp_path)[file] == before
    assert await storage.semantic_model_owner("orders", data_source=DS) == DS
    models = model_documents(kind, tmp_path)
    assert file in refs(models[DS])
    assert file not in refs(models["sales"])
    assert [(r["from"], r["to"]) for r in models[DS]["relationships"]] == [("orders", "customers")]
    assert cross_model_relationships(kind, tmp_path, "sales") != []
    assert not any(mentions(r, "orders") for r in cross_model_relationships(kind, tmp_path, "sales"))


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_moves_between_named_models_by_its_reference(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    for model in (customers_dataset(), orders_dataset(), returns_dataset(), stores_dataset()):
        await storage.save_dataset(shop(model), _validate=False)
    await storage.create_semantic_model(name="sales", data_source=DS, datasets=["orders", "customers", "stores"])
    await storage.create_semantic_model(name="support", data_source=DS, datasets=["returns"])
    before = texts(kind, tmp_path)
    files = {n: dataset_file(kind, tmp_path, n) for n in ("orders", "customers")}
    await storage.edit_semantic_model("support", data_source=DS, add_datasets=["orders"])
    await storage.create_semantic_model(name="archive", data_source=DS, datasets=["customers"])
    after = texts(kind, tmp_path)
    assert {n: after[f] for n, f in files.items()} == {n: before[f] for n, f in files.items()}
    assert await storage.semantic_model_owner("orders", data_source=DS) == "support"
    assert await storage.semantic_model_owner("customers", data_source=DS) == "archive"
    models = model_documents(kind, tmp_path)
    assert refs(models["sales"]) == [dataset_file(kind, tmp_path, "stores")]
    assert sorted(refs(models["support"])) == sorted([dataset_file(kind, tmp_path, "returns"), files["orders"]])
    assert refs(models["archive"]) == [files["customers"]]
    assert DS not in models


@pytest.mark.parametrize("kind", STORAGES)
async def test_add_datasets_moves_the_reference_into_a_named_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(shop(returns_dataset()), _validate=False)
    file = dataset_file(kind, tmp_path, "returns")
    before = texts(kind, tmp_path)[file]
    await storage.edit_semantic_model("sales", data_source=DS, add_datasets=["returns"])
    assert texts(kind, tmp_path)[file] == before
    models = model_documents(kind, tmp_path)
    assert refs(models["sales"]) == [file]
    assert DS not in models
    assert await storage.semantic_model_owner("returns", data_source=DS) == "sales"


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_inline_dataset_stays_inline_when_its_model_is_edited(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(shop(returns_dataset()), _validate=False)
    await storage.edit_semantic_model("sales", data_source=DS, description="Edited.", add_datasets=["returns"])
    orders = await must_get(storage, "orders")
    await storage.save_dataset(orders.model_copy(update={"description": "Edited orders."}))
    doc = model_documents(kind, tmp_path)["sales"]
    assert [d["name"] for d in doc["datasets"]] == ["orders", "customers"]
    assert refs(doc) == [dataset_file(kind, tmp_path, "returns")]
    assert {d["dataset"]["name"] for d in dataset_documents(kind, tmp_path).values()} == {"returns"}


def into_sales(model: SlayerDataset) -> SlayerDataset:
    return shop(model).model_copy(update={"relationships": [], "semantic_model": "sales"})


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_created_in_a_flat_model_is_inline(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    files = set(texts(kind, tmp_path))
    await storage.save_dataset(into_sales(returns_dataset()), _validate=False)
    assert set(texts(kind, tmp_path)) == files
    doc = model_documents(kind, tmp_path)["sales"]
    assert [d["name"] for d in doc["datasets"]] == ["orders", "customers", "returns"]
    assert "dataset_refs" not in doc
    assert dataset_documents(kind, tmp_path) == {}
    assert await storage.semantic_model_owner("returns", data_source=DS) == "sales"


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_created_in_a_split_model_is_a_dataset_document(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    await storage.save_dataset(shop(stores_dataset()).model_copy(update={"relationships": []}), _validate=False)
    await storage.edit_semantic_model("sales", data_source=DS, add_datasets=["stores"])
    await storage.save_dataset(into_sales(returns_dataset()), _validate=False)
    doc = model_documents(kind, tmp_path)["sales"]
    assert [d["name"] for d in doc["datasets"]] == ["orders", "customers"]
    assert refs(doc) == [dataset_file(kind, tmp_path, "stores"), dataset_file(kind, tmp_path, "returns")]
    assert stored_dataset(kind, tmp_path, "returns")["name"] == "returns"
    assert await storage.semantic_model_owner("returns", data_source=DS) == "sales"


async def test_native_authoring_without_an_ossie_file(tmp_path: Path) -> None:
    """Spec scenario: Native authoring without an Ossie file."""
    async with document_engine(documents=(), base=tmp_path) as engine:
        storage = engine.storage
        for model in (customers_dataset(), orders_dataset(buyer=True)):
            await storage.save_dataset(shop(model))
        before = {n: (await must_get(storage, n)).model_dump(mode="json", exclude={"version", "semantic_model"})
                  for n in ("orders", "customers")}
        files = {n: texts("yaml", tmp_path)[dataset_file("yaml", tmp_path, n)] for n in ("orders", "customers")}
        revenue = by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue")
        await mcp_text(storage, "create_semantic_model", name="sales", data_source=DS, datasets=["orders", "customers"])
        await mcp_text(storage, "edit_semantic_model", name="sales", data_source=DS, metrics=[{
            "name": "revenue_per_customer",
            "expression": "AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)"}])
        for name in ("orders", "customers"):
            assert await storage.semantic_model_owner(name, data_source=DS) == "sales"
            assert (await must_get(storage, name)).model_dump(
                mode="json", exclude={"version", "semantic_model"}) == before[name]
            assert texts("yaml", tmp_path)[dataset_file("yaml", tmp_path, name)] == files[name]
        assert await storage.list_semantic_models(data_source=DS) == [(DS, "sales")]
        assert by(await engine.execute(REGION_QUERY), "customers.region", "orders.total_revenue") == revenue
        assert await single(engine, "revenue_per_customer") == 162.5


# --------------------------------------------------------------------------- #
# File names
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_name_that_is_not_a_file_name(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A name that is not a file name."""
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset("orders/2024 Q1", sql_table="orders"), _validate=False)
    assert dataset_file(kind, tmp_path, "orders/2024 Q1") == "orders%2F2024%20Q1.dataset.yaml"
    assert refs(model_documents(kind, tmp_path)[DS]) == ["orders%252F2024%2520Q1.dataset.yaml"]
    assert await storage.list_datasets(data_source=DS) == ["orders/2024 Q1"]


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("name", [
    pytest.param("a/b", id="slash"), pytest.param("a\\b", id="backslash"),
    pytest.param("café_ü", id="unicode"), pytest.param("x" * 120, id="120-bytes"), pytest.param("ok-name_1", id="plain"),
])
async def test_a_readable_stem_is_the_name_or_its_percent_encoding(tmp_path: Path, kind: str, name: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset(name, sql_table="orders"), _validate=False)
    file = dataset_file(kind, tmp_path, name)
    assert file == f"{readable_stem(name)}.dataset.yaml"
    assert refs(model_documents(kind, tmp_path)[DS]) == [iri_of(file)]
    assert (await must_get(storage, name)).name == name


def _merged_export(*, rest: str) -> dict[str, Any]:
    """A merged export of ``DS`` recording the document ``support``; dataset ``rest`` belongs to no recorded document."""
    return {"version": "0.3.0.dev0", "name": DS,
            "datasets": [dataset(rest, source="orders", fields=[field("order_id")]),
                         dataset("tickets", source="tickets", fields=[field("ticket_id")])],
            "nonportable_extensions": [{"vendor_name": "SLAYER", "data": {
                "version": 3, "kind": "semantic_model", "entity": DS,
                "documents": [{"name": "support", "datasets": ["tickets"]}]}}]}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_named_dotdot_is_stored_under_its_encoding(tmp_path: Path, kind: str,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)
    await store_with_datasource(kind, tmp_path)
    path = tmp_path / "in" / "export.yaml"
    path.parent.mkdir()
    path.write_text(yaml.safe_dump(_merged_export(rest=".."), sort_keys=False), encoding="utf-8")
    where = ["--storage", str(tmp_path / "store") if kind == "yaml" else str(tmp_path / "store.db")]
    result = run_cli_in_process(["import-osi", str(path), "--datasource", DS, *where])
    assert result.returncode == 0, result.stdout + result.stderr
    assert dataset_file(kind, tmp_path, "..") == "%2E%2E.dataset.yaml"
    assert refs(model_documents(kind, tmp_path)[DS]) == [iri_of("%2E%2E.dataset.yaml")]
    assert stored_dataset(kind, tmp_path, "..")["name"] == ".."
    if kind == "yaml":
        root = folder(tmp_path).resolve()
        assert all(p.resolve().is_relative_to(root) for p in (tmp_path / "store").rglob("*.dataset.yaml"))


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("name", [
    pytest.param("x" * 121, id="121-bytes"), pytest.param("y" * 128, id="128-chars"),
    pytest.param("z" * 400, id="400-chars"), pytest.param("é" * 70, id="multibyte-over-120"),
    pytest.param("CON", id="reserved-device"),
])
async def test_a_long_or_reserved_stem_is_cut_and_hashed(tmp_path: Path, kind: str, name: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    await storage.save_dataset(table_dataset(name, sql_table="orders"), _validate=False)
    file = dataset_file(kind, tmp_path, name)
    stem = file.removesuffix(".dataset.yaml")
    assert stem.endswith(hash_suffix(name))
    assert len(stem.encode("utf-8")) <= 120
    assert readable_stem(name).startswith(stem.removesuffix(hash_suffix(name)))
    assert stem.upper() not in RESERVED
    assert refs(model_documents(kind, tmp_path)[DS]) == [iri_of(file)]
    assert (await must_get(storage, name)).name == name


async def _with_split_sales(storage: StorageBackend) -> None:
    """``orders`` in the datasource model, then the split model ``sales`` made from a seed dataset."""
    await storage.save_dataset(table_dataset("orders", fields=["id", "lower"]), _validate=False)
    await storage.save_dataset(table_dataset("seed"), _validate=False)
    await storage.create_semantic_model(name="sales", data_source=DS, datasets=["seed"])


def _upper_orders() -> SlayerDataset:
    return table_dataset("Orders", sql_table="orders", fields=["id", "upper"], semantic_model="sales")


@pytest.mark.parametrize("kind", STORAGES)
async def test_names_differing_only_in_case(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Names differing only in case."""
    storage = await store_with_datasource(kind, tmp_path)
    await _with_split_sales(storage)
    await storage.save_dataset(_upper_orders(), _validate=False)
    lower, upper = dataset_file(kind, tmp_path, "orders"), dataset_file(kind, tmp_path, "Orders")
    assert lower == "orders.dataset.yaml"
    assert upper == f"Orders{hash_suffix('Orders')}.dataset.yaml"
    assert lower.casefold() != upper.casefold()
    models = model_documents(kind, tmp_path)
    assert refs(models[DS]) == [iri_of(lower)]
    assert iri_of(upper) in refs(models["sales"])
    assert [f["name"] for f in stored_dataset(kind, tmp_path, "orders")["fields"]] == ["id", "lower"]
    assert [f["name"] for f in stored_dataset(kind, tmp_path, "Orders")["fields"]] == ["id", "upper"]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_file_name_is_kept_while_the_name_stays(tmp_path: Path, kind: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    await _with_split_sales(storage)
    await storage.save_dataset(_upper_orders(), _validate=False)
    hashed = dataset_file(kind, tmp_path, "Orders")
    assert await storage.delete_dataset("orders", data_source=DS) is True
    upper = await must_get(storage, "Orders")
    await storage.save_dataset(upper.model_copy(update={"description": "Edited."}), _validate=False)
    assert dataset_file(kind, tmp_path, "Orders") == hashed
    seed = iri_of(dataset_file(kind, tmp_path, "seed"))
    assert [r for r in refs(model_documents(kind, tmp_path)["sales"]) if r != seed] == [iri_of(hashed)]


async def test_a_2_0_model_document_keeps_its_file_name(tmp_path: Path) -> None:
    place(tmp_path, "sales.yaml", yaml.safe_load(SALES))
    storage = await store_with_datasource("yaml", tmp_path)
    await storage.edit_semantic_model("sales", data_source=DS, description="Edited.")
    assert model_file("yaml", tmp_path, "sales") == "sales.yaml"
    assert set(texts("yaml", tmp_path)) == {"sales.yaml"}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_document_name_that_is_not_a_file_name(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A document name that is not a file name."""
    storage = make_storage(kind, tmp_path)
    text = document_text(name="Sales / EMEA", datasets=[dataset("orders", fields=[field("order_id")])])
    await storage.save_semantic_model_text(data_source=DS, name="Sales / EMEA", text=text)
    assert await storage.list_semantic_models(data_source=DS) == [(DS, "Sales / EMEA")]
    assert await storage.get_semantic_model_text("Sales / EMEA", data_source=DS) == text
    assert set(texts(kind, tmp_path)) == {"Sales%20%2F%20EMEA.semantic_model.yaml"}
    assert await storage.semantic_model_owner("orders", data_source=DS) == "Sales / EMEA"


# --------------------------------------------------------------------------- #
# Surfaces
# --------------------------------------------------------------------------- #
async def test_inspect_and_summary_show_the_datasource_model() -> None:
    async with document_engine(documents=()) as engine:
        for model in (customers_dataset(), orders_dataset()):
            await engine.storage.save_dataset(shop(model))
        inspected = await mcp_text(engine.storage, "inspect", reference=f"{DS}.{DS}", entity_type="semantic_model")
        for token in ("orders", "customers"):
            assert token in inspected
        summary = await mcp_text(engine.storage, "datasets_summary", datasource_name=DS)
        assert f"Semantic model: `{DS}`" in summary


async def test_the_cli_lists_and_shows_the_datasource_model(tmp_path: Path) -> None:
    storage = await store_with_datasource("yaml", tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    where = ["--storage", str(tmp_path / "store")]
    listed = run_cli_in_process(["semantic-models", "list", "--datasource", DS, *where])
    assert listed.returncode == 0, listed.stderr
    assert DS in listed.stdout
    assert "orders" in listed.stdout
    shown = run_cli_in_process(["semantic-models", "show", DS, "--datasource", DS, *where])
    assert shown.returncode == 0, shown.stderr
    assert "orders.dataset.yaml" in shown.stdout


async def test_rest_lists_and_reads_the_datasource_model(tmp_path: Path) -> None:
    storage = await store_with_datasource("yaml", tmp_path)
    await storage.save_dataset(table_dataset("orders"), _validate=False)
    async with AsyncClient(transport=ASGITransport(app=create_app(storage=storage)), base_url="http://t") as client:
        listed = await client.get("/semantic-models", params={"data_source": DS})
        assert listed.status_code == 200, listed.text
        assert f'"{DS}"' in listed.text
        got = await client.get(f"/semantic-models/{DS}/{DS}")
        assert got.status_code == 200, got.text
        assert refs(yaml.safe_load(got.json()["text"])) == ["orders.dataset.yaml"]
