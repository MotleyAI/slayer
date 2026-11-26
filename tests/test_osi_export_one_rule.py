"""Spec: importers/osi-export — one export rule for every dataset of every model, the datasource model included."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.osi.exporter import export_osi
from slayer.sql.ossie_source import parse_source
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import seed_backend
from tests._dev2243_fixtures import (
    DS, SALES, dataset, document_text, field, importer_module, make_storage, semantic_model_module, store_document,
)
from tests._dev2277_fixtures import SEED_TABLES
from tests._engine_helpers import owned_query_engine
from tests._osi_export_fixtures import customers
from tests._osi_export_helpers import dataset as exported_dataset
from tests._osi_export_helpers import export, field as exported_field, make_env, run_sql, save, stored, validate
from tests._stored_format_fixtures import slayer_entry, stored_dataset

SUPPORT = document_text(name="support", description="Returns.", datasets=[
    dataset("returns", primary_key=["return_id"], fields=[field("return_id"), field("customer_id"),
                                                          field("amount", datatype="Float")]),
])
STORES = SlayerDataset(
    name="stores", sql_table="stores", data_source=DS,
    fields=[DatasetField(name="store_id", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.STRING)],
    relationships=[DatasetRelationship(name="stores_to_orders", to="orders", from_columns=["customer_id"],
                                       to_columns=["customer_id"],
                                       cardinality=RelationshipCardinality.MANY_TO_ONE)],
)
MANAGERS = SlayerDataset(
    name="managers", sql_table="managers", data_source=DS,
    fields=[DatasetField(name="manager_id", datatype=DataType.STRING, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING)],
)
ORDERS_DOC = document_text(name="sales", datasets=[
    dataset("orders", primary_key=["o_orderkey"], fields=[field("o_orderkey", datatype="Integer"), field("status"),
                                                          field("amount", datatype="Float")]),
])
STATUS_UP = {"dialects": [{"dialect": "DUCKDB", "expression": "upper(status)"},
                          {"dialect": "SNOWFLAKE", "expression": "UPPER(status)"}]}


def facts_model(name: str) -> str:
    """The datasource model ``name`` declaring ``facts`` with Ossie keys SLayer's own shape cannot hold."""
    return document_text(name=name, datasets=[dataset(
        "facts", source="orders", unique_keys=[["o_custkey", "region"]],
        fields=[field("o_custkey", datatype="Integer"), field("region"),
                field("ordered_on", datatype="Date", dimension={"is_time": False}),
                {"name": "status_up", "expression": STATUS_UP, "datatype": "String"}])])


def seeded_config(base: Path) -> DatasourceConfig:
    base.mkdir(parents=True, exist_ok=True)
    db = base / "seed.duckdb"
    seed_backend("duckdb", str(db), SEED_TABLES)
    return DatasourceConfig(name=DS, type="duckdb", database=str(db))


@contextlib.asynccontextmanager
async def engine_over(base: Path, config: DatasourceConfig, *, documents: tuple[str, ...] = (),
                      datasets: tuple[SlayerDataset, ...] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over a fresh YAML store in ``base`` holding ``documents`` and datasource-model ``datasets``."""
    async with owned_query_engine(storage=make_storage("yaml", base), datasources=[config]) as engine:
        for text in documents:
            await store_document(engine.storage, text, data_source=config.name)
        for model in datasets:
            await engine.save_dataset(model.model_copy(update={"data_source": config.name}))
        yield engine


async def import_text(storage: StorageBackend, text: str, *, base: Path) -> None:
    path = base / "exported.osi.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    result = await importer_module().import_osi(storage=storage, path=path, data_source=DS)
    assert result.refused == [], result.refused


def assert_valid(doc: dict[str, Any]) -> None:
    validate(doc)
    assert semantic_model_module().ossie_validity_errors(doc) == []


# --------------------------------------------------------------------------- #
# SLAYER content that changes what core means
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("extensions", [True, False], ids=["extensions", "no-extensions"])
async def test_a_slayer_row_filter_exports_as_a_filtered_source(tmp_path: Path, extensions: bool) -> None:
    """Spec scenario: A SLayer row filter exports as a filtered source."""
    env = await make_env(tmp_path)
    await YAMLStorage(base_dir=str(env.store)).save_semantic_model_text(
        data_source=env.datasource, name="sales", text=ORDERS_DOC)
    orders = await stored(env, "orders")
    await save(env, orders.model_copy(update={"filters": ["status <> 'test'"]}))
    result = await export(env, extensions=extensions)
    assert_valid(result.document)
    source = exported_dataset(result.document, "orders")["source"]
    assert parse_source(source).is_query
    assert sorted(r[0] for r in run_sql(env, f"SELECT o_orderkey FROM ({source}) AS t")) == [1, 2, 3, 5]


# --------------------------------------------------------------------------- #
# Stored Ossie keys
# --------------------------------------------------------------------------- #
async def test_stored_ossie_keys_export_as_stored(tmp_path: Path) -> None:
    """Spec scenario: Stored Ossie keys export as stored."""
    env = await make_env(tmp_path)
    storage = YAMLStorage(base_dir=str(env.store))
    await storage.create_semantic_model(data_source=env.datasource, content=facts_model(env.datasource))
    await save(env, customers())
    assert await storage.semantic_model_owner("customers", env.datasource) == env.datasource
    result = await export(env)
    assert_valid(result.document)
    facts = exported_dataset(result.document, "facts")
    assert facts["unique_keys"] == [["o_custkey", "region"]]
    ordered_on = exported_field(facts, "ordered_on")
    assert ordered_on is not None
    assert ordered_on["dimension"] == {"is_time": False}
    status_up = exported_field(facts, "status_up")
    assert status_up is not None
    assert status_up["expression"] == STATUS_UP


async def test_a_datasource_model_exports_its_datasets_as_stored(tmp_path: Path) -> None:
    config = seeded_config(tmp_path)
    async with engine_over(tmp_path, config, datasets=(STORES.model_copy(update={"relationships": []}),
                                                       MANAGERS)) as engine:
        first = await export_osi(engine=engine, datasource=DS, extensions=True)
        second = await export_osi(engine=engine, datasource=DS, extensions=True)
    assert first.text == second.text
    assert_valid(first.document)
    for name in ("managers", "stores"):
        kept = stored_dataset("yaml", tmp_path, name)
        exported = exported_dataset(first.document, name)
        assert {k: v for k, v in exported.items() if k != "nonportable_extensions"} == {
            k: v for k, v in kept.items() if k != "nonportable_extensions"}


# --------------------------------------------------------------------------- #
# Merge and re-import
# --------------------------------------------------------------------------- #
async def test_two_documents_and_a_datasource_model_dataset_merge(tmp_path: Path) -> None:
    """Spec scenario: Two documents and a standalone dataset merge."""
    config = seeded_config(tmp_path / "src")
    async with engine_over(tmp_path / "src", config, documents=(SALES, SUPPORT), datasets=(STORES,)) as engine:
        result = await export_osi(engine=engine, datasource=DS, extensions=True)
    doc = yaml.safe_load(result.text)
    assert_valid(doc)
    assert doc["name"] == DS
    assert [d["name"] for d in doc["datasets"]] == ["customers", "orders", "returns", "stores"]
    assert any(r["from"] == "stores" and r["to"] == "orders" for r in doc["relationships"])
    assert slayer_entry(doc) is not None
    storage = make_storage("yaml", tmp_path / "dst")
    await storage.save_datasource(config)
    await import_text(storage, result.text, base=tmp_path / "dst")
    assert sorted(await storage.list_semantic_models(DS)) == [(DS, DS), (DS, "sales"), (DS, "support")]
    assert await storage.semantic_model_owner("orders", DS) == "sales"
    assert await storage.semantic_model_owner("returns", DS) == "support"
    assert await storage.semantic_model_owner("stores", DS) == DS
    restored = await storage.get_dataset("stores", data_source=DS)
    assert restored is not None
    assert [r.to for r in restored.relationships] == ["orders"]


async def test_the_merged_export_is_deterministic(tmp_path: Path) -> None:
    async with engine_over(tmp_path, seeded_config(tmp_path), documents=(SALES, SUPPORT), datasets=(STORES,)) as engine:
        first = await export_osi(engine=engine, datasource=DS, extensions=True)
        second = await export_osi(engine=engine, datasource=DS, extensions=True)
        assert await engine.storage.semantic_model_owner("stores", DS) == DS
    assert first.text == second.text
