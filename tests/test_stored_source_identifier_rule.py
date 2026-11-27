"""A stored ``source`` reads by Ossie's identifier rule and every writer spells it so both rules name one table.

Spec: models/table-names › A document source reads by Ossie's identifier rule; › Every stored source names the table
SLayer reads.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.osi.exporter import export_osi
from tests._dev2243_fixtures import dataset, document_text, field, store_document
from tests._dev2171_fixtures import collect_over_engine
from tests._engine_helpers import disposable_engine, owned_query_engine
from tests._storage_upgrade_cli import store_path, upgrade
from tests._stored_format_fixtures import (
    DS, STORAGES, make_storage, model_documents, refs, reopen, stored_dataset, texts,
)
from tests._stored_upgrade_fixtures import raw_store

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

#: Every table holds one row whose ``v`` tells it apart from its case twins.
SEED = (
    "CREATE TABLE public.orders (v INTEGER, order_id INTEGER, customer_id INTEGER, amount INTEGER)",
    "INSERT INTO public.orders VALUES (7, 1, 1, 70)",
    'CREATE TABLE public."Orders" (v INTEGER, order_id INTEGER, customer_id INTEGER, amount INTEGER)',
    'INSERT INTO public."Orders" VALUES (8, 1, 1, 100)',
    "CREATE TABLE public.customers (id INTEGER, region TEXT)", "INSERT INTO public.customers VALUES (1, 'North')",
    'CREATE TABLE public."ArtifactsCore" (v INTEGER)', 'INSERT INTO public."ArtifactsCore" VALUES (6)',
    "CREATE TABLE public.artifactscore (v INTEGER)", "INSERT INTO public.artifactscore VALUES (60)",
    'CREATE SCHEMA "sales.v2"', 'CREATE TABLE "sales.v2".orders (v INTEGER)', 'INSERT INTO "sales.v2".orders VALUES (1)',
    "CREATE SCHEMA sales", 'CREATE TABLE sales."order items" (v INTEGER)', 'INSERT INTO sales."order items" VALUES (2)',
    "CREATE TABLE sales.orders (v INTEGER)", "INSERT INTO sales.orders VALUES (99)",
    'CREATE SCHEMA "Sales"', 'CREATE TABLE "Sales"."we""ird" (v INTEGER)', 'INSERT INTO "Sales"."we""ird" VALUES (3)',
    'CREATE TABLE "Sales".orders (v INTEGER)', 'INSERT INTO "Sales".orders VALUES (5)',
    'CREATE TABLE public."select" (v INTEGER)', 'INSERT INTO public."select" VALUES (4)',
    "CREATE SCHEMA ingest", 'CREATE TABLE ingest."ArtifactsCore" (v INTEGER)', 'INSERT INTO ingest."ArtifactsCore" VALUES (16)',
)

#: ``(sql_table, the stored source, the table's v)``.
MATRIX = [
    pytest.param('"sales.v2".orders', '"sales.v2".orders', 1, id="quoted-dot"),
    pytest.param('sales."order items"', 'sales."order items"', 2, id="space"),
    pytest.param('"Sales"."we""ird"', '"Sales"."we""ird"', 3, id="embedded-quote"),
    pytest.param('public."select"', 'public."select"', 4, id="reserved-word"),
    pytest.param("Sales.orders", '"Sales".orders', 5, id="mixed-part"),
    pytest.param("ArtifactsCore", '"ArtifactsCore"', 6, id="mixed-case-table"),
]


def _datasource(postgresql: Any) -> DatasourceConfig:
    info = postgresql.info
    return DatasourceConfig.model_validate({"name": DS, "type": "postgres", "host": info.host, "port": info.port,
                                            "database": info.dbname, "username": info.user, "password": ""})


@pytest.fixture
def pg(postgresql: Any) -> DatasourceConfig:
    cur = postgresql.cursor()
    for statement in SEED:
        cur.execute(statement)
    postgresql.commit()
    return _datasource(postgresql)


@pytest.fixture
async def yaml_engine(pg: DatasourceConfig, tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with owned_query_engine(storage=make_storage("yaml", tmp_path), datasources=[pg]) as engine:
        yield engine


async def _v(engine: SlayerQueryEngine, name: str) -> int:
    resp = await engine.execute({"population": name, "metrics": [{"metric": f"sum({name}.v)", "alias": "v"}]})
    return int(resp.data[0]["v"])


def _v_dataset(name: str, *, sql_table: str) -> SlayerDataset:
    return SlayerDataset(name=name, data_source=DS, sql_table=sql_table,
                         fields=[DatasetField(name="v", datatype=DataType.INTEGER)])


def _orders_document(source: str) -> str:
    return document_text(datasets=[dataset("orders", source=source, fields=[field("v", datatype="Integer")])])


# --------------------------------------------------------------------------- #
# Reading a document source
# --------------------------------------------------------------------------- #
async def test_an_unquoted_mixed_case_part_folds(yaml_engine: SlayerQueryEngine) -> None:
    """Spec scenario: An unquoted mixed-case part folds."""
    await store_document(yaml_engine.storage, _orders_document("public.Orders"))
    assert await _v(yaml_engine, "orders") == 7


async def test_a_quoted_part_is_exact(yaml_engine: SlayerQueryEngine) -> None:
    """Spec scenario: A quoted part is exact."""
    await store_document(yaml_engine.storage, _orders_document('public."Orders"'))
    assert await _v(yaml_engine, "orders") == 8


async def test_an_unquoted_mixed_case_table_alone_folds(yaml_engine: SlayerQueryEngine) -> None:
    await store_document(yaml_engine.storage, _orders_document("ArtifactsCore"))
    assert await _v(yaml_engine, "orders") == 60


# --------------------------------------------------------------------------- #
# Writing a source
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_mixed_case_table_name_is_quoted(pg: DatasourceConfig, tmp_path: Path, kind: str) -> None:
    """Spec scenario: A mixed-case table name is quoted."""
    async with owned_query_engine(storage=make_storage(kind, tmp_path), datasources=[pg]) as engine:
        await engine.storage.save_dataset(_v_dataset("artifacts", sql_table="ArtifactsCore"))
        assert stored_dataset(kind, tmp_path, "artifacts")["source"] == '"ArtifactsCore"'
        assert await _v(engine, "artifacts") == 6


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("sql_table", "source", "value"), MATRIX)
async def test_qualified_names_keep_every_part(pg: DatasourceConfig, tmp_path: Path, kind: str, sql_table: str,
                                               source: str, value: int) -> None:
    """Spec scenario: Qualified names keep every part."""
    async with owned_query_engine(storage=make_storage(kind, tmp_path), datasources=[pg]) as engine:
        await engine.storage.save_dataset(_v_dataset("t", sql_table=sql_table))
        assert stored_dataset(kind, tmp_path, "t")["source"] == source
        assert await _v(engine, "t") == value
        stored = texts(kind, tmp_path)
    async with owned_query_engine(storage=reopen(kind, tmp_path)) as engine:
        reloaded = await engine.storage.get_dataset("t", data_source=DS)
        assert reloaded is not None
        assert await _v(engine, "t") == value
        await engine.storage.save_dataset(reloaded)
        assert texts(kind, tmp_path) == stored
        exported = await export_osi(engine=engine, datasource=DS)
        assert [d["source"] for d in exported.document["datasets"] if d["name"] == "t"] == [source]


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_migrated_mixed_case_table_still_reads_it(pg: DatasourceConfig, tmp_path: Path, kind: str) -> None:
    legacy = _v_dataset("artifacts", sql_table="ArtifactsCore").model_dump(mode="json", exclude_none=True)
    storage = await raw_store(backend=kind, base=str(tmp_path / "store") if kind == "yaml" else str(tmp_path),
                              datasource=pg, models=[legacy])
    async with owned_query_engine(storage=storage) as engine:
        assert await _v(engine, "artifacts") == 6
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    assert stored_dataset(kind, tmp_path, "artifacts")["source"] == '"ArtifactsCore"'
    async with owned_query_engine(storage=reopen(kind, tmp_path)) as engine:
        assert await _v(engine, "artifacts") == 6


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_2_0_postgres_store_upgrades(pg: DatasourceConfig, tmp_path: Path, kind: str) -> None:
    """Spec scenario: A 2.0 store upgrades."""
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="public.Orders",
        fields=[DatasetField(name="order_id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="amount", datatype=DataType.INTEGER)],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        meta={"osi_ai_context": {"instructions": "one row per order line"}})
    customers = SlayerDataset(name="customers", data_source=DS, sql_table="public.customers",
                              fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                                      DatasetField(name="region")])
    storage = await raw_store(backend=kind, base=str(tmp_path / "store") if kind == "yaml" else str(tmp_path),
                              datasource=pg, models=[d.model_dump(mode="json", exclude_none=True)
                                                     for d in (orders, customers)])
    query = {"population": "orders", "dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}
    async with owned_query_engine(storage=storage) as engine:
        before = (await engine.execute(query)).data
    assert [sorted(map(str, r.values())) for r in before] == [["100", "North"]]
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    stored = stored_dataset(kind, tmp_path, "orders")
    assert stored["source"] == 'public."Orders"'
    assert stored["primary_key"] == ["order_id", "customer_id"]
    assert stored["ai_context"] == {"instructions": "one row per order line"}
    assert sorted(refs(model_documents(kind, tmp_path)[DS])) == ["customers.dataset.yaml", "orders.dataset.yaml"]
    async with owned_query_engine(storage=reopen(kind, tmp_path)) as engine:
        assert (await engine.execute(query)).data == before


# --------------------------------------------------------------------------- #
# Ingestion and importers
# --------------------------------------------------------------------------- #
def _table_part(source: str) -> exp.Identifier:
    part = exp.to_table(source, dialect="postgres").parts[-1]
    assert isinstance(part, exp.Identifier)
    return part


async def test_ingestion_writes_a_quoted_source(pg: DatasourceConfig, tmp_path: Path) -> None:
    async with owned_query_engine(storage=make_storage("yaml", tmp_path), datasources=[pg]) as engine:
        result = await ingest_datasource_idempotent(datasource=pg, storage=engine.storage,
                                                    client=engine.client_for(pg), schema="ingest")
        assert result.errors == []
        names = await engine.storage.list_datasets(data_source=DS)
        stored = {n: stored_dataset("yaml", tmp_path, n)["source"] for n in names}
        exact = [n for n, s in stored.items() if _table_part(s).name == "ArtifactsCore"]
        assert len(exact) == 1, stored
        assert _table_part(stored[exact[0]]).args.get("quoted") is True
        assert await _v(engine, exact[0]) == 16


async def test_an_importer_writes_a_quoted_source(pg: DatasourceConfig, postgresql: Any, tmp_path: Path) -> None:
    project = CubeProject.model_validate({"cubes": [{"name": "artifacts", "sql_table": 'public."ArtifactsCore"',
                                                     "dimensions": [{"name": "v", "sql": "{CUBE}.v",
                                                                     "type": "number"}]}]})
    converter = CubeToSlayerConverter(project=project, data_source=DS)
    info = postgresql.info
    with disposable_engine(f"postgresql+psycopg://{info.user}@{info.host}:{info.port}/{info.dbname}") as sa_engine:
        collect_over_engine(collect=lambda client: converter.collect_metadata(client=client), engine=sa_engine)
    converted = converter.convert()
    async with owned_query_engine(storage=make_storage("yaml", tmp_path), datasources=[pg]) as engine:
        for model in converted.datasets:
            await engine.storage.save_dataset(model)
        assert stored_dataset("yaml", tmp_path, "artifacts")["source"] == 'public."ArtifactsCore"'
        assert await _v(engine, "artifacts") == 6

