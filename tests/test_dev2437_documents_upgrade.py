"""Trust and enforced keys in documents and stored upgrades (spec: models/semantic-model-documents › Trust, enforced
keys and an unset cardinality round-trip; models/stored-upgrade › Relationships stored before trust upgrade as
trusted)."""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Optional

import pytest
import yaml

from slayer.core.enums import RelationshipCardinality
from slayer.core.errors import EnforcedKeyError
from slayer.core.models import DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.cube.models import CubeCube, CubeDimension, CubeJoin, CubeProject
from slayer.database import engine_factory
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtDimension, DbtEntity, DbtProject, DbtSemanticModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.osi.exporter import export_osi
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._cube_helpers import convert
from tests._dev1737_fixtures import seed_backend
from tests._dev2243_fixtures import DS as DOC_DS, SALES, STORAGES, document_engine, store_document
from tests._dev2277_fixtures import returns_dataset
from tests._dev2437_fixtures import (
    BACKENDS, DUP_C1, REGION, REVENUE, customers_dataset, orders_dataset, region_revenue, retail_engine, tables,
)
from tests._stored_upgrade_fixtures import BACKENDS as STORE_KINDS, DS as STORE_DS, memory_v2, raw_store, run

#: The versions this change introduces; a concurrent bump renumbers only this line.
UPGRADED_AT = {"SlayerModel": 29, "SlayerQuery": 19, "Memory": 14}
PRE = {entity: version - 1 for entity, version in UPGRADED_AT.items()}
BY_REGION: dict[str, Any] = {"dimensions": [REGION], "metrics": [REVENUE]}
TO_CUSTOMERS = {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["customer_id"],
                "cardinality": "many_to_one"}
_NEW_KEYS = ("trusted", "enforced_keys")


def trust(relationship: DatasetRelationship) -> Optional[bool]:
    return getattr(relationship, "trusted", None)


def slayer_entries(entity: dict[str, Any]) -> str:
    """The ``SLAYER`` extension payloads of a document entity as one JSON text."""
    return json.dumps([e.get("data") for e in entity.get("nonportable_extensions") or []
                       if e.get("vendor_name") == "SLAYER"])


# --------------------------------------------------------------------------- #
# Stored upgrade
# --------------------------------------------------------------------------- #
def _strip_new_keys(value: Any) -> Any:
    """``value`` as a pre-trust payload: no ``trusted`` / ``enforced_keys`` anywhere."""
    if isinstance(value, dict):
        return {k: _strip_new_keys(v) for k, v in value.items() if k not in _NEW_KEYS}
    if isinstance(value, list):
        return [_strip_new_keys(v) for v in value]
    return value


def pre_doc(dataset: SlayerDataset, **update: Any) -> dict[str, Any]:
    doc = _strip_new_keys(dataset.model_dump(mode="json", exclude_none=True))
    return {**doc, **update, "version": PRE["SlayerModel"], "data_source": STORE_DS}


def extended_query(*, version: Optional[int] = PRE["SlayerQuery"]) -> dict[str, Any]:
    """``BY_REGION`` over ``orders`` with ``orders → customers`` declared in its query extension."""
    body: dict[str, Any] = {"population": "orders", **BY_REGION,
                            "extensions": [{"dataset": "orders", "relationships": [TO_CUSTOMERS]}]}
    return body if version is None else {"version": version, **body}


def base_docs(*, orders_relationships: bool = True, orders: Optional[dict] = None) -> list[dict[str, Any]]:
    if orders is None:
        orders = pre_doc(orders_dataset()) if orders_relationships else pre_doc(orders_dataset(), relationships=[])
    return [pre_doc(customers_dataset()), orders, pre_doc(returns_dataset())]


@contextlib.asynccontextmanager
async def pre_store(tmp_path: Path, *, kind: str, dialect: str = "sqlite", models: tuple[dict, ...] = (),
                    memories: tuple[dict, ...] = (), orders_relationships: bool = True,
                    orders: Optional[dict] = None) -> AsyncGenerator[StorageBackend]:
    """A raw store at the pre-trust versions over the duplicated-C1 seed."""
    db = str(tmp_path / f"seed.{'duckdb' if dialect == 'duckdb' else 'db'}")
    seed_backend(dialect, db, tables(DUP_C1))
    ds = DatasourceConfig(name=STORE_DS, type=dialect, database=db)
    try:
        yield await raw_store(backend=kind, base=str(tmp_path / f"store-{kind}"), datasource=ds,
                              models=[*base_docs(orders_relationships=orders_relationships, orders=orders),
                                      *models],
                              memories=memories)
    finally:
        engine_factory.invalidate_engine(ds)


async def get(storage: StorageBackend, name: str) -> SlayerDataset:
    found = await storage.get_dataset(name, data_source=STORE_DS)
    assert found is not None, name
    return found


def extension_relationships(query: SlayerQuery) -> list[DatasetRelationship]:
    out = [r for e in query.extensions or [] for r in e.relationships or []]
    assert out
    return out


def test_versions_bumped() -> None:
    # SlayerModel 30 then unwraps AGG in field expressions; 31 types BigQuery TIMESTAMP fields DateTimeTz.
    assert {entity: CURRENT_VERSIONS[entity] for entity in UPGRADED_AT} == {**UPGRADED_AT, "SlayerModel": 31}


@pytest.mark.parametrize("kind", STORE_KINDS)
@pytest.mark.parametrize("dialect", BACKENDS)
async def test_a_stored_model_keeps_its_inline_join(tmp_path: Path, kind: str, dialect: str) -> None:
    """Spec scenario: A stored model keeps its inline join."""
    async with pre_store(tmp_path, kind=kind, dialect=dialect) as storage:
        for name in ("orders", "returns"):
            assert [trust(r) for r in (await get(storage, name)).relationships] == [True]
        resp = await run(storage, BY_REGION)
    assert region_revenue(resp) == {"EAST": 650.0, "WEST": 300.0}


@pytest.mark.parametrize("kind", STORE_KINDS)
@pytest.mark.parametrize("query_version", [PRE["SlayerQuery"], None], ids=["versioned", "version-less"])
async def test_a_stored_querys_extension_relationship_is_trusted(tmp_path: Path, kind: str,
                                                                 query_version: Optional[int]) -> None:
    """Spec scenario: A stored query's extension relationship is trusted."""
    saved = {"version": PRE["SlayerModel"], "name": "sq", "data_source": STORE_DS,
             "source_queries": [extended_query(version=query_version)]}
    async with pre_store(tmp_path, kind=kind, models=(saved,), orders_relationships=False) as storage:
        [stage] = (await get(storage, "sq")).source_queries or []
        assert isinstance(stage, SlayerQuery)
        assert [trust(r) for r in extension_relationships(stage)] == [True]


@pytest.mark.parametrize("kind", STORE_KINDS)
async def test_a_query_backed_models_own_relationship_is_trusted(tmp_path: Path, kind: str) -> None:
    saved = {"version": PRE["SlayerModel"], "name": "by_customer", "data_source": STORE_DS,
             "source_queries": [{"version": PRE["SlayerQuery"], "population": "orders",
                                 "dimensions": ["orders.customer_id"], "metrics": ["sum(orders.amount)"]}],
             "relationships": [{**TO_CUSTOMERS}]}
    async with pre_store(tmp_path, kind=kind, models=(saved,)) as storage:
        assert [trust(r) for r in (await get(storage, "by_customer")).relationships] == [True]


@pytest.mark.parametrize("kind", STORE_KINDS)
@pytest.mark.parametrize("dialect", BACKENDS)
async def test_a_stored_memorys_query_is_upgraded(tmp_path: Path, kind: str, dialect: str) -> None:
    """Spec scenario: A stored memory's query is upgraded."""
    memory = memory_v2(memory_id="m_region", query=extended_query(), version=PRE["Memory"])
    async with pre_store(tmp_path, kind=kind, dialect=dialect, memories=(memory,),
                         orders_relationships=False) as storage:
        loaded = await storage.get_memory("m_region")
        assert loaded.query is not None
        assert [trust(r) for r in extension_relationships(loaded.query)] == [True]
        resp = await run(storage, loaded.query)
    assert region_revenue(resp)["EAST"] == 650.0


def test_fresh_input_is_never_upgraded() -> None:
    fresh = _strip_new_keys(orders_dataset().model_dump(mode="json", exclude_none=True))
    fresh.pop("version", None)
    assert [trust(r) for r in SlayerDataset.model_validate(fresh).relationships] == [False]


@pytest.mark.parametrize("kind", STORE_KINDS)
async def test_a_current_stored_relationship_stays_untrusted(tmp_path: Path, kind: str) -> None:
    current = {**pre_doc(orders_dataset()), "version": UPGRADED_AT["SlayerModel"]}
    async with pre_store(tmp_path, kind=kind, orders=current) as storage:
        assert [trust(r) for r in (await get(storage, "orders")).relationships] == [False]


# --------------------------------------------------------------------------- #
# Documents
# --------------------------------------------------------------------------- #
@contextlib.asynccontextmanager
async def dirty_sales(kind: str, *, documents: tuple[str, ...] = (SALES,)) -> AsyncGenerator[SlayerQueryEngine]:
    async with document_engine(kind=kind, backend="duckdb", documents=documents, tables=tables(DUP_C1)) as eng:
        yield eng


async def doc_get(storage: StorageBackend, name: str) -> SlayerDataset:
    found = await storage.get_dataset(name, data_source=DOC_DS)
    assert found is not None, name
    return found


async def stored_doc(storage: StorageBackend) -> dict[str, Any]:
    text = await storage.get_semantic_model_text("sales", data_source=DOC_DS)
    assert text is not None
    return yaml.safe_load(text)


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_imported_retail_model_is_never_trusted(kind: str) -> None:
    """Spec scenario: An imported document is never trusted."""
    async with retail_engine(kind=kind) as eng:
        orders = await eng.storage.get_dataset("orders", data_source="wide_table")
        assert orders is not None
        assert [trust(r) for r in orders.relationships] == [False]
        resp = await eng.execute(BY_REGION)
    assert region_revenue(resp)["EAST"] == 350.0


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_ossie_relationship_without_an_entry_loads_untrusted_many_to_one(kind: str) -> None:
    async with dirty_sales(kind) as eng:
        [rel] = (await doc_get(eng.storage, "orders")).relationships
        customers = await doc_get(eng.storage, "customers")
        assert (rel.cardinality, trust(rel)) == (RelationshipCardinality.MANY_TO_ONE, False)
        assert getattr(customers, "enforced_keys", None) == []
        resp = await eng.execute(BY_REGION)
    assert region_revenue(resp)["EAST"] == 350.0


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_trusted_relationship_round_trips(kind: str) -> None:
    """Spec scenario: A trusted relationship round-trips."""
    async with dirty_sales(kind) as eng:
        orders = await doc_get(eng.storage, "orders")
        [rel] = orders.relationships
        await eng.storage.save_dataset(orders.model_copy(update={"relationships": [rel.model_copy(
            update={"trusted": True})]}))
        [written] = (await stored_doc(eng.storage))["relationships"]
        assert "trusted" in slayer_entries(written)
        assert [trust(r) for r in (await doc_get(eng.storage, "orders")).relationships] == [True]
        exported = (await export_osi(engine=eng, datasource=DOC_DS, extensions=True)).text
    async with dirty_sales(kind, documents=(exported,)) as eng:
        assert [trust(r) for r in (await doc_get(eng.storage, "orders")).relationships] == [True]
        resp = await eng.execute(BY_REGION)
    assert region_revenue(resp)["EAST"] == 650.0


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_unset_cardinality_round_trips(kind: str) -> None:
    """Spec scenario: An unset cardinality round-trips."""
    async with dirty_sales(kind) as eng:
        orders = await doc_get(eng.storage, "orders")
        [rel] = orders.relationships
        await eng.storage.save_dataset(orders.model_copy(update={"relationships": [rel.model_copy(
            update={"cardinality": None})]}))
        [written] = (await stored_doc(eng.storage))["relationships"]
        assert slayer_entries(written) != "[]"
        assert [r.cardinality for r in (await doc_get(eng.storage, "orders")).relationships] == [None]
        exported = (await export_osi(engine=eng, datasource=DOC_DS, extensions=True)).text
    async with dirty_sales(kind, documents=(exported,)) as eng:
        assert [r.cardinality for r in (await doc_get(eng.storage, "orders")).relationships] == [None]


@pytest.mark.parametrize("kind", STORAGES)
async def test_enforced_keys_round_trip(kind: str) -> None:
    """Spec scenario: Enforced keys round-trip."""
    async with dirty_sales(kind) as eng:
        customers = await doc_get(eng.storage, "customers")
        await eng.storage.save_dataset(customers.model_copy(update={"enforced_keys": [["customer_id"]]}))
        [written] = [d for d in (await stored_doc(eng.storage))["datasets"] if d["name"] == "customers"]
        assert "enforced_keys" in slayer_entries(written)
        assert getattr(await doc_get(eng.storage, "customers"), "enforced_keys", None) == [["customer_id"]]
        exported = (await export_osi(engine=eng, datasource=DOC_DS, extensions=True)).text
    async with dirty_sales(kind, documents=(exported,)) as eng:
        assert getattr(await doc_get(eng.storage, "customers"), "enforced_keys", None) == [["customer_id"]]


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_undeclared_enforced_key_is_refused_by_an_extensions_save(kind: str) -> None:
    async with dirty_sales(kind) as eng:
        customers = await doc_get(eng.storage, "customers")
        undeclared = customers.model_copy(update={"enforced_keys": [["region"]]})
        with pytest.raises(EnforcedKeyError, match="region"):
            await eng.storage.save_dataset_extensions(undeclared)
        [written] = [d for d in (await stored_doc(eng.storage))["datasets"] if d["name"] == "customers"]
    assert "enforced_keys" not in slayer_entries(written)


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_extensions_save_re_pointing_a_key_field_drops_its_enforced_key(kind: str) -> None:
    async with dirty_sales(kind) as eng:
        customers = await doc_get(eng.storage, "customers")
        await eng.storage.save_dataset(customers.model_copy(update={"enforced_keys": [["customer_id"]]}))
        stored = await doc_get(eng.storage, "customers")
        fields = [f.model_copy(update={"expression": "region"}) if f.name == "customer_id" else f for f in stored.fields]
        await eng.storage.save_dataset_extensions(stored.model_copy(update={"fields": fields}))
        assert (await doc_get(eng.storage, "customers")).enforced_keys == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_stale_dataset_entry_restores_no_enforced_keys(kind: str) -> None:
    """A hand-edited document source leaves its entry's enforced keys describing the old table."""
    async with dirty_sales(kind) as eng:
        customers = await doc_get(eng.storage, "customers")
        await eng.storage.save_dataset(customers.model_copy(update={"enforced_keys": [["customer_id"]]}))
        doc = await stored_doc(eng.storage)
        [entry] = [d for d in doc["datasets"] if d["name"] == "customers"]
        assert "enforced_keys" in slayer_entries(entry)
        entry["source"] = "customers_v2"
        await store_document(eng.storage, yaml.safe_dump(doc, sort_keys=False), data_source=DOC_DS)
        repointed = await doc_get(eng.storage, "customers")
    assert repointed.sql_table == "customers_v2"
    assert repointed.enforced_keys == []


def test_a_cube_import_is_never_trusted() -> None:
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.customer_id = {customers.id}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True)]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True)]),
    ])
    models, _ = convert(project)
    assert [trust(r) for r in models["orders"].relationships] == [False]
    assert [getattr(m, "enforced_keys", None) for m in models.values()] == [[], []]


def test_a_dbt_import_is_never_trusted() -> None:
    project = DbtProject(semantic_models=[
        DbtSemanticModel(name="orders", model="orders",
                         entities=[DbtEntity(name="order_id", type="primary", expr="id"),
                                   DbtEntity(name="customer_id", type="foreign")]),
        DbtSemanticModel(name="customers", model="customers",
                         entities=[DbtEntity(name="customer_id", type="primary", expr="id")],
                         dimensions=[DbtDimension(name="region", type="categorical")]),
    ], metrics=[])
    datasets = {d.name: d for d in DbtToSlayerConverter(project=project, data_source="test_db").convert().datasets}
    assert [trust(r) for r in datasets["orders"].relationships] == [False]
    assert [getattr(d, "enforced_keys", None) for d in datasets.values()] == [[], []]
