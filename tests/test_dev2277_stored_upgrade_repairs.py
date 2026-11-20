"""Stored upgrade to qualified references: the spine, extension edges and nested stage versions (models/stored-upgrade)."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._stored_upgrade_fixtures import (
    AMOUNT_BY_REGION, DS, by_dimension, customers_v10, memory_v2, orders_v10, query_backed_v10, raw_store, run,
    seed_shop,
)

MODEL_V = CURRENT_VERSIONS["SlayerModel"]
PRE_MODEL = MODEL_V - 1
PRE_QUERY = 16  # before the qualified-reference step
SPINE_FILTER = "timestamp >= '2024-01-01 00:00:00'"


@pytest.fixture
def shop_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db, dialect="sqlite")
    ds = DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "database": db})
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def store(tmp_path: Path, ds: DatasourceConfig, *, models: tuple[dict, ...] = (),
                memories: tuple[dict, ...] = (), orders: dict | None = None) -> StorageBackend:
    base = [orders or orders_v10(version=PRE_MODEL), customers_v10(version=PRE_MODEL)]
    return await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=ds,
                           models=[*base, *models], memories=memories)


def variant(name: str) -> dict:
    return {"version": PRE_MODEL, "name": name, "sql_table": "orders", "data_source": DS,
            "columns": [{"name": "timestamp", "type": "TIMESTAMP", "primary_key": True}], "joins": []}


async def first_stage(storage: StorageBackend, name: str):
    model = await storage.get_dataset(name, data_source=DS)
    assert model is not None
    return (model.source_queries or [])[0]


class TestSpinePopulation:
    @pytest.mark.parametrize("source", ["time_spine", "TIME_SPINE"])
    async def test_spine_rooted_stage_reads_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                      source: str) -> None:
        body = {"version": PRE_QUERY, "source_model": source, "filters": [SPINE_FILTER],
                "time_dimensions": [{"dimension": "timestamp", "granularity": "year"}]}
        storage = await store(tmp_path, shop_ds, models=(query_backed_v10(name="q", stages=[body], version=PRE_MODEL),))
        q = await first_stage(storage, "q")
        assert q.filter == [f"time_spine.{SPINE_FILTER}"]
        assert [td.field for td in q.time_dimensions or []] == ["time_spine.timestamp"]

    async def test_spine_rooted_stage_beside_a_case_variant(self, tmp_path: Path, shop_ds: DatasourceConfig) -> None:
        body = {"version": PRE_QUERY, "source_model": "TIME_SPINE", "filters": [SPINE_FILTER]}
        storage = await store(tmp_path, shop_ds, models=(
            variant("Time_Spine"), query_backed_v10(name="q", stages=[body], version=PRE_MODEL)))
        assert (await first_stage(storage, "q")).filter == [f"TIME_SPINE.{SPINE_FILTER}"]

    async def test_spine_rooted_memory_reads_the_spine(self, tmp_path: Path, shop_ds: DatasourceConfig) -> None:
        body = {"version": PRE_QUERY, "source_model": "time_spine", "filters": [SPINE_FILTER, "orders.amount > 0"]}
        storage = await store(tmp_path, shop_ds, memories=(memory_v2(memory_id="m", query=body, version=12),))
        mem = await storage.get_memory("m")
        assert mem.query is not None
        assert mem.query.filter == [f"time_spine.{SPINE_FILTER}", "orders.amount > 0"]


class TestPathsToTheSpine:
    async def test_spine_reached_through_a_joined_dataset_is_kept(self, tmp_path: Path,
                                                                  shop_ds: DatasourceConfig) -> None:
        text = "customers.time_spine.timestamp >= '2023-01-01 00:00:00'"
        body = {"version": PRE_QUERY, "source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "rev"}],
                "filters": [text]}
        storage = await store(tmp_path, shop_ds, models=(query_backed_v10(name="q", stages=[body], version=PRE_MODEL),))
        assert (await first_stage(storage, "q")).filter == [text]


class TestExtensionEdges:
    async def test_reference_through_a_query_local_edge(self, tmp_path: Path, shop_ds: DatasourceConfig) -> None:
        body = {"version": PRE_QUERY, "source_model": "orders", "dimensions": ["cust.region"],
                "measures": [{"formula": "sum(amount)", "name": "rev"}],
                "extensions": [{"source_name": "orders", "joins": [{
                    "name": "cust", "target_model": "customers", "join_pairs": [["customer_id", "id"]],
                    "cardinality": "many_to_one"}]}]}
        storage = await store(tmp_path, shop_ds, orders=orders_v10(version=PRE_MODEL, joins=[]),
                              models=(query_backed_v10(name="q", stages=[body], version=PRE_MODEL),))
        q = await first_stage(storage, "q")
        assert [d.field for d in q.dimensions or []] == ["orders.cust.region"]
        assert by_dimension(await run(storage, q), dim_suffix="region") == AMOUNT_BY_REGION


class TestNestedStageVersions:
    @pytest.mark.parametrize("model_version", [MODEL_V, PRE_MODEL])
    async def test_older_nested_stage_qualifies_whatever_its_document_version(
        self, tmp_path: Path, shop_ds: DatasourceConfig, model_version: int,
    ) -> None:
        body = {"version": 9, "source_model": "orders", "dimensions": ["customers.region"],
                "measures": [{"formula": "sum(amount * 2)", "name": "rev2"}]}
        storage = await store(tmp_path, shop_ds, models=(
            query_backed_v10(name="q", stages=[body], version=model_version),))
        q = await first_stage(storage, "q")
        assert [m.metric for m in q.metrics or []] == ["sum(orders.amount * 2)"]
        assert by_dimension(await run(storage, q), dim_suffix="region") == {
            k: 2 * v for k, v in AMOUNT_BY_REGION.items()}


class TestRootlessStages:
    async def test_the_one_dataset_reaching_every_read_is_the_population(self, tmp_path: Path,
                                                                        shop_ds: DatasourceConfig) -> None:
        body = {"version": PRE_QUERY, "dimensions": ["status"], "measures": [{"formula": "count(*)", "name": "n"}]}
        storage = await store(tmp_path, shop_ds, models=(query_backed_v10(name="q", stages=[body], version=PRE_MODEL),))
        q = await first_stage(storage, "q")
        assert q.population is None
        assert [d.field for d in q.dimensions or []] == ["orders.status"]
        assert [m.metric for m in q.metrics or []] == ["count(orders.*)"]

    @pytest.mark.parametrize("dimensions", [[], ["status"]], ids=["nothing-read", "two-datasets-reach"])
    async def test_no_single_dataset_is_unloadable(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                   dimensions: list[str]) -> None:
        archive = {"version": PRE_MODEL, "name": "archive", "sql_table": "orders", "data_source": DS,
                   "columns": [{"name": "status", "type": "TEXT", "primary_key": True}], "joins": []}
        body = {"version": PRE_QUERY, "dimensions": dimensions, "measures": [{"formula": "count(*)", "name": "n"}]}
        storage = await store(tmp_path, shop_ds, models=(
            archive, query_backed_v10(name="q", stages=[body], version=PRE_MODEL)))
        with pytest.raises(StoredDocumentLoadError, match="names no population"):
            await storage.get_dataset("q", data_source=DS)
