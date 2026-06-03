"""Tests for graph-backed Cypher pre-filter (slayer/search/graph.py).

Positive-path tests that build and query a real LadybugDB graph require the
``ladybug`` package to be installed; they are skipped gracefully otherwise.
Tests for the "not installed" degradation path and the storage fingerprint ABC
do not need ladybug and always run.

These tests fail until slayer/search/graph.py is created and until all
functions specified here are implemented.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import AsyncIterator
from unittest.mock import patch

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.search.graph import (
    clear_cache,
    get_filtered_ids,
    is_available,
)
from slayer.search.service import SearchResponse, SearchService
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def rich_storage() -> AsyncIterator[YAMLStorage]:
    """2 datasources, 3 models (joins, measures, custom aggregations), 4
    memories — including one memory→memory cross-reference."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)

        await storage.save_datasource(
            DatasourceConfig(name="shop", type="sqlite", database=":memory:")
        )
        await storage.save_datasource(
            DatasourceConfig(name="analytics", type="sqlite", database=":memory:")
        )

        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="shop",
                description="Order transactions",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(
                        name="amount",
                        sql="amount",
                        type=DataType.DOUBLE,
                        label="Order amount",
                        description="Net order value in USD",
                    ),
                    Column(name="status", sql="status", type=DataType.TEXT),
                ],
                measures=[
                    ModelMeasure(
                        name="total_revenue",
                        formula="amount:sum",
                        label="Total revenue",
                        description="Sum of order amounts",
                    )
                ],
                aggregations=[
                    Aggregation(
                        name="weighted_avg",
                        formula="SUM({col} * weight) / SUM(weight)",
                    )
                ],
                joins=[
                    ModelJoin(
                        target_model="customers",
                        join_pairs=[["customer_id", "id"]],
                    )
                ],
            )
        )

        await storage.save_model(
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="shop",
                description="Customer master data",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="email", sql="email", type=DataType.TEXT),
                    Column(name="region", sql="region", type=DataType.TEXT),
                ],
            )
        )

        await storage.save_model(
            SlayerModel(
                name="events",
                sql_table="events",
                data_source="analytics",
                description="Clickstream events",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(
                        name="event_type", sql="event_type", type=DataType.TEXT
                    ),
                ],
            )
        )

        # 4 memories: 2 tagged on shop columns, 1 on analytics model,
        # 1 with a memory→memory cross-reference.
        mem1 = await storage.save_memory(
            learning="Orders with status=cancelled must be excluded from revenue.",
            entities=["shop.orders.amount", "shop.orders.status"],
        )
        await storage.save_memory(
            learning="Customer email is PII — do not expose in dashboards.",
            entities=["shop.customers.email"],
        )
        await storage.save_memory(
            learning="Events table is append-only.",
            entities=["analytics.events"],
        )
        await storage.save_memory(
            learning="See also the revenue cancellation note.",
            entities=[f"memory:{mem1.id}"],
        )

        clear_cache()
        yield storage


@pytest_asyncio.fixture
async def shop_only_storage() -> AsyncIterator[YAMLStorage]:
    """Minimal shop datasource with one model — used for fingerprint/cache tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(
            DatasourceConfig(name="shop", type="sqlite", database=":memory:")
        )
        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="shop",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                ],
            )
        )
        clear_cache()
        yield storage


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------


def test_is_available_returns_bool() -> None:
    result = is_available()
    assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# StorageBackend.graph_fingerprint() — no ladybug needed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_storage_graph_fingerprint_returns_str(
    shop_only_storage: YAMLStorage,
) -> None:
    fp = await shop_only_storage.graph_fingerprint()
    assert isinstance(fp, str)
    assert fp != ""


@pytest.mark.asyncio
async def test_yaml_storage_fingerprint_changes_after_write(
    shop_only_storage: YAMLStorage,
) -> None:
    fp_before = await shop_only_storage.graph_fingerprint()
    await shop_only_storage.save_memory(
        learning="A new memory changes the fingerprint.",
        entities=[],
    )
    fp_after = await shop_only_storage.graph_fingerprint()
    assert fp_after != fp_before


@pytest.mark.asyncio
async def test_yaml_storage_fingerprint_changes_after_delete(
    shop_only_storage: YAMLStorage,
) -> None:
    fp_before = await shop_only_storage.graph_fingerprint()
    await shop_only_storage.delete_model("orders", data_source="shop")
    fp_after = await shop_only_storage.graph_fingerprint()
    assert fp_after != fp_before


@pytest.mark.asyncio
async def test_sqlite_storage_graph_fingerprint_returns_str() -> None:
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:")
        )
        fp = await storage.graph_fingerprint()
        assert isinstance(fp, str)
        assert fp != ""
    finally:
        os.unlink(db_path)


@pytest.mark.asyncio
async def test_sqlite_storage_fingerprint_changes_after_write() -> None:
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:")
        )
        await storage.save_model(
            SlayerModel(
                name="m",
                sql_table="m",
                data_source="ds",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            )
        )
        fp_before = await storage.graph_fingerprint()
        await storage.save_memory(learning="new memory", entities=[])
        fp_after = await storage.graph_fingerprint()
        assert fp_after != fp_before
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Graph construction — node counts (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_graph_memory_node_count(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Memory) RETURN m.id AS id", rich_storage
    )
    assert len(ids) == 4


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_graph_datasource_node_count(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (d:Datasource) RETURN d.id AS id", rich_storage
    )
    assert len(ids) == 2
    assert "shop" in ids
    assert "analytics" in ids


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_graph_model_node_count(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Model) RETURN m.id AS id", rich_storage
    )
    assert len(ids) == 3
    assert "shop.orders" in ids
    assert "shop.customers" in ids
    assert "analytics.events" in ids


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_graph_column_node_count(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (c:Column) RETURN c.id AS id", rich_storage
    )
    # orders: 3, customers: 3, events: 2 → 8 total
    assert len(ids) == 8


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_graph_measure_node_count(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (ms:Measure) RETURN ms.id AS id", rich_storage
    )
    assert len(ids) == 1
    assert "shop.orders.total_revenue" in ids


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_graph_aggregation_node_count(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (a:Aggregation) RETURN a.id AS id", rich_storage
    )
    assert len(ids) == 1
    assert "shop.orders.weighted_avg" in ids


# ---------------------------------------------------------------------------
# Node properties (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_memory_node_learning_property(rich_storage: YAMLStorage) -> None:
    # Use WHERE on the learning property to verify it's indexed correctly.
    ids = await get_filtered_ids(
        "MATCH (m:Memory) WHERE m.learning CONTAINS 'cancelled' "
        "RETURN m.id AS id",
        rich_storage,
    )
    assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_column_node_data_type_property(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (c:Column {data_type: 'DOUBLE'}) RETURN c.id AS id",
        rich_storage,
    )
    assert "shop.orders.amount" in ids
    assert "shop.orders.status" not in ids  # TEXT, not DOUBLE


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_model_description_property(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Model) WHERE m.description CONTAINS 'Order' "
        "RETURN m.id AS id",
        rich_storage,
    )
    assert "shop.orders" in ids


# ---------------------------------------------------------------------------
# MENTIONS edges (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_mentions_memory_to_column(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Memory)-[:MENTIONS]->(c:Column {id: 'shop.orders.amount'}) "
        "RETURN m.id AS id",
        rich_storage,
    )
    assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_mentions_memory_to_model(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Model {id: 'analytics.events'}) "
        "RETURN m.id AS id",
        rich_storage,
    )
    assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_mentions_memory_to_datasource(rich_storage: YAMLStorage) -> None:
    """MENTIONS to a Datasource node works (memory entity tag at ds level)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(
            DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
        )
        # A memory whose entity resolves to just the datasource
        await storage.save_memory(learning="mydb datasource note.", entities=["mydb"])
        clear_cache()

        ids = await get_filtered_ids(
            "MATCH (m:Memory)-[:MENTIONS]->(d:Datasource) RETURN m.id AS id",
            storage,
        )
        assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_mentions_memory_to_measure(rich_storage: YAMLStorage) -> None:
    """MENTIONS to a Measure node (memory entity tag at measure level)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:")
        )
        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                measures=[ModelMeasure(name="rev", formula="id:sum")],
            )
        )
        await storage.save_memory(
            learning="Rev measure note.", entities=["ds.orders.rev"]
        )
        clear_cache()

        ids = await get_filtered_ids(
            "MATCH (m:Memory)-[:MENTIONS]->(ms:Measure) RETURN m.id AS id",
            storage,
        )
        assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_mentions_memory_to_memory(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m1:Memory)-[:MENTIONS]->(m2:Memory) RETURN m1.id AS id",
        rich_storage,
    )
    assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_memory_id_stored_as_canonical_form(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Memory) RETURN m.id AS id", rich_storage
    )
    assert all(id_.startswith("memory:") for id_ in ids)


# ---------------------------------------------------------------------------
# CONTAINS edges (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_contains_datasource_to_model(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (d:Datasource {id: 'shop'})-[:CONTAINS]->(m:Model) "
        "RETURN m.id AS id",
        rich_storage,
    )
    assert ids == {"shop.orders", "shop.customers"}


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_contains_model_to_column(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Model {id: 'shop.orders'})-[:CONTAINS]->(c:Column) "
        "RETURN c.id AS id",
        rich_storage,
    )
    assert ids == {"shop.orders.id", "shop.orders.amount", "shop.orders.status"}


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_contains_model_to_measure(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Model {id: 'shop.orders'})-[:CONTAINS]->(ms:Measure) "
        "RETURN ms.id AS id",
        rich_storage,
    )
    assert ids == {"shop.orders.total_revenue"}


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_contains_model_to_aggregation(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Model {id: 'shop.orders'})-[:CONTAINS]->(a:Aggregation) "
        "RETURN a.id AS id",
        rich_storage,
    )
    assert ids == {"shop.orders.weighted_avg"}


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_contains_multi_hop_datasource_to_column(
    rich_storage: YAMLStorage,
) -> None:
    ids = await get_filtered_ids(
        "MATCH (d:Datasource {id: 'shop'})-[:CONTAINS*2]->(c:Column) "
        "RETURN c.id AS id",
        rich_storage,
    )
    assert "shop.orders.amount" in ids
    assert "shop.customers.email" in ids
    assert "analytics.events.event_type" not in ids


# ---------------------------------------------------------------------------
# JOINS edges (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_joins_model_to_model(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Model {id: 'shop.orders'})-[:JOINS]->(t:Model) "
        "RETURN t.id AS id",
        rich_storage,
    )
    assert ids == {"shop.customers"}


# ---------------------------------------------------------------------------
# Multi-label union matching (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_multi_label_union_column_and_measure(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (n:Column:Measure) RETURN n.id AS id", rich_storage
    )
    assert "shop.orders.amount" in ids          # Column
    assert "shop.orders.total_revenue" in ids   # Measure


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_multi_label_all_data_model_entities(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (n:Datasource:Model:Column:Measure:Aggregation) RETURN n.id AS id",
        rich_storage,
    )
    assert "shop" in ids                        # Datasource
    assert "shop.orders" in ids                 # Model
    assert "shop.orders.amount" in ids          # Column
    assert "shop.orders.total_revenue" in ids   # Measure
    assert "shop.orders.weighted_avg" in ids    # Aggregation
    assert not any(id_.startswith("memory:") for id_ in ids)


# ---------------------------------------------------------------------------
# Hidden entity exclusion (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_hidden_column_excluded_from_graph() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:")
        )
        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="internal_flag", type=DataType.TEXT, hidden=True),
                ],
            )
        )
        clear_cache()
        all_cols = await get_filtered_ids(
            "MATCH (c:Column) RETURN c.id AS id", storage
        )
        assert "ds.orders.internal_flag" not in all_cols
        assert "ds.orders.id" in all_cols


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_hidden_model_excluded_from_graph() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:")
        )
        await storage.save_model(
            SlayerModel(
                name="visible",
                sql_table="visible",
                data_source="ds",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            )
        )
        await storage.save_model(
            SlayerModel(
                name="hidden_model",
                sql_table="hidden_table",
                data_source="ds",
                hidden=True,
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            )
        )
        clear_cache()
        ids = await get_filtered_ids(
            "MATCH (m:Model) RETURN m.id AS id", storage
        )
        assert "ds.hidden_model" not in ids
        assert "ds.visible" in ids


# ---------------------------------------------------------------------------
# Cache: hit and miss (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_cache_hit_does_not_rebuild(shop_only_storage: YAMLStorage) -> None:
    from slayer.search.graph import build_graph

    with patch("slayer.search.graph.build_graph", wraps=build_graph) as mock_build:
        await get_filtered_ids(
            "MATCH (m:Model) RETURN m.id AS id", shop_only_storage
        )
        await get_filtered_ids(
            "MATCH (d:Datasource) RETURN d.id AS id", shop_only_storage
        )
        assert mock_build.call_count == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_cache_miss_after_storage_write(shop_only_storage: YAMLStorage) -> None:
    from slayer.search.graph import build_graph

    with patch("slayer.search.graph.build_graph", wraps=build_graph) as mock_build:
        await get_filtered_ids(
            "MATCH (m:Model) RETURN m.id AS id", shop_only_storage
        )
        first_count = mock_build.call_count

        await shop_only_storage.save_memory(
            learning="A new memory triggers a fingerprint change.",
            entities=[],
        )

        await get_filtered_ids(
            "MATCH (m:Memory) RETURN m.id AS id", shop_only_storage
        )
        assert mock_build.call_count == first_count + 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_two_storage_paths_use_independent_caches() -> None:
    from slayer.search.graph import build_graph

    with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
        s1 = YAMLStorage(base_dir=tmp1)
        s2 = YAMLStorage(base_dir=tmp2)
        for s, ds_name in [(s1, "ds1"), (s2, "ds2")]:
            await s.save_datasource(
                DatasourceConfig(name=ds_name, type="sqlite", database=":memory:")
            )
            await s.save_model(
                SlayerModel(
                    name="m",
                    sql_table="m",
                    data_source=ds_name,
                    columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                )
            )
        clear_cache()

        with patch("slayer.search.graph.build_graph", wraps=build_graph) as mock_build:
            await get_filtered_ids("MATCH (d:Datasource) RETURN d.id AS id", s1)
            await get_filtered_ids("MATCH (d:Datasource) RETURN d.id AS id", s2)
            assert mock_build.call_count == 2

        ids1 = await get_filtered_ids(
            "MATCH (d:Datasource) RETURN d.id AS id", s1
        )
        ids2 = await get_filtered_ids(
            "MATCH (d:Datasource) RETURN d.id AS id", s2
        )
        assert "ds1" in ids1 and "ds2" not in ids1
        assert "ds2" in ids2 and "ds1" not in ids2


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_concurrent_rebuild_rebuilds_once(
    shop_only_storage: YAMLStorage,
) -> None:
    """Two coroutines racing on a cold cache should trigger exactly one rebuild."""
    from slayer.search.graph import build_graph

    clear_cache()
    with patch("slayer.search.graph.build_graph", wraps=build_graph) as mock_build:
        await asyncio.gather(
            get_filtered_ids("MATCH (m:Model) RETURN m.id AS id", shop_only_storage),
            get_filtered_ids("MATCH (d:Datasource) RETURN d.id AS id", shop_only_storage),
        )
        assert mock_build.call_count == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_inaccessible_fingerprint_triggers_rebuild(
    shop_only_storage: YAMLStorage,
) -> None:
    from slayer.search.graph import build_graph

    await get_filtered_ids("MATCH (m:Model) RETURN m.id AS id", shop_only_storage)

    with (
        patch("slayer.search.graph.build_graph", wraps=build_graph) as mock_build,
        patch.object(
            type(shop_only_storage),
            "graph_fingerprint",
            side_effect=OSError("disk read failed"),
        ),
    ):
        await get_filtered_ids("MATCH (m:Model) RETURN m.id AS id", shop_only_storage)
        assert mock_build.call_count == 1


# ---------------------------------------------------------------------------
# Cypher validation — no ladybug needed
# ---------------------------------------------------------------------------


def test_validate_cypher_accepts_match_return() -> None:
    from slayer.search.graph import _validate_cypher

    _validate_cypher("MATCH (m:Memory) RETURN m.id AS id")
    _validate_cypher(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Column) WHERE e.id = 'x' RETURN m.id AS id"
    )
    _validate_cypher(
        "MATCH (e:Column)<-[:CONTAINS*1..3]-(d:Datasource) RETURN e.id AS id"
    )


@pytest.mark.parametrize(
    "bad_cypher",
    [
        "CREATE (n:Memory {id: 'x'})",
        "MERGE (n:Memory {id: 'x'})",
        "MATCH (n) DELETE n",
        "MATCH (n) SET n.x = 1",
        "DROP TABLE Memory",
        "CALL apoc.something()",
        # Missing AS id alias
        "MATCH (m:Memory) RETURN m.id",
        # Multiple statements
        "MATCH (m:Memory) RETURN m.id AS id; MATCH (n) RETURN n.id AS id",
    ],
)
def test_validate_cypher_rejects_invalid(bad_cypher: str) -> None:
    from slayer.search.graph import _validate_cypher

    with pytest.raises(ValueError):
        _validate_cypher(bad_cypher)


# ---------------------------------------------------------------------------
# Cypher execution (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_cypher_property_filter(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (c:Column {id: 'shop.orders.amount'}) RETURN c.id AS id",
        rich_storage,
    )
    assert ids == {"shop.orders.amount"}


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_cypher_path_traversal_memories_of_model(
    rich_storage: YAMLStorage,
) -> None:
    ids = await get_filtered_ids(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Column) "
        "WHERE e.id STARTS WITH 'shop.orders.' "
        "RETURN m.id AS id",
        rich_storage,
    )
    # Only mem1 mentions shop.orders.* columns
    assert len(ids) == 1


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_cypher_multi_hop_contains(rich_storage: YAMLStorage) -> None:
    ids = await get_filtered_ids(
        "MATCH (d:Datasource {id: 'shop'})-[:CONTAINS*1..2]->(n) "
        "RETURN n.id AS id",
        rich_storage,
    )
    assert "shop.orders" in ids
    assert "shop.customers" in ids
    assert "shop.orders.amount" in ids
    assert "shop.customers.email" in ids
    assert "analytics.events" not in ids


# ---------------------------------------------------------------------------
# Zero-match Cypher (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_zero_match_cypher_returns_empty_response_with_warning(
    rich_storage: YAMLStorage,
) -> None:
    service = SearchService(storage=rich_storage)
    response = await service.search(
        entities=["shop.orders.amount"],
        cypher_filter=(
            "MATCH (m:Memory {id: 'memory:nonexistent-9999'}) RETURN m.id AS id"
        ),
    )
    assert response.memories == []
    assert response.example_queries == []
    assert response.entities == []
    assert any("zero" in w.lower() or "no" in w.lower() for w in response.warnings)


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_zero_match_cypher_preserves_prior_warnings(
    rich_storage: YAMLStorage,
) -> None:
    service = SearchService(storage=rich_storage)
    response = await service.search(
        entities=["shop.orders.amount", "shop.nonexistent.col"],
        cypher_filter=(
            "MATCH (m:Memory {id: 'memory:nonexistent-9999'}) RETURN m.id AS id"
        ),
    )
    assert any("shop.nonexistent.col" in w for w in response.warnings)


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_zero_match_cypher_does_not_call_channels(
    rich_storage: YAMLStorage,
) -> None:
    service = SearchService(storage=rich_storage)
    with (
        patch.object(service, "_run_channel_1", wraps=service._run_channel_1) as ch1,
        patch.object(service, "_run_channel_2", wraps=service._run_channel_2) as ch2,
        patch.object(service, "_run_channel_3", wraps=service._run_channel_3) as ch3,
    ):
        await service.search(
            entities=["shop.orders.amount"],
            cypher_filter=(
                "MATCH (m:Memory {id: 'memory:nonexistent-9999'}) RETURN m.id AS id"
            ),
        )
        ch1.assert_not_called()
        ch2.assert_not_called()
        ch3.assert_not_called()


# ---------------------------------------------------------------------------
# cypher_filter=None — zero overhead, no graph code invoked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_cypher_filter_does_not_invoke_graph(
    rich_storage: YAMLStorage,
) -> None:
    service = SearchService(storage=rich_storage)
    with patch("slayer.search.graph.get_filtered_ids") as mock_gfi:
        await service.search(
            entities=["shop.orders.amount"],
            question="revenue",
        )
        mock_gfi.assert_not_called()


# ---------------------------------------------------------------------------
# LadybugDB not installed — no ladybug needed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ladybug_not_installed_with_cypher_filter_raises(
    rich_storage: YAMLStorage,
) -> None:
    with patch("slayer.search.graph.is_available", return_value=False):
        service = SearchService(storage=rich_storage)
        with pytest.raises(ValueError, match="(?i)ladybug|graph|not installed"):
            await service.search(
                entities=["shop.orders.amount"],
                cypher_filter="MATCH (m:Memory) RETURN m.id AS id",
            )


@pytest.mark.asyncio
async def test_ladybug_not_installed_without_filter_no_error(
    rich_storage: YAMLStorage,
) -> None:
    with patch("slayer.search.graph.is_available", return_value=False):
        service = SearchService(storage=rich_storage)
        response = await service.search(entities=["shop.orders.amount"])
        assert isinstance(response, SearchResponse)


@pytest.mark.asyncio
async def test_ladybug_not_installed_without_filter_no_warning(
    rich_storage: YAMLStorage,
) -> None:
    """When cypher_filter=None and ladybug is absent, no graph-related warning
    must appear — the missing dep is invisible to callers who don't use it."""
    with patch("slayer.search.graph.is_available", return_value=False):
        service = SearchService(storage=rich_storage)
        response = await service.search(entities=["shop.orders.amount"])
        graph_warnings = [
            w for w in response.warnings
            if "ladybug" in w.lower() or "graph" in w.lower()
        ]
        assert graph_warnings == []


# ---------------------------------------------------------------------------
# candidate_ids propagation to channels (no ladybug needed — mock get_filtered_ids)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_candidate_ids_exact_set_passed_to_all_channels(
    rich_storage: YAMLStorage,
) -> None:
    """Mock get_filtered_ids to return a known set; assert every channel
    receives exactly that set."""
    expected_ids = frozenset({"memory:1", "memory:2"})
    service = SearchService(storage=rich_storage)

    with (
        patch(
            "slayer.search.graph.get_filtered_ids",
            return_value=expected_ids,
        ),
        patch.object(service, "_run_channel_1", wraps=service._run_channel_1) as ch1,
        patch.object(service, "_run_channel_2", wraps=service._run_channel_2) as ch2,
        patch.object(service, "_run_channel_3", wraps=service._run_channel_3) as ch3,
    ):
        await service.search(
            entities=["shop.orders.amount"],
            question="revenue",
            cypher_filter="MATCH (m:Memory) RETURN m.id AS id",
        )

    for mock_ch in (ch1, ch2, ch3):
        assert mock_ch.call_args is not None, f"{mock_ch} was never called"
        call_kwargs = mock_ch.call_args.kwargs
        assert call_kwargs.get("candidate_ids") == expected_ids, (
            f"channel {mock_ch} received wrong candidate_ids: "
            f"{call_kwargs.get('candidate_ids')!r}"
        )


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_candidate_ids_results_are_exact_subset_of_cypher_output(
    rich_storage: YAMLStorage,
) -> None:
    """Results returned by search must be a subset of the nodes returned by
    Cypher — no memory outside the candidate set must surface."""
    # Cypher: only the memory that mentions shop.orders.amount
    cypher = (
        "MATCH (m:Memory)-[:MENTIONS]->(c:Column {id: 'shop.orders.amount'}) "
        "RETURN m.id AS id"
    )
    expected_candidates = await get_filtered_ids(cypher, rich_storage)
    assert len(expected_candidates) == 1  # sanity: fixture has exactly 1 such memory

    service = SearchService(storage=rich_storage)
    response = await service.search(
        entities=["shop.orders.amount"],
        cypher_filter=cypher,
        max_memories=10,
    )

    returned_ids = {h.id for h in response.memories}
    # Every returned memory id, when prefixed with "memory:", must be in candidates
    for bare_id in returned_ids:
        assert f"memory:{bare_id}" in expected_candidates, (
            f"memory {bare_id!r} was returned but not in Cypher candidate set "
            f"{expected_candidates!r}"
        )


# ---------------------------------------------------------------------------
# cypher_filter + datasource intersection (requires ladybug)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not is_available(), reason="ladybug not installed")
@pytest.mark.asyncio
async def test_cypher_filter_and_datasource_are_intersected(
    rich_storage: YAMLStorage,
) -> None:
    service = SearchService(storage=rich_storage)
    # Cypher returns memories mentioning shop columns; datasource='analytics'
    # scopes the corpus → intersection is empty
    response = await service.search(
        entities=["shop.orders.amount"],
        datasource="analytics",
        cypher_filter=(
            "MATCH (m:Memory)-[:MENTIONS]->(c:Column) "
            "WHERE c.id STARTS WITH 'shop.' "
            "RETURN m.id AS id"
        ),
    )
    assert response.memories == []


# ---------------------------------------------------------------------------
# Stale candidate IDs cause no error (no ladybug needed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_canonical_ids_in_candidate_set_cause_no_error(
    shop_only_storage: YAMLStorage,
) -> None:
    """If candidate_ids contains a memory id that no longer exists in storage
    (deleted after Cypher ran), search must not raise — the id simply won't
    surface in results."""
    mem = await shop_only_storage.save_memory(
        learning="A memory to be deleted.", entities=[]
    )
    stale_id = f"memory:{mem.id}"

    # Delete the memory from storage
    await shop_only_storage.delete_memory(mem.id)

    service = SearchService(storage=shop_only_storage)
    # Inject the stale id as if it came from Cypher
    with patch(
        "slayer.search.graph.get_filtered_ids",
        return_value=frozenset({stale_id}),
    ):
        response = await service.search(
            cypher_filter="MATCH (m:Memory) RETURN m.id AS id",
            question="deleted memory",
        )
    assert isinstance(response, SearchResponse)
    result_ids = {h.id for h in response.memories}
    assert mem.id not in result_ids


# ---------------------------------------------------------------------------
# Regex false-positive fix: mutation keywords inside string literals
# ---------------------------------------------------------------------------


def test_validate_cypher_accepts_mutation_keyword_in_string_literal() -> None:
    """A Cypher query whose property value contains a mutation keyword as a
    standalone word must NOT be rejected by the safety validator."""
    from slayer.search.graph import _validate_cypher

    # 'call me' contains the CALL keyword but is a string literal.
    _validate_cypher(
        "MATCH (m:Memory) WHERE m.learning CONTAINS 'call me' RETURN m.id AS id"
    )
    # 'set operations' contains SET as a standalone word.
    _validate_cypher(
        "MATCH (m:Memory) WHERE m.learning = 'set operations' RETURN m.id AS id"
    )
    # Double-quoted literal with DROP.
    _validate_cypher(
        'MATCH (c:Column) WHERE c.description = "drop rate" RETURN c.id AS id'
    )


def test_validate_cypher_still_rejects_bare_mutation_keyword() -> None:
    """A mutation keyword that is NOT inside a string literal must still be
    rejected — the literal-stripping must not be too greedy."""
    from slayer.search.graph import _validate_cypher

    with pytest.raises(ValueError, match="mutation keyword"):
        _validate_cypher("MATCH (m:Memory) SET m.x = 1 RETURN m.id AS id")

    with pytest.raises(ValueError, match="mutation keyword"):
        _validate_cypher("CALL apoc.something() YIELD value RETURN value AS id")


# ---------------------------------------------------------------------------
# cypher_filter respected by the recency fallback path
# ---------------------------------------------------------------------------


async def test_cypher_filter_applied_to_recency_fallback(
    shop_only_storage: YAMLStorage,
) -> None:
    """When cypher_filter is set but no entities/question are provided, the
    recency fallback must only return memories whose id is in the Cypher
    result set — not the full corpus."""
    mem_in = await shop_only_storage.save_memory(
        learning="This memory should appear.", entities=[]
    )
    mem_out = await shop_only_storage.save_memory(
        learning="This memory should NOT appear.", entities=[]
    )
    allowed_id = f"memory:{mem_in.id}"

    service = SearchService(storage=shop_only_storage)
    with patch(
        "slayer.search.graph.get_filtered_ids",
        return_value=frozenset({allowed_id}),
    ):
        response = await service.search(
            cypher_filter="MATCH (m:Memory) RETURN m.id AS id",
        )

    result_ids = {h.id for h in response.memories}
    assert mem_in.id in result_ids
    assert mem_out.id not in result_ids
