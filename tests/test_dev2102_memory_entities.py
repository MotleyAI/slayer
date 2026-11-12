"""Memory entity extraction of fields a query's own extensions add."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery
from slayer.memories.resolver import extract_entities_from_query
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def storage(tmp_path: Path) -> AsyncIterator[StorageBackend]:
    s = YAMLStorage(base_dir=str(tmp_path))
    await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
    await s.save_model(SlayerModel(
        name="orders", data_source="mydb", sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="ship_region_id", sql="ship_region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    await s.save_model(SlayerModel(
        name="customers", data_source="mydb", sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
    ))
    await s.save_model(SlayerModel(
        name="regions", data_source="mydb", sql_table="regions",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    ))
    yield s


def _orders_extension() -> ModelExtension:
    return ModelExtension(
        source_name="orders",
        columns=[Column(name="premium", sql="amount * 2", type=DataType.DOUBLE)],
        measures=[ModelMeasure(name="premium_total", formula="sum(premium)")],
    )


class TestRootExtensionFields:
    async def test_projected_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders", extensions=[_orders_extension()],
            dimensions=[ColumnRef(name="premium")], measures=[ModelMeasure(formula="count(*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_aggregated_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders", extensions=[_orders_extension()],
            measures=[ModelMeasure(formula="avg(premium)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_root_qualified_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders", extensions=[_orders_extension()],
            dimensions=[ColumnRef(name="orders.premium")], measures=[ModelMeasure(formula="count(*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_measure_records_reads_through_extension_column(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders", extensions=[_orders_extension()],
            measures=[ModelMeasure(formula="premium_total")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert not {"mydb.orders.premium", "mydb.orders.premium_total"} & set(result.canonical_forms)


class TestJoinedExtensionFields:
    async def test_joined_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders",
            extensions=[ModelExtension(
                source_name="customers",
                columns=[Column(name="premium", sql="region_id + 1", type=DataType.DOUBLE)],
            )],
            dimensions=[ColumnRef(name="customers.premium")], measures=[ModelMeasure(formula="count(*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.customers.region_id" in result.canonical_forms
        assert "mydb.customers.premium" not in result.canonical_forms


def _projecting(path: str, *, extension: ModelExtension) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders", extensions=[extension],
        dimensions=[ColumnRef(name=path)], measures=[ModelMeasure(formula="count(*)")],
    )


_ROOT_JOIN = ModelExtension(
    source_name="orders", joins=[ModelJoin(target_model="regions", join_pairs=[["ship_region_id", "id"]])],
)
_JOINED_JOIN = ModelExtension(
    source_name="customers", joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
)


class TestExtensionJoinPaths:
    @pytest.mark.parametrize("path", ["orders.regions.name", "regions.name"])
    async def test_root_extension_join(self, storage: StorageBackend, path: str) -> None:
        result = await extract_entities_from_query(_projecting(path, extension=_ROOT_JOIN), storage=storage)
        assert "mydb.regions.name" in result.canonical_forms

    @pytest.mark.parametrize("path", ["orders.customers.regions.name", "customers.regions.name"])
    async def test_joined_extension_join(self, storage: StorageBackend, path: str) -> None:
        result = await extract_entities_from_query(_projecting(path, extension=_JOINED_JOIN), storage=storage)
        assert "mydb.regions.name" in result.canonical_forms

    async def test_short_form_route_resolves_in_the_query_datasource(self, named_storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders", dimensions=[ColumnRef(name="regions.name")],
            measures=[ModelMeasure(formula="count(*)")], extensions=[_JOINED_JOIN],
        )
        result = await extract_entities_from_query(q, storage=named_storage)
        assert "mydb.regions.name" in result.canonical_forms
        assert "otherdb.regions.name" not in result.canonical_forms

    async def test_extension_column_reads_through_extension_join(self, storage: StorageBackend) -> None:
        extension = _ROOT_JOIN.model_copy(update={
            "columns": [Column(name="region_label", sql="regions.name", type=DataType.TEXT)],
        })
        result = await extract_entities_from_query(_projecting("region_label", extension=extension), storage=storage)
        assert "mydb.regions.name" in result.canonical_forms
        assert "mydb.orders.region_label" not in result.canonical_forms


#: ``buyer`` names a new query-local edge (non-stored pairs) or aliases the stored ``orders → customers`` edge.
_NAMED_JOINS = {
    "new_edge": ModelJoin(target_model="customers", name="buyer", join_pairs=[["ship_region_id", "region_id"]]),
    "alias": ModelJoin(target_model="customers", name="buyer", join_pairs=[["customer_id", "id"]]),
}


_AGGREGATED_PATHS = [("count(buyer.name)", "mydb.customers.name"), ("count(buyer.*)", "mydb.customers")]


def _assert_no_edge_token(forms: list[str]) -> None:
    assert not any("buyer" in f for f in forms), forms


class TestNamedExtensionJoins:
    @pytest.mark.parametrize("path", ["buyer.name", "orders.buyer.name"])
    @pytest.mark.parametrize("kind", sorted(_NAMED_JOINS))
    async def test_named_join_records_stored_target(self, storage: StorageBackend, kind: str, path: str) -> None:
        extension = ModelExtension(source_name="orders", joins=[_NAMED_JOINS[kind]])
        result = await extract_entities_from_query(_projecting(path, extension=extension), storage=storage)
        assert "mydb.customers.name" in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    @pytest.mark.parametrize("path", ["buyer.premium", "orders.buyer.premium"])
    @pytest.mark.parametrize("kind", sorted(_NAMED_JOINS))
    async def test_extension_column_through_named_join(self, storage: StorageBackend, kind: str, path: str) -> None:
        q = SlayerQuery(
            source_model="orders",
            extensions=[
                ModelExtension(source_name="orders", joins=[_NAMED_JOINS[kind]]),
                ModelExtension(
                    source_name="customers",
                    columns=[Column(name="premium", sql="region_id + 1", type=DataType.DOUBLE)],
                ),
            ],
            dimensions=[ColumnRef(name=path)], measures=[ModelMeasure(formula="count(*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.customers.region_id" in result.canonical_forms
        assert "mydb.customers.premium" not in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    @pytest.mark.parametrize(("formula", "entity"), _AGGREGATED_PATHS)
    @pytest.mark.parametrize("kind", sorted(_NAMED_JOINS))
    async def test_aggregated_named_join_path(
        self, storage: StorageBackend, kind: str, formula: str, entity: str,
    ) -> None:
        q = SlayerQuery(
            source_model="orders", extensions=[ModelExtension(source_name="orders", joins=[_NAMED_JOINS[kind]])],
            measures=[ModelMeasure(formula=formula)],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert entity in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)


@pytest_asyncio.fixture
async def named_storage(storage: StorageBackend) -> StorageBackend:
    """``orders``'s stored join to ``customers`` named ``buyer``; ``otherdb`` (first priority) also has ``regions``."""
    orders = await storage.get_model("orders", data_source="mydb")
    assert orders is not None
    await storage.save_model(orders.model_copy(update={
        "joins": [ModelJoin(target_model="customers", name="buyer", join_pairs=[["customer_id", "id"]])],
    }))
    await storage.save_datasource(DatasourceConfig(name="otherdb", type="postgres", host="y"))
    await storage.save_model(SlayerModel(
        name="regions", data_source="otherdb", sql_table="regions",
        columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                 Column(name="name", sql="name", type=DataType.TEXT)],
    ))
    await storage.set_datasource_priority(["otherdb", "mydb"])
    return storage


class TestStoredNamedJoinsWithoutExtensions:
    @pytest.mark.parametrize("path", ["buyer.name", "orders.buyer.name"])
    async def test_named_join_records_stored_target(self, named_storage: StorageBackend, path: str) -> None:
        q = SlayerQuery(
            source_model="orders", dimensions=[ColumnRef(name=path)], measures=[ModelMeasure(formula="count(*)")],
        )
        result = await extract_entities_from_query(q, storage=named_storage)
        assert "mydb.customers.name" in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    @pytest.mark.parametrize(("formula", "entity"), _AGGREGATED_PATHS)
    async def test_aggregated_named_join_path(self, named_storage: StorageBackend, formula: str, entity: str) -> None:
        q = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula=formula)])
        result = await extract_entities_from_query(q, storage=named_storage)
        assert entity in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    async def test_unrouted_model_resolves_to_priority_winner(self, named_storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders", dimensions=[ColumnRef(name="regions.name")],
            measures=[ModelMeasure(formula="count(*)")],
        )
        result = await extract_entities_from_query(q, storage=named_storage)
        assert "otherdb.regions.name" in result.canonical_forms
        assert "mydb.regions.name" not in result.canonical_forms



class TestRootlessQueries:
    async def test_extension_field_of_the_inferred_population(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            extensions=[_orders_extension()], dimensions=[ColumnRef(name="orders.id")],
            measures=[ModelMeasure(formula="avg(orders.premium)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert {"mydb.orders", "mydb.orders.amount"} <= set(result.canonical_forms)
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_inferred_population_is_the_engines(self, storage: StorageBackend) -> None:
        q = SlayerQuery(dimensions=[ColumnRef(name="customers.name")], measures=[ModelMeasure(formula="count(*)")])
        result = await extract_entities_from_query(q, storage=storage)
        assert result.canonical_forms == ["mydb.customers", "mydb.customers.name"]
