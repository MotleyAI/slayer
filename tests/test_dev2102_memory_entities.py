"""Memory entity extraction of fields a query's own extensions add."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import DatasetExtension, QueryDimension, QueryMetric, SlayerQuery
from slayer.memories.resolver import extract_entities_from_query
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def storage(tmp_path: Path) -> AsyncIterator[StorageBackend]:
    s = YAMLStorage(base_dir=str(tmp_path))
    await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
    await s.save_dataset(SlayerDataset(
        name="orders", data_source="mydb", sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="ship_region_id", sql="ship_region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    ))
    await s.save_dataset(SlayerDataset(
        name="customers", data_source="mydb", sql_table="customers",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
            DatasetField(name="region_id", sql="region_id", datatype=DataType.FLOAT),
        ],
    ))
    await s.save_dataset(SlayerDataset(
        name="regions", data_source="mydb", sql_table="regions",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
        ],
    ))
    yield s


class TestRootExtensionFields:
    async def test_projected_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.amount * 2", alias="premium")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_aggregated_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(population="orders", metrics=[QueryMetric(metric="avg(orders.amount * 2)")])
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_root_qualified_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.amount * 2", alias="premium")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_measure_records_reads_through_extension_column(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="orders", metrics=[QueryMetric(metric="sum(orders.amount * 2)", alias="premium_total")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms
        assert not {"mydb.orders.premium", "mydb.orders.premium_total"} & set(result.canonical_forms)


class TestJoinedExtensionFields:
    async def test_joined_column_records_its_stored_reads(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.region_id + 1", alias="premium")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.customers.region_id" in result.canonical_forms
        assert "mydb.customers.premium" not in result.canonical_forms


def _projecting(path: str, *, extension: DatasetExtension) -> SlayerQuery:
    return SlayerQuery(
        population="orders", extensions=[extension],
        dimensions=[QueryDimension(field=path)], metrics=[QueryMetric(metric="count(orders.*)")],
    )


_ROOT_JOIN = DatasetExtension(
    dataset="orders", relationships=[DatasetRelationship(to="regions", from_columns=["ship_region_id"], to_columns=["id"])],
)
_JOINED_JOIN = DatasetExtension(
    dataset="customers", relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
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
            population="orders", dimensions=[QueryDimension(field="regions.name")],
            metrics=[QueryMetric(metric="count(orders.*)")], extensions=[_JOINED_JOIN],
        )
        result = await extract_entities_from_query(q, storage=named_storage)
        assert "mydb.regions.name" in result.canonical_forms
        assert "otherdb.regions.name" not in result.canonical_forms

    async def test_extension_column_reads_through_extension_join(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="orders", extensions=[_ROOT_JOIN],
            dimensions=[QueryDimension(field="regions.name", alias="region_label")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.regions.name" in result.canonical_forms
        assert "mydb.orders.region_label" not in result.canonical_forms


#: ``buyer`` names a new query-local edge (non-stored pairs) or aliases the stored ``orders → customers`` edge.
_NAMED_JOINS = {
    "new_edge": DatasetRelationship(to="customers", name="buyer", from_columns=["ship_region_id"], to_columns=["region_id"]),
    "alias": DatasetRelationship(to="customers", name="buyer", from_columns=["customer_id"], to_columns=["id"]),
}


_AGGREGATED_PATHS = [("count(buyer.name)", "mydb.customers.name"), ("count(buyer.*)", "mydb.customers")]


def _assert_no_edge_token(forms: list[str]) -> None:
    assert not any("buyer" in f for f in forms), forms


class TestNamedExtensionJoins:
    @pytest.mark.parametrize("path", ["buyer.name", "orders.buyer.name"])
    @pytest.mark.parametrize("kind", sorted(_NAMED_JOINS))
    async def test_named_join_records_stored_target(self, storage: StorageBackend, kind: str, path: str) -> None:
        extension = DatasetExtension(dataset="orders", relationships=[_NAMED_JOINS[kind]])
        result = await extract_entities_from_query(_projecting(path, extension=extension), storage=storage)
        assert "mydb.customers.name" in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    @pytest.mark.parametrize("path", ["buyer.premium", "orders.buyer.premium"])
    @pytest.mark.parametrize("kind", sorted(_NAMED_JOINS))
    async def test_extension_column_through_named_join(self, storage: StorageBackend, kind: str, path: str) -> None:
        q = SlayerQuery(
            population="orders",
            extensions=[DatasetExtension(dataset="orders", relationships=[_NAMED_JOINS[kind]])],
            dimensions=[QueryDimension(field=f"{path.removesuffix('premium')}region_id + 1", alias="premium")],
            metrics=[QueryMetric(metric="count(orders.*)")],
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
            population="orders", extensions=[DatasetExtension(dataset="orders", relationships=[_NAMED_JOINS[kind]])],
            metrics=[QueryMetric(metric=formula)],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert entity in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)


@pytest_asyncio.fixture
async def named_storage(storage: StorageBackend) -> StorageBackend:
    """``orders``'s stored relationship to ``customers`` named ``buyer``; ``otherdb`` (first priority) also has ``regions``."""
    orders = await storage.get_dataset("orders", data_source="mydb")
    assert orders is not None
    await storage.save_dataset(orders.model_copy(update={
        "relationships": [DatasetRelationship(to="customers", name="buyer", from_columns=["customer_id"], to_columns=["id"])],
    }))
    await storage.save_datasource(DatasourceConfig(name="otherdb", type="postgres", host="y"))
    await storage.save_dataset(SlayerDataset(
        name="regions", data_source="otherdb", sql_table="regions",
        fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                 DatasetField(name="name", sql="name", datatype=DataType.STRING)],
    ))
    await storage.set_datasource_priority(["otherdb", "mydb"])
    return storage


class TestStoredNamedJoinsWithoutExtensions:
    @pytest.mark.parametrize("path", ["buyer.name", "orders.buyer.name"])
    async def test_named_join_records_stored_target(self, named_storage: StorageBackend, path: str) -> None:
        q = SlayerQuery(
            population="orders", dimensions=[QueryDimension(field=path)], metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=named_storage)
        assert "mydb.customers.name" in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    @pytest.mark.parametrize(("formula", "entity"), _AGGREGATED_PATHS)
    async def test_aggregated_named_join_path(self, named_storage: StorageBackend, formula: str, entity: str) -> None:
        q = SlayerQuery(population="orders", metrics=[QueryMetric(metric=formula)])
        result = await extract_entities_from_query(q, storage=named_storage)
        assert entity in result.canonical_forms
        _assert_no_edge_token(result.canonical_forms)

    async def test_unrouted_model_resolves_to_priority_winner(self, named_storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="orders", dimensions=[QueryDimension(field="regions.name")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=named_storage)
        assert "otherdb.regions.name" in result.canonical_forms
        assert "mydb.regions.name" not in result.canonical_forms



class TestRootlessQueries:
    async def test_extension_field_of_the_inferred_population(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            dimensions=[QueryDimension(field="orders.id")], metrics=[QueryMetric(metric="avg(orders.amount * 2)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert {"mydb.orders", "mydb.orders.amount"} <= set(result.canonical_forms)
        assert "mydb.orders.premium" not in result.canonical_forms

    async def test_inferred_population_is_the_engines(self, storage: StorageBackend) -> None:
        q = SlayerQuery(dimensions=[QueryDimension(field="customers.name")],
                        metrics=[QueryMetric(metric="count(customers.*)")])
        result = await extract_entities_from_query(q, storage=storage)
        assert result.canonical_forms == ["mydb.customers", "mydb.customers.name"]


@pytest_asyncio.fixture
async def cased_storage(tmp_path: Path) -> AsyncIterator[StorageBackend]:
    s = YAMLStorage(base_dir=str(tmp_path / "cased"))
    await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
    await s.save_dataset(SlayerDataset(
        name="Orders", data_source="mydb", sql_table="orders",
        fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                 DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                 DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT)],
        relationships=[DatasetRelationship(to="Customers", from_columns=["customer_id"], to_columns=["id"])],
    ))
    await s.save_dataset(SlayerDataset(
        name="Customers", data_source="mydb", sql_table="customers",
        fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                 DatasetField(name="name", sql="name", datatype=DataType.STRING)],
    ))
    yield s


class TestModelNameCase:
    async def test_a_source_model_written_in_another_case(self, cased_storage: StorageBackend) -> None:
        q = SlayerQuery(population="orders", dimensions=[QueryDimension(field="orders.amount")],
                        metrics=[QueryMetric(metric="count(orders.*)")])
        result = await extract_entities_from_query(q, storage=cased_storage)
        assert {"mydb.Orders", "mydb.Orders.amount"} <= set(result.canonical_forms)

    async def test_an_extension_written_in_another_case(self, cased_storage: StorageBackend) -> None:
        q = SlayerQuery(
            population="Orders", metrics=[QueryMetric(metric="Customers.shout")],
            extensions=[DatasetExtension(dataset="customers",
                                       metrics=[DatasetMetric(name="shout", expression="max(upper(name))")])],
        )
        result = await extract_entities_from_query(q, storage=cased_storage)
        assert "mydb.Customers.name" in result.canonical_forms
        assert "mydb.Customers.shout" not in result.canonical_forms
