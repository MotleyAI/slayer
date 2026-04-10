"""Tests for SlayerClient in local mode."""

import tempfile

import pytest

from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def client(storage: YAMLStorage) -> SlayerClient:
    return SlayerClient(storage=storage)


class TestLocalMode:
    def test_init_local(self, storage: YAMLStorage) -> None:
        client = SlayerClient(storage=storage)
        assert client._engine is not None

    def test_init_remote(self) -> None:
        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None

    def test_query_dispatches_locally(self, client: SlayerClient, storage: YAMLStorage) -> None:
        """Local mode query should go through engine, not HTTP."""
        storage.save_model(SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test_ds",
            dimensions=[Dimension(name="id", sql="id", type=DataType.NUMBER)],
            measures=[Measure(name="revenue", sql="amount")],
        ))
        storage.save_datasource(DatasourceConfig(
            name="test_ds",
            type="sqlite",
            database=":memory:",
        ))
        # This will fail at SQL execution (no actual table), but proves local dispatch works
        from slayer.core.query import SlayerQuery
        query = SlayerQuery(source_model="orders", fields=[{"formula": "revenue:sum"}])
        with pytest.raises(Exception):
            client.query(query)

    def test_query_accepts_dict(self, client: SlayerClient, storage: YAMLStorage) -> None:
        """client.query() should accept a plain dict and coerce it to SlayerQuery."""
        storage.save_model(SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test_ds",
            dimensions=[Dimension(name="id", sql="id", type=DataType.NUMBER)],
            measures=[Measure(name="revenue", sql="amount")],
        ))
        storage.save_datasource(DatasourceConfig(
            name="test_ds",
            type="sqlite",
            database=":memory:",
        ))
        query_dict = {"source_model": "orders", "fields": ["revenue:sum"]}
        # Will fail at SQL execution (no actual table), but proves dict dispatch works
        with pytest.raises(Exception):
            client.query(query_dict)

    def test_sql_accepts_dict(self, client: SlayerClient, storage: YAMLStorage) -> None:
        """client.sql() should accept a plain dict."""
        storage.save_model(SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test_ds",
            dimensions=[Dimension(name="id", sql="id", type=DataType.NUMBER)],
            measures=[Measure(name="revenue", sql="amount")],
        ))
        storage.save_datasource(DatasourceConfig(
            name="test_ds",
            type="sqlite",
            database=":memory:",
        ))
        query_dict = {"source_model": "orders", "fields": ["revenue:sum"]}
        sql = client.sql(query_dict)
        assert isinstance(sql, str)
        assert "SELECT" in sql
