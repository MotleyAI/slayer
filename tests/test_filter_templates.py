"""Tests for filter template variable resolution."""

import sqlite3
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


class TestResolveValue:
    """Unit tests for variable resolution in filter values."""

    def test_exact_variable_returns_native_type(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["customer_id == '{cid}'"],
            variables={"cid": 42},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["customer_id == '42'"]
        assert resolved.variables is None

    def test_string_substitution(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["contains(name, 'prefix_{suffix}')"],
            variables={"suffix": "test"},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["contains(name, 'prefix_test')"]

    def test_missing_variable_kept_as_is(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["x == '{missing}'"],
            variables={"other": 1},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["x == '{missing}'"]

    def test_no_variables_returns_same(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["x == 'hello'"],
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["x == 'hello'"]

    def test_non_string_values_unchanged(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["x > 100"],
            variables={"x": 999},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["x > 100"]

    def test_multiple_variables_in_one_value(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["x == '{a}-{b}'"],
            variables={"a": "hello", "b": "world"},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["x == 'hello-world'"]

    def test_composite_filter_resolved(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["status == '{status}' or region == '{region}'"],
            variables={"status": "active", "region": "US"},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["status == 'active' or region == 'US'"]

    def test_date_variable(self) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["created_at > '{start}'"],
            variables={"start": "2024-01-01"},
        )
        resolved = query.resolve_variables()
        assert resolved.filters == ["created_at > '2024-01-01'"]


class TestFilterTemplateIntegration:
    """Integration test: filter templates with a real SQLite DB."""

    @pytest.fixture
    def env(self, tmp_path: Path):
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE orders (id INTEGER, status TEXT, amount REAL, customer_id INTEGER)")
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?)",
            [(1, "completed", 100, 10), (2, "completed", 200, 10),
             (3, "pending", 50, 20), (4, "cancelled", 75, 30)],
        )
        conn.commit()
        conn.close()

        storage_dir = str(tmp_path / "storage")
        storage = YAMLStorage(base_dir=storage_dir)
        storage.save_datasource(DatasourceConfig(name="testdb", type="sqlite", database=db_path))
        storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="testdb",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER),
                Dimension(name="status", sql="status", type=DataType.STRING),
                Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            ],
            measures=[
                Measure(name="count", type=DataType.COUNT),
                Measure(name="total", sql="amount", type=DataType.SUM),
            ],
        ))
        return SlayerQueryEngine(storage=storage)

    @pytest.mark.integration
    def test_filter_by_status_variable(self, env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["status == '{status}'"],
            variables={"status": "completed"},
        )
        result = env.execute(query=query)
        assert result.data[0]["orders.count"] == 2

    @pytest.mark.integration
    def test_filter_by_customer_id_variable(self, env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "total"}],
            filters=["customer_id == '{cid}'"],
            variables={"cid": 10},
        )
        result = env.execute(query=query)
        assert result.data[0]["orders.total"] == 300.0
