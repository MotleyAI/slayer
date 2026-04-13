"""Tests for the MCP server tools."""

import asyncio
import json
import os
import tempfile
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    DatasourceConfig,
    Dimension,
    Measure,
    ModelJoin,
    SlayerModel,
)
from slayer.mcp.server import (
    _format_table,
    _friendly_db_error,
    create_mcp_server,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def mcp_server(storage: YAMLStorage):
    return create_mcp_server(storage=storage)


def _call(mcp_server, name: str, arguments: dict[str, Any] = {}) -> str:
    """Call an MCP tool and return the text result."""
    content_blocks, result_dict = asyncio.run(mcp_server.call_tool(name, arguments))
    return content_blocks[0].text


class TestDatasourceSummary:
    def test_empty(self, mcp_server) -> None:
        result = _call(mcp_server, "datasource_summary")
        data = json.loads(result)
        assert data["datasources"] == []
        assert data["models"] == []
        assert data["model_count"] == 0

    def test_with_models(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders",
            sql_table="t",
            data_source="test",
            dimensions=[Dimension(name="status", type=DataType.STRING)],
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = _call(mcp_server, "datasource_summary")
        parsed = json.loads(result)
        assert parsed["model_count"] == 1
        assert parsed["models"][0]["name"] == "orders"
        assert len(parsed["models"][0]["dimensions"]) == 1
        assert len(parsed["models"][0]["measures"]) == 1

    def test_hidden_models_excluded(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="visible", sql_table="t", data_source="test"))
        storage.save_model(SlayerModel(name="hidden", sql_table="t", data_source="test", hidden=True))
        result = _call(mcp_server, "datasource_summary")
        parsed = json.loads(result)
        assert parsed["model_count"] == 1
        assert parsed["models"][0]["name"] == "visible"

    def test_includes_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        from slayer.core.models import DatasourceConfig
        storage.save_datasource(DatasourceConfig(
            name="mydb", type="postgres", host="localhost", description="Production DB",
        ))
        result = _call(mcp_server, "datasource_summary")
        parsed = json.loads(result)
        assert parsed["datasources"][0]["name"] == "mydb"
        assert parsed["datasources"][0]["description"] == "Production DB"


class TestInspectModel:
    def test_not_found(self, mcp_server) -> None:
        result = _call(mcp_server, "inspect_model", {"model_name": "nonexistent"})
        assert "not found" in result

    def test_found_with_schema(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="test",
            sql_table="t",
            data_source="test",
            description="Test model",
            dimensions=[Dimension(name="x", type=DataType.STRING)],
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = _call(mcp_server, "inspect_model", {"model_name": "test"})
        parsed = json.loads(result)
        assert parsed["name"] == "test"
        assert parsed["description"] == "Test model"
        assert len(parsed["dimensions"]) == 1
        assert parsed["dimensions"][0]["type"] == "string"
        assert len(parsed["measures"]) == 1

    def test_show_sql(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="test", sql_table="public.test", data_source="test"))
        result = _call(mcp_server, "inspect_model", {"model_name": "test", "show_sql": True})
        parsed = json.loads(result)
        assert parsed["sql_table"] == "public.test"


class TestCreateModel:
    def test_create(self, mcp_server, storage: YAMLStorage) -> None:
        result = _call(mcp_server, "create_model", {
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "dimensions": [
                {"name": "id", "sql": "id", "type": "number", "primary_key": "true"},
                {"name": "status", "sql": "status", "type": "string"},
            ],
            "measures": [
                {"name": "revenue", "sql": "amount"},
            ],
        })
        assert "orders" in result
        assert "created" in result
        assert storage.get_model("orders") is not None

    def test_create_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        result = _call(mcp_server, "create_model", {
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "measures": [
                {"name": "revenue", "sql": "amount", "allowed_aggregations": ["sum", "avg"]},
            ],
        })
        assert "created" in result
        model = storage.get_model("orders")
        assert model.measures[0].allowed_aggregations == ["sum", "avg"]

    def test_create_reports_replaced(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "create_model", {"name": "orders", "sql_table": "t2", "data_source": "test"})
        assert "replaced" in result

    def test_create_from_query_rejects_mixed_params(self, mcp_server) -> None:
        result = _call(mcp_server, "create_model", {
            "name": "bad",
            "query": {"source_model": "orders", "fields": ["*:count"]},
            "sql_table": "public.orders",
        })
        assert "Error" in result
        assert "query" in result
        assert "sql_table" in result

    def test_create_from_query_routes_to_engine(self, mcp_server, storage: YAMLStorage) -> None:
        # Without a real datasource/data, the engine will return a friendly error —
        # but the error message proves we routed to the query path.
        storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test_ds",
            measures=[Measure(name="amount", sql="amount")],
        ))
        result = _call(mcp_server, "create_model", {
            "name": "summary",
            "query": {"source_model": "orders", "fields": ["amount:sum"]},
        })
        # Should fail on missing datasource, not on "missing sql_table"
        assert "Datasource" in result


class TestEditModel:
    """Tests for the edit_model MCP tool with upsert semantics."""

    # --- Measure upserts ---

    def test_upsert_new_measure(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "measures": [{"name": "total", "sql": "amount"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created measure 'total'" in c for c in parsed["changes"])
        model = storage.get_model("orders")
        assert len(model.measures) == 2

    def test_upsert_measure_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "measures": [{"name": "total", "sql": "amount", "allowed_aggregations": ["sum", "avg"]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = storage.get_model("orders")
        total = next(m for m in model.measures if m.name == "total")
        assert total.allowed_aggregations == ["sum", "avg"]

    def test_upsert_existing_measure(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting an existing measure updates it instead of erroring."""
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount", description="old")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "measures": [{"name": "revenue", "sql": "price"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated measure 'revenue'" in c for c in parsed["changes"])
        model = storage.get_model("orders")
        assert len(model.measures) == 1
        assert model.measures[0].sql == "price"

    def test_upsert_existing_measure_partial_update(self, mcp_server, storage: YAMLStorage) -> None:
        """Partial upsert: only specified fields change, others are preserved."""
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount", description="Total revenue")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "measures": [{"name": "revenue", "description": "Updated description"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        m = storage.get_model("orders").measures[0]
        assert m.description == "Updated description"
        assert m.sql == "amount"  # unchanged

    # --- Dimension upserts ---

    def test_upsert_new_dimension(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created dimension 'region'" in c for c in parsed["changes"])
        assert any(d.name == "region" for d in storage.get_model("orders").dimensions)

    def test_upsert_existing_dimension_partial_update(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            dimensions=[Dimension(name="status", sql="status", type=DataType.STRING)],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "dimensions": [{"name": "status", "description": "Order status"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        d = storage.get_model("orders").dimensions[0]
        assert d.description == "Order status"
        assert d.sql == "status"  # unchanged
        assert d.type == DataType.STRING  # unchanged

    def test_upsert_multiple_mixed_create_update(self, mcp_server, storage: YAMLStorage) -> None:
        """One new + one existing entity in the same call."""
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "measures": [
                {"name": "revenue", "description": "Updated"},
                {"name": "profit", "sql": "revenue - cost"},
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated measure 'revenue'" in c for c in parsed["changes"])
        assert any("created measure 'profit'" in c for c in parsed["changes"])
        model = storage.get_model("orders")
        assert len(model.measures) == 2

    def test_invalid_dimension_type_on_upsert(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "dimensions": [{"name": "bad", "type": "invalid_type"}],
        })
        assert "Invalid" in result

    # --- Aggregation upserts ---

    def test_upsert_new_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "aggregations": [{"name": "my_agg", "formula": "SUM({value})"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = storage.get_model("orders")
        assert len(model.aggregations) == 1
        assert model.aggregations[0].name == "my_agg"

    def test_upsert_existing_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", formula="SUM({value})")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "aggregations": [{"name": "my_agg", "formula": "AVG({value})"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = storage.get_model("orders")
        assert model.aggregations[0].formula == "AVG({value})"

    def test_remove_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", formula="SUM({value})")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove": {"aggregations": ["my_agg"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(storage.get_model("orders").aggregations) == 0

    # --- Join upserts ---

    def test_upsert_new_join(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = storage.get_model("orders")
        assert len(model.joins) == 1
        assert model.joins[0].target_model == "customers"

    def test_upsert_existing_join(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "joins": [{"target_model": "customers", "join_pairs": [["buyer_id", "id"]]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = storage.get_model("orders")
        assert len(model.joins) == 1
        assert model.joins[0].join_pairs == [["buyer_id", "id"]]

    def test_remove_join(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove": {"joins": ["customers"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(storage.get_model("orders").joins) == 0

    # --- Filter management ---

    def test_add_filter(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "add_filters": ["deleted_at IS NULL"],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert "deleted_at IS NULL" in storage.get_model("orders").filters

    def test_add_duplicate_filter_skipped(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            filters=["deleted_at IS NULL"],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "add_filters": ["deleted_at IS NULL"],
        })
        # No changes because the filter already exists
        assert "No changes" in result

    def test_remove_filter(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            filters=["deleted_at IS NULL"],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove_filters": ["deleted_at IS NULL"],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(storage.get_model("orders").filters) == 0

    def test_remove_filter_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove_filters": ["nonexistent"],
        })
        assert "Filter not found" in result

    # --- Scalar metadata ---

    def test_update_description(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "description": "Updated",
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert storage.get_model("orders").description == "Updated"

    def test_set_sql_table(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "sql_table": "public.orders",
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert storage.get_model("orders").sql_table == "public.orders"

    def test_set_hidden(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "hidden": True,
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert storage.get_model("orders").hidden is True

    # --- Multiple changes ---

    def test_multiple_changes(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "description": "Orders table",
            "measures": [{"name": "total", "sql": "amount"}],
            "dimensions": [{"name": "status", "sql": "status", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(parsed["changes"]) == 3
        model = storage.get_model("orders")
        assert model.description == "Orders table"
        assert len(model.measures) == 2
        assert any(d.name == "status" for d in model.dimensions)

    # --- Typed remove ---

    def test_remove_measure_typed(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount"), Measure(name="total", sql="x")],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove": {"measures": ["total"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = storage.get_model("orders")
        assert len(model.measures) == 1

    def test_remove_dimension_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove": {"dimensions": ["nonexistent"]},
        })
        assert "not found" in result

    def test_remove_invalid_key(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove": {"invalid": ["x"]},
        })
        assert "Invalid remove key" in result

    def test_remove_then_recreate_same_call(self, mcp_server, storage: YAMLStorage) -> None:
        """Remove a dimension then upsert one with the same name in the same call."""
        storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            dimensions=[Dimension(name="status", sql="old_col", type=DataType.STRING)],
        ))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "remove": {"dimensions": ["status"]},
            "dimensions": [{"name": "status", "sql": "new_col", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        d = storage.get_model("orders").dimensions[0]
        assert d.sql == "new_col"

    # --- Error cases ---

    def test_model_not_found(self, mcp_server) -> None:
        result = _call(mcp_server, "edit_model", {
            "model_name": "nope",
            "description": "test",
        })
        assert "not found" in result

    def test_no_changes(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {"model_name": "orders"})
        assert "No changes" in result

    def test_cross_field_validation_error(self, mcp_server, storage: YAMLStorage) -> None:
        """allowed_aggregations referencing a non-existent aggregation should fail."""
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "edit_model", {
            "model_name": "orders",
            "measures": [{"name": "rev", "sql": "amount", "allowed_aggregations": ["nonexistent_agg"]}],
        })
        assert "Validation error" in result or "not a built-in aggregation" in result


class TestDatasources:
    def test_list_empty(self, mcp_server) -> None:
        result = _call(mcp_server, "list_datasources")
        assert "No datasources configured" in result
        assert "create_datasource" in result

    def test_create_and_list(self, mcp_server, storage: YAMLStorage) -> None:
        result = _call(mcp_server, "create_datasource", {
            "name": "mydb",
            "type": "postgres",
            "host": "localhost",
        })
        assert "mydb" in result
        assert storage.get_datasource("mydb") is not None

        result = _call(mcp_server, "list_datasources")
        assert "mydb" in result
        assert "postgres" in result

    def test_create_reports_connection_failure(self, mcp_server) -> None:
        result = _call(mcp_server, "create_datasource", {
            "name": "bad",
            "type": "postgres",
            "host": "localhost",
            "port": 59999,
            "database": "nonexistent",
        })
        assert "created" in result
        assert "connection test failed" in result.lower()

    def test_create_reports_replaced(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
        result = _call(mcp_server, "create_datasource", {"name": "ds", "type": "sqlite", "database": ":memory:"})
        assert "replaced" in result

    def test_list_with_malformed_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        # A valid datasource alongside a malformed one
        storage.save_datasource(DatasourceConfig(name="good", type="sqlite", database=":memory:"))
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = _call(mcp_server, "list_datasources")
        assert "good (sqlite)" in result
        assert "bad" in result
        assert "ERROR" in result

    def test_summary_with_malformed_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_datasource(DatasourceConfig(name="good", type="sqlite", database=":memory:"))
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = _call(mcp_server, "datasource_summary")
        data = json.loads(result)
        names = [d["name"] for d in data["datasources"]]
        assert "good" in names
        assert "bad" in names
        bad_entry = next(d for d in data["datasources"] if d["name"] == "bad")
        assert "error" in bad_entry

    def test_describe_malformed_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = _call(mcp_server, "describe_datasource", {"name": "bad"})
        assert "invalid" in result.lower()

    def test_describe_not_found(self, mcp_server) -> None:
        result = _call(mcp_server, "describe_datasource", {"name": "nope"})
        assert "not found" in result

    def test_describe_shows_details(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_datasource(DatasourceConfig(
            name="testds", type="postgres", host="localhost", port=5432, database="testdb", username="user",
        ))
        result = _call(mcp_server, "describe_datasource", {"name": "testds"})
        assert "Datasource: testds" in result
        assert "Type: postgres" in result
        assert "Host: localhost" in result
        assert "Database: testdb" in result
        assert "Connection:" in result


class TestDeleteTools:
    def test_delete_model(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = _call(mcp_server, "delete_model", {"name": "orders"})
        assert "deleted" in result
        assert storage.get_model("orders") is None

    def test_delete_model_not_found(self, mcp_server) -> None:
        result = _call(mcp_server, "delete_model", {"name": "nope"})
        assert "not found" in result

    def test_delete_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
        result = _call(mcp_server, "delete_datasource", {"name": "ds"})
        assert "deleted" in result
        assert storage.get_datasource("ds") is None

    def test_delete_datasource_not_found(self, mcp_server) -> None:
        result = _call(mcp_server, "delete_datasource", {"name": "nope"})
        assert "not found" in result



class TestIngestionIdSkipping:
    def test_id_columns_skip_sum_avg(self) -> None:
        from slayer.engine.ingestion import _is_id_column
        assert _is_id_column("id") is True
        assert _is_id_column("user_id") is True
        assert _is_id_column("customer_key") is True
        assert _is_id_column("role_fk") is True
        assert _is_id_column("primary_pk") is True
        assert _is_id_column("amount") is False
        assert _is_id_column("quantity") is False
        assert _is_id_column("price") is False
        assert _is_id_column("width") is False


class TestFriendlyErrors:
    def test_password_error(self) -> None:
        msg = _friendly_db_error(Exception("password authentication failed for user"))
        assert "Database error:" in msg
        assert "Check that username and password" in msg

    def test_database_not_found(self) -> None:
        msg = _friendly_db_error(Exception('database "foo" does not exist'))
        assert "Verify the database name" in msg

    def test_connection_refused(self) -> None:
        msg = _friendly_db_error(Exception("connection refused"))
        assert "Check that the database server is running" in msg

    def test_unknown_error(self) -> None:
        msg = _friendly_db_error(Exception("something weird"))
        assert "Database error:" in msg
        assert "Hint:" not in msg


class TestFormatTable:
    def test_empty(self) -> None:
        assert _format_table(data=[], columns=[]) == "No results."

    def test_basic(self) -> None:
        data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        result = _format_table(data=data, columns=["a", "b"])
        assert "a | b" in result
        assert "1 | x" in result
        assert "2 | y" in result

    def test_truncation(self) -> None:
        data = [{"x": i} for i in range(100)]
        result = _format_table(data=data, columns=["x"], max_rows=10)
        assert "100 total rows" in result
        assert "showing first 10" in result
