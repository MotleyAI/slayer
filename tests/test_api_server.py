"""Tests for the FastAPI server."""

import tempfile

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import QueryRequest, create_app
from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def client(storage: YAMLStorage) -> TestClient:
    app = create_app(storage=storage)
    return TestClient(app)


class TestHealth:
    def test_health(self, client: TestClient) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestModels:
    def test_list_empty(self, client: TestClient) -> None:
        resp = client.get("/models")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_create_and_get(self, client: TestClient) -> None:
        model = {
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test",
            "columns": [
                {"name": "id", "sql": "id", "type": "number"},
                {"name": "revenue", "sql": "amount", "type": "number"},
            ],
        }
        resp = client.post("/models", json=model)
        assert resp.status_code == 200
        assert resp.json()["name"] == "orders"

        resp = client.get("/models/orders")
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "orders"
        assert len(body["columns"]) == 2

    def test_list_after_create(self, client: TestClient) -> None:
        client.post("/models", json={"name": "a", "sql_table": "t", "data_source": "test"})
        client.post("/models", json={"name": "b", "sql_table": "t", "data_source": "test"})
        resp = client.get("/models")
        names = sorted(m["name"] for m in resp.json())
        assert names == ["a", "b"]

    def test_list_includes_description(self, client: TestClient) -> None:
        client.post("/models", json={"name": "orders", "sql_table": "t", "data_source": "test", "description": "Order data"})
        resp = client.get("/models")
        entry = resp.json()[0]
        assert entry["name"] == "orders"
        assert entry["description"] == "Order data"

    def test_update(self, client: TestClient) -> None:
        client.post("/models", json={"name": "orders", "sql_table": "t", "data_source": "test"})
        client.put("/models/orders", json={"name": "orders", "sql_table": "t", "data_source": "test", "description": "Updated"})
        resp = client.get("/models/orders")
        assert resp.json()["description"] == "Updated"

    def test_delete(self, client: TestClient) -> None:
        client.post("/models", json={"name": "orders", "sql_table": "t", "data_source": "test"})
        resp = client.delete("/models/orders")
        assert resp.status_code == 200
        resp = client.get("/models/orders")
        assert resp.status_code == 404

    def test_delete_nonexistent(self, client: TestClient) -> None:
        resp = client.delete("/models/nonexistent")
        assert resp.status_code == 404

    def test_get_nonexistent(self, client: TestClient) -> None:
        resp = client.get("/models/nonexistent")
        assert resp.status_code == 404

    def test_hidden_model_excluded_from_list(self, client: TestClient) -> None:
        client.post("/models", json={"name": "visible", "sql_table": "t", "data_source": "test"})
        client.post("/models", json={"name": "secret", "sql_table": "t", "data_source": "test", "hidden": True})
        resp = client.get("/models")
        names = [m["name"] for m in resp.json()]
        assert "visible" in names
        assert "secret" not in names

    def test_hidden_columns_excluded_from_get(self, client: TestClient) -> None:
        model = {
            "name": "orders",
            "sql_table": "t",
            "data_source": "test",
            "columns": [
                {"name": "id", "sql": "id", "type": "number"},
                {"name": "internal_flag", "sql": "flag", "type": "string", "hidden": True},
                {"name": "revenue", "sql": "amount", "type": "number"},
                {"name": "secret_sum", "sql": "x", "type": "number", "hidden": True},
            ],
        }
        client.post("/models", json=model)
        resp = client.get("/models/orders")
        data = resp.json()
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["id", "revenue"]

    def test_all_named_measures_returned_from_get(self, client: TestClient) -> None:
        """``ModelMeasure`` has no ``hidden`` field; every saved measure must come
        back from ``/models/{name}``.
        """
        model = {
            "name": "orders",
            "sql_table": "t",
            "data_source": "test",
            "columns": [{"name": "amount", "sql": "amount", "type": "number"}],
            "measures": [
                {"name": "aov", "formula": "amount:sum / *:count"},
                {"name": "revenue", "formula": "amount:sum"},
            ],
        }
        client.post("/models", json=model)
        resp = client.get("/models/orders")
        data = resp.json()
        names = [m["name"] for m in data["measures"]]
        assert names == ["aov", "revenue"]


class TestDatasources:
    def test_list_empty(self, client: TestClient) -> None:
        resp = client.get("/datasources")
        assert len(resp.json()) == 0

    def test_create_and_list(self, client: TestClient) -> None:
        ds = {"name": "mydb", "type": "postgres", "host": "localhost"}
        resp = client.post("/datasources", json=ds)
        assert resp.status_code == 200

        resp = client.get("/datasources")
        entries = resp.json()
        assert len(entries) == 1
        assert entries[0]["name"] == "mydb"
        assert entries[0]["type"] == "postgres"

    def test_get_datasource(self, client: TestClient) -> None:
        ds = {"name": "mydb", "type": "postgres", "host": "localhost", "password": "secret"}
        client.post("/datasources", json=ds)
        resp = client.get("/datasources/mydb")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "mydb"
        assert data["password"] == "***"

    def test_get_datasource_not_found(self, client: TestClient) -> None:
        resp = client.get("/datasources/nope")
        assert resp.status_code == 404

    def test_delete(self, client: TestClient) -> None:
        client.post("/datasources", json={"name": "mydb", "type": "postgres"})
        resp = client.delete("/datasources/mydb")
        assert resp.status_code == 200
        resp = client.get("/datasources")
        assert len(resp.json()) == 0

    def test_delete_nonexistent(self, client: TestClient) -> None:
        resp = client.delete("/datasources/nope")
        assert resp.status_code == 404


class TestQuery:
    def test_query_missing_model(self, client: TestClient) -> None:
        resp = client.post("/query", json={"source_model": "nonexistent", "fields": [{"formula": "*:count"}]})
        assert resp.status_code == 400

    def test_query_missing_datasource(self, client: TestClient, storage: YAMLStorage) -> None:
        storage.save_model(SlayerModel(
            name="orders",
            sql_table="t",
            data_source="missing_ds",
            columns=[Column(name="revenue", sql="amount", type=DataType.NUMBER)],
        ))
        resp = client.post("/query", json={"source_model": "orders", "measures": [{"formula": "revenue:sum"}]})
        assert resp.status_code == 400

    def test_request_measures_payload_reaches_slayer_query(self) -> None:
        """v2 `measures` key must be declared on QueryRequest so FastAPI keeps it."""
        req = QueryRequest.model_validate(
            {"source_model": "orders", "measures": [{"formula": "*:count"}]}
        )
        slayer_query = SlayerQuery.model_validate(req.model_dump(exclude_none=True))
        assert slayer_query.measures is not None
        assert len(slayer_query.measures) == 1
        assert slayer_query.measures[0].formula == "*:count"

    def test_request_legacy_fields_payload_migrates(self) -> None:
        """Legacy v1 `fields` key flows through `extra='allow'` and SlayerQuery's v1→v2 migration."""
        req = QueryRequest.model_validate(
            {"source_model": "orders", "fields": [{"formula": "*:count"}]}
        )
        slayer_query = SlayerQuery.model_validate(req.model_dump(exclude_none=True))
        assert slayer_query.measures is not None
        assert len(slayer_query.measures) == 1
        assert slayer_query.measures[0].formula == "*:count"
