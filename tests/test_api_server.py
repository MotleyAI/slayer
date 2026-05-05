"""Tests for the FastAPI server."""

import os
import shutil
import tempfile

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import QueryRequest, create_app
from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.storage.yaml_storage import YAMLStorage


# `create_app` builds an MCP server (~2 s of FastMCP/pydantic schema gen)
# plus a FastAPI app, and the app captures the storage instance. Sharing
# both across the session and resetting the YAML files between tests
# avoids paying that cost on every test in this file.
@pytest.fixture(scope="session")
def _shared_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture(scope="session")
def _shared_client(_shared_storage: YAMLStorage) -> TestClient:
    app = create_app(storage=_shared_storage)
    return TestClient(app)


def _reset_yaml_storage(storage: YAMLStorage) -> None:
    # v4 nests models under ``models/<data_source>/``; recurse into
    # subdirectories instead of unlinking only top-level entries.
    for sub in ("models", "datasources"):
        d = os.path.join(storage.base_dir, sub)
        if os.path.isdir(d):
            for entry in os.listdir(d):
                path = os.path.join(d, entry)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)


@pytest.fixture
def storage(_shared_storage: YAMLStorage) -> YAMLStorage:
    _reset_yaml_storage(_shared_storage)
    return _shared_storage


@pytest.fixture
def client(_shared_client: TestClient, storage: YAMLStorage) -> TestClient:
    # Depending on `storage` ensures the per-test reset runs first.
    return _shared_client


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
        resp = client.post("/query", json={"source_model": "nonexistent", "measures": [{"formula": "*:count"}]})
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


class TestQueryBackedModelsAPI:
    """REST surface for query-backed models — POST /models with source_queries
    and POST /query with run-by-name body shape.
    """

    def test_post_models_creates_query_backed_model(
        self, client: TestClient, storage: YAMLStorage
    ) -> None:
        # Set up upstream + a datasource so cache refresh succeeds at save time.
        from slayer.core.models import DatasourceConfig
        import asyncio
        asyncio.get_event_loop().run_until_complete(
            storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=":memory:"
            ))
        )
        asyncio.get_event_loop().run_until_complete(
            storage.save_model(SlayerModel(
                name="upstream", sql_table="t", data_source="ds",
                columns=[Column(name="amount", sql="amount", type=DataType.NUMBER)],
            ))
        )
        resp = client.post("/models", json={
            "name": "qb_via_api",
            "data_source": "ds",
            "source_queries": [{
                "source_model": "upstream",
                "measures": [{"formula": "amount:sum"}],
                "dry_run": True,
            }],
            "query_variables": {},
        })
        assert resp.status_code == 200, resp.text
        # GET returns the model with source_queries + query_variables
        get_resp = client.get("/models/qb_via_api")
        assert get_resp.status_code == 200
        body = get_resp.json()
        assert "source_queries" in body
        assert body["source_queries"][0]["source_model"] == "upstream"

    def test_post_models_rejects_user_columns_on_query_backed(
        self, client: TestClient, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        import asyncio
        asyncio.get_event_loop().run_until_complete(
            storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=":memory:"
            ))
        )
        asyncio.get_event_loop().run_until_complete(
            storage.save_model(SlayerModel(
                name="upstream", sql_table="t", data_source="ds",
                columns=[Column(name="amount", sql="amount", type=DataType.NUMBER)],
            ))
        )
        resp = client.post("/models", json={
            "name": "qb_bad",
            "data_source": "ds",
            "source_queries": [{
                "source_model": "upstream",
                "measures": [{"formula": "amount:sum"}],
            }],
            "columns": [{"name": "x", "type": "string"}],
        })
        # 400 with "auto-generated" in detail
        assert resp.status_code == 400
        assert "auto-generated" in resp.text or "must not be supplied" in resp.text

    def test_post_query_run_by_name_rejects_extra_query_fields(
        self, client: TestClient
    ) -> None:
        resp = client.post("/query", json={
            "name": "some_model",
            "source_model": "other",
        })
        assert resp.status_code == 400
        assert "no other query fields" in resp.text or "may not be set" in resp.text

    def test_post_query_run_by_name_rejects_whole_periods_only(
        self, client: TestClient
    ) -> None:
        # Codex review of PR #67 (commit 73f69b0): the disallowed-field check
        # had forgotten ``whole_periods_only``. Silently ignoring it is the
        # worst possible behavior — this test pins the rejection.
        resp = client.post("/query", json={
            "name": "some_model",
            "whole_periods_only": True,
        })
        assert resp.status_code == 400
        assert "no other query fields" in resp.text or "may not be set" in resp.text

    def test_post_query_run_by_name_requires_model(
        self, client: TestClient
    ) -> None:
        resp = client.post("/query", json={"name": "nonexistent"})
        assert resp.status_code == 400

    def test_post_query_either_name_or_source_model_required(
        self, client: TestClient
    ) -> None:
        resp = client.post("/query", json={})
        assert resp.status_code == 400


class TestOpenAPI400Documentation:
    """Endpoints that raise HTTPException(400) should declare it in OpenAPI
    so generated SDKs surface the error shape (Sonar S8415).
    """

    def test_query_endpoint_documents_400(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        responses = spec["paths"]["/query"]["post"]["responses"]
        assert "400" in responses

    def test_post_models_documents_400(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        responses = spec["paths"]["/models"]["post"]["responses"]
        assert "400" in responses

    def test_put_model_documents_400(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        responses = spec["paths"]["/models/{name}"]["put"]["responses"]
        assert "400" in responses

    def test_post_query_run_by_name_dry_run_returns_sql_without_executing(
        self, client: TestClient, storage: YAMLStorage
    ) -> None:
        """``{"name": "m", "dry_run": true}`` must populate ``sql`` in the response
        without ever calling the SQL client.
        """
        from slayer.core.models import DatasourceConfig
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=":memory:")
        ))
        loop.run_until_complete(storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="ds",
            columns=[Column(name="amount", sql="amount", type=DataType.NUMBER)],
        )))
        # Save a query-backed model whose stage does NOT have dry_run set.
        setup_resp = client.post("/models", json={
            "name": "qb_dryrun",
            "data_source": "ds",
            "source_queries": [{
                "source_model": "upstream",
                "measures": [{"formula": "amount:sum"}],
            }],
        })
        assert setup_resp.status_code == 200, setup_resp.text

        from slayer.sql.client import SlayerSQLClient
        execute_calls = 0
        real_execute = SlayerSQLClient.execute

        async def counting_execute(self, *a, **kw):
            nonlocal execute_calls
            execute_calls += 1
            return await real_execute(self, *a, **kw)

        SlayerSQLClient.execute = counting_execute  # type: ignore[method-assign]
        try:
            resp = client.post("/query", json={"name": "qb_dryrun", "dry_run": True})
        finally:
            SlayerSQLClient.execute = real_execute  # type: ignore[method-assign]
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("sql") is not None
        assert "amount" in body["sql"].lower()
        assert execute_calls == 0, "dry_run=True must not execute SQL"
