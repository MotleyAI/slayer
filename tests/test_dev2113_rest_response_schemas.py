"""Client-called REST routes publish named response schemas; their JSON bodies stay as they were."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage

CLIENT_ROUTES = [
    ("get", "/health"),
    ("get", "/models"),
    ("post", "/models"),
    ("get", "/models/{name}"),
    ("delete", "/models/{name}"),
    ("get", "/datasources"),
    ("post", "/datasources"),
    ("get", "/datasources/priority"),
    ("put", "/datasources/priority"),
    ("post", "/memories"),
    ("delete", "/memories/{memory_id}"),
    ("post", "/search"),
    ("post", "/inspect"),
    ("post", "/recommend-root-model"),
]


@pytest.fixture
def api(tmp_path: Path) -> Iterator[TestClient]:
    db = tmp_path / "shop.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE orders_t (id REAL, status TEXT, amount REAL)")
        conn.execute("INSERT INTO orders_t VALUES (1, 'paid', 10), (2, 'open', 5)")
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    run_sync(storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=str(db))))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders_t", data_source="ds", description="Orders",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="secret", sql="secret", type=DataType.TEXT, hidden=True),
        ],
    )))
    with TestClient(create_app(storage=storage)) as client:
        yield client


def _shape(value: Any) -> Any:
    """Key structure and scalar types; list items collapse to their distinct shapes."""
    if isinstance(value, dict):
        return {k: _shape(v) for k, v in value.items()}
    if isinstance(value, list):
        shapes = {repr(_shape(v)): _shape(v) for v in value}
        return [shapes[k] for k in sorted(shapes)]
    return type(value).__name__


def _ok(response: Any) -> Any:
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.parametrize(("method", "path"), CLIENT_ROUTES)
def test_route_declares_named_response_schema(api: TestClient, method: str, path: str) -> None:
    schema = api.app.openapi()["paths"][path][method]["responses"]["200"]["content"]["application/json"]["schema"]
    named = schema.get("$ref") or schema.get("items", {}).get("$ref")
    assert named, f"{method.upper()} {path} has an anonymous response schema: {schema}"


class TestBodiesUnchanged:
    def test_health(self, api: TestClient) -> None:
        assert _ok(api.get("/health")) == {"status": "ok"}

    def test_list_models(self, api: TestClient) -> None:
        body = _ok(api.get("/models"))
        assert body[0] == {"name": "orders", "data_source": "ds", "description": "Orders"}
        assert _shape(body) == [{"name": "str", "data_source": "str", "description": "str"}]

    def test_get_model(self, api: TestClient) -> None:
        body = _ok(api.get("/models/orders"))
        assert [c["name"] for c in body["columns"]] == ["id", "status", "amount"]
        assert _shape(body) == {
            "version": "int", "name": "str", "sql_table": "str", "query_variables": {},
            "data_source": "str",
            "columns": [{"name": "str", "sql": "str", "type": "str", "primary_key": "bool", "unique": "bool", "hidden": "bool"}],
            "measures": [], "aggregations": [], "joins": [], "filters": [],
            "description": "str", "hidden": "bool", "access_tags": [],
        }

    def test_create_and_delete_model(self, api: TestClient) -> None:
        model = {"name": "m2", "sql_table": "orders_t", "data_source": "ds", "columns": [{"name": "id", "sql": "id", "type": "DOUBLE"}]}
        assert _ok(api.post("/models", json=model)) == {"status": "created", "name": "m2"}
        assert _ok(api.delete("/models/m2")) == {"status": "deleted", "name": "m2"}

    def test_datasources(self, api: TestClient) -> None:
        assert _ok(api.get("/datasources")) == [{"name": "ds", "type": "sqlite"}]
        created = _ok(api.post("/datasources", json={"name": "ds2", "type": "sqlite", "database": ":memory:"}))
        assert created == {"status": "created", "name": "ds2"}

    def test_datasource_priority(self, api: TestClient) -> None:
        assert _ok(api.get("/datasources/priority")) == {"priority": []}
        assert _ok(api.put("/datasources/priority", json={"priority": ["ds"]})) == {"status": "ok", "priority": ["ds"]}

    def test_memories(self, api: TestClient) -> None:
        saved = _ok(api.post("/memories", json={"learning": "paid means settled", "linked_entities": ["orders.status"], "id": "mem1"}))
        assert saved == {"memory_id": "mem1", "resolved_entities": ["ds.orders.status"], "warnings": []}
        assert _ok(api.delete("/memories/mem1")) == {"deleted_id": "mem1"}

    def test_search(self, api: TestClient) -> None:
        api.post("/memories", json={"learning": "paid means settled", "linked_entities": ["orders.status"], "id": "mem1"})
        body = _ok(api.post("/search", json={"question": "paid", "max_results": 3, "compact": True}))
        hit_shapes = _shape(body["results"])
        assert {"kind": "str", "id": "str", "score": "float", "text": "str", "description": "str",
                "matched_entities": [], "query": "NoneType"} in hit_shapes
        assert {k: v for k, v in body.items() if k != "results"} == {"resolved_input_entities": [], "warnings": []}

    def test_inspect(self, api: TestClient) -> None:
        body = _ok(api.post("/inspect", json={"reference": "orders", "entity_type": "model", "num_rows": 1}))
        assert list(body) == ["result"]
        assert body["result"].startswith("# `orders`")

    def test_recommend_root_model(self, api: TestClient) -> None:
        body = _ok(api.post("/recommend-root-model", json={"items": ["orders.amount", "orders.status"]}))
        assert body == {
            "data_source": "ds", "root_model": "orders", "reachable": True,
            "item_paths": [
                {"input_item": "orders.amount", "path": "amount", "attachment": False, "unrelated": False},
                {"input_item": "orders.status", "path": "status", "attachment": False, "unrelated": False},
            ],
            "coverage": [], "message": "All items are reachable from 'orders'.", "warnings": [],
        }
