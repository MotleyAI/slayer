"""REST endpoint tests for the unified Memory surface (DEV-1357 v2).

Two endpoints land on the FastAPI server:

* ``POST /memories``         body ``{learning, linked_entities}``
* ``DELETE /memories/{id}``

Memory retrieval is part of ``POST /search`` (covered in
``test_search_surfaces.py``). ``linked_entities`` accepts either a list
of entity strings or an inline ``SlayerQuery`` payload (dict). Errors
map: ``EntityResolutionError`` / ``AmbiguousModelError`` /
``ValueError`` → ``400``; ``MemoryNotFoundError`` → ``404``.
"""

import os
import shutil
import sqlite3
import tempfile
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(scope="session")
def _shared_storage() -> Generator[YAMLStorage, None, None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture(scope="session")
def _shared_client(_shared_storage: YAMLStorage) -> TestClient:
    app = create_app(storage=_shared_storage)
    return TestClient(app)


def _reset_yaml_storage(storage: YAMLStorage) -> None:
    for sub in ("models", "datasources"):
        d = os.path.join(storage.base_dir, sub)
        if os.path.isdir(d):
            for entry in os.listdir(d):
                path = os.path.join(d, entry)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
    for f in (
        "priority.yaml",
        "memories.yaml",
        "embeddings.yaml.legacy",
        "counters.yaml.legacy",
    ):
        p = os.path.join(storage.base_dir, f)
        if os.path.exists(p):
            os.remove(p)
    # See _reset_storage in test_memories_mcp.py — clear rows in place
    # rather than deleting the SidecarEmbeddingStore's open db file.
    emb_path = os.path.join(storage.base_dir, "embeddings.db")
    if os.path.exists(emb_path):
        with sqlite3.connect(emb_path) as conn:
            conn.execute("DELETE FROM embeddings")


@pytest.fixture
def storage(_shared_storage: YAMLStorage) -> YAMLStorage:
    _reset_yaml_storage(_shared_storage)
    return _shared_storage


@pytest.fixture
def client(_shared_client: TestClient, storage: YAMLStorage) -> TestClient:
    return _shared_client


@pytest.fixture
async def seeded(storage: YAMLStorage) -> YAMLStorage:
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="x")
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            data_source="mydb",
            sql_table="orders",
            columns=[
                Column(
                    name="id",
                    sql="id",
                    type=DataType.DOUBLE,
                    primary_key=True,
                ),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="invoices",
            data_source="mydb",
            sql_table="invoices",
            columns=[
                Column(
                    name="id",
                    sql="id",
                    type=DataType.DOUBLE,
                    primary_key=True,
                ),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
    )
    await storage.set_datasource_priority(["mydb"])
    return storage


class TestPostMemories:
    async def test_save_with_entity_list(
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        resp = client.post(
            "/memories",
            json={
                "learning": "orders.amount is in cents",
                "linked_entities": ["mydb.orders.amount"],
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["memory_id"] == 1
        assert body["resolved_entities"] == ["mydb.orders.amount"]
        loaded = await seeded.get_memory(1)
        assert loaded.learning == "orders.amount is in cents"
        assert loaded.query is None

    async def test_save_with_query(
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        resp = client.post(
            "/memories",
            json={
                "learning": "Revenue by status",
                "linked_entities": {
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                },
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["memory_id"] == 1
        assert "mydb.orders" in body["resolved_entities"]
        assert "mydb.orders.amount" in body["resolved_entities"]
        loaded = await seeded.get_memory(1)
        assert loaded.query is not None

    async def test_resolution_error_returns_400(  # NOSONAR(S7503) — keeps a uniform async signature across the class so the seeded async fixture binds the same way for every test
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        resp = client.post(
            "/memories",
            json={
                "learning": "x",
                "linked_entities": ["amount"],  # ambiguous
            },
        )
        assert resp.status_code == 400, resp.text
        # Body carries a useful message about the offending segment.
        detail = (resp.json().get("detail") or "").lower()
        assert "amount" in detail or "ambiguous" in detail

    async def test_empty_entity_list_returns_400(  # NOSONAR(S7503) — keeps a uniform async signature across the class so the seeded async fixture binds the same way for every test
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        resp = client.post(
            "/memories",
            json={"learning": "x", "linked_entities": []},
        )
        assert resp.status_code == 400


class TestDeleteMemory:
    async def test_delete_existing(
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        memory = await seeded.save_memory(
            learning="x", entities=["mydb.orders"]
        )
        resp = client.delete(f"/memories/{memory.id}")
        assert resp.status_code == 200, resp.text
        assert resp.json()["deleted_id"] == memory.id
        assert await seeded.list_memories() == []

    async def test_delete_missing_returns_404(  # NOSONAR(S7503) — keeps a uniform async signature across the class so the seeded async fixture binds the same way for every test
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        resp = client.delete("/memories/999")
        assert resp.status_code == 404


class TestRecallEndpointRemoved:
    async def test_recall_endpoint_returns_404(  # NOSONAR(S7503) — keeps async fixture binding consistent
        self, client: TestClient, seeded: YAMLStorage
    ) -> None:
        """``POST /memories/recall`` is gone; retrieval is now ``POST /search``."""
        resp = client.post(
            "/memories/recall",
            json={"about": ["mydb.orders.amount"]},
        )
        # FastAPI returns 405 because /memories/{id} captures the path
        # with the wrong method, or 404 if no route matches.
        assert resp.status_code in (404, 405)
