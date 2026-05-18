"""DEV-1428: REST memory endpoints accept string ids.

* ``POST /memories`` accepts an optional ``id`` field in the body.
* ``DELETE /memories/{memory_id}`` flips path-param type to ``str``.
* Charset violation → 400; missing memory → 404.
"""

from __future__ import annotations

import os
import tempfile
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def client() -> Iterator[TestClient]:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        import asyncio

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(
                storage.save_datasource(
                    DatasourceConfig(
                        name="mydb", type="sqlite", database=":memory:",
                    )
                )
            )
            loop.run_until_complete(
                storage.save_model(
                    SlayerModel(
                        name="orders",
                        sql_table="orders",
                        data_source="mydb",
                        columns=[
                            Column(name="id", sql="id", primary_key=True),
                            Column(name="amount", sql="amount"),
                        ],
                    )
                )
            )
        finally:
            loop.close()
        app = create_app(storage=storage)
        yield TestClient(app)


class TestRestStringIds:
    def test_post_with_id(self, client: TestClient) -> None:
        r = client.post(
            "/memories",
            json={
                "learning": "x",
                "linked_entities": ["mydb.orders.amount"],
                "id": "kb.test",
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["memory_id"] == "kb.test"

    def test_delete_string_path(self, client: TestClient) -> None:
        r = client.post(
            "/memories",
            json={
                "learning": "x",
                "linked_entities": ["mydb.orders.amount"],
                "id": "kb.del",
            },
        )
        assert r.status_code == 200
        r = client.delete("/memories/kb.del")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["deleted_id"] == "kb.del"

    def test_charset_violation_400(self, client: TestClient) -> None:
        r = client.post(
            "/memories",
            json={
                "learning": "x",
                "linked_entities": ["mydb.orders.amount"],
                "id": "bad:id",
            },
        )
        assert r.status_code == 400, r.text

    def test_missing_memory_404(self, client: TestClient) -> None:
        r = client.delete("/memories/does-not-exist")
        assert r.status_code == 404, r.text
