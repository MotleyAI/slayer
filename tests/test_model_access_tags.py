"""``SlayerModel.access_tags``: default, round-trips through every surface, and the v15 upgrade."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import AsyncGenerator, Generator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.inspect.service import InspectService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.yaml_storage import YAMLStorage
from tests._model_access_fixtures import make_storage, mcp_text, must_get
from tests._stored_upgrade_fixtures import DS as SHOP, customers_v10, orders_v10, raw_doc, raw_store

DS = "tagds"
SECRET_TAG = "zz-secret-tag"


def _model(name: str = "items", **extra) -> SlayerModel:
    return SlayerModel(name=name, sql_table="items", data_source=DS,
                       columns=[Column(name="id", type=DataType.INT, primary_key=True)], **extra)


@pytest.fixture(params=["yaml", "sqlite"])
async def storage(request) -> AsyncGenerator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmp:
        s = make_storage(backend=request.param, base=tmp)
        await s.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=os.path.join(tmp, "x.db")))
        yield s


class TestField:
    def test_defaults_to_untagged(self) -> None:
        assert _model().access_tags == []

    def test_schema_version_is_15(self) -> None:
        assert CURRENT_VERSIONS["SlayerModel"] == 15
        assert _model().version == 15

    def test_json_round_trip(self) -> None:
        model = _model(access_tags=["hr", "HR Team"], hidden=True)
        again = SlayerModel.model_validate(json.loads(model.model_dump_json()))
        assert again.access_tags == ["hr", "HR Team"]
        assert again.hidden is True


class TestStorage:
    @pytest.mark.parametrize("tags", [["hr"], ["HR Team"], ["hr", "fin"]])
    async def test_tags_round_trip(self, storage: StorageBackend, tags: list[str]) -> None:
        await storage.save_model(_model(access_tags=tags))
        assert (await must_get(storage, "items", DS)).access_tags == tags


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
async def test_v14_model_loads_untagged_and_writes_back_at_v15(backend: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = await raw_store(
            backend=backend, base=tmp,
            datasource=DatasourceConfig(name=SHOP, type="sqlite", database=os.path.join(tmp, "x.db")),
            models=[orders_v10(version=14), customers_v10(version=14)],
        )
        assert (await must_get(storage, "orders", SHOP)).access_tags == []
        assert (await raw_doc(storage, "orders"))["version"] == 15


class TestMcp:
    async def call(self, storage: StorageBackend, tool: str, **arguments) -> str:
        return await mcp_text(storage, tool, **arguments)

    async def test_create_model_sets_tags(self, storage: StorageBackend) -> None:
        out = await self.call(storage, "create_model", name="items", sql_table="items", data_source=DS,
                              columns=[{"name": "id", "type": "number", "primary_key": True}], access_tags=["hr"])
        assert "Error" not in out, out
        assert (await must_get(storage, "items", DS)).access_tags == ["hr"]

    async def test_create_query_backed_model_sets_tags(self, storage: StorageBackend) -> None:
        await storage.save_model(_model())
        out = await self.call(storage, "create_model", name="item_count", access_tags=["hr"],
                              query={"source_model": "items", "measures": [{"formula": "count(*)", "name": "n"}]})
        assert "Error" not in out, out
        assert (await must_get(storage, "item_count", DS)).access_tags == ["hr"]

    async def test_edit_model_changes_tags(self, storage: StorageBackend) -> None:
        await storage.save_model(_model(access_tags=["hr"]))
        out = await self.call(storage, "edit_model", model_name="items", access_tags=["hr", "HR Team"])
        assert "Error" not in out, out
        assert (await must_get(storage, "items", DS)).access_tags == ["hr", "HR Team"]

    async def test_edit_model_without_tags_keeps_them(self, storage: StorageBackend) -> None:
        await storage.save_model(_model(access_tags=["hr"]))
        await self.call(storage, "edit_model", model_name="items", description="d")
        assert (await must_get(storage, "items", DS)).access_tags == ["hr"]


class TestInspectAndSearch:
    async def test_inspect_shows_tags(self, storage: StorageBackend) -> None:
        await storage.save_model(_model(access_tags=[SECRET_TAG]))
        for fmt in ("markdown", "json"):
            out = await InspectService(storage=storage).inspect(
                reference=f"{DS}.items", entity_type="model", compact=False, format=fmt,
            )
            assert SECRET_TAG in out

    async def test_tags_are_not_search_text(self, storage: StorageBackend) -> None:
        await storage.save_model(_model(access_tags=[SECRET_TAG]))
        response = await SearchService(storage=storage).search(question="items", compact=False, max_results=20)
        assert response.results
        assert all(SECRET_TAG not in hit.text for hit in response.results)


class TestRestAndClient:
    @pytest.fixture
    def rest(self) -> Generator[tuple[TestClient, YAMLStorage]]:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            run_sync(storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=os.path.join(tmp, "x.db"))))
            yield TestClient(create_app(storage=storage)), storage

    def test_rest_create_and_get(self, rest: tuple[TestClient, YAMLStorage]) -> None:
        client, storage = rest
        resp = client.post("/models", json=_model(access_tags=["hr"]).model_dump(mode="json"))
        assert resp.status_code == 200, resp.text
        assert run_sync(must_get(storage, "items", DS)).access_tags == ["hr"]
        got = client.get("/models/items", params={"data_source": DS})
        assert got.status_code == 200
        assert got.json()["access_tags"] == ["hr"]

    def test_rest_update_changes_tags(self, rest: tuple[TestClient, YAMLStorage]) -> None:
        client, storage = rest
        run_sync(storage.save_model(_model(access_tags=["hr"])))
        resp = client.put("/models/items", json=_model(access_tags=["fin"]).model_dump(mode="json"))
        assert resp.status_code == 200, resp.text
        assert run_sync(must_get(storage, "items", DS)).access_tags == ["fin"]

    async def test_local_client_reads_tags(self, storage: StorageBackend) -> None:
        await storage.save_model(_model(access_tags=["hr"]))
        model = await SlayerClient(storage=storage).get_model("items", data_source=DS)
        assert model is not None
        assert model.access_tags == ["hr"]
