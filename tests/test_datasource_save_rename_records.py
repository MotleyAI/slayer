"""A datasource save keeps its granularity rename records (spec: models/stored-upgrade)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.errors import GranularityDefinitionError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from slayer.storage.upgrade import upgrade_store
from tests._granularity_rename_fixtures import (
    BACKENDS,
    DS,
    EXPECTED_BUCKETS,
    bucket_totals,
    datasource_doc,
    granularity_definition,
    granularity_store,
    live_db,
    raw_documents,
    reopen,
    write_raw_store,
)

RECORD = ("left_granularity", "left")


def _stored_granularities(*, backend: str, base: str) -> list[tuple[str, str | None]]:
    stored = yaml.safe_load(raw_documents(backend=backend, base=base)[f"datasources/{DS}"])  # JSON is YAML
    return [(g["name"], g.get("renamed_from")) for g in stored.get("granularities") or []]


def _config(base: str, granularities: list[dict]) -> DatasourceConfig:
    return DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "database": str(Path(base, "live.db")),
                                            "granularities": granularities})


async def _recorded_store(*, backend: str, base: str) -> StorageBackend:
    """Datasource ``DS`` stored by the upgrade: ``left_granularity`` recording ``left``."""
    store = await granularity_store(backend=backend, base=base, name="left")
    assert (await upgrade_store(store)).complete
    return reopen(backend=backend, base=base)


def _wrapped(storage: StorageBackend, view: str) -> StorageBackend:
    if view == "engine":
        return SlayerQueryEngine(storage=storage).storage
    return AccessFilteredStorage(storage, tags=[], bypass=view == "admin")


@pytest.mark.parametrize("view", ["storage", "engine", "filtered", "admin"])
@pytest.mark.parametrize("backend", BACKENDS)
async def test_saving_a_loaded_datasource_whose_rename_is_pending_keeps_it(
    tmp_path: Path, backend: str, view: str,
) -> None:
    base = str(tmp_path / backend)
    raw_store = await granularity_store(backend=backend, base=base, name="left")
    storage = raw_store if view == "storage" else _wrapped(raw_store, view)
    loaded = await storage.get_datasource(DS)
    assert loaded is not None
    await storage.save_datasource(loaded)
    assert _stored_granularities(backend=backend, base=base) == [RECORD]
    assert await bucket_totals(reopen(backend=backend, base=base)) == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_config_built_without_the_record_keeps_it(tmp_path: Path, backend: str) -> None:
    base = str(tmp_path / backend)
    storage = await _recorded_store(backend=backend, base=base)
    await storage.save_datasource(_config(base, [granularity_definition("left_granularity")]))
    assert _stored_granularities(backend=backend, base=base) == [RECORD]
    assert await bucket_totals(reopen(backend=backend, base=base)) == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_saving_after_the_upgrade_keeps_the_record(tmp_path: Path, backend: str) -> None:
    base = str(tmp_path / backend)
    storage = await _recorded_store(backend=backend, base=base)
    loaded = await storage.get_datasource(DS)
    assert loaded is not None
    await storage.save_datasource(loaded)
    assert _stored_granularities(backend=backend, base=base) == [RECORD]
    assert await bucket_totals(reopen(backend=backend, base=base)) == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_saving_the_reserved_spelling_is_refused(tmp_path: Path, backend: str) -> None:
    base = str(tmp_path / backend)
    storage = await granularity_store(backend=backend, base=base, name="left")
    before = raw_documents(backend=backend, base=base)
    config = _config(base, [granularity_definition("left")])
    with pytest.raises(GranularityDefinitionError, match="'left'"):
        await storage.save_datasource(config)
    assert raw_documents(backend=backend, base=base) == before


_INVALID_RECORDS: dict[str, tuple[list[dict], str]] = {
    "not-reserved": ([{**granularity_definition("fw"), "base": "week", "origin": "2000-01-03",
                       "renamed_from": "fiscal_week"}], "fw"),
    "duplicate": ([{**granularity_definition("left_granularity"), "renamed_from": "left"},
                   {**granularity_definition("left_g"), "renamed_from": "LEFT"}], "left_g"),
}


async def _plain_store(*, backend: str, base: str) -> StorageBackend:
    return await write_raw_store(backend=backend, base=base, datasources=[datasource_doc(db_path=live_db(base))])


@pytest.mark.parametrize("case", list(_INVALID_RECORDS))
@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_invalid_record_is_refused(tmp_path: Path, backend: str, case: str) -> None:
    base = str(tmp_path / backend)
    storage = await _plain_store(backend=backend, base=base)
    granularities, named = _INVALID_RECORDS[case]
    before = raw_documents(backend=backend, base=base)
    config = _config(base, granularities)
    with pytest.raises(GranularityDefinitionError, match=f"'{named}'"):
        await storage.save_datasource(config)
    assert raw_documents(backend=backend, base=base) == before


async def _call(server: Any, *, name: str, arguments: dict[str, Any]) -> str:
    return (await server.call_tool(name=name, arguments=arguments)).content[0].text


class TestSurfaces:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_mcp_edit_of_the_description_keeps_the_record(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        server = create_mcp_server(storage=await granularity_store(backend=backend, base=base, name="left"))
        text = await _call(server, name="edit_datasource", arguments={"name": DS, "description": "events"})
        assert "updated" in text, text
        assert _stored_granularities(backend=backend, base=base) == [RECORD]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_mcp_edit_of_the_granularities_keeps_the_record(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        server = create_mcp_server(storage=await _recorded_store(backend=backend, base=base))
        text = await _call(server, name="edit_datasource", arguments={
            "name": DS, "granularities": [granularity_definition("left_granularity")]})
        assert "updated" in text, text
        assert _stored_granularities(backend=backend, base=base) == [RECORD]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_mcp_edit_with_an_invalid_record_is_refused(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        server = create_mcp_server(storage=await _plain_store(backend=backend, base=base))
        before = raw_documents(backend=backend, base=base)
        granularities, named = _INVALID_RECORDS["not-reserved"]
        text = await _call(server, name="edit_datasource", arguments={"name": DS, "granularities": granularities})
        assert named in text, text
        assert "updated" not in text, text
        assert raw_documents(backend=backend, base=base) == before

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_rest_save_of_a_loaded_datasource_keeps_the_record(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        storage = await granularity_store(backend=backend, base=base, name="left")
        loaded = await storage.get_datasource(DS)
        assert loaded is not None
        client = TestClient(create_app(storage=storage))
        body = {**loaded.model_dump(mode="json", exclude_none=True),
                "granularities": [granularity_definition("left_granularity")]}
        created = client.post("/datasources", json=body)
        assert created.status_code in (200, 201), created.text
        assert _stored_granularities(backend=backend, base=base) == [RECORD]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_rest_save_with_an_invalid_record_is_refused(self, tmp_path: Path, backend: str) -> None:
        base = str(tmp_path / backend)
        storage = await _plain_store(backend=backend, base=base)
        client = TestClient(create_app(storage=storage))
        before = raw_documents(backend=backend, base=base)
        granularities, named = _INVALID_RECORDS["duplicate"]
        body = {"name": DS, "type": "sqlite", "database": str(Path(base, "live.db")), "granularities": granularities}
        refused = client.post("/datasources", json=body)
        assert refused.status_code in (400, 422), refused.text
        assert named in refused.text
        assert raw_documents(backend=backend, base=base) == before
