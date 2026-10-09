"""Stored query-backed models drop their cached backing SQL at v15; a dry run renders it instead."""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncGenerator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.inspect.service import InspectService
from slayer.storage.base import StorageBackend
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    orders_v10,
    query_backed_v10,
    raw_doc,
    raw_store,
    rev_query,
    run,
    seed_shop,
)

CACHED_SQL = "SELECT 1 AS stale_marker"


@pytest.fixture(params=BACKENDS)
async def storage(request) -> AsyncGenerator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "shop.db")
        seed_shop(db, dialect="sqlite")
        stored_rev = {
            **query_backed_v10(name="rev", stages=[rev_query()], version=14),
            "backing_query_sql": CACHED_SQL, "columns": [{"name": "rev", "type": "DOUBLE"}],
        }
        yield await raw_store(
            backend=request.param, base=os.path.join(tmp, "store"),
            datasource=DatasourceConfig(name=DS, type="sqlite", database=db),
            models=[orders_v10(version=14), customers_v10(version=14), stored_rev],
        )


def test_field_is_gone() -> None:
    assert "backing_query_sql" not in SlayerModel.model_fields


async def test_v14_model_loads_without_backing_sql(storage: StorageBackend) -> None:
    model = await storage.get_model("rev", data_source=DS)
    assert model is not None
    assert "backing_query_sql" not in model.model_dump()
    doc = await raw_doc(storage, "rev")
    assert doc["version"] == 15
    assert "backing_query_sql" not in doc


@pytest.mark.parametrize("fmt", ["markdown", "json"])
async def test_inspect_shows_no_backing_sql(storage: StorageBackend, fmt: str) -> None:
    out = await InspectService(storage=storage).inspect(
        reference=f"{DS}.rev", entity_type="model", compact=False, show_sql=True, format=fmt,
    )
    assert "Backing Query SQL" not in out
    assert "backing_query_sql" not in out
    assert "stale_marker" not in out


def test_rest_returns_no_backing_sql() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "shop.db")
        seed_shop(db, dialect="sqlite")
        stored_rev = {
            **query_backed_v10(name="rev", stages=[rev_query()], version=14),
            "backing_query_sql": CACHED_SQL, "columns": [{"name": "rev", "type": "DOUBLE"}],
        }
        storage = run_sync(raw_store(
            backend="yaml", base=os.path.join(tmp, "store"),
            datasource=DatasourceConfig(name=DS, type="sqlite", database=db),
            models=[orders_v10(version=14), customers_v10(version=14), stored_rev],
        ))
        response = TestClient(create_app(storage=storage)).get("/models/rev", params={"data_source": DS})
        assert response.status_code == 200, response.text
        assert "backing_query_sql" not in response.json()
        assert "stale_marker" not in response.text


async def test_dry_run_renders_the_sql(storage: StorageBackend) -> None:
    response = await run(storage, {"source_model": "rev", "measures": [{"formula": "rev:sum"}]}, dry_run=True)
    assert response.sql is not None
    assert "orders" in response.sql
    assert "stale_marker" not in response.sql
