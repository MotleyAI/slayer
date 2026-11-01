"""A ``client_factory`` can replace direct-DB SQL clients (gateway/proxy mode).

Phase 0 of the SLayer migration: SEMANTIX routes every SLayer statement through
sqlboard instead of opening its own connections. This test pins the injection
seam that makes that possible without a hard fork.
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.storage.sqlite_storage import SQLiteStorage


@pytest.fixture
def datasource() -> DatasourceConfig:
    return DatasourceConfig(
        name="gateway",
        type="postgres",
        connection_string="postgresql+psycopg2://never:never@127.0.0.1:1/never-dialed",
    )


class RecordingClient(SlayerSQLClient):
    """A stand-in client that records construction and never touches a DB."""

    instances: list["RecordingClient"] = []

    def __init__(self, datasource: DatasourceConfig) -> None:
        super().__init__(datasource=datasource)
        self.instances.append(self)


@pytest.fixture(autouse=True)
def _reset_instances():
    RecordingClient.instances.clear()


@pytest.mark.asyncio
async def test_client_factory_used_for_query_path(tmp_path, datasource):
    storage = SQLiteStorage(str(tmp_path / "slayer.db"))
    await storage.save_datasource(datasource)
    engine = SlayerQueryEngine(storage=storage, client_factory=RecordingClient)

    client = engine._client_for(datasource)
    assert isinstance(client, RecordingClient)
    # Cached: same instance on repeat access.
    assert engine._client_for(datasource) is client
    assert len(RecordingClient.instances) == 1


@pytest.mark.asyncio
async def test_default_factory_is_slayer_sql_client(tmp_path, datasource):
    storage = SQLiteStorage(str(tmp_path / "slayer.db"))
    engine = SlayerQueryEngine(storage=storage)

    client = engine._client_for(datasource)
    assert type(client) is SlayerSQLClient


@pytest.mark.asyncio
async def test_get_column_types_uses_client_for(tmp_path, datasource):
    """``get_column_types`` must go through ``_client_for`` (no inline construction)."""
    storage = SQLiteStorage(str(tmp_path / "slayer.db"))
    await storage.save_datasource(datasource)
    engine = SlayerQueryEngine(storage=storage)

    class FakeClient(SlayerSQLClient):
        def __init__(self, datasource):
            super().__init__(datasource=datasource)
            self.called = False

        async def get_column_types(self, sql: str) -> dict[str, str]:
            self.called = True
            return {}

    fake = FakeClient(datasource)
    engine._client_for = lambda ds: fake  # type: ignore[assignment]

    # No model needed: _client_for is never reached because the model is missing,
    # so instead exercise the path with a model we know exists.
    # Build a minimal table-backed model so the probe path runs far enough to
    # invoke the client.
    from slayer.core.enums import DataType
    from slayer.core.models import Column, ModelMeasure, SlayerModel

    model = SlayerModel(
        name="m",
        sql_table="public.t",
        data_source=datasource.name,
        columns=[Column(name="x", sql="x", type=DataType.INT)],
        measures=[ModelMeasure(name="s", formula="sum(x)")],
    )
    await storage.save_model(model)

    await engine.get_column_types("m")
    assert fake.called, "get_column_types bypassed _client_for"
