"""DEV-1588: ``inspect`` is exposed on all four surfaces — MCP tool,
REST ``POST /inspect``, CLI ``slayer inspect``, and ``SlayerClient.inspect``
/ ``inspect_sync``. ``inspect_model`` is kept as a thin passthrough with a
``DEPRECATED`` note (behaviorally unchanged).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
from types import SimpleNamespace
from typing import AsyncIterator, Iterator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.storage.yaml_storage import YAMLStorage


async def _seed(storage: YAMLStorage) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        description="One row per placed order.",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(
                name="amount", sql="amount", type=DataType.DOUBLE,
                description="Order total in USD.",
            ),
        ],
        measures=[ModelMeasure(name="aov", formula="amount:sum / *:count")],
        aggregations=[Aggregation(name="big", formula="MAX({col})")],
    ))
    await storage.set_datasource_priority(["mydb"])


# ---------------------------------------------------------------------------
# MCP
# ---------------------------------------------------------------------------


async def _call_tool(server, *, name: str, arguments: dict) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


@pytest.fixture
async def mcp_storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed(st)
        yield st


class TestMcpSurface:
    async def test_inspect_tool_registered(self, mcp_storage: YAMLStorage) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        tools = await server.list_tools()
        names = {t.name for t in tools}
        assert "inspect" in names

    async def test_inspect_tool_column(self, mcp_storage: YAMLStorage) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        out = await _call_tool(
            server, name="inspect",
            arguments={
                "reference": "mydb.orders.amount",
                "entity_type": "column",
                "compact": False,
            },
        )
        assert "Column: mydb.orders.amount" in out

    async def test_inspect_model_passthrough_still_works(
        self, mcp_storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        out = await _call_tool(
            server, name="inspect_model",
            arguments={"model_name": "orders", "compact": False},
        )
        assert "# Model: `orders`" in out

    async def test_inspect_model_marked_deprecated(
        self, mcp_storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        tools = {t.name: t for t in await server.list_tools()}
        assert "inspect_model" in tools
        assert "DEPRECATED" in (tools["inspect_model"].description or "")


# ---------------------------------------------------------------------------
# REST
# ---------------------------------------------------------------------------


@pytest.fixture
def rest_client() -> Iterator[TestClient]:
    import asyncio

    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_seed(storage))
        finally:
            loop.close()
        yield TestClient(create_app(storage=storage))


class TestRestSurface:
    def test_inspect_column(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={
                "reference": "mydb.orders.amount",
                "entity_type": "column",
                "compact": False,
            },
        )
        assert r.status_code == 200
        assert "Column: mydb.orders.amount" in r.json()["result"]

    def test_inspect_json_format(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={
                "reference": "mydb.orders.amount",
                "entity_type": "column",
                "compact": False,
                "format": "json",
            },
        )
        assert r.status_code == 200
        payload = json.loads(r.json()["result"])
        assert payload["canonical_id"] == "mydb.orders.amount"

    def test_invalid_entity_type_is_400(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={"reference": "mydb.orders", "entity_type": "banana"},
        )
        assert r.status_code == 400

    def test_extra_field_rejected(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={
                "reference": "mydb.orders",
                "entity_type": "model",
                "bogus": 1,
            },
        )
        assert r.status_code == 422


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
async def cli_storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed(st)
        yield st


def _inspect_args(**overrides) -> SimpleNamespace:
    base = dict(
        reference="mydb.orders.amount",
        entity_type="column",
        compact=False,
        format="markdown",
        num_rows=3,
        show_sql=False,
        sections=None,
        descriptions_max_chars=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class TestCliSurface:
    async def test_run_inspect_prints_render(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(args=_inspect_args(), storage=cli_storage)
        assert "Column: mydb.orders.amount" in buf.getvalue()

    async def test_run_inspect_error_path(self, cli_storage: YAMLStorage) -> None:
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(reference="mydb.orders.nope"),
                storage=cli_storage,
            )
        assert "nope" in buf.getvalue()


# ---------------------------------------------------------------------------
# SlayerClient
# ---------------------------------------------------------------------------


@pytest.fixture
async def client_storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed(st)
        yield st


class TestSlayerClientSurface:
    async def test_inspect_async_local(self, client_storage: YAMLStorage) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(storage=client_storage)
        out = await client.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        assert "Column: mydb.orders.amount" in out

    def test_inspect_sync_local(self, client_storage: YAMLStorage) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(storage=client_storage)
        out = client.inspect_sync(
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        assert "Column: mydb.orders.amount" in out

    def test_inspect_sync_remote_posts_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None  # remote mode
        captured: dict = {}

        def _fake_request_sync(*, method, path, json=None, params=None):
            captured.update({"method": method, "path": path, "json": json})
            return {"result": "REMOTE RENDER"}

        monkeypatch.setattr(client, "_request_sync", _fake_request_sync)
        out = client.inspect_sync(
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        assert out == "REMOTE RENDER"
        assert captured["path"] == "/inspect"
        assert captured["json"]["reference"] == "mydb.orders.amount"
        assert captured["json"]["entity_type"] == "column"
