"""Surface smoke tests for recommend_root_model (DEV-1626): MCP tool, REST
endpoint, CLI handler, SlayerClient (local-engine), plus a regression test
that the ``_expand_join_graph`` refactor preserves directed reachability.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import AsyncIterator

import pytest_asyncio
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


async def _seed(s: YAMLStorage) -> None:
    await s.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))

    def col(n, t=DataType.TEXT, pk=False):
        return Column(name=n, sql=n, type=t, primary_key=pk)

    await s.save_model(SlayerModel(
        name="orders", data_source="mydb", sql_table="orders",
        columns=[col("id", DataType.INT, pk=True), col("status")],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]], join_type=JoinType.LEFT),
            ModelJoin(target_model="products", join_pairs=[["product_id", "id"]], join_type=JoinType.LEFT),
        ],
    ))
    await s.save_model(SlayerModel(
        name="customers", data_source="mydb", sql_table="customers",
        columns=[col("id", DataType.INT, pk=True), col("name")],
    ))
    await s.save_model(SlayerModel(
        name="products", data_source="mydb", sql_table="products",
        columns=[col("id", DataType.INT, pk=True), col("category")],
    ))
    # Disconnected model for the no-root rendering path.
    await s.save_model(SlayerModel(
        name="tickets", data_source="mydb", sql_table="tickets",
        columns=[col("id", DataType.INT, pk=True), col("subject")],
    ))


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)
        await _seed(s)
        yield s


class TestMcpTool:
    async def test_markdown_names_root(self, storage) -> None:
        mcp = create_mcp_server(storage=storage)
        blocks, _ = await mcp.call_tool(
            name="recommend_root_model",
            arguments={"items": ["customers.name", "products.category"]},
        )
        text = blocks[0].text
        assert "orders" in text
        assert "customers.name" in text

    async def test_json_format(self, storage) -> None:
        mcp = create_mcp_server(storage=storage)
        blocks, _ = await mcp.call_tool(
            name="recommend_root_model",
            arguments={"items": ["orders.status"], "data_source": "mydb", "format": "json"},
        )
        payload = json.loads(blocks[0].text)
        assert payload["root_model"] == "orders"
        assert payload["reachable"] is True

    async def test_no_root_renders_coverage(self, storage) -> None:
        mcp = create_mcp_server(storage=storage)
        blocks, _ = await mcp.call_tool(
            name="recommend_root_model",
            arguments={"items": ["customers.name", "tickets.subject"], "format": "json"},
        )
        payload = json.loads(blocks[0].text)
        assert payload["root_model"] is None
        assert payload["reachable"] is False
        assert payload["coverage"]


class TestRestEndpoint:
    def test_post_ok(self, storage) -> None:
        client = TestClient(create_app(storage=storage))
        resp = client.post(
            "/recommend-root-model",
            json={"items": ["customers.name", "products.category"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["root_model"] == "orders"
        assert body["data_source"] == "mydb"

    def test_returns_item_paths(self, storage) -> None:
        client = TestClient(create_app(storage=storage))
        resp = client.post(
            "/recommend-root-model",
            json={"items": ["customers.name", "products.category"]},
        )
        paths = {ip["input_item"]: ip["path"] for ip in resp.json()["item_paths"]}
        assert paths == {
            "customers.name": "customers.name",
            "products.category": "products.category",
        }

    def test_post_unresolvable_item_400(self, storage) -> None:
        client = TestClient(create_app(storage=storage))
        resp = client.post(
            "/recommend-root-model",
            json={"items": ["orders.no_such_column"], "data_source": "mydb"},
        )
        assert resp.status_code == 400

    def test_post_wrong_kind_400(self, storage) -> None:
        # Bare model reference (no leaf) → ValueError → HTTP 400.
        client = TestClient(create_app(storage=storage))
        resp = client.post(
            "/recommend-root-model",
            json={"items": ["customers"], "data_source": "mydb"},
        )
        assert resp.status_code == 400


class TestCliHandler:
    def test_cli_text(self, storage, capsys) -> None:
        from slayer.cli import _run_recommend_root_model

        args = argparse.Namespace(
            storage=storage.base_dir, models_dir=None,
            items=["customers.name", "products.category"],
            data_source=None, format="text",
        )
        _run_recommend_root_model(args)
        out = capsys.readouterr().out
        assert "orders" in out

    def test_cli_json(self, storage, capsys) -> None:
        from slayer.cli import _run_recommend_root_model

        args = argparse.Namespace(
            storage=storage.base_dir, models_dir=None,
            items=["orders.status"], data_source="mydb", format="json",
        )
        _run_recommend_root_model(args)
        payload = json.loads(capsys.readouterr().out)
        assert payload["root_model"] == "orders"


class TestCliParserWiring:
    def test_main_registers_subcommand(self, storage, monkeypatch, capsys) -> None:
        # Exercises the actual `slayer recommend-root-model` argparse wiring.
        import sys

        from slayer.cli import main

        monkeypatch.setattr(sys, "argv", [
            "slayer", "recommend-root-model",
            "customers.name", "products.category",
            "--storage", storage.base_dir, "--format", "json",
        ])
        main()
        payload = json.loads(capsys.readouterr().out)
        assert payload["root_model"] == "orders"


class TestSlayerClient:
    def test_local_engine_sync(self, storage) -> None:
        from slayer.client.slayer_client import SlayerClient
        from slayer.core.recommend import RootModelRecommendation

        client = SlayerClient(storage=storage)
        rec = client.recommend_root_model_sync(["customers.name", "products.category"])
        assert isinstance(rec, RootModelRecommendation)
        assert rec.root_model == "orders"

    async def test_local_engine_async(self, storage) -> None:
        from slayer.client.slayer_client import SlayerClient
        from slayer.core.recommend import RootModelRecommendation

        client = SlayerClient(storage=storage)
        rec = await client.recommend_root_model(["customers.name", "products.category"])
        assert isinstance(rec, RootModelRecommendation)
        assert rec.root_model == "orders"


class TestExpandJoinGraphRegression:
    async def test_directed_reachability_preserved(self, storage) -> None:
        # The refactor routes _expand_join_graph through JoinGraph.reachable_from;
        # directed transitive closure must be unchanged.
        engine = SlayerQueryEngine(storage=storage)
        try:
            touched = {"orders"}
            await engine._expand_join_graph(touched=touched, data_source="mydb")
            assert touched == {"orders", "customers", "products"}
        finally:
            await engine.aclose()
