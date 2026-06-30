"""DEV-1588: ``inspect`` is exposed on all four surfaces — MCP tool,
REST ``POST /inspect``, CLI ``slayer inspect``, and ``SlayerClient.inspect``
/ ``inspect_sync``. ``inspect_model`` is kept as a thin passthrough with a
``DEPRECATED`` note (behaviorally unchanged).
"""

from __future__ import annotations

import asyncio
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
            Column(
                name="customer_id", sql="customer_id", type=DataType.INT,
                description="FK to customers.",
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


def _seed_sync(storage: YAMLStorage) -> None:
    """Run the async ``_seed`` on a throwaway loop, for sync fixtures."""
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_seed(storage))
    finally:
        loop.close()


@pytest.fixture
def rest_client() -> Iterator[TestClient]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        _seed_sync(storage)
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
def cli_storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        _seed_sync(st)
        yield st


def _inspect_args(**overrides) -> SimpleNamespace:
    base = {
        "reference": "mydb.orders.amount",
        "entity_type": "column",
        "compact": False,
        "format": "markdown",
        "num_rows": 3,
        "show_sql": False,
        "sections": None,
        "descriptions_max_chars": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class TestCliSurface:
    def test_run_inspect_prints_render(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(args=_inspect_args(), storage=cli_storage)
        assert "Column: mydb.orders.amount" in buf.getvalue()

    def test_run_inspect_not_found_prints_string(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        # A not-found lookup is a descriptive string result (exit 0), not a
        # raised error — consistent with the inspect_model precedent.
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(reference="mydb.orders.nope"),
                storage=cli_storage,
            )
        assert "nope" in buf.getvalue()

    def test_run_inspect_invalid_arg_exits_nonzero(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        # Invalid args raise ValueError → _exit_with_error → sys.exit(1).
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), pytest.raises(SystemExit) as exc:
            _run_inspect(
                args=_inspect_args(entity_type="banana"),
                storage=cli_storage,
            )
        assert exc.value.code == 1
        assert "entity_type" in buf.getvalue()


# ---------------------------------------------------------------------------
# SlayerClient
# ---------------------------------------------------------------------------


@pytest.fixture
def client_storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        _seed_sync(st)
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


# ===========================================================================
# DEV-1612: reference accepts str | list[str] (batched single-kind lookup)
# ===========================================================================

_BLOCK_SEP = "\n\n---\n\n"


class TestMcpBatch:
    async def test_inspect_tool_list_markdown(
        self, mcp_storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        out = await _call_tool(
            server, name="inspect",
            arguments={
                "reference": ["mydb.orders.amount", "mydb.orders.customer_id"],
                "entity_type": "column",
                "compact": False,
            },
        )
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.customer_id" in out
        assert out.index("amount") < out.index("customer_id")

    async def test_inspect_tool_list_json(
        self, mcp_storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        out = await _call_tool(
            server, name="inspect",
            arguments={
                "reference": ["mydb.orders.amount", "mydb.orders.customer_id"],
                "entity_type": "column",
                "compact": False,
                "format": "json",
            },
        )
        arr = json.loads(out)
        assert isinstance(arr, list)
        assert [e["canonical_id"] for e in arr] == [
            "mydb.orders.amount", "mydb.orders.customer_id",
        ]

    async def test_inspect_tool_schema_accepts_list(
        self, mcp_storage: YAMLStorage
    ) -> None:
        # The widened tool advertises ``reference`` as accepting a list too,
        # and the docstring mentions the batch/list behavior.
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        tool = {t.name: t for t in await server.list_tools()}["inspect"]
        ref_schema = json.dumps(tool.inputSchema["properties"]["reference"])
        assert "array" in ref_schema
        assert "list" in (tool.description or "").lower()

    async def test_inspect_tool_list_one_bad_id(
        self, mcp_storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=mcp_storage)
        out = await _call_tool(
            server, name="inspect",
            arguments={
                "reference": ["mydb.orders.amount", "mydb.orders.nope"],
                "entity_type": "column",
                "compact": False,
                "format": "json",
            },
        )
        arr = json.loads(out)
        assert arr[0]["canonical_id"] == "mydb.orders.amount"
        assert arr[1]["reference"] == "mydb.orders.nope"
        assert "error" in arr[1]


class TestRestBatch:
    def test_list_markdown_result(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={
                "reference": ["mydb.orders.amount", "mydb.orders.customer_id"],
                "entity_type": "column",
                "compact": False,
            },
        )
        assert r.status_code == 200
        result = r.json()["result"]
        assert "## mydb.orders.amount" in result
        assert "## mydb.orders.customer_id" in result

    def test_list_json_result_is_array_string(
        self, rest_client: TestClient
    ) -> None:
        r = rest_client.post(
            "/inspect",
            json={
                "reference": ["mydb.orders.amount", "mydb.orders.customer_id"],
                "entity_type": "column",
                "compact": False,
                "format": "json",
            },
        )
        assert r.status_code == 200
        # The REST envelope always carries a STRING under ``result``; for a list
        # input that string is a JSON-array, not a native array response.
        result = r.json()["result"]
        assert isinstance(result, str)
        arr = json.loads(result)
        assert isinstance(arr, list)
        assert [e["canonical_id"] for e in arr] == [
            "mydb.orders.amount", "mydb.orders.customer_id",
        ]

    def test_single_element_array_stays_list(
        self, rest_client: TestClient
    ) -> None:
        r = rest_client.post(
            "/inspect",
            json={
                "reference": ["mydb.orders.amount"],
                "entity_type": "column",
                "compact": False,
                "format": "json",
            },
        )
        assert r.status_code == 200
        arr = json.loads(r.json()["result"])
        assert isinstance(arr, list)
        assert len(arr) == 1

    def test_empty_list_is_400(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={"reference": [], "entity_type": "column"},
        )
        assert r.status_code == 400

    def test_non_string_member_is_422(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={"reference": ["mydb.orders.amount", 1], "entity_type": "column"},
        )
        assert r.status_code == 422


class TestCliBatch:
    def test_run_inspect_single_element_list_is_bare(
        self, cli_storage: YAMLStorage
    ) -> None:
        # argparse nargs="+" always yields a list; the CLI adapter maps a
        # one-element list back to a bare str → byte-for-byte single output.
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(reference=["mydb.orders.amount"]),
                storage=cli_storage,
            )
        out = buf.getvalue()
        assert "Column: mydb.orders.amount" in out
        assert "## mydb.orders.amount" not in out

    def test_run_inspect_multi_positional_is_batch(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(
                    reference=["mydb.orders.amount", "mydb.orders.customer_id"],
                ),
                storage=cli_storage,
            )
        out = buf.getvalue()
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.customer_id" in out

    def test_parser_accepts_multiple_positionals(
        self, cli_storage: YAMLStorage, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # End-to-end: ``slayer inspect c1 c2 --type column`` must parse two
        # positionals (nargs="+") and batch them.
        import sys

        from slayer.cli import main

        storage_dir = os.path.join(str(tmp_path), "store")
        os.makedirs(storage_dir, exist_ok=True)
        _seed_sync(YAMLStorage(base_dir=storage_dir))

        argv = [
            "slayer", "inspect",
            "mydb.orders.amount", "mydb.orders.customer_id",
            "--type", "column", "--storage", storage_dir,
        ]
        monkeypatch.setattr(sys, "argv", argv)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main()
        out = buf.getvalue()
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.customer_id" in out


class TestSlayerClientBatch:
    async def test_inspect_async_local_list(
        self, client_storage: YAMLStorage
    ) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(storage=client_storage)
        out = await client.inspect(
            reference=["mydb.orders.amount", "mydb.orders.customer_id"],
            entity_type="column", compact=False,
        )
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.customer_id" in out

    def test_inspect_sync_local_list(
        self, client_storage: YAMLStorage
    ) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(storage=client_storage)
        out = client.inspect_sync(
            reference=["mydb.orders.amount", "mydb.orders.customer_id"],
            entity_type="column", compact=False,
        )
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.customer_id" in out

    def test_inspect_sync_remote_posts_list_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None  # remote mode
        captured: dict = {}

        def _fake_request_sync(*, method, path, json=None, params=None):
            captured.update({"json": json})
            return {"result": "REMOTE BATCH"}

        monkeypatch.setattr(client, "_request_sync", _fake_request_sync)
        out = client.inspect_sync(
            reference=["mydb.orders.amount", "mydb.orders.customer_id"],
            entity_type="column", compact=False,
        )
        assert out == "REMOTE BATCH"
        # The list is carried through to the HTTP body verbatim.
        assert captured["json"]["reference"] == [
            "mydb.orders.amount", "mydb.orders.customer_id",
        ]

    async def test_inspect_async_remote_posts_list_body(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None  # remote mode
        captured: dict = {}

        async def _fake_request(*, method, path, json=None, params=None):
            captured.update({"json": json})
            return {"result": "REMOTE BATCH ASYNC"}

        monkeypatch.setattr(client, "_request", _fake_request)
        out = await client.inspect(
            reference=["mydb.orders.amount", "mydb.orders.customer_id"],
            entity_type="column", compact=False,
        )
        assert out == "REMOTE BATCH ASYNC"
        assert captured["json"]["reference"] == [
            "mydb.orders.amount", "mydb.orders.customer_id",
        ]
