"""Every query surface takes the conformance beside the query and reaches the same strict binding.

Spec: queries/ossie-l3-binding › The conformance is chosen beside the query; Strict refusals are one typed error on
every surface; mcp/query-tool › Single polymorphic query argument; cli/commands › slayer query takes a conformance.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError

from slayer import usage_reporting
from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.mcp.server import create_mcp_server
from slayer.storage.base import StorageBackend
from tests._cli_inprocess import run_cli_in_process
from tests._dev2217_fixtures import DS, STRICT, fixture_texts, load_sql, ossie_query_error, strict_error
from tests._dev2243_fixtures import make_storage, store_document
from tests._engine_helpers import owned_query_engine

PARTIAL = {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue", "tickets.ticket_count"]}
ADMITTED = {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}
LIMITED = {"metrics": ["orders.total_revenue"], "limit": 10}
CODE = "E_PARTIAL_DIMENSION_REACHABILITY"


async def prepare_store(base: Path) -> StorageBackend:
    """A YAML store under ``base`` holding the retail fixture's datasource and document."""
    sql, model = fixture_texts("retail")
    database = base / f"{DS}.duckdb"
    load_sql(sql, database)
    storage = make_storage("yaml", base)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(database)))
    await store_document(storage, model, data_source=DS)
    return storage


def _assert_partial(exc: Any) -> None:
    assert issubclass(type(exc), ossie_query_error())
    assert exc.code == CODE
    assert (exc.dimension, exc.metric) == ("customers.region", "tickets.ticket_count")


# --------------------------------------------------------------------------- #
# Engine and embedded client
# --------------------------------------------------------------------------- #
async def test_engine(tmp_path: Path) -> None:
    storage = await prepare_store(tmp_path)
    async with owned_query_engine(storage=storage) as engine:
        _assert_partial(await strict_error(engine, PARTIAL, CODE))
        assert (await engine.execute(PARTIAL)).data


async def test_unknown_conformance_on_the_engine(tmp_path: Path) -> None:
    """An unknown conformance is refused."""
    storage = await prepare_store(tmp_path)
    async with owned_query_engine(storage=storage) as engine:
        with pytest.raises(ValueError, match="slayer") as raised:
            await engine.execute({"no_such_part": 1}, conformance="ossie")
    assert "ossie-l3" in str(raised.value)
    assert not issubclass(type(raised.value), ossie_query_error())


async def test_embedded_client_async(tmp_path: Path) -> None:
    client = SlayerClient(storage=await prepare_store(tmp_path))
    with pytest.raises(ossie_query_error()) as raised:
        await client.query(PARTIAL, conformance=STRICT)
    _assert_partial(raised.value)
    response = await client.query(ADMITTED, conformance=STRICT)
    assert response.columns == ["region", "total_revenue"]
    with pytest.raises(ossie_query_error()):
        await client.sql(LIMITED, conformance=STRICT)
    assert await client.sql(ADMITTED, conformance=STRICT)
    with pytest.raises(ossie_query_error()):
        await client.explain(LIMITED, conformance=STRICT)
    with pytest.raises(ValueError, match="ossie-l3"):
        await client.query(ADMITTED, conformance="ossie")


def test_embedded_client_sync(tmp_path: Path) -> None:
    client = SlayerClient(storage=asyncio.run(prepare_store(tmp_path)))
    with pytest.raises(ossie_query_error()) as raised:
        client.query_sync(PARTIAL, conformance=STRICT)
    _assert_partial(raised.value)
    assert client.query_sync(ADMITTED, conformance=STRICT).columns == ["region", "total_revenue"]
    with pytest.raises(ossie_query_error()):
        client.sql_sync(LIMITED, conformance=STRICT)
    assert client.sql_sync(ADMITTED, conformance=STRICT)
    with pytest.raises(ossie_query_error()):
        client.explain_sync(LIMITED, conformance=STRICT)
    frame = pd.DataFrame(client.query_sync(ADMITTED, conformance=STRICT).data)
    assert frame.columns.tolist() == ["region", "total_revenue"]


CLIENT_METHODS = ["query", "sql", "explain"]
UNREADABLE = {"no_such_part": 1}


@pytest.mark.parametrize("method", CLIENT_METHODS)
async def test_unknown_conformance_on_the_embedded_client(tmp_path: Path, method: str) -> None:
    client = SlayerClient(storage=await prepare_store(tmp_path))
    run = getattr(client, method)
    with pytest.raises(ValueError, match="slayer") as raised:
        await run(UNREADABLE, conformance="ossie")
    assert "ossie-l3" in str(raised.value)


@pytest.mark.parametrize("method", CLIENT_METHODS)
def test_unknown_conformance_on_the_embedded_sync_client(tmp_path: Path, method: str) -> None:
    client = SlayerClient(storage=asyncio.run(prepare_store(tmp_path)))
    run = getattr(client, f"{method}_sync")
    with pytest.raises(ValueError, match="slayer") as raised:
        run(UNREADABLE, conformance="ossie")
    assert "ossie-l3" in str(raised.value)


# --------------------------------------------------------------------------- #
# REST and the remote client
# --------------------------------------------------------------------------- #
@pytest.fixture
def rest(tmp_path: Path) -> Iterator[TestClient]:
    with TestClient(create_app(storage=asyncio.run(prepare_store(tmp_path)))) as client:
        yield client


def test_rest_refusal_carries_the_code(rest: TestClient) -> None:
    """The REST body carries the code."""
    response = rest.post("/query", params={"conformance": STRICT}, json={"metrics": ["orders.no_such_metric"]})
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "E_NAME_NOT_FOUND"
    assert detail["message"].startswith("OssieQueryError: E_NAME_NOT_FOUND")


def test_rest_refusal_carries_the_attributes(rest: TestClient) -> None:
    response = rest.post("/query", params={"conformance": STRICT}, json=PARTIAL)
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert (detail["code"], detail["dimension"], detail["metric"]) == (CODE, "customers.region",
                                                                       "tickets.ticket_count")
    assert rest.post("/query", json=PARTIAL).status_code == 200


def test_rest_takes_the_conformance_in_the_url(rest: TestClient) -> None:
    """REST takes the conformance in the URL; the body is read as the query alone."""
    response = rest.post("/query", params={"conformance": STRICT}, json={"metrics": ["orders.total_revenue"]})
    assert response.status_code == 200, response.text
    assert response.json()["data"] == [{"total_revenue": 650}]
    assert response.json()["columns"] == ["total_revenue"]
    limited = rest.post("/query", params={"conformance": STRICT}, json=LIMITED)
    assert limited.status_code == 400
    assert limited.json()["detail"]["code"] == "E_INVALID_QUERY"


def test_rest_run_by_name_refuses_query_parts_beside_the_name(tmp_path: Path) -> None:
    storage = asyncio.run(prepare_store(tmp_path))
    asyncio.run(storage.save_dataset(SlayerDataset(name="by_region", data_source=DS,
                                                   source_queries=[SlayerQuery.model_validate(ADMITTED)])))
    with TestClient(create_app(storage=storage)) as rest:
        assert rest.post("/query", params={"conformance": STRICT}, json={"name": "by_region"}).status_code == 200
        response = rest.post("/query", params={"conformance": STRICT},
                             json={"name": "by_region", "metrics": ["orders.total_revenue"]})
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "E_INVALID_QUERY"


def test_rest_strict_reads_execution_flags_as_validated(rest: TestClient) -> None:
    """``"false"`` validates as false, so the query runs rather than dry-running."""
    response = rest.post("/query", params={"conformance": STRICT},
                         json={"metrics": ["orders.total_revenue"], "dry_run": "false", "explain": "false"})
    assert response.status_code == 200, response.text
    assert response.json()["data"] == [{"total_revenue": 650}]
    assert response.json().get("sql") is None


def test_rest_unknown_conformance(rest: TestClient) -> None:
    response = rest.post("/query", params={"conformance": "ossie"}, json=ADMITTED)
    assert 400 <= response.status_code < 500
    assert "ossie-l3" in response.text
    assert "slayer" in response.text


def _routed(monkeypatch: pytest.MonkeyPatch, app: Any) -> None:
    """Route the remote client's httpx traffic to ``app`` in process."""
    async_client = httpx.AsyncClient
    sync_factory: Callable[..., Any] = lambda *_a, **_k: TestClient(app)  # noqa: E731
    async_factory: Callable[..., Any] = lambda *_a, **_k: async_client(  # noqa: E731
        transport=httpx.ASGITransport(app=app), base_url="http://testserver")
    monkeypatch.setattr(httpx, "Client", sync_factory)
    monkeypatch.setattr(httpx, "AsyncClient", async_factory)


def test_remote_client_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _routed(monkeypatch, create_app(storage=asyncio.run(prepare_store(tmp_path))))
    client = SlayerClient(url="http://testserver")
    with pytest.raises(ossie_query_error()) as raised:
        client.query_sync(PARTIAL, conformance=STRICT)
    _assert_partial(raised.value)
    assert client.query_sync(ADMITTED, conformance=STRICT).columns == ["region", "total_revenue"]
    with pytest.raises(ossie_query_error()):
        client.sql_sync(LIMITED, conformance=STRICT)
    with pytest.raises(ossie_query_error()):
        client.explain_sync(LIMITED, conformance=STRICT)


@pytest.mark.parametrize("query", [
    pytest.param(SlayerQuery.model_validate({"dimensions": None, "metrics": ["orders.total_revenue"]}), id="explicit-null"),
    pytest.param([SlayerQuery.model_validate({"metrics": ["orders.total_revenue"]})], id="list-of-models"),
])
async def test_remote_client_sends_the_query_as_set(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                    query: Any) -> None:
    """The server judges what the caller set: an explicit null and a stage list are refused with the code."""
    _routed(monkeypatch, create_app(storage=await prepare_store(tmp_path)))
    client = SlayerClient(url="http://testserver")
    with pytest.raises(ossie_query_error()) as raised:
        await client.query(query, conformance=STRICT)
    assert raised.value.code == "E_INVALID_QUERY"
    typed = await client.query(SlayerQuery.model_validate({"metrics": ["orders.total_revenue"]}), conformance=STRICT)
    assert typed.data == [{"total_revenue": 650}]


async def test_remote_client_async(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _routed(monkeypatch, create_app(storage=await prepare_store(tmp_path)))
    client = SlayerClient(url="http://testserver")
    with pytest.raises(ossie_query_error()) as raised:
        await client.query({"metrics": ["orders.no_such_metric"]}, conformance=STRICT)
    assert raised.value.code == "E_NAME_NOT_FOUND"
    with pytest.raises(ossie_query_error()) as partial:
        await client.query(PARTIAL, conformance=STRICT)
    _assert_partial(partial.value)
    assert (await client.query(ADMITTED, conformance=STRICT)).columns == ["region", "total_revenue"]
    with pytest.raises(ossie_query_error()):
        await client.sql(LIMITED, conformance=STRICT)
    assert await client.sql(ADMITTED, conformance=STRICT)
    with pytest.raises(ossie_query_error()):
        await client.explain(LIMITED, conformance=STRICT)


@pytest.mark.parametrize("method", CLIENT_METHODS)
async def test_unknown_conformance_on_the_remote_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                        method: str) -> None:
    _routed(monkeypatch, create_app(storage=await prepare_store(tmp_path)))
    client = SlayerClient(url="http://testserver")
    run, run_sync = getattr(client, method), getattr(client, f"{method}_sync")
    with pytest.raises(Exception) as raised:  # noqa: PT011 — refused locally or by the server; never judged
        await run(UNREADABLE, conformance="ossie")
    with pytest.raises(Exception) as raised_sync:  # noqa: PT011
        await asyncio.to_thread(run_sync, UNREADABLE, conformance="ossie")
    for exc in (raised.value, raised_sync.value):
        assert not issubclass(type(exc), (TypeError, ossie_query_error()))


# --------------------------------------------------------------------------- #
# MCP
# --------------------------------------------------------------------------- #
async def _mcp_text(server: Any, **arguments: Any) -> str:
    blocks, _ = await server.call_tool(name="query", arguments=arguments)
    return blocks[0].text


async def test_mcp_schema_lists_the_conformance(tmp_path: Path) -> None:
    server = create_mcp_server(storage=await prepare_store(tmp_path), _seed_help=False)
    tool = next(t for t in await server.list_tools() if t.name == "query")
    prop = tool.inputSchema["properties"]["conformance"]
    assert set(prop.get("enum") or next(o["enum"] for o in prop.get("anyOf", []) if "enum" in o)) == {"slayer",
                                                                                                    STRICT}


async def test_mcp_conformance_selects_the_strict_binding(tmp_path: Path) -> None:
    """The conformance argument selects the strict binding."""
    server = create_mcp_server(storage=await prepare_store(tmp_path), _seed_help=False)
    with pytest.raises(ToolError) as raised:
        await _mcp_text(server, query=LIMITED, conformance=STRICT)
    assert str(raised.value).removeprefix("Error executing tool query: ").startswith(
        "OssieQueryError: E_INVALID_QUERY")
    assert await _mcp_text(server, query=LIMITED)


MALFORMED = [
    pytest.param({"metrics": ["orders.total_revenue"], "foo": 1}, id="unknown-part"),
    pytest.param({"dimensions": [{"field": "customers.region", "foo": 1}]}, id="entry-property"),
    pytest.param({"metrics": "orders.total_revenue"}, id="wrong-type"),
    pytest.param({"metrics": [1]}, id="entry-type"),
]


@pytest.mark.parametrize("query", MALFORMED)
async def test_mcp_judges_the_query_as_submitted(tmp_path: Path, query: dict[str, Any]) -> None:
    """MCP hands the strict binding the input as submitted, so a malformed query is E_INVALID_QUERY."""
    server = create_mcp_server(storage=await prepare_store(tmp_path), _seed_help=False)
    with pytest.raises(ToolError) as strict_raised:
        await _mcp_text(server, query=query, conformance=STRICT)
    assert str(strict_raised.value).removeprefix("Error executing tool query: ").startswith(
        "OssieQueryError: E_INVALID_QUERY")
    with pytest.raises(ToolError) as native_raised:
        await _mcp_text(server, query=query)
    assert "OssieQueryError" not in str(native_raised.value)


async def test_strict_usage_reporting_sees_the_query_as_submitted(tmp_path: Path,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """REST and MCP report a strict query's features from the input as submitted, as the CLI does."""
    observed: list[Any] = []
    monkeypatch.setattr(usage_reporting, "observe_query", observed.append)
    storage = await prepare_store(tmp_path)
    with TestClient(create_app(storage=storage)) as rest:
        assert rest.post("/query", params={"conformance": STRICT}, json=ADMITTED).status_code == 200
    await _mcp_text(create_mcp_server(storage=storage, _seed_help=False), query=ADMITTED, conformance=STRICT)
    assert observed == [ADMITTED, ADMITTED]


async def test_mcp_reaches_the_same_binding(tmp_path: Path) -> None:
    server = create_mcp_server(storage=await prepare_store(tmp_path), _seed_help=False)
    with pytest.raises(ToolError, match=f"OssieQueryError: {CODE}"):
        await _mcp_text(server, query=PARTIAL, conformance=STRICT)
    out = json.loads(await _mcp_text(server, query=ADMITTED, conformance=STRICT, format="json"))
    data = out["data"] if isinstance(out, dict) else out
    assert [list(row) for row in data] == [["region", "total_revenue"]] * len(data)
    with pytest.raises(ToolError):
        await _mcp_text(server, query=ADMITTED, conformance="ossie")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _cli(tmp_path: Path, query: Any, *extra: str) -> Any:
    asyncio.run(prepare_store(tmp_path))
    return run_cli_in_process(["query", json.dumps(query), "--storage", str(tmp_path / "store"), *extra])


def test_a_strict_refusal_through_the_cli(tmp_path: Path) -> None:
    """A strict refusal through the CLI."""
    result = _cli(tmp_path, PARTIAL, "--conformance", STRICT)
    assert result.returncode != 0
    lines = result.stderr.splitlines()
    assert len(lines) == 1, result.stderr
    assert lines[0].startswith(f"OssieQueryError: {CODE}")
    assert "customers.region" in lines[0]
    assert "tickets.ticket_count" in lines[0]


def test_an_empty_query_list_is_a_strict_refusal_through_the_cli(tmp_path: Path) -> None:
    result = _cli(tmp_path, [], "--conformance", STRICT)
    assert result.returncode != 0
    assert "E_INVALID_QUERY" in result.stderr


def test_output_names_through_the_cli(tmp_path: Path) -> None:
    """Output names through the CLI."""
    result = _cli(tmp_path, ADMITTED, "--conformance", STRICT, "--format", "json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert [list(row) for row in data] == [["region", "total_revenue"]] * len(data)
    table = _cli(tmp_path / "table", ADMITTED, "--conformance", STRICT)
    assert table.stdout.splitlines()[0] == "region | total_revenue"


def test_cli_default_conformance_keeps_native_keys(tmp_path: Path) -> None:
    result = _cli(tmp_path, ADMITTED, "--format", "json")
    assert result.returncode == 0, result.stderr
    assert list(json.loads(result.stdout)[0]) == ["customers.region", "orders.total_revenue"]


def test_cli_refuses_an_unknown_conformance(tmp_path: Path) -> None:
    result = _cli(tmp_path, ADMITTED, "--conformance", "ossie")
    assert result.returncode == 2
    assert "ossie-l3" in result.stderr


def test_rest_names_the_conformances_before_judging_the_body(rest: TestClient) -> None:
    response = rest.post("/query", params={"conformance": "ossie"}, json={"metrics": 123})
    assert response.status_code == 400
    assert "ossie-l3" in response.text
    assert "slayer" in response.text


async def test_remote_strict_refuses_a_query_object_naming_a_saved_query(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """A query object's ``name`` would run a saved query on the wire; it is no Ossie part, as on the engine."""
    storage = await prepare_store(tmp_path)
    await storage.save_dataset(SlayerDataset(name="by_region", data_source=DS,
                                             source_queries=[SlayerQuery.model_validate(ADMITTED)]))
    _routed(monkeypatch, create_app(storage=storage))
    client = SlayerClient(url="http://testserver")
    with pytest.raises(ossie_query_error()) as raised:
        await client.query({"name": "by_region"}, conformance=STRICT)
    assert raised.value.code == "E_INVALID_QUERY"
