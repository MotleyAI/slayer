"""Saved query-backed models inherit edges like a stage; inspect lists them, marked inherited.

Spec: openspec …/specs/queries/query-backed-inline — "Saved relation relationships survive splicing",
"Inspect lists a saved relation's inherited edges"; …/specs/queries/multi-stage — "Declared edges win over inherited ones".
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import tempfile
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.cli import _run_inspect
from slayer.core.errors import UnrelatedDatasetError
from slayer.core.models import DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2219_fixtures import (
    DIALECTS,
    DS,
    REVENUE_BY_REGION,
    base_datasets,
    by,
    customers,
    join,
    m,
    over,
    per_customer,
    shop,
    with_datasets,
)


def _saved(name: str = "customer_revenue", *, relationships: list[dict] | None = None,
           metrics: list[dict] | None = None, dry_run: bool = False) -> SlayerDataset:
    """Query-backed: ``orders`` grouped by ``customer_id``."""
    stage: dict[str, Any] = {"population": "orders", "dimensions": ["customer_id"],
                             "metrics": metrics or [m("sum(amount)", "revenue")]}
    if dry_run:
        stage["dry_run"] = True
    return SlayerDataset.model_validate({"name": name, "data_source": DS, "source_queries": [stage],
                                         "relationships": relationships or []})


def _raw_saved(name: str, stage: dict, **kw: Any) -> SlayerDataset:
    return SlayerDataset.model_validate({"name": name, "data_source": DS, "source_queries": [stage], **kw})


# --------------------------------------------------------------------------- #
# Execution.
# --------------------------------------------------------------------------- #
class TestExecution:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_a_saved_relation_inherits_like_a_stage(self, dialect: str) -> None:
        async with shop(dialect) as engine:
            await engine.save_dataset(_saved())
            saved = await engine.execute(over("customer_revenue", dimensions=["customers.region"]))
            listed = await engine.execute([per_customer("customer_revenue"),
                                           over("customer_revenue", dimensions=["customers.region"])])
        assert by(saved, key="customers.region", value="total") == REVENUE_BY_REGION
        assert by(listed, key="customers.region", value="total") == REVENUE_BY_REGION

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_an_identical_declared_edge_applies_once(self, dialect: str) -> None:
        async with shop(dialect) as engine:
            await engine.save_dataset(_saved("undeclared"))
            await engine.save_dataset(_saved(relationships=[join("customers", [("customer_id", "id")])]))
            undeclared = await engine.execute(over("undeclared", dimensions=["customers.region"]))
            resp = await engine.execute(over("customer_revenue", dimensions=["customers.region"]))
        assert by(undeclared, key="customers.region", value="total") == REVENUE_BY_REGION
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_a_reverse_declaration_absorbs_the_inherited_edge(self, dialect: str) -> None:
        back = customers()
        back = back.model_copy(update={"relationships": [*back.relationships, DatasetRelationship(
            to="customer_revenue", from_columns=["id"], to_columns=["customer_id"])]})
        async with shop(dialect, with_datasets(back)) as engine:
            await engine.save_dataset(_saved())
            resp = await engine.execute(over("customer_revenue", dimensions=["customers.region"]))
        assert by(resp, key="customers.region", value="total") == REVENUE_BY_REGION

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_ordinary_stage_hides_a_source_edge_whose_key_it_drops(self, dialect: str) -> None:
        stage = per_customer(dimensions=["status"])
        async with shop(dialect) as engine:
            stages: list[Any] = [stage, over("per_customer", dimensions=["customers.region"])]
            with pytest.raises(UnrelatedDatasetError):
                await engine.execute(stages)


# --------------------------------------------------------------------------- #
# Inspect — MCP, REST and CLI.
# --------------------------------------------------------------------------- #
_CYCLE = ("cycle_a", "cycle_b")


async def _seed(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=":memory:"))
    for dataset in base_datasets():
        await storage.save_dataset(dataset)
    engine = SlayerQueryEngine(storage=storage)
    try:
        await engine.save_dataset(_saved(dry_run=True))
        await engine.save_dataset(_saved("customer_revenue_declared", dry_run=True,
                                         relationships=[join("customers", [("customer_id", "id")])]))
    finally:
        await engine.aclose()
    await storage.save_dataset(_raw_saved(_CYCLE[0], {"population": _CYCLE[1], "dimensions": ["customer_id"]}))
    await storage.save_dataset(_raw_saved(_CYCLE[1], {"population": _CYCLE[0], "dimensions": ["customer_id"]}))
    await storage.save_dataset(_raw_saved("bad_key", {"population": "orders", "dimensions": ["nope_column"],
                                                      "metrics": [m("sum(amount)", "revenue")]}))
    await storage.save_dataset(_raw_saved("needs_var", {
        "population": "orders", "dimensions": ["customer_id"], "metrics": [m("sum(amount)", "revenue")],
        "filter": ["amount > {threshold_var}"]}))
    await storage.save_dataset(_raw_saved("stale_key", {
        "population": "orders", "dimensions": ["customer_id"], "metrics": [m("sum(amount)", "revenue")]},
        relationships=[join("customers", [("stale_col", "id")])]))
    await storage.save_dataset(SlayerDataset.model_validate({
        "name": "stage_cycle", "data_source": DS,
        "source_queries": [{"name": "a", "population": "b", "dimensions": ["customer_id"]},
                           {"name": "b", "population": "a", "dimensions": ["customer_id"]},
                           {"population": "a", "dimensions": ["customer_id"]}]}))


def _seed_sync(storage: YAMLStorage) -> None:
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_seed(storage))
    finally:
        loop.close()


def _args(name: str) -> dict[str, Any]:
    return {"reference": f"{DS}.{name}", "entity_type": "dataset", "compact": False, "format": "json",
            "sections": ["relationships"]}


def _inherited(payload: dict) -> list[dict]:
    return [r for r in payload["relationships"] if r.get("inherited")]


def _check_listed(payload: dict) -> None:
    (edge,) = _inherited(payload)
    assert edge["to"] == "customers"
    assert edge["from_columns"] == ["customer_id"]
    assert edge["to_columns"] == ["id"]
    assert edge["inherited_from"] == {"dataset": "orders", "relationship": "customers"}
    assert payload["inherited_relationships_error"] is None


def _check_absorbed(payload: dict) -> None:
    to_customers = [r for r in payload["relationships"] if r["to"] == "customers"]
    assert len(to_customers) == 1
    assert not to_customers[0].get("inherited")


_FAILURE_TOKENS = {_CYCLE[0]: _CYCLE[0], "bad_key": "nope_column", "needs_var": "threshold_var",
                   "stage_cycle": "Cycle in query list", "stale_key": "stale_col"}


def _check_failure(payload: dict, name: str) -> None:
    assert _inherited(payload) == []
    message = payload["inherited_relationships_error"]
    assert isinstance(message, str)
    assert _FAILURE_TOKENS[name] in message


@pytest.fixture
async def mcp_server() -> AsyncIterator[Any]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await _seed(storage)
        server = create_mcp_server(storage=storage)
        yield server
        await getattr(server, "_slayer_engine").aclose()


async def _mcp(server: Any, name: str, **overrides: Any) -> str:
    blocks, _ = await server.call_tool(name="inspect", arguments={**_args(name), **overrides})
    return blocks[0].text


@pytest.fixture
def rest_client() -> Iterator[TestClient]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        _seed_sync(storage)
        yield TestClient(create_app(storage=storage))


def _rest(client: TestClient, name: str) -> dict:
    r = client.post("/inspect", json=_args(name))
    assert r.status_code == 200, r.text
    return json.loads(r.json()["result"])


@pytest.fixture
def cli_storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        _seed_sync(storage)
        yield storage


def _cli(storage: YAMLStorage, name: str) -> dict:
    args = SimpleNamespace(**_args(name), num_rows=3, show_sql=False, descriptions_max_chars=None)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _run_inspect(args=args, storage=storage)
    return json.loads(buf.getvalue())


class TestInspectMcp:
    async def test_inherited_edges_are_listed(self, mcp_server) -> None:
        _check_listed(json.loads(await _mcp(mcp_server, "customer_revenue")))

    async def test_markdown_marks_the_edge_inherited(self, mcp_server) -> None:
        out = await _mcp(mcp_server, "customer_revenue", format="markdown")
        (row,) = [line for line in out.splitlines() if line.startswith("|") and "customers" in line]
        assert "inherited" in row
        assert "orders" in row

    async def test_a_declared_relationship_absorbs_the_inherited_one(self, mcp_server) -> None:
        _check_absorbed(json.loads(await _mcp(mcp_server, "customer_revenue_declared")))

    @pytest.mark.parametrize("name", list(_FAILURE_TOKENS))
    async def test_planning_failures_do_not_fail_inspection(self, mcp_server, name: str) -> None:
        _check_failure(json.loads(await _mcp(mcp_server, name)), name)

    async def test_the_summary_lists_the_inherited_target(self, mcp_server) -> None:
        blocks, _ = await mcp_server.call_tool(name="datasets_summary",
                                               arguments={"datasource_name": DS, "format": "json"})
        payload = json.loads(blocks[0].text)
        (entry,) = [d for d in payload["datasets"] if d["name"] == "customer_revenue"]
        assert "customers" in entry["relationships_to"]

    @pytest.mark.parametrize("compact", [True, False])
    async def test_the_summary_names_each_inherited_edges_source(self, mcp_server, compact: bool) -> None:
        payload = await _summary(mcp_server, compact=compact)
        (entry,) = [d for d in payload["datasets"] if d["name"] == "customer_revenue"]
        assert entry["inherited_relationships"] == [
            {"name": None, "to": "customers", "inherited_from": {"dataset": "orders", "relationship": "customers"}}]
        assert entry["inherited_relationships_error"] is None

    async def test_the_summary_carries_a_planning_error(self, mcp_server) -> None:
        payload = await _summary(mcp_server, compact=True)
        (entry,) = [d for d in payload["datasets"] if d["name"] == "needs_var"]
        assert entry["inherited_relationships"] == []
        assert "threshold_var" in entry["inherited_relationships_error"]

    async def test_the_markdown_summary_carries_provenance_and_errors(self, mcp_server) -> None:
        blocks, _ = await mcp_server.call_tool(name="datasets_summary",
                                               arguments={"datasource_name": DS, "format": "markdown"})
        lines = blocks[0].text.splitlines()
        (listed,) = [line for line in lines if line.startswith("Inherited relationships:")]
        assert "`orders.customers`" in listed
        assert any(line.startswith("Inherited relationships unavailable:") and "threshold_var" in line
                   for line in lines)

    async def test_the_compact_collection_carries_provenance_and_errors(self, mcp_server) -> None:
        blocks, _ = await mcp_server.call_tool(name="inspect", arguments={
            "entity_type": "dataset", "compact": True, "format": "json"})
        (group,) = [g for g in json.loads(blocks[0].text)["datasources"] if g["data_source"] == DS]
        entries = {d["name"]: d for d in group["datasets"]}
        assert entries["customer_revenue"]["inherited_relationships"][0]["inherited_from"] == {
            "dataset": "orders", "relationship": "customers"}
        assert "threshold_var" in entries["needs_var"]["inherited_relationships_error"]

    async def test_markdown_inspection_carries_a_planning_error(self, mcp_server) -> None:
        out = await _mcp(mcp_server, "needs_var", format="markdown")
        (line,) = [x for x in out.splitlines() if "Inherited relationships could not be derived" in x]
        assert "threshold_var" in line

    async def test_a_profiling_failure_does_not_hide_the_relationships(self, mcp_server) -> None:
        payload = json.loads(await _mcp(mcp_server, _CYCLE[0], sections=["relationships", "samples"]))
        _check_failure(payload, _CYCLE[0])


async def _summary(server: Any, *, compact: bool) -> dict:
    blocks, _ = await server.call_tool(name="datasets_summary",
                                       arguments={"datasource_name": DS, "format": "json", "compact": compact})
    return json.loads(blocks[0].text)


class TestInspectRest:
    def test_inherited_edges_are_listed(self, rest_client: TestClient) -> None:
        _check_listed(_rest(rest_client, "customer_revenue"))

    def test_a_declared_relationship_absorbs_the_inherited_one(self, rest_client: TestClient) -> None:
        _check_absorbed(_rest(rest_client, "customer_revenue_declared"))

    @pytest.mark.parametrize("name", list(_FAILURE_TOKENS))
    def test_planning_failures_do_not_fail_inspection(self, rest_client: TestClient, name: str) -> None:
        _check_failure(_rest(rest_client, name), name)


class TestInspectCli:
    def test_inherited_edges_are_listed(self, cli_storage: YAMLStorage) -> None:
        _check_listed(_cli(cli_storage, "customer_revenue"))

    def test_a_declared_relationship_absorbs_the_inherited_one(self, cli_storage: YAMLStorage) -> None:
        _check_absorbed(_cli(cli_storage, "customer_revenue_declared"))

    @pytest.mark.parametrize("name", list(_FAILURE_TOKENS))
    def test_planning_failures_do_not_fail_inspection(self, cli_storage: YAMLStorage, name: str) -> None:
        _check_failure(_cli(cli_storage, name), name)
