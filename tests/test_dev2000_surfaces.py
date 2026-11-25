"""The query zone on refinement, the response and every surface (spec: queries/time-zones, saved-query-refinement)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer import cli
from slayer.api.server import QueryRequest, create_app
from slayer.core.query import QueryRefinement, SlayerQuery, refine_query
from slayer.core.models import DatasourceConfig
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

from tests._cli_inprocess import run_cli_in_process
from tests._deprecated_input_helpers import mcp_payload
from tests._dev2000_fixtures import BACKENDS, DS, NY, TOKYO, tz_engine
from tests._dev2000_surface_fixtures import (
    LOCAL_DAY,
    LOCAL_DAY_ROWS,
    MONTHLY,
    MONTHLY_NY,
    MONTHLY_STORED,
    MONTHLY_TOKYO,
    local_days,
    months,
    scenario_store,
)
from tests._mcp_results import text_blocks

UNKNOWN = "Mars/Olympus"
PARIS = "Europe/Paris"


async def _call(server: Any, *, name: str, arguments: dict[str, Any]) -> str:
    return text_blocks(await server.call_tool(name=name, arguments=arguments))[0].text


async def _datasource(storage: YAMLStorage, name: str) -> DatasourceConfig | None:
    return await storage.get_datasource(name)


async def _field_zone(storage: YAMLStorage, field: str) -> Any:
    dataset = await storage.get_dataset("orders", data_source=DS)
    assert dataset is not None
    return next(f for f in dataset.fields if f.name == field).time_zone


# ---------------------------------------------------------------------------
# Refinement
# ---------------------------------------------------------------------------

class TestRefineQueryMerge:
    def _saved(self) -> SlayerQuery:
        return SlayerQuery.model_validate({**MONTHLY, "time_zone": TOKYO})

    def test_supplied_replaces(self) -> None:
        merged = refine_query(saved=self._saved(), refinement=QueryRefinement.model_validate({"time_zone": NY}))
        assert merged.time_zone == NY

    def test_explicit_null_clears(self) -> None:
        merged = refine_query(saved=self._saved(), refinement=QueryRefinement.model_validate({"time_zone": None}))
        assert merged.time_zone is None

    @pytest.mark.parametrize("refinement", [{}, {"limit": 2}])
    def test_omitted_keeps(self, refinement: dict[str, Any]) -> None:
        merged = refine_query(saved=self._saved(), refinement=QueryRefinement.model_validate(refinement))
        assert merged.time_zone == TOKYO

    def test_refinement_supplies_a_zone_to_a_zone_less_query(self) -> None:
        saved = SlayerQuery.model_validate(MONTHLY)
        merged = refine_query(saved=saved, refinement=QueryRefinement.model_validate({"time_zone": NY}))
        assert merged.time_zone == NY


@pytest.mark.parametrize("backend", BACKENDS)
class TestRefiningTheZone:
    async def test_refined_zone_buckets_in_new_york(self, backend: str) -> None:
        async with tz_engine(backend) as engine:
            await engine.create_dataset_from_query(query=MONTHLY, name="monthly")
            plain = await engine.execute("monthly")
            refined = await engine.execute("monthly", refine={"time_zone": NY})
            by_hand = await engine.execute(SlayerQuery.model_validate({**MONTHLY, "time_zone": NY}))
        assert months(plain.data) == MONTHLY_STORED
        assert months(refined.data) == MONTHLY_NY
        assert refined.data == by_hand.data
        assert refined.time_zone == NY

    async def test_saved_zone_cleared_or_kept(self, backend: str) -> None:
        async with tz_engine(backend) as engine:
            await engine.create_dataset_from_query(query={**MONTHLY, "time_zone": TOKYO}, name="monthly_tokyo")
            cleared = await engine.execute("monthly_tokyo", refine={"time_zone": None})
            kept = await engine.execute("monthly_tokyo")
        assert months(cleared.data) == MONTHLY_STORED
        assert cleared.time_zone is None
        assert months(kept.data) == MONTHLY_TOKYO
        assert kept.time_zone == TOKYO


# ---------------------------------------------------------------------------
# The response
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
async def test_response_reports_the_zone(backend: str) -> None:
    async with tz_engine(backend) as engine:
        zoned = await engine.execute(SlayerQuery.model_validate(LOCAL_DAY))
        plain = await engine.execute(SlayerQuery.model_validate({k: v for k, v in LOCAL_DAY.items() if k != "time_zone"}))
        dry = await engine.execute(SlayerQuery.model_validate(LOCAL_DAY), dry_run=True)
    assert zoned.time_zone == NY
    assert plain.time_zone is None
    assert dry.time_zone == NY
    assert local_days(zoned.data) == LOCAL_DAY_ROWS


async def test_response_reports_the_final_stage_zone() -> None:
    async with tz_engine("sqlite") as engine:
        resp = await engine.execute([
            {"name": "s1", "population": "orders", "dimensions": ["orders.id", "orders.ordered_at"], "time_zone": TOKYO},
            {"population": "s1", "dimensions": ["s1.id"], "time_zone": NY},
        ])
    assert resp.time_zone == NY


# ---------------------------------------------------------------------------
# Query surfaces: REST, MCP, CLI
# ---------------------------------------------------------------------------

def test_query_request_carries_the_zone() -> None:
    request = QueryRequest.model_validate(LOCAL_DAY)
    assert SlayerQuery.model_validate(request.model_dump(exclude_unset=True)).time_zone == NY


class TestRestQuery:
    async def test_post_query(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        resp = TestClient(create_app(storage=storage)).post("/query", json=LOCAL_DAY)
        assert resp.status_code == 200, resp.text
        assert local_days(resp.json()["data"]) == LOCAL_DAY_ROWS

    async def test_post_query_refuses_an_unknown_zone(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        client = TestClient(create_app(storage=storage))
        assert client.post("/query", json=LOCAL_DAY).status_code == 200
        resp = client.post("/query", json={**LOCAL_DAY, "time_zone": UNKNOWN})
        assert resp.status_code in (400, 422), resp.text
        assert UNKNOWN in resp.text

    async def test_run_by_name_refines_the_zone(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path), saved={"monthly": MONTHLY})
        resp = TestClient(create_app(storage=storage)).post(
            "/query", json={"name": "monthly", "refine": {"time_zone": NY}})
        assert resp.status_code == 200, resp.text
        assert months(resp.json()["data"]) == MONTHLY_NY


class TestMcpQuery:
    async def test_query_tool(self, tmp_path: Path) -> None:
        server = create_mcp_server(storage=await scenario_store(str(tmp_path)))
        text = await _call(server, name="query", arguments={"query": LOCAL_DAY, "format": "json"})
        assert local_days(mcp_payload(text)["data"]) == LOCAL_DAY_ROWS
        assert mcp_payload(text)["time_zone"] == LOCAL_DAY["time_zone"]

    @pytest.mark.parametrize(("fmt", "line"), [("csv", "# time zone: {}"), ("markdown", "Time zone: {}")])
    async def test_text_formats_name_the_zone(self, tmp_path: Path, fmt: str, line: str) -> None:
        server = create_mcp_server(storage=await scenario_store(str(tmp_path)))
        text = await _call(server, name="query", arguments={"query": LOCAL_DAY, "format": fmt})
        assert line.format(LOCAL_DAY["time_zone"]) in text.splitlines()
        plain = {k: v for k, v in LOCAL_DAY.items() if k != "time_zone"}
        assert "time zone" not in (await _call(server, name="query", arguments={"query": plain, "format": fmt})).lower()

    def test_query_tool_schema_advertises_the_zone(self, tmp_path: Path) -> None:
        tools = {t.name: t for t in asyncio.run(create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path))).list_tools())}
        assert "time_zone" in json.dumps(tools["query"].input_schema)


class TestCliQuery:
    def test_query_json(self, tmp_path: Path) -> None:
        storage = asyncio.run(scenario_store(str(tmp_path)))
        result = run_cli_in_process(["query", json.dumps(LOCAL_DAY), "--storage", storage.base_dir, "--format", "json"])
        assert result.returncode == 0, result.stdout + result.stderr
        assert local_days(json.loads(result.stdout)) == LOCAL_DAY_ROWS

    def test_refine_by_name(self, tmp_path: Path) -> None:
        storage = asyncio.run(scenario_store(str(tmp_path), saved={"monthly": MONTHLY}))
        result = run_cli_in_process(["query", "monthly", "--storage", storage.base_dir, "--format", "json",
                                     "--refine", json.dumps({"time_zone": NY})])
        assert result.returncode == 0, result.stdout + result.stderr
        assert months(json.loads(result.stdout)) == MONTHLY_NY


# ---------------------------------------------------------------------------
# Datasource zone: MCP, CLI, REST
# ---------------------------------------------------------------------------

class TestDatasourceZoneSurfaces:
    async def test_mcp_create(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        server = create_mcp_server(storage=storage)
        await _call(server, name="create_datasource", arguments={
            "name": "good", "type": "sqlite", "database": ":memory:", "auto_ingest": False, "time_zone": TOKYO})
        good = await _datasource(storage, "good")
        assert good is not None
        assert good.time_zone == TOKYO
        text = await _call(server, name="create_datasource", arguments={
            "name": "bad", "type": "sqlite", "database": ":memory:", "auto_ingest": False, "time_zone": UNKNOWN})
        assert UNKNOWN in text
        assert await _datasource(storage, "bad") is None

    async def test_mcp_edit(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="gds", type="sqlite", database=":memory:"))
        server = create_mcp_server(storage=storage)
        await _call(server, name="edit_datasource", arguments={"name": "gds", "time_zone": TOKYO})
        edited = await _datasource(storage, "gds")
        assert edited is not None
        assert edited.time_zone == TOKYO
        for bad in (UNKNOWN, "civil"):
            text = await _call(server, name="edit_datasource", arguments={"name": "gds", "time_zone": bad})
            assert bad in text
            unchanged = await _datasource(storage, "gds")
            assert unchanged is not None
            assert unchanged.time_zone == TOKYO
        await _call(server, name="edit_datasource", arguments={"name": "gds", "description": "d"})
        kept = await _datasource(storage, "gds")
        assert kept is not None
        assert kept.time_zone == TOKYO
        await _call(server, name="edit_datasource", arguments={"name": "gds", "time_zone": None})
        cleared = await _datasource(storage, "gds")
        assert cleared is not None
        assert cleared.time_zone is None

    def test_cli_create(self, tmp_path: Path) -> None:
        base = str(tmp_path)
        result = run_cli_in_process(["datasources", "--storage", base, "create", "sqlite:///:memory:", "--name", "gds",
                                     "--time-zone", TOKYO])
        assert result.returncode == 0, result.stdout + result.stderr
        stored = asyncio.run(YAMLStorage(base_dir=base).get_datasource("gds"))
        assert stored is not None
        assert stored.time_zone == TOKYO

    def test_cli_create_refuses_an_unknown_zone(self, tmp_path: Path) -> None:
        base = str(tmp_path)
        good = run_cli_in_process(["datasources", "--storage", base, "create", "sqlite:///:memory:", "--name", "good",
                                   "--time-zone", "UTC"])
        assert good.returncode == 0, good.stdout + good.stderr
        result = run_cli_in_process(["datasources", "--storage", base, "create", "sqlite:///:memory:", "--name", "bad",
                                     "--time-zone", UNKNOWN])
        assert result.returncode != 0
        assert UNKNOWN in result.stdout + result.stderr
        assert asyncio.run(YAMLStorage(base_dir=base).get_datasource("bad")) is None

    def test_cli_demo_refuses_an_unknown_zone_before_building(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(cli, "build_jaffle_shop", lambda **_: pytest.fail("the demo must not be built"))
        store = str(tmp_path / "store")
        result = run_cli_in_process(["datasources", "--storage", store, "create", "demo", "--time-zone", UNKNOWN])
        assert result.returncode == 1, result.stdout + result.stderr
        assert UNKNOWN in result.stderr
        assert asyncio.run(YAMLStorage(base_dir=store).list_datasources()) == []

    def test_rest_create_round_trips(self, tmp_path: Path) -> None:
        client = TestClient(create_app(storage=YAMLStorage(base_dir=str(tmp_path))))
        created = client.post("/datasources", json={"name": "gds", "type": "sqlite", "database": ":memory:",
                                                    "time_zone": TOKYO})
        assert created.status_code in (200, 201), created.text
        assert client.get("/datasources/gds").json()["time_zone"] == TOKYO
        bad = client.post("/datasources", json={"name": "bad", "type": "sqlite", "database": ":memory:",
                                                "time_zone": UNKNOWN})
        assert bad.status_code in (400, 422), bad.text


# ---------------------------------------------------------------------------
# Field zone: MCP dataset tools
# ---------------------------------------------------------------------------

class TestFieldZoneSurfaces:
    async def test_mcp_edit_dataset_sets_and_clears(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        server = create_mcp_server(storage=storage)
        await _call(server, name="edit_dataset", arguments={
            "dataset_name": "orders", "fields": [{"name": "ordered_at", "time_zone": TOKYO}]})
        assert await _field_zone(storage, "ordered_at") == TOKYO
        assert await _field_zone(storage, "local_slot") == "civil"
        await _call(server, name="edit_dataset", arguments={
            "dataset_name": "orders", "fields": [{"name": "ordered_at", "time_zone": None}]})
        assert await _field_zone(storage, "ordered_at") is None

    async def test_mcp_edit_dataset_refuses_an_instant_zone(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        server = create_mcp_server(storage=storage)
        text = await _call(server, name="edit_dataset", arguments={
            "dataset_name": "orders", "fields": [{"name": "paid_at", "time_zone": "UTC"}]})
        assert "paid_at" in text
        assert await _field_zone(storage, "paid_at") is None

    async def test_mcp_create_dataset(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        server = create_mcp_server(storage=storage)
        await _call(server, name="create_dataset", arguments={
            "name": "orders_tokyo", "sql_table": "orders", "data_source": DS,
            "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                       {"name": "ordered_at", "datatype": "DateTime", "time_zone": TOKYO}]})
        created = await storage.get_dataset("orders_tokyo", data_source=DS)
        assert created is not None
        assert next(f for f in created.fields if f.name == "ordered_at").time_zone == TOKYO
        text = await _call(server, name="create_dataset", arguments={
            "name": "orders_bad", "sql_table": "orders", "data_source": DS,
            "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                       {"name": "ordered_at", "datatype": "DateTime", "time_zone": UNKNOWN}]})
        assert UNKNOWN in text
        assert await storage.get_dataset("orders_bad", data_source=DS) is None


# ---------------------------------------------------------------------------
# Inspect
# ---------------------------------------------------------------------------

class TestInspectShowsZones:
    async def test_datasource(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        ds = await _datasource(storage, DS)
        assert ds is not None
        await storage.save_datasource(ds.model_copy(update={"time_zone": PARIS}))
        text = await _call(create_mcp_server(storage=storage), name="inspect", arguments={
            "entity_type": "datasource", "reference": DS, "compact": False})
        assert PARIS in text

    async def test_dataset_and_field(self, tmp_path: Path) -> None:
        storage = await scenario_store(str(tmp_path))
        server = create_mcp_server(storage=storage)
        await _call(server, name="edit_dataset", arguments={
            "dataset_name": "orders", "fields": [{"name": "ordered_at", "time_zone": TOKYO}]})
        dataset_text = await _call(server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "orders", "compact": False})
        assert TOKYO in dataset_text
        assert "civil" in dataset_text
        field_text = await _call(server, name="inspect", arguments={
            "entity_type": "field", "reference": "orders.ordered_at", "compact": False})
        assert TOKYO in field_text
