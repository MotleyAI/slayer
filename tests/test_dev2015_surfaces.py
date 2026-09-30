"""The spine and datasource granularities on every surface (MCP, REST, CLI, inspect, search), plus the dbt importer."""

from __future__ import annotations

import asyncio
import json
import tempfile
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import DatasourceConfig
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtMeasure, DbtMetric, DbtProject, DbtSemanticModel
from slayer.mcp.server import create_mcp_server
from slayer.search.service import SearchService
from slayer.storage.yaml_storage import YAMLStorage

from tests._cli_inprocess import run_cli_in_process
from tests._dev2015_fixtures import GRANULARITIES, GRANULARITY_NAMES, spine_engine, spine_models


async def _call(server, *, name: str, arguments: dict[str, Any]) -> str:
    blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return blocks[0].text


async def _spine_storage(base_dir: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    for model in spine_models():
        await storage.save_model(model, _validate=False)
    return storage


def _granularity_dump(cfg: DatasourceConfig) -> list[dict[str, Any]]:
    return [g.model_dump(mode="json") for g in cfg.granularities]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# The spine is a built-in model on every listing surface
# ---------------------------------------------------------------------------

class TestSpineListedWithItsWiredModels:
    async def test_mcp_listing_and_inspection(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        server = create_mcp_server(storage=storage)
        summary = await _call(server, name="models_summary", arguments={"datasource_name": "test"})
        assert "time_spine" in summary
        for tool, args in (
            ("inspect", {"entity_type": "model", "reference": "time_spine", "compact": False}),
            ("inspect_model", {"model_name": "time_spine", "compact": False}),
        ):
            text = await _call(server, name=tool, arguments=args)
            assert "timestamp" in text, tool
            assert "built-in" in text.lower(), tool
            for model, axis in (("orders", "order_date"), ("returns", "return_date")):
                assert model in text, (tool, model)
                assert axis in text, (tool, model)

    async def test_search_finds_the_spine(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        response = await SearchService(storage=storage).search(question="time spine calendar", max_results=20)
        assert any(h.kind == "model" and "time_spine" in h.id for h in response.results)

    def test_rest_listing(self) -> None:
        storage = asyncio.run(_spine_storage(tempfile.mkdtemp()))
        client = TestClient(create_app(storage=storage))
        names = {m["name"] for m in client.get("/models", params={"data_source": "test"}).json()}
        assert "time_spine" in names
        model = client.get("/models/time_spine", params={"data_source": "test"})
        assert model.status_code == 200
        assert [c["name"] for c in model.json()["columns"]] == ["timestamp"]

    def test_cli_listing(self) -> None:
        base = tempfile.mkdtemp()
        asyncio.run(_spine_storage(base))
        result = run_cli_in_process(["models", "--storage", base, "list"])
        assert result.returncode == 0, result.stderr
        assert "time_spine" in result.stdout


# ---------------------------------------------------------------------------
# Datasource granularities round-trip through every create / edit surface
# ---------------------------------------------------------------------------

class TestGranularitiesRoundTrip:
    async def test_mcp_edit_then_storage_rest_and_inspect(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        await storage.save_datasource(DatasourceConfig(name="gds", type="sqlite", database=":memory:"))
        server = create_mcp_server(storage=storage)
        await _call(server, name="edit_datasource", arguments={"name": "gds", "granularities": GRANULARITIES})
        stored = await storage.get_datasource("gds")
        assert stored is not None
        dumped = _granularity_dump(stored)
        assert [g["name"] for g in dumped] == list(GRANULARITY_NAMES)
        assert str(dumped[2]["origin"]).startswith("2000-01-01")  # quarter_hour's defaulted origin
        rest = TestClient(create_app(storage=storage)).get("/datasources/gds").json()
        assert rest["granularities"] == dumped
        text = await _call(server, name="inspect", arguments={"entity_type": "datasource", "reference": "gds",
                                                              "compact": False})
        for name in GRANULARITY_NAMES:
            assert name in text
        assert "2000-04-01" in text

    async def test_mcp_create(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        server = create_mcp_server(storage=storage)
        await _call(server, name="create_datasource", arguments={
            "name": "gds", "type": "sqlite", "database": ":memory:", "auto_ingest": False,
            "granularities": GRANULARITIES,
        })
        stored = await storage.get_datasource("gds")
        assert stored is not None
        assert [g["name"] for g in _granularity_dump(stored)] == list(GRANULARITY_NAMES)

    async def test_mcp_edit_rejects_an_invalid_definition(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        await storage.save_datasource(DatasourceConfig(name="gds", type="sqlite", database=":memory:"))
        server = create_mcp_server(storage=storage)
        text = await _call(server, name="edit_datasource", arguments={
            "name": "gds", "granularities": [{"name": "month", "base": "day"}],
        })
        assert "month" in text
        stored = await storage.get_datasource("gds")
        assert stored is not None
        assert _granularity_dump(stored) == []

    def test_rest_create(self) -> None:
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        client = TestClient(create_app(storage=storage))
        created = client.post("/datasources", json={
            "name": "gds", "type": "sqlite", "database": ":memory:", "granularities": GRANULARITIES,
        })
        assert created.status_code in (200, 201), created.text
        got = client.get("/datasources/gds").json()
        assert [g["name"] for g in got["granularities"]] == list(GRANULARITY_NAMES)
        bad = client.post("/datasources", json={
            "name": "bad", "type": "sqlite", "database": ":memory:", "granularities": [{"name": "sum", "base": "day"}],
        })
        assert bad.status_code in (400, 422)

    def test_cli_create(self) -> None:
        base = tempfile.mkdtemp()
        result = run_cli_in_process([
            "datasources", "--storage", base, "create", "sqlite:///:memory:", "--name", "gds",
            "--granularities", json.dumps(GRANULARITIES),
        ])
        assert result.returncode == 0, result.stderr
        stored = asyncio.run(YAMLStorage(base_dir=base).get_datasource("gds"))
        assert stored is not None
        assert [g["name"] for g in _granularity_dump(stored)] == list(GRANULARITY_NAMES)


# ---------------------------------------------------------------------------
# dbt importer
# ---------------------------------------------------------------------------

def _dbt_project(field: str, value: Any) -> DbtProject:
    return DbtProject(
        semantic_models=[DbtSemanticModel(name="orders", model="orders",
                                          measures=[DbtMeasure(name="revenue", agg="sum", expr="amount")])],
        metrics=[DbtMetric.model_validate({
            "name": "gap_filled_rev", "type": "simple",
            "type_params": {"measure": {"name": "revenue", field: value}},
        })],
    )


def _converted_measure(result, name: str):
    for model in result.models:
        for measure in model.measures:
            if measure.name == name:
                return measure
    return None


class TestDbtImporter:
    def test_fill_nulls_with_becomes_coalesce(self) -> None:
        result = DbtToSlayerConverter(project=_dbt_project("fill_nulls_with", 0), data_source="test").convert()
        measure = _converted_measure(result, "gap_filled_rev")
        assert measure is not None
        assert measure.formula.replace(" ", "") in {"coalesce(amount:sum,0)", "coalesce(sum(amount),0)"}
        assert not any(e.metric_name == "gap_filled_rev" for e in result.unconverted_metrics)

    def test_join_to_timespine_is_accepted_with_a_note(self) -> None:
        result = DbtToSlayerConverter(project=_dbt_project("join_to_timespine", True), data_source="test").convert()
        measure = _converted_measure(result, "gap_filled_rev")
        assert measure is not None
        assert measure.formula.replace(" ", "") in {"amount:sum", "sum(amount)"}
        notes = [e for e in (*result.unconverted_metrics, *result.warnings) if e.metric_name == "gap_filled_rev"]
        assert notes
        assert all(e.severity == "info" for e in notes)
        assert "time_spine" in result.render_report()


@pytest.mark.parametrize("backend", ["sqlite"])
async def test_spine_query_through_mcp(backend) -> None:
    async with spine_engine(backend) as eng:
        server = create_mcp_server(storage=eng.storage)
        text = await _call(server, name="query", arguments={"query": {
            "measures": [{"formula": "sum(orders.amount)", "name": "o"}],
            "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month",
                                 "date_range": ["2025-01-01", "2025-03-31"]}],
        }})
    assert "2025-03" in text
    assert "time_spine" in text
