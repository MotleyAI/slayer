"""The spine and datasource granularities on every surface (MCP, REST, CLI, inspect, search), plus the dbt importer."""

from __future__ import annotations

import asyncio
import json
import tempfile
from typing import Any, Optional

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer import cli
from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.errors import DefaultTimeDimensionTypeError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetMetric, SlayerDataset
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtMeasure, DbtMetric, DbtProject, DbtSemanticModel
from slayer.ingest_report import ConversionResult
from slayer.mcp.server import create_mcp_server
from slayer.search.service import SearchService
from slayer.storage.yaml_storage import YAMLStorage

from tests._cli_inprocess import run_cli_in_process
from tests._dev2015_fixtures import GRANULARITIES, GRANULARITY_NAMES, returns_dataset, spine_engine, spine_datasets


async def _call(server, *, name: str, arguments: dict[str, Any]) -> str:
    blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return blocks[0].text


async def _spine_storage(base_dir: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    for model in spine_datasets():
        await storage.save_dataset(model, _validate=False)
    return storage


def _granularity_dump(cfg: DatasourceConfig) -> list[dict[str, Any]]:
    return [g.model_dump(mode="json") for g in cfg.granularities]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# The spine is a built-in dataset on every listing surface
# ---------------------------------------------------------------------------

class TestSpineListedWithItsWiredModels:
    async def test_mcp_listing_and_inspection(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        server = create_mcp_server(storage=storage)
        summary = await _call(server, name="datasets_summary", arguments={"datasource_name": "test"})
        assert "time_spine" in summary
        text = await _call(server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "time_spine", "compact": False,
        })
        assert "timestamp" in text
        assert "built-in" in text.lower()
        for model, axis in (("orders", "order_date"), ("returns", "return_date")):
            assert model in text, model
            assert axis in text, model

    async def test_listing_marks_it_built_in_and_points_at_inspect(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        server = create_mcp_server(storage=storage)
        summary = await _call(server, name="datasets_summary", arguments={"datasource_name": "test"})
        spine_block = summary[summary.index("## `time_spine`"):]
        assert "Built-in virtual dataset" in spine_block
        assert "inspect(reference='test.time_spine', entity_type='dataset')" in spine_block
        assert "order_date" not in spine_block
        detail = await _call(server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "test.time_spine", "compact": False,
        })
        assert "orders via order_date" in detail

    async def test_search_finds_the_spine(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        response = await SearchService(storage=storage).search(question="time spine calendar", max_results=20)
        assert any(h.kind == "dataset" and "time_spine" in h.id for h in response.results)

    def test_rest_listing(self) -> None:
        storage = asyncio.run(_spine_storage(tempfile.mkdtemp()))
        client = TestClient(create_app(storage=storage))
        names = {m["name"] for m in client.get("/datasets", params={"data_source": "test"}).json()}
        assert "time_spine" in names
        model = client.get("/datasets/time_spine", params={"data_source": "test"})
        assert model.status_code == 200
        assert [c["name"] for c in model.json()["fields"]] == ["timestamp"]

    def test_rest_unknown_datasource_has_no_spine(self) -> None:
        storage = asyncio.run(_spine_storage(tempfile.mkdtemp()))
        client = TestClient(create_app(storage=storage))
        assert client.get("/datasets", params={"data_source": "missing"}).json() == []
        assert client.get("/datasets/time_spine", params={"data_source": "missing"}).status_code == 404

    def test_cli_listing(self) -> None:
        base = tempfile.mkdtemp()
        asyncio.run(_spine_storage(base))
        result = run_cli_in_process(["datasets", "--storage", base, "list"])
        assert result.returncode == 0, result.stderr
        assert "time_spine" in result.stdout

    def test_cli_show(self) -> None:
        base = tempfile.mkdtemp()
        asyncio.run(_spine_storage(base))
        result = run_cli_in_process(["datasets", "--storage", base, "show", "time_spine"])
        assert result.returncode == 0, result.stdout + result.stderr
        assert "timestamp" in result.stdout


# ---------------------------------------------------------------------------
# A declared default_time_dimension must name a date or time field; the reserved name
# fails the same way on every save surface
# ---------------------------------------------------------------------------

def _bad_default(name: str = "returns_by_amount") -> SlayerDataset:
    return returns_dataset(default_time_dimension="amount").model_copy(update={"name": name})


def _stored_default(storage: YAMLStorage, name: str) -> Any:
    model = asyncio.run(storage.get_dataset(name, data_source="test"))
    return None if model is None else model.default_time_dimension


class TestDeclaredDefaultTimeDimensionType:
    async def test_storage_save(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        model = _bad_default()
        with pytest.raises(DefaultTimeDimensionTypeError) as exc:
            await storage.save_dataset(model)
        assert "returns_by_amount" in str(exc.value)
        assert "amount" in str(exc.value)
        assert await storage.get_dataset("returns_by_amount", data_source="test") is None

    async def test_text_rejected_and_temporal_accepted(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        text = SlayerDataset(name="by_text", sql_table="returns", data_source="test",
                           default_time_dimension="day_text", fields=[DatasetField(name="day_text")])
        with pytest.raises(DefaultTimeDimensionTypeError):
            await storage.save_dataset(text)
        for column_type in (DataType.DATE, DataType.DATETIME):
            model = SlayerDataset(name=f"by_{column_type.value.lower()}", sql_table="returns", data_source="test",
                                default_time_dimension="day", fields=[DatasetField(name="day", datatype=column_type)])
            await storage.save_dataset(model)
            stored = await storage.get_dataset(model.name, data_source="test")
            assert stored is not None
            assert stored.default_time_dimension == "day"

    async def test_mcp_edit_model(self) -> None:
        storage = await _spine_storage(tempfile.mkdtemp())
        server = create_mcp_server(storage=storage)
        text = await _call(server, name="edit_dataset", arguments={
            "dataset_name": "returns", "default_time_dimension": "amount",
        })
        assert "amount" in text
        stored = await storage.get_dataset("returns", data_source="test")
        assert stored is not None
        assert stored.default_time_dimension is None

    @pytest.mark.parametrize("name", ["returns_by_amount", "time_spine"])
    def test_rest_create(self, name) -> None:
        storage = asyncio.run(_spine_storage(tempfile.mkdtemp()))
        resp = TestClient(create_app(storage=storage)).post(
            "/datasets", json=_bad_default(name).model_dump(mode="json"),
        )
        assert resp.status_code == 400, resp.text
        assert name in resp.json()["detail"]
        if name != "time_spine":
            assert _stored_default(storage, name) is None

    @pytest.mark.parametrize("name", ["returns_by_amount", "time_spine"])
    def test_cli_create(self, name, tmp_path) -> None:
        base = str(tmp_path / "store")
        storage = asyncio.run(_spine_storage(base))
        path = tmp_path / "model.yaml"
        path.write_text(yaml.safe_dump(_bad_default(name).model_dump(mode="json")))
        result = run_cli_in_process(["datasets", "--storage", base, "create", str(path)])
        assert result.returncode == 1, result.stdout + result.stderr
        assert name in result.stderr
        if name != "time_spine":
            assert _stored_default(storage, name) is None


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

    def test_cli_create_demo(self, monkeypatch: pytest.MonkeyPatch) -> None:
        base = tempfile.mkdtemp()
        monkeypatch.setattr(cli, "build_jaffle_shop", lambda **_: False)
        result = run_cli_in_process([
            "datasources", "--storage", base, "create", "demo", "--name", "gds",
            "--granularities", json.dumps(GRANULARITIES),
        ])
        assert result.returncode == 0, result.stdout + result.stderr
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


def _converted_metric(result: ConversionResult, name: str) -> Optional[DatasetMetric]:
    for model in result.datasets:
        for measure in model.metrics:
            if measure.name == name:
                return measure
    return None


class TestDbtImporter:
    def test_fill_nulls_with_becomes_coalesce(self) -> None:
        result = DbtToSlayerConverter(project=_dbt_project("fill_nulls_with", 0), data_source="test").convert()
        measure = _converted_metric(result, "gap_filled_rev")
        assert measure is not None
        assert measure.expression.replace(" ", "") == "coalesce(sum(amount),0)"
        assert not any(e.metric_name == "gap_filled_rev" for e in result.unconverted_metrics)

    def test_join_to_timespine_is_accepted_with_a_note(self) -> None:
        result = DbtToSlayerConverter(project=_dbt_project("join_to_timespine", True), data_source="test").convert()
        measure = _converted_metric(result, "gap_filled_rev")
        assert measure is not None
        assert measure.expression.replace(" ", "") == "sum(amount)"
        notes = [e for e in (*result.unconverted_metrics, *result.warnings) if e.metric_name == "gap_filled_rev"]
        assert notes
        assert all(e.severity == "info" for e in notes)
        assert "time_spine" in result.render_report()


@pytest.mark.parametrize("backend", ["sqlite"])
async def test_spine_query_through_mcp(backend) -> None:
    async with spine_engine(backend) as eng:
        server = create_mcp_server(storage=eng.storage)
        text = await _call(server, name="query", arguments={"query": {
            "metrics": [{"metric": "sum(orders.amount)", "alias": "o"}],
            "time_dimensions": [{"field": "time_spine.timestamp", "granularity": "month",
                                 "date_range": ["2025-01-01", "2025-03-31"]}],
        }})
    assert "2025-03" in text
    assert "time_spine" in text
