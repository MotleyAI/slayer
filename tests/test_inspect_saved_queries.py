"""Saved queries are discoverable from the datasets they read (spec: queries/saved-query-refinement)."""

from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.dataset_render import _resolve_inspect_sections, _truncate_description
from slayer.inspect.service import InspectService
from slayer.mcp.server import create_mcp_server
from slayer.search.service import SearchService
from slayer.storage.yaml_storage import YAMLStorage
from tests._saved_query_refinement_fixtures import DESCRIPTIONS, build_refine_storage, call_tool_json

EXTRA_DESCRIPTIONS = {
    "ext_revenue": "Doubled revenue over an extended orders model",
    "rollup": "Rollup whose inner stage shares the model name",
    "target_totals": "Totals per target region",
    "monthly_rollup": "Total over the monthly revenue months",
}
ON_ORDERS = {
    **DESCRIPTIONS,
    "ext_revenue": EXTRA_DESCRIPTIONS["ext_revenue"],
    "rollup": EXTRA_DESCRIPTIONS["rollup"],
}


def _revenue(pop: str) -> dict:
    return {"metric": f"sum({pop}.amount)", "alias": "revenue"}


def _plain_dataset(name: str) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", sql_table="orders", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="region", datatype=DataType.STRING),
        DatasetField(name="amount", datatype=DataType.FLOAT),
    ])


@pytest.fixture
async def storage(tmp_path) -> YAMLStorage:
    store = await build_refine_storage(str(tmp_path))
    await store.save_dataset(_plain_dataset("targets"))
    await store.save_dataset(_plain_dataset("untouched"))
    engine = SlayerQueryEngine(storage=store)
    try:
        for name, query in (
            ("ext_revenue", {
                "population": "orders",
                "extensions": [{"dataset": "orders",
                                "metrics": [{"expression": "sum(amount * 2)", "name": "dbl"}]}],
                "metrics": [{"metric": "orders.dbl", "alias": "d"}],
            }),
            ("rollup", [
                {"name": "rollup", "population": "orders", "dimensions": ["orders.region"], "metrics": [_revenue("orders")]},
                {"population": "rollup", "metrics": [{"metric": "sum(rollup.revenue)", "alias": "total"}]},
            ]),
            ("target_totals", {"population": "targets", "dimensions": ["targets.region"], "metrics": [_revenue("targets")]}),
            ("monthly_rollup", {"population": "monthly_revenue",
                                "metrics": [{"metric": "sum(monthly_revenue.revenue)", "alias": "total"}]}),
        ):
            await engine.create_dataset_from_query(query=query, name=name, description=EXTRA_DESCRIPTIONS[name])
        await engine.save_dataset(SlayerDataset(
            name="hidden_q", data_source="test", hidden=True, description="Hidden saved query",
            source_queries=[SlayerQuery.model_validate({"population": "orders", "metrics": [_revenue("orders")]})],
        ))
    finally:
        await engine.aclose()
    return store


async def _inspect(storage: YAMLStorage, dataset: str, **kwargs: Any) -> str:
    return await InspectService(storage=storage).inspect(reference=f"test.{dataset}", entity_type="dataset", **kwargs)


def _listed(payload: Dict[str, Any]) -> Dict[str, Any]:
    entries = payload.get("saved_queries", [])
    names = [e["name"] for e in entries]
    assert len(names) == len(set(names)), names
    return {e["name"]: e["description"] for e in entries}


class TestModelViews:
    @pytest.mark.parametrize("compact", [True, False])
    async def test_json_lists_saved_queries(self, storage: YAMLStorage, compact: bool) -> None:
        payload = json.loads(await _inspect(storage, "orders", compact=compact, format="json"))
        assert _listed(payload) == ON_ORDERS

    async def test_full_markdown_section(self, storage: YAMLStorage) -> None:
        text = await _inspect(storage, "orders", compact=False)
        assert "## Saved queries" in text
        for name, description in ON_ORDERS.items():
            assert f"`{name}` — {description}" in text
        assert "hidden_q" not in text
        assert "target_totals" not in text

    async def test_compact_markdown_lists_names(self, storage: YAMLStorage) -> None:
        text = await _inspect(storage, "orders", compact=True)
        assert "Saved queries" in text
        for name in ON_ORDERS:
            assert name in text
        assert "hidden_q" not in text

    async def test_other_model_lists_only_its_own(self, storage: YAMLStorage) -> None:
        payload = json.loads(await _inspect(storage, "targets", compact=False, format="json"))
        assert _listed(payload) == {"target_totals": EXTRA_DESCRIPTIONS["target_totals"]}

    async def test_saved_query_on_a_saved_query(self, storage: YAMLStorage) -> None:
        payload = json.loads(await _inspect(storage, "monthly_revenue", compact=False, format="json"))
        assert _listed(payload) == {"monthly_rollup": EXTRA_DESCRIPTIONS["monthly_rollup"]}

    @pytest.mark.parametrize("model", ["untouched", "rollup"])
    @pytest.mark.parametrize("compact", [True, False])
    async def test_empty_listing_omitted(self, storage: YAMLStorage, model: str, compact: bool) -> None:
        payload = json.loads(await _inspect(storage, model, compact=compact, format="json"))
        assert "saved_queries" not in payload
        assert "Saved queries" not in await _inspect(storage, model, compact=compact)

    @pytest.mark.parametrize("compact", [True, False])
    async def test_descriptions_truncated(self, storage: YAMLStorage, compact: bool) -> None:
        payload = json.loads(await _inspect(storage, "orders", compact=compact, format="json",
                                            descriptions_max_chars=5))
        assert _listed(payload)["monthly_revenue"] == _truncate_description(DESCRIPTIONS["monthly_revenue"], 5)

    async def test_inspect_model_tool(self, storage: YAMLStorage) -> None:
        server = create_mcp_server(storage=storage)
        payload = await call_tool_json(
            server, name="inspect", arguments={"entity_type": "dataset", "reference": "orders", "compact": False, "format": "json", "sections": ["fields"]},
        )
        assert "saved_queries" not in payload
        payload = await call_tool_json(
            server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "orders", "compact": False, "format": "json", "sections": ["saved_queries"]},
        )
        assert _listed(payload) == ON_ORDERS


class TestSectionSelection:
    def test_saved_queries_is_a_known_section(self) -> None:
        assert _resolve_inspect_sections(["saved_queries"]) == (["saved_queries"], [])
        assert "saved_queries" in _resolve_inspect_sections(None)[0]
        assert _resolve_inspect_sections(["saved_queries", "fish"]) == (["saved_queries"], ["fish"])

    async def test_excluded(self, storage: YAMLStorage) -> None:
        text = await _inspect(storage, "orders", compact=False, sections=["fields"])
        assert "## Saved queries" not in text
        payload = json.loads(await _inspect(storage, "orders", compact=False, sections=["fields"], format="json"))
        assert "saved_queries" not in payload

    async def test_included_alone(self, storage: YAMLStorage) -> None:
        text = await _inspect(storage, "orders", compact=False, sections=["saved_queries"])
        assert "## Saved queries" in text
        payload = json.loads(await _inspect(storage, "orders", compact=False, sections=["saved_queries"],
                                            format="json"))
        assert _listed(payload) == ON_ORDERS
        assert "columns" not in payload


class TestSkeletonListings:
    async def test_datasource_markdown(self, storage: YAMLStorage) -> None:
        text = await InspectService(storage=storage).inspect(
            reference="test", entity_type="datasource", compact=False,
        )
        orders_block = text.split("## `orders`", 1)[1].split("\n## ", 1)[0]
        assert "Saved queries" in orders_block
        for name in ON_ORDERS:
            assert name in orders_block
        untouched_block = text.split("## `untouched`", 1)[1].split("\n## ", 1)[0]
        assert "Saved queries" not in untouched_block

    async def test_datasource_json(self, storage: YAMLStorage) -> None:
        payload = json.loads(await InspectService(storage=storage).inspect(
            reference="test", entity_type="datasource", compact=False, format="json",
        ))
        models = {m["name"]: m for m in payload["datasets"]}
        assert _listed(models["orders"]) == ON_ORDERS
        assert "saved_queries" not in models["untouched"]
        assert "hidden_q" not in models

    async def test_datasource_collection_json(self, storage: YAMLStorage) -> None:
        payload = json.loads(await InspectService(storage=storage).inspect(
            reference=None, entity_type="datasource", compact=False, format="json",
        ))
        (entry,) = payload["datasources"]
        models = {m["name"]: m for m in entry["datasets"]}
        assert _listed(models["orders"]) == ON_ORDERS


class TestSearch:
    async def test_search_finds_saved_query_by_description(self, storage: YAMLStorage) -> None:
        response = await SearchService(storage=storage).search(question="paid revenue per calendar month")
        assert "test.monthly_revenue" in {hit.id for hit in response.results}
