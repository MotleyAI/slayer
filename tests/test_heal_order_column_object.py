"""Order column objects carrying an expression; the advertised order schema (specs: queries/positions, mcp/tool-surface)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import ValidationError

from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._query_heal_fixtures import BACKENDS, assert_equivalent, heal_engine, table
from tests._saved_query_refinement_fixtures import refine_engine

SUM_DESC_OBJECT = {"column": {"name": "sum(amount)"}, "direction": "desc"}
SUM_DESC_STRING = {"column": "sum(amount)", "direction": "desc"}


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with heal_engine(request.param) as eng:
        yield eng


def by_region(order: list[dict[str, Any]]) -> dict[str, Any]:
    return {"source_model": "orders", "dimensions": ["region"], "measures": ["count(*)"], "order": order}


class TestOrderItemCoercion:
    @pytest.mark.parametrize("expr", ["sum(amount)", "amount:sum", "sum(amount) / count(*)", "rank(sum(amount), direction='desc')"])
    def test_object_equals_string_form(self, expr: str) -> None:
        healed = SlayerQuery.model_validate(by_region([{"column": {"name": expr}}]))
        canonical = SlayerQuery.model_validate(by_region([{"column": expr}]))
        assert healed.model_dump() == canonical.model_dump()

    def test_refinement_object_equals_string_form(self) -> None:
        healed = QueryRefinement.model_validate({"order": [SUM_DESC_OBJECT]})
        canonical = QueryRefinement.model_validate({"order": [SUM_DESC_STRING]})
        assert healed.model_dump() == canonical.model_dump()

    @pytest.mark.parametrize("column", [
        {"name": "sum(amount)", "model": "orders"}, {"name": "sum(amount)", "label": "Total"}, {"name": "bad name"},
    ])
    def test_object_with_other_keys_or_no_expression_keeps_error(self, column: dict[str, Any]) -> None:
        with pytest.raises(ValidationError, match="Invalid name"):
            SlayerQuery.model_validate(by_region([{"column": column}]))


class TestExecuted:
    async def test_expression_in_object_form(self, engine: SlayerQueryEngine) -> None:
        resp = await assert_equivalent(engine, by_region([SUM_DESC_OBJECT]), by_region([SUM_DESC_STRING]), ordered=True)
        assert [r["orders.region"] for r in resp.data] == ["EU", "US", None]

    async def test_stored_raw_formula_round_trips(self, engine: SlayerQueryEngine) -> None:
        stored = SlayerQuery.model_validate(by_region([SUM_DESC_STRING])).model_dump(mode="json")
        assert stored["order"][0]["raw_formula"] == "sum(amount)"
        reloaded = SlayerQuery.model_validate(stored)
        assert reloaded.model_dump(mode="json") == stored
        await assert_equivalent(engine, reloaded, by_region([SUM_DESC_STRING]), ordered=True)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_refinement_order_object(backend: str) -> None:
    async with refine_engine(backend) as (eng, _):
        healed = await eng.execute("monthly_revenue", refine={"order": [{"column": {"name": "sum(amount)"}}]})
        expected = await eng.execute("monthly_revenue", refine={"order": [{"column": "sum(amount)"}]})
    assert healed.columns == expected.columns
    assert table(healed, ordered=True) == table(expected, ordered=True)
    assert healed.sql == expected.sql


class TestAdvertisedSchema:
    @pytest.fixture
    async def order_item_schema(self, tmp_path) -> dict[str, Any]:
        server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False)
        tools = {t.name: t for t in await server.list_tools()}
        return tools["query"].inputSchema["$defs"]["OrderItem"]

    def test_column_accepts_string_or_object(self, order_item_schema: dict[str, Any]) -> None:
        column = order_item_schema["properties"]["column"]
        branches = column.get("anyOf") or column.get("oneOf") or [column]
        assert any(b.get("type") == "string" for b in branches), column
        assert any(b.get("$ref", "").endswith("/ColumnRef") for b in branches), column

    def test_raw_formula_not_published(self, order_item_schema: dict[str, Any]) -> None:
        assert "raw_formula" not in order_item_schema["properties"]

    def test_raw_formula_still_accepted_on_input(self) -> None:
        query = SlayerQuery.model_validate(by_region([{"column": "sum(amount)", "raw_formula": "sum(amount)"}]))
        assert (query.order or [])[0].raw_formula == "sum(amount)"
