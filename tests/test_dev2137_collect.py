"""``collect``: the distinct sorted list of a value's non-NULL inputs per cell (spec: aggregations/collect)."""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from mcp.types import TextContent

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType
from slayer.core.errors import SlayerError
from slayer.core.models import Aggregation, DatasetField
from slayer.mcp.server import create_mcp_server
from tests._dev2120_fixtures import NATIVE_LIST_DIALECTS, contractors_dataset, emit_contractors, stored_contractors
from tests._dev2137_fixtures import all_datasets, as_list, by, list_engine, metric, query


@pytest.fixture(scope="module")
async def engine():
    async with list_engine() as e:
        yield e


async def _collected(engine, expression: str, *, by_key: str = "contractors.team", **body: Any) -> dict[Any, Any]:
    resp = await engine.execute(query(dimensions=[by_key], metrics=[metric(expression, "c")], **body))
    return {k: as_list(v) for k, v in by(resp, by_key, "c").items()}


class TestValues:
    async def test_values_per_team(self, engine) -> None:
        assert await _collected(engine, "collect(city)") == {"A": ["Oslo"], "B": ["Oslo", "Rome"]}

    async def test_elements_per_team(self, engine) -> None:
        assert await _collected(engine, "collect(skills.value)") == {"A": ["go", "sql"], "B": ["go"]}

    async def test_all_inputs_null(self, engine) -> None:
        resp = await engine.execute(query(metrics=[metric("collect(contractors.city)", "c")],
                                          filter=["contractors.team = 'A' AND contractors.city IS NULL"]))
        assert as_list(resp.data[0]["c"]) == []

    async def test_a_cell_without_rows_is_empty(self, engine) -> None:
        got = await _collected(engine, "collect(contractors.skills.value)", by_key="contractors.name")
        assert got == {"Ann": ["go", "sql"], "Bob": ["sql"], "Cid": [], "Dee": [], "Eve": ["go"]}

    async def test_integers_sorted_numerically(self, engine) -> None:
        assert await _collected(engine, "collect(rate)") == {"A": [10, 20], "B": [30, 40, 50]}

    async def test_scalar_expression(self, engine) -> None:
        assert await _collected(engine, "collect(upper(city))") == {"A": ["OSLO"], "B": ["OSLO", "ROME"]}

    async def test_boolean_expression(self, engine) -> None:
        got = await _collected(engine, "collect(contractors.rate > 25)")
        assert {k: [bool(x) for x in v] for k, v in got.items()} == {"A": [False], "B": [True]}


def _typed_dataset(type_: DataType) -> Any:
    base = contractors_dataset()
    extra = DatasetField(name="x", sql="id", datatype=type_)
    return base.model_copy(update={"fields": [*base.fields, extra]})


class TestEligibility:
    def test_is_a_builtin(self) -> None:
        assert "collect" in BUILTIN_AGGREGATIONS
        assert Aggregation(name="collect").expression is None

    @pytest.mark.parametrize("type_", ["String", "Integer", "Float", "Boolean", "Date", "DateTime"])
    async def test_eligible_on_scalar_columns(self, type_: str) -> None:
        await emit_contractors(query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.x)", "c")]),
                               dialect="duckdb", datasets=[_typed_dataset(DataType(type_))])

    async def test_collect_of_a_list(self, engine) -> None:
        q = query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.skills)", "c")])
        with pytest.raises(SlayerError, match=r"collect\(skills\.value\)"):
            await engine.execute(q)

    async def test_collect_of_an_opaque_column(self) -> None:
        q = query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.x)", "c")])
        models = [_typed_dataset(DataType.OPAQUE)]
        with pytest.raises(SlayerError):
            await emit_contractors(q, dialect="duckdb", datasets=models)


class TestListResults:
    async def test_filtering_on_a_collected_list(self, engine) -> None:
        got = await _collected(engine, "collect(contractors.skills.value)", filter=["contains(collect(contractors.skills.value), 'sql')"])
        assert set(got) == {"A"}

    async def test_list_function_in_a_formula(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["contractors.team"], metrics=[
            metric("length(collect(contractors.skills.value))", "n")]))
        assert {r["contractors.team"]: int(r["n"]) for r in resp.data} == {"A": 2, "B": 1}

    async def test_collect_as_a_dimension(self, engine) -> None:
        q = query(dimensions=[{"field": "collect(contractors.city, partition_by=contractors.team)", "alias": "c"}])
        resp = await engine.execute(q)
        teams = {r["team"] for r in stored_contractors()}
        expected = {tuple(sorted({r["city"] for r in stored_contractors() if r["team"] == t and r["city"] is not None}))
                    for t in teams}
        assert {tuple(as_list(r["c"]) or ()) for r in resp.data} == expected

    async def test_json_result(self, engine) -> None:
        server = create_mcp_server(storage=engine.storage)
        blocks, _ = await server.call_tool(name="query", arguments={
            "query": {"population": "contractors", "dimensions": ["contractors.team"],
                      "metrics": [{"metric": "collect(contractors.city)", "alias": "c"}]},
            "format": "json"})
        block = next(iter(blocks))
        assert isinstance(block, TextContent), block
        decoded = json.loads(block.text)
        rows = decoded["data"] if isinstance(decoded, dict) else decoded
        assert {r["contractors.team"]: r["c"] for r in rows} == {"A": ["Oslo"], "B": ["Oslo", "Rome"]}

    async def test_markdown_cell_is_json_text(self, engine) -> None:
        resp = await engine.execute(query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.city)", "c")]))
        assert '["Oslo", "Rome"]' in resp.to_markdown() or '["Oslo","Rome"]' in resp.to_markdown()


#: Each dialect's lowering marker (design D7).
LOWERING = {"bigquery": "IGNORE NULLS", "snowflake": "WITHIN GROUP", "clickhouse": "GROUPUNIQARRAY",
            "trino": "ARRAY_SORT", "postgres": "ARRAY_AGG", "duckdb": "DISTINCT"}


class TestEmission:
    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    async def test_lowering(self, dialect: str) -> None:
        sql = await emit_contractors(query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.city)", "c")]),
                                     dialect=dialect)
        assert LOWERING[dialect] in sql.upper(), sql

    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    async def test_empty_result_is_an_empty_list(self, dialect: str) -> None:
        sql = await emit_contractors(query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.city)", "c")]),
                                     dialect=dialect)
        assert "NULLIF" not in sql.upper(), sql
        if dialect in ("postgres", "duckdb", "bigquery", "trino"):
            assert "COALESCE" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "trino"])
    async def test_null_inputs_filtered(self, dialect: str) -> None:
        sql = await emit_contractors(query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.city)", "c")]),
                                     dialect=dialect)
        assert "IS NOT NULL" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    async def test_elements_per_team_emit(self, dialect: str) -> None:
        await emit_contractors(query(dimensions=["contractors.team"], metrics=[metric("collect(contractors.skills.value)", "c")]),
                               dialect=dialect, datasets=all_datasets())


_TYPED_EMPTY = (r"CAST\(\s*(ARRAY\s*)?\[\]\s+AS\b", r"ARRAY<\w+>\s*\[\]")


class TestJoinBackOfAPrimaryKey:
    @pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
    async def test_join_back_fallback_is_untyped(self, dialect: str) -> None:
        sql = await emit_contractors(query(dimensions=["contractors.name"], metrics=[metric("collect(assignments.id)", "c")]),
                                     dialect=dialect, datasets=all_datasets())
        assert not [p for p in _TYPED_EMPTY if re.search(p, sql, re.I)], sql

    async def test_join_back_without_a_row_is_empty(self, engine) -> None:
        got = await _collected(engine, "collect(assignments.id)", by_key="contractors.name")
        assert {k: [int(x) for x in v] for k, v in got.items()} == {
            "Ann": [1, 2], "Bob": [3], "Cid": [4], "Dee": [], "Eve": [5]}
