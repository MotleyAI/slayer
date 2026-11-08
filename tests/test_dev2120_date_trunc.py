"""``date_trunc(unit, x)`` is the granularity call ``<unit>(x)`` (spec: queries/date-functions)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import UnknownFunctionError, UnknownGranularityError
from slayer.core.models import Aggregation
from slayer.core.query import SlayerQuery
from slayer.core.syntax import parse_expr
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2120_fixtures import EXEC_BACKENDS, emit_lib, lib_engine, lib_ids, lib_query, lib_values


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


def _q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "lib", **kw})


class TestParse:
    @pytest.mark.parametrize("unit", ["second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year"])
    def test_is_the_granularity_call(self, unit: str) -> None:
        assert parse_expr(f"date_trunc('{unit}', ts)") == parse_expr(f"{unit}(ts)")

    def test_unit_case_insensitive(self) -> None:
        assert parse_expr("DATE_TRUNC('MONTH', ts)") == parse_expr("month(ts)")

    @pytest.mark.parametrize("text", ["date_trunc('month')", "date_trunc('month', ts, 1)", "date_trunc(1, ts)"])
    def test_malformed_rejected(self, text: str) -> None:
        with pytest.raises(ValueError):
            parse_expr(text)

    async def test_malformed_over_a_column_rejected_at_binding(self, engine: SlayerQueryEngine) -> None:
        query = lib_query("date_trunc(word, ts)")
        with pytest.raises(ValueError, match="date_trunc"):
            await engine.execute(query)

    def test_shadowable(self) -> None:
        assert Aggregation(name="date_trunc", formula="SUM({value})").name == "date_trunc"


class TestPositions:
    async def test_row_value_filter(self, engine: SlayerQueryEngine) -> None:
        trunc = lib_ids(await engine.execute(lib_query(filters=["date_trunc('month', ts) = '2025-03'"])))
        gran = lib_ids(await engine.execute(lib_query(filters=["month(ts) = '2025-03'"])))
        assert trunc == gran == {1, 2, 7, 10}

    async def test_time_dimension(self, engine: SlayerQueryEngine) -> None:
        trunc = await engine.execute(_q(dimensions=["date_trunc('month', ts)"]))
        gran = await engine.execute(_q(dimensions=["month(ts)"]))
        assert trunc.columns == gran.columns
        assert trunc.sql == gran.sql
        assert sorted(map(repr, trunc.data)) == sorted(map(repr, gran.data))  # DuckDB GROUP BY order is arbitrary

    async def test_order_key(self, engine: SlayerQueryEngine) -> None:
        order = [{"column": "date_trunc('month', ts)", "direction": "desc"}]
        trunc = await engine.execute(_q(dimensions=["month(ts)"], order=order))
        gran = await engine.execute(_q(dimensions=["month(ts)"], order=[{"column": "month(ts)", "direction": "desc"}]))
        assert trunc.sql == gran.sql

    async def test_aggregation_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_q(measures=[{"formula": "max(date_trunc('month', ts))", "name": "v"}]))
        assert str(resp.data[0]["lib.v"])[:10] == "2025-06-01"

    async def test_arithmetic(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("date_diff('day', date_trunc('month', ts), ts)",
                                                           filters=["id IN (1, 2)"])))
        assert {k: int(v) for k, v in values.items()} == {1: 4, 2: 30}

    async def test_same_sql_as_the_granularity_call(self) -> None:
        trunc = await emit_lib(lib_query("date_trunc('quarter', ts)"), dialect="postgres")
        gran = await emit_lib(lib_query("quarter(ts)"), dialect="postgres")
        assert trunc == gran


class TestUnknownUnit:
    async def test_dimension_entry(self, engine: SlayerQueryEngine) -> None:
        query = _q(dimensions=["date_trunc('fortnight', ts)"])
        with pytest.raises(UnknownGranularityError):
            await engine.execute(query)

    @pytest.mark.parametrize("unit", ["doy", "dow", "weekday", "dayofweek", "isodow"])
    def test_date_part_names_are_not_units(self, unit: str) -> None:
        with pytest.raises(ValueError, match="unknown date_trunc"):
            parse_expr(f"date_trunc('{unit}', ts)")

    async def test_expression_raises_the_granularity_calls_error(self, engine: SlayerQueryEngine) -> None:
        query = lib_query("date_trunc('fortnight', ts)")
        with pytest.raises(UnknownFunctionError) as trunc:
            await engine.execute(query)
        query = lib_query("fortnight(ts)")
        with pytest.raises(UnknownFunctionError) as gran:
            await engine.execute(query)
        assert isinstance(trunc.value, (UnknownGranularityError, type(gran.value))), trunc.value
