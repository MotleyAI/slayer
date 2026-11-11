"""``date_trunc(unit, x)`` is the time bucket; ``<part>(x)`` is ``date_part`` (spec: queries/date-functions)."""

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


def _explicit(unit: str, **kw: Any) -> SlayerQuery:
    return _q(time_dimensions=[{"dimension": "ts", "granularity": unit}], **kw)


class TestParse:
    @pytest.mark.parametrize("unit", ["second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year"])
    def test_is_the_explicit_time_dimension(self, unit: str) -> None:
        trunc = _q(dimensions=[f"date_trunc('{unit}', ts)"])
        assert not trunc.dimensions
        assert trunc.time_dimensions == _explicit(unit).time_dimensions

    @pytest.mark.parametrize("part", ["second", "minute", "hour", "day", "week", "month", "quarter", "year"])
    def test_part_call_is_date_part(self, part: str) -> None:
        assert parse_expr(f"{part}(ts)") == parse_expr(f"date_part('{part}', ts)")

    def test_unit_case_insensitive(self) -> None:
        assert parse_expr("DATE_TRUNC('MONTH', ts)") == parse_expr("date_trunc('month', ts)")

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
        assert trunc == {1, 2, 7, 10}

    async def test_time_dimension(self, engine: SlayerQueryEngine) -> None:
        trunc = await engine.execute(_q(dimensions=["date_trunc('month', ts)"]))
        explicit = await engine.execute(_explicit("month"))
        assert trunc.columns == explicit.columns
        assert trunc.sql == explicit.sql
        assert sorted(map(repr, trunc.data)) == sorted(map(repr, explicit.data))  # DuckDB GROUP BY order is arbitrary

    async def test_order_key(self, engine: SlayerQueryEngine) -> None:
        trunc = await engine.execute(_q(dimensions=["date_trunc('month', ts)"],
                                        order=[{"column": "date_trunc('month', ts)", "direction": "desc"}]))
        explicit = await engine.execute(_explicit("month", order=[{"column": "ts", "direction": "desc"}]))
        assert trunc.sql == explicit.sql

    async def test_aggregation_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_q(measures=[{"formula": "max(date_trunc('month', ts))", "name": "v"}]))
        assert str(resp.data[0]["lib.v"])[:10] == "2025-06-01"

    async def test_arithmetic(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("date_diff('day', date_trunc('month', ts), ts)",
                                                           filters=["id IN (1, 2)"])))
        assert {k: int(v) for k, v in values.items()} == {1: 4, 2: 30}

    async def test_same_sql_as_the_explicit_time_dimension(self) -> None:
        trunc = await emit_lib(_q(dimensions=["date_trunc('quarter', ts)"]), dialect="postgres")
        explicit = await emit_lib(_explicit("quarter"), dialect="postgres")
        assert trunc == explicit


class TestUnknownUnit:
    async def test_dimension_entry(self, engine: SlayerQueryEngine) -> None:
        query = _q(dimensions=["date_trunc('fortnight', ts)"])
        with pytest.raises(UnknownGranularityError):
            await engine.execute(query)

    @pytest.mark.parametrize("unit", ["doy", "dow", "weekday", "dayofweek", "isodow"])
    def test_date_part_names_are_not_units(self, unit: str) -> None:
        with pytest.raises(ValueError, match="unknown date_trunc"):
            parse_expr(f"date_trunc('{unit}', ts)")

    async def test_expression_unknown_unit_is_an_unknown_granularity(self, engine: SlayerQueryEngine) -> None:
        trunc_query = lib_query("date_trunc('fortnight', ts)")
        with pytest.raises(UnknownGranularityError):
            await engine.execute(trunc_query)
        call_query = lib_query("fortnight(ts)")
        with pytest.raises(UnknownFunctionError):
            await engine.execute(call_query)
