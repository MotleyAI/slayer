"""Ossie's DAYOFWEEK and MILLISECOND parts, TIMESTAMP_NTZ literals, TO_DATE and TO_TIMESTAMP (spec:
queries/date-functions)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime
from typing import Any

import pytest

from slayer.core.errors import UnknownFunctionError
from slayer.core.models import Aggregation
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2120_fixtures import assert_cell
from tests._ossie_functions_fixtures import (
    BACKENDS, assert_twins, by_id, emit_fn, fn_dataset, fn_body, fn_engine, fn_query, ids, named, refusal,
)
from tests._ossie_gate import TIER1


@pytest.fixture(scope="module", params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with fn_engine(request.param) as e:
        yield e


def ints(values: dict[int, Any]) -> dict[int, Any]:
    return {k: None if v is None else int(v) for k, v in values.items()}


MILLISECONDS = {1: 123, 2: 1, 3: 999, 4: 0, 5: 500, 6: 0}
SECONDS = {1: 42, 2: 42, 3: 42, 4: 42, 5: 40, 6: 0}
#: ISO weekday per row: 2024-12-30 is a Monday, 2024-12-29 and 1969-07-20 Sundays.
WEEKDAYS = {1: 1, 2: 1, 3: 1, 4: 1, 5: 7, 6: 7}
DAYOFWEEK_SPELLINGS = ["EXTRACT(DAYOFWEEK FROM fn.created_at)", "DATE_PART('dayofweek', fn.created_at)",
                       "date_part(DAYOFWEEK, fn.created_at)", "extract(dayofweek from fn.created_at)"]
MILLISECOND_SPELLINGS = ["EXTRACT(MILLISECOND FROM fn.created_at)", "DATE_PART('MILLISECOND', fn.created_at)",
                         "date_part(millisecond, fn.created_at)"]


class TestMillisecond:
    async def test_millisecond_is_the_truncated_component(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query(named("date_part('millisecond', fn.created_at)", "ms"),
                                             named("date_part('second', fn.created_at)", "s")))
        assert ints(by_id(resp, "ms")) == MILLISECONDS
        assert ints(by_id(resp, "s")) == SECONDS

    async def test_millisecond_of_a_date(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("date_part('millisecond', fn.order_date)"))
        assert ints(by_id(resp)) == dict.fromkeys(range(1, 7), 0)

    @pytest.mark.parametrize("written", MILLISECOND_SPELLINGS)
    async def test_spelling_is_its_twin(self, engine: SlayerQueryEngine, written: str) -> None:
        await assert_twins(engine, written, "date_part('millisecond', fn.created_at)")

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("column", ["created_at", "order_date"])
    async def test_emits_on_every_dialect(self, dialect: str, column: str) -> None:
        await emit_fn(fn_query(f"EXTRACT(MILLISECOND FROM fn.{column})"), dialect=dialect)

    async def test_millisecond_function_refused(self, engine: SlayerQueryEngine) -> None:
        error = await refusal(engine, fn_body("MILLISECOND(fn.created_at)"))
        for remedy in ("EXTRACT(MILLISECOND FROM x)", "date_part('millisecond', x)"):
            assert remedy in str(error), str(error)

    @pytest.mark.parametrize("expr", ["date_add(fn.created_at, 5, 'millisecond')",
                                      "date_diff('millisecond', fn.created_at, fn.shipped_at)",
                                      "date_trunc('millisecond', fn.created_at)"])
    async def test_not_a_unit(self, engine: SlayerQueryEngine, expr: str) -> None:
        await refusal(engine, fn_body(expr))


class TestDayOfWeek:
    async def test_values_are_iso(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query("EXTRACT(DAYOFWEEK FROM fn.created_at)"))
        assert ints(by_id(resp)) == WEEKDAYS

    @pytest.mark.parametrize("written", DAYOFWEEK_SPELLINGS)
    async def test_spelling_is_its_twin(self, engine: SlayerQueryEngine, written: str) -> None:
        await assert_twins(engine, written, "date_part('day_of_week', fn.created_at)")

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("written", DAYOFWEEK_SPELLINGS)
    async def test_emits_its_twin_sql(self, written: str, dialect: str) -> None:
        twin = await emit_fn(fn_query("date_part('day_of_week', fn.created_at)"), dialect=dialect)
        assert await emit_fn(fn_query(written), dialect=dialect) == twin

    async def test_extract_part_in_a_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query(filter=["extract(dayofweek from fn.created_at) = 1"]))
        assert ids(resp) == {1, 2, 3, 4}

    @pytest.mark.parametrize("name", ["DAYOFWEEK", "dayofweek"])
    async def test_function_form_refused(self, engine: SlayerQueryEngine, name: str) -> None:
        error = await refusal(engine, fn_body(f"{name}(fn.created_at)"))
        for remedy in ("EXTRACT(DAYOFWEEK FROM x)", "date_part('day_of_week', x)", "date_part('day_of_week', x) % 7"):
            assert remedy in str(error), str(error)

    @pytest.mark.parametrize("expr", ["extract(dow from fn.created_at)", "date_part('weekday', fn.created_at)"])
    async def test_dow_and_weekday_parts_stay_refused(self, engine: SlayerQueryEngine, expr: str) -> None:
        error = await refusal(engine, fn_body(filter=[f"{expr} = 1"]))
        assert "date_part('day_of_week', x) % 7" in str(error), str(error)


class TestPartOnlyNamesAsAggregations:
    @pytest.mark.parametrize("name", ["millisecond", "dayofweek"])
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_model_aggregation_wins(self, backend: str, name: str) -> None:
        dataset = fn_dataset(aggregations=[Aggregation(name=name, expression="MAX({value})")])
        async with fn_engine(backend, datasets=[dataset]) as eng:
            resp = await eng.execute({"population": "fn", "metrics": [{"metric": f"{name}(fn.amount)", "alias": "m"}]})
        assert [float(r["m"]) for r in resp.data] == [30.0]


#: TIMESTAMP_NTZ literal → its TIMESTAMP spelling and value.
NTZ = {
    "TIMESTAMP_NTZ '2024-01-15 10:30:00'": ("TIMESTAMP '2024-01-15 10:30:00'", datetime(2024, 1, 15, 10, 30)),
    "timestamp_ntz '2024-01-15'": ("TIMESTAMP '2024-01-15'", datetime(2024, 1, 15)),
    "TIMESTAMP_NTZ '2024-01-15 10:30:00.25'": ("TIMESTAMP '2024-01-15 10:30:00.25'",
                                               datetime(2024, 1, 15, 10, 30, 0, 250000)),
}


class TestTimestampNtzLiteral:
    @pytest.mark.parametrize("written", sorted(NTZ))
    async def test_value_and_twin(self, engine: SlayerQueryEngine, written: str) -> None:
        twin, value = NTZ[written]
        resp = await assert_twins(engine, written, twin)
        (column,) = [c for c in resp.columns if c != "fn.id"]
        for row in resp.data:
            assert_cell(row[column], value, where=written)

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("written", sorted(NTZ))
    async def test_emits_its_twin_sql(self, written: str, dialect: str) -> None:
        twin = await emit_fn(fn_query(NTZ[written][0]), dialect=dialect)
        assert await emit_fn(fn_query(written), dialect=dialect) == twin

    async def test_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(fn_query(filter=["fn.created_at >= TIMESTAMP_NTZ '2024-12-30 10:15:42.5'"]))
        assert ids(resp) == {3}

    @pytest.mark.parametrize("text", ["'2024-13-01 00:00'", "'soon'"])
    async def test_malformed_literal(self, engine: SlayerQueryEngine, text: str) -> None:
        ntz = await refusal(engine, fn_body(filter=[f"fn.created_at >= TIMESTAMP_NTZ {text}"]))
        plain = await refusal(engine, fn_body(filter=[f"fn.created_at >= TIMESTAMP {text}"]))
        assert type(ntz) is type(plain), (type(ntz), type(plain))
        assert text.strip("'") in str(ntz), str(ntz)


#: Ossie parser → its CAST twin and value.
PARSERS = {
    "TO_DATE('2024-01-15')": ("CAST('2024-01-15' AS DATE)", date(2024, 1, 15)),
    "to_date('2024-01-15')": ("CAST('2024-01-15' AS DATE)", date(2024, 1, 15)),
    "TO_TIMESTAMP('2024-01-15 10:30:00')": ("CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)",
                                            datetime(2024, 1, 15, 10, 30)),
}


class TestToDateAndTimestamp:
    @pytest.mark.parametrize("written", sorted(PARSERS))
    async def test_value_and_twin(self, engine: SlayerQueryEngine, written: str) -> None:
        twin, value = PARSERS[written]
        resp = await assert_twins(engine, written, twin)
        (column,) = [c for c in resp.columns if c != "fn.id"]
        for row in resp.data:
            assert_cell(row[column], value, where=written)

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("written", sorted(PARSERS))
    async def test_emits_its_twin_sql(self, written: str, dialect: str) -> None:
        twin = await emit_fn(fn_query(PARSERS[written][0]), dialect=dialect)
        assert await emit_fn(fn_query(written), dialect=dialect) == twin

    @pytest.mark.parametrize("expr", ["TO_DATE(fn.code, 'YYYY-MM-DD')", "TO_TIMESTAMP(fn.code, 'YYYY-MM-DD HH24:MI:SS')",
                                      "TO_DATE()", "TO_TIMESTAMP('a', 'b', 'c')"])
    async def test_other_forms_are_unknown_functions(self, engine: SlayerQueryEngine, expr: str) -> None:
        error = await refusal(engine, fn_body(expr))
        assert isinstance(error, UnknownFunctionError), (type(error), str(error))
        assert expr.split("(", 1)[0].lower() in str(error).lower(), str(error)

    @pytest.mark.parametrize("name", ["to_date", "to_timestamp"])
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_model_aggregation_wins(self, backend: str, name: str) -> None:
        dataset = fn_dataset(aggregations=[Aggregation(name=name, expression="MAX({value})")])
        async with fn_engine(backend, datasets=[dataset]) as eng:
            resp = await eng.execute({"population": "fn", "metrics": [{"metric": f"{name}(fn.amount)", "alias": "m"}]})
        assert [float(r["m"]) for r in resp.data] == [30.0]
