"""TRY_CAST to an integer type accepts only integer text (spec: queries/casts › TRY_CAST turns invalid input into NULL)."""

from __future__ import annotations

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._cast_fixtures import TRY_CAST_CASE_IDS, TRY_CAST_TARGET_CASES, check_try_cast_targets
from tests._dev2101_fixtures import EXEC_BACKENDS, TIER1, by_key, emit, m, make_exec_engine, q, single


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _value(value, *, target: str):
    if value is None:
        return None
    return float(value) if target == "DOUBLE" else int(value)


class TestIntegerRule:
    async def test_every_case_in_one_query(self, engine: SlayerQueryEngine) -> None:
        await check_try_cast_targets(engine, model="orders")

    @pytest.mark.parametrize(("source", "target", "expected"), TRY_CAST_TARGET_CASES, ids=TRY_CAST_CASE_IDS)
    async def test_case(self, engine: SlayerQueryEngine, source: str, target: str, expected) -> None:
        resp = await engine.execute(q(measures=[m(f"max(TRY_CAST({source} AS {target}))")]))
        assert _value(single(resp), target=target) == expected, resp.sql

    async def test_fractional_double_column(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": "TRY_CAST(price / 2 AS INT)", "name": "x"}]))
        got = {int(k): (None if v is None else int(v)) for k, v in by_key(resp, "id", "x").items()}
        assert got == {1: 1, 2: None, 3: 2, 4: None, 5: None}, resp.sql

    async def test_rounding_is_the_author_s_choice(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("max(TRY_CAST(round(CAST(1.6 AS DOUBLE)) AS INT))")]))
        assert _value(single(resp), target="INT") == 2, resp.sql


class TestEmission:
    @pytest.mark.parametrize("source", ["code", "CAST(1.5 AS DOUBLE)", "price / 2"])
    @pytest.mark.parametrize("target", ["INT", "SMALLINT", "BIGINT", "INTEGER"])
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_integer_targets_emit_everywhere(self, dialect: str, target: str, source: str) -> None:
        await emit(q(dimensions=["id", {"expression": f"TRY_CAST({source} AS {target})", "name": "x"}]),
                   dialect=dialect)
