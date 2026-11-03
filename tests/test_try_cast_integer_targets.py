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

    @pytest.mark.parametrize("bound", ["-9223372036854775808", "9223372036854775807"])
    async def test_int64_bounds_of_an_integer(self, engine: SlayerQueryEngine, bound: str) -> None:
        resp = await engine.execute(q(measures=[m(f"max(TRY_CAST(CAST('{bound}' AS BIGINT) AS BIGINT))")]))
        assert _value(single(resp), target="BIGINT") == int(bound), resp.sql

    @pytest.mark.parametrize(("source", "expected"), [("2.0", 2), ("CAST(quantity AS INT) * 2.0", 8), ("quantity", 4)])
    async def test_number_that_is_not_integer_text(self, engine: SlayerQueryEngine, source: str, expected: int) -> None:
        resp = await engine.execute(q(measures=[m(f"max(TRY_CAST({source} AS INT))")]))
        assert _value(single(resp), target="INT") == expected, resp.sql


class TestJsonSource:
    @pytest.mark.parametrize(("json", "expected"), [
        ('{{"v": 32768}}', None), ('{{"v": "32768"}}', None), ('{{"v": 7}}', 7), ('{{"v": "7"}}', 7), ('{{"v": 1.5}}', None),
    ])
    async def test_narrow_target_range(self, engine: SlayerQueryEngine, json: str, expected) -> None:
        resp = await engine.execute(q(measures=[m(f"max(TRY_CAST(json_value('{json}', '$.v') AS SMALLINT))")]))
        assert _value(single(resp), target="SMALLINT") == expected, resp.sql


class TestBooleanSource:
    @pytest.mark.parametrize("target", ["INT", "SMALLINT", "DOUBLE"])
    async def test_reads_as_one_or_zero(self, engine: SlayerQueryEngine, target: str) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": f"TRY_CAST(amount > 15 AS {target})", "name": "x"}]))
        got = {int(k): _value(v, target=target) for k, v in by_key(resp, "id", "x").items()}
        assert got == {1: 0, 2: 1, 3: 1, 4: 1, 5: None}, resp.sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_emits_everywhere(self, dialect: str) -> None:
        await emit(q(dimensions=["id", {"expression": "TRY_CAST(amount > 15 AS INT)", "name": "x"}]), dialect=dialect)


class TestParameterisedTarget:
    async def test_invalid_text_is_null(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": "TRY_CAST(code AS DECIMAL(10,2))", "name": "x"}]))
        got = {int(k): (None if v is None else float(v)) for k, v in by_key(resp, "id", "x").items()}
        assert got == {1: 12.0, 2: None, 3: None, 4: 7.0, 5: None}, resp.sql


@pytest.fixture(params=["duckdb"])
async def duckdb_engine(request):
    async for e in make_exec_engine(request):
        yield e


class TestUnsignedTarget:
    @pytest.mark.parametrize(("source", "expected"), [("'1.5'", None), ("' 7 '", 7), ("'-1'", None), ("'1e3'", None)])
    async def test_integer_rule(self, duckdb_engine: SlayerQueryEngine, source: str, expected) -> None:
        resp = await duckdb_engine.execute(q(measures=[m(f"max(TRY_CAST({source} AS UBIGINT))")]))
        assert _value(single(resp), target="UBIGINT") == expected, resp.sql

    @pytest.mark.parametrize(("source", "target", "expected"), [
        ("CAST(1e19 AS DOUBLE)", "UBIGINT", 10 ** 19), ("CAST(2e19 AS DOUBLE)", "UBIGINT", None),
        ("CAST(-1 AS DOUBLE)", "UBIGINT", None), ("CAST(1e19 AS DOUBLE)", "HUGEINT", 10 ** 19),
    ])
    async def test_wide_target_of_a_number(self, duckdb_engine: SlayerQueryEngine, source: str, target: str,
                                           expected) -> None:
        resp = await duckdb_engine.execute(q(measures=[m(f"max(TRY_CAST({source} AS {target}))")]))
        assert _value(single(resp), target=target) == expected, resp.sql


class TestEmission:
    @pytest.mark.parametrize("source", ["code", "CAST(1.5 AS DOUBLE)", "price / 2"])
    @pytest.mark.parametrize("target", ["INT", "SMALLINT", "BIGINT", "INTEGER"])
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_integer_targets_emit_everywhere(self, dialect: str, target: str, source: str) -> None:
        await emit(q(dimensions=["id", {"expression": f"TRY_CAST({source} AS {target})", "name": "x"}]),
                   dialect=dialect)

    @pytest.mark.parametrize(("dialect", "target"), [
        ("mysql", "BIGINT UNSIGNED"), ("clickhouse", "UInt64"), ("clickhouse", "Int128"), ("clickhouse", "UInt256"),
        ("duckdb", "HUGEINT"),
    ])
    async def test_wide_target_of_a_number_casts_to_the_target(self, dialect: str, target: str) -> None:
        sql = await emit(q(dimensions=["id", {"expression": f"TRY_CAST(price / 2 AS {target})", "name": "x"}]),
                         dialect=dialect)
        assert "AS BIGINT" not in sql.upper(), sql
        assert "AS SIGNED" not in sql.upper(), sql
