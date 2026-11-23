"""CAST and TRY_CAST have one meaning on every dialect (spec: queries/casts)."""

from __future__ import annotations

import re
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.engine.query_engine import SlayerQueryEngine
from tests._cast_fixtures import (
    CAST_MATRIX,
    CastCase,
    as_bool,
    assert_cast_value,
    casts_engine,
    check_author_casts,
    check_boolean_casts,
    check_casts_columns,
)
from tests._dev2101_fixtures import EXEC_BACKENDS, TIER1, by_key, emit, engine_for, m, make_exec_engine, q, single

#: Exact-decimal rows need more than a double, so SQLite runs every other row.
SQLITE_MATRIX = [c for c in CAST_MATRIX if not c.exact]


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(scope="module")
async def sqlite_module():
    async for e in engine_for("sqlite"):
        yield e


@pytest.fixture(scope="module")
async def duckdb_module():
    async for e in engine_for("duckdb"):
        yield e


@pytest.fixture(params=EXEC_BACKENDS)
async def casts(request):
    async for e in casts_engine(request.param):
        yield e


@pytest.fixture
async def sqlite_engine():
    async for e in engine_for("sqlite"):
        yield e


@pytest.fixture
async def duckdb_engine():
    async for e in engine_for("duckdb"):
        yield e


async def _check(engine: SlayerQueryEngine, case: CastCase) -> None:
    resp = await engine.execute(q(metrics=[m(f"max({case.expr})")]))
    assert_cast_value(single(resp), case.expected, where=f"{case.expr}\n{resp.sql}")


class TestMatrix:
    @pytest.mark.parametrize("case", SQLITE_MATRIX, ids=[c.expr for c in SQLITE_MATRIX])
    async def test_sqlite(self, sqlite_module: SlayerQueryEngine, case: CastCase) -> None:
        await _check(sqlite_module, case)

    @pytest.mark.parametrize("case", CAST_MATRIX, ids=[c.expr for c in CAST_MATRIX])
    async def test_duckdb(self, duckdb_module: SlayerQueryEngine, case: CastCase) -> None:
        await _check(duckdb_module, case)

    async def test_every_case_in_one_query_on_sqlite(self, sqlite_module: SlayerQueryEngine) -> None:
        await check_author_casts(sqlite_module, dataset="orders", exact_decimals=False)

    async def test_every_case_in_one_query_on_duckdb(self, duckdb_module: SlayerQueryEngine) -> None:
        await check_author_casts(duckdb_module, dataset="orders")

    async def test_boolean_over_a_column(self, engine: SlayerQueryEngine) -> None:
        await check_boolean_casts(engine, dataset="orders")

    async def test_columns(self, casts: SlayerQueryEngine) -> None:
        await check_casts_columns(casts)


class TestSqliteCasts:
    """SQLite casts to boolean, decimal and varchar types."""

    @pytest.mark.parametrize(("expr", "expected"), [
        ("CAST('true' AS BOOLEAN)", 1), ("CAST(' No ' AS BOOLEAN)", 0), ("CAST('abc' AS BOOLEAN)", None),
        ("CAST('T' AS BOOLEAN)", 1), ("CAST('yes' AS BOOLEAN)", 1), ("CAST('0' AS BOOLEAN)", 0),
        ("CAST('12abc' AS BOOLEAN)", None), ("CAST(5 AS BOOLEAN)", 1), ("CAST(-1 AS BOOLEAN)", 1),
        ("CAST(12345.6 AS DECIMAL(3,1))", None), ("CAST('12abc' AS DECIMAL(10,2))", None),
        ("CAST('1e' AS DECIMAL(5,1))", None), ("CAST('12.345' AS DECIMAL(10,2))", 12.35),
        ("CAST('abcdef' AS VARCHAR(3))", "abc"),
    ])
    async def test_value(self, sqlite_engine: SlayerQueryEngine, expr: str, expected: Any) -> None:
        resp = await sqlite_engine.execute(q(metrics=[m(f"max({expr})")]))
        assert_cast_value(single(resp), expected, where=f"{expr}\n{resp.sql}")

    async def test_decimal_text_is_never_a_prefix(self, sqlite_engine: SlayerQueryEngine) -> None:
        resp = await sqlite_engine.execute(q(dimensions=["orders.id", {"field": "CAST(orders.code AS DECIMAL(10,2))", "alias": "x"}]))
        got = {int(k): (None if v is None else float(v)) for k, v in by_key(resp, "id", "x").items()}
        assert got == {1: 12.0, 2: None, 3: None, 4: 7.0, 5: None}, resp.sql

    async def test_boolean_text_column(self, sqlite_engine: SlayerQueryEngine) -> None:
        resp = await sqlite_engine.execute(q(dimensions=["orders.id", {"field": "CAST(orders.code AS BOOLEAN)", "alias": "x"}]))
        assert by_key(resp, "id", "x") == {1: None, 2: None, 3: None, 4: None, 5: None}, resp.sql


class TestNonFiniteDoubles:
    @pytest.mark.parametrize("text", ["NaN", "Infinity", "-Infinity"])
    @pytest.mark.parametrize("target", ["INT", "SMALLINT", "BIGINT"])
    async def test_try_cast_is_null(self, duckdb_engine: SlayerQueryEngine, text: str, target: str) -> None:
        resp = await duckdb_engine.execute(q(metrics=[m(f"max(TRY_CAST(CAST('{text}' AS DOUBLE) AS {target}))")]))
        assert single(resp) is None, resp.sql


def _sql(sql: str, dialect: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect=dialect)


class TestEmission:
    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("case", CAST_MATRIX, ids=[c.expr for c in CAST_MATRIX])
    async def test_emits_everywhere(self, case: CastCase, dialect: str) -> None:
        await emit(q(metrics=[m(f"max({case.expr})")]), dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("expr", [
        "CAST(orders.quantity AS BOOLEAN)", as_bool("CAST(orders.quantity - 2 AS BOOLEAN)"),
        "CAST(orders.amount AS INT)", "CAST(orders.amount / 3 AS DECIMAL(10,2))", "CAST(orders.amount AS DECIMAL)",
        "CAST(orders.status AS VARCHAR(3))", "TRY_CAST(orders.amount / 3 AS SMALLINT)", "TRY_CAST(orders.amount AS BOOLEAN)",
    ])
    async def test_column_forms_emit_everywhere(self, expr: str, dialect: str) -> None:
        await emit(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]), dialect=dialect)

    @pytest.mark.parametrize(("dialect", "spelling"), [
        ("snowflake", "VARCHAR(3)"), ("bigquery", "VARCHAR(3)"), ("bigquery", "STRING(3)"), ("snowflake", "STRING(3)"),
    ])
    async def test_varchar_length_keeps_the_first_characters(self, dialect: str, spelling: str) -> None:
        sql = await emit(q(dimensions=["orders.id", {"field": f"CAST(orders.status AS {spelling})", "alias": "x"}]),
                         dialect=dialect)
        assert re.search(r"\b(SUBSTR|SUBSTRING|LEFT)\(", sql, re.IGNORECASE), sql

    async def test_bigquery_bare_decimal_has_scale_zero(self) -> None:
        sql = await emit(q(dimensions=["orders.id", {"field": "CAST(orders.amount AS DECIMAL)", "alias": "x"}]),
                         dialect="bigquery")
        assert "BIGNUMERIC(38, 0)" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery"])
    async def test_decimal_scale_is_kept(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["orders.id", {"field": "CAST(orders.amount AS DECIMAL(10,2))", "alias": "x"}]),
                         dialect=dialect)
        casts = [c for c in _sql(sql, dialect).find_all(exp.Cast) if "amount" in c.this.sql().lower()]
        assert casts and all([e.sql() for e in c.to.expressions][-1:] == ["2"] for c in casts), sql
