"""CAST and TRY_CAST have one meaning on every dialect (spec: queries/casts)."""

from __future__ import annotations

import re
from typing import Any

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.core.errors import CastTypeError
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
        ("CAST('abcdef' AS VARCHAR(3))", "abc"), (f"CAST('{'1' * 30}' AS DECIMAL(38,0))", 1.111111111111111e29),
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


@pytest.mark.parametrize(("expr", "expected"), [
    ("TRY_CAST(json_value('{{\"v\": \"abcdef\"}}', '$.v') AS VARCHAR(3))", "abc"),
    ("TRY_CAST(json_value('{{\"v\": 12.345}}', '$.v') AS DECIMAL(10,2))", 12.35),
    ("TRY_CAST(json_value('{{\"v\": \"x\"}}', '$.v') AS DECIMAL(10,2))", None),
])
async def test_try_cast_of_json_value_applies_length_and_scale(engine: SlayerQueryEngine, expr: str,
                                                               expected: Any) -> None:
    resp = await engine.execute(q(metrics=[m(f"max({expr})")]))
    assert_cast_value(single(resp), expected, where=f"{expr}\n{resp.sql}")


class TestNonFiniteDoubles:
    @pytest.mark.parametrize("text", ["NaN", "Infinity", "-Infinity"])
    @pytest.mark.parametrize("target", ["INT", "SMALLINT", "BIGINT"])
    async def test_try_cast_is_null(self, duckdb_engine: SlayerQueryEngine, text: str, target: str) -> None:
        resp = await duckdb_engine.execute(q(metrics=[m(f"max(TRY_CAST(CAST('{text}' AS DOUBLE) AS {target}))")]))
        assert single(resp) is None, resp.sql


def _sql(sql: str, dialect: str) -> Expr:
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
        assert "CAST(ROUND(orders.amount, 0) AS BIGNUMERIC)" in sql, sql
        assert not re.search(r"\b(BIG)?NUMERIC\(", sql, re.IGNORECASE), sql

    async def test_snowflake_decimal_scale_is_kept(self) -> None:
        sql = await emit(q(dimensions=["orders.id", {"field": "CAST(orders.amount AS DECIMAL(10,2))", "alias": "x"}]),
                         dialect="snowflake")
        casts = [c for c in _sql(sql, "snowflake").find_all(exp.Cast) if "amount" in c.this.sql().lower()]
        assert casts, sql
        assert all([e.sql() for e in c.to.expressions][-1:] == ["2"] for c in casts), sql

    async def test_bigquery_decimal_literal_is_read_exactly(self) -> None:
        # A BigQuery number literal with a fraction is a FLOAT64.
        sql = await emit(q(dimensions=["orders.id", {"field": "CAST(9223372036854775806.5 AS DECIMAL(20,1))",
                                                     "alias": "x"}]), dialect="bigquery")
        assert "ROUND(CAST('9223372036854775806.5' AS BIGNUMERIC), 1)" in sql, sql

    async def test_bigquery_try_cast_literal_past_bignumeric_is_safe(self) -> None:
        sql = await emit(q(dimensions=["orders.id", {"field": "TRY_CAST(1e40 AS DECIMAL(38,0))", "alias": "x"}]),
                         dialect="bigquery")
        assert re.search(r"SAFE_CAST\(CAST\('1e\+?40' AS STRING\) AS BIGNUMERIC\)", sql, re.IGNORECASE), sql
        assert not re.search(r"(?<!SAFE_)CAST\('1e\+?40' AS BIGNUMERIC\)", sql, re.IGNORECASE), sql

    async def test_bigquery_decimal_scale_is_rounded_outside_the_cast(self) -> None:
        # BigQuery refuses a parameterized CAST target.
        sql = await emit(q(dimensions=["orders.id", {"field": "CAST(orders.amount AS DECIMAL(10,2))", "alias": "x"}]),
                         dialect="bigquery")
        assert "CAST(ROUND(orders.amount, 2) AS NUMERIC)" in sql, sql
        assert not re.search(r"\b(NUMERIC|STRING)\(", sql, re.IGNORECASE), sql

    @pytest.mark.parametrize("expr", [
        "TRY_CAST(orders.amount AS DECIMAL(10,2))", "CAST(orders.status AS VARCHAR(3))", "CAST('1.5' AS NUMERIC(38,0))",
    ])
    async def test_bigquery_cast_targets_take_no_parameters(self, expr: str) -> None:
        sql = await emit(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]), dialect="bigquery")
        assert not re.search(r"\b(NUMERIC|STRING)\(", sql, re.IGNORECASE), sql

    @pytest.mark.parametrize(("expr", "reader"), [
        ("CAST(orders.amount AS DECIMAL(10,2))", "'Decimal(38, 3)'"),
        ("CAST(orders.amount AS DECIMAL(38,2))", "'Decimal(76, 3)'"),
        ("CAST(orders.status AS DECIMAL(10,2))", "toDecimal128OrNull("),
        ("CAST(orders.status AS DECIMAL(38,38))", "toDecimal256OrNull("),
        ("CAST(orders.amount AS DECIMAL(75,74))", "'Decimal(76, 75)'"),
    ])
    async def test_clickhouse_rounding_intermediate_holds_the_target(self, expr: str, reader: str) -> None:
        # The guard digit needs precision + 1 digits; Decimal128 holds 38.
        sql = await emit(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]), dialect="clickhouse")
        assert reader in sql, sql

    @pytest.mark.parametrize(("dialect", "type_text"), [
        ("clickhouse", "DECIMAL(76,76)"), ("clickhouse", "DECIMAL(76,2)"),
        ("bigquery", "DECIMAL(40,39)"), ("bigquery", "DECIMAL(50,2)"),
    ])
    async def test_a_decimal_the_dialect_cannot_hold_is_refused(self, dialect: str, type_text: str) -> None:
        query = q(dimensions=["orders.id", {"field": f"CAST(orders.status AS {type_text})", "alias": "x"}])
        with pytest.raises(CastTypeError):
            await emit(query, dialect=dialect)

    @pytest.mark.parametrize("type_text", ["DECIMAL(76,38)", "DECIMAL(38,38)", "DECIMAL"])
    async def test_bigquery_accepts_what_bignumeric_holds(self, type_text: str) -> None:
        await emit(q(dimensions=["orders.id", {"field": f"CAST(orders.status AS {type_text})", "alias": "x"}]),
                   dialect="bigquery")

    @pytest.mark.parametrize("spelling", ["VARCHAR(3)", "NVARCHAR(3)"])
    async def test_sql_server_varchar_length_counts_code_points_in_nvarchar(self, spelling: str) -> None:
        # VARCHAR holds only the collation's code page and is sized in bytes under a UTF-8 one.
        sql = await emit(q(dimensions=["orders.id", {"field": f"CAST(orders.status AS {spelling})", "alias": "x"}]),
                         dialect="tsql")
        assert "COLLATE Latin1_General_100_CS_AS_SC, 1, 3) AS NVARCHAR(MAX))" in sql, sql
        assert not re.search(r"\bVARCHAR\(", sql), sql
