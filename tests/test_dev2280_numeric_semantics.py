"""Division and rounding follow Ossie's numeric semantics (spec: queries/scalar-functions); the executed cases live
in the library matrix (``tests/_dev2120_fixtures.py`` › ``_numeric_semantics_cases``)."""

from __future__ import annotations

import math
from decimal import Decimal

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from tests._dev2120_fixtures import EXEC_BACKENDS, TIER1, emit_lib, lib_engine, lib_query, lib_values

_FLOATING = (exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT)


def _round_operands(sql: str, *, dialect: str) -> list[Expression]:
    return [node.this for node in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Round)]


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("expr", ["round(d, 0)", "round(e, 2)"])
async def test_decimal_round_keeps_its_exact_operand(dialect: str, expr: str) -> None:
    sql = await emit_lib(lib_query(expr), dialect=dialect)
    operands = _round_operands(sql, dialect=dialect)
    assert operands, sql
    floated = [o for o in operands if isinstance(o, exp.Cast) and o.to.this in _FLOATING]
    assert floated == [], sql


@pytest.mark.parametrize("expr", ["round(d, 0)", "round(e, 2)"])
async def test_decimal_round_returns_an_exact_value(expr: str) -> None:
    async with lib_engine("duckdb") as engine:
        got = lib_values(await engine.execute(lib_query(expr, filters=["id in (1, 2)"])))
    assert all(isinstance(v, Decimal) for v in got.values()), got


@pytest.mark.parametrize("backend", EXEC_BACKENDS)
async def test_float_ties_round_to_a_neighbour(backend: str) -> None:
    async with lib_engine(backend) as engine:
        got = lib_values(await engine.execute(lib_query("round(x, 0)", filters=["id in (1, 7)"])))
    assert float(got[1]) in (0.0, 1.0)
    assert float(got[7]) in (-2.0, -1.0)
    assert all(math.isfinite(float(v)) for v in got.values())
