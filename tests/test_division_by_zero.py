"""A division whose divisor is 0 is NULL on every backend, never an error, an infinity or NaN."""

from __future__ import annotations

import sqlglot
import pytest
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import SQLGLOT_NAMES

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import _engine_generate, seeded_exec_engine

ORDERS = TableSpec(
    name="orders", columns=[("id", "INT"), ("amount", "DOUBLE"), ("status", "TEXT")],
    rows=[(1, 10.0, "ok"), (2, 20.0, "ok")],
)


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
    )


def _empty_population_ratios() -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "orders",
        "measures": [
            {"formula": "sum(amount) / count(*)", "name": "avg_amount"},
            {"formula": "count(id) / count(*)", "name": "share"},
        ],
        "filters": ["status = 'none'"],
    })


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
async def test_a_ratio_over_an_empty_population_is_null(backend: str) -> None:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [ORDERS]), models=[_orders()],
    ) as (engine, _db):
        resp = await engine.execute(_empty_population_ratios())
    assert [(r["orders.avg_amount"], r["orders.share"]) for r in resp.data] == [(None, None)]


@pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
async def test_every_dialect_guards_the_divisor(dialect: str) -> None:
    sql = await _engine_generate(query=_empty_population_ratios(), model=_orders(), dialect=dialect)
    divisions = list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Div))
    assert divisions, sql
    assert all(
        isinstance(d.expression, exp.Nullif) or (
            isinstance(d.expression, exp.Anonymous) and d.expression.name.upper() == "NULLIF"
        )
        for d in divisions
    ), sql


async def test_a_nonzero_literal_divisor_is_not_guarded() -> None:
    query = SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "sum(amount) / 2", "name": "half"}]})
    sql = await _engine_generate(query=query, model=_orders(), dialect="postgres")
    assert "NULLIF" not in sql.upper(), sql
