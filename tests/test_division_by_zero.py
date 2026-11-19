"""A division whose divisor is 0 is NULL on every backend, never an error, an infinity or NaN."""

from __future__ import annotations

import sqlglot
import pytest
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import SQLGLOT_NAMES

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import _engine_generate, seeded_exec_engine

ORDERS = TableSpec(
    name="orders", columns=[("id", "INT"), ("amount", "DOUBLE"), ("status", "TEXT")],
    rows=[(1, 10.0, "ok"), (2, 20.0, "ok")],
)


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
        ],
    )


def _empty_population_ratios() -> SlayerQuery:
    return SlayerQuery.model_validate({
        "population": "orders",
        "metrics": [
            {"metric": "sum(amount) / count(*)", "alias": "avg_amount"},
            {"metric": "count(id) / count(*)", "alias": "share"},
        ],
        "filter": ["status = 'none'"],
    })


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
async def test_a_ratio_over_an_empty_population_is_null(backend: str) -> None:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [ORDERS]), datasets=[_orders()],
    ) as (engine, _db):
        resp = await engine.execute(_empty_population_ratios())
    assert [(r["orders.avg_amount"], r["orders.share"]) for r in resp.data] == [(None, None)]


@pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
async def test_every_dialect_guards_the_divisor(dialect: str) -> None:
    sql = await _engine_generate(query=_empty_population_ratios(), dataset=_orders(), dialect=dialect)
    divisions = list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Div))
    assert divisions, sql
    assert all(
        isinstance(d.expression, exp.Nullif) or (
            isinstance(d.expression, exp.Anonymous) and d.expression.name.upper() == "NULLIF"
        )
        for d in divisions
    ), sql


async def test_a_nonzero_literal_divisor_is_not_guarded() -> None:
    query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(amount) / 2", "alias": "half"}]})
    sql = await _engine_generate(query=query, dataset=_orders(), dialect="postgres")
    assert "NULLIF" not in sql.upper(), sql
