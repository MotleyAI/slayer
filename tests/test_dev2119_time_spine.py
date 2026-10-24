"""An expression is never an axis; a fact expression time dimension is a dimension of P (spec: queries/time-spine)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import TimeSpineError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.refs import auto_name_from_expression
from slayer.engine.query_engine import SlayerResponse
from tests._dev1737_fixtures import TableSpec
from tests._dev2015_fixtures import (
    BACKENDS,
    JAN_JUN,
    MONTHS,
    TWO_FACTS,
    bucket_key,
    num,
    spine_engine,
    spine_models,
    spine_query,
    spine_td,
    spine_tables,
    value,
)

EFF_EXPR = "coalesce(orders.shipped_at, orders.order_date)"
SPINE_EXPRESSIONS = ["time_spine.timestamp + INTERVAL '1 day'", "coalesce(time_spine.timestamp, orders.order_date)"]

#: Order 1 (Jan, N, 100) ships in Feb, order 2 (Jan, S, 50) never ships, order 3 (Feb, N, 70) ships in March.
ORDERS = TableSpec(
    name="orders",
    columns=[("id", "INT"), ("customer_id", "INT"), ("order_date", "DATE"), ("shipped_at", "DATE"),
             ("amount", "DOUBLE")],
    rows=[(1, 1, "2025-01-10", "2025-02-02", 100.0), (2, 2, "2025-01-20", None, 50.0),
          (3, 1, "2025-02-05", "2025-03-01", 70.0)],
)
EFF_MONTHS = ["2025-01", "2025-02", "2025-03"]
#: (o, n) per (spine month, coalesced month); returns are broadcast per spine month.
ORDER_CELLS = {("2025-01", "2025-01"): (50.0, 1.0), ("2025-01", "2025-02"): (100.0, 1.0),
               ("2025-02", "2025-03"): (70.0, 1.0)}
RETURNS_BY_MONTH = {"2025-02": 20.0, "2025-03": 10.0, "2025-05": 5.0}


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="order_date",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="order_date", type=DataType.DATE),
            Column(name="shipped_at", type=DataType.DATE),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="eff", type=DataType.DATE, sql="COALESCE(shipped_at, order_date)"),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _models() -> list[SlayerModel]:
    return [_orders_model() if model.name == "orders" else model for model in spine_models()]


def _tables() -> list[TableSpec]:
    return [ORDERS if table.name == "orders" else table for table in spine_tables()]


def _expected() -> dict[tuple[str, str], tuple]:
    out: dict[tuple[str, str], tuple] = {}
    for spine in MONTHS:
        for eff in EFF_MONTHS:
            o, n = ORDER_CELLS.get((spine, eff), (None, 0.0))
            out[(spine, eff)] = (o, RETURNS_BY_MONTH.get(spine), n)
    return out


def _cells(resp: SlayerResponse) -> dict[tuple[str, str], tuple]:
    [bucket] = [k for k in resp.columns if k != "time_spine.timestamp" and k.rsplit(".", 1)[-1] not in ("o", "r", "n")]
    out: dict[tuple[str, str], tuple] = {}
    for row in resp.data:
        key = (bucket_key(row["time_spine.timestamp"]), bucket_key(row[bucket]))
        assert key not in out, key
        out[key] = tuple(num(value(row, name)) for name in ("o", "r", "n"))
    return out


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with spine_engine(request.param, models=_models(), tables=_tables()) as eng:
        yield eng


class TestSpineExpressionRejected:
    @pytest.mark.parametrize("expression", SPINE_EXPRESSIONS)
    @pytest.mark.parametrize("with_axis", [False, True], ids=["alone", "beside-the-axis"])
    async def test_typed_error_before_sql(self, engine, expression: str, with_axis: bool) -> None:
        tds = [spine_td()] if with_axis else []
        query = spine_query(measures=TWO_FACTS, time_dimensions=[
            *tds, {"dimension": expression, "granularity": "month", "date_range": JAN_JUN},
        ])
        with pytest.raises(TimeSpineError) as info:
            await engine.execute(query, dry_run=True)
        msg = str(info.value)
        assert expression in msg or auto_name_from_expression(expression) in msg, msg
        assert "time_spine.timestamp" in (info.value.suggestion or ""), msg


class TestFactExpressionIsADimensionOfP:
    async def test_values_per_spine_month_and_bucket(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS, time_dimensions=[
            spine_td(), {"dimension": EFF_EXPR, "granularity": "month"},
        ]))
        assert _cells(resp) == _expected()

    async def test_equal_to_the_column_holding_the_value(self, engine) -> None:
        expr = await engine.execute(spine_query(measures=TWO_FACTS, time_dimensions=[
            spine_td(), {"dimension": EFF_EXPR, "granularity": "month"},
        ]))
        col = await engine.execute(spine_query(measures=TWO_FACTS, time_dimensions=[
            spine_td(), {"dimension": "orders.eff", "granularity": "month"},
        ]))
        assert _cells(expr) == _cells(col)
