"""Shared models, oracles and checks for boolean aggregation parameters and conditional branch types.

Underscore-prefixed so pytest skips collection. Data: the idiom dataset (``tests/_mcp_idiom_fixtures.py``).
``orders.true_amount`` is a custom aggregation over a ``{flag}`` placeholder; ``regions.weighted_avg``
overrides the built-in by name, over ``regions.rank`` (= ``id``).

Paid orders: 1 (10), 3 (45), 4 (100), 5 (50), 8 (60), 10 (75), 11 (5): total 345 over 7 rows.
Paid above 10: 45, 100, 50, 60, 75 → 330 / 5. Paid per customer: Alice 10 + 100 + 5, Bob 50 + 60, Carol 45 + 75.
Customers with credit > 150: Bob 200, Carol 300; every ordering customer has both statuses.
"""

from __future__ import annotations

import statistics
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ScalarOperandTypeError
from slayer.core.models import Aggregation, AggregationParam, Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._mcp_idiom_fixtures import idiom_models

PAID = "status = 'paid'"
#: Orders 1–11: amount and the integer of ``status = 'paid'``.
_AMOUNTS = [10, 20, 45, 100, 50, 70, 30, 60, 25, 75, 5]
_PAID01 = [1, 0, 1, 1, 1, 0, 0, 1, 0, 1, 1]

PAID_AVG = 345 / 7
PAID_AVG_ABOVE_10 = 330 / 5
PAID_AVG_BY_CUSTOMER = {1: 115 / 3, 2: 55.0, 3: 60.0}
PAID_AVG_BY_NAME = {"Alice": 115 / 3, "Bob": 55.0, "Carol": 60.0, "Dan": None, "Eve": None}
COVAR_SAMP_PAID = statistics.covariance(_AMOUNTS, _PAID01)
CORR_PAID = statistics.correlation(_AMOUNTS, _PAID01)
CREDIT_ABOVE_150_AVG = 250.0
PAID_TOTAL = 345
MONTH = [{"dimension": "order_date", "granularity": "month"}]


def dev2177_models(*, data_source: str = "test") -> list[SlayerModel]:
    orders, customers, regions = idiom_models(data_source=data_source)
    orders = orders.model_copy(update={"aggregations": [
        Aggregation(name="true_amount", formula="SUM(CASE WHEN {flag} THEN {value} END)",
                    params=[AggregationParam(name="flag", sql="TRUE")]),
    ]})
    regions = regions.model_copy(update={"columns": [
        *regions.columns, Column(name="rank", sql="id", type=DataType.INT),
    ], "aggregations": [
        Aggregation(name="weighted_avg", formula="SUM({value} * {weight}) * 1.0 / SUM({weight})",
                    params=[AggregationParam(name="weight", sql="1")]),
    ]})
    return [orders, customers, regions]


def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def m(formula: str, name: str = "v") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def num(value: Any) -> float | None:
    return None if value is None else float(value)


def single(resp: SlayerResponse, name: str = "v", *, model: str = "orders") -> Any:
    assert len(resp.data) == 1, resp.data
    return resp.data[0][f"{model}.{name}"]


# --------------------------------------------------------------------------- #
# Boolean aggregation parameters (spec: aggregations/boolean-inputs)
# --------------------------------------------------------------------------- #
async def check_boolean_weight(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(measures=[m(f"weighted_avg(amount, weight={PAID})")]))
    assert num(single(resp)) == pytest.approx(PAID_AVG), resp.sql


async def check_boolean_other(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(measures=[
        m(f"covar_samp(amount, other={PAID})", "cv"), m(f"corr(amount, other={PAID})", "cr"),
    ]))
    assert num(single(resp, "cv")) == pytest.approx(COVAR_SAMP_PAID), resp.sql
    assert num(single(resp, "cr")) == pytest.approx(CORR_PAID), resp.sql


async def check_boolean_weight_under_filter(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(measures=[m(f"weighted_avg(amount, weight={PAID}) FILTER (WHERE amount > 10)")]))
    assert num(single(resp)) == pytest.approx(PAID_AVG_ABOVE_10), resp.sql


async def check_boolean_weight_windowed(engine: SlayerQueryEngine) -> None:
    """Each month's trailing-window value equals ``avg(amount, window=)`` over the paid rows alone."""
    resp = await engine.execute(q(time_dimensions=MONTH, measures=[
        m(f"weighted_avg(amount, weight={PAID}, window='90d')"),
    ]))
    restricted = await engine.execute(q(time_dimensions=MONTH, measures=[m("avg(amount, window='90d')")],
                                         filters=[PAID]))
    got = {str(r["orders.order_date"])[:7]: num(r["orders.v"]) for r in resp.data}
    want = {str(r["orders.order_date"])[:7]: num(r["orders.v"]) for r in restricted.data}
    assert want, (got, want)
    assert got.keys() >= want.keys(), (got, want)
    for month, value in want.items():
        assert got[month] == pytest.approx(value), (month, resp.sql)


async def check_boolean_weight_partitioned(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(dimensions=["customer_id", "status"], measures=[
        m(f"weighted_avg(amount, weight={PAID}, partition_by=customer_id)"),
    ]))
    assert len(resp.data) == 6, resp.data
    for row in resp.data:
        assert num(row["orders.v"]) == pytest.approx(PAID_AVG_BY_CUSTOMER[int(row["orders.customer_id"])]), resp.sql


async def check_boolean_weight_cross_model(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "customers", "dimensions": ["name"],
        "measures": [m("weighted_avg(orders.amount, weight=orders.status = 'paid')")],
    }))
    got = {r["customers.name"]: num(r["customers.v"]) for r in resp.data}
    assert got == pytest.approx(PAID_AVG_BY_NAME), resp.sql


async def check_boolean_weight_associated(engine: SlayerQueryEngine) -> None:
    """The association producer picks the boolean parameter per entity before weighting."""
    resp = await engine.execute(q(
        dimensions=["status"], to_many_handling="associate",
        measures=[m("weighted_avg(customers.credit, weight=customers.credit > 150)")],
    ))
    got = {r["orders.status"]: num(r["orders.v"]) for r in resp.data}
    assert got == pytest.approx({"open": CREDIT_ABOVE_150_AVG, "paid": CREDIT_ABOVE_150_AVG}), resp.sql
    assert "associated" in {w.kind for w in resp.warnings}


async def check_custom_aggregation_receives_raw_boolean(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(measures=[m(f"true_amount(amount, flag={PAID})")]))
    assert num(single(resp)) == PAID_TOTAL, resp.sql


async def check_builtin_named_override_lowers(engine: SlayerQueryEngine) -> None:
    """Regions 2, 3 and 4 weigh 1, North 0."""
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "regions", "measures": [m("weighted_avg(rank, weight=name <> 'North')")],
    }))
    assert num(single(resp, model="regions")) == pytest.approx(3.0), resp.sql


BOOLEAN_PARAMETER_CHECKS = [
    check_boolean_weight, check_boolean_other, check_boolean_weight_under_filter, check_boolean_weight_windowed,
    check_boolean_weight_partitioned, check_boolean_weight_cross_model, check_boolean_weight_associated,
    check_builtin_named_override_lowers,
]


# --------------------------------------------------------------------------- #
# Conditional branch types (spec: queries/mixed-type-coercion)
# --------------------------------------------------------------------------- #
CONCAT_BRANCHES = [
    "CASE WHEN amount > 1 THEN 'a' || 'b' ELSE amount END",
    "CASE WHEN amount > 1 THEN concat('a', 'b') ELSE amount END",
    "CASE WHEN amount > 1 THEN status || 'b' ELSE amount END",
]


def dimension_query(expression: str) -> SlayerQuery:
    return q(dimensions=[{"expression": expression, "name": "x"}])


async def assert_branch_mismatch(engine: SlayerQueryEngine, query: SlayerQuery | list, *types: str,
                                 call: tuple[str, ...] = ()) -> None:
    """Plan-time ``ScalarOperandTypeError`` naming every type in ``types``, one of ``call`` and a cast."""
    with pytest.raises(ScalarOperandTypeError) as info:
        await engine.execute(query)
    message = str(info.value)
    for t in types:
        assert t in message, message
    if call:
        assert any(c in message.lower() for c in call), message
    assert "cast" in message.lower(), message


async def check_concat_branches_rejected(engine: SlayerQueryEngine) -> None:
    for expression in CONCAT_BRANCHES:
        await assert_branch_mismatch(engine, dimension_query(expression), "TEXT", "INT")
