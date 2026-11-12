"""``first`` / ``last`` never pick a NULL ranking key, on every dialect (specs: aggregations/functional-form,
aggregations/trailing-window)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import sqlglot
from sqlglot import exp

from slayer.engine.query_engine import SlayerQueryEngine
from tests import _dev1915_fixtures as W
from tests._dev2215_fixtures import BACKENDS, TIER1, check_events, dev2215_engine, events_model, events_query, m, q
from tests._engine_helpers import _engine_generate


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=BACKENDS)
async def windowed(request) -> AsyncIterator[SlayerQueryEngine]:
    async for eng in W.make_exec_engine(request):
        yield eng


def ranking_orders(sql: str, *, dialect: str) -> list[exp.Expr]:
    """The first ORDER BY item of every ranking window in ``sql``."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    out = []
    for window in parsed.find_all(exp.Window):
        if isinstance(window.this, (exp.RowNumber, exp.Rank)) and window.args.get("order"):
            out.append(window.args["order"].expressions[0])
    return out


def has_null_flag(item: exp.Expr) -> bool:
    return any(isinstance(node.expression, exp.Null) for node in item.find_all(exp.Is))


# --------------------------------------------------------------------------- #
# Plain, partitioned and cross-model
# --------------------------------------------------------------------------- #
async def test_null_keys_are_skipped_in_both_directions(engine: SlayerQueryEngine) -> None:
    await check_events(engine)


async def test_partitioned_cross_model(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(dimensions=["customers.region", "customers.name"], measures=[
        m("last(customers.status, customers.updated_at, partition_by=customers.region)", "l"),
        m("first(customers.status, customers.updated_at, partition_by=customers.region)", "f"),
    ]))
    got = {r["orders.customers.name"]: (r["orders.f"], r["orders.l"]) for r in resp.data}
    assert got == {"A": ("gold", "gold"), "B": ("gold", "gold"), "C": ("bronze", "bronze"), "D": ("bronze", "bronze")}


async def test_partitioned_on_the_home_model(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(source_model="events", dimensions=["id", "customer_id"], measures=[
        m("last(status, updated_at, partition_by=customer_id)", "l")]))
    by_id = {int(r["events.id"]): r["events.l"] for r in resp.data}
    assert by_id == {1: "c", 2: "c", 3: "c", 4: None, 5: None, 6: None, 7: None}


# --------------------------------------------------------------------------- #
# Windowed (the DEV-1915 trailing-window seed)
# --------------------------------------------------------------------------- #
async def test_windowed_explicit_ranking_column_with_null_keys(windowed: SlayerQueryEngine) -> None:
    resp = await windowed.execute(W.q(time_dimensions=W.month_td(), measures=[
        {"formula": "last(amount, updated_at, window='90d')", "name": "l"},
        {"formula": "first(amount, updated_at, window='90d')", "name": "f"},
    ]))
    assert {k: float(v) for k, v in W.by_month(resp, "orders.l").items()} == W.LAST_UPDATED_90D
    assert {k: float(v) for k, v in W.by_month(resp, "orders.f").items()} == {
        "2024-01": 100.0, "2024-02": 100.0, "2024-03": 200.0}


async def test_windowed_interval_of_only_null_keys(windowed: SlayerQueryEngine) -> None:
    resp = await windowed.execute(W.q(dimensions=["region"], time_dimensions=W.month_td(), measures=[
        {"formula": "last(amount, updated_at, window='90d')", "name": "l"}]))
    cells = {(r["orders.region"], W.month_key(r["orders.created_at"])): r["orders.l"] for r in resp.data}
    assert cells[("EU", "2024-02")] is None


# --------------------------------------------------------------------------- #
# Emission: the null flag is AST, on every Tier-1 dialect
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", TIER1)
async def test_plain_first_last_order_by_a_null_flag(dialect: str) -> None:
    sql = await _engine_generate(query=events_query(), model=events_model(), dialect=dialect)
    orders = ranking_orders(sql, dialect=dialect)
    assert len(orders) >= 2, sql
    assert all(has_null_flag(o) for o in orders), sql


@pytest.mark.parametrize("dialect", TIER1)
async def test_windowed_first_last_order_by_a_null_flag(dialect: str) -> None:
    sql = await W.gen(W.q(time_dimensions=W.month_td(), measures=[
        {"formula": "last(amount, updated_at, window='90d')", "name": "l"},
        {"formula": "first(amount, window='90d')", "name": "f"},
    ]), dialect=dialect)
    orders = ranking_orders(sql, dialect=dialect)
    assert len(orders) >= 2, sql
    assert all(has_null_flag(o) for o in orders), sql
