"""Boolean aggregation inputs on a real Postgres (no ``sum`` / ``min`` / ``max`` over boolean there).

Spec: aggregations/boolean-inputs.
"""

from collections.abc import AsyncIterator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2046_fixtures import (
    FLAG_BY_REGION,
    FLAG_TOTALS,
    GT15_TOTALS,
    VIP_ASSOC_SUM_BY_REGION,
    by_dim,
    by_month,
    dev2046_datasets,
    m,
    month_td,
    orders_q,
    seed_statements,
)
from tests._pg_helpers import client_datasource, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> AsyncIterator[SlayerQueryEngine]:
    cur = postgresql.cursor()
    for stmt in seed_statements("postgres"):
        cur.execute(stmt)
    postgresql.commit()
    async with pg_query_engine(client_datasource(postgresql.info), base_dir=str(tmp_path),
                               datasets=dev2046_datasets(data_source="pg")) as engine:
        yield engine


def _num(value):
    return None if value is None else float(value)


@pytest.mark.integration
async def test_column_aggregates(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(metrics=[
        m("sum(orders.flag)", "s"), m("avg(orders.flag)", "a"), m("min(orders.flag)", "mn"), m("max(orders.flag)", "mx"),
        m("count(orders.flag)", "c"), m("count_distinct(orders.flag)", "cd"),
    ]))
    row = resp.data[0]
    assert row["s"] == FLAG_TOTALS["sum"]
    assert not isinstance(row["s"], bool)
    assert _num(row["a"]) == pytest.approx(FLAG_TOTALS["avg"])
    assert row["mn"] is FLAG_TOTALS["min"]
    assert row["mx"] is FLAG_TOTALS["max"]
    assert row["c"] == FLAG_TOTALS["count"]
    assert row["cd"] == FLAG_TOTALS["count_distinct"]


@pytest.mark.integration
async def test_by_region(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(
        dimensions=["orders.region"], metrics=[m(f"{agg}(orders.flag)", agg) for agg in ("sum", "avg", "min", "max")],
    ))
    assert by_dim(resp, "region", "sum") == FLAG_BY_REGION["sum"]
    assert {r: pytest.approx(_num(v)) for r, v in by_dim(resp, "region", "avg").items()} == FLAG_BY_REGION["avg"]
    assert by_dim(resp, "region", "min") == FLAG_BY_REGION["min"]
    assert by_dim(resp, "region", "max") == FLAG_BY_REGION["max"]


@pytest.mark.integration
async def test_comparison_source(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(metrics=[
        m("sum(orders.amount > 15)", "s"), m("avg(orders.amount > 15)", "a"), m("count(orders.amount > 15)", "c"),
        m("min(orders.amount > 15)", "mn"), m("max(orders.amount > 15)", "mx"), m("sum(orders.big_order)", "sb"),
    ]))
    row = resp.data[0]
    assert row["s"] == GT15_TOTALS["sum"]
    assert _num(row["a"]) == pytest.approx(GT15_TOTALS["avg"])
    assert row["c"] == GT15_TOTALS["count"]
    assert row["mn"] is False
    assert row["mx"] is True
    assert row["sb"] == GT15_TOTALS["sum"]


@pytest.mark.integration
@pytest.mark.parametrize("having", ["max(orders.flag) = true", "sum(orders.flag) > 1", "sum(orders.amount > 15) > 1", "max(orders.amount > 15) = true"])
async def test_post_aggregation_filters(pg_engine: SlayerQueryEngine, having: str) -> None:
    resp = await pg_engine.execute(orders_q(dimensions=["orders.region"], metrics=[m("sum(orders.flag)")], filter=[having]))
    assert by_dim(resp, "region") == {"east": FLAG_BY_REGION["sum"]["east"]}


@pytest.mark.integration
async def test_association_pick(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(
        dimensions=["orders.region"],
        metrics=[m("sum(customers.vip)", "s"), m("max(customers.vip)", "mx")],
        to_many_handling="associate",
    ))
    assert by_dim(resp, "region", "s") == VIP_ASSOC_SUM_BY_REGION
    assert by_dim(resp, "region", "mx") == {"east": True, "west": False}


@pytest.mark.integration
async def test_first_last_pick(pg_engine: SlayerQueryEngine) -> None:
    """The ranked pick over a boolean value (no ``max(boolean)`` on Postgres)."""
    resp = await pg_engine.execute(orders_q(
        dimensions=["orders.region"], metrics=[m("first(orders.flag)", "f"), m("last(orders.flag)", "l")],
    ))
    assert by_dim(resp, "region", "f") == {"east": True, "west": None}
    assert by_dim(resp, "region", "l") == {"east": True, "west": False}
    windowed = await pg_engine.execute(orders_q(
        time_dimensions=month_td(), metrics=[m("first(orders.flag, window='90d')")],
    ))
    assert set(by_month(windowed).values()) == {True}


@pytest.mark.integration
async def test_statistical_aggregations(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(metrics=[
        m("median(orders.flag)", "md"), m("percentile(orders.flag, p=0.5)", "p"),
        m("stddev_samp(orders.amount > 15)", "sd"), m("weighted_avg(orders.flag, weight=orders.amount)", "w"),
    ]))
    row = resp.data[0]
    assert _num(row["md"]) == pytest.approx(0.5)
    assert _num(row["p"]) == pytest.approx(0.5)
    assert _num(row["sd"]) == pytest.approx((1 / 3) ** 0.5)
    assert _num(row["w"]) == pytest.approx(0.5)


@pytest.mark.integration
async def test_literal_sources(pg_engine: SlayerQueryEngine) -> None:
    resp = await pg_engine.execute(orders_q(dimensions=["orders.region"], metrics=[
        m("sum(True)", "s"), m("max(True)", "mx"), m("sum(1 > 2)", "f"),
    ]))
    assert by_dim(resp, "region", "s") == {"east": 3, "west": 2}
    assert by_dim(resp, "region", "mx") == {"east": True, "west": True}
    assert by_dim(resp, "region", "f") == {"east": 0, "west": 0}


@pytest.mark.integration
async def test_numeric_positions(pg_engine: SlayerQueryEngine) -> None:
    """Postgres has no boolean arithmetic: each boolean below must read as its integer."""
    resp = await pg_engine.execute(orders_q(metrics=[
        m("sum(orders.flag * orders.amount)", "fa"), m("sum((orders.amount > 15) + 1)", "p1"), m("max(round(orders.flag))", "r"),
        m("sum(coalesce(orders.flag, 0))", "c"), m("sum(iif(orders.amount > 15, orders.flag, 2))", "i"),
    ]))
    row = resp.data[0]
    assert row["fa"] == 30
    assert row["p1"] == 5
    assert _num(row["r"]) == 1
    assert row["c"] == 2
    assert row["i"] == 7
    filtered = await pg_engine.execute(orders_q(metrics=[m("count(orders.*)")], filter=["orders.flag = 1"]))
    assert filtered.data[0]["v"] == 2
    measures = await pg_engine.execute(orders_q(
        dimensions=["orders.region"], metrics=[m("(sum(orders.amount) > 50) + (count(orders.*) > 1)")],
    ))
    assert by_dim(measures, "region") == {"east": 2, "west": None}


@pytest.mark.integration
async def test_compared_with_numeric_expressions(pg_engine: SlayerQueryEngine) -> None:
    """Postgres has no boolean = integer: a boolean beside a numeric scalar or aggregate reads as its integer."""
    rows = await pg_engine.execute(orders_q(metrics=[m("count(orders.*)")], filter=["orders.flag = sign(orders.amount)"]))
    assert rows.data[0]["v"] == 2
    groups = await pg_engine.execute(orders_q(
        dimensions=["orders.region"], metrics=[m("count(orders.*)")], filter=["max(orders.flag) = count_distinct(orders.region)"],
    ))
    assert by_dim(groups, "region") == {"east": 3}


@pytest.mark.integration
async def test_weighted_avg_integer_inputs(pg_engine: SlayerQueryEngine) -> None:
    """Integer value and weight never divide as integers; a NULL value's weight is skipped."""
    resp = await pg_engine.execute(orders_q(metrics=[m("weighted_avg(orders.amount, weight=orders.customer_id)")]))
    assert _num(resp.data[0]["v"]) == pytest.approx(9000 / 400)
