"""Transforms evaluate over their operand's domain, never the query's rows (spec: queries/transforms,
queries/semantics, queries/partitioned-aggregates, queries/computed-dimensions, queries/positions)."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import TimeAxisError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev1836_fixtures import make_exec_engine as make_cm_engine
from tests._dev1836_fixtures import month_td as cm_month_td
from tests._dev1836_fixtures import q as cm_q
from tests._dev1839_fixtures import (
    MIXED_RANK,
    MIXED_RANK_OF,
    RCC_TOTAL,
    REGION_CUMSUM,
)
from tests._dev1839_fixtures import make_exec_engine as make_orders_engine
from tests._dev1839_fixtures import month_key, month_td
from tests._dev1839_fixtures import q as orders_q
from tests._dev1839_fixtures import rows_by
from tests._dev1847_fixtures import SPEND_BAND_EXPR, sales_q
from tests._dev1868_fixtures import CM_PART_TIER
from tests._dev1900_fixtures import cust_q
from tests._dev1900_fixtures import make_exec_engine as make_spend_engine
from tests._dev1847_fixtures import make_exec_engine as make_sales_engine
from tests._dev2015_fixtures import JAN_JUN, by_bucket, calendar_datasets, m, spine_engine, spine_query
from tests._dev2212_fixtures import (
    BACKENDS, DS, orders_dataset, rows, run, seed_datasets, seed_engine, table, with_policy,
)
from tests._engine_helpers import seeded_exec_engine

R, SEG = "customers.region", "customers.segment"
REV, RET = "orders.total_revenue", "returns.total_returns"
MONTH = "orders.ordered_at"
PR = "percent_rank(count(orders.*))"
NT = "ntile(count(orders.*), n=2)"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _mt(name: str, expr: str) -> dict[str, str]:
    return {"metric": expr, "alias": name}


def _by(resp: SlayerResponse, keys: tuple[str, ...], name: str) -> dict[tuple, Any]:
    out = {tuple(month_key(r[k]) if k == MONTH else r[k] for k in keys): r[name] for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one key"
    return {k: (float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v) for k, v in out.items()}


def _same_cells(alone: dict, both: dict, *, added_null: bool = True) -> None:
    """Every cell of ``alone`` keeps its value in ``both``; each row only ``both`` has reads NULL."""
    assert set(alone) <= set(both)
    for key, v in alone.items():
        assert both[key] == v, key
    if added_null:
        assert all(both[k] is None for k in set(both) - set(alone)), {k: both[k] for k in set(both) - set(alone)}


# --------------------------------------------------------------------------- #
# Daily sales: sales 10 (2025-01-05), 30 (2025-03-05); refunds 5 (2025-02-05), all joining `days`.
# --------------------------------------------------------------------------- #
def _days(*dates: str) -> TableSpec:
    return TableSpec(name="days", columns=[("day", "DATE")], rows=[(d,) for d in dates])


SALES = TableSpec(name="sales", columns=[("sale_id", "TEXT"), ("day", "DATE"), ("amount", "DOUBLE")],
                  rows=[("S1", "2025-01-05", 10.0), ("S2", "2025-03-05", 30.0)])


def _refunds(*extra: tuple) -> TableSpec:
    return TableSpec(name="refunds", columns=[("refund_id", "TEXT"), ("day", "DATE"), ("amount", "DOUBLE")],
                     rows=[("F1", "2025-02-05", 5.0), *extra])


def _dated(name: str, pk: str) -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table=name, data_source=DS,
        fields=[DatasetField(name=pk, datatype=DataType.STRING, primary_key=True),
                DatasetField(name="day", datatype=DataType.DATE), DatasetField(name="amount", datatype=DataType.FLOAT)],
        relationships=[DatasetRelationship(to="days", from_columns=["day"], to_columns=["day"])],
    )


DAILY = [SlayerDataset(name="days", sql_table="days", data_source=DS,
                       fields=[DatasetField(name="day", datatype=DataType.DATE, primary_key=True)]),
         _dated("sales", "sale_id"), _dated("refunds", "refund_id")]
BY_DAY_MONTH = [{"field": "days.day", "granularity": "month"}]
REFUND = _mt("f", "sum(refunds.amount)")


@contextlib.asynccontextmanager
async def daily_engine(backend: str, *, april: bool = False) -> AsyncGenerator[SlayerQueryEngine]:
    days = ["2025-01-05", "2025-02-05", "2025-03-05", *(["2025-04-05"] if april else [])]
    refunds = _refunds(("F2", "2025-04-05", 7.0)) if april else _refunds()
    tables = [_days(*days), SALES, refunds]
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, tables),
                                  datasets=DAILY) as (eng, _):
        yield eng


def _monthly(resp: SlayerResponse, name: str) -> dict[str, Any]:
    out = {str(r["days.day"])[:7]: r[name] for r in resp.data}
    assert len(out) == len(resp.data)
    return {k: None if v is None else float(v) for k, v in out.items()}


async def _daily_pair(backend: str, expr: str, *, april: bool = False) -> tuple[dict, dict]:
    async with daily_engine(backend, april=april) as eng:
        alone = await run(eng, time_dimensions=BY_DAY_MONTH, metrics=[_mt("t", expr)])
        both = await run(eng, time_dimensions=BY_DAY_MONTH, metrics=[_mt("t", expr), REFUND])
    return _monthly(alone, "t"), _monthly(both, "t")


# --------------------------------------------------------------------------- #
# queries/transforms › Transforms evaluate over their operand's domain
# --------------------------------------------------------------------------- #
async def test_a_sibling_that_adds_rows_leaves_rank_family_values_unchanged(backend: str) -> None:
    metrics = [_mt("pr", PR), _mt("nt", NT)]
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], metrics=metrics)
        both = await run(eng, dimensions=[R], metrics=[*metrics, RET])
    assert table(alone, R, "pr", "nt") == rows(("EAST", 1, 2), ("WEST", 0, 1))
    assert table(both, R, "pr", "nt") == rows(("EAST", 1, 2), ("WEST", 0, 1), ("NORTH", None, None))


async def test_lag_skips_gaps_without_the_spine(backend: str) -> None:
    alone, both = await _daily_pair(backend, "lag(sum(sales.amount))")
    assert alone == {"2025-01": None, "2025-03": 10.0}
    assert both == {"2025-01": None, "2025-02": None, "2025-03": 10.0}


async def test_a_nested_transform_is_a_constituent_at_its_result_grain(backend: str) -> None:
    alone, both = await _daily_pair(backend, "lag(cumsum(sum(sales.amount)))")
    assert alone == {"2025-01": None, "2025-03": 10.0}
    assert both == {"2025-01": None, "2025-02": None, "2025-03": 10.0}


async def test_a_collapsing_pick_reads_the_operands_cells(backend: str) -> None:
    alone, both = await _daily_pair(backend, "last(sum(sales.amount))", april=True)
    assert alone == {"2025-01": 30.0, "2025-03": 30.0}
    assert both == {"2025-01": 30.0, "2025-02": 30.0, "2025-03": 30.0, "2025-04": 30.0}


@pytest.mark.parametrize("hide", ["model_filter", "policy"])
async def test_rows_hidden_before_aggregation_have_no_cell(backend: str, hide: str) -> None:
    metrics = [_mt("pr", PR), RET]
    if hide == "model_filter":
        datasets = seed_datasets(orders=orders_dataset(filters=["status = 'completed'"]))
        async with seed_engine(backend, datasets=datasets) as eng:
            resp = await run(eng, dimensions=[R], metrics=metrics)
    else:
        completed = SessionPolicy(ruleset=ColumnFilterRuleset(column="status", value="completed",
                                                              on_unapplicable="pass"))
        async with seed_engine(backend) as eng, with_policy(eng, completed) as scoped:
            resp = await run(scoped, dimensions=[R], metrics=metrics)
    # completed orders: O1 C1, O3 C2 — EAST is the domain's only cell; WEST's open order does not exist
    assert table(resp, R, "pr", RET) == rows(("EAST", 0, 25), ("NORTH", None, 10))


async def test_a_coarser_operand_ranks_its_own_cells(backend: str) -> None:
    rank = _mt("r", "rank(sum(orders.amount, partition_by=customers.region), direction='desc')")
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, SEG], metrics=[rank])
    assert table(resp, R, SEG, "r") == rows(("EAST", "retail", 1), ("EAST", "corporate", 1), ("WEST", "retail", 2))


async def test_a_mixed_grain_operand_ranks_the_union_grain_cells(backend: str) -> None:
    expr = ("rank(sum(orders.amount, partition_by=customers.region) - "
            "sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc')")
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, SEG], metrics=[_mt("r", expr)])
    # diffs: (EAST, retail) 350-300=50, (EAST, corporate) 350-50=300, (WEST, retail) 300-300=0
    assert table(resp, R, SEG, "r") == rows(("EAST", "corporate", 1), ("EAST", "retail", 2), ("WEST", "retail", 3))


async def test_a_named_population_does_not_widen_the_domain(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R], metrics=[_mt("pr", PR)])
    assert table(resp, R, "pr") == rows(("EAST", 1), ("WEST", 0), ("NORTH", None))


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_hand_made_calendar_population_is_not_a_dense_series(backend: str) -> None:
    async with spine_engine(backend, datasets=calendar_datasets()) as eng:
        cal = await eng.execute(SlayerQuery.model_validate({
            "population": "calendar", "metrics": [m("lag(sum(returns.amount))", "lg")],
            "time_dimensions": [{"field": "calendar.date", "granularity": "month", "date_range": JAN_JUN}],
        }))
        spine = await eng.execute(spine_query(metrics=[m("lag(sum(returns.amount))", "lg")]))
    got = by_bucket(cal, ["lg"], bucket="calendar.date")
    assert [got[k][0] for k in sorted(got)] == [None, None, 20.0, None, 10.0, None]
    dense = by_bucket(spine, ["lg"])
    assert [dense[k][0] for k in sorted(dense)] == [None, None, 20.0, 10.0, None, 5.0]


async def test_filter_and_order_positions_read_the_same_values(backend: str) -> None:
    async with seed_engine(backend) as eng:
        plain = await run(eng, dimensions=[R], metrics=[REV, RET, _mt("pr", PR)])
        filtered = await run(eng, dimensions=[R], metrics=[REV, RET], filter=[f"{PR} = 1"])
        ordered = await run(eng, dimensions=[R], metrics=[REV, RET], order=[{"field": PR, "direction": "desc"}])
    assert table(filtered, R, REV, RET) == rows(("EAST", 350, 25))
    regions = [r[R] for r in ordered.data]
    assert regions.index("EAST") < regions.index("WEST")
    assert table(ordered, R, REV, RET) == table(plain, R, REV, RET)


async def test_a_filter_only_transform_supplies_no_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[RET], filter=[f"{PR} = 1"])
    assert table(resp, R, RET) == rows(("EAST", 25))


async def test_an_order_only_transform_supplies_no_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[RET], order=[{"field": PR, "direction": "desc"}])
    assert table(resp, R, RET) == rows(("EAST", 25), ("NORTH", 10))


# --------------------------------------------------------------------------- #
# queries/semantics › Compositionality: a transform keeps its values when a sibling adds rows
# --------------------------------------------------------------------------- #
async def test_a_transform_keeps_its_values_when_a_sibling_adds_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], metrics=[_mt("pr", PR)])
        both = await run(eng, dimensions=[R], metrics=[_mt("pr", PR), RET])
    assert table(alone, R, "pr") == rows(("EAST", 1), ("WEST", 0))
    assert table(both, R, "pr") == rows(("EAST", 1), ("WEST", 0), ("NORTH", None))


@pytest.mark.parametrize("expr", [
    "lag(sum(orders.amount))", "cumsum(sum(orders.amount))", "last(sum(orders.amount))",
    "first(sum(orders.amount))", "rank(sum(orders.amount), direction='desc')", PR, NT,
    "lead(sum(orders.amount))",
])
async def test_sibling_rows_never_change_a_time_bucketed_transform(backend: str, expr: str) -> None:
    td = [{"field": MONTH, "granularity": "month"}]
    async with seed_engine(backend, orphan=True) as eng:
        alone = await run(eng, dimensions=[R], time_dimensions=td, metrics=[_mt("t", expr)])
        both = await run(eng, dimensions=[R], time_dimensions=td, metrics=[_mt("t", expr), RET])
    _same_cells(_by(alone, (R, MONTH), "t"), _by(both, (R, MONTH), "t"))


# --------------------------------------------------------------------------- #
# Calendar-step series regime and consecutive_periods read the domain
# --------------------------------------------------------------------------- #
async def test_series_regime_shift_ignores_a_siblings_bucket(backend: str) -> None:
    alone, both = await _daily_pair(backend, "time_shift(cumsum(sum(sales.amount)), -1)")
    # March reads February's running total: no cell, never the sibling's February row
    assert alone == {"2025-01": None, "2025-03": None}
    assert (both["2025-01"], both["2025-03"]) == (None, None)


async def test_re_aggregation_regime_shift_is_unchanged(backend: str) -> None:
    alone, both = await _daily_pair(backend, "time_shift(sum(sales.amount), -2)")
    assert alone["2025-03"] == both["2025-03"] == 10.0


async def test_consecutive_periods_is_not_extended_by_a_siblings_bucket(backend: str) -> None:
    alone, both = await _daily_pair(backend, "consecutive_periods(sum(sales.amount) > 0)")
    assert alone == {"2025-01": 1.0, "2025-03": 1.0}
    assert both == {"2025-01": 1.0, "2025-02": None, "2025-03": 1.0}


async def test_consecutive_periods_ignores_a_measure_filter(backend: str) -> None:
    async with daily_engine(backend) as eng:
        resp = await run(eng, time_dimensions=BY_DAY_MONTH,
                         metrics=[_mt("t", "consecutive_periods(sum(sales.amount) > 0)"), REFUND],
                         filter=["sum(refunds.amount) > 0 or sum(sales.amount) > 20"])
    assert _monthly(resp, "t") == {"2025-02": None, "2025-03": 1.0}


# --------------------------------------------------------------------------- #
# Recursive domains
# --------------------------------------------------------------------------- #
TD = [{"field": MONTH, "granularity": "month"}]


async def test_preserving_over_collapsing(backend: str) -> None:
    expr = "rank(last(sum(orders.amount)), direction='desc')"
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], time_dimensions=TD, metrics=[_mt("t", expr)])
        both = await run(eng, dimensions=[R], time_dimensions=TD, metrics=[_mt("t", expr), RET])
    # last per region: EAST 200 (Feb), WEST 300 → WEST 1, EAST 2 on every bucket of the region
    expected = {("EAST", "2025-01"): 2.0, ("EAST", "2025-02"): 2.0, ("WEST", "2025-02"): 1.0}
    assert _by(alone, (R, MONTH), "t") == expected
    _same_cells(expected, _by(both, (R, MONTH), "t"))


async def test_collapsing_over_preserving(backend: str) -> None:
    alone, both = await _daily_pair(backend, "last(cumsum(sum(sales.amount)))", april=True)
    assert alone == {"2025-01": 40.0, "2025-03": 40.0}
    assert both == {"2025-01": 40.0, "2025-02": 40.0, "2025-03": 40.0, "2025-04": 40.0}


async def test_re_aggregation_over_a_transform(backend: str) -> None:
    expr = "max(rank(sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc'))"
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], metrics=[_mt("t", expr)])
        both = await run(eng, dimensions=[R], metrics=[_mt("t", expr), RET])
    # (region, segment) totals: (EAST, retail) 300, (WEST, retail) 300, (EAST, corporate) 50 → ranks 1, 1, 3
    assert table(alone, R, "t") == rows(("EAST", 3), ("WEST", 1))
    assert table(both, R, "t") == rows(("EAST", 3), ("WEST", 1), ("NORTH", None))


# --------------------------------------------------------------------------- #
# Mixed-grain operands (12.3 at the operand grain)
# --------------------------------------------------------------------------- #
async def test_a_nested_mixed_grain_operand(backend: str) -> None:
    expr = ("rank(cumsum(sum(orders.amount, partition_by=[customers.region, orders.ordered_at])) - "
            "sum(orders.amount, partition_by=customers.region), direction='desc')")
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], time_dimensions=TD, metrics=[_mt("t", expr)])
        both = await run(eng, dimensions=[R], time_dimensions=TD, metrics=[_mt("t", expr), RET])
    # cumsum per region: EAST Jan 150, Feb 350; WEST Feb 300 → minus region totals: -200, 0, 0
    assert _by(alone, (R, MONTH), "t") == {("EAST", "2025-01"): 3.0, ("EAST", "2025-02"): 1.0,
                                           ("WEST", "2025-02"): 1.0}
    _same_cells(_by(alone, (R, MONTH), "t"), _by(both, (R, MONTH), "t"))


async def test_an_unrelated_constituent_extends_to_the_finer_cells(backend: str) -> None:
    expr = "rank(sum(orders.amount) + count(tickets.*), direction='desc')"
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[_mt("t", expr)])
    assert table(resp, R, "t") == rows(("EAST", 1), ("WEST", 2))


AGG_FREE = "rank(customers.region, direction='asc')"


async def test_an_aggregate_free_operand_over_a_product_anchor_takes_its_cells(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "tickets.priority"], metrics=[_mt("t", AGG_FREE)])
    assert table(resp, R, "tickets.priority", "t") == rows(
        *((region, p, rank) for region, rank in (("EAST", 1), ("NORTH", 3), ("WEST", 5)) for p in ("high", "low")))


async def test_an_aggregate_free_operand_takes_the_anchors_cells(backend: str) -> None:
    async with seed_engine(backend) as eng:
        alone = await run(eng, dimensions=[R], metrics=[_mt("t", AGG_FREE)])
        both = await run(eng, dimensions=[R], metrics=[_mt("t", AGG_FREE), REV])
    expected = rows(("EAST", 1), ("NORTH", 2), ("WEST", 3))
    assert table(alone, R, "t") == table(both, R, "t") == expected


# --------------------------------------------------------------------------- #
# Position twins
# --------------------------------------------------------------------------- #
TWIN = ("rank(sum(orders.amount, partition_by=customers.region) - "
        "sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc')")


async def test_position_twins_read_identical_values(backend: str) -> None:
    async with seed_engine(backend) as eng:
        projected = await run(eng, dimensions=[R, SEG], metrics=[_mt("t", TWIN), RET])
        filtered = await run(eng, dimensions=[R, SEG], metrics=[REV, RET], filter=[f"{TWIN} = 1"])
        ordered = await run(eng, dimensions=[R, SEG], metrics=[REV], order=[{"field": TWIN, "direction": "asc"}])
    assert table(projected, R, SEG, "t") == rows(
        ("EAST", "corporate", 1), ("EAST", "retail", 2), ("WEST", "retail", 3), ("NORTH", "retail", None))
    assert table(filtered, R, SEG, REV, RET) == rows(("EAST", "corporate", 50, None))
    assert [(r[R], r[SEG]) for r in ordered.data] == [("EAST", "corporate"), ("EAST", "retail"), ("WEST", "retail")]


async def test_hidden_transforms_supply_no_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        filtered = await run(eng, dimensions=[R, SEG], metrics=[RET], filter=[f"{TWIN} > 0"])
        ordered = await run(eng, dimensions=[R, SEG], metrics=[RET], order=[{"field": TWIN, "direction": "asc"}])
    # returns' cells: (EAST, retail) 25, (NORTH, retail) 10
    assert table(filtered, R, SEG, RET) == rows(("EAST", "retail", 25))
    assert table(ordered, R, SEG, RET) == rows(("EAST", "retail", 25), ("NORTH", "retail", 10))


# --------------------------------------------------------------------------- #
# queries/partitioned-aggregates › transforms over partitioned inners
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def orders_engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async for engine in make_orders_engine(request):
        yield engine


@pytest.fixture(params=BACKENDS)
async def sales_engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async for engine in make_sales_engine(request):
        yield engine


async def test_running_total_accumulates_within_the_operands_non_time_dimensions(orders_engine) -> None:
    resp = await orders_engine.execute(orders_q(
        dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
        metrics=[QueryMetric(metric="cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at]))",
                             alias="c")],
    ))
    got = {(r["orders.region"], r["orders.city"], month_key(r["orders.ordered_at"])): float(r["c"])
           for r in resp.data}
    assert got == {key: REGION_CUMSUM[(key[0], key[2])] for key in got}
    assert len(got) == 6


async def test_rank_of_an_attached_total_ranks_the_partition_cells(orders_engine) -> None:
    resp = await orders_engine.execute(orders_q(
        dimensions=["orders.region", "orders.city"],
        metrics=[QueryMetric(metric="rank(sum(orders.amount, partition_by=orders.region), direction='desc')",
                             alias="r")],
    ))
    got = {k: int(r["r"]) for k, r in rows_by(resp, "orders.region", "orders.city").items()}
    # region totals North 100, NULL 60, South 50
    assert got == {("North", "CityA"): 1, ("North", "CityB"): 1, ("North", None): 1,
                   (None, "CityD"): 2, ("South", "CityC"): 3}


async def test_rank_over_sales_ranks_the_five_regions(sales_engine) -> None:
    resp = await sales_engine.execute(sales_q(
        dimensions=["sales.region", "sales.city"],
        metrics=[QueryMetric(metric="rank(sum(sales.amount, partition_by=sales.region), direction='desc')",
                             alias="r")],
    ))
    got = {(r["sales.region"], r["sales.city"]): r["r"] for r in resp.data}
    by_region = {region: {v for (rg, _), v in got.items() if rg == region} for region, _ in got}
    # East 180, South 140, North 90, Gap 20; Void's all-NULL total ranks NULL
    assert by_region == {"East": {1}, "South": {2}, "North": {3}, "Gap": {4}, "Void": {None}}


async def test_ranking_partition_cells_of_a_computed_band(sales_engine) -> None:
    resp = await sales_engine.execute(sales_q(
        dimensions=[{"field": SPEND_BAND_EXPR, "alias": "spend_band"}, "sales.city"],
        metrics=[QueryMetric(metric="rank(sum(sales.amount, partition_by=spend_band), direction='desc')",
                             alias="r")],
    ))
    by_band: dict[Optional[str], set] = {}
    for r in resp.data:
        by_band.setdefault(r["spend_band"], set()).add(r["r"])
    assert by_band == {"hi": {1}, "lo": {2}}


async def test_transform_over_mixed_grain_arithmetic_ranks_union_cells(orders_engine) -> None:
    dims = ["orders.region", "orders.city", "orders.channel"]
    base = await orders_engine.execute(orders_q(dimensions=dims, metrics=[QueryMetric(metric="sum(orders.amount)",
                                                                                      alias="s")]))
    both = await orders_engine.execute(orders_q(dimensions=dims, metrics=[
        QueryMetric(metric="sum(orders.amount)", alias="s"), QueryMetric(metric=MIXED_RANK, alias="rm")]))
    by = rows_by(both, *dims)
    assert set(by) == set(RCC_TOTAL)
    for key, r in by.items():
        assert int(r["rm"]) == MIXED_RANK_OF[key[:2]], key
    assert {k: r["s"] for k, r in rows_by(base, *dims).items()} == {k: r["s"] for k, r in by.items()}


# --------------------------------------------------------------------------- #
# queries/computed-dimensions + queries/positions › dimension and measure forms read one domain
# --------------------------------------------------------------------------- #
REGION_RANK = "rank(sum(orders.amount, partition_by=orders.region), direction='desc')"
REGION_RANK_OF = {"North": 1, None: 2, "South": 3}


@pytest.mark.parametrize("position", ["dimension", "measure"])
async def test_context_grain_ranks_the_regions_in_both_forms(orders_engine, position: str) -> None:
    dims: list[Any] = ["orders.region", "orders.city"]
    if position == "dimension":
        query = orders_q(dimensions=[*dims, {"field": REGION_RANK, "alias": "r"}],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")])
    else:
        query = orders_q(dimensions=dims, metrics=[QueryMetric(metric=REGION_RANK, alias="r")])
    resp = await orders_engine.execute(query)
    for r in resp.data:
        assert int(r["r"]) == REGION_RANK_OF[r["orders.region"]], r


async def test_same_mixed_grain_transform_as_dimension_and_measure(orders_engine) -> None:
    dims = ["orders.region", "orders.city", "orders.channel"]
    resp = await orders_engine.execute(orders_q(
        dimensions=[*dims, {"field": MIXED_RANK, "alias": "rr"}],
        metrics=[QueryMetric(metric=MIXED_RANK, alias="rm"), QueryMetric(metric="sum(orders.amount)", alias="s")],
    ))
    by = rows_by(resp, *dims)
    assert set(by) == set(RCC_TOTAL)
    for key, r in by.items():
        assert int(r["rr"]) == int(r["rm"]) == MIXED_RANK_OF[key[:2]], key


async def test_a_transform_dimensions_expression_as_a_measure_alone(orders_engine) -> None:
    dims = ["orders.region", "orders.city", "orders.channel"]
    resp = await orders_engine.execute(orders_q(dimensions=dims, metrics=[QueryMetric(metric=MIXED_RANK,
                                                                                     alias="rm")]))
    by = rows_by(resp, *dims)
    assert set(by) == set(RCC_TOTAL)
    for key, r in by.items():
        assert int(r["rm"]) == MIXED_RANK_OF[key[:2]], key


async def test_measure_filter_and_order_agree_on_a_transform_dimension(orders_engine) -> None:
    dims: list[Any] = ["orders.region", "orders.city", "orders.channel", {"field": MIXED_RANK, "alias": "rr"}]
    filtered = await orders_engine.execute(orders_q(
        dimensions=dims, metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        filter=[f"{MIXED_RANK} = 1"]))
    assert {(r["orders.region"], r["orders.city"]) for r in filtered.data} == {
        k for k, v in MIXED_RANK_OF.items() if v == 1}
    ordered = await orders_engine.execute(orders_q(
        dimensions=dims, metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        order=[{"field": MIXED_RANK, "direction": "asc"}]))
    ranks = [int(r["rr"]) for r in ordered.data]
    assert ranks == sorted(ranks)


# --------------------------------------------------------------------------- #
# A time-ordered transform over a time-less operand fails in every position (Axiom 11.3)
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def cm_engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async for engine in make_cm_engine(request):
        yield engine


def _tier_q(**kw: Any) -> SlayerQuery:
    return cm_q(dimensions=["customers.tier"], time_dimensions=cm_month_td(), **kw)


@pytest.mark.parametrize("position", ["measure", "filter", "order"])
async def test_a_time_ordered_transform_over_a_time_less_cross_model_operand_fails(cm_engine, position: str) -> None:
    cumsum = f"cumsum({CM_PART_TIER})"
    amount = QueryMetric(metric="sum(orders.amount)", alias="s")
    query = {
        "measure": lambda: _tier_q(metrics=[QueryMetric(metric=cumsum, alias="c")]),
        "filter": lambda: _tier_q(metrics=[amount], filter=[f"{cumsum} > 200"]),
        "order": lambda: _tier_q(metrics=[amount], order=[{"field": cumsum, "direction": "desc"}]),
    }[position]()
    with pytest.raises(TimeAxisError) as exc:
        await cm_engine.execute(query)
    assert "partition_by" in str(exc.value)


async def test_a_time_ordered_transform_over_a_time_less_local_operand_fails(orders_engine) -> None:
    query = orders_q(dimensions=["orders.region"], time_dimensions=month_td(), metrics=[
        QueryMetric(metric="cumsum(sum(orders.amount, partition_by=orders.region))", alias="c")])
    with pytest.raises(TimeAxisError):
        await orders_engine.execute(query)


async def test_rank_over_a_cross_model_partitioned_sum_ranks_its_cells(cm_engine) -> None:
    resp = await cm_engine.execute(_tier_q(metrics=[
        QueryMetric(metric=f"rank({CM_PART_TIER}, direction='desc')", alias="r")]))
    # tier spend totals: gold 160, silver 150, bronze 40; the NULL tier's total is NULL
    expected = {"gold": 1, "silver": 2, "bronze": 3, None: None}
    for r in resp.data:
        assert r["r"] == expected[r["customers.tier"]], r


# --------------------------------------------------------------------------- #
# A NULL time bucket is adjacent to no bucket, on every dialect
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def spend_engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async for engine in make_spend_engine(request):
        yield engine


async def test_a_null_time_bucket_is_adjacent_to_no_bucket(spend_engine) -> None:
    resp = await spend_engine.execute(cust_q(time_dimensions=month_td(), metrics=[
        QueryMetric(metric="sum(customers.spend)", alias="s"),
        QueryMetric(metric="cumsum(sum(customers.spend))", alias="cs"),
        QueryMetric(metric="lag(sum(customers.spend))", alias="lg"),
        QueryMetric(metric="last(sum(customers.spend))", alias="l"),
    ]))
    got = {None if r[MONTH] is None else month_key(r[MONTH]):
           tuple(None if r[k] is None else float(r[k]) for k in ("s", "cs", "lg", "l")) for r in resp.data}
    assert got == {
        "2024-01": (100.0, 100.0, None, 110.0), "2024-02": (150.0, 250.0, 100.0, 110.0),
        "2024-03": (100.0, 350.0, 150.0, 110.0), "2024-04": (110.0, 460.0, 100.0, 110.0),
        None: (55.0, 55.0, None, 110.0),
    }
