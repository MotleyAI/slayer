"""Ranking keys (one predicate for both forms), default keys on query dimensions, row supply, spine,
boolean typing and time-axis refusals of ranked aggregations.

Spec: aggregations/functional-form; queries/semantics; queries/population; queries/computed-dimensions;
queries/partitioned-aggregates; queries/type-errors. Oracles are hand-computed in the comments.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import SlayerError, TimeAxisError, UnsafeRelationshipInputError
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery
from slayer.engine.plan import plan_query
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests import _dev1841_fixtures as f1841
from tests import _dev2046_fixtures as f2046
from tests import _dev2119_fixtures as f2119
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev1739_fixtures import make_exec_engine as make_region_engine
from tests._dev1739_fixtures import month_key, month_td
from tests._dev1824_fixtures import q as orders_q
from tests._dev1914_fixtures import LAST_BY_STATUS_SIGNUP, known_status_signup
from tests._dev2212_fixtures import BACKENDS, run, seed_engine
from tests._engine_helpers import seeded_exec_engine

SEG, REG = "customers.segment", "customers.region"


def _m(expr: str, name: str = "v") -> dict[str, str]:
    return {"metric": expr, "alias": name}


def _num(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def _cells(resp: SlayerResponse, *keys: str, value: str = "v") -> dict:
    out = {tuple(r[k] for k in keys): _num(r[value]) for r in resp.data}
    assert len(out) == len(resp.data), resp.data
    return out


def _grain_warnings(resp: SlayerResponse) -> list:
    return [w for w in resp.warnings or [] if getattr(w, "kind", None) in ("broadcast", "associated")]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


# --------------------------------------------------------------------------- #
# Row-form keys: an attached value the home determines (Appendix B seed)
# --------------------------------------------------------------------------- #
class TestAttachedRowKeys:
    async def test_an_attached_aggregate_ranks_the_rows(self, backend: str) -> None:
        # region averages EAST 350/3, WEST 300; keys O1 -16.7, O2 83.3, O3 -66.7, O4 0
        async with seed_engine(backend) as eng:
            resp = await run(eng, dimensions=[SEG], metrics=[_m(
                "last(orders.amount, orders.amount - avg(orders.amount, partition_by=customers.region))")])
        assert _cells(resp, SEG) == {("retail",): 200.0, ("corporate",): 50.0}

    async def test_an_attached_transform_ranks_the_rows(self, backend: str) -> None:
        # per-order totals ranked desc: O4 1, O2 2, O1 3, O3 4
        async with seed_engine(backend) as eng:
            resp = await run(eng, dimensions=[SEG], metrics=[_m(
                "last(orders.amount, rank(sum(orders.amount, partition_by=orders.order_id), direction='desc'))")])
        assert _cells(resp, SEG) == {("retail",): 100.0, ("corporate",): 50.0}

    async def test_an_unrelated_constituent_broadcasts_onto_the_operand_cells(self, backend: str) -> None:
        # (region, month) totals + 3 tickets: EAST Jan 153, Feb 203 → 203; WEST Feb 303
        async with seed_engine(backend) as eng:
            resp = await run(eng, dimensions=[REG], time_dimensions=[{"field": "orders.ordered_at", "granularity": "month"}],
                             metrics=[_m("last(sum(orders.amount) + count(tickets.*))")])
        assert {(r[REG], _num(r["v"])) for r in resp.data} == {("EAST", 203.0), ("WEST", 303.0)}

    async def test_an_attached_key_the_home_does_not_determine_fails(self, backend: str) -> None:
        metrics = [_m("last(customers.segment, count(orders.*, partition_by=orders.status))")]
        async with seed_engine(backend) as eng:
            with pytest.raises(UnsafeRelationshipInputError, match="status"):
                await run(eng, dimensions=[REG], metrics=metrics)


# --------------------------------------------------------------------------- #
# An aggregate ranks an aggregate's cells (an argmax)
# --------------------------------------------------------------------------- #
SALES = TableSpec(name="orders", columns=[("id", "INT"), ("region", "TEXT"), ("product", "TEXT"),
                                          ("revenue", "DOUBLE"), ("units", "DOUBLE")],
                  rows=[(1, "EAST", "P1", 100.0, 5.0), (2, "EAST", "P2", 40.0, 9.0),
                        (3, "WEST", "P1", 70.0, 2.0), (4, "WEST", "P3", 10.0, 1.0)])
SALES_DS = SlayerDataset(name="orders", sql_table="orders", data_source="test", fields=[
    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
    DatasetField(name="region", datatype=DataType.STRING), DatasetField(name="product", datatype=DataType.STRING),
    DatasetField(name="revenue", datatype=DataType.FLOAT), DatasetField(name="units", datatype=DataType.FLOAT)])


async def test_an_aggregate_ranks_an_aggregates_cells(backend: str) -> None:
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, [SALES]),
                                  datasets=[SALES_DS]) as (eng, _):
        resp = await eng.execute(SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["orders.region", "orders.product"],
            "metrics": [_m("last(sum(orders.revenue), sum(orders.units), partition_by=orders.region)")]}))
    assert _cells(resp, "orders.region", "orders.product") == {
        ("EAST", "P1"): 40.0, ("EAST", "P2"): 40.0, ("WEST", "P1"): 70.0, ("WEST", "P3"): 70.0}
    assert not _grain_warnings(resp)


# --------------------------------------------------------------------------- #
# Default ranking key on a query dimension (dev2119 orders)
# --------------------------------------------------------------------------- #
class TestDefaultKeyOnAQueryDimension:
    async def test_a_temporal_computed_dimension_is_dropped(self, backend: str) -> None:
        # by created month: Dec id6 60; Jan ids 1, 2 → latest EFF id2 20; Feb ids 3, 4 → id4 40; Mar id5 50
        async with f2119.engine_for(backend) as eng:
            resp = await eng.execute(f2119.q(
                dimensions=[{"field": f2119.EFF, "alias": "e"}], time_dimensions=[f2119.td("orders.created_at")],
                metrics=[f2119.m("last(orders.amount)")]))
        tkey = f2119.column(resp, "created_at")
        got = sorted((f2119.bucket(r[tkey]), _num(r["v"])) for r in resp.data)
        assert got == [("2024-12", 60.0), ("2025-01", 20.0), ("2025-01", 20.0),
                       ("2025-02", 40.0), ("2025-02", 40.0), ("2025-03", 50.0)]
        assert not _grain_warnings(resp)

    async def test_a_raw_default_time_dimension_is_dropped(self, backend: str) -> None:
        # lower(city): paris ids 1, 4 → 40; berlin ids 2, 5 → 50; nyc ids 3, 6 → id3 (latest) 30
        async with f2119.engine_for(backend) as eng:
            resp = await eng.execute(f2119.q(
                dimensions=[{"field": "lower(orders.city)", "alias": "c"}, "orders.created_at"],
                metrics=[f2119.m("last(orders.amount)")]))
        got = sorted((r["c"], _num(r["v"])) for r in resp.data)
        assert got == [("berlin", 50.0), ("berlin", 50.0), ("nyc", 30.0), ("nyc", 30.0),
                       ("paris", 40.0), ("paris", 40.0)]
        assert not _grain_warnings(resp)


# --------------------------------------------------------------------------- #
# dev1739 orders: region-month totals N Jan 30 / Feb 70, S Jan 25 / Mar 25, NULL Mar 60
# --------------------------------------------------------------------------- #
@pytest.fixture(params=list(BACKENDS))
async def region_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in make_region_engine(request):
        yield e


REGION_TOTAL = "sum(orders.amount, partition_by=[orders.region, orders.ordered_at])"


def _region_month(resp: SlayerResponse, *extra: str, value: str = "v") -> dict:
    out = {(r["orders.region"], *(r[k] for k in extra), month_key(r["orders.ordered_at"])): _num(r[value])
           for r in resp.data}
    assert len(out) == len(resp.data), resp.data
    return out


async def test_a_second_order_last_as_a_dimension(region_engine: SlayerQueryEngine) -> None:
    resp = await region_engine.execute(orders_q(
        dimensions=["orders.region", {"field": f"last({REGION_TOTAL})", "alias": "d"}],
        time_dimensions=month_td(), metrics=[QueryMetric(metric="sum(orders.amount)", alias="v")]))
    got = {(r["orders.region"], month_key(r["orders.ordered_at"])): (_num(r["d"]), _num(r["v"])) for r in resp.data}
    assert got == {("North", "2024-01"): (70.0, 30.0), ("North", "2024-02"): (70.0, 70.0),
                   ("South", "2024-01"): (25.0, 25.0), ("South", "2024-03"): (25.0, 25.0),
                   (None, "2024-03"): (60.0, 60.0)}


class TestRankedOverACityGrainedOperand:
    async def test_without_the_bucket_fails(self, region_engine: SlayerQueryEngine) -> None:
        query = orders_q(dimensions=["orders.region"], time_dimensions=month_td(), metrics=[
            QueryMetric(metric="last(sum(orders.amount, partition_by=[orders.city, orders.region]))", alias="v")])
        with pytest.raises(TimeAxisError):
            await region_engine.execute(query)

    async def test_with_the_bucket_executes_at_city_grain(self, region_engine: SlayerQueryEngine) -> None:
        expr = "last(sum(orders.amount, partition_by=[orders.city, orders.region, orders.ordered_at]))"
        resp = await region_engine.execute(orders_q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric=expr, alias="v")]))
        assert _region_month(resp, "orders.city") == {
            ("North", "CityA", "2024-01"): 30.0, ("North", "CityB", "2024-02"): 40.0,
            ("North", None, "2024-02"): 30.0, ("South", "CityC", "2024-01"): 25.0,
            ("South", "CityC", "2024-03"): 25.0, (None, "CityD", "2024-03"): 60.0}


AXISLESS = "last(sum(orders.amount, partition_by=orders.region))"


@pytest.mark.parametrize("kw", [
    {"metrics": [QueryMetric(metric=AXISLESS, alias="v")]},
    {"metrics": [QueryMetric(metric="sum(orders.amount)", alias="v")], "filter": [f"{AXISLESS} > 0"]},
    {"metrics": [QueryMetric(metric="sum(orders.amount)", alias="v")],
     "order": [OrderItem(field=AXISLESS, direction="desc")]},
    {"metrics": [QueryMetric(metric=f"max({AXISLESS})", alias="v")]},
], ids=["measure", "filter", "order", "aggregation_source"])
async def test_a_ranked_aggregation_without_the_axis_fails_in_every_position(region_engine, kw) -> None:
    query = orders_q(dimensions=["orders.region"], time_dimensions=month_td(), **kw)
    with pytest.raises(TimeAxisError) as info:
        await region_engine.execute(query)
    assert not isinstance(info.value, NotImplementedError)


async def test_no_time_dimension_fails(region_engine: SlayerQueryEngine) -> None:
    query = orders_q(dimensions=["orders.region"], metrics=[QueryMetric(metric="last(sum(orders.amount))", alias="v")])
    with pytest.raises(TimeAxisError):
        await region_engine.execute(query)


# --------------------------------------------------------------------------- #
# A spine-backed operand picks its empty last bucket (daily sales 10 on 2025-01-05, 30 on 2025-03-05)
# --------------------------------------------------------------------------- #
DAILY = TableSpec(name="sales", columns=[("id", "INT"), ("day", "DATE"), ("amount", "DOUBLE")],
                  rows=[(1, "2025-01-05", 10.0), (2, "2025-03-05", 30.0)])
DAILY_DS = SlayerDataset(name="sales", sql_table="sales", data_source="test", default_time_dimension="day",
                         fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                                 DatasetField(name="day", datatype=DataType.DATE),
                                 DatasetField(name="amount", datatype=DataType.FLOAT)],
                         metrics=[DatasetMetric(name="n_last", expression="last(count(*))")])


@pytest.mark.parametrize("expr", ["last(count(sales.*))", "sales.n_last"], ids=["inline", "stored"])
async def test_a_spine_backed_operand_picks_its_empty_last_bucket(backend: str, expr: str) -> None:
    metric = [_m(expr)]
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, [DAILY]),
                                  datasets=[DAILY_DS]) as (eng, _):
        spine = await eng.execute(SlayerQuery.model_validate({"metrics": metric, "time_dimensions": [
            {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-04-30"]}]}))
        plain = await eng.execute(SlayerQuery.model_validate({"population": "sales", "metrics": metric,
                                                              "time_dimensions": [{"field": "sales.day", "granularity": "month"}]}))
    assert sorted(float(r["v"]) for r in spine.data) == [0.0, 0.0, 0.0, 0.0]
    assert sorted(float(r["v"]) for r in plain.data) == [1.0, 1.0]


# --------------------------------------------------------------------------- #
# Boolean typing (dev2046 orders: max(flag) by month Jan true, Feb true, Mar false → last false)
# --------------------------------------------------------------------------- #
LAST_FLAG = "last(max(orders.flag))"
BOOL_CASES: dict[str, tuple[dict, dict]] = {
    "measure": ({"metrics": [_m(LAST_FLAG)]},
                {"2025-01": False, "2025-02": False, "2025-03": False}),
    "comparison": ({"metrics": [_m(f"{LAST_FLAG} = max(orders.flag)")]},
                   {"2025-01": False, "2025-02": False, "2025-03": True}),
    "filter_kept": ({"metrics": [_m(LAST_FLAG)], "filter": [f"{LAST_FLAG} = false"]},
                    {"2025-01": False, "2025-02": False, "2025-03": False}),
    "filter_dropped": ({"metrics": [_m(LAST_FLAG)], "filter": [f"{LAST_FLAG} = true"]}, {}),
}


def _flag_query(kw: dict) -> SlayerQuery:
    return f2046.orders_q(time_dimensions=f2046.month_td(), **kw)


def _bool_months(resp: SlayerResponse) -> dict:
    return {f2046.month_key(r[k]): f2046.as_bool(r["v"]) for r in resp.data for k in resp.columns if "ordered_at" in k}


class TestBooleanTyping:
    @pytest.mark.parametrize("case", list(BOOL_CASES))
    async def test_values(self, backend: str, case: str) -> None:
        kw, want = BOOL_CASES[case]
        async with _flag_engine(backend) as eng:
            resp = await eng.execute(_flag_query(kw))
        assert _bool_months(resp) == want

    @pytest.mark.parametrize("expr", [LAST_FLAG, f"{LAST_FLAG} = max(orders.flag)"])
    def test_plan_types_boolean(self, expr: str) -> None:
        models = f2046.dev2046_datasets()
        bundle = ResolvedSourceBundle(dialect="duckdb", population=models[0], referenced_datasets=models[1:])
        planned = plan_query(query=_flag_query({"metrics": [_m(expr)]}), bundle=bundle)
        # A re-aggregation's value reaches the stage as a row slot; a comparison over it is a combined one.
        (slot,) = [s for s in [*planned.row_slots, *planned.aggregate_slots, *planned.combined_expression_slots]
                   if s.public_name == "v"]
        assert slot.type == DataType.BOOLEAN

    async def test_as_an_aggregation_source(self, backend: str) -> None:
        async with _flag_engine(backend) as eng:
            resp = await eng.execute(_flag_query({"metrics": [_m(f"count({LAST_FLAG})")]}))
        assert sorted(float(r["v"]) for r in resp.data) == [1.0, 1.0, 1.0]


@contextlib.asynccontextmanager
async def _flag_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    seed = f2046.seed_duckdb if backend == "duckdb" else f2046.seed_sqlite
    async with seeded_exec_engine(dialect=backend, seed=seed, datasets=f2046.dev2046_datasets()) as (eng, _):
        yield eng


# --------------------------------------------------------------------------- #
# Row supply: first / last supply the rows sum supplies (ranked-supply seed)
# --------------------------------------------------------------------------- #
SUPPLY_TABLES = [
    TableSpec(name="customers", columns=[("id", "INT"), ("name", "TEXT"), ("credit", "DOUBLE"), ("signup_at", "DATE")],
              rows=[(1, "Ann", 50.0, "2024-03-01"), (2, "Bob", 70.0, "2024-02-01"), (3, "Cy", 60.0, "2024-01-15"),
                    (4, "Eve", None, "2024-04-01"), (5, "Fay", 25.0, "2024-05-01")]),
    TableSpec(name="orders", columns=[("id", "INT"), ("customer_id", "INT"), ("status", "TEXT"), ("amount", "DOUBLE"),
                                      ("ordered_at", "DATE")],
              rows=[(1, 1, "open", 30.0, "2024-03-01"), (2, 2, "paid", 5.0, "2024-02-01"),
                    (3, 3, "open", 60.0, "2024-04-01"), (4, 3, "paid", 8.0, "2024-01-10")]),
    TableSpec(name="order_items", columns=[("id", "INT"), ("order_id", "INT"), ("product_id", "INT"), ("qty", "DOUBLE")],
              rows=[(1, 1, 1, 1.0), (2, 2, 2, 1.0), (3, 4, 2, 1.0)]),
    TableSpec(name="products", columns=[("id", "INT"), ("category", "TEXT")],
              rows=[(1, "food"), (2, "toys"), (5, "books")]),
]


def _rel(to: str, frm: str) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=[frm], to_columns=["id"],
                               cardinality=RelationshipCardinality.MANY_TO_ONE)


def _ds(name: str, fields: list[tuple[str, DataType]], rels: Sequence[DatasetRelationship] = (), **kw: Any) -> SlayerDataset:
    return SlayerDataset(name=name, sql_table=name, data_source="test", relationships=list(rels), **kw, fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        *(DatasetField(name=n, datatype=t) for n, t in fields)])


SUPPLY_DATASETS = [
    _ds("customers", [("name", DataType.STRING), ("credit", DataType.FLOAT), ("signup_at", DataType.DATE)]),
    _ds("orders", [("customer_id", DataType.INTEGER), ("status", DataType.STRING), ("amount", DataType.FLOAT),
                   ("ordered_at", DataType.DATE)], [_rel("customers", "customer_id")], default_time_dimension="ordered_at"),
    _ds("order_items", [("order_id", DataType.INTEGER), ("product_id", DataType.INTEGER), ("qty", DataType.FLOAT)],
        [_rel("orders", "order_id"), _rel("products", "product_id")]),
    _ds("products", [("category", DataType.STRING)]),
]


@contextlib.asynccontextmanager
async def _supply_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, SUPPLY_TABLES),
                                  datasets=SUPPLY_DATASETS) as (eng, _):
        yield eng


class TestRankedRowSupply:
    async def test_an_associated_ranked_metric_supplies_the_tuples_its_home_rows_reach(self, backend: str) -> None:
        # open: Ann (03-01) 50, Cy 60 → Ann; paid: Bob (02-01) 70, Cy (01-15) → Bob; no orders: Eve, Fay (latest) 25
        async with _supply_engine(backend) as eng:
            ranked = await run(eng, dimensions=["orders.status"], metrics=[_m("last(customers.credit, customers.signup_at)")])
            summed = await run(eng, dimensions=["orders.status"], metrics=[_m("sum(customers.credit)")])
        assert _cells(ranked, "orders.status") == {("open",): 50.0, ("paid",): 70.0, (None,): 25.0}
        assert set(_cells(ranked, "orders.status")) == set(_cells(summed, "orders.status"))

    async def test_a_ranked_metric_adds_no_row_its_home_rows_do_not_reach(self, backend: str) -> None:
        # food: order 1 (30); toys: orders 2 (02-01, 5), 4 (01-10, 8) → 5; no items: order 3 (60); books: no item
        async with _supply_engine(backend) as eng:
            ranked = await run(eng, dimensions=["products.category"], metrics=[_m("last(orders.amount)")])
            summed = await run(eng, dimensions=["products.category"], metrics=[_m("sum(orders.amount)")])
        assert _cells(ranked, "products.category") == {("food",): 30.0, ("toys",): 5.0, (None,): 60.0}
        assert set(_cells(ranked, "products.category")) == set(_cells(summed, "products.category"))


# --------------------------------------------------------------------------- #
# Associated ranked picks associate like any aggregation; error mode refuses
# --------------------------------------------------------------------------- #
@pytest.fixture(params=list(BACKENDS))
async def assoc_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async for e in f1841.make_exec_engine(request):
        yield e


ASSOC_KW = {
    "dimensions": ["orders.status"],
    "time_dimensions": [f1841.TimeDimension(field="customers.signup_at", granularity=f1841.TimeGranularity.MONTH)],
    "metrics": [QueryMetric(metric="last(customers.spend)", alias="l")],
}


async def test_an_associated_ranked_pick_associates(assoc_engine: SlayerQueryEngine) -> None:
    resp = await assoc_engine.execute(f1841.assoc_q(**ASSOC_KW))
    assert known_status_signup(resp, "l") == LAST_BY_STATUS_SIGNUP


async def test_an_associated_ranked_pick_refuses_under_error_mode(assoc_engine: SlayerQueryEngine) -> None:
    query = f1841.error_q(**ASSOC_KW)
    with pytest.raises((SlayerError, ValueError), match="status") as info:
        await assoc_engine.execute(query)
    assert not isinstance(info.value, NotImplementedError)
