"""A query dimension's value read by measures, order targets and filters
(spec: queries/positions, queries/computed-dimensions, queries/partitioned-aggregates)."""

from __future__ import annotations

import re
from decimal import Decimal
from collections import Counter, defaultdict
from typing import List, Optional

import pytest

from slayer.core.errors import (
    PartitionKeyError,
    PositionTypingError,
    QueryTypeError,
    ReaggregationError,
    TimeAxisError,
    TransformInputError,
)
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ColumnKey,
    Grain,
    InKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    TimePointCmpKey,
    TransformKey,
    ValueKey,
)
from slayer.core.query import SlayerQuery
from slayer.engine.compile.stages import _prune_functionally_determined_grain
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql import staged_plan
from slayer.sql.generator import SQLGenerator
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1847_fixtures import (
    _SALES_ROWS_WIDE,
    ModelMeasure,
    chain_q,
    dev1847_models,
    gen,
    make_exec_engine,
    sales_q,
)
from tests._dev1865_fixtures import month_td
from tests._dev1865_fixtures import plan as orders_plan
from tests._dev1865_fixtures import q as orders_q
from tests._engine_helpers import _extract_cte_body

P = "amount:sum(partition_by=region)"
R = "avg(sum(amount, partition_by=[city, region]), partition_by=region)"
C = "amount:sum(partition_by=[city, region])"
TOT = ModelMeasure(formula="amount:sum", name="tot")
N = ModelMeasure(formula="count(*)", name="n")

RD_P = {"expression": P, "name": "rd"}
RD_R = {"expression": R, "name": "rd"}
X_C = {"expression": C, "name": "x"}
RK_P = {"expression": f"rank({P}, direction='desc')", "name": "rk"}
Q2 = {"expression": "quantity * 2", "name": "q2"}
CITY_BAND = {"expression": f"CASE WHEN {C} > 45 THEN 'hi' ELSE 'lo' END", "name": "band"}

P_BY_REGION = {"North": 90.0, "South": 140.0, "East": 180.0, "Gap": 20.0, "Void": None}
R_BY_REGION = {"North": 45.0, "South": 70.0, "East": 60.0, "Gap": 10.0, "Void": None}
TOT_BY_REGION = P_BY_REGION
QTY_TIMES_COUNT = {1.0: 4, 2.0: 12, 3.0: 9, 4.0: 4, 5.0: 5}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _m(formula: str, name: str = "m") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def _bundle(source: str = "sales") -> ResolvedSourceBundle:
    models = dev1847_models()
    [host] = [m for m in models if m.name == source]
    return ResolvedSourceBundle(dialect="duckdb", source_model=host,
                                referenced_models=[m for m in models if m is not host])


def _col(resp, name: str, key: str = "sales.region") -> dict:
    return {r[key]: r[f"sales.{name}"] for r in resp.data}


def _regions(resp) -> List[str]:
    return [r["sales.region"] for r in resp.data]


def _plus(d: dict, k: float) -> dict:
    return {r: (None if v is None else v + k) for r, v in d.items()}


def _desc_nulls_last(values: List[Optional[float]]) -> List[Optional[float]]:
    present = sorted((v for v in values if v is not None), reverse=True)
    return present + [None] * (len(values) - len(present))


def _cells_by_region_q2() -> dict:
    """``{(region, q2): (row count, sum(quantity * 2))}`` from the raw rows."""
    out: dict = defaultdict(lambda: [0, 0.0])
    for row in _SALES_ROWS_WIDE:
        region, quantity = row[1], row[5]
        cell = out[(region, quantity * 2)]
        cell[0] += 1
        cell[1] += quantity * 2
    return {k: tuple(v) for k, v in out.items()}


# --------------------------------------------------------------------------- #
# queries/computed-dimensions — arithmetic over a computed dimension's aggregate.
# --------------------------------------------------------------------------- #
class TestAggregateDimensionAsMeasure:
    async def test_partitioned_aggregate(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RD_P],
            measures=[_m(f"{P} + 1", "p1"), _m(f"{P} * 2 + amount:sum", "p2")]))
        assert _col(resp, "p1") == _plus(P_BY_REGION, 1)
        assert _col(resp, "p2") == {"North": 270.0, "South": 420.0, "East": 540.0,
                                    "Gap": 60.0, "Void": None}

    async def test_reaggregation(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RD_R],
            measures=[_m(f"{R} + 1", "r1"), _m(f"{R} * 2 + amount:sum", "r2")]))
        assert _col(resp, "r1") == _plus(R_BY_REGION, 1)
        assert _col(resp, "r2") == {"North": 180.0, "South": 280.0, "East": 300.0,
                                    "Gap": 40.0, "Void": None}

    async def test_finer_grained_aggregate_reads_the_dimension(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", X_C], measures=[_m(C, "c"), _m(f"{C} + 1", "c1")]))
        assert len(resp.data) == 9
        for row in resp.data:
            x = row["sales.x"]
            assert row["sales.c"] == x
            assert row["sales.c1"] == (None if x is None else x + 1)


class TestAggregateDimensionAsOrderTarget:
    @pytest.mark.parametrize(("dim", "agg", "expected"), [
        pytest.param(RD_P, P, ["East", "South", "North", "Gap", "Void"], id="partitioned"),
        pytest.param(RD_R, R, ["South", "East", "North", "Gap", "Void"], id="reaggregation"),
    ])
    async def test_arithmetic(self, exec_engine, dim, agg, expected):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", dim], measures=[TOT],
            order=[{"column": f"{agg} + 1", "direction": "desc"}]))
        assert _regions(resp) == expected

    async def test_arithmetic_over_transform(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RK_P], measures=[TOT],
            order=[{"column": f"rank({P}, direction='desc') + 1", "direction": "desc"}]))
        assert [r["sales.rk"] for r in resp.data] == [4, 3, 2, 1, None]

    async def test_finer_grained_aggregate(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", X_C], measures=[TOT],
            order=[{"column": C, "direction": "desc"}]))
        assert [r["sales.x"] for r in resp.data] == [
            100.0, 80.0, 60.0, 50.0, 40.0, 30.0, 12.0, 8.0, None]


class TestFinerGrainedAggregateDimensionInFilter:
    async def test_measure_typed_filter_reads_the_dimension(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", X_C], measures=[TOT], filters=[f"{C} < amount:sum"]))
        got = [(r["sales.region"], r["sales.x"], r["sales.tot"]) for r in resp.data]
        assert got == [("East", 50.0, 100.0)]


# --------------------------------------------------------------------------- #
# queries/positions — a dimension value reads the grouped value in every position.
# --------------------------------------------------------------------------- #
class TestPlainDimensionValue:
    async def test_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["quantity"], measures=[_m("quantity * count(*)")]))
        assert _col(resp, "m", key="sales.quantity") == QTY_TIMES_COUNT

    async def test_order(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["quantity"], measures=[N],
            order=[{"column": "quantity * count(*)", "direction": "desc"}]))
        assert [r["sales.quantity"] for r in resp.data][:2] == [2.0, 3.0]

    async def test_filter(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["quantity"], measures=[N], filters=["quantity * count(*) > 5"]))
        assert {r["sales.quantity"] for r in resp.data} == {2.0, 3.0}

    async def test_conditional(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[_m("iif(region == 'North', sum(amount), 0)")]))
        assert _col(resp, "m") == {"North": 90.0, "South": 0, "East": 0, "Gap": 0, "Void": 0}

    async def test_aggregate_free_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["quantity"], measures=[_m("quantity + 1")]))
        assert _col(resp, "m", key="sales.quantity") == {q: q + 1 for q in QTY_TIMES_COUNT}

    @pytest.mark.parametrize(("formula", "hits"), [
        pytest.param("quantity in (1, 2)", {1.0, 2.0}, id="in"),
        pytest.param("quantity not in (1, 2)", {3.0, 4.0, 5.0}, id="not-in"),
    ])
    async def test_aggregate_free_in_predicate(self, exec_engine, formula, hits):
        resp = await exec_engine.execute(sales_q(
            dimensions=["quantity"], measures=[_m(formula)]))
        got = {q: bool(v) for q, v in _col(resp, "m", key="sales.quantity").items()}
        assert got == {q: q in hits for q in QTY_TIMES_COUNT}

    async def test_literal_measure(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[_m("1"), _m("'x'", "s"), N]))
        assert _col(resp, "m") == dict.fromkeys(P_BY_REGION, 1)
        assert _col(resp, "s") == dict.fromkeys(P_BY_REGION, "x")


class TestPredicateDimension:
    async def test_in_dimension(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["quantity", {"expression": "quantity in (1, 2)", "name": "b"}],
            measures=[N]))
        got = {r["sales.quantity"]: bool(r["sales.b"]) for r in resp.data}
        assert got == {q: q in (1.0, 2.0) for q in QTY_TIMES_COUNT}


# An aggregate-free measure without dimensions is the one grand-total cell, even when empty.
class TestGrandTotalCell:
    @pytest.mark.parametrize("filters", [
        pytest.param([], id="all-rows"),
        pytest.param(["quantity > 100"], id="empty"),
    ])
    async def test_constant_measures(self, exec_engine, filters):
        resp = await exec_engine.execute(sales_q(
            measures=[_m("1", "lit"), _m("1 + 2", "arith"), _m("abs(-1)", "call"),
                      _m("1 in (1, 2)", "pred")],
            filters=filters))
        assert len(resp.data) == 1
        [row] = resp.data
        assert (row["sales.lit"], row["sales.arith"], row["sales.call"]) == (1, 3, 1)
        assert bool(row["sales.pred"]) is True

    async def test_cross_model(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            measures=[_m("1"), _m("customers.id:count", "c")]))
        assert resp.data == [{"corders.m": 1, "corders.c": 3}]


# --------------------------------------------------------------------------- #
# IN over aggregates: phase follows the operands, and no column is dropped.
# --------------------------------------------------------------------------- #
IN_P = f"{P} in (90, 140)"
TOP2 = "rank(amount:sum, direction='desc') in (1, 2)"
P_HIT = {"North": True, "South": True, "East": False, "Gap": False, "Void": None}
TOP2_HIT = {"North": False, "South": True, "East": True, "Gap": False, "Void": None}


def _truth(resp, key: str = "sales.region") -> dict:
    src = key.split(".", 1)[0]
    return {r[key]: (None if r[f"{src}.m"] is None else bool(r[f"{src}.m"])) for r in resp.data}


class TestInPredicateOverAggregates:
    @pytest.mark.parametrize(("dims", "formula", "expected"), [
        pytest.param(["region"], IN_P, P_HIT, id="partitioned"),
        pytest.param(["region", RD_P], IN_P, P_HIT, id="partitioned-dimension"),
        pytest.param(["region"], f"{P} not in (90, 140)",
                     {k: None if v is None else not v for k, v in P_HIT.items()}, id="not-in"),
        pytest.param(["region"], f"{R} in (45, 70)", P_HIT, id="reaggregation"),
        pytest.param(["region"], "amount:sum in (90, 140)", P_HIT, id="plain-aggregate"),
        pytest.param(["region"], TOP2, TOP2_HIT, id="transform"),
    ])
    async def test_measure(self, exec_engine, dims, formula, expected):
        resp = await exec_engine.execute(sales_q(dimensions=dims, measures=[_m(formula)]))
        assert _truth(resp) == expected

    async def test_inside_conditional(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[_m(f"iif({TOP2}, 1, 0)")]))
        assert _col(resp, "m") == {k: int(bool(v)) for k, v in TOP2_HIT.items()}

    async def test_transform_over_predicate(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[_m(f"rank({IN_P}, direction='desc')")]))
        assert _col(resp, "m") == {"North": 1, "South": 1, "East": 3, "Gap": 3, "Void": None}

    async def test_cross_model(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[_m("customers.id:count in (2, 99)")]))
        assert _truth(resp, key="corders.customers.regions.name") == {
            "North": True, "South": False}


class TestInPredicatePositionParity:
    @pytest.mark.parametrize(("formula", "hits", "ordered"), [
        pytest.param(IN_P, P_HIT, ["North", "South", "East", "Gap", "Void"], id="partitioned"),
        pytest.param(TOP2, TOP2_HIT, ["East", "South", "Gap", "North", "Void"], id="transform"),
    ])
    async def test_measure_filter_order_agree(self, exec_engine, formula, hits, ordered):
        measured = await exec_engine.execute(sales_q(dimensions=["region"],
                                                     measures=[_m(formula)]))
        assert _truth(measured) == hits
        filtered = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[TOT], filters=[formula]))
        assert {r["sales.region"] for r in filtered.data} == {k for k, v in hits.items() if v}
        by_order = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[TOT],
            order=[{"column": formula, "direction": "desc"},
                   {"column": "region", "direction": "asc"}]))
        assert [r["sales.region"] for r in by_order.data] == ordered


class TestPredicatePhase:
    _COL = ColumnKey(path=(), leaf="amount")
    _AGG = AggregateKey(source=_COL, agg="sum")
    _RANK = TransformKey(op="rank", input=_AGG)

    @pytest.mark.parametrize(("operand", "phase"), [
        pytest.param(_COL, Phase.ROW, id="row"),
        pytest.param(_AGG, Phase.AGGREGATE, id="aggregate"),
        pytest.param(_RANK, Phase.POST, id="post"),
    ])
    def test_phase_follows_the_operands(self, operand, phase):
        lit = LiteralKey(value=Decimal(1))
        assert InKey(column=operand, values=(lit,)).phase == phase
        assert TimePointCmpKey(op=">=", operand=operand, point="2025-Q1").phase == phase


class TestPredicateGrainPruning:
    def test_in_over_a_non_grain_column_is_kept(self):
        region, city = ColumnKey(path=(), leaf="region"), ColumnKey(path=(), leaf="city")
        p = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum",
                         partition_keys=Grain.of({region}))
        d = ScalarCallKey(name="iif", args=(
            InKey(column=city, values=(LiteralKey(value="Alpha"),)), p, Decimal(0)))
        assert d in _prune_functionally_determined_grain(Grain.of({region, d})).keys


class TestUnrenderedPublicSlotFailsClosed:
    async def test_combined_select(self, monkeypatch):
        monkeypatch.setattr(staged_plan, "combined_composite_slot_ids", lambda _pq: set())
        query = sales_q(dimensions=["region"], measures=[_m(f"{P} + 1")])
        with pytest.raises(ValueError, match="silently dropped"):
            await gen(query, dialect="duckdb")

    async def test_transform_chain(self, monkeypatch):
        monkeypatch.setattr(SQLGenerator, "_unmaterialised_post_slots",
                            staticmethod(lambda _pq, _aliases: []))
        query = sales_q(dimensions=["region"], measures=[_m("rank(amount:sum, direction='desc') + 1")])
        with pytest.raises(ValueError, match="silently dropped"):
            await gen(query, dialect="duckdb")


class TestComputedPlainDimensionValue:
    async def test_combined_with_count(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", Q2], measures=[_m("quantity * 2 + count(*)")]))
        cells = _cells_by_region_q2()
        got = {(r["sales.region"], r["sales.q2"]): r["sales.m"] for r in resp.data}
        assert got == {k: k[1] + n for k, (n, _) in cells.items()}


class TestJoinedDimensionValue:
    async def test_conditional_over_joined_dimension(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[_m("iif(customers.regions.name == 'North', sum(amount), 0)")]))
        got = {r["corders.customers.regions.name"]: r["corders.m"] for r in resp.data}
        assert got == {"North": 70.0, "South": 0}

    async def test_joined_key_times_aggregate(self, exec_engine):
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.region_id"],
            measures=[_m("customers.region_id * sum(amount)")]))
        got = {r["corders.customers.region_id"]: r["corders.m"] for r in resp.data}
        assert got == {1: 70.0, 2: 200.0}


class TestStageDimensionValue:
    async def test_conditional_over_stage_dimension(self, exec_engine):
        resp = await exec_engine.execute([
            SlayerQuery.model_validate({
                "name": "st", "source_model": "sales", "dimensions": ["region", "city"],
                "measures": [{"formula": "amount:sum", "name": "tot"}]}),
            SlayerQuery.model_validate({
                "source_model": "st", "dimensions": ["region"],
                "measures": [{"formula": "iif(region == 'North', sum(tot), 0)", "name": "m"}]}),
        ])
        got = {r["st.region"]: r["st.m"] for r in resp.data}
        assert got == {"North": 90.0, "South": 0, "East": 0, "Gap": 0, "Void": 0}


class TestAggregationInternalsStayRowLevel:
    async def test_partitioned_dimension_inside_outer_sum(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", RD_P], measures=[_m(f"sum({P}) + {P}")]))
        assert _col(resp, "m") == {"North": 180.0, "South": 280.0, "East": 360.0,
                                   "Gap": 40.0, "Void": None}

    async def test_computed_plain_dimension_inside_sum(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", Q2], measures=[_m("sum(quantity * 2) + quantity * 2")]))
        cells = _cells_by_region_q2()
        got = {(r["sales.region"], r["sales.q2"]): r["sales.m"] for r in resp.data}
        assert got == {k: s + k[1] for k, (_, s) in cells.items()}


# --------------------------------------------------------------------------- #
# Law 4 — measure, measure-typed filter and order agree on a dimension value.
# --------------------------------------------------------------------------- #
_PARITY = [
    pytest.param(sales_q, ["quantity"], "quantity * count(*)", "sales.quantity", 5,
                 id="plain"),
    pytest.param(sales_q, ["region"], "iif(region == 'North', sum(amount), 0)",
                 "sales.region", 0, id="conditional"),
    pytest.param(sales_q, ["region", RD_P], f"{P} + amount:sum", "sales.region", 200,
                 id="partitioned"),
    pytest.param(sales_q, ["region", RD_R], f"{R} + amount:sum", "sales.region", 150,
                 id="reaggregation"),
    pytest.param(chain_q, ["customers.regions.name"],
                 "iif(customers.regions.name == 'North', sum(amount), 0)",
                 "corders.customers.regions.name", 0, id="joined-conditional"),
    pytest.param(chain_q, ["customers.region_id"], "customers.region_id * sum(amount)",
                 "corders.customers.region_id", 100, id="joined-key"),
]


class TestPositionParity:
    @pytest.mark.parametrize(("make_q", "dims", "expr", "key", "t"), _PARITY)
    async def test_filter_and_order_follow_the_measure(self, exec_engine, make_q, dims,
                                                       expr, key, t):
        e_key = key.split(".", 1)[0] + ".e"
        measured = await exec_engine.execute(make_q(dimensions=dims, measures=[_m(expr, "e")]))
        values = {r[key]: r[e_key] for r in measured.data}

        filtered = await exec_engine.execute(make_q(
            dimensions=dims, measures=[N], filters=[f"{expr} > {t}"]))
        kept = {k for k, v in values.items() if v is not None and v > t}
        assert kept
        assert {r[key] for r in filtered.data} == kept

        ordered = await exec_engine.execute(make_q(
            dimensions=dims, measures=[N], order=[{"column": expr, "direction": "desc"}]))
        assert [values[r[key]] for r in ordered.data] == _desc_nulls_last(list(values.values()))


# --------------------------------------------------------------------------- #
# Typed errors — a row-level reference that is not a dimension value.
# --------------------------------------------------------------------------- #
def _assert_typing_error(exc: PositionTypingError, *, blocker: str, position: str) -> None:
    """``position``: ``"measure 'm'"`` (the location) or ``"order"`` / ``"filter"`` (the summary)."""
    if position.startswith("measure"):
        assert exc.location == position, str(exc)
    else:
        assert f"{position} expression" in exc.summary, exc.summary
    assert re.search(rf"(?<![\w.]){re.escape(blocker)}(?![\w(])", exc.summary), exc.summary
    assert "query grain" in exc.summary, exc.summary
    assert REGROUP_LEAF_PREFIX not in str(exc)


class TestMeasureTypingErrors:
    @pytest.mark.parametrize(("make_q", "source", "dims", "formula", "blocker"), [
        pytest.param(sales_q, "sales", ["region"], "amount + sum(amount)", "amount",
                     id="mixed"),
        pytest.param(chain_q, "corders", ["customers.regions.name"],
                     "customers.region_id * sum(amount)", "customers.region_id",
                     id="joined"),
    ])
    @pytest.mark.parametrize("position", ["measure 'm'", "order"])
    def test_non_dimension_row_reference(self, make_q, source, dims, formula, blocker,
                                         position):
        if position == "order":
            query = make_q(dimensions=dims, measures=[TOT],
                           order=[{"column": formula, "direction": "asc"}])
        else:
            query = make_q(dimensions=dims, measures=[_m(formula)])
        bundle = _bundle(source)
        with pytest.raises(PositionTypingError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_typing_error(ei.value, blocker=blocker, position=position)

    def test_aggregate_free_measure_keeps_the_aggregation_remedy(self):
        query = sales_q(dimensions=["region"], measures=[_m("round(amount, 2)")])
        bundle = _bundle()
        with pytest.raises(PositionTypingError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_typing_error(ei.value, blocker="amount", position="measure 'm'")
        assert "needs an aggregation inside an expression" in str(ei.value)
        assert "sum(amount)" in str(ei.value)

    def test_raw_time_column_under_bucketed_time_dimension(self):
        expr = "iif(ordered_at > '2024-01-15', count(*), 0)"
        as_measure = orders_q(dimensions=["region"], time_dimensions=month_td(),
                              measures=[_m(expr)])
        as_filter = orders_q(dimensions=["region"], time_dimensions=month_td(),
                             measures=[_m("amount:sum", "s")], filters=[f"{expr} > 0"])
        with pytest.raises(PositionTypingError) as measure_ei:
            orders_plan(as_measure)
        with pytest.raises(PositionTypingError) as filter_ei:
            orders_plan(as_filter)
        _assert_typing_error(measure_ei.value, blocker="ordered_at", position="measure 'm'")
        _assert_typing_error(filter_ei.value, blocker="ordered_at", position="filter")


class TestExistingMeasureErrorsTakePrecedence:
    @pytest.mark.parametrize(("dims", "formula"), [
        pytest.param(["region"], "amount:sum(partition_by=city)", id="partition-key"),
        pytest.param(["region"], "amount:sum(partition_by=city) + amount",
                     id="partition-key-and-row-reference"),
        pytest.param(["region", CITY_BAND], C, id="banded-dimension-aggregate"),
    ])
    def test_partition_key_error(self, dims, formula):
        query, bundle = sales_q(dimensions=dims, measures=[_m(formula)]), _bundle()
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=query, bundle=bundle)
        assert re.search(r"\bcity\b", ei.value.summary), ei.value.summary
        assert ei.value.location == "measure 'm'"

    @pytest.mark.parametrize(("formula", "family"), [
        pytest.param("rank(amount, direction='desc') + amount", TransformInputError, id="transform-input"),
        pytest.param("cumsum(amount:sum) + amount", TimeAxisError, id="time-axis"),
        pytest.param(f"sum({C}, window='90d') + amount", ReaggregationError,
                     id="reaggregation-window"),
    ])
    def test_other_families(self, formula, family):
        query, bundle = sales_q(dimensions=["region"], measures=[_m(formula)]), _bundle()
        with pytest.raises(QueryTypeError) as ei:
            plan_query(query=query, bundle=bundle)
        assert type(ei.value) is family, str(ei.value)


# --------------------------------------------------------------------------- #
# Plan structure — a dimension value inside a measure attaches once, row phase.
# --------------------------------------------------------------------------- #
def _is_p(key: ValueKey) -> bool:
    return (isinstance(key, AggregateKey) and key.agg == "sum"
            and key.partition_keys is not None and len(key.partition_keys) == 1)


def _is_r(key: ValueKey) -> bool:
    return isinstance(key, AggregateKey) and key.agg == "avg"


class TestDimensionValuePlan:
    @pytest.mark.parametrize(("dim", "agg", "match"), [
        pytest.param(RD_P, P, _is_p, id="partitioned"),
        pytest.param(RD_R, R, _is_r, id="reaggregation"),
    ])
    def test_row_attach_only(self, dim, agg, match):
        query = sales_q(dimensions=["region", dim], measures=[_m(f"{agg} + 1")])
        plan = plan_query(query=query, bundle=_bundle())
        phases = Counter(a.attach_phase for a in plan.regroup_attach_plans
                         if any(match(s.original_key) for s in a.substitutions))
        assert phases == Counter({"row": 1})

    @pytest.mark.parametrize(("dim", "agg", "fn"), [
        pytest.param(RD_P, P, "SUM(", id="partitioned"),
        pytest.param(RD_R, R, "AVG(", id="reaggregation"),
    ])
    async def test_one_producer_joined_once(self, dim, agg, fn):
        sql = await gen(sales_q(dimensions=["region", dim], measures=[_m(f"{agg} + 1")]),
                        dialect="duckdb")
        assert REGROUP_LEAF_PREFIX not in sql
        [cte] = [c for c in re.findall(r"(_cm_\w+) AS \(", sql)
                 if fn in _extract_cte_body(sql, re.escape(c))]
        assert len(re.findall(rf"JOIN {cte}\b", sql)) == 1, sql
        assert_scope_closed(sql, dialect="duckdb")
