"""Boolean aggregation inputs, executed on SQLite and DuckDB (hand-computed oracles).

Spec: aggregations/boolean-inputs; aggregations/expression-aggregation › "Row-level
expressions can be aggregated" (comparison source), "Gate and type semantics".
"""

from __future__ import annotations

import pytest

from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.query import SlayerQuery

from tests._dev2046_fixtures import (
    CROSS_DATASET_FLAG_SUM_BY_NAME,
    FLAG_90D_BY_MONTH,
    FLAG_BY_REGION,
    FLAG_CUMSUM_BY_MONTH,
    FLAG_MAX_BY_CUSTOMER,
    FLAG_SUM_BY_CUSTOMER,
    FLAG_SUM_BY_MONTH,
    FLAG_TOTALS,
    GT15_TOTALS,
    STATUS_IN_SUM,
    VIP_ASSOC_SUM_BY_REGION,
    as_bool,
    by_dim,
    by_month,
    customers_dataset,
    m,
    make_exec_engine,
    month_td,
    orders_dataset,
    orders_q,
    seed_sqlite,
    single,
)
from tests._engine_helpers import seeded_exec_engine
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _num(value):
    return None if value is None else float(value)


class TestColumnAggregates:
    async def test_sum_counts_trues(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[m("sum(orders.flag)")]))
        value = single(resp)
        assert not isinstance(value, bool)
        assert value == FLAG_TOTALS["sum"]

    async def test_avg_is_share_of_true_rows(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[m("avg(orders.flag)")]))
        assert _num(single(resp)) == pytest.approx(FLAG_TOTALS["avg"])

    async def test_by_region(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.region"],
            metrics=[m(f"{agg}(orders.flag)", agg) for agg in ("sum", "avg", "min", "max")],
        ))
        assert {r: v for r, v in by_dim(resp, "region", "sum").items()} == FLAG_BY_REGION["sum"]
        assert {r: pytest.approx(_num(v)) for r, v in by_dim(resp, "region", "avg").items()} == FLAG_BY_REGION["avg"]
        assert {r: as_bool(v) for r, v in by_dim(resp, "region", "min").items()} == FLAG_BY_REGION["min"]
        assert {r: as_bool(v) for r, v in by_dim(resp, "region", "max").items()} == FLAG_BY_REGION["max"]

    async def test_min_max_counts(self, engine) -> None:
        """min / max stay boolean; the count family keeps the boolean (false counted, NULL not)."""
        resp = await engine.execute(orders_q(metrics=[
            m("min(orders.flag)", "mn"), m("max(orders.flag)", "mx"), m("count(orders.flag)", "c"),
            m("count_distinct(orders.flag)", "cd"), m("sum(orders.flag)", "s"),
        ]))
        row = resp.data[0]
        assert as_bool(row["mn"]) is FLAG_TOTALS["min"]
        assert as_bool(row["mx"]) is FLAG_TOTALS["max"]
        assert row["c"] == FLAG_TOTALS["count"]
        assert row["cd"] == FLAG_TOTALS["count_distinct"]
        assert row["s"] == FLAG_TOTALS["sum"]
        assert not isinstance(row["s"], bool)

    async def test_derived_boolean_column(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[m("sum(orders.big_order)", "s"), m("avg(orders.big_order)", "a")]))
        row = resp.data[0]
        assert row["s"] == GT15_TOTALS["sum"]
        assert not isinstance(row["s"], bool)
        assert _num(row["a"]) == pytest.approx(GT15_TOTALS["avg"])

    async def test_measure_formats(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(orders_q(metrics=[m("sum(orders.flag)", "s"), m("avg(orders.flag)", "a")]))
        assert resp.attributes.metrics["s"].format == NumberFormat(type=NumberFormatType.INTEGER)
        assert resp.attributes.metrics["a"].format == NumberFormat(type=NumberFormatType.PERCENT)


class TestExpressionSources:
    @pytest.mark.parametrize("agg", ["sum", "avg", "count"])
    async def test_comparison(self, engine, agg: str) -> None:
        resp = await engine.execute(orders_q(metrics=[m(f"{agg}(orders.amount > 15)")]))
        value = single(resp)
        assert not isinstance(value, bool)
        assert _num(value) == pytest.approx(GT15_TOTALS[agg])

    async def test_comparison_by_region(self, engine) -> None:
        resp = await engine.execute(orders_q(dimensions=["orders.region"], metrics=[m("sum(orders.amount > 15)")]))
        # West's amounts are all NULL: a SUM over no non-NULL input is NULL (Axiom 4).
        assert by_dim(resp, "region") == {"east": 2, "west": None}

    async def test_boolean_expressions(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[
            m("sum(coalesce(orders.flag, false))", "sc"),
            m("avg(coalesce(orders.flag, false))", "ac"),
            m("sum(nullif(orders.flag, false))", "sn"),
            m("max(coalesce(orders.flag, false))", "mc"),
        ]))
        row = resp.data[0]
        assert row["sc"] == 2
        assert not isinstance(row["sc"], bool)
        assert _num(row["ac"]) == pytest.approx(0.4)
        assert row["sn"] == 2
        assert not isinstance(row["sn"], bool)
        assert as_bool(row["mc"]) is True

    async def test_in_predicate(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[
            m("sum(orders.status in ('ok', 'hold'))", "s"),
            m("count_distinct(orders.status in ('ok', 'hold'))", "cd"),
            m("sum(orders.status not in ('ok', 'hold'))", "sn"),
        ]))
        row = resp.data[0]
        assert row["s"] == STATUS_IN_SUM
        assert not isinstance(row["s"], bool)
        assert row["cd"] == 2
        assert row["sn"] == 1

    async def test_time_point_comparison(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(orders_q(metrics=[
            m("sum(orders.ordered_at >= '2025-02')", "s"), m("max(orders.ordered_at >= '2025-03')", "mx"),
        ]))
        row = resp.data[0]
        assert row["s"] == 3
        assert not isinstance(row["s"], bool)
        assert as_bool(row["mx"]) is True
        assert resp.attributes.metrics["s"].format == NumberFormat(type=NumberFormatType.INTEGER)

    async def test_connective(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[m("sum(orders.flag and orders.amount > 15)", "a"), m("sum(not orders.flag)", "n")]))
        row = resp.data[0]
        assert row["a"] == 1
        assert row["n"] == 2


class TestPredicateOverAttachedValue:
    @pytest.mark.parametrize("formula,expected", [
        ("sum(count(orders.amount) in (2, 3))", {100: 1, 200: 0}),
        ("sum(count(orders.amount) not in (2, 3))", {100: 0, 200: 1}),
        ("sum(count(orders.amount) > 1 and count(orders.amount) < 5)", {100: 1, 200: 0}),
        ("count(count(orders.amount) in (1, 2))", {100: 1, 200: 1}),
        ("sum(max(orders.ordered_at) >= '2025-03')", {100: 0, 200: 1}),
    ])
    async def test_per_customer(self, engine, formula: str, expected: dict) -> None:
        resp = await engine.execute(orders_q(dimensions=["orders.customer_id"], metrics=[m(formula)]))
        assert by_dim(resp, "customer_id") == expected


class TestPostAggregationFilters:
    @pytest.mark.parametrize("having", ["max(orders.flag) = true", "sum(orders.flag) > 1", "sum(orders.amount > 15) > 1", "avg(orders.flag) > 0.5"])
    async def test_keeps_matching_groups(self, engine, having: str) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.region"], metrics=[m("sum(orders.flag)")], filter=[having],
        ))
        assert by_dim(resp, "region") == {"east": 2}


class TestComposition:
    async def test_cross_model_sum(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "customers", "dimensions": ["customers.name"], "metrics": [m("sum(orders.flag)")],
        }))
        values = by_dim(resp, "name", dataset="customers")
        assert values == CROSS_DATASET_FLAG_SUM_BY_NAME
        assert not any(isinstance(v, bool) for v in values.values())

    async def test_stage_reaggregation(self, engine) -> None:
        resp = await engine.execute([
            {"name": "s1", "population": "orders", "dimensions": ["orders.customer_id"],
             "metrics": [{"metric": "sum(orders.flag)", "alias": "fs"}, {"metric": "max(orders.flag)", "alias": "fm"}]},
            {"population": "s1",
             "metrics": [{"metric": "sum(s1.fs)", "alias": "a"}, {"metric": "max(s1.fm)", "alias": "b"}]},
        ])
        row = resp.data[0]
        assert row["a"] == sum(FLAG_SUM_BY_CUSTOMER.values())
        assert not isinstance(row["a"], bool)
        assert as_bool(row["b"]) is max(FLAG_MAX_BY_CUSTOMER.values())

    async def test_first_stage_values(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.customer_id"], metrics=[m("sum(orders.flag)", "s"), m("max(orders.flag)", "mx")],
        ))
        assert by_dim(resp, "customer_id", "s") == FLAG_SUM_BY_CUSTOMER
        assert {k: as_bool(v) for k, v in by_dim(resp, "customer_id", "mx").items()} == FLAG_MAX_BY_CUSTOMER

    async def test_cumsum(self, engine) -> None:
        resp = await engine.execute(orders_q(
            time_dimensions=month_td(), metrics=[m("sum(orders.flag)", "s"), m("cumsum(sum(orders.flag))", "cs")],
        ))
        assert by_month(resp, "s") == FLAG_SUM_BY_MONTH
        assert {k: _num(v) for k, v in by_month(resp, "cs").items()} == FLAG_CUMSUM_BY_MONTH
        assert "AS BOOLEAN" not in (resp.sql or "").upper()

    async def test_trailing_window(self, engine) -> None:
        resp = await engine.execute(orders_q(time_dimensions=month_td(), metrics=[m("sum(orders.flag, window='90d')")]))
        values = by_month(resp)
        assert values == FLAG_90D_BY_MONTH
        assert not any(isinstance(v, bool) for v in values.values())

    async def test_partition_by(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.region", "orders.status"], metrics=[m("sum(orders.flag, partition_by=orders.region)")],
        ))
        got = {(r["orders.region"], r["orders.status"]): r["v"] for r in resp.data}
        assert got == {("east", "ok"): 2, ("east", "hold"): 2, ("west", "void"): 0, ("west", "ok"): 0}
        assert not any(isinstance(v, bool) for v in got.values())

    async def test_association_pick(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.region"], metrics=[m("sum(customers.vip)")], to_many_handling="associate",
        ))
        values = by_dim(resp, "region")
        assert values == VIP_ASSOC_SUM_BY_REGION
        assert not any(isinstance(v, bool) for v in values.values())


class TestGates:
    async def test_avg_allowed_by_default_after_validation(self) -> None:
        """Scenario: a BOOLEAN column without allowed_aggregations validates and serves avg."""
        models = [orders_dataset(), customers_dataset()]
        flag = models[0].get_field("flag")
        assert flag is not None
        assert flag.allowed_aggregations is None
        async with seeded_exec_engine(dialect="sqlite", seed=seed_sqlite, datasets=models, validate=True) as (engine, _):
            resp = await engine.execute(orders_q(metrics=[m("avg(orders.flag)")]))
            assert _num(single(resp)) == pytest.approx(FLAG_TOTALS["avg"])

    @pytest.mark.parametrize("formula,expected", [
        ("median(orders.flag)", 0.5),
        ("median(orders.amount > 15)", 1.0),
        ("percentile(orders.flag, p=0.5)", 0.5),
        ("stddev_samp(coalesce(orders.flag, false))", 0.3 ** 0.5),
        ("stddev_samp(orders.amount > 15)", (1 / 3) ** 0.5),
        ("weighted_avg(orders.flag, weight=orders.amount)", 0.5),
    ])
    async def test_statistical_over_boolean_reads_the_integer(self, engine, formula: str, expected: float) -> None:
        resp = await engine.execute(orders_q(metrics=[m(formula)]))
        assert _num(single(resp)) == pytest.approx(expected)


class TestLiteralSources:
    async def test_by_region(self, engine) -> None:
        resp = await engine.execute(orders_q(dimensions=["orders.region"], metrics=[
            m("sum(True)", "s"), m("avg(True)", "a"), m("max(True)", "mx"), m("count(True)", "c"), m("sum(1 > 2)", "f"),
        ]))
        assert by_dim(resp, "region", "s") == {"east": 3, "west": 2}
        assert {r: _num(v) for r, v in by_dim(resp, "region", "a").items()} == {"east": 1.0, "west": 1.0}
        assert {r: as_bool(v) for r, v in by_dim(resp, "region", "mx").items()} == {"east": True, "west": True}
        assert by_dim(resp, "region", "c") == {"east": 3, "west": 2}
        assert by_dim(resp, "region", "f") == {"east": 0, "west": 0}


class TestNumericPositions:
    """Outside aggregation a boolean is its integer wherever a number is needed."""

    async def test_arithmetic_and_scalars(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[
            m("sum(orders.flag * orders.amount)", "fa"), m("sum((orders.amount > 15) + 1)", "p1"), m("max(round(orders.flag))", "r"),
            m("sum(coalesce(orders.flag, 0))", "c"), m("sum(iif(orders.amount > 15, orders.flag, 2))", "i"),
        ]))
        row = resp.data[0]
        assert row["fa"] == 30
        assert row["p1"] == 5
        assert _num(row["r"]) == 1
        assert row["c"] == 2
        assert row["i"] == 7

    async def test_compared_with_a_number(self, engine) -> None:
        resp = await engine.execute(orders_q(metrics=[m("count(orders.*)")], filter=["orders.flag = 1"]))
        assert single(resp) == 2

    async def test_arithmetic_over_boolean_measures(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["orders.region"], metrics=[m("(sum(orders.amount) > 50) + (count(orders.*) > 1)")],
        ))
        assert by_dim(resp, "region") == {"east": 2, "west": None}


class TestWeightedAvg:
    async def test_null_values_skip_their_weight(self, engine) -> None:
        """Values 10/20/30 weighted 100/100/200; the two NULL-amount rows' weights (200 each) don't count."""
        resp = await engine.execute(orders_q(metrics=[m("weighted_avg(orders.amount, weight=orders.customer_id)")]))
        assert _num(single(resp)) == pytest.approx(9000 / 400)
