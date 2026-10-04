"""Boolean aggregation inputs, executed on SQLite and DuckDB (hand-computed oracles).

Spec: aggregations/boolean-inputs; aggregations/expression-aggregation › "Row-level
expressions can be aggregated" (comparison source), "Gate and type semantics".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.query import SlayerQuery

from tests._dev2046_fixtures import (
    CROSS_MODEL_FLAG_SUM_BY_NAME,
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
    customers_model,
    m,
    make_exec_engine,
    month_td,
    orders_model,
    orders_q,
    seed_sqlite,
    single,
)
from tests._engine_helpers import seeded_exec_engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _num(value):
    return None if value is None else float(value)


class TestColumnAggregates:
    async def test_sum_counts_trues(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[m("sum(flag)")]))
        value = single(resp)
        assert not isinstance(value, bool)
        assert value == FLAG_TOTALS["sum"]

    async def test_avg_is_share_of_true_rows(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[m("avg(flag)")]))
        assert _num(single(resp)) == pytest.approx(FLAG_TOTALS["avg"])

    async def test_by_region(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["region"],
            measures=[m(f"{agg}(flag)", agg) for agg in ("sum", "avg", "min", "max")],
        ))
        assert {r: v for r, v in by_dim(resp, "region", "sum").items()} == FLAG_BY_REGION["sum"]
        assert {r: pytest.approx(_num(v)) for r, v in by_dim(resp, "region", "avg").items()} == FLAG_BY_REGION["avg"]
        assert {r: as_bool(v) for r, v in by_dim(resp, "region", "min").items()} == FLAG_BY_REGION["min"]
        assert {r: as_bool(v) for r, v in by_dim(resp, "region", "max").items()} == FLAG_BY_REGION["max"]

    async def test_min_max_counts(self, engine) -> None:
        """min / max stay boolean; the count family keeps the boolean (false counted, NULL not)."""
        resp = await engine.execute(orders_q(measures=[
            m("min(flag)", "mn"), m("max(flag)", "mx"), m("count(flag)", "c"),
            m("count_distinct(flag)", "cd"), m("sum(flag)", "s"),
        ]))
        row = resp.data[0]
        assert as_bool(row["orders.mn"]) is FLAG_TOTALS["min"]
        assert as_bool(row["orders.mx"]) is FLAG_TOTALS["max"]
        assert row["orders.c"] == FLAG_TOTALS["count"]
        assert row["orders.cd"] == FLAG_TOTALS["count_distinct"]
        assert row["orders.s"] == FLAG_TOTALS["sum"]
        assert not isinstance(row["orders.s"], bool)

    async def test_derived_boolean_column(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[m("sum(big_order)", "s"), m("avg(big_order)", "a")]))
        row = resp.data[0]
        assert row["orders.s"] == GT15_TOTALS["sum"]
        assert not isinstance(row["orders.s"], bool)
        assert _num(row["orders.a"]) == pytest.approx(GT15_TOTALS["avg"])

    async def test_measure_formats(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[m("sum(flag)", "s"), m("avg(flag)", "a")]))
        assert resp.attributes.measures["orders.s"].format == NumberFormat(type=NumberFormatType.INTEGER)
        assert resp.attributes.measures["orders.a"].format == NumberFormat(type=NumberFormatType.PERCENT)


class TestExpressionSources:
    @pytest.mark.parametrize("agg", ["sum", "avg", "count"])
    async def test_comparison(self, engine, agg: str) -> None:
        resp = await engine.execute(orders_q(measures=[m(f"{agg}(amount > 15)")]))
        value = single(resp)
        assert not isinstance(value, bool)
        assert _num(value) == pytest.approx(GT15_TOTALS[agg])

    async def test_comparison_by_region(self, engine) -> None:
        resp = await engine.execute(orders_q(dimensions=["region"], measures=[m("sum(amount > 15)")]))
        assert by_dim(resp, "region") == {"east": 2, "west": 0}

    async def test_boolean_expressions(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[
            m("sum(coalesce(flag, false))", "sc"),
            m("avg(coalesce(flag, false))", "ac"),
            m("sum(nullif(flag, false))", "sn"),
            m("max(coalesce(flag, false))", "mc"),
        ]))
        row = resp.data[0]
        assert row["orders.sc"] == 2
        assert not isinstance(row["orders.sc"], bool)
        assert _num(row["orders.ac"]) == pytest.approx(0.4)
        assert row["orders.sn"] == 2
        assert not isinstance(row["orders.sn"], bool)
        assert as_bool(row["orders.mc"]) is True

    async def test_in_predicate(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[
            m("sum(status in ('ok', 'hold'))", "s"),
            m("count_distinct(status in ('ok', 'hold'))", "cd"),
            m("sum(status not in ('ok', 'hold'))", "sn"),
        ]))
        row = resp.data[0]
        assert row["orders.s"] == STATUS_IN_SUM
        assert not isinstance(row["orders.s"], bool)
        assert row["orders.cd"] == 2
        assert row["orders.sn"] == 1

    async def test_time_point_comparison(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[
            m("sum(ordered_at >= '2025-02')", "s"), m("max(ordered_at >= '2025-03')", "mx"),
        ]))
        row = resp.data[0]
        assert row["orders.s"] == 3
        assert not isinstance(row["orders.s"], bool)
        assert as_bool(row["orders.mx"]) is True
        assert resp.attributes.measures["orders.s"].format == NumberFormat(type=NumberFormatType.INTEGER)

    async def test_connective(self, engine) -> None:
        resp = await engine.execute(orders_q(measures=[m("sum(flag and amount > 15)", "a"), m("sum(not flag)", "n")]))
        row = resp.data[0]
        assert row["orders.a"] == 1
        assert row["orders.n"] == 2


class TestPredicateOverAttachedValue:
    @pytest.mark.parametrize("formula,expected", [
        ("sum(count(amount) in (2, 3))", {100: 1, 200: 0}),
        ("sum(count(amount) not in (2, 3))", {100: 0, 200: 1}),
        ("sum(count(amount) > 1 and count(amount) < 5)", {100: 1, 200: 0}),
        ("count(count(amount) in (1, 2))", {100: 1, 200: 1}),
        ("sum(max(ordered_at) >= '2025-03')", {100: 0, 200: 1}),
    ])
    async def test_per_customer(self, engine, formula: str, expected: dict) -> None:
        resp = await engine.execute(orders_q(dimensions=["customer_id"], measures=[m(formula)]))
        assert by_dim(resp, "customer_id") == expected


class TestPostAggregationFilters:
    @pytest.mark.parametrize("having", ["max(flag) = true", "sum(flag) > 1", "sum(amount > 15) > 1", "avg(flag) > 0.5"])
    async def test_keeps_matching_groups(self, engine, having: str) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["region"], measures=[m("sum(flag)")], filters=[having],
        ))
        assert by_dim(resp, "region") == {"east": 2}


class TestComposition:
    async def test_cross_model_sum(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "customers", "dimensions": ["name"], "measures": [m("sum(orders.flag)")],
        }))
        values = by_dim(resp, "name", model="customers")
        assert values == CROSS_MODEL_FLAG_SUM_BY_NAME
        assert not any(isinstance(v, bool) for v in values.values())

    async def test_stage_reaggregation(self, engine) -> None:
        resp = await engine.execute([
            {"name": "s1", "source_model": "orders", "dimensions": ["customer_id"],
             "measures": [{"formula": "sum(flag)", "name": "fs"}, {"formula": "max(flag)", "name": "fm"}]},
            {"source_model": "s1",
             "measures": [{"formula": "sum(fs)", "name": "a"}, {"formula": "max(fm)", "name": "b"}]},
        ])
        row = resp.data[0]
        assert row["s1.a"] == sum(FLAG_SUM_BY_CUSTOMER.values())
        assert not isinstance(row["s1.a"], bool)
        assert as_bool(row["s1.b"]) is max(FLAG_MAX_BY_CUSTOMER.values())

    async def test_first_stage_values(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["customer_id"], measures=[m("sum(flag)", "s"), m("max(flag)", "mx")],
        ))
        assert by_dim(resp, "customer_id", "s") == FLAG_SUM_BY_CUSTOMER
        assert {k: as_bool(v) for k, v in by_dim(resp, "customer_id", "mx").items()} == FLAG_MAX_BY_CUSTOMER

    async def test_cumsum(self, engine) -> None:
        resp = await engine.execute(orders_q(
            time_dimensions=month_td(), measures=[m("sum(flag)", "s"), m("cumsum(sum(flag))", "cs")],
        ))
        assert by_month(resp, "s") == FLAG_SUM_BY_MONTH
        assert {k: _num(v) for k, v in by_month(resp, "cs").items()} == FLAG_CUMSUM_BY_MONTH
        assert "AS BOOLEAN" not in (resp.sql or "").upper()

    async def test_trailing_window(self, engine) -> None:
        resp = await engine.execute(orders_q(time_dimensions=month_td(), measures=[m("sum(flag, window='90d')")]))
        values = by_month(resp)
        assert values == FLAG_90D_BY_MONTH
        assert not any(isinstance(v, bool) for v in values.values())

    async def test_partition_by(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["region", "status"], measures=[m("sum(flag, partition_by=region)")],
        ))
        got = {(r["orders.region"], r["orders.status"]): r["orders.v"] for r in resp.data}
        assert got == {("east", "ok"): 2, ("east", "hold"): 2, ("west", "void"): 0, ("west", "ok"): 0}
        assert not any(isinstance(v, bool) for v in got.values())

    async def test_association_pick(self, engine) -> None:
        resp = await engine.execute(orders_q(
            dimensions=["region"], measures=[m("sum(customers.vip)")], to_many_handling="associate",
        ))
        values = by_dim(resp, "region")
        assert values == VIP_ASSOC_SUM_BY_REGION
        assert not any(isinstance(v, bool) for v in values.values())


class TestGates:
    async def test_avg_allowed_by_default_after_validation(self) -> None:
        """Scenario: a BOOLEAN column without allowed_aggregations validates and serves avg."""
        models = [orders_model(), customers_model()]
        flag = models[0].get_column("flag")
        assert flag is not None
        assert flag.allowed_aggregations is None
        async with seeded_exec_engine(dialect="sqlite", seed=seed_sqlite, models=models, validate=True) as (engine, _):
            resp = await engine.execute(orders_q(measures=[m("avg(flag)")]))
            assert _num(single(resp)) == pytest.approx(FLAG_TOTALS["avg"])

    @pytest.mark.parametrize("formula", ["median(flag)", "median(amount > 15)", "stddev_samp(coalesce(flag, false))"])
    async def test_statistical_over_boolean_rejected_at_binding(self, engine, formula: str) -> None:
        query = orders_q(measures=[m(formula)])
        with pytest.raises(SlayerError, match=r"(?i)boolean") as exc:
            await engine.execute(query, dry_run=True)
        assert "cannot aggregate a" not in str(exc.value)
