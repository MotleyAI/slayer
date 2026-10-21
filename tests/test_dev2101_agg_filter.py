"""``agg(…) FILTER (WHERE c)`` (spec: aggregations/functional-form › An aggregation accepts a FILTER clause)."""

from __future__ import annotations

import pytest

from slayer.core.errors import QueryTypeError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import (
    EXEC_BACKENDS,
    TIER1,
    by_key,
    emit,
    m,
    make_exec_engine,
    num,
    q,
    single,
)

PAID = "status = 'paid'"
MONTH = [{"dimension": "created_at", "granularity": "month"}]


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


async def _filtered_equals_restricted(engine: SlayerQueryEngine, formula: str, **kw) -> None:
    """``formula FILTER (WHERE paid)`` equals ``formula`` over a query restricted to the paid rows."""
    filtered = await engine.execute(q(measures=[m(f"{formula} FILTER (WHERE {PAID})")], **kw))
    restricted = await engine.execute(q(measures=[m(formula)], filters=[PAID], **kw))
    assert len(filtered.data) == len(restricted.data)
    key = lambda r: tuple(str(v) for k, v in sorted(r.items()) if k != "orders.v")  # noqa: E731
    got = {key(r): num(r["orders.v"]) for r in filtered.data}
    want = {key(r): num(r["orders.v"]) for r in restricted.data}
    assert got.keys() >= want.keys()
    for k, v in want.items():
        assert got[k] == pytest.approx(v), (formula, k)


class TestFilterValues:
    async def test_filtered_count_by_region(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m(f"count(*) FILTER (WHERE {PAID})")]))
        assert {k: num(v) for k, v in by_key(resp, "region").items()} == {"EU": 2, "US": 1, None: 0}

    async def test_filtered_sum_equals_masked_sum(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[
            m(f"sum(amount) FILTER (WHERE {PAID})", "f"), m(f"sum(iif({PAID}, amount, NULL))", "masked"),
        ]))
        filtered = {r["orders.region"]: num(r["orders.f"]) for r in resp.data}
        masked = {r["orders.region"]: num(r["orders.masked"]) for r in resp.data}
        assert filtered == masked == {"EU": 30.0, "US": 40.0, None: None}

    @pytest.mark.parametrize(("formula", "expected"), [
        ("count(discount)", 1), ("count(distinct region)", 2), ("avg(amount)", 70 / 3), ("min(amount)", 10.0),
        ("max(amount)", 40.0), ("sum(amount)", 70.0),
    ])
    async def test_builtin_aggregations(self, engine: SlayerQueryEngine, formula: str, expected: float) -> None:
        resp = await engine.execute(q(measures=[m(f"{formula} FILTER (WHERE {PAID})")]))
        assert num(single(resp)) == pytest.approx(expected)

    async def test_weighted_average_masks_numerator_and_denominator(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(f"weighted_avg(price, weight=quantity) FILTER (WHERE {PAID})")]))
        assert num(single(resp)) == pytest.approx(3.6)
        await _filtered_equals_restricted(engine, "weighted_avg(price, weight=quantity)")

    @pytest.mark.parametrize("formula", [
        "corr(price, other=quantity)", "covar_samp(price, other=quantity)", "covar_pop(price, other=quantity)",
    ])
    async def test_paired_statistics_mask_both_inputs(self, engine: SlayerQueryEngine, formula: str) -> None:
        await _filtered_equals_restricted(engine, formula)

    async def test_custom_aggregation(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(f"wsum(price) FILTER (WHERE {PAID})")]))
        assert num(single(resp)) == pytest.approx(18.0)
        await _filtered_equals_restricted(engine, "wsum(price, weight=quantity)")

    async def test_filtered_last_picks_the_newest_passing_row(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "balances", "dimensions": ["customer_id"],
            "measures": [{"formula": "last(balance, updated_at) FILTER (WHERE status = 'active')", "name": "v"}],
        }))
        assert {int(k): num(v) for k, v in by_key(resp, "customer_id", model="balances").items()} == {1: 150.0, 2: 80.0}

    async def test_filtered_first_picks_the_oldest_passing_row(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "balances", "dimensions": ["customer_id"],
            "measures": [{"formula": "first(balance, updated_at) FILTER (WHERE status = 'inactive')", "name": "v"}],
        }))
        assert {int(k): num(v) for k, v in by_key(resp, "customer_id", model="balances").items()} == {1: 999.0, 2: 70.0}

    async def test_with_partition_by(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region", "city"], measures=[
            m(f"sum(amount, partition_by=region) FILTER (WHERE {PAID})"),
        ]))
        got = {(r["orders.region"], r["orders.city"]): num(r["orders.v"]) for r in resp.data}
        assert got == {("EU", "Paris"): 30.0, ("EU", "Berlin"): 30.0, ("US", "NYC"): 40.0, ("US", "LA"): 40.0,
                       (None, "Rome"): None}

    async def test_with_window(self, engine: SlayerQueryEngine) -> None:
        await _filtered_equals_restricted(engine, "sum(amount, window='30d')", time_dimensions=MONTH)
        resp = await engine.execute(q(measures=[m(f"sum(amount, window='30d') FILTER (WHERE {PAID})")],
                                      time_dimensions=MONTH))
        got = {str(r["orders.created_at"])[:7]: num(r["orders.v"]) for r in resp.data}
        assert got == {"2024-01": 30.0, "2024-02": 40.0, "2024-03": None}


class TestFilterPositions:
    async def test_unnamed_filtered_aggregation_never_collides(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=["sum(amount)", f"sum(amount) FILTER (WHERE {PAID})"]))
        assert len(resp.columns) == 2
        assert sorted(float(v) for v in resp.data[0].values()) == [70.0, 100.0]

    async def test_post_aggregate_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("count(*)")],
                                      filters=[f"count(*) FILTER (WHERE {PAID}) > 1"]))
        assert set(by_key(resp, "region")) == {"EU"}

    async def test_order_target(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("count(*)")],
                                      order=[{"column": f"count(*) FILTER (WHERE {PAID})", "direction": "desc"}]))
        assert [r["orders.region"] for r in resp.data] == ["EU", "US", None]

    async def test_count_distinct_order_target(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["status"], measures=[m("count(*)")],
                                      order=[{"column": "count(distinct region)", "direction": "desc"},
                                             {"column": "status"}]))
        assert [r["orders.status"] for r in resp.data] == ["paid", "Pending", "void"]

    async def test_inside_arithmetic(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(f"sum(amount) FILTER (WHERE {PAID}) / sum(amount)")]))
        assert num(single(resp)) == pytest.approx(0.7)

    async def test_inside_a_transform(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(time_dimensions=MONTH, measures=[m(f"cumsum(count(*) FILTER (WHERE {PAID}))")]))
        assert sorted(float(r["orders.v"]) for r in resp.data) == [2.0, 3.0, 3.0]

    async def test_inline_model_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            source_model={"source_name": "orders", "measures": [
                {"name": "paid_total", "formula": f"sum(amount) FILTER (WHERE {PAID})"},
            ]},
            measures=["paid_total"],
        ))
        assert num(single(resp, "paid_total")) == 70.0


class TestFilterConditionIsRowLevel:
    @pytest.mark.parametrize("measures", [
        [m("sum(amount) FILTER (WHERE amount > avg(amount))")],
        [m("sum(amount) FILTER (WHERE cumsum(sum(amount)) > 1)")],
        [m("sum(amount) FILTER (WHERE amount > avg(amount, partition_by=region))")],
    ], ids=["aggregate", "transform", "partitioned-aggregate"])
    async def test_non_row_level_condition_rejected(self, engine: SlayerQueryEngine, measures) -> None:
        query = q(measures=measures)
        with pytest.raises(QueryTypeError) as info:
            await engine.execute(query)
        assert "FILTER" in str(info.value), str(info.value)


class TestFilterEmission:
    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("formula", [
        "count(*)", "sum(amount)", "count(distinct region)", "weighted_avg(price, weight=quantity)",
        "last(amount, created_at)", "corr(price, other=quantity)",
    ])
    async def test_emits(self, dialect: str, formula: str) -> None:
        await emit(q(dimensions=["region"], measures=[m(f"{formula} FILTER (WHERE {PAID})")]), dialect=dialect)
