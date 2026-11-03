"""``FILTER (WHERE c)`` on a custom aggregation restricts every row its formula reads.

Spec: aggregations/functional-form › An aggregation accepts a FILTER clause.
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.keys import AggregateKey, ArithmeticKey, ColumnKey, LiteralKey, ValueKey
from slayer.core.query import SlayerQuery
from slayer.engine import plan
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.planned import PlannedQuery, RegroupAttachPlan
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import SQLGenerator
from tests._dev2101_fixtures import (
    CUSTOM_PAID,
    EXEC_BACKENDS,
    TIER1,
    by_key,
    check_filtered_custom_aggregations,
    customers_model,
    dev2101_models,
    emit,
    m,
    make_exec_engine,
    num,
    orders_model,
    q,
    single,
)

PAID = "status = 'paid'"
ROWS_PAID = f"rows(amount) FILTER (WHERE {PAID})"
MONTH = [{"dimension": "created_at", "granularity": "month"}]


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _by_region(resp: SlayerResponse, value: str = "v") -> dict[Any, float | None]:
    return {k: num(v) for k, v in by_key(resp, "region", value).items()}


def _by_month(resp: SlayerResponse) -> dict[str, float | None]:
    return {str(r["orders.created_at"])[:7]: num(r["orders.v"]) for r in resp.data}


class TestValues:
    @pytest.mark.parametrize(("formula", "by_region", "total"), CUSTOM_PAID, ids=[f for f, _, _ in CUSTOM_PAID])
    async def test_grouped_and_ungrouped(self, engine: SlayerQueryEngine, formula: str, by_region: dict,
                                         total: float) -> None:
        measure = m(f"{formula} FILTER (WHERE {PAID})")
        assert _by_region(await engine.execute(q(dimensions=["region"], measures=[measure]))) == pytest.approx(by_region)
        assert num(single(await engine.execute(q(measures=[measure])))) == pytest.approx(total)

    async def test_shared_server_check(self, engine: SlayerQueryEngine) -> None:
        await check_filtered_custom_aggregations(engine)

    async def test_placeholder_only_formula_equals_the_restricted_query(self, engine: SlayerQueryEngine) -> None:
        filtered = await engine.execute(q(measures=[m(f"wsum(price) FILTER (WHERE {PAID})")]))
        restricted = await engine.execute(q(measures=[m("wsum(price)")], filters=[PAID]))
        assert num(single(filtered)) == pytest.approx(18.0)
        assert num(single(filtered)) == pytest.approx(num(single(restricted)))

    async def test_ungrouped_with_no_passing_rows_reads_the_formula_over_no_rows(
        self, engine: SlayerQueryEngine,
    ) -> None:
        resp = await engine.execute(q(measures=[m("rows(amount) FILTER (WHERE status = 'none')")]))
        assert num(single(resp)) == 0


class TestComposition:
    async def test_with_partition_by(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region", "city"], measures=[
            m(f"rows(amount, partition_by=region) FILTER (WHERE {PAID})"),
        ]))
        got = {(r["orders.region"], r["orders.city"]): num(r["orders.v"]) for r in resp.data}
        assert got == {("EU", "Paris"): 2, ("EU", "Berlin"): 2, ("US", "NYC"): 1, ("US", "LA"): 1, (None, "Rome"): None}

    async def test_query_row_filter_and_clause_both_restrict(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(ROWS_PAID)], filters=["amount > 15"]))
        assert num(single(resp)) == 2

    async def test_joined_model_condition(self, engine: SlayerQueryEngine) -> None:
        measure = m("rows(amount) FILTER (WHERE customers.tier = 'gold')")
        resp = await engine.execute(q(measures=[measure]))
        assert num(single(resp)) == 3
        assert not resp.warnings
        resp = await engine.execute(q(dimensions=["region"], measures=[measure]))
        assert _by_region(resp) == {"EU": 2, "US": None, None: 1}
        assert not resp.warnings

    async def test_unfiltered_siblings_unchanged(self, engine: SlayerQueryEngine) -> None:
        siblings = [m("rows(amount)", "r"), m("count(*)", "c")]
        alone = await engine.execute(q(dimensions=["region"], measures=siblings))
        resp = await engine.execute(q(dimensions=["region"], measures=[m(ROWS_PAID), *siblings]))
        assert len(resp.data) == 3
        assert _by_region(resp) == {"EU": 2, "US": 1, None: None}
        for name in ("r", "c"):
            assert _by_region(resp, name) == _by_region(alone, name) == {"EU": 2, "US": 2, None: 1}


class TestWindow:
    async def test_unfiltered_calibration(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(time_dimensions=MONTH, measures=[m("rows(amount, window='2m')")]))
        assert _by_month(resp) == {"2024-01": 2, "2024-02": 4, "2024-03": 3}

    async def test_bucket_without_passing_rows_reads_its_frame(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(time_dimensions=MONTH, measures=[
            m(f"rows(amount, window='2m') FILTER (WHERE {PAID})"),
        ]))
        assert _by_month(resp) == {"2024-01": 2, "2024-02": 3, "2024-03": 1}

    async def test_date_range_bounds_buckets_not_the_frame(self, engine: SlayerQueryEngine) -> None:
        td = [{**MONTH[0], "date_range": ["2024-02-01", "2024-03-31"]}]
        custom = await engine.execute(q(time_dimensions=td, measures=[
            m(f"rows(amount, window='2m') FILTER (WHERE {PAID})"),
        ]))
        builtin = await engine.execute(q(time_dimensions=td, measures=[
            m(f"count(*, window='2m') FILTER (WHERE {PAID})"),
        ]))
        assert _by_month(custom) == _by_month(builtin) == {"2024-02": 3, "2024-03": 1}

    async def test_empty_frame_reads_null(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(time_dimensions=MONTH, measures=[
            m("rows(amount, window='2m') FILTER (WHERE status = 'void')"),
        ]))
        assert _by_month(resp) == {"2024-01": None, "2024-02": None, "2024-03": 1}


class TestPositions:
    async def test_saved_measure_equals_the_inline_form(self, engine: SlayerQueryEngine) -> None:
        for dims in ([], ["region"]):
            saved = await engine.execute(q(dimensions=dims, measures=[m("paid_rows")]))
            inline = await engine.execute(q(dimensions=dims, measures=[m(ROWS_PAID)]))
            assert saved.data == inline.data

    async def test_inside_arithmetic(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m(f"{ROWS_PAID} / count(*)")]))
        assert _by_region(resp) == {"EU": 1, "US": 0.5, None: None}

    async def test_measure_typed_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("count(*)")], filters=[f"{ROWS_PAID} > 1"]))
        assert set(by_key(resp, "region")) == {"EU"}

    @pytest.mark.parametrize(("direction", "expected"), [("desc", ["EU", "US", None]), ("asc", ["US", "EU", None])])
    async def test_order_target(self, engine: SlayerQueryEngine, direction: str, expected: list) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("count(*)")], order=[
            {"column": ROWS_PAID, "direction": direction, "nulls": "last"},
        ]))
        assert [r["orders.region"] for r in resp.data] == expected


def _attaches(pq: PlannedQuery) -> list[RegroupAttachPlan]:
    out = []
    for a in pq.regroup_attach_plans:
        out.append(a)
        out.extend(_attaches(a.producer_plan))
    return out


def _aggregates(pq: PlannedQuery, agg: str) -> list[AggregateKey]:
    return [s.key for s in pq.aggregate_slots if isinstance(s.key, AggregateKey) and s.key.agg == agg]


class TestProducerIsolation:
    async def test_same_condition(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[
            m(ROWS_PAID, "r"), m(f"per_order(amount) FILTER (WHERE {PAID})", "p"),
        ]))
        assert _by_region(resp, "r") == {"EU": 2, "US": 1, None: None}
        assert _by_region(resp, "p") == pytest.approx({"EU": 15, "US": 40, None: None})

    async def test_different_conditions_beside_unfiltered(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[
            m(ROWS_PAID, "paid"), m("rows(amount) FILTER (WHERE status = 'Pending')", "pending"), m("rows(amount)", "all"),
        ]))
        assert _by_region(resp, "paid") == {"EU": 2, "US": 1, None: None}
        assert _by_region(resp, "pending") == {"EU": None, "US": 1, None: None}
        assert _by_region(resp, "all") == {"EU": 2, "US": 2, None: 1}

    def test_different_conditions_never_share_a_producer(self) -> None:
        orders, *rest = dev2101_models()
        bundle = ResolvedSourceBundle(dialect="sqlite", source_model=orders, referenced_models=rest)
        planned = plan.plan_query(query=q(dimensions=["region"], measures=[
            m(ROWS_PAID, "paid"), m("rows(amount) FILTER (WHERE status = 'Pending')", "pending"),
        ]), bundle=bundle)
        producers = [a for a in _attaches(planned) if _aggregates(a.producer_plan, "rows")]
        assert len(producers) >= 2, planned.regroup_attach_plans


class TestCrossModelAndStage:
    async def test_cross_model_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "customers", "dimensions": ["name"],
            "measures": [{"formula": "rows(orders.amount) FILTER (WHERE orders.status = 'paid')", "name": "v"}],
        }))
        assert {k: num(v) for k, v in by_key(resp, "name", model="customers").items()} == {"A": 2, "B": 1, "C": None}
        assert not resp.warnings

    @pytest.mark.parametrize(("mode", "expected"), [
        ("associate", {"EU": 1, "US": None, None: 1}), ("broadcast", {"EU": 2, "US": 2, None: 2}),
    ])
    async def test_unattributable_dimension(self, engine: SlayerQueryEngine, mode: str, expected: dict) -> None:
        resp = await engine.execute(q(dimensions=["region"], to_many_handling=mode, measures=[
            m("rows(customers.name) FILTER (WHERE customers.tier = 'gold')"),
        ]))
        assert _by_region(resp) == expected

    async def test_non_final_stage(self, engine: SlayerQueryEngine) -> None:
        stage = q(name="s", dimensions=["region"], measures=[m(ROWS_PAID, "pr")])
        final = SlayerQuery.model_validate({"source_model": "s", "measures": [{"formula": "sum(pr)", "name": "v"}]})
        resp = await engine.execute([stage, final])
        assert num(single(resp, model="s")) == 3


class TestTyping:
    @pytest.mark.parametrize("condition", [
        "amount > avg(amount)",
        "cumsum(sum(amount)) > 1",
        "amount > avg(amount, partition_by=region)",
        "balances.status = 'active'",
    ], ids=["aggregate", "transform", "partitioned-aggregate", "undetermined"])
    async def test_custom_rejected_like_builtin(self, engine: SlayerQueryEngine, condition: str) -> None:
        errors = []
        for agg in ("sum", "rows"):
            query = q(measures=[m(f"{agg}(amount) FILTER (WHERE {condition})")])
            with pytest.raises(Exception) as info:  # noqa: B017 — parity, whatever the type
                await engine.execute(query)
            errors.append(info.value)
        builtin, custom = errors
        assert type(custom) is type(builtin)
        assert str(custom).splitlines()[0] == str(builtin).splitlines()[0]


class TestEmission:
    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("formula", ["rows(amount)", "per_order(amount)", "wsum(price)"])
    async def test_emits(self, dialect: str, formula: str) -> None:
        await emit(q(dimensions=["region"], measures=[m(f"{formula} FILTER (WHERE {PAID})")]), dialect=dialect)


def _paid_condition() -> ValueKey:
    return ArithmeticKey(op="=", operands=(ColumnKey(leaf="status"), LiteralKey(value="paid")))


class TestRendererFailsClosed:
    def test_filtered_custom_key_never_renders_inline(self) -> None:
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="rows", filter=_paid_condition(), builtin=False)
        gen = SQLGenerator(dialect="postgres")
        orders = orders_model()
        bundle = ResolvedSourceBundle(dialect="postgres", source_model=orders, referenced_models=[customers_model()])
        with pytest.raises(RuntimeError, match="(?i)filter"):
            gen._build_agg_render_spec_from_planned(
                slot=None, key=key, source_model=orders, source_relation="orders", full_alias="orders.v",
                bundle=bundle,
            )
