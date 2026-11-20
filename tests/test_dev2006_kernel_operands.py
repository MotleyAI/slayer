"""Re-aggregation over ranked (first/last) and windowed operands.

Spec: openspec …/specs/queries/partitioned-aggregates — "Re-aggregation over ranked
and windowed operands"; python.sql.arc42.md P10 (own ordering / own frame → own producer).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Optional

import pytest

from slayer.core.enums import RANKED_AGGREGATIONS
from slayer.core.errors import ReaggregationError, TimeAxisError
from slayer.core.keys import AggregateKey, FieldKey, walk_value_keys, window_kwarg_of
from slayer.engine import plan
from slayer.engine.compile import stages
from slayer.ir.planned import (
    PlainProducerKernel,
    PlannedQuery,
    RankedProducerKernel,
    RegroupAttachPlan,
    TrailingWindowProducerKernel,
    regroup_producer_identity,
)
from slayer.sql.generator import SQLGenerator
from slayer.sql.scope_check import assert_scope_closed

from slayer.core.query import QueryMetric

from tests import _dev1841_fixtures as f1841
from tests._dev1914_fixtures import LAST_BY_STATUS_SIGNUP, known_status_signup
from tests._dev2006_fixtures import (
    AVG_LAST_BY_CUSTOMER,
    BALANCE_BY_BAND,
    COUNT_LAST_BY_CUSTOMER,
    CROSS_MODEL_COUNT_BY_NAME,
    CROSS_MODEL_SUM_BY_NAME,
    CUMSUM_LAST_BY_CUSTOMER_MONTH,
    FEB,
    FIRST,
    FIRST_BY_CUSTOMER,
    GLOBAL_LAST_BY_ACCOUNT,
    JAN,
    LAST,
    LAST_FAMILY_BY_CUSTOMER,
    LAST_BY_CUSTOMER,
    LAST_BY_CUSTOMER_MONTH,
    LAST_MINUS_FIRST_BY_CUSTOMER,
    LAST_PER_ROW_BY_CUSTOMER,
    LAST_PLUS_LAST_RECORDED_BY_CUSTOMER,
    LAST_PLUS_MAX_BY_CUSTOMER,
    LAST_RECORDED_BY_CUSTOMER,
    PB,
    PB_MONTH,
    SHIFTED_30D_BY_CUSTOMER_MONTH,
    SHIFTED_LAST_BY_CUSTOMER_MONTH,
    WEIGHTED_AVG_GLOBAL,
    WINDOW_30D_BY_CUSTOMER_MONTH,
    WINDOW_30D_PLUS_60D_BY_CUSTOMER_MONTH,
    by_customer,
    by_customer_month,
    dev2006_bundle,
    dev2006_datasets,
    m,
    make_exec_engine,
    month_td,
    num,
    snap_q,
    warnings_of,
)
from tests._engine_helpers import _engine_generate

SEMI = f"sum(last(account_snapshots.balance, account_snapshots.snapshot_date, {PB}))"
TWO_RANKINGS = f"sum(last(account_snapshots.balance, account_snapshots.snapshot_date, {PB}) + last(account_snapshots.balance, account_snapshots.recorded_at, {PB}))"
TWO_WINDOWS = f"sum(sum(account_snapshots.balance, window='30d', {PB}) + sum(account_snapshots.balance, window='60d', {PB}))"
CM_LAST = "last(account_snapshots.balance, partition_by=[account_snapshots.account_id, customers.id])"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def dev1841_engine(request):
    async for engine in f1841.make_exec_engine(request):
        yield engine


def _by_customer_q(*formulas: str, **kw):
    return snap_q(dimensions=["account_snapshots.customer_id"], metrics=[m(f, f"v{i}" if i else "v") for i, f in enumerate(formulas)], **kw)


def _approx(actual: dict, expected: dict) -> None:
    assert set(actual) == set(expected)
    for k, v in expected.items():
        assert actual[k] == (None if v is None else pytest.approx(v)), k


async def _sql(query) -> str:
    models = dev2006_datasets()
    return await _engine_generate(query=query, dataset=models[0], extra_datasets=models[1:], dialect="duckdb")


def _plan(query, *, root: str = "account_snapshots") -> PlannedQuery:
    return plan.plan_query(query=query, bundle=dev2006_bundle(root=root))


def _kernel_requiring(k) -> bool:
    return isinstance(k, AggregateKey) and (
        k.agg in RANKED_AGGREGATIONS or window_kwarg_of(k) is not None
    )


def _attaches(
    pq: PlannedQuery, owner: Optional[RegroupAttachPlan] = None,
) -> Iterator[tuple[Optional[RegroupAttachPlan], RegroupAttachPlan]]:
    """(owning attach or None, attach) over the whole producer tree."""
    for a in pq.regroup_attach_plans:
        yield owner, a
        yield from _attaches(a.producer_plan, owner=a)


def _kernel_slot_keys(pq: PlannedQuery, kernel=None):
    """(kernel of the producer owning the slot, kernel-requiring key) over the tree."""
    for s in [*pq.aggregate_slots, *pq.combined_expression_slots]:
        for k in walk_value_keys(s.key):
            if _kernel_requiring(k):
                yield kernel, k
    for a in pq.regroup_attach_plans:
        yield from _kernel_slot_keys(a.producer_plan, kernel=a.kernel)


def _assert_kernel_invariant(pq: PlannedQuery) -> None:
    """A ranked key renders only in a ranked kernel producer, a windowed one in a window producer."""
    for kernel, k in _kernel_slot_keys(pq):
        want = TrailingWindowProducerKernel if window_kwarg_of(k) is not None else RankedProducerKernel
        assert isinstance(kernel, want), f"{k} renders under {type(kernel).__name__}"


def _kernel_attaches(pq: PlannedQuery, kind: type) -> list[tuple[Optional[RegroupAttachPlan], RegroupAttachPlan]]:
    return [(o, a) for o, a in _attaches(pq) if isinstance(a.kernel, kind)]


def _ranked_kernel(attach: RegroupAttachPlan) -> RankedProducerKernel:
    assert isinstance(attach.kernel, RankedProducerKernel)
    return attach.kernel


def _ranking_leaf(attach: RegroupAttachPlan) -> str:
    key = _ranked_kernel(attach).ranking_time_key
    assert isinstance(key, FieldKey)
    return key.leaf


def _window_raw(attach: RegroupAttachPlan) -> str:
    assert isinstance(attach.kernel, TrailingWindowProducerKernel)
    return attach.kernel.window_raw


# --------------------------------------------------------------------------- #
# Executed values
# --------------------------------------------------------------------------- #
class TestSemiAdditive:
    async def test_last_balance_summed_per_customer(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(SEMI))
        _approx(by_customer(resp), LAST_BY_CUSTOMER)

    async def test_implicit_ranking_column(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"sum({LAST})"))
        _approx(by_customer(resp), LAST_BY_CUSTOMER)

    async def test_explicit_recorded_at_ranking_column(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"sum(last(account_snapshots.balance, account_snapshots.recorded_at, {PB}))"))
        _approx(by_customer(resp), LAST_RECORDED_BY_CUSTOMER)


class TestOuterAggregationFamily:
    async def test_avg_count_and_sum_of_first(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"avg({LAST})", f"count({LAST})", f"sum({FIRST})"))
        _approx(by_customer(resp, "v"), AVG_LAST_BY_CUSTOMER)
        _approx(by_customer(resp, "v1"), COUNT_LAST_BY_CUSTOMER)
        _approx(by_customer(resp, "v2"), FIRST_BY_CUSTOMER)

    @pytest.mark.parametrize("outer", sorted(LAST_FAMILY_BY_CUSTOMER))
    async def test_scalar_family(self, exec_engine, outer):
        resp = await exec_engine.execute(_by_customer_q(f"{outer}({LAST})"))
        _approx(by_customer(resp), LAST_FAMILY_BY_CUSTOMER[outer])


class TestSeveralConstituents:
    async def test_last_minus_first(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"sum({LAST} - {FIRST})"))
        _approx(by_customer(resp), LAST_MINUS_FIRST_BY_CUSTOMER)

    async def test_each_last_keeps_its_own_ranking_column(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(TWO_RANKINGS))
        _approx(by_customer(resp), LAST_PLUS_LAST_RECORDED_BY_CUSTOMER)

    async def test_ranked_and_plain_constituents_mixed(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"sum({LAST} + max(account_snapshots.balance, {PB}))"))
        _approx(by_customer(resp), LAST_PLUS_MAX_BY_CUSTOMER)


class TestBucketedOperand:
    @pytest.mark.parametrize("ranking", ["", "account_snapshots.snapshot_date, "])
    async def test_month_bucketed_operand(self, exec_engine, ranking):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
            metrics=[m(f"sum(last(account_snapshots.balance, {ranking}{PB_MONTH}))")],
        ))
        _approx(by_customer_month(resp), LAST_BY_CUSTOMER_MONTH)


class TestWindowedOperands:
    async def _single_stage(self, engine) -> dict:
        resp = await engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
            metrics=[m("sum(account_snapshots.balance, window='30d')")],
        ))
        return by_customer_month(resp)

    @pytest.mark.parametrize("formula", [
        "sum(sum(account_snapshots.balance, window='30d'))",
        f"sum(sum(account_snapshots.balance, window='30d', {PB}))",
    ])
    async def test_equals_the_single_stage_windowed_measure(self, exec_engine, formula):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(), metrics=[m(formula)],
        ))
        vals = by_customer_month(resp)
        _approx(vals, WINDOW_30D_BY_CUSTOMER_MONTH)
        _approx(vals, await self._single_stage(exec_engine))

    async def test_each_windowed_inner_keeps_its_own_window(self, exec_engine):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(), metrics=[m(TWO_WINDOWS)],
        ))
        _approx(by_customer_month(resp), WINDOW_30D_PLUS_60D_BY_CUSTOMER_MONTH)

    async def test_without_a_time_dimension_is_a_typed_error(self, exec_engine):
        query = _by_customer_q(f"sum(sum(account_snapshots.balance, window='30d', {PB}))")
        with pytest.raises(TimeAxisError):
            await exec_engine.execute(query)


class TestConsumerPositions:
    async def test_measure_typed_filter(self, exec_engine):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], metrics=[m("sum(account_snapshots.balance)")],
            filter=[f"sum({LAST}) > 100"],
        ))
        assert [r["account_snapshots.customer_id"] for r in resp.data] == [100]

    @pytest.mark.parametrize("direction,expected", [("desc", [100, 200]), ("asc", [200, 100])])
    async def test_order_key(self, exec_engine, direction, expected):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], metrics=[m("sum(account_snapshots.balance)")],
            order=[{"field": f"sum({LAST})", "direction": direction}],
        ))
        assert [r["account_snapshots.customer_id"] for r in resp.data] == expected

    async def test_inside_arithmetic(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"sum({LAST}) / count(account_snapshots.*)"))
        _approx(by_customer(resp), LAST_PER_ROW_BY_CUSTOMER)

    async def test_transform_input(self, exec_engine):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
            metrics=[m(f"cumsum(sum(last(account_snapshots.balance, {PB_MONTH})))")],
        ))
        _approx(by_customer_month(resp), CUMSUM_LAST_BY_CUSTOMER_MONTH)

    async def test_computed_dimension(self, exec_engine):
        band = {
            "field": f"CASE WHEN sum({LAST}, partition_by=[account_snapshots.customer_id]) > 100 THEN 'big' ELSE 'small' END",
            "alias": "band",
        }
        resp = await exec_engine.execute(snap_q(dimensions=[band], metrics=[m("sum(account_snapshots.balance)")]))
        vals = {r["band"]: num(r["v"]) for r in resp.data}
        _approx(vals, BALANCE_BY_BAND)

    async def test_aggregation_parameter(self, exec_engine):
        resp = await exec_engine.execute(snap_q(metrics=[m(
            f"weighted_avg(account_snapshots.balance, weight=sum({LAST}, partition_by=[account_snapshots.customer_id]))"
        )]))
        assert num(resp.data[0]["v"]) == pytest.approx(WEIGHTED_AVG_GLOBAL)

    async def test_explicit_outer_partition(self, exec_engine):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
            metrics=[m(f"sum({LAST}, partition_by=[account_snapshots.customer_id])")],
        ))
        expected = {(c, mo): v for c, v in LAST_BY_CUSTOMER.items() for mo in (JAN, FEB)}
        _approx(by_customer_month(resp), expected)

    async def test_nested_reaggregation(self, exec_engine):
        resp = await exec_engine.execute(snap_q(metrics=[m(f"max(sum({LAST}, partition_by=[account_snapshots.customer_id]))")]))
        assert num(resp.data[0]["v"]) == pytest.approx(LAST_BY_CUSTOMER[100])


class TestStructuralTwins:
    async def test_both_consumers_read_the_same_values(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(f"sum({LAST})", f"avg({LAST})"))
        _approx(by_customer(resp, "v"), LAST_BY_CUSTOMER)
        _approx(by_customer(resp, "v1"), AVG_LAST_BY_CUSTOMER)

    async def test_the_ranked_operand_is_computed_once(self):
        sql = await _sql(_by_customer_q(f"sum({LAST})", f"avg({LAST})"))
        assert sql.count("ROW_NUMBER()") == 1


class TestCrossModelOperand:
    async def test_sum_and_count_per_customer_name(self, exec_engine):
        resp = await exec_engine.execute(snap_q(
            population="customers", dimensions=["customers.name"],
            metrics=[m(f"sum({CM_LAST})"), m(f"count({CM_LAST})", "c")],
        ))
        sums = {r["customers.name"]: num(r["v"]) for r in resp.data}
        counts = {r["customers.name"]: r["c"] for r in resp.data}
        _approx(sums, CROSS_MODEL_SUM_BY_NAME)
        assert counts == CROSS_MODEL_COUNT_BY_NAME


class TestAttributabilityModes:
    FORMULA = "sum(last(account_snapshots.balance, partition_by=[account_snapshots.account_id]))"

    async def test_broadcast_repeats_the_global_value(self, exec_engine):
        resp = await exec_engine.execute(_by_customer_q(self.FORMULA, to_many_handling="broadcast"))
        _approx(by_customer(resp), {100: GLOBAL_LAST_BY_ACCOUNT, 200: GLOBAL_LAST_BY_ACCOUNT})
        assert warnings_of(resp, "broadcast")

    @pytest.mark.parametrize("mode", [None, "associate"])
    async def test_associate_reconciles_per_customer(self, exec_engine, mode):
        resp = await exec_engine.execute(_by_customer_q(self.FORMULA, to_many_handling=mode))
        _approx(by_customer(resp), LAST_BY_CUSTOMER)
        assert warnings_of(resp, "associated")

    async def test_error_mode_refuses_typed(self, exec_engine):
        query = _by_customer_q(self.FORMULA, to_many_handling="error")
        with pytest.raises(ReaggregationError, match="customer_id"):
            await exec_engine.execute(query)


class TestAssociatedRankedPick:
    KW = {
        "dimensions": ["orders.status"],
        "time_dimensions": [f1841.TimeDimension(
            field="customers.signup_at", granularity=f1841.TimeGranularity.MONTH)],
        "metrics": [QueryMetric(metric="last(customers.spend)", alias="l")],
    }

    async def test_cross_model_last_needing_association(self, dev1841_engine):
        resp = await dev1841_engine.execute(f1841.assoc_q(**self.KW))
        assert known_status_signup(resp, "l") == LAST_BY_STATUS_SIGNUP
        assert warnings_of(resp, "associated")

    async def test_default_mode_associates(self, dev1841_engine):
        resp = await dev1841_engine.execute(f1841.q(**self.KW))
        assert known_status_signup(resp, "l") == LAST_BY_STATUS_SIGNUP
        assert warnings_of(resp, "associated")
        assert not warnings_of(resp, "broadcast")


# --------------------------------------------------------------------------- #
# Plan structure (no DB)
# --------------------------------------------------------------------------- #
class TestPlanStructure:
    def test_carrier_nests_the_ranked_kernel(self) -> None:
        pq = _plan(_by_customer_q(SEMI))
        _assert_kernel_invariant(pq)
        [(carrier, ranked)] = _kernel_attaches(pq, RankedProducerKernel)
        assert carrier is not None
        assert carrier.attach_phase == "row"
        assert _ranked_kernel(ranked).agg == "last"
        assert _ranking_leaf(ranked) == "snapshot_date"

    def test_distinct_ranking_keys_get_distinct_producers(self) -> None:
        pq = _plan(_by_customer_q(TWO_RANKINGS))
        _assert_kernel_invariant(pq)
        ranked = [a for _, a in _kernel_attaches(pq, RankedProducerKernel)]
        assert sorted(_ranking_leaf(a) for a in ranked) == ["recorded_at", "snapshot_date"]
        assert len({regroup_producer_identity(a) for a in ranked}) == 2

    def test_distinct_windows_get_distinct_producers(self) -> None:
        pq = _plan(snap_q(dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(), metrics=[m(TWO_WINDOWS)]))
        _assert_kernel_invariant(pq)
        windowed = [a for _, a in _kernel_attaches(pq, TrailingWindowProducerKernel)]
        assert sorted(_window_raw(a) for a in windowed) == ["30d", "60d"]
        assert len({regroup_producer_identity(a) for a in windowed}) == 2

    def test_structural_twin_interns_to_one_producer(self) -> None:
        pq = _plan(_by_customer_q(f"sum({LAST})", f"avg({LAST})"))
        _assert_kernel_invariant(pq)
        ranked = [a for _, a in _kernel_attaches(pq, RankedProducerKernel)]
        assert ranked
        assert len({regroup_producer_identity(a) for a in ranked}) == 1

    @pytest.mark.parametrize("query", [
        _by_customer_q(SEMI),
        _by_customer_q(TWO_RANKINGS),
        _by_customer_q(f"sum({LAST} + max(account_snapshots.balance, {PB}))"),
        snap_q(dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(), metrics=[m(TWO_WINDOWS)]),
        snap_q(dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
               metrics=[m(f"cumsum(sum(last(account_snapshots.balance, {PB_MONTH})))")]),
    ])
    async def test_emitted_sql_is_scope_closed(self, query) -> None:
        assert_scope_closed(await _sql(query), dialect="duckdb")


class TestPlanTimeInvariant:
    def test_an_inline_kernel_requiring_aggregate_trips_the_plan_assertion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real = stages._producer_nesting_rule

        def inline_last_roots(*args, **kwargs):
            nests = real(*args, **kwargs)
            return lambda root, phase: nests(root, phase) and not (
                phase != "row" and isinstance(root, AggregateKey) and root.agg == "last"
            )

        monkeypatch.setattr(stages, "_producer_nesting_rule", inline_last_roots)
        query, bundle = _by_customer_q(SEMI), dev2006_bundle()
        with pytest.raises(AssertionError) as excinfo:
            plan.plan_query(query=query, bundle=bundle)
        assert any(entry.name == "_emit_planned" for entry in excinfo.traceback)
        assert "last" in str(excinfo.value)


class TestRendererBackstop:
    def test_a_ranked_slot_under_the_plain_renderer_raises(self) -> None:
        pq = _plan(_by_customer_q("max(account_snapshots.balance)"))
        [slot] = [s for s in pq.aggregate_slots if isinstance(s.key, AggregateKey)]
        ranked = slot.model_copy(update={"key": slot.key.model_copy(update={"agg": "last"})})
        corrupted = pq.model_copy(update={"aggregate_slots": [
            ranked if s.id == slot.id else s for s in pq.aggregate_slots
        ]})
        gen, bundle = SQLGenerator(dialect="duckdb"), dev2006_bundle()
        with pytest.raises(RuntimeError, match="ranked-kernel producer"):
            gen.generate_from_planned(planned_query=corrupted, bundle=bundle)


# --------------------------------------------------------------------------- #
# Regression pins: kernel sites that already work
# --------------------------------------------------------------------------- #
class TestShiftedKernels:
    @pytest.mark.parametrize("inner,kind", [
        ("last(account_snapshots.balance)", RankedProducerKernel),
        ("sum(account_snapshots.balance, window='30d')", TrailingWindowProducerKernel),
    ])
    def test_own_grain_leaf_carries_its_kernel(self, inner, kind) -> None:
        pq = _plan(snap_q(dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
                          metrics=[m(f"time_shift({inner}, -1)")]))
        [shifted] = [a for a in pq.regroup_attach_plans if a.attach_phase == "shifted"]
        assert isinstance(shifted.kernel, kind)
        _assert_kernel_invariant(pq)

    @pytest.mark.parametrize("inner,kind", [
        ("last(account_snapshots.balance)", RankedProducerKernel),
        ("sum(account_snapshots.balance, window='30d')", TrailingWindowProducerKernel),
    ])
    def test_composite_leaf_nests_its_kernel(self, inner, kind) -> None:
        pq = _plan(snap_q(dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
                          metrics=[m(f"time_shift({inner} + 1, -1)")]))
        [shifted] = [a for a in pq.regroup_attach_plans if a.attach_phase == "shifted"]
        assert isinstance(shifted.kernel, PlainProducerKernel)
        assert any(isinstance(a.kernel, kind) for a in shifted.producer_plan.regroup_attach_plans)
        _assert_kernel_invariant(pq)

    @pytest.mark.parametrize("inner,offset,expected", [
        ("last(account_snapshots.balance)", 0.0, SHIFTED_LAST_BY_CUSTOMER_MONTH),
        ("last(account_snapshots.balance) + 1", 1.0, SHIFTED_LAST_BY_CUSTOMER_MONTH),
        ("sum(account_snapshots.balance, window='30d')", 0.0, SHIFTED_30D_BY_CUSTOMER_MONTH),
        ("sum(account_snapshots.balance, window='30d') + 1", 1.0, SHIFTED_30D_BY_CUSTOMER_MONTH),
    ])
    async def test_shifted_values(self, exec_engine, inner, offset, expected):
        resp = await exec_engine.execute(snap_q(
            dimensions=["account_snapshots.customer_id"], time_dimensions=month_td(),
            metrics=[m(f"time_shift({inner}, -1)")],
        ))
        _approx(by_customer_month(resp), {k: None if v is None else v + offset for k, v in expected.items()})


class TestCrossModelKernels:
    def test_cross_model_ranked_operand_is_target_rooted(self) -> None:
        pq = _plan(snap_q(population="customers", dimensions=["customers.name"], metrics=[m(f"sum({CM_LAST})")]),
                   root="customers")
        _assert_kernel_invariant(pq)
        [(carrier, ranked)] = _kernel_attaches(pq, RankedProducerKernel)
        assert carrier is not None
        assert carrier.attach_phase == "row"
        assert ranked.producer_root_dataset == "account_snapshots"
