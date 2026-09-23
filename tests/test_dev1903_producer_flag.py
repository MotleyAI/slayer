"""No producer flag: the user's query and a synthesized producer enter through
separate entry points over one core; inside a producer a root nests iff its grain is
a strict subset of the producer grain (design D3, D6)."""

from __future__ import annotations

import inspect

import pytest

from slayer.core.keys import AggregateKey, ArithmeticKey, TransformKey
from slayer.engine import elaborate as elaborate_mod
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.compile import compile_query
from slayer.engine.compile import stages
from slayer.engine.compile.stages import compile_synthesized
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query
from slayer.core.query import SlayerQuery
from slayer.ir.planned import PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope

from tests._dev1836_fixtures import dev1836_models
from tests._dev1900_fixtures import cust_q, dev1900_models
from tests._dev1836_fixtures import q as dev1836_q
from tests._dev1832_fixtures import ModelMeasure, dev1832_models, month_td, monthly_q
from tests._dev1847_fixtures import (
    INNER_CR,
    broadcast_warnings,
    dev1847_models,
    make_exec_engine,
    reagg,
    sales_q,
)
from tests._engine_helpers import plan_as_producer
from tests.test_filtered_local_isolation import _bundle, _claim_amount, _s5_bundle

TOP_ENTRIES = [elaborate_query, compile_query, plan_query]
MODE_FLAGS = ("disable_host_rooted_isolation", "enable_producer_regroups", "in_producer")
#: 430 over the nine non-NULL [city, region] cells, broadcast to every cell.
PROBE_A_VALUE = 430.0 / 9.0
PROBE_A_CELLS = {("East", "P"), ("Gap", "P"), ("North", "P"), ("South", "P"),
                 ("Void", "P"), ("East", "Q"), ("North", "Q"), ("South", "Q")}


def _probe_a_query() -> SlayerQuery:
    return sales_q(
        dimensions=["region", "product"], to_many_handling="broadcast",
        measures=[reagg("avg", INNER_CR, name="acc", partition_by="product")])


def _sales_bundle() -> ResolvedSourceBundle:
    models = dev1847_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _monthly_plan(formula: str) -> PlannedQuery:
    models = dev1832_models()
    root = next(m for m in models if m.name == "monthly")
    return plan_query(
        query=monthly_q(dimensions=["region"], time_dimensions=month_td(),
                        measures=[ModelMeasure(formula=formula, name="t")]),
        bundle=ResolvedSourceBundle(
            source_model=root, referenced_models=[m for m in models if m is not root]))


def _shifted(pq: PlannedQuery):
    """The one shifted attach, pinned to the one non-series ``time_shift`` slot."""
    [attach] = [a for a in pq.regroup_attach_plans if a.attach_phase == "shifted"]
    [slot] = [s for s in (*pq.row_slots, *pq.aggregate_slots, *pq.combined_expression_slots)
              if isinstance(s.key, TransformKey) and s.key.op == "time_shift"
              and s.series is False]
    assert attach.attach_phase == "shifted"
    assert attach.shift_of == slot.id
    return attach


def _host_plain_combined(pq: PlannedQuery) -> list:
    return [a for a in pq.regroup_attach_plans
            if a.attach_phase == "combined" and a.producer_root_model is None
            and a.kernel.kind == "plain"]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _dev1836_bundle() -> ResolvedSourceBundle:
    models = dev1836_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=list(models[1:]))


def _customers_bundle() -> ResolvedSourceBundle:
    models = dev1900_models()
    src = next(m for m in models if m.name == "customers")
    return ResolvedSourceBundle(source_model=src,
                                referenced_models=[m for m in models if m is not src])


def _semi_join_and_mask_counts(pq: PlannedQuery) -> list:
    out = [(len(pq.semi_join_filters), len(pq.masks))]
    for a in pq.regroup_attach_plans:
        out += _semi_join_and_mask_counts(a.producer_plan)
    return out


def _assert_no_mode_flag(fn) -> None:
    """No known flag name and no boolean-typed or boolean-defaulted parameter."""
    for name, param in inspect.signature(fn).parameters.items():
        assert name not in MODE_FLAGS, (fn, name)
        assert not isinstance(param.default, bool), (fn, name)
        assert "bool" not in str(param.annotation), (fn, name)


class TestTwoEntryPoints:
    @pytest.mark.parametrize("fn", TOP_ENTRIES, ids=lambda f: f.__name__)
    def test_top_entry_has_no_mode_flag(self, fn):
        _assert_no_mode_flag(fn)

    def test_producer_entries_have_no_mode_flag(self):
        _assert_no_mode_flag(compile_synthesized)
        _assert_no_mode_flag(elaborate_mod.elaborate_synthesized)

    def test_producer_must_state_its_population(self):
        param = inspect.signature(compile_synthesized).parameters["population_filters"]
        assert param.default is inspect.Parameter.empty
        assert param.kind is inspect.Parameter.KEYWORD_ONLY

    def test_elaborate_synthesized_takes_a_prebound_only(self):
        params = inspect.signature(elaborate_mod.elaborate_synthesized).parameters
        assert params["prebound"].default is inspect.Parameter.empty
        assert "query" not in params

    def test_core_is_private(self):
        assert not hasattr(stages, "compile_prebound")


class TestTopOnlySteps:
    """The once-per-query steps run at the top and never through the producer entry."""

    def test_filter_split(self):
        bundle = _dev1836_bundle()
        query = dev1836_q(
            dimensions=["status", "channel"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            filters=["status = 'ok' and amount:sum(partition_by=channel) > 0"])
        top = elaborate_query(query=query, bundle=bundle)
        assert top.prebound is not None and len(top.prebound.bound_filters) == 2
        scope = resolve_scope(query=query, bundle=bundle, stage_schemas={})
        prebound = bind_query_inputs(query=query, bundle=bundle, scope=scope,
                                     stage_schemas={})
        producer = elaborate_mod.elaborate_synthesized(
            prebound, bundle=bundle, scope=scope, stage_schemas={})
        assert producer.prebound is not None and len(producer.prebound.bound_filters) == 1

    def test_population_disposal(self):
        """A fanning row filter rides the host EXISTS at the top; a producer with no
        inherited population keeps it as its own mask."""
        query = cust_q(dimensions=["tier"],
                       measures=[ModelMeasure(formula="spend:sum", name="sp")],
                       filters=["orders.status = 'ok'"])
        assert _semi_join_and_mask_counts(
            plan_query(query=query, bundle=_customers_bundle())) == [(1, 0)]
        assert _semi_join_and_mask_counts(
            plan_as_producer(query=query, bundle=_customers_bundle())) == [(0, 1)]

    def test_redundant_partition_strip(self):
        """A row-attach root partitioned by exactly the query grain is stripped at the
        top; the producer entry leaves the key as written."""
        query = sales_q(dimensions=["region"], measures=[ModelMeasure(
            formula="sum(quantity * sum(amount, partition_by=product), partition_by=region)",
            name="x")])

        def outer_partition(pq: PlannedQuery):
            [slot] = [s for s in pq.aggregate_slots
                      if isinstance(s.key, AggregateKey) and s.key.agg == "sum"]
            assert isinstance(slot.key, AggregateKey)
            return slot.key.partition_keys

        assert outer_partition(plan_query(query=query, bundle=_sales_bundle())) is None
        assert outer_partition(plan_as_producer(query=query, bundle=_sales_bundle())) \
            is not None

    def test_total_routing(self, monkeypatch):
        """Blinded discovery raises at the top; a producer holds the root inline."""
        monkeypatch.setattr(stages, "discover_roots", lambda *_a, **_k: [])
        query = dev1836_q(dimensions=["status", "channel"], measures=[
            ModelMeasure(formula="amount:sum(partition_by=channel)", name="pt")])
        with pytest.raises(ValueError, match="no routing disposition"):
            plan_query(query=query, bundle=_dev1836_bundle())
        [slot] = plan_as_producer(query=query, bundle=_dev1836_bundle()).aggregate_slots
        assert isinstance(slot.key, AggregateKey) and slot.key.partition_keys is not None


class TestStrictSubsetNesting:
    def test_outer_reaggregation_answer_compiles_inline(self):
        """The outer answer's ``[product]`` is not a strict subset of its producer grain."""
        pq = plan_query(query=_probe_a_query(), bundle=_sales_bundle())
        [outer] = pq.regroup_attach_plans
        producer = outer.producer_plan
        [slot] = producer.aggregate_slots
        assert isinstance(slot.key, AggregateKey) and slot.key.agg == "avg"
        assert [a.attach_phase for a in producer.regroup_attach_plans] == ["row"]

    async def test_outer_reaggregation_values(self, exec_engine):
        resp = await exec_engine.execute(_probe_a_query())
        got = {(r["sales.region"], r["sales.product"]): r["sales.acc"] for r in resp.data}
        assert set(got) == PROBE_A_CELLS
        for cell, value in got.items():
            assert float(value) == pytest.approx(PROBE_A_VALUE), cell
        assert broadcast_warnings(resp)

    def test_carrier_nests_every_strict_constituent(self):
        """``[city]`` and ``[region]`` are strict subsets of the ``[city, region]`` carrier grain."""
        pq = plan_query(query=sales_q(dimensions=["region"], measures=[ModelMeasure(
            formula="avg(sum(amount, partition_by=city) + sum(amount, partition_by=region))",
            name="x")]), bundle=_sales_bundle())
        [outer] = pq.regroup_attach_plans
        [carrier] = outer.producer_plan.regroup_attach_plans
        assert carrier.attach_phase == "row"
        assert sorted(a.partition_display for a in carrier.producer_plan.regroup_attach_plans) \
            == [["city"], ["region"]]

    @pytest.mark.parametrize(("formula", "dimensions", "bundle"), [
        pytest.param("loss_payment_amt:sum", ["claim.claim_number"],
                     lambda: _bundle(_claim_amount()), id="filter-crossing"),
        pytest.param("region_pay:sum", None, _s5_bundle, id="input-crossing"),
    ])
    def test_own_answer_at_producer_grain_never_nests(self, formula, dimensions, bundle):
        host = bundle().source_model
        q = SlayerQuery(source_model=host.name, dimensions=dimensions,
                        measures=[ModelMeasure(formula=formula, name="m0")])
        pq = plan_as_producer(query=q, bundle=bundle())
        assert _host_plain_combined(pq) == []
        assert any(isinstance(s.key, AggregateKey) and s.key.agg == "sum"
                   for s in pq.aggregate_slots)


class TestShiftedProducerNesting:
    """Pins: the shifted producer already runs with nested discovery on."""

    def test_own_grain_ranked_answer_does_not_nest(self):
        attach = _shifted(_monthly_plan("time_shift(amount:last, -1)"))
        assert attach.kernel.kind == "ranked"
        assert attach.producer_plan.regroup_attach_plans == []
        [slot] = attach.producer_plan.aggregate_slots
        assert isinstance(slot.key, AggregateKey) and slot.key.agg == "last"
        assert attach.answer_slot_id == slot.id

    @pytest.mark.parametrize(("formula", "kernel"), [
        pytest.param("time_shift(amount:last / 2, -1)", "ranked", id="ranked"),
        pytest.param("time_shift(amount:sum(window='90d') / 2, -1)", "trailing-window",
                     id="windowed"),
    ])
    def test_strict_constituent_nests(self, formula, kernel):
        pq = _monthly_plan(formula)
        attach = _shifted(pq)
        assert attach.kernel.kind == "plain"
        [slot] = attach.producer_plan.aggregate_slots
        assert isinstance(slot.key, ArithmeticKey)
        assert attach.answer_slot_id == slot.id
        [nested] = attach.producer_plan.regroup_attach_plans
        assert nested.kernel.kind == kernel
        assert nested.attach_phase == "combined"

    def test_carried_attach_is_reused_not_renested(self):
        pq = _monthly_plan("time_shift(amount:sum / amount:sum(partition_by=[region]), -1)")
        attach = _shifted(pq)
        [base] = [a for a in pq.regroup_attach_plans if a.attach_phase == "combined"]
        [carried] = attach.producer_plan.regroup_attach_plans
        assert carried is base
        [slot] = [s for s in attach.producer_plan.aggregate_slots
                   if isinstance(s.key, ArithmeticKey)]
        assert attach.answer_slot_id == slot.id
