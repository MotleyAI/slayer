"""No producer flag: the user's query and a synthesized producer enter through
separate entry points over one core; inside a producer a root nests iff its grain is
a strict subset of the producer grain (design D3, D6)."""

from __future__ import annotations

import inspect
from collections.abc import Callable

import pytest

from slayer.core.keys import AggregateKey, ArithmeticKey, TransformKey
from slayer.engine import elaborate as elaborate_mod
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.compile import compile_query
from slayer.engine.compile import stages
from slayer.engine.compile.stages import compile_synthesized
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query, plan_stages
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.ir import elaborated as elaborated_mod
from slayer.ir.planned import PlannedQuery
from slayer.ir.prebound import StrictQueryCarrier
from slayer.ir.source_bundle import ResolvedSourceBundle, resolve_scope

from tests._dev1836_fixtures import dev1836_datasets
from tests._dev1900_fixtures import cust_q, dev1900_datasets
from tests._dev1836_fixtures import q as dev1836_q
from tests._dev1832_fixtures import dev1832_datasets, month_td, monthly_q
from tests._dev1847_fixtures import (
    INNER_CR,
    SPEND_BAND_EXPR,
    dev1847_datasets,
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
        dimensions=["sales.region", "sales.product"], to_many_handling="broadcast",
        metrics=[reagg("avg", INNER_CR, name="acc", partition_by="[]")])


def _sales_bundle() -> ResolvedSourceBundle:
    models = dev1847_datasets()
    return ResolvedSourceBundle(dialect="postgres", population=models[0], referenced_datasets=models[1:])


def _monthly_plan(expression: str) -> PlannedQuery:
    models = dev1832_datasets()
    root = next(m for m in models if m.name == "monthly")
    return plan_query(
        query=monthly_q(dimensions=["monthly.region"], time_dimensions=month_td("monthly"),
                        metrics=[QueryMetric(metric=expression, alias="t")]),
        bundle=ResolvedSourceBundle(
            dialect="postgres",
            population=root, referenced_datasets=[m for m in models if m is not root]))


def _shifted(pq: PlannedQuery):
    """The one shifted attach, pinned to the one ``time_shift`` slot."""
    [attach] = [a for a in pq.regroup_attach_plans if a.attach_phase == "shifted"]
    [slot] = [s for s in (*pq.row_slots, *pq.aggregate_slots, *pq.combined_expression_slots)
              if isinstance(s.key, TransformKey) and s.key.op == "time_shift"]
    assert attach.attach_phase == "shifted"
    assert attach.shift_of == slot.id
    return attach


def _host_plain_combined(pq: PlannedQuery) -> list:
    return [a for a in pq.regroup_attach_plans
            if a.attach_phase == "combined" and a.producer_root_dataset is None
            and a.kernel.kind == "plain"]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _dev1836_bundle() -> ResolvedSourceBundle:
    models = dev1836_datasets()
    return ResolvedSourceBundle(dialect="postgres", population=models[0], referenced_datasets=list(models[1:]))


def _customers_bundle() -> ResolvedSourceBundle:
    models = dev1900_datasets()
    src = next(m for m in models if m.name == "customers")
    return ResolvedSourceBundle(dialect="postgres", population=src,
                                referenced_datasets=[m for m in models if m is not src])


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
        param = inspect.signature(compile_synthesized).parameters["population"]
        assert param.default is inspect.Parameter.empty
        assert param.kind is inspect.Parameter.KEYWORD_ONLY

    def test_elaborate_synthesized_takes_a_prebound_only(self):
        params = inspect.signature(elaborate_mod.elaborate_synthesized).parameters
        assert params["prebound"].default is inspect.Parameter.empty
        assert "query" not in params

    def test_core_is_private(self):
        assert not hasattr(stages, "compile_prebound")

    def test_producer_context_grain_is_required(self):
        field = stages.ProducerContext.model_fields["enclosing_grain"]
        assert field.is_required()
        assert "None" not in str(field.annotation)
        assert "Optional" not in str(field.annotation)


def _producer_env():
    bundle = _dev1836_bundle()
    query = dev1836_q(dimensions=["orders.status"],
                      metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")])
    scope = resolve_scope(bundle=bundle)
    prebound = bind_query_inputs(query=query, bundle=bundle, scope=scope)
    return query, prebound, elaborate_mod.elaborate_synthesized(
        prebound, bundle=bundle, scope=scope, stage_schemas={})


class TestTwoEnvironmentTypes:
    def test_entries_return_their_own_type(self):
        query, _, producer = _producer_env()
        top = elaborate_query(query=query, bundle=_dev1836_bundle())
        assert isinstance(top, elaborated_mod.ElaboratedStage)
        assert isinstance(producer, elaborated_mod.ElaboratedProducer)
        assert not isinstance(producer, elaborated_mod.ElaboratedStage)

    def test_compile_query_rejects_a_producer_environment(self):
        _, _, producer = _producer_env()
        with pytest.raises((TypeError, ValueError)):
            compile_query(elaborated=producer)  # pyright: ignore[reportArgumentType]

    def test_each_environment_rejects_the_other_carrier(self):
        query, prebound, producer = _producer_env()
        carrier = StrictQueryCarrier(population="orders", prebound=prebound)
        inputs = dict(scope=producer.scope, bundle=producer.bundle, prebound=producer.prebound)
        with pytest.raises(ValueError):
            elaborated_mod.ElaboratedStage(query=carrier, **inputs)  # pyright: ignore[reportArgumentType]
        with pytest.raises(ValueError):
            elaborated_mod.ElaboratedProducer(query=query, **inputs)  # pyright: ignore[reportArgumentType]


def _count_top_only(monkeypatch: pytest.MonkeyPatch) -> dict:
    calls = {"dispose": 0, "total_routing": 0}
    dispose, total = stages.dispose_population_filters, stages._assert_total_routing

    def counted_dispose(*a, **k):
        calls["dispose"] += 1
        return dispose(*a, **k)

    def counted_total(*a, **k):
        calls["total_routing"] += 1
        return total(*a, **k)

    monkeypatch.setattr(stages, "dispose_population_filters", counted_dispose)
    monkeypatch.setattr(stages, "_assert_total_routing", counted_total)
    return calls


class TestEverySiteStatesItsPopulation:
    """The cross-dataset producer re-roots its own filters; every other site inherits."""

    @pytest.fixture
    def populations(self, monkeypatch: pytest.MonkeyPatch) -> list:
        seen: list = []
        real = stages.compile_synthesized

        def recording(*a, **k):
            seen.append(type(k["population"]).__name__)
            return real(*a, **k)

        monkeypatch.setattr(stages, "compile_synthesized", recording)
        return seen

    @pytest.mark.parametrize(("plan", "expected"), [
        pytest.param(lambda: plan_query(query=dev1836_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="cm")]),
            bundle=_dev1836_bundle()), {"NoInheritedPopulation"}, id="cross-model"),
        pytest.param(lambda: plan_query(query=dev1836_q(
            dimensions=["orders.status", "orders.channel"],
            metrics=[QueryMetric(metric="sum(orders.amount, partition_by=orders.channel)", alias="pt")]),
            bundle=_dev1836_bundle()), {"InheritedPopulation"}, id="local-regroup"),
        pytest.param(lambda: plan_query(query=cust_q(
            dimensions=["customers.tier"], metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
            order=[{"field": "orders.amount", "direction": "asc"}]),
            bundle=_customers_bundle()), {"InheritedPopulation"}, id="wrap"),
        pytest.param(lambda: _monthly_plan("time_shift(sum(monthly.amount), -1)"),
                     {"InheritedPopulation"}, id="shifted"),
        pytest.param(lambda: plan_query(query=sales_q(
            dimensions=["sales.region"], metrics=[reagg("avg", INNER_CR, name="a")]),
            bundle=_sales_bundle()), {"InheritedPopulation"},
            id="reaggregation-outer-and-carrier"),
    ])
    def test_population_kind_per_site(self, populations, plan, expected):
        plan()
        assert populations, populations
        assert set(populations) == expected, populations


class TestTopOnlySteps:
    """The once-per-query steps run at the top and never through the producer entry."""

    def test_filter_split(self):
        bundle = _dev1836_bundle()
        query = dev1836_q(
            dimensions=["orders.status", "orders.channel"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            filter=["orders.status = 'ok' and sum(orders.amount, partition_by=orders.channel) > 0"])
        top = elaborate_query(query=query, bundle=bundle)
        assert top.prebound is not None
        assert len(top.prebound.bound_filters) == 2
        scope = resolve_scope(bundle=bundle)
        prebound = bind_query_inputs(query=query, bundle=bundle, scope=scope)
        producer = elaborate_mod.elaborate_synthesized(
            prebound, bundle=bundle, scope=scope, stage_schemas={})
        assert producer.prebound is not None
        assert len(producer.prebound.bound_filters) == 1

    def test_population_disposal(self):
        """A fanning row filter rides the host EXISTS at the top; a producer with no
        inherited population keeps it as its own mask."""
        query = cust_q(dimensions=["customers.tier"],
                       metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
                       filter=["orders.status = 'ok'"])
        assert _semi_join_and_mask_counts(
            plan_query(query=query, bundle=_customers_bundle())) == [(1, 0)]
        assert _semi_join_and_mask_counts(
            plan_as_producer(query=query, bundle=_customers_bundle())) == [(0, 1)]

    def test_redundant_partition_strip(self):
        """A row-attach root partitioned by exactly the query grain is stripped at the
        top; the producer entry leaves the key as written."""
        query = sales_q(dimensions=["sales.region"], metrics=[QueryMetric(
            metric="sum(sales.quantity * sum(sales.amount, partition_by=sales.product), partition_by=sales.region)",
            alias="x")])

        def outer_partition(pq: PlannedQuery):
            [slot] = [s for s in pq.aggregate_slots
                      if isinstance(s.key, AggregateKey) and s.key.agg == "sum"]
            assert isinstance(slot.key, AggregateKey)
            return slot.key.partition_keys

        assert outer_partition(plan_query(query=query, bundle=_sales_bundle())) is None
        assert outer_partition(plan_as_producer(query=query, bundle=_sales_bundle())) \
            is not None

    def test_non_root_authored_stage_is_top_level(self, monkeypatch: pytest.MonkeyPatch):
        """Every authored DAG stage disposes and checks routing exactly once; its own
        producer does neither."""
        calls = _count_top_only(monkeypatch)
        s1 = cust_q(name="s1", dimensions=["customers.tier"], metrics=[
            QueryMetric(metric="sum(customers.spend)", alias="sp"),
            QueryMetric(metric="sum(customers.spend, partition_by=customers.tier)", alias="pt")],
            filter=["orders.status = 'ok'"])
        root = SlayerQuery.model_validate({
            "population": "s1", "dimensions": ["s1.tier"],
            "metrics": [{"metric": "sum(s1.sp)", "alias": "t"}]})
        stage_plans = plan_stages(queries=[s1, root], bundle=_customers_bundle())
        assert calls == {"dispose": 2, "total_routing": 2}
        assert [len(p.semi_join_filters) for p in stage_plans] == [1, 0]
        assert len(stage_plans[0].regroup_attach_plans) == 1

    @pytest.mark.parametrize("plan", [
        pytest.param(lambda: plan_query(query=cust_q(
            dimensions=["customers.tier"], metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
            filter=["orders.status = 'ok'"],
            order=[{"field": "orders.amount", "direction": "asc"}]),
            bundle=_customers_bundle()), id="late-wrap"),
        pytest.param(lambda: _monthly_plan("time_shift(sum(monthly.amount), -1)"), id="shifted"),
    ])
    def test_late_producers_never_run_top_only_steps(self, monkeypatch: pytest.MonkeyPatch, plan):
        calls = _count_top_only(monkeypatch)
        pq = plan()
        assert pq.regroup_attach_plans
        assert calls == {"dispose": 1, "total_routing": 1}

    def test_total_routing(self, monkeypatch: pytest.MonkeyPatch):
        """Blinded discovery raises at the top; a producer holds the root inline."""
        monkeypatch.setattr(stages, "discover_roots", lambda *_a, **_k: [])
        query = dev1836_q(dimensions=["orders.status", "orders.channel"], metrics=[
            QueryMetric(metric="sum(orders.amount, partition_by=orders.channel)", alias="pt")])
        bundle = _dev1836_bundle()
        with pytest.raises(ValueError, match="no routing disposition"):
            plan_query(query=query, bundle=bundle)
        [slot] = plan_as_producer(query=query, bundle=_dev1836_bundle()).aggregate_slots
        assert isinstance(slot.key, AggregateKey)
        assert slot.key.partition_keys is not None


class TestStrictSubsetNesting:
    def test_outer_reaggregation_answer_compiles_inline(self):
        """The outer answer is the producer's own, broadcast-dropped answer."""
        pq = plan_query(query=_probe_a_query(), bundle=_sales_bundle())
        [outer] = pq.regroup_attach_plans
        producer = outer.producer_plan
        [slot] = producer.aggregate_slots
        assert isinstance(slot.key, AggregateKey)
        assert slot.key.agg == "avg"
        assert [a.attach_phase for a in producer.regroup_attach_plans] == ["row"]

    async def test_outer_reaggregation_values(self, exec_engine):
        resp = await exec_engine.execute(_probe_a_query())
        got = {(r["sales.region"], r["sales.product"]): r["acc"] for r in resp.data}
        assert set(got) == PROBE_A_CELLS
        for cell, value in got.items():
            assert float(value) == pytest.approx(PROBE_A_VALUE), cell

    def test_carrier_nests_every_strict_constituent(self):
        """``[city]`` and ``[region]`` are strict subsets of the ``[city, region]`` carrier grain."""
        pq = plan_query(query=sales_q(dimensions=["sales.region"], metrics=[QueryMetric(
            metric="avg(sum(sales.amount, partition_by=sales.city) + sum(sales.amount, partition_by=sales.region))",
            alias="x")]), bundle=_sales_bundle())
        [outer] = pq.regroup_attach_plans
        [carrier] = outer.producer_plan.regroup_attach_plans
        assert carrier.attach_phase == "row"
        assert sorted(a.partition_display for a in carrier.producer_plan.regroup_attach_plans) \
            == [["city"], ["region"]]

    @pytest.mark.parametrize(("formula", "dimensions", "bundle"), [
        pytest.param("sum(claim_amount.loss_payment_amt)", ["claim.claim_number"],
                     lambda: _bundle(_claim_amount()), id="filter-crossing"),
        pytest.param("sum(orders.region_pay)", None, _s5_bundle, id="input-crossing"),
    ])
    def test_own_answer_at_producer_grain_never_nests(
        self, formula: str, dimensions, bundle: Callable[[], ResolvedSourceBundle],
    ) -> None:
        host = bundle().population
        assert host is not None
        q = SlayerQuery(population=host.name, dimensions=dimensions,
                        metrics=[QueryMetric(metric=formula, alias="m0")])
        pq = plan_as_producer(query=q, bundle=bundle())
        assert _host_plain_combined(pq) == []
        assert any(isinstance(s.key, AggregateKey) and s.key.agg == "sum"
                   for s in pq.aggregate_slots)


BAND = {"field": SPEND_BAND_EXPR, "alias": "spend_band"}


# Constant weight per band: lo averages its 8 non-NULL amounts (90); Void's NULLs skip their weight.
_W_BY_BAND = {("hi",): 68.0, ("lo",): 90.0 / 8.0}
_W_BY_BAND_REGION = {("hi", "East"): 60.0, ("hi", "North"): 60.0, ("hi", "South"): 100.0,
                     ("lo", "Gap"): 20.0 / 3, ("lo", "North"): 10.0, ("lo", "South"): 20.0,
                     ("lo", "Void"): None}


class TestOffGrainConstituentNests:
    @pytest.mark.parametrize(("dimensions", "expected"), [
        pytest.param([BAND], _W_BY_BAND, id="band"),
        pytest.param([BAND, "sales.region"], _W_BY_BAND_REGION, id="band-region"),
    ])
    async def test_executes_at_the_band_grain(self, exec_engine, dimensions, expected):
        """Grained at ``spend_band`` alone (the nested aggregate is opaque): a constant
        weight per band."""
        query = sales_q(dimensions=dimensions, metrics=[QueryMetric(
            metric="weighted_avg(sales.amount, weight=rank(sum(sales.amount, partition_by=spend_band), direction='desc'))",
            alias="w")])
        resp = await exec_engine.execute(query)
        names = [d if isinstance(d, str) else d["alias"] for d in dimensions]
        got = {tuple(r[n] for n in names): r["w"] for r in resp.data}
        assert got == {k: None if v is None else pytest.approx(v) for k, v in expected.items()}


class TestShiftedProducerNesting:
    """Pins: the shifted producer already runs with nested discovery on."""

    def test_own_grain_ranked_answer_does_not_nest(self):
        attach = _shifted(_monthly_plan("time_shift(last(monthly.amount), -1)"))
        assert attach.kernel.kind == "ranked"
        assert attach.producer_plan.regroup_attach_plans == []
        [slot] = attach.producer_plan.aggregate_slots
        assert isinstance(slot.key, AggregateKey)
        assert slot.key.agg == "last"
        assert attach.answer_slot_id == slot.id

    @pytest.mark.parametrize(("formula", "kernel"), [
        pytest.param("time_shift(last(monthly.amount) / 2, -1)", "ranked", id="ranked"),
        pytest.param("time_shift(sum(monthly.amount, window='90d') / 2, -1)", "trailing-window",
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
        pq = _monthly_plan("time_shift(sum(monthly.amount) / sum(monthly.amount, partition_by=[monthly.region]), -1)")
        attach = _shifted(pq)
        [base] = [a for a in pq.regroup_attach_plans if a.attach_phase == "combined"]
        [carried] = attach.producer_plan.regroup_attach_plans
        assert carried is base
        [slot] = [s for s in attach.producer_plan.aggregate_slots
                   if isinstance(s.key, ArithmeticKey)]
        assert attach.answer_slot_id == slot.id
