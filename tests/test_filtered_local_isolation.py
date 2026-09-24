"""Planner unit tests for cross-model-filtered local measure isolation.

Pins: (1) the cross-model planner's trigger fires for empty-path aggregates
whose filter references a non-host join path, not for same-model / no-filter
cases; (2) the recursive host-rooted sub-plan does not re-isolate; (3) a
filtered-local first/last holds its measure inline.
"""

from __future__ import annotations

import importlib

import pytest

from slayer.core.enums import DataType, JoinType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelMeasure,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.ir.planned import MaskTyping
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query

from tests._engine_helpers import plan_as_producer


# Fixtures


def _loss_payment() -> SlayerModel:
    return SlayerModel(
        name="loss_payment", data_source="test", sql_table="Loss_Payment",
        columns=[
            Column(name="claim_amount_id", type=DataType.INT, primary_key=True),
            Column(name="has_flag", sql="1", type=DataType.DOUBLE),
        ],
    )


def _loss_reserve() -> SlayerModel:
    return SlayerModel(
        name="loss_reserve", data_source="test", sql_table="Loss_Reserve",
        columns=[
            Column(name="claim_amount_id", type=DataType.INT, primary_key=True),
            Column(name="has_flag", sql="1", type=DataType.DOUBLE),
        ],
    )


def _claim() -> SlayerModel:
    return SlayerModel(
        name="claim", data_source="test", sql_table="Claim",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="claim_number", type=DataType.TEXT),
        ],
    )


def _claim_amount(
    *, with_time: bool = False, with_filter_first: bool = False,
) -> SlayerModel:
    cols = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="claim_id", type=DataType.INT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="total_amount", sql="amount", type=DataType.DOUBLE),
        # Same-model filter — must NOT trigger isolation.
        Column(name="paid_amount", sql="amount", filter="claim_id > 0", type=DataType.DOUBLE),
        # Cross-model filter — must trigger.
        Column(
            name="loss_payment_amt", sql="amount",
            filter="loss_payment.has_flag = 1", type=DataType.DOUBLE,
        ),
    ]
    if with_time:
        cols.append(Column(name="created_at", type=DataType.TIMESTAMP))
    if with_filter_first:
        cols.append(Column(
            name="latest_payment", sql="amount",
            filter="loss_payment.has_flag = 1", type=DataType.DOUBLE,
        ))
    return SlayerModel(
        name="claim_amount", data_source="test", sql_table="Claim_Amount",
        columns=cols,
        joins=[
            ModelJoin(
                target_model="loss_payment",
                join_pairs=[["id", "claim_amount_id"]],
                join_type="inner",
            ),
            ModelJoin(
                target_model="loss_reserve",
                join_pairs=[["id", "claim_amount_id"]],
                join_type="inner",
            ),
            ModelJoin(
                target_model="claim",
                join_pairs=[["claim_id", "id"]],
            ),
        ],
        default_time_dimension="created_at" if with_time else None,
    )


def _bundle(host: SlayerModel) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=host,
        referenced_models=[_loss_payment(), _loss_reserve(), _claim()],
    )


def _orders_with_derived_eu_filter(*, eu_amount_filter: str):
    """``orders`` host whose ``eu_amount`` filter references derived ``is_eu`` (crosses to customers.region); ``eu_amount_filter`` varies the ref shape."""
    host = SlayerModel(
        name="orders", data_source="test", sql_table="Orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(
                name="is_eu", type=DataType.DOUBLE,
                sql="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END",
            ),
            Column(
                name="eu_amount", sql="amount", filter=eu_amount_filter,
                type=DataType.DOUBLE,
            ),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
    )
    customers = SlayerModel(
        name="customers", data_source="test", sql_table="Customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
        ],
    )
    bundle = ResolvedSourceBundle(
        source_model=host, referenced_models=[customers],
    )
    return host, bundle


# Trigger predicate: cross-model planner invocation


class TestCrossModelPlannerTriggerPredicate:
    """Trigger: invoke the cross-model planner if source.path is non-empty OR the filter references a non-host join path."""

    def test_no_filter_local_measure_does_not_trigger(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "total_amount:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert planned.regroup_attach_plans == [], (
            f"Plain local aggregate must not trigger isolation; got attaches: {planned.regroup_attach_plans}"
        )

    def test_same_model_filter_does_not_trigger(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "paid_amount:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert planned.regroup_attach_plans == [], (
            f"Same-model filter must not trigger isolation; got attaches: {planned.regroup_attach_plans}"
        )

    def test_dotted_cross_model_filter_triggers_isolation(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        attaches = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert len(attaches) == 1, (
            f"Expected 1 host-rooted producer; got {attaches}"
        )
        assert attaches[0].producer_root_model is None, (
            f"Expected a HOST-rooted producer for filtered-local; got root "
            f"{attaches[0].producer_root_model!r}"
        )

    def test_derived_ref_cross_model_filter_triggers_isolation(self):
        _, bundle = _orders_with_derived_eu_filter(
            eu_amount_filter="is_eu = 1",
        )
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "eu_amount:sum"}],
            dimensions=["id"],
        )
        planned = plan_query(query=q, bundle=bundle)
        attaches = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert len(attaches) == 1, (
            f"Derived-ref cross-model filter must trigger isolation; got "
            f"{attaches}"
        )
        assert attaches[0].producer_root_model is None

    def test_cross_model_agg_still_triggers_via_path(self):
        """A non-empty ``source.path`` triggers the cross-model planner; the producer roots at the join target ``loss_payment``."""
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment.has_flag:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_model == "loss_payment"

    def test_cross_model_agg_with_target_column_filter_does_not_become_filtered_local(self):
        """A cross-model aggregate whose TARGET column has its own filter keeps genuine cross-model semantics, not host-rooted filtered-local."""
        regions = SlayerModel(
            name="regions", data_source="test", sql_table="regions",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="is_premium", type=DataType.DOUBLE),
            ],
        )
        customers = SlayerModel(
            name="customers", data_source="test", sql_table="customers",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="revenue", type=DataType.DOUBLE),
                Column(
                    name="premium_rev", sql="revenue",
                    filter="regions.is_premium = 1", type=DataType.DOUBLE,
                ),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        orders = SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        bundle = ResolvedSourceBundle(
            source_model=orders, referenced_models=[customers, regions],
        )
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.premium_rev:sum"}],
            dimensions=["id"],
        )
        planned = plan_query(query=q, bundle=bundle)
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_model == "customers"


# Producer mode


class TestProducerMode:
    """A host-rooted producer holds its own answer inline; it never re-isolates."""

    def test_isolated_sub_plan_has_no_nested_cma_plans(self):
        """The host-rooted sub-plan must contain zero nested attaches, else the filtered measure re-isolates."""
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        # The nested scope is the producer plan itself.
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
        assert sub.regroup_attach_plans == [], (
            f"Host-rooted producer must hold its measure inline (recursion "
            f"suppression); got nested attaches: {sub.regroup_attach_plans}"
        )


# First/last + zero nested CMA plans


class TestHostModelFiltersInteractions:
    """Host ``SlayerModel.filters`` apply inside the sub-plan; the known limit (no aggregate-measure refs) raises ``ValueError``."""

    def test_host_model_filter_lands_inside_filtered_local_sub_plan(self):
        """A host ``model.filters`` entry must appear in the sub-plan's Mode-A carrier."""
        host = SlayerModel(
            name="claim_amount", data_source="test", sql_table="Claim_Amount",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(
                    name="loss_payment_amt", sql="amount",
                    filter="loss_payment.has_flag = 1", type=DataType.DOUBLE,
                ),
            ],
            joins=[ModelJoin(
                target_model="loss_payment",
                join_pairs=[["id", "claim_amount_id"]],
                join_type="inner",
            )],
            filters=["amount > 0"],
        )
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[_loss_payment()],
        )
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["id"],
        )
        planned = plan_query(query=q, bundle=bundle)
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
        sub_filter_texts = [mf.text for mf in sub.mode_a_filters]
        assert "amount > 0" in sub_filter_texts, (
            f"Host model filter must land inside the host-rooted sub-plan; "
            f"got sub-plan filter texts {sub_filter_texts!r}"
        )

    def test_host_model_filter_referencing_aggregate_measure_raises(self):
        """``model.filters`` referencing an aggregate measure must raise at construction (``ValidationError`` is a ``ValueError``), not emit bad SQL."""
        columns = [
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(
                name="loss_payment_amt", sql="amount",
                filter="loss_payment.has_flag = 1", type=DataType.DOUBLE,
            ),
        ]
        joins = [ModelJoin(
            target_model="loss_payment",
            join_pairs=[["id", "claim_amount_id"]],
            join_type=JoinType.INNER,
        )]
        with pytest.raises(ValueError, match=r"(?i)aggregation reference|measure"):
            SlayerModel(
                name="claim_amount", data_source="test", sql_table="Claim_Amount",
                columns=columns,
                joins=joins,
                # Illegal: model.filters cannot reference an aggregate measure.
                filters=["loss_payment_amt:sum > 0"],
            )


class TestFirstLastNoNestedCmaPlans:
    """A filtered-local first/last isolates flatly — one ranked plan, its ``Column.filter`` a predicate inside its own CTE."""

    def test_filtered_local_first_last_isolates_without_nesting(self):
        host = _claim_amount(with_time=True, with_filter_first=True)
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "latest_payment:last"}],
            dimensions=["claim.claim_number"],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        # The local ranked first/last desugars into a ranked-kernel producer attach.
        assert len(planned.regroup_attach_plans) == 1
        attach = planned.regroup_attach_plans[0]
        assert attach.kernel.kind == "ranked"
        assert attach.producer_root_model is None  # rooted at the host
        # The crossing filter rides on the source column; no second plan needed.
        assert len(attach.producer_plan.aggregate_slots) == 1


# Widened Law-3 trigger: ANY crossing input isolates.


def _s5_regions() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="payment_amount", type=DataType.DOUBLE),
            Column(name="weight", type=DataType.DOUBLE),
        ],
    )


def _s5_customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="weight", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _s5_orders() -> SlayerModel:
    """Host with one derived column per crossing-input kind, plus custom aggregations."""
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="region_pay", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
            Column(name="cust_weight_d", sql="customers.weight",
                   type=DataType.DOUBLE),
            # Sibling derived chain: doubled_pop -> pop_helper -> crossing.
            Column(name="pop_helper", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
            Column(name="doubled_pop", sql="pop_helper * 2",
                   type=DataType.DOUBLE),
            # Derived TIME column crossing a join (explicit-arg use only).
            Column(name="cross_time", sql="customers.signup_at",
                   type=DataType.TIMESTAMP),
            Column(name="local_double", sql="amount * 2", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
        aggregations=[
            # User-supplied template-fragment kwarg (`scale=` str values).
            Aggregation(
                name="scaled_sum", formula="SUM({value}) / {scale}",
                params=[AggregationParam(name="scale", sql="1")],
            ),
            # Model-default fragment CROSSES a join — the param sql is the crossing input.
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
        ],
    )


def _s5_bundle(host: SlayerModel | None = None) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=host or _s5_orders(),
        referenced_models=[_s5_customers(), _s5_regions()],
    )


def _s5_plans(formula: str, *, dimensions=None, **plan_kwargs):
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": formula, "name": "m0"}],
        dimensions=dimensions,
    )
    planned = plan_query(query=q, bundle=_s5_bundle(), **plan_kwargs)
    # Keep only HOST-rooted PLAIN-kernel producers (ranked/windowed roots excluded).
    return planned, [
        a for a in planned.regroup_attach_plans
        if a.attach_phase == "combined"
        and a.producer_root_model is None
        and a.kernel.kind == "plain"
    ]


def _assert_single_ranked_host(planned):
    """A first/last isolates through the ranked route, one regroup producer with no nested plan. Returns the attach."""
    assert len(planned.regroup_attach_plans) == 1, (
        f"Expected exactly one regroup-attach producer; "
        f"got {planned.regroup_attach_plans}"
    )
    attach = planned.regroup_attach_plans[0]
    assert attach.kernel.kind == "ranked", attach.kernel
    assert attach.producer_root_model is None, attach.producer_root_model
    assert attach.producer_plan.regroup_attach_plans == [], (
        f"the ranked producer must hold its measure inline; "
        f"got nested attaches: {attach.producer_plan.regroup_attach_plans}"
    )
    return attach


def _assert_single_host_rooted(plans):
    """One HOST-rooted producer attach, its producer free of nested isolation."""
    assert len(plans) == 1, (
        f"Expected exactly one host-rooted producer; got {len(plans)}: {plans}"
    )
    attach = plans[0]
    assert attach.producer_root_model is None, (
        f"Widened Law-3 trigger must produce a HOST-rooted producer; got "
        f"root {attach.producer_root_model!r}"
    )
    assert attach.producer_plan.regroup_attach_plans == [], (
        f"Host-rooted producer must hold its measure inline (recursion "
        f"suppression); got nested attaches: {attach.producer_plan.regroup_attach_plans}"
    )
    return attach


def test_reference_closure_module_exists():
    """Canary: the reference-closure helper module must import."""

    importlib.import_module("slayer.engine.reference_closure")


class TestCrossingFirstLastStaysRanked:
    """A first/last isolates via the ranked route whatever its inputs do (row
    ordering subsumes crossing): one flat ranked host plan. Only the ranking TIME arg surfaces the crossed hop."""

    def test_derived_source_first_last_stays_ranked(self):
        planned, _ = _s5_plans("region_pay:last(orders.created_at)")
        _assert_single_ranked_host(planned)

    def test_structural_time_arg_stays_ranked(self):
        planned, _ = _s5_plans("amount:last(customers.signup_at)")
        attach = _assert_single_ranked_host(planned)
        # The ranking key reaches through the customers join (path () if non-crossing).
        assert attach.kernel.ranking_time_key.path == ("customers",)

    def test_derived_time_arg_stays_ranked(self):
        planned, _ = _s5_plans("amount:last(cross_time)")
        _assert_single_ranked_host(planned)


class TestWidenedLaw3TriggerCrossingInputs:
    """A LOCAL aggregate isolates host-rooted when ANY input crosses a join:
    source ``Column.sql``, positional/time args, kwargs. Filter crossing is pinned above."""

    def test_derived_source_path_alias_triggers(self):
        _, plans = _s5_plans("region_pay:sum")
        _assert_single_host_rooted(plans)

    def test_derived_source_single_dot_triggers(self):
        _, plans = _s5_plans("cust_weight_d:sum")
        _assert_single_host_rooted(plans)

    def test_sibling_derived_chain_triggers(self):
        # doubled_pop -> pop_helper -> crossing; sibling derived refs expand first.
        _, plans = _s5_plans("doubled_pop:sum")
        _assert_single_host_rooted(plans)

    def test_structural_kwarg_triggers(self):
        _, plans = _s5_plans("amount:weighted_avg(weight=customers.weight)")
        _assert_single_host_rooted(plans)

    def test_derived_kwarg_triggers(self):
        _, plans = _s5_plans("amount:weighted_avg(weight=region_pay)")
        _assert_single_host_rooted(plans)

    def test_user_template_fragment_kwarg_crossing_triggers(self):
        _, plans = _s5_plans(
            "amount:scaled_sum(scale='customers.regions.weight')",
        )
        _assert_single_host_rooted(plans)

    def test_model_default_fragment_kwarg_crossing_triggers(self):
        # Crossing input is the model-default AggregationParam sql fragment.
        _, plans = _s5_plans("amount:wscaled_sum")
        _assert_single_host_rooted(plans)

    def test_composite_crossing_leaf_isolates_individually(self):
        # `region_pay:sum + amount:sum`: only the crossing LEAF isolates (one
        # hidden host-rooted plan); the local leaf stays in the base — exactly one.
        planned, plans = _s5_plans("region_pay:sum + amount:sum")
        attach = _assert_single_host_rooted(plans)
        (sub,) = attach.substitutions
        ph_slot = next(
            s for s in planned.row_slots if s.key == sub.placeholder
        )
        assert ph_slot.hidden, (
            "The composite's crossing leaf is consumed through a HIDDEN "
            "placeholder slot (the composite renders in the combined SELECT, "
            "not the leaf)"
        )

    def test_local_derived_source_does_not_trigger(self):
        _, plans = _s5_plans("local_double:sum")
        assert plans == []

    def test_literal_kwarg_does_not_trigger(self):
        _, plans = _s5_plans("amount:percentile(p=0.5)")
        assert plans == []

    def test_local_template_fragment_does_not_trigger(self):
        _, plans = _s5_plans("amount:scaled_sum(scale='amount * 2')")
        assert plans == []

    def test_unparseable_template_fragment_does_not_trigger(self):
        # Parity with the Column.filter scan: an unparseable fragment contributes
        # no paths (defensive fallback), preserving pre-Stage-5 behavior.
        _, plans = _s5_plans("amount:scaled_sum(scale='%% !! ((')")
        assert plans == []

    def test_crossing_dimension_only_does_not_trigger(self):
        # A joined dimension is Law-1 (base-pull); only aggregate inputs trigger Law-3.
        _, plans = _s5_plans("amount:sum", dimensions=["customers.region_id"])
        assert plans == []

    def test_local_time_arg_does_not_trigger(self):
        _, plans = _s5_plans("amount:last(orders.created_at)")
        assert plans == []

    def test_source_path_still_target_rooted(self):
        # A genuine cross-model aggregate stays target-rooted at ``customers``.
        planned, cma = _s5_plans("customers.weight:sum")
        assert cma == []
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_model == "customers"

    def test_producer_mode_keeps_target_rooted(self):
        # Producer mode still plans a cross-model aggregate target-rooted.
        planned = plan_as_producer(query=SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.weight:sum", name="m0")],
        ), bundle=_s5_bundle())
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert attach.producer_root_model == "customers"

    def test_identical_key_shared_between_public_and_composite_leaf(self):
        # Identical AggregateKeys intern to one slot → one plan, even across a
        # public measure and a composite leaf.
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "region_pay:sum"},
                {"formula": "region_pay:sum + amount:sum", "name": "combo"},
            ],
        )
        planned = plan_query(query=q, bundle=_s5_bundle())
        attaches = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert len(attaches) == 1, (
            f"identical crossing keys must share one producer; got {attaches}"
        )

    def test_two_distinct_crossing_measures_two_plans(self):
        # Two DISTINCT crossing aggregates → two separate host-rooted plans.
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "region_pay:sum"},
                {"formula": "cust_weight_d:sum"},
            ],
        )
        planned = plan_query(query=q, bundle=_s5_bundle())
        attaches = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert len(attaches) == 2

    @staticmethod
    def _sub_row_bound_filters(sub) -> list:
        """Field (row-population) mask entries of a sub-plan."""
        return [m for m in sub.masks if m.typing == MaskTyping.FIELD]

    def test_host_row_filter_propagates_into_widened_sub_plan(self):
        # A host ROW filter must propagate into the sub-plan's filters.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "region_pay:sum"}],
            filters=["amount > 50"],
        )
        planned = plan_query(query=q, bundle=_s5_bundle())
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
        assert self._sub_row_bound_filters(sub), (
            f"Host ROW filter must propagate into the host-rooted sub-plan; "
            f"got sub-plan masks {sub.masks!r}"
        )

    def test_pathed_host_row_filter_propagates_into_widened_sub_plan(self):
        # A host ROW filter whose expression itself crosses a join still roots at
        # the host, so it too propagates into the sub-plan.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "region_pay:sum"}],
            filters=["customers.weight > 1"],
        )
        planned = plan_query(query=q, bundle=_s5_bundle())
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
        assert self._sub_row_bound_filters(sub), (
            f"Pathed host ROW filter must propagate into the host-rooted "
            f"sub-plan; got sub-plan masks {sub.masks!r}"
        )
