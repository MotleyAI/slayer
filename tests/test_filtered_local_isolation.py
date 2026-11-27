"""Planner unit tests for cross-dataset-filtered local metric isolation.

Pins: (1) the cross-dataset planner's trigger fires for empty-path aggregates
whose filter references a non-host relationship path, not for same-dataset / no-filter
cases; (2) the recursive host-rooted sub-plan does not re-isolate; (3) a
filtered-local first/last holds its metric inline.
"""

from __future__ import annotations

import importlib

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, JoinType, TimeGranularity
from slayer.core.errors import UnanalyzableAggregationParameterError
from slayer.core.keys import FieldKey
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import QueryMetric, SlayerQuery, TimeDimension
from slayer.ir.planned import MaskTyping, PlannedQuery, RankedProducerKernel, RegroupAttachPlan
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query
from slayer.sql.generator import SQLGenerator

from tests._engine_helpers import plan_as_producer


# Fixtures


def _loss_payment() -> SlayerDataset:
    return SlayerDataset(
        name="loss_payment", data_source="test", sql_table="Loss_Payment",
        fields=[
            DatasetField(name="claim_amount_id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="has_flag", expression="1", datatype=DataType.FLOAT),
        ],
    )


def _loss_reserve() -> SlayerDataset:
    return SlayerDataset(
        name="loss_reserve", data_source="test", sql_table="Loss_Reserve",
        fields=[
            DatasetField(name="claim_amount_id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="has_flag", expression="1", datatype=DataType.FLOAT),
        ],
    )


def _claim() -> SlayerDataset:
    return SlayerDataset(
        name="claim", data_source="test", sql_table="Claim",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="claim_number", datatype=DataType.STRING),
        ],
    )


def _claim_amount(
    *, with_time: bool = False, with_filter_first: bool = False,
) -> SlayerDataset:
    cols = [
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="claim_id", datatype=DataType.INTEGER),
        DatasetField(name="amount", datatype=DataType.FLOAT),
        DatasetField(name="total_amount", expression="amount", datatype=DataType.FLOAT),
        # Same-dataset filter — must NOT trigger isolation.
        DatasetField(name="paid_amount", expression="amount", filter="claim_id > 0", datatype=DataType.FLOAT),
        # Cross-dataset filter — must trigger.
        DatasetField(
            name="loss_payment_amt", expression="amount",
            filter="loss_payment.has_flag = 1", datatype=DataType.FLOAT,
        ),
    ]
    if with_time:
        cols.append(DatasetField(name="created_at", datatype=DataType.DATETIME))
    if with_filter_first:
        cols.append(DatasetField(
            name="latest_payment", expression="amount",
            filter="loss_payment.has_flag = 1", datatype=DataType.FLOAT,
        ))
    return SlayerDataset(
        name="claim_amount", data_source="test", sql_table="Claim_Amount",
        fields=cols,
        relationships=[
            DatasetRelationship(
                to="loss_payment",
                from_columns=["id"], to_columns=["claim_amount_id"],
                join_type="inner",
            ),
            DatasetRelationship(
                to="loss_reserve",
                from_columns=["id"], to_columns=["claim_amount_id"],
                join_type="inner",
            ),
            DatasetRelationship(
                to="claim",
                from_columns=["claim_id"], to_columns=["id"],
            ),
        ],
        default_time_dimension="created_at" if with_time else None,
    )


def _bundle(host: SlayerDataset) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=host,
        referenced_datasets=[_loss_payment(), _loss_reserve(), _claim()],
    )


def _orders_with_derived_eu_filter(*, eu_amount_filter: str):
    """``orders`` host whose ``eu_amount`` filter references derived ``is_eu`` (crosses to customers.region); ``eu_amount_filter`` varies the ref shape."""
    host = SlayerDataset(
        name="orders", data_source="test", sql_table="Orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(
                name="is_eu", datatype=DataType.FLOAT,
                expression="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END",
            ),
            DatasetField(
                name="eu_amount", expression="amount", filter=eu_amount_filter,
                datatype=DataType.FLOAT,
            ),
        ],
        relationships=[DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"],
        )],
    )
    customers = SlayerDataset(
        name="customers", data_source="test", sql_table="Customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", datatype=DataType.STRING),
        ],
    )
    bundle = ResolvedSourceBundle(
        dialect="postgres",
        population=host, referenced_datasets=[customers],
    )
    return host, bundle


# Trigger predicate: cross-dataset planner invocation


class TestCrossModelPlannerTriggerPredicate:
    """Trigger: invoke the cross-dataset planner if source.path is non-empty OR the filter references a non-host relationship path."""

    def test_no_filter_local_measure_does_not_trigger(self):
        host = _claim_amount()
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "sum(claim_amount.total_amount)"}],
            "dimensions": ["claim.claim_number"],
        })
        planned = plan_query(query=q, bundle=_bundle(host))
        assert planned.regroup_attach_plans == [], (
            f"Plain local aggregate must not trigger isolation; got attaches: {planned.regroup_attach_plans}"
        )

    def test_same_model_filter_does_not_trigger(self):
        host = _claim_amount()
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "sum(claim_amount.paid_amount)"}],
            "dimensions": ["claim.claim_number"],
        })
        planned = plan_query(query=q, bundle=_bundle(host))
        assert planned.regroup_attach_plans == [], (
            f"Same-dataset filter must not trigger isolation; got attaches: {planned.regroup_attach_plans}"
        )

    def test_dotted_cross_model_filter_triggers_isolation(self):
        host = _claim_amount()
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "sum(claim_amount.loss_payment_amt)"}],
            "dimensions": ["claim.claim_number"],
        })
        planned = plan_query(query=q, bundle=_bundle(host))
        attaches = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert len(attaches) == 1, (
            f"Expected 1 host-rooted producer; got {attaches}"
        )
        assert attaches[0].producer_root_dataset is None, (
            f"Expected a HOST-rooted producer for filtered-local; got root "
            f"{attaches[0].producer_root_dataset!r}"
        )

    def test_derived_ref_cross_model_filter_triggers_isolation(self):
        _, bundle = _orders_with_derived_eu_filter(
            eu_amount_filter="orders.is_eu = 1",
        )
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.eu_amount)"}],
            "dimensions": ["orders.id"],
        })
        planned = plan_query(query=q, bundle=bundle)
        attaches = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert len(attaches) == 1, (
            f"Derived-ref cross-dataset filter must trigger isolation; got "
            f"{attaches}"
        )
        assert attaches[0].producer_root_dataset is None

    def test_cross_model_agg_still_triggers_via_path(self):
        """A non-empty ``source.path`` triggers the cross-dataset planner; the producer roots at the relationship target ``loss_payment``."""
        host = _claim_amount()
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "sum(loss_payment.has_flag)"}],
            "dimensions": ["claim.claim_number"],
        })
        planned = plan_query(query=q, bundle=_bundle(host))
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_dataset == "loss_payment"

    def test_cross_model_agg_with_target_column_filter_does_not_become_filtered_local(self):
        """A cross-dataset aggregate whose TARGET field has its own filter keeps genuine cross-dataset semantics, not host-rooted filtered-local."""
        regions = SlayerDataset(
            name="regions", data_source="test", sql_table="regions",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="is_premium", datatype=DataType.FLOAT),
            ],
        )
        customers = SlayerDataset(
            name="customers", data_source="test", sql_table="customers",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region_id", datatype=DataType.INTEGER),
                DatasetField(name="revenue", datatype=DataType.FLOAT),
                DatasetField(
                    name="premium_rev", expression="revenue",
                    filter="regions.is_premium = 1", datatype=DataType.FLOAT,
                ),
            ],
            relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        )
        orders = SlayerDataset(
            name="orders", data_source="test", sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        )
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=orders, referenced_datasets=[customers, regions],
        )
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(customers.premium_rev)"}],
            "dimensions": ["orders.id"],
        })
        planned = plan_query(query=q, bundle=bundle)
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_dataset == "customers"


# Producer mode


class TestProducerMode:
    """A host-rooted producer holds its own answer inline; it never re-isolates."""

    def test_isolated_sub_plan_has_no_nested_cma_plans(self):
        """The host-rooted sub-plan must contain zero nested attaches, else the filtered metric re-isolates."""
        host = _claim_amount()
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "sum(claim_amount.loss_payment_amt)"}],
            "dimensions": ["claim.claim_number"],
        })
        planned = plan_query(query=q, bundle=_bundle(host))
        # The nested scope is the producer plan itself.
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
        assert sub.regroup_attach_plans == [], (
            f"Host-rooted producer must hold its metric inline (recursion "
            f"suppression); got nested attaches: {sub.regroup_attach_plans}"
        )


# First/last + zero nested CMA plans


class TestHostModelFiltersInteractions:
    """Host ``SlayerDataset.filters`` apply inside the sub-plan; the known limit (no aggregate-metric refs) raises ``ValueError``."""

    def test_host_model_filter_lands_inside_filtered_local_sub_plan(self):
        """Every read of the host's table, the combined sub-plan's included, is its filtered dataset."""
        host = SlayerDataset(
            name="claim_amount", data_source="test", sql_table="Claim_Amount",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="amount", datatype=DataType.FLOAT),
                DatasetField(
                    name="loss_payment_amt", expression="amount",
                    filter="loss_payment.has_flag = 1", datatype=DataType.FLOAT,
                ),
            ],
            relationships=[DatasetRelationship(
                to="loss_payment",
                from_columns=["id"], to_columns=["claim_amount_id"],
                join_type="inner",
            )],
            filters=["amount > 0"],
        )
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=host, referenced_datasets=[_loss_payment()],
        )
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "sum(claim_amount.loss_payment_amt)"}],
            "dimensions": ["claim_amount.id"],
        })
        planned = plan_query(query=q, bundle=bundle)
        assert [a for a in planned.regroup_attach_plans if a.attach_phase == "combined"]
        sql = SQLGenerator(dialect="postgres").generate_from_planned(planned, bundle=bundle)
        reads = [t for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table) if t.name == "Claim_Amount"]
        assert reads, sql
        for table in reads:
            select = table.parent_select
            assert select is not None, sql
            assert isinstance(select.parent, exp.Subquery), sql
            where = select.args.get("where")
            assert where is not None, sql
            assert "amount > 0" in where.sql(dialect="postgres").lower(), sql

    def test_host_model_filter_referencing_aggregate_measure_raises(self):
        """``dataset.filters`` referencing an aggregate metric must raise at construction (``ValidationError`` is a ``ValueError``), not emit bad SQL."""
        columns = [
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(
                name="loss_payment_amt", expression="amount",
                filter="loss_payment.has_flag = 1", datatype=DataType.FLOAT,
            ),
        ]
        joins = [DatasetRelationship(
            to="loss_payment",
            from_columns=["id"], to_columns=["claim_amount_id"],
            join_type=JoinType.INNER,
        )]
        with pytest.raises(ValueError, match=r"(?i)aggregation reference|measure"):
            SlayerDataset(
                name="claim_amount", data_source="test", sql_table="Claim_Amount",
                fields=columns,
                relationships=joins,
                # Illegal: dataset.filters cannot reference an aggregate metric.
                filters=["loss_payment_amt:sum > 0"],
            )


class TestFirstLastNoNestedCmaPlans:
    """A filtered-local first/last isolates flatly — one ranked plan, its ``DatasetField.filter`` a predicate inside its own CTE."""

    def test_filtered_local_first_last_isolates_without_nesting(self):
        host = _claim_amount(with_time=True, with_filter_first=True)
        q = SlayerQuery.model_validate({
            "population": "claim_amount",
            "metrics": [{"metric": "last(claim_amount.latest_payment)"}],
            "dimensions": ["claim.claim_number"],
            "time_dimensions": [
                TimeDimension(
                    field="claim_amount.created_at",
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        })
        planned = plan_query(query=q, bundle=_bundle(host))
        # The local ranked first/last desugars into a ranked-kernel producer attach.
        assert len(planned.regroup_attach_plans) == 1
        attach = planned.regroup_attach_plans[0]
        assert attach.kernel.kind == "ranked"
        assert attach.producer_root_dataset is None  # rooted at the host
        # The crossing filter rides on the source field; no second plan needed.
        assert len(attach.producer_plan.aggregate_slots) == 1


# Widened Law-3 trigger: ANY crossing input isolates.


def _s5_regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="payment_amount", datatype=DataType.FLOAT),
            DatasetField(name="weight", datatype=DataType.FLOAT),
        ],
    )


def _s5_customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="weight", datatype=DataType.FLOAT),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _s5_orders() -> SlayerDataset:
    """Host with one derived field per crossing-input kind, plus custom aggregations."""
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="region_pay", expression="customers.regions.payment_amount",
                   datatype=DataType.FLOAT),
            DatasetField(name="cust_weight_d", expression="customers.weight",
                   datatype=DataType.FLOAT),
            # Sibling derived chain: doubled_pop -> pop_helper -> crossing.
            DatasetField(name="pop_helper", expression="customers.regions.payment_amount",
                   datatype=DataType.FLOAT),
            DatasetField(name="doubled_pop", expression="orders.pop_helper * 2",
                   datatype=DataType.FLOAT),
            # Derived TIME field crossing a relationship (explicit-arg use only).
            DatasetField(name="cross_time", expression="customers.signup_at",
                   datatype=DataType.DATETIME),
            DatasetField(name="local_double", expression="amount * 2", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"],
        )],
        aggregations=[
            # User-supplied template-fragment kwarg (`scale=` str values).
            Aggregation(
                name="scaled_sum", expression="SUM({value}) / {scale}",
                params=[AggregationParam(name="scale", sql="1")],
            ),
            # Dataset-default fragment CROSSES a relationship — the param sql is the crossing input.
            Aggregation(
                name="wscaled_sum", expression="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
        ],
    )


def _s5_bundle(host: SlayerDataset | None = None) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=host or _s5_orders(),
        referenced_datasets=[_s5_customers(), _s5_regions()],
    )


def _s5_plans(expression: str, *, dimensions=None, **plan_kwargs):
    q = SlayerQuery.model_validate({
        "population": "orders",
        "metrics": [{"metric": expression, "alias": "m0"}],
        "dimensions": dimensions,
    })
    planned = plan_query(query=q, bundle=_s5_bundle(), **plan_kwargs)
    # Keep only HOST-rooted PLAIN-kernel producers (ranked/windowed roots excluded).
    return planned, [
        a for a in planned.regroup_attach_plans
        if a.attach_phase == "combined"
        and a.producer_root_dataset is None
        and a.kernel.kind == "plain"
    ]


def _assert_single_ranked_host(planned: PlannedQuery) -> RegroupAttachPlan:
    """A first/last isolates through the ranked route, one regroup producer with no nested plan. Returns the attach."""
    assert len(planned.regroup_attach_plans) == 1, (
        f"Expected exactly one regroup-attach producer; "
        f"got {planned.regroup_attach_plans}"
    )
    attach = planned.regroup_attach_plans[0]
    assert attach.kernel.kind == "ranked", attach.kernel
    assert attach.producer_root_dataset is None, attach.producer_root_dataset
    assert attach.producer_plan.regroup_attach_plans == [], (
        f"the ranked producer must hold its metric inline; "
        f"got nested attaches: {attach.producer_plan.regroup_attach_plans}"
    )
    return attach


def _assert_single_host_rooted(plans: list[RegroupAttachPlan]) -> RegroupAttachPlan:
    """One HOST-rooted producer attach, its producer free of nested isolation."""
    assert len(plans) == 1, (
        f"Expected exactly one host-rooted producer; got {len(plans)}: {plans}"
    )
    attach = plans[0]
    assert attach.producer_root_dataset is None, (
        f"Widened Law-3 trigger must produce a HOST-rooted producer; got "
        f"root {attach.producer_root_dataset!r}"
    )
    assert attach.producer_plan.regroup_attach_plans == [], (
        f"Host-rooted producer must hold its metric inline (recursion "
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
        planned, _ = _s5_plans("last(orders.region_pay, orders.created_at)")
        _assert_single_ranked_host(planned)

    def test_structural_time_arg_stays_ranked(self):
        planned, _ = _s5_plans("last(orders.amount, customers.signup_at)")
        attach = _assert_single_ranked_host(planned)
        # The ranking key reaches through the customers relationship (path () if non-crossing).
        assert isinstance(attach.kernel, RankedProducerKernel)
        assert isinstance(attach.kernel.ranking_time_key, FieldKey)
        assert attach.kernel.ranking_time_key.path == ("customers",)

    def test_derived_time_arg_stays_ranked(self):
        planned, _ = _s5_plans("last(orders.amount, orders.cross_time)")
        _assert_single_ranked_host(planned)


class TestWidenedLaw3TriggerCrossingInputs:
    """A LOCAL aggregate isolates host-rooted when ANY input crosses a relationship:
    source ``DatasetField.sql``, positional/time args, kwargs. Filter crossing is pinned above."""

    def test_derived_source_path_alias_triggers(self):
        _, plans = _s5_plans("sum(orders.region_pay)")
        _assert_single_host_rooted(plans)

    def test_derived_source_single_dot_triggers(self):
        _, plans = _s5_plans("sum(orders.cust_weight_d)")
        _assert_single_host_rooted(plans)

    def test_sibling_derived_chain_triggers(self):
        # doubled_pop -> pop_helper -> crossing; sibling derived refs expand first.
        _, plans = _s5_plans("sum(orders.doubled_pop)")
        _assert_single_host_rooted(plans)

    def test_structural_kwarg_triggers(self):
        _, plans = _s5_plans("weighted_avg(orders.amount, weight=customers.weight)")
        _assert_single_host_rooted(plans)

    def test_derived_kwarg_triggers(self):
        _, plans = _s5_plans("weighted_avg(orders.amount, weight=orders.region_pay)")
        _assert_single_host_rooted(plans)

    def test_user_template_fragment_kwarg_crossing_triggers(self):
        _, plans = _s5_plans(
            "scaled_sum(orders.amount, scale='customers.regions.weight')",
        )
        _assert_single_host_rooted(plans)

    def test_model_default_fragment_kwarg_crossing_triggers(self):
        # Crossing input is the dataset-default AggregationParam sql fragment.
        _, plans = _s5_plans("wscaled_sum(orders.amount)")
        _assert_single_host_rooted(plans)

    def test_composite_crossing_leaf_isolates_individually(self):
        # `sum(region_pay) + sum(amount)`: only the crossing LEAF isolates (one
        # hidden host-rooted plan); the local leaf stays in the base — exactly one.
        planned, plans = _s5_plans("sum(orders.region_pay) + sum(orders.amount)")
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
        _, plans = _s5_plans("sum(orders.local_double)")
        assert plans == []

    def test_literal_kwarg_does_not_trigger(self):
        _, plans = _s5_plans("percentile(orders.amount, p=0.5)")
        assert plans == []

    def test_local_template_fragment_does_not_trigger(self):
        _, plans = _s5_plans("scaled_sum(orders.amount, scale='orders.amount * 2')")
        assert plans == []

    def test_unparseable_template_fragment_fails_at_binding(self):
        with pytest.raises(UnanalyzableAggregationParameterError, match="scale"):
            _s5_plans("scaled_sum(orders.amount, scale='%% !! ((')")

    def test_crossing_dimension_only_does_not_trigger(self):
        # A joined dimension is Law-1 (base-pull); only aggregate inputs trigger Law-3.
        _, plans = _s5_plans("sum(orders.amount)", dimensions=["customers.region_id"])
        assert plans == []

    def test_local_time_arg_does_not_trigger(self):
        _, plans = _s5_plans("last(orders.amount, orders.created_at)")
        assert plans == []

    def test_source_path_still_target_rooted(self):
        # A genuine cross-dataset aggregate stays target-rooted at ``customers``.
        planned, cma = _s5_plans("sum(customers.weight)")
        assert cma == []
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_dataset == "customers"

    def test_producer_mode_keeps_target_rooted(self):
        # Producer mode still plans a cross-dataset aggregate target-rooted.
        planned = plan_as_producer(query=SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="sum(customers.weight)", alias="m0")],
        ), bundle=_s5_bundle())
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert attach.producer_root_dataset == "customers"

    def test_identical_key_shared_between_public_and_composite_leaf(self):
        # Identical AggregateKeys intern to one slot → one plan, even across a
        # public metric and a composite leaf.
        q = SlayerQuery(
            population="orders",
            metrics=[
                {"metric": "sum(orders.region_pay)"},
                {"metric": "sum(orders.region_pay) + sum(orders.amount)", "alias": "combo"},
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
            population="orders",
            metrics=[
                {"metric": "sum(orders.region_pay)"},
                {"metric": "sum(orders.cust_weight_d)"},
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
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.region_pay)"}],
            "filter": ["orders.amount > 50"],
        })
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
        # A host ROW filter whose expression itself crosses a relationship still roots at
        # the host, so it too propagates into the sub-plan.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.region_pay)"}],
            "filter": ["customers.weight > 1"],
        })
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
