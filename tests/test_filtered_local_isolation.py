"""DEV-1503 — planner unit tests for cross-model-filtered local measure isolation.

These tests pin the planner-side contract of DEV-1503's isolation feature:

1. ``SqlExprKey.referenced_join_paths`` is populated at binder time and carries
   the typed non-host join paths the filter touches (after derived-ref
   expansion), so the planner reads structural information — not parsed SQL
   text — when deciding whether to isolate.
2. The cross-model planner's invocation predicate fires for empty-path
   aggregates whose ``column_filter_key`` references at least one non-host
   join path, and DOES NOT fire for same-model filters or no-filter cases.
3. The host-rooted sub-plan compiled by ``plan_query`` with
   ``disable_dev1503_isolation=True`` does NOT recursively re-isolate the
   same filtered-local measure (otherwise the isolation recurses forever).
4. A filtered-local first/last measure's host-rooted sub-plan contains
   zero nested ``cross_model_aggregate_plans`` — so the
   ``skip_cross_model_aggs=True`` + local-first/last crash path in
   ``_build_base_select_for_planned`` stays unreachable.

These complement the generator-shape tests in
``tests/test_sql_generator.py::TestIsolatedFilteredMeasureCTEs``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import AggregateKey, SqlExprKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.planned import CrossModelAggregatePlan
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
        # No-filter local measure.
        Column(name="total_amount", sql="amount", type=DataType.DOUBLE),
        # Same-model filter — should NOT trigger isolation.
        Column(name="paid_amount", sql="amount", filter="claim_id > 0", type=DataType.DOUBLE),
        # Cross-model filter (direct dotted ref) — SHOULD trigger.
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
    """Build an ``orders`` host whose ``eu_amount`` filter references the
    derived ``is_eu`` column (whose own sql crosses to ``customers.region``).

    ``eu_amount_filter`` lets each test exercise a different ref shape
    (bare ``is_eu = 1``, self-qualified ``orders.is_eu = 1``) — the
    rest of the setup (host + customers + bundle) is shared, matching
    DEV-1503's "filter expansion must surface the cross-model path
    regardless of ref qualification" contract.
    """
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


def _agg_slot_for(planned, name: str):
    """Find the public aggregate slot whose canonical alias contains ``name``."""
    for slot in planned.aggregate_slots:
        if name in (slot.declared_name or "") or name in (slot.public_name or ""):
            return slot
    return None


# ---------------------------------------------------------------------------
# SqlExprKey.referenced_join_paths — populated at binder time
# ---------------------------------------------------------------------------


class TestSqlExprKeyReferencedJoinPaths:
    """The binder must compute the non-host join paths a ``Column.filter``
    touches and stamp them on the ``SqlExprKey`` as a typed field —
    so the planner reads structural data, not parsed SQL text."""

    def test_same_model_filter_has_no_referenced_paths(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "paid_amount:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        slot = _agg_slot_for(planned, "paid_amount")
        assert slot is not None
        assert isinstance(slot.key, AggregateKey)
        cfk = slot.key.column_filter_key
        assert isinstance(cfk, SqlExprKey)
        # Same-model filter: no non-host paths.
        assert cfk.referenced_join_paths == (), (
            f"Same-model filter must have empty referenced_join_paths; got {cfk.referenced_join_paths!r}"
        )

    def test_dotted_cross_model_filter_records_join_path(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        slot = _agg_slot_for(planned, "loss_payment_amt")
        assert slot is not None
        assert isinstance(slot.key, AggregateKey)
        cfk = slot.key.column_filter_key
        assert isinstance(cfk, SqlExprKey)
        # The filter ``loss_payment.has_flag = 1`` references the
        # ``loss_payment`` join from the host.
        assert ("loss_payment",) in cfk.referenced_join_paths, (
            f"Expected ('loss_payment',) in referenced_join_paths; got {cfk.referenced_join_paths!r}"
        )

    def test_self_qualified_derived_ref_records_expanded_path(self):
        """A ``Column.filter`` like ``filter="orders.is_eu = 1"`` —
        self-qualified to the anchor relation — must trip the same
        derived-expansion gate as the bare ``filter="is_eu = 1"`` form.
        Without that, ``orders.is_eu`` is treated as same-model and the
        join through ``customers`` is missed (CodeRabbit thread 1)."""
        _, bundle = _orders_with_derived_eu_filter(
            eu_amount_filter="orders.is_eu = 1",
        )
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "eu_amount:sum"}],
        )
        planned = plan_query(query=q, bundle=bundle)
        slot = _agg_slot_for(planned, "eu_amount")
        assert slot is not None
        assert isinstance(slot.key, AggregateKey)
        cfk = slot.key.column_filter_key
        assert isinstance(cfk, SqlExprKey)
        assert ("customers",) in cfk.referenced_join_paths, (
            f"Self-qualified derived ref must surface expanded cross-model "
            f"path; got {cfk.referenced_join_paths!r}"
        )

    def test_derived_ref_cross_model_filter_records_expanded_path(self):
        """``Column.filter`` that references a host derived column whose
        own sql crosses a join (1494's derived-ref flavour) must surface
        the EXPANDED join path on ``referenced_join_paths``."""
        _, bundle = _orders_with_derived_eu_filter(
            eu_amount_filter="is_eu = 1",
        )
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "eu_amount:sum"}],
        )
        planned = plan_query(query=q, bundle=bundle)
        slot = _agg_slot_for(planned, "eu_amount")
        assert slot is not None
        cfk = slot.key.column_filter_key
        assert isinstance(cfk, SqlExprKey)
        # Even though the literal canonical_sql is ``is_eu = 1``, the
        # derived-ref expansion at bind time must surface ``customers``.
        assert ("customers",) in cfk.referenced_join_paths, (
            f"derived-ref expansion missed customers path; got {cfk.referenced_join_paths!r}"
        )


# ---------------------------------------------------------------------------
# Trigger predicate: cross-model planner invocation
# ---------------------------------------------------------------------------


class TestCrossModelPlannerTriggerPredicate:
    """Pins the extended trigger: invoke the cross-model planner if the
    aggregate's source.path is non-empty OR its column_filter_key references
    at least one non-host join path."""

    def test_no_filter_local_measure_does_not_trigger(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "total_amount:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert planned.cross_model_aggregate_plans == [], (
            f"Plain local aggregate must not trigger isolation; got plans: {planned.cross_model_aggregate_plans}"
        )

    def test_same_model_filter_does_not_trigger(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "paid_amount:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert planned.cross_model_aggregate_plans == [], (
            f"Same-model filter must not trigger isolation; got plans: {planned.cross_model_aggregate_plans}"
        )

    def test_dotted_cross_model_filter_triggers_isolation(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert len(planned.cross_model_aggregate_plans) == 1, (
            f"Expected 1 isolated plan; got {len(planned.cross_model_aggregate_plans)}"
        )
        plan = planned.cross_model_aggregate_plans[0]
        assert isinstance(plan, CrossModelAggregatePlan)
        # The CTE is rooted at the HOST model for the filtered-local case.
        assert plan.cte_root_model == "claim_amount", (
            f"Expected cte_root_model='claim_amount' for filtered-local; got {plan.cte_root_model!r}"
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
        assert len(planned.cross_model_aggregate_plans) == 1, (
            f"Derived-ref cross-model filter must trigger isolation; got {len(planned.cross_model_aggregate_plans)} plans"
        )
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.cte_root_model == "orders"

    def test_cross_model_agg_still_triggers_via_path(self):
        """Pre-existing case must still work: an aggregate with a non-empty
        ``source.path`` triggers the cross-model planner regardless of any
        column_filter_key — extending the predicate must not regress this.
        Also pins ``target_model`` semantics: for a genuine cross-model
        aggregate, ``target_model`` is the join target (``loss_payment``)
        and ``cte_root_model`` stays None (Codex review #7)."""
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment.has_flag:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert len(planned.cross_model_aggregate_plans) == 1, (
            f"Cross-model aggregate must still trigger; got {len(planned.cross_model_aggregate_plans)}"
        )
        plan = planned.cross_model_aggregate_plans[0]
        # ``target_model`` is the join target.
        assert plan.target_model == "loss_payment", (
            f"Expected target_model='loss_payment' for cross-model agg; got {plan.target_model!r}"
        )
        # ``cte_root_model`` stays None for genuine cross-model — the existing
        # forward / re-rooted rendering path is unchanged.
        assert plan.cte_root_model is None, (
            f"Genuine cross-model aggregate must keep cte_root_model=None; got {plan.cte_root_model!r}"
        )

    def test_cross_model_agg_with_target_column_filter_does_not_become_filtered_local(self):
        """DEV-1494's case: a CROSS-MODEL aggregate whose TARGET column has
        its own ``Column.filter`` (e.g. ``customers.premium_rev:sum`` where
        ``premium_rev`` filters via ``regions.is_premium = 1``) must keep
        genuine cross-model semantics — NOT be reinterpreted as a host-rooted
        filtered-local plan.

        Concretely: ``target_model`` is the join target (``customers``),
        ``cte_root_model`` is None (existing cross-model rendering path),
        and the target's own join graph reachability for the filter is
        handled by the existing cross-model CTE — not by host-rooted
        isolation.

        Pins Codex review #6.
        """
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
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        # Genuine cross-model: target is `customers`, NOT reinterpreted as host-rooted.
        assert plan.target_model == "customers", (
            f"Cross-model + target-filter must keep target_model='customers'; got {plan.target_model!r}"
        )
        assert plan.cte_root_model is None, (
            f"Cross-model + target-filter must keep cte_root_model=None (NOT host-rooted); "
            f"got {plan.cte_root_model!r}"
        )


# ---------------------------------------------------------------------------
# Recursion suppression
# ---------------------------------------------------------------------------


class TestRecursionSuppression:
    """The host-rooted sub-plan built by ``IsolatedCteCrossModelPlanner`` for a
    filtered-local measure is compiled by ``plan_query`` recursively. Without
    suppression, the sub-plan's same filtered-local aggregate would re-trigger
    the isolation rule → infinite recursion. ``disable_dev1503_isolation=True``
    on the recursive ``plan_query`` call suppresses the DEV-1503 trigger so the
    sub-plan compiles cleanly with the filtered measure as a plain local aggregate."""

    def test_disable_kwarg_suppresses_trigger(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(
            query=q, bundle=_bundle(host), disable_dev1503_isolation=True,
        )
        # With the trigger suppressed, the filtered measure does NOT isolate
        # — it stays as a plain local aggregate.
        assert planned.cross_model_aggregate_plans == [], (
            f"disable_dev1503_isolation=True must suppress isolation; "
            f"got plans: {planned.cross_model_aggregate_plans}"
        )
        # The aggregate slot must still exist on the (now non-isolated) plan.
        slot = _agg_slot_for(planned, "loss_payment_amt")
        assert slot is not None

    def test_isolated_sub_plan_has_no_nested_cma_plans(self):
        """An isolated filtered-local measure's host-rooted sub-plan
        (attached via ``rerooted_plan``) must contain zero nested
        ``cross_model_aggregate_plans``. Without recursion suppression the
        sub-plan's same filtered measure would re-isolate → infinite recursion
        or a doubly-wrapped CTE. Pin both invariants here so a regression
        surfaces as a structural assertion, not a stack overflow."""
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(query=q, bundle=_bundle(host))
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        # Filtered-local plans always carry a host-rooted nested sub-plan.
        assert plan.rerooted_plan is not None, (
            "Filtered-local plan must carry rerooted_plan (the host-rooted sub-plan)"
        )
        sub = plan.rerooted_plan
        assert sub.cross_model_aggregate_plans == [], (
            f"Host-rooted sub-plan must have NO nested cross_model_aggregate_plans; "
            f"got {sub.cross_model_aggregate_plans}"
        )


# ---------------------------------------------------------------------------
# First/last + zero nested CMA plans
# ---------------------------------------------------------------------------


class TestHostModelFiltersInteractions:
    """Host ``SlayerModel.filters`` (Mode-A always-applied WHERE filters)
    must apply inside the host-rooted filtered-local sub-plan, and the
    documented known limit (model filters cannot reference aggregate
    measures) must surface as a clear ``ValueError`` rather than emit
    invalid SQL."""

    def test_host_model_filter_lands_inside_filtered_local_sub_plan(self):
        """A ``SlayerModel.filters`` entry on the host model must apply
        inside the host-rooted filtered-local sub-plan — the sub-plan is
        compiled by ``plan_query`` recursively, so it picks up the host's
        ``model.filters`` via the normal predicate path. Pin this by
        asserting the model filter's text appears in the sub-plan's
        ``filters_by_phase``.

        Pins Codex review #2 (host model filters inside the sub-plan).
        """
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
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.rerooted_plan is not None
        sub = plan.rerooted_plan
        # The host model filter must appear in the sub-plan's filters_by_phase.
        sub_filter_texts = [
            fp.text for fp in sub.filters_by_phase if fp.text is not None
        ]
        assert "amount > 0" in sub_filter_texts, (
            f"Host model filter must land inside the host-rooted sub-plan; "
            f"got sub-plan filter texts {sub_filter_texts!r}"
        )

    def test_host_model_filter_referencing_aggregate_measure_raises(self):
        """Known documented limit: ``SlayerModel.filters`` are Mode-A text
        WHERE filters and cannot reference aggregate measures. Attempting to
        construct such a model must surface as a typed validation error
        (``ValueError`` — Pydantic ``ValidationError`` is a subclass), NOT
        silently emit invalid SQL downstream.

        Pins Codex review #3 (known limit pinned as an error).
        """
        # Pydantic catches the illegal filter at model construction time
        # (sql_predicate's ``_reject_dsl_constructs`` runs in the field
        # validator). ``ValidationError`` is a ``ValueError`` subclass, so a
        # broad ``ValueError`` match catches both this and any later
        # surfacing layer.
        with pytest.raises(ValueError, match=r"(?i)aggregation colon syntax|measure"):
            SlayerModel(
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
                # Illegal: model.filters cannot reference an aggregate measure.
                filters=["loss_payment_amt:sum > 0"],
            )


class TestFirstLastNoNestedCmaPlans:
    """``_build_base_select_for_planned`` raises NotImplementedError when
    ``skip_cross_model_aggs=True`` AND any local first/last aggregate is in
    base_render_order. The isolated filtered-local first/last case routes
    through that combination — so the host-rooted sub-plan MUST NOT contain
    any nested cross-model aggregate plans, or the renderer crashes.

    Pin this structurally on the sub-plan; the rendering test that proves
    the SQL emits ``_last_rn`` inside the _cm_ CTE lives in
    ``test_sql_generator.py::TestIsolatedFilteredMeasureCTEs``."""

    def test_filtered_local_first_last_sub_plan_is_clean(self):
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
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.rerooted_plan is not None
        # The host-rooted sub-plan carries the local first/last aggregate.
        # It must not contain any nested cross-model aggregate plans —
        # otherwise the generator's first/last + skip_cross_model_aggs
        # combination would raise NotImplementedError.
        assert plan.rerooted_plan.cross_model_aggregate_plans == [], (
            f"Filtered-local first/last sub-plan must be clean of nested CMA plans; "
            f"got {plan.rerooted_plan.cross_model_aggregate_plans}"
        )
