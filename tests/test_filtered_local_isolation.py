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
   ``disable_host_rooted_isolation=True`` (DEV-1709 rename of
   ``disable_dev1503_isolation``) does NOT recursively re-isolate the
   same filtered-local measure (otherwise the isolation recurses forever).
4. A filtered-local first/last measure's host-rooted producer plan holds
   its measure inline — zero nested attaches — so nothing recurses.

These complement the generator-shape tests in
``tests/test_sql_generator.py::TestIsolatedFilteredMeasureCTEs``.
"""

from __future__ import annotations

import importlib

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import AggregateKey, Phase, SqlExprKey
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.column_filter_paths import compute_column_filter_join_paths
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
    """Find the aggregate slot whose canonical alias contains ``name`` — the
    top level first, then inside producer plans (DEV-1838 D5: a crossing
    aggregate lives on its host-rooted producer)."""
    for attach in planned.regroup_attach_plans:
        nested = _agg_slot_for(attach.producer_plan, name)
        if nested is not None:
            return nested
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

    def test_dialect_fallback_chain_recovers_mysql_backtick_filter(self):
        """A ``Column.filter`` using MySQL backtick identifiers must still
        surface its non-anchor join paths even though Postgres can't
        parse backticks. The fallback chain in
        ``compute_column_filter_join_paths`` tries multiple dialects;
        without it, the helper would silently return ``()`` and the
        DEV-1503 trigger would miss, letting the cross-model filter
        join leak into ``_base`` and silently corrupt the aggregate
        (Codex round 7).
        """
        host = _claim_amount()
        bundle = _bundle(host)
        # Backtick-quoted (MySQL/T-SQL style) joined alias ref.
        paths = compute_column_filter_join_paths(
            canonical_sql="`loss_payment`.`has_flag` = 1",
            anchor_model=host,
            anchor_relation="claim_amount",
            bundle=bundle,
        )
        assert ("loss_payment",) in paths, (
            f"Dialect fallback must recover the join path from a "
            f"backtick-quoted filter; got {paths!r}"
        )

    def test_dialect_fallback_chain_recovers_derived_ref_with_backticks(self):
        """A derived column whose ``Column.sql`` uses MySQL backtick
        syntax must still expand correctly when referenced from a
        ``Column.filter`` — the round-7 outer fallback chain didn't
        cover ``expand_derived_refs_sync`` itself, so a derived expansion
        on backtick SQL would silently fail under hard-coded Postgres,
        dropping the join path (Codex round 9).
        """
        host = SlayerModel(
            name="orders", data_source="test", sql_table="Orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
                # Derived column whose sql uses MySQL backtick syntax —
                # Postgres can't parse this.
                Column(
                    name="is_eu", type=DataType.DOUBLE,
                    sql="CASE WHEN `customers`.`region` = 'EU' THEN 1 ELSE 0 END",
                ),
                Column(
                    name="eu_amount", sql="amount", filter="is_eu = 1",
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
        paths = compute_column_filter_join_paths(
            canonical_sql="is_eu = 1",
            anchor_model=host,
            anchor_relation="orders",
            bundle=bundle,
        )
        assert ("customers",) in paths, (
            f"Dialect fallback must cover the derived-expansion path; "
            f"got {paths!r}"
        )

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
        # DEV-1838 D5: filtered-local isolates as a HOST-rooted producer.
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
        # DEV-1836: a genuine cross-model aggregate desugars into a TARGET-rooted
        # regroup producer (rooted at the join target ``loss_payment``), not a
        # host-rooted filtered-local plan.
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_model == "loss_payment"

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
        # DEV-1836: genuine cross-model, desugared into a TARGET-rooted producer
        # rooted at ``customers`` — NOT reinterpreted as a host-rooted
        # filtered-local plan (root None would be a local producer).
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_model == "customers"


# ---------------------------------------------------------------------------
# Recursion suppression
# ---------------------------------------------------------------------------


class TestRecursionSuppression:
    """The host-rooted sub-plan built by ``IsolatedCteCrossModelPlanner`` for a
    filtered-local measure is compiled by ``plan_query`` recursively. Without
    suppression, the sub-plan's same filtered-local aggregate would re-trigger
    the isolation rule → infinite recursion. ``disable_host_rooted_isolation=True``
    (DEV-1709 rename of ``disable_dev1503_isolation``) on the recursive
    ``plan_query`` call suppresses the host-rooted trigger so the
    sub-plan compiles cleanly with the filtered measure as a plain local aggregate."""

    def test_disable_kwarg_suppresses_trigger(self):
        host = _claim_amount()
        q = SlayerQuery(
            source_model="claim_amount",
            measures=[{"formula": "loss_payment_amt:sum"}],
            dimensions=["claim.claim_number"],
        )
        planned = plan_query(
            query=q, bundle=_bundle(host), disable_host_rooted_isolation=True,
        )
        # With local discovery suppressed, the filtered measure does NOT
        # desugar — it stays as a plain local aggregate.
        assert planned.regroup_attach_plans == [], (
            f"disable_host_rooted_isolation=True must suppress the desugar; "
            f"got attaches: {planned.regroup_attach_plans}"
        )
        # The aggregate slot must still exist on the (now non-isolated) plan.
        slot = _agg_slot_for(planned, "loss_payment_amt")
        assert slot is not None

    def test_isolated_sub_plan_has_no_nested_cma_plans(self):
        """An isolated filtered-local measure's host-rooted sub-plan
        (attached via ``rerooted_plan``) must contain zero nested
        attaches. Without recursion suppression the
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
        # DEV-1838 D5: the nested scope is the producer plan itself.
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
        assert sub.regroup_attach_plans == [], (
            f"Host-rooted producer must hold its measure inline (recursion "
            f"suppression); got nested attaches: {sub.regroup_attach_plans}"
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
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        sub = attach.producer_plan
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
    """A filtered-local first/last isolates FLATLY — one ranked plan, no nested
    sub-plan at all.

    It used to be a host-rooted ``_cm_`` plan wrapping a re-rooted sub-plan that
    carried the ranking, and this class pinned the one thing that made that
    legal: the sub-plan had to contain no nested cross-model plan of its own, or
    the generator hit ``first/last`` + ``skip_cross_model_aggs`` and raised.

    ``RankedAggregatePlan`` removes the nesting rather than constraining it. The
    aggregate's ``Column.filter`` is a predicate inside its own CTE, which is
    where the crossing it introduces belongs — so there is no second plan to
    keep clean."""

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
        # DEV-1835/DEV-1838: the local ranked first/last desugars into a
        # ranked-kernel producer attach; the ranking lives on the kernel.
        assert len(planned.regroup_attach_plans) == 1
        attach = planned.regroup_attach_plans[0]
        assert attach.kernel.kind == "ranked"
        assert attach.producer_root_model is None  # rooted at the host
        producer = attach.producer_plan
        # The measure's crossing ``Column.filter`` rides on the aggregate's own
        # key, which the producer slot names — nothing needs a second plan.
        (slot,) = producer.aggregate_slots
        assert slot.key.column_filter_key is not None


# ---------------------------------------------------------------------------
# DEV-1709 (Stage 5) — widened Law-3 trigger: ANY crossing input isolates
# ---------------------------------------------------------------------------


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
    """Host with one derived column per crossing-input kind DEV-1709 widens
    the Law-3 trigger to, plus custom aggregations for the template-fragment
    kwarg kinds and local negatives."""
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            # Derived source crossing via multi-hop `__` path alias.
            Column(name="region_pay", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
            # Derived source crossing via single-dot form.
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
            # Local derived (negative).
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
            # Model-default fragment CROSSES a join — the AggregationParam
            # sql itself is the crossing input.
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
    # DEV-1838 D5 — the widened crossing-input trigger now desugars onto
    # HOST-rooted PLAIN-kernel producers (ranked/windowed roots desugar on
    # their own trigger and are excluded here).
    return planned, [
        a for a in planned.regroup_attach_plans
        if a.attach_phase == "combined"
        and a.producer_root_model is None
        and a.kernel.kind == "plain"
    ]


def _assert_single_ranked_host(planned):
    """A first/last isolates through the RANKED route (DEV-1748), now DESUGARED
    into a regroup producer (DEV-1835).

    Its crossing input still triggers isolation — the widened Law-3 verdict is
    unchanged — but the kind is ranked, because needing its own ROW ORDERING is
    the stronger reason and subsumes the crossing one. The ranked plan now lives
    inside the single regroup-attach producer; top-level ranked/cross-model
    plans are empty. Asserted flatly: the producer carries no nested cross-model
    plan, so there is nothing to recurse into. Returns the ranked plan.
    """
    assert len(planned.regroup_attach_plans) == 1, (
        f"Expected exactly one regroup-attach producer; "
        f"got {planned.regroup_attach_plans}"
    )
    attach = planned.regroup_attach_plans[0]
    # DEV-1838 D4: the ranking rides on the attach kernel, host-rooted.
    assert attach.kernel.kind == "ranked", attach.kernel
    assert attach.producer_root_model is None, attach.producer_root_model
    assert attach.producer_plan.regroup_attach_plans == [], (
        f"the ranked producer must hold its measure inline; "
        f"got nested attaches: {attach.producer_plan.regroup_attach_plans}"
    )
    return attach


def _assert_single_host_rooted(plans):
    """DEV-1838 D5 — one HOST-rooted producer attach, its producer free of
    nested isolation (the recursion-suppression invariant, one form over)."""
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


def test_aggregate_input_paths_module_exists():
    """Canary: ``tests/test_aggregate_input_paths.py`` now hard-imports the
    DEV-1709 helper at module top, so its absence surfaces as a loud collection
    error there. This keeps that absence explicit here too. Codex test-review
    finding #1."""

    importlib.import_module("slayer.engine.aggregate_input_paths")


class TestCrossingFirstLastStaysRanked:
    """DEV-1709 x DEV-1748 — a first/last isolates via the RANKED route
    whatever its inputs do: needing its own ROW ORDERING is the stronger reason
    and subsumes the crossing one, so the classifier routes it to a ranked CTE
    BEFORE the crossing-input branch runs. These pin that INTERACTION — a
    crossing first/last produces ONE flat ranked host plan, not a second
    cross-model ``_cm_`` plan — which is not redundant with the ranked-render
    coverage.

    The crossed hop is NOT visible on a host-rooted ranked plan (join_chain is
    always ``[]``, target_path ``()``); only the ranking TIME arg surfaces it,
    asserted below for the structural case. The crossings buried in a derived
    ``Column.sql`` (the source and derived-time-arg cases) are pinned at the
    RENDER level by the DEV-1748 first/last matrix and ranked-CTE tests
    (``test_dev1748_first_last_matrix.py`` / ``test_dev1748_ranked_plan.py``)."""

    def test_derived_source_first_last_stays_ranked(self):
        planned, _ = _s5_plans("region_pay:last(orders.created_at)")
        _assert_single_ranked_host(planned)

    def test_structural_time_arg_stays_ranked(self):
        planned, _ = _s5_plans("amount:last(customers.signup_at)")
        attach = _assert_single_ranked_host(planned)
        # The one plan-level handle the crossing surfaces on: the ranking key
        # reaches through the customers join. A non-crossing time arg has path
        # ``()``, so this is what makes the case discriminate crossing at all.
        assert attach.kernel.ranking_time_key.path == ("customers",)

    def test_derived_time_arg_stays_ranked(self):
        planned, _ = _s5_plans("amount:last(cross_time)")
        _assert_single_ranked_host(planned)


class TestWidenedLaw3TriggerCrossingInputs:
    """DEV-1709 — a LOCAL aggregate isolates host-rooted when ANY input
    crosses a join: source ``Column.sql`` (D1), positional args incl. the
    explicit time arg (D2), kwargs (typed refs, user template fragments,
    and model-default ``AggregationParam`` fragments). ``Column.filter``
    crossing is the pre-existing DEV-1503 half, pinned above. The first/last
    manifestations of these triggers route to the ranked CTE instead — see
    ``TestCrossingFirstLastStaysRanked``."""

    # -- source Column.sql crossing (D1) ---------------------------------

    def test_derived_source_path_alias_triggers(self):
        _, plans = _s5_plans("region_pay:sum")
        _assert_single_host_rooted(plans)

    def test_derived_source_single_dot_triggers(self):
        _, plans = _s5_plans("cust_weight_d:sum")
        _assert_single_host_rooted(plans)

    def test_sibling_derived_chain_triggers(self):
        # doubled_pop -> pop_helper -> customers__regions: the scan must
        # expand sibling derived refs before looking for crossed paths.
        _, plans = _s5_plans("doubled_pop:sum")
        _assert_single_host_rooted(plans)

    # -- kwargs ----------------------------------------------------------

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
        # No user kwarg at all — the crossing input is the model-default
        # AggregationParam sql fragment (`w` -> customers__regions.weight).
        _, plans = _s5_plans("amount:wscaled_sum")
        _assert_single_host_rooted(plans)

    # -- composite lowering (F3) -----------------------------------------

    def test_composite_crossing_leaf_isolates_individually(self):
        # `region_pay:sum + amount:sum`: the crossing LEAF isolates (one
        # hidden host-rooted plan); the local leaf stays in the base — so
        # exactly ONE plan, not two, and not a plan for the composite slot.
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

    # -- negatives -------------------------------------------------------

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
        # Parity with the Mode-A Column.filter scan: an unparseable fragment
        # contributes no paths (defensive fallback, NOT an endorsement) —
        # pre-Stage-5 behavior is preserved for fragments the dialect
        # fallback chain cannot parse. See DEV-1709 Codex plan-review F1.
        _, plans = _s5_plans("amount:scaled_sum(scale='%% !! ((')")
        assert plans == []

    def test_crossing_dimension_only_does_not_trigger(self):
        # ROW-phase crossing (a joined dimension) is Law-1 territory —
        # it base-pulls; only AGGREGATE inputs trigger Law-3 isolation.
        _, plans = _s5_plans("amount:sum", dimensions=["customers.region_id"])
        assert plans == []

    def test_local_time_arg_does_not_trigger(self):
        _, plans = _s5_plans("amount:last(orders.created_at)")
        assert plans == []

    # -- interactions ----------------------------------------------------

    def test_source_path_still_target_rooted(self):
        # A genuine cross-model aggregate keeps target-rooted semantics —
        # the widened host-rooted branch must not reinterpret it. DEV-1836: it
        # desugars into a producer rooted at the target ``customers``.
        planned, cma = _s5_plans("customers.weight:sum")
        assert cma == []
        assert len(planned.regroup_attach_plans) == 1
        assert planned.regroup_attach_plans[0].producer_root_model == "customers"

    def test_disable_flag_suppresses_widened_trigger(self):
        _, plans = _s5_plans(
            "region_pay:sum", disable_host_rooted_isolation=True,
        )
        assert plans == [], (
            "disable_host_rooted_isolation=True must suppress the widened "
            "crossing-input trigger exactly like the DEV-1503 filter trigger"
        )

    def test_disable_flag_keeps_target_rooted(self):
        # Codex test-review #4: the flag suppresses ONLY host-rooted
        # isolation — a genuine cross-model aggregate (source.path
        # non-empty) must still plan target-rooted under the flag.
        planned, _ = _s5_plans(
            "customers.weight:sum", disable_host_rooted_isolation=True,
        )
        # DEV-1838 (2.5): under the flag, cross-model still isolates — as a
        # TARGET-rooted producer (its own measure is local; no recursion).
        (attach,) = [
            a for a in planned.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert attach.producer_root_model == "customers"

    def test_identical_key_shared_between_public_and_composite_leaf(self):
        # Codex test-review #6: IDENTICAL AggregateKeys intern to one slot
        # → one plan, even when one use is a public measure and the other
        # a composite leaf. (Distinct keys → distinct plans is pinned by
        # test_two_distinct_crossing_measures_two_plans.)
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
        # Per-AggregateKey-slot isolation (interview decision): two DISTINCT
        # crossing aggregates → two separate host-rooted plans (no CTE
        # merging machinery in Stage 5).
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
        """ROW-phase BOUND filter entries of a sub-plan. User query-filters
        propagate as typed bound expressions (``FilterPhase.text`` is None
        for them — only Mode-A model filters keep text)."""
        return [
            fp for fp in sub.filters_by_phase
            if fp.expression is not None and fp.phase == Phase.ROW
        ]

    def test_host_row_filter_propagates_into_widened_sub_plan(self):
        # F4: a host ROW filter constrains the host-rooted scope — it must
        # propagate into the sub-plan's filters (same rule as DEV-1503).
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
            f"got sub-plan filters {sub.filters_by_phase!r}"
        )

    def test_pathed_host_row_filter_propagates_into_widened_sub_plan(self):
        # F4 (Codex plan-review F5): a host-rooted ROW filter whose
        # EXPRESSION itself crosses a join still roots at the host, so it
        # too propagates into the host-rooted sub-plan (which registers the
        # join inside the CTE scope). The rendered-SQL counterpart is
        # test_sql_generator.py::TestDev1709WidenedIsolationShapes::
        # test_pathed_host_row_filter_inherited_into_cte.
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
            f"sub-plan; got sub-plan filters {sub.filters_by_phase!r}"
        )
