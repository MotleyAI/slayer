"""DEV-1746 stage 6 — one isolation decision, and the DEV-1688 seam.

Whether an aggregate gets its own CTE, and where that CTE is rooted, was decided
by three predicates inlined in the planner's aggregate loop. They ran in a fixed
order and each knew about the others by omission: the windowed skip existed
because a windowed measure would otherwise trip the crossing-input trigger, and
the crossing-input trigger excluded path-bearing sources because the
target-rooted branch had already claimed them. Reading one meant reading all
three, and a future cardinality-aware change would have had to land in each.

They are one classifier now. This module pins two things:

* **Every isolation kind is reachable and correctly classified** — including the
  two that exist only because of the omissions above (a windowed measure whose
  ``Column.filter`` crosses a join, which must stay WINDOWED rather than
  becoming host-rooted; and a sub-plan, where host-rooted isolation is
  suppressed to stop the recursion).
* **The decision did not change.** This is a refactor, so the classifier's
  verdict must agree with what the planner actually built — asserted by
  comparing the classification against the plans, not by restating the
  predicate.

``may_inline_crossing_inputs`` is the DEV-1688 seam: hardcoded ``False`` so
every crossing aggregate isolates, which is today's behaviour. It is pinned both
ways — the constant, and that flipping it actually changes the verdict, so the
seam is load-bearing rather than decorative.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.keys import AggregateKey, ColumnKey, SqlExprKey
from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine import isolation as isolation_mod
from slayer.engine.isolation import (
    IsolationKind,
    classify_isolation,
    may_inline_crossing_inputs,
)
from slayer.engine.planned import PlannedQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._cross_model_chain import (
    _countries,
    _customers_v2,
    _orders_x,
    _regions,
)


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_x(),
        referenced_models=[_customers_v2(), _regions(), _countries()],
    )


def _crossing_bundle() -> ResolvedSourceBundle:
    """A bundle whose host ``orders_x`` carries a LOCAL ``eu_amount`` measure
    whose ``Column.filter`` crosses to ``customers_v2`` — the crossing-input
    shape the host-rooted and seam tests share."""
    crossing = _orders_x().columns[0].model_copy(update={
        "name": "eu_amount", "sql": "amount",
        "filter": "customers_v2.status = 'eu'", "primary_key": False,
    })
    return ResolvedSourceBundle(
        source_model=_orders_x(extra_columns=[crossing]),
        referenced_models=[_customers_v2(), _regions(), _countries()],
    )


def _classify_all(query: SlayerQuery) -> "tuple[dict, PlannedQuery]":
    """Every aggregate slot's isolation kind keyed by slot id, and the plan."""
    bundle = _bundle()
    planned = plan_query(query=query, bundle=bundle)
    windowed_ids = {
        p.aggregate_slot_id for p in planned.windowed_aggregate_plans
    }
    return {
        slot.id: classify_isolation(
            slot=slot, windowed_slot_ids=windowed_ids, bundle=bundle,
        )
        for slot in planned.aggregate_slots
    }, planned


_MONTH = [TimeDimension(
    dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
)]


# =========================================================================== #
# Each kind is reachable and correctly named.
# =========================================================================== #
class TestEveryIsolationKindIsClassified:

    def test_a_purely_local_aggregate_is_not_isolated(self) -> None:
        kinds, planned = _classify_all(SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
        ))
        assert set(kinds.values()) == {IsolationKind.NONE}, kinds
        assert not planned.cross_model_aggregate_plans
        assert not planned.windowed_aggregate_plans

    def test_a_target_rooted_aggregate_is_classified(self) -> None:
        kinds, planned = _classify_all(SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        assert IsolationKind.TARGET_ROOTED in kinds.values(), kinds
        assert len(planned.cross_model_aggregate_plans) == 1

    def test_a_windowed_aggregate_is_desugared_into_a_regroup_producer(
        self,
    ) -> None:
        """MIGRATED (DEV-1835): a LOCAL windowed measure is DESUGARED into a
        regroup producer BEFORE isolation classification, so it no longer
        reaches ``classify_isolation`` as WINDOWED. The windowed work moves into
        a ``regroup_attach_plans`` producer (rendered as a ``_cm_`` CTE) whose
        nested plan carries the windowed plan; the top-level plan has no
        windowed plan and no cross-model plan for it."""
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                time_dimensions=list(_MONTH),
                measures=[ModelMeasure(
                    formula="amount:sum(window='90d')", name="w",
                )],
            ),
            bundle=_bundle(),
        )
        assert not planned.windowed_aggregate_plans
        assert not planned.cross_model_aggregate_plans
        assert len(planned.regroup_attach_plans) == 1, planned.regroup_attach_plans
        producer = planned.regroup_attach_plans[0].producer_plan
        assert producer.windowed_aggregate_plans, (
            "the desugared producer must carry the windowed plan"
        )

    def test_a_local_aggregate_with_a_crossing_filter_is_host_rooted(
        self,
    ) -> None:
        """The trigger that exists so a LOCAL aggregate whose ``Column.filter``
        reaches another model still gets its own rows."""
        bundle = _crossing_bundle()
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="eu_amount:sum", name="eu")],
            ),
            bundle=bundle,
        )
        kinds = {
            slot.id: classify_isolation(
                slot=slot, windowed_slot_ids=set(), bundle=bundle,
            )
            for slot in planned.aggregate_slots
        }
        assert IsolationKind.HOST_ROOTED in kinds.values(), kinds
        assert planned.cross_model_aggregate_plans, (
            "a crossing Column.filter must produce an isolated CTE"
        )


# =========================================================================== #
# The two cases that only exist because the old predicates knew about each other.
# =========================================================================== #
class TestOrderingBetweenTheOldPredicates:

    def test_a_windowed_measure_with_a_crossing_filter_stays_windowed(
        self,
    ) -> None:
        """MIGRATED (DEV-1835): the windowed skip still comes first. A crossing
        ``Column.filter`` windowed measure is DESUGARED into a regroup producer,
        and inside that producer's nested plan it renders as a windowed plan —
        NOT additionally isolated into a cross-model ``_cm_`` plan. The 'isolated
        twice' hazard the original predicate ordering guarded against stays
        guarded, one level down."""
        bundle = _crossing_bundle()
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders_x",
                time_dimensions=list(_MONTH),
                measures=[ModelMeasure(
                    formula="eu_amount:sum(window='90d')", name="w",
                )],
            ),
            bundle=bundle,
        )
        assert not planned.windowed_aggregate_plans
        assert not planned.cross_model_aggregate_plans
        assert len(planned.regroup_attach_plans) == 1, planned.regroup_attach_plans
        producer = planned.regroup_attach_plans[0].producer_plan
        assert producer.windowed_aggregate_plans, (
            "the crossing-filter windowed measure must render as a windowed plan "
            "inside the producer"
        )
        assert not producer.cross_model_aggregate_plans, (
            "the windowed measure was ALSO isolated into a _cm_ cross-model "
            "plan — the windowed skip no longer comes first"
        )

    def test_suppressing_host_rooted_isolation_yields_none(self) -> None:
        """Inside a sub-plan the crossing input renders inline — legal there,
        because the CTE is the aggregate's own scope, and required because the
        sub-plan holds the same measure and would otherwise recurse."""
        bundle = _crossing_bundle()
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="eu_amount:sum", name="eu")],
            ),
            bundle=bundle,
        )
        for slot in planned.aggregate_slots:
            assert classify_isolation(
                slot=slot,
                windowed_slot_ids=set(),
                bundle=bundle,
                disable_host_rooted_isolation=True,
            ) is IsolationKind.NONE

    def test_a_target_rooted_source_is_not_reclassified_as_host_rooted(
        self,
    ) -> None:
        """The crossing trigger only applies to a LOCAL aggregate; a source that
        already names another model is target-rooted, whatever else it crosses."""
        kinds, _ = _classify_all(SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        ))
        assert IsolationKind.TARGET_ROOTED in kinds.values(), kinds
        assert IsolationKind.HOST_ROOTED not in kinds.values(), kinds


# =========================================================================== #
# The decision did not change.
# =========================================================================== #
class TestClassificationAgreesWithThePlan:

    @pytest.mark.parametrize(
        "query",
        [
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum")],
            ),
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="customers_v2.lifetime_value:sum"),
                    ModelMeasure(formula="amount:sum"),
                ],
            ),
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                time_dimensions=_MONTH,
                measures=[
                    ModelMeasure(formula="amount:sum(window='90d')", name="w"),
                    ModelMeasure(formula="customers_v2.lifetime_value:sum"),
                    ModelMeasure(formula="amount:avg"),
                ],
            ),
            SlayerQuery(
                source_model="orders_x",
                measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            ),
        ],
        ids=["local", "local+cm", "mixed", "scalar_cm"],
    )
    def test_every_isolated_slot_is_classified_isolated_and_no_other(
        self, query: SlayerQuery,
    ) -> None:
        """The refactor's real assertion: the classifier's verdict matches what
        the planner BUILT, for every aggregate slot in the query."""
        kinds, planned = _classify_all(query)
        cm_ids = {
            p.aggregate_slot_id for p in planned.cross_model_aggregate_plans
        }
        wm_ids = {
            p.aggregate_slot_id for p in planned.windowed_aggregate_plans
        }
        for sid, kind in kinds.items():
            if sid in wm_ids:
                assert kind is IsolationKind.WINDOWED, (sid, kind)
            elif sid in cm_ids:
                assert kind.needs_own_cte, (sid, kind)
                assert kind is not IsolationKind.WINDOWED, (sid, kind)
            else:
                assert kind is IsolationKind.NONE, (
                    f"slot {sid} classified {kind} but the planner built no CTE "
                    f"for it"
                )


# =========================================================================== #
# The DEV-1688 seam.
# =========================================================================== #
class TestMayInlineSeam:

    def test_inlining_a_crossing_input_is_refused(self) -> None:
        """Hardcoded ``False``: a crossing input is isolated, always. Inlining
        one is only safe when the crossed join is provably 1:N-free, which needs
        cardinality metadata SLayer does not carry yet."""
        assert may_inline_crossing_inputs([("customers_v2",)]) is False
        assert may_inline_crossing_inputs([]) is False

    def test_the_seam_is_load_bearing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Flipping the seam must change the verdict.

        Otherwise it is decorative — a hook that reads nothing and decides
        nothing, which is exactly what DEV-1688 must not inherit. With it
        returning ``True`` a crossing-input aggregate stops being isolated,
        which is the behaviour a cardinality-aware version would enable.
        """
        bundle = _crossing_bundle()
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="eu_amount:sum", name="eu")],
            ),
            bundle=bundle,
        )
        slot = next(
            s for s in planned.aggregate_slots
            if classify_isolation(
                slot=s, windowed_slot_ids=set(), bundle=bundle,
            ) is IsolationKind.HOST_ROOTED
        )
        monkeypatch.setattr(
            isolation_mod, "may_inline_crossing_inputs", lambda paths: True,
        )
        assert classify_isolation(
            slot=slot, windowed_slot_ids=set(), bundle=bundle,
        ) is IsolationKind.NONE, (
            "flipping may_inline_crossing_inputs did not change the verdict — "
            "the seam is not consulted"
        )

    def test_the_render_time_seam_is_separate_and_still_false(self) -> None:
        """``ScopeFrame.may_inline`` guards individual values at the projection
        boundary; this module's seam guards whole aggregates at plan time. Both
        are ``False``; they are pinned together so neither is mistaken for the
        other when DEV-1688 lands."""
        from slayer.sql.scope import ScopeFrame

        assert ScopeFrame.may_inline(
            ScopeFrame.__new__(ScopeFrame), [("customers_v2",)],
        ) is False


class TestCrossingInputPathsUnionsFilterAndStructural:
    """DEV-1783 item 6 — ``_crossing_input_paths`` must UNION a local
    aggregate's ``Column.filter`` crossings with its structural input crossings
    (source ``Column.sql`` / args / kwargs). The pre-fix early-return reported
    the filter paths ALONE, hiding a crossing kwarg from the isolation
    decision and letting a fan-out-multiplying aggregate inline."""

    def test_filter_and_kwarg_crossings_are_both_reported(self) -> None:
        key = AggregateKey(
            agg="sum",
            source=ColumnKey(path=(), leaf="amount"),
            kwargs=(("weight", ColumnKey(path=("customers_v2",), leaf="balance")),),
            column_filter_key=SqlExprKey(
                canonical_sql="customers_v2__regions.name = 'X'",
                referenced_join_paths=(("customers_v2", "regions"),),
            ),
        )
        paths = isolation_mod._crossing_input_paths(key=key, bundle=_bundle())
        assert ("customers_v2", "regions") in paths, paths  # column_filter_key
        assert ("customers_v2",) in paths, paths            # kwarg — dropped pre-fix
        # Order-stable + de-duplicated: filter paths first, then structural.
        assert paths == [("customers_v2", "regions"), ("customers_v2",)], paths
