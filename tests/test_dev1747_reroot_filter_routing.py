"""DEV-1747 B6 — no silent filter drops in rerooting.

The defect, reproducible today on this corpus: a cross-model aggregate whose
CTE is RE-ROOTED at its target ends up with EMPTY routing lists and ZERO
warnings, whatever the filter is::

    reachable   customers.regions.name == 'Alpha'  -> where [] having [] dropped []
    host-local  status == 'A'                      -> where [] having [] dropped []
    unreachable order_tags.name == 'rush'          -> where [] having [] dropped []

Two mechanisms conspire. ``_maybe_reroot_cross_model_plan`` decides which
filters ride into the re-rooted CTE by TRY-BINDING each one against the target
scope and swallowing ``_REROOT_BIND_ERRORS`` — a tuple that includes bare
``ValueError``, so a planner bug is indistinguishable from "not reachable". It
then CLEARS ``dropped_filter_warnings`` / ``where_filter_ids`` /
``having_filter_ids`` / ``applied_filter_ids``, throwing away the routing the
decision table had already produced.

B6 (with D6/D7) fixes both: decide reroot-vs-forward FIRST, then run
``classify_host_filter`` exactly once per host filter in the coordinate system
of the CTE that will actually exist, and keep the result. Unreachable filters
warn per §5.5. Binder/planner failures RAISE — they never masquerade as
expected drops.

D7 scopes this to FILTERS. An unreachable rerooted DIMENSION still drops (the
documented reroot contract), but structurally — not via a swallowed exception.

Refs: DEV-1747 (B6, D6, D7), DEV-1742 §5.4 / §5.5.
"""
from __future__ import annotations

import os
import tempfile
import warnings

import pytest

from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.stage_planner import plan_query
from tests._dev1747_fixtures import (
    dev1747_bundle,
    make_sqlite_engine,
    seed_dev1747_sqlite,
)

#: A cross-model aggregate PLUS a dimension one hop PAST the target, which is
#: what makes the planner re-root the CTE at ``customers`` instead of using the
#: forward-path shape.
_CROSS_MODEL_MEASURE = {"formula": "customers.spend:sum", "name": "cs"}

#: Reachable from the re-rooted target (``customers -> regions``).
FILTER_REACHABLE = "customers.regions.name == 'Alpha'"
#: Purely host-local — filters host rows, stays at the host base, never warns.
FILTER_HOST_LOCAL = "status == 'A'"
#: Off the target's graph entirely (``orders -> order_tags``) — unreachable
#: from a CTE rooted at ``customers``.
FILTER_UNREACHABLE = "order_tags.name == 'rush'"

#: A HOST-ROOTED shape (``cte_root_model == "orders"``): ordering by a derived
#: column whose ``Column.sql`` crosses. The DEV-1503/DEV-1709 helpers live only
#: on this route, so a sentinel aimed at them has to plan THIS query — a
#: target-rooted one leaves them untouched and asserts nothing.
_HOST_ROOTED_QUERY = SlayerQuery(
    source_model="orders",
    dimensions=[ColumnRef(name="status")],
    measures=[{"formula": "amount:sum", "name": "rev"}],
    order=[OrderItem(column=ColumnRef(name="cust_region"), direction="asc")],
)


def _query(*filters: str) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="name", model="customers.regions")],
        measures=[{"formula": "amount:sum", "name": "rev"}, _CROSS_MODEL_MEASURE],
        filters=list(filters) or None,
    )


def _plans(*filters: str):
    plan = plan_query(query=_query(*filters), bundle=dev1747_bundle())
    assert plan.cross_model_aggregate_plans, "no cross-model plan was produced"
    return plan.cross_model_aggregate_plans


def _sole_plan(*filters: str):
    plans = _plans(*filters)
    assert len(plans) == 1, f"expected one cross-model plan, got {len(plans)}"
    return plans[0]


# ---------------------------------------------------------------------------
# Group 1 — routing survives the reroot (D6)
# ---------------------------------------------------------------------------
class TestRoutingSurvivesReroot:
    def test_the_plan_is_actually_rerooted(self) -> None:
        """Guard for the rest of the module: if the shape stops re-rooting,
        every assertion below becomes vacuous."""
        assert _sole_plan(FILTER_REACHABLE).rerooted_plan is not None

    def test_reachable_filter_is_routed_not_blanked(self) -> None:
        plan = _sole_plan(FILTER_REACHABLE)
        assert plan.applied_filter_ids, (
            "the re-rooted CTE applies this filter, so the plan must SAY so — "
            "today the routing lists are cleared wholesale"
        )
        # ...and the SUB-PLAN is where it is applied. ``where_filter_ids`` is
        # not an audit, it is an instruction to the host base to SKIP the
        # filter because the FORWARD CTE took it over. A re-rooted plan has no
        # forward CTE, and the predicate is host-evaluable by construction, so
        # the host must keep applying it — otherwise rows the user excluded
        # come back carrying a NULL measure.
        assert not plan.where_filter_ids and not plan.having_filter_ids, (
            "a re-rooted plan told the host base to skip a filter that only "
            "the CTE applies"
        )
        # The audit has to be backed by something: the sub-plan must actually
        # carry the filter it claims is applied, or "applied" is a label on
        # nothing.
        assert plan.rerooted_plan.filters_by_phase, (
            f"the audit claims {sorted(plan.applied_filter_ids)} applied, but "
            f"the re-rooted sub-plan carries no filters at all"
        )

    def test_host_local_filter_is_neither_propagated_nor_warned(self) -> None:
        """``DROP_HOST_LOCAL``: the host base applies it and the join-back
        propagates the cardinality reduction, so pushing it into the CTE would
        risk binding a bare name to a same-named TARGET column."""
        plan = _sole_plan(FILTER_HOST_LOCAL)
        assert not plan.where_filter_ids
        assert not plan.having_filter_ids
        assert not plan.dropped_filter_warnings

    def test_routing_lists_are_not_cleared_wholesale(self) -> None:
        """The clear-and-redecide block sets all four lists to ``[]`` at once.
        With a reachable filter present, that is observably wrong."""
        plan = _sole_plan(FILTER_REACHABLE, FILTER_HOST_LOCAL)
        assert plan.applied_filter_ids, (
            "all routing was cleared even though a reachable filter exists"
        )

    def test_mixed_filters_route_independently(self) -> None:
        plan = _sole_plan(FILTER_REACHABLE, FILTER_HOST_LOCAL, FILTER_UNREACHABLE)
        assert plan.applied_filter_ids, "the reachable filter was not applied"
        assert plan.dropped_filter_warnings, "the unreachable filter did not warn"


# ---------------------------------------------------------------------------
# Group 1b — classified EXACTLY once, in the CTE's coordinate system (D6)
# ---------------------------------------------------------------------------
def _classifier_spy(monkeypatch) -> list:
    """Record every ``classify_host_filter`` call and its arguments.

    Patched as BOUND in ``cross_model_planner`` — the module both defines and
    calls it, and a future move of the call site into another module would make
    this spy silently record nothing, which the vacuity assertions below catch.
    """
    from slayer.engine import cross_model_planner

    calls: list = []
    original = cross_model_planner.classify_host_filter

    def _recording(**kwargs):
        route = original(**kwargs)
        calls.append({**kwargs, "route": route})
        return route

    monkeypatch.setattr(cross_model_planner, "classify_host_filter", _recording)
    return calls


class TestClassifiedExactlyOnce:
    def test_each_host_filter_is_classified_once_per_cte(self, monkeypatch) -> None:
        """D6's actual claim. Warning DEDUP at the engine boundary hides a
        double classification, so counting warnings cannot establish this — the
        count has to come from the classifier itself.

        One cross-model aggregate ⇒ one CTE ⇒ exactly one classification per
        host filter.
        """
        calls = _classifier_spy(monkeypatch)
        _sole_plan(FILTER_REACHABLE, FILTER_HOST_LOCAL, FILTER_UNREACHABLE)
        assert calls, "classify_host_filter was never called — spy is vacuous"
        per_filter: dict = {}
        for call in calls:
            fid = call["host_filter"].filter_id
            per_filter[fid] = per_filter.get(fid, 0) + 1
        assert set(per_filter.values()) == {1}, (
            f"host filters were classified more than once: {per_filter} — the "
            f"clear-and-redecide block re-runs the decision (D6)"
        )

    def test_the_classifier_sees_the_cte_actual_root(self, monkeypatch) -> None:
        """The coordinate-system half of D6. The re-rooted CTE is rooted at
        ``customers``, so the classifier must be asked about ``target_path ==
        ("customers",)``. Classifying against the FORWARD path and then
        re-rooting the CTE is how a reachable filter ends up judged unreachable
        (and vice versa)."""
        calls = _classifier_spy(monkeypatch)
        plan = _sole_plan(FILTER_REACHABLE)
        assert plan.rerooted_plan is not None, "shape stopped re-rooting"
        assert calls, "classify_host_filter was never called — spy is vacuous"
        target_paths = {call["target_path"] for call in calls}
        assert target_paths == {("customers",)}, (
            f"the classifier was asked about {target_paths}, not the CTE's "
            f"actual root ('customers',)"
        )

    def test_the_classifier_receives_the_structural_summary(
        self, monkeypatch,
    ) -> None:
        """§5.3's structural crossing metadata is the input the decision is
        made from. A classifier called with an EMPTY summary would judge every
        filter host-local and drop nothing — passing the warning tests below by
        accident."""
        calls = _classifier_spy(monkeypatch)
        _sole_plan(FILTER_UNREACHABLE)
        crossed = {
            path
            for call in calls
            for path in call["host_filter"].crossed_join_paths
        }
        assert ("order_tags",) in crossed, (
            f"the unreachable filter reached the classifier without its "
            f"crossed-path summary; saw {crossed}"
        )


# ---------------------------------------------------------------------------
# Group 2 — unreachable warns instead of vanishing (B6 / §5.5)
# ---------------------------------------------------------------------------
class TestUnreachableWarns:
    def test_unreachable_filter_produces_a_warning_on_the_plan(self) -> None:
        plan = _sole_plan(FILTER_UNREACHABLE)
        assert plan.dropped_filter_warnings, (
            "an unreachable filter was dropped from the re-rooted CTE with no "
            "warning — the B6 defect"
        )

    def test_warning_carries_the_original_filter_text(self) -> None:
        """§5.5 payload fidelity — a warning that does not name the user's own
        filter text cannot be acted on."""
        warning = _sole_plan(FILTER_UNREACHABLE).dropped_filter_warnings[0]
        assert FILTER_UNREACHABLE in warning.filter_text

    def test_warning_carries_a_reason(self) -> None:
        warning = _sole_plan(FILTER_UNREACHABLE).dropped_filter_warnings[0]
        assert warning.reason and "reach" in warning.reason.lower()

    async def test_exactly_one_warning_per_filter_per_execute(self) -> None:
        """The boundary dedups per filter identity. Two cross-model measures
        classify the same filter twice; the user must still see it once."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="name", model="customers.regions")],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
                {"formula": "customers.regions.population:sum", "name": "pop"},
            ],
            filters=[FILTER_UNREACHABLE],
        )
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute(query)
        dropped = [
            w for w in caught
            if isinstance(w.message, UnreachableFilterDroppedWarning)
        ]
        assert len(dropped) == 1, (
            f"expected exactly one UnreachableFilterDroppedWarning, got "
            f"{len(dropped)}: {[str(w.message) for w in dropped]}"
        )

    async def test_warnings_as_errors_mode_surfaces_the_drop(self) -> None:
        """Someone running with ``-W error`` must be stopped, not silently
        given fewer rows than they asked for."""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            with warnings.catch_warnings():
                warnings.simplefilter("error", UnreachableFilterDroppedWarning)
                with pytest.raises(UnreachableFilterDroppedWarning):
                    await engine.execute(_query(FILTER_UNREACHABLE))

    async def test_two_textually_distinct_filters_warn_separately(self) -> None:
        """Identity is per FILTER, not per text-dedup bucket — two different
        unreachable filters must not collapse into one warning."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="name", model="customers.regions")],
            measures=[{"formula": "amount:sum", "name": "rev"}, _CROSS_MODEL_MEASURE],
            filters=["order_tags.name == 'rush'", "order_tags.name == 'gift'"],
        )
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute(query)
        dropped = [
            w for w in caught
            if isinstance(w.message, UnreachableFilterDroppedWarning)
        ]
        assert len(dropped) == 2


# ---------------------------------------------------------------------------
# Group 3 — internal failures RAISE (§5.5)
# ---------------------------------------------------------------------------
class TestInternalFailuresRaise:
    def test_the_swallow_and_drop_path_is_never_taken(self, monkeypatch) -> None:
        """``_REROOT_BIND_ERRORS`` includes bare ``ValueError``, so any planner
        bug inside the reroot path currently reads as "filter unreachable".

        A RUNTIME sentinel rather than a source scan: emptying the tuple makes
        every ``except _REROOT_BIND_ERRORS`` catch nothing, so if production
        still swallows there, the swallowed exception escapes here. A grep for
        the symbol would instead pass the moment someone renamed it.
        """
        from slayer.engine import cross_model_planner

        monkeypatch.setattr(cross_model_planner, "_REROOT_BIND_ERRORS", ())
        plan = _sole_plan(FILTER_REACHABLE, FILTER_HOST_LOCAL, FILTER_UNREACHABLE)
        assert plan.rerooted_plan is not None
        assert plan.applied_filter_ids
        assert plan.dropped_filter_warnings

    def test_the_text_filter_classifier_is_never_called(self, monkeypatch) -> None:
        """P-J state 1: ``_classify_subplan_filters`` — which re-derives sub-plan
        filters from ``routing.text`` — stays in the file but must lose every
        production caller. D6 routes from the typed classification instead.

        Exercised on the HOST-ROOTED route: the helper is called only from
        ``_plan_filtered_local``, so a target-rooted query would leave this
        sentinel untripped and the test would assert nothing.
        """
        from slayer.engine import cross_model_planner

        assert hasattr(cross_model_planner, "_classify_subplan_filters"), (
            "the helper was deleted; P-J defers deletion to PR 6"
        )

        def _boom(*_a, **_kw):
            raise AssertionError(
                "_classify_subplan_filters is still on the production path — "
                "D6 classifies once, structurally, against the CTE's own root"
            )

        monkeypatch.setattr(
            cross_model_planner, "_classify_subplan_filters", _boom,
        )
        plan = plan_query(
            query=_HOST_ROOTED_QUERY, bundle=dev1747_bundle(),
        )
        assert plan.cross_model_aggregate_plans, "no host-rooted CTE was planned"
        assert plan.cross_model_aggregate_plans[0].cte_root_model == "orders", (
            "the shape stopped being host-rooted — the sentinel would be "
            "untripped for the wrong reason"
        )

    def test_no_bare_except_in_the_reroot_path(self) -> None:
        """Scoped to the reroot functions rather than the whole module, so an
        unrelated ``except Exception`` elsewhere in the file cannot fail this
        (or, worse, be deleted to make it pass)."""
        import inspect

        from slayer.engine import cross_model_planner

        for name in (
            "_maybe_reroot_cross_model_plan",
            "_plan_filtered_local",
            "_route_host_filters",
        ):
            target = getattr(cross_model_planner, name, None) or getattr(
                cross_model_planner.IsolatedCteCrossModelPlanner, name, None,
            )
            if target is None:
                continue
            source = inspect.getsource(target)
            assert "except Exception" not in source, (
                f"{name} swallows all exceptions — that re-creates the B6 defect"
            )

    def test_planner_failure_propagates_rather_than_warning(self, monkeypatch) -> None:
        """A genuine internal error must not be reported as an expected drop."""
        from slayer.engine import cross_model_planner

        boom = RuntimeError("planner exploded")

        def _explode(**_kwargs):
            raise boom

        monkeypatch.setattr(
            cross_model_planner, "classify_host_filter", _explode, raising=True,
        )
        with pytest.raises(RuntimeError, match="planner exploded"):
            _plans(FILTER_REACHABLE)


# ---------------------------------------------------------------------------
# Group 4 — D7: dims still drop, but structurally
# ---------------------------------------------------------------------------
class TestUnreachableDimensionsStillDrop:
    def test_unreachable_dimension_is_dropped_without_a_warning(self) -> None:
        """D7 keeps the documented reroot contract for dims — the change is
        that the decision is structural, not a swallowed bind failure."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[
                ColumnRef(name="name", model="customers.regions"),
                ColumnRef(name="name", model="order_tags"),
            ],
            measures=[{"formula": "amount:sum", "name": "rev"}, _CROSS_MODEL_MEASURE],
        )
        plan = plan_query(query=query, bundle=dev1747_bundle())
        cma = plan.cross_model_aggregate_plans[0]
        assert cma.rerooted_plan is not None
        assert not cma.dropped_filter_warnings, (
            "a dropped DIMENSION must not raise a dropped-FILTER warning (D7)"
        )

    def test_reachable_dimensions_still_form_the_grain(self) -> None:
        plan = _sole_plan()
        assert plan.rerooted_grain_pairs, (
            "the re-rooted CTE lost its grain — the join-back would broadcast"
        )


# ---------------------------------------------------------------------------
# Group 5 — D2: the host-rooted dispatch accepts a path-bearing key
# ---------------------------------------------------------------------------
class TestFilteredLocalDispatchAccounting:
    """``_dispatch_filtered_local`` is reached only when ``source.path`` is
    EMPTY (``if not path:`` in ``IsolatedCteCrossModelPlanner.plan``), and it
    raises "this is a plain local aggregate" unless it finds a crossing input.

    A ``grain="host"`` wrap has a NON-empty path and no crossing *input* — its
    crossing IS the path. Both halves therefore have to change together, and
    each fails in a different way: the first sends the wrap to a target-rooted
    CTE (silent scalar CROSS JOIN), the second raises. Neither is visible in a
    test that only checks the emitted SQL has an ORDER BY.
    """

    def _grouped_joined_order(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "amount:sum", "name": "rev"}],
            order=[OrderItem(
                column=ColumnRef(name="name", model="customers.regions"),
                direction="asc",
            )],
        )

    def _dispatch_spy(self, monkeypatch) -> list:
        from slayer.engine.cross_model_planner import IsolatedCteCrossModelPlanner

        calls: list = []
        original = IsolatedCteCrossModelPlanner._dispatch_filtered_local

        def _recording(self, **kwargs):
            calls.append(kwargs["aggregate_key"])
            return original(self, **kwargs)

        monkeypatch.setattr(
            IsolatedCteCrossModelPlanner, "_dispatch_filtered_local", _recording,
        )
        return calls

    def test_host_grain_wrap_dispatches_to_the_host_rooted_route(
        self, monkeypatch,
    ) -> None:
        calls = self._dispatch_spy(monkeypatch)
        plan_query(query=self._grouped_joined_order(), bundle=dev1747_bundle())
        assert calls, (
            "the host-grain wrap never reached _dispatch_filtered_local — it "
            "was routed to a TARGET-rooted CTE, which degenerates to a scalar "
            "CROSS JOIN (D2)"
        )
        assert calls[0].grain == "host"
        assert calls[0].source.path == ("customers", "regions"), (
            "the wrap lost its path on the way to the host-rooted route"
        )

    def test_a_target_grain_aggregate_does_not_take_that_route(
        self, monkeypatch,
    ) -> None:
        """The contrast: a genuine cross-model measure must keep going to the
        target-rooted CTE. Widening the dispatch to every path-bearing key
        would move it, and its value would silently become per-host-group."""
        calls = self._dispatch_spy(monkeypatch)
        plan_query(
            query=SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    {"formula": "amount:sum", "name": "rev"},
                    _CROSS_MODEL_MEASURE,
                ],
            ),
            bundle=dev1747_bundle(),
        )
        assert not calls, (
            f"a target-grain aggregate was routed host-rooted: {calls}"
        )

    def test_the_path_counts_as_the_crossing_input(self) -> None:
        """The accounting change itself. Without it the dispatch raises
        ``ValueError('… this is a plain local aggregate')`` — a path-bearing
        key has no ``column_filter_key`` and no crossing arg to find."""
        plan = plan_query(
            query=self._grouped_joined_order(), bundle=dev1747_bundle(),
        )
        assert plan.cross_model_aggregate_plans, "no host-rooted CTE was planned"
        cma = plan.cross_model_aggregate_plans[0]
        assert cma.cte_root_model == "orders", (
            f"the wrap's CTE is rooted at {cma.cte_root_model!r}, not the host"
        )
