"""DEV-1747 B6 — no silent filter drops in rerooting.

The defect, reproducible today on this corpus: a cross-model aggregate whose
CTE is RE-ROOTED at its target ends up with EMPTY routing lists and ZERO
warnings, whatever the filter is::

    reachable   customers.regions.name == 'Alpha'  -> where [] having [] dropped []
    host-local  status == 'A'                      -> where [] having [] dropped []
    unreachable order_tags.name == 'rush'          -> where [] having [] dropped []

Two mechanisms conspire. ``_maybe_reroot_cross_model_plan`` decides which
filters ride into the re-rooted CTE by TRY-BINDING each one against the target
scope and swallowing a broad error tuple that included bare ``ValueError``, so
a planner bug was indistinguishable from "not reachable". It
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

Coverage note: the common forward abandon (``not needs_reroot``) is exercised
by the target-grain dispatch tests below; the late ``_forward_only()`` fallbacks
(a missing target/host-prebound, routing that applies nothing, or an absent
sub-plan aggregate) guard internal invariants unreachable from a well-formed
query and are intentionally not pinned.

Refs: DEV-1747 (B6, D6, D7), DEV-1742 §5.4 / §5.5.
"""
from __future__ import annotations

import ast
import os
import tempfile
import textwrap
import warnings

import pytest

from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.stage_planner import plan_query
from tests._dev1747_fixtures import (
    ALPHA_SPEND_ALL,
    ALPHA_SPEND_GOLD,
    GROUP_A_AMOUNT,
    REGION_A_HIGH,
    REGION_A_LOW,
    REGION_B_ONLY,
    dev1747_bundle,
    make_sqlite_engine,
    seed_dev1747_sqlite,
)
from slayer.engine import cross_model_planner
from slayer.engine.cross_model_planner import IsolatedCteCrossModelPlanner
import inspect

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
#: Reachable from BOTH scopes and, unlike the others, it changes the aggregate
#: INSIDE a group that survives it: region Alpha keeps its gold customer and
#: loses its silver one, so ``cs`` drops from 1040 to 1000 while the Alpha row
#: stays. That is what distinguishes "the re-rooted CTE applied its own copy"
#: from "the host filtered and the join-back happened to pick the right group".
FILTER_TARGET_ATTRIBUTE = "customers.tier == 'gold'"
#: An AGGREGATE-phase predicate over the isolated aggregate itself. The host
#: base cannot evaluate it — the aggregate does not live in ``_base`` — so
#: unlike a ROW-phase filter it must STAY routed to the CTE.
FILTER_AGGREGATE_REF = "customers.spend:sum > 500"

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
        assert not plan.where_filter_ids, (
            f"a re-rooted plan routed {plan.where_filter_ids} to a forward "
            f"CTE's WHERE that does not exist, so the host base skips it"
        )
        assert not plan.having_filter_ids, (
            f"a re-rooted plan routed {plan.having_filter_ids} to a forward "
            f"CTE's HAVING that does not exist"
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
        assert warning.reason, "the warning carries no reason at all"
        assert "reach" in warning.reason.lower(), warning.reason

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
            query = _query(FILTER_UNREACHABLE)
            with warnings.catch_warnings():
                warnings.simplefilter("error", UnreachableFilterDroppedWarning)
                with pytest.raises(UnreachableFilterDroppedWarning):
                    await engine.execute(query)

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


    def test_no_bare_except_in_the_reroot_path(self) -> None:
        """Scoped to the reroot functions rather than the whole module, so an
        unrelated ``except Exception`` elsewhere in the file cannot fail this
        (or, worse, be deleted to make it pass).

        The B6 defect was a broad tuple ``except (ValueError, …)`` — not a
        literal ``except Exception`` — and a bare ``except:`` hides the same
        thing. All three forms must fail this, and a rename must not skip it."""
        broad = {"Exception", "BaseException", "ValueError"}
        checked = 0
        for name in (
            "_maybe_reroot_cross_model_plan",
            "_plan_filtered_local",
            "_route_host_filters",
        ):
            target = getattr(cross_model_planner, name, None) or getattr(
                cross_model_planner.IsolatedCteCrossModelPlanner, name, None,
            )
            assert target is not None, (
                f"{name} is gone — the reroot path was renamed and this guard "
                f"now checks nothing"
            )
            checked += 1
            tree = ast.parse(textwrap.dedent(inspect.getsource(target)))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ExceptHandler):
                    continue
                assert node.type is not None, (
                    f"{name} has a bare ``except:`` — re-creates the B6 defect"
                )
                caught = (
                    node.type.elts if isinstance(node.type, ast.Tuple)
                    else [node.type]
                )
                names = {n.id for n in caught if isinstance(n, ast.Name)}
                # A broad catch that RE-RAISES doesn't swallow — the B6 defect
                # is the silent drop, not catching per se. Flag only handlers
                # that catch broad and never raise.
                reraises = any(isinstance(n, ast.Raise) for n in ast.walk(node))
                assert not (names & broad) or reraises, (
                    f"{name} SWALLOWS {sorted(names & broad)} without re-raising "
                    f"— a swallowed broad error is exactly the B6 defect"
                )
        assert checked == 3, "not every reroot function was checked"

    def test_planner_failure_propagates_rather_than_warning(self, monkeypatch) -> None:
        """A genuine internal error must not be reported as an expected drop."""

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


# ---------------------------------------------------------------------------
# Group 5 — the ROWS, not the plan (the regression the plan fields hid)
# ---------------------------------------------------------------------------
class TestRerootedFilterStillNarrowsTheHost:
    """The first attempt at B6 stopped blanking the routing lists wholesale.
    That was right about ``applied_filter_ids`` — an AUDIT, "some scope
    evaluates this" — and wrong about ``where_filter_ids`` /
    ``having_filter_ids``, which are an INSTRUCTION: the forward CTE took this
    filter over, so the host base must not apply it.

    A re-rooted plan has no forward CTE (the sub-plan replaces it and carries
    its own re-anchored filters), and the predicate is host-evaluable by
    construction, since it was bound against the host. So the instruction told
    the host base to skip a filter nothing else applied THERE, and rows the
    user excluded came back with a NULL measure attached.

    Every plan-level assertion in this module passed throughout: the plan was
    self-consistent, and the wrongness existed only in the rows. These tests
    execute.
    """

    @staticmethod
    async def _rows_and_warnings(*filters: str):
        """Rows plus every warning the execute emitted.

        Warnings are CAPTURED rather than suppressed: a planner that
        misclassified a reachable predicate as unreachable would drop it from
        the CTE, warn about it, and — because the host still applies it — often
        return correct-looking rows anyway. The warning is the only signal.
        """
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                response = await engine.execute(_query(*filters))
        dropped = [
            w for w in caught
            if isinstance(w.message, UnreachableFilterDroppedWarning)
        ]
        return response.data, dropped

    @classmethod
    async def _rows(cls, *filters: str):
        rows, _ = await cls._rows_and_warnings(*filters)
        return rows

    def test_every_fixture_here_actually_reroots(self) -> None:
        """Vacuity guard for the whole class. Every assertion below is about
        what re-rooting does; if the planner quietly stopped re-rooting and
        compiled an equivalent forward plan, the row assertions would still
        pass and would be testing nothing."""
        for label, flt in (
            ("reachable", FILTER_REACHABLE),
            ("host-local", FILTER_HOST_LOCAL),
            ("unreachable", FILTER_UNREACHABLE),
            ("target-attribute", FILTER_TARGET_ATTRIBUTE),
            ("aggregate-ref", FILTER_AGGREGATE_REF),
        ):
            assert _sole_plan(flt).rerooted_plan is not None, (
                f"the {label} fixture no longer re-roots, so its row "
                f"assertions no longer test re-rooting"
            )

    async def test_a_reachable_filter_narrows_the_host_rows(self) -> None:
        """``customers.regions.name == 'Alpha'`` keeps ONE region group.

        Without the fix the host base is unfiltered, so all four regions
        survive and three of them carry a NULL measure — the user's filter
        silently became "annotate, don't exclude".
        """
        rows = await self._rows(FILTER_REACHABLE)
        regions = [r["orders.customers.regions.name"] for r in rows]
        assert regions == [REGION_A_LOW], (
            f"the re-rooted CTE applied the filter but the host base did not, "
            f"so excluded regions came back: {regions}"
        )

    async def test_the_excluded_regions_do_not_come_back_as_nulls(self) -> None:
        """States the failure mode directly rather than by row count: the
        symptom is specifically a row PRESENT with a NULL measure, which a
        count assertion alone would not distinguish from a genuinely empty
        group the user asked to see."""
        rows = await self._rows(FILTER_REACHABLE)
        leaked = [
            r for r in rows
            if r["orders.customers.regions.name"] in (
                REGION_A_HIGH, REGION_B_ONLY, None,
            )
        ]
        assert not leaked, (
            f"regions the filter excludes are present with a NULL measure: "
            f"{leaked}"
        )

    async def test_the_surviving_group_keeps_its_own_values(self) -> None:
        """The other half: narrowing the host must not disturb the row that
        SHOULD be there. A fix that over-filtered — applying the re-rooted
        predicate at the host in the TARGET's coordinate system — would empty
        the result instead, and the two assertions above would both pass."""
        rows = await self._rows(FILTER_REACHABLE)
        assert len(rows) == 1, rows
        assert rows[0]["orders.rev"] == 11.0, (
            f"the surviving group's own measure changed: {rows[0]}"
        )
        assert rows[0]["orders.cs"] == ALPHA_SPEND_ALL, (
            f"the cross-model measure changed: {rows[0]}"
        )

    async def test_a_host_local_filter_still_narrows_the_host(self) -> None:
        """The control for the branch that was ALREADY right: a host-local
        filter never had routing ids, so it must be unaffected. ``status ==
        'A'`` keeps the two orders of group A, which span two regions."""
        rows = await self._rows(FILTER_HOST_LOCAL)
        regions = sorted(
            str(r["orders.customers.regions.name"]) for r in rows
        )
        assert regions == sorted([REGION_A_LOW, REGION_A_HIGH]), regions
        assert sum(r["orders.rev"] for r in rows) == GROUP_A_AMOUNT, rows

    async def test_an_unreachable_filter_still_narrows_the_host(self) -> None:
        """The filter the re-rooted CTE CANNOT evaluate must still apply at the
        host — that is what the dropped-filter warning promises ("it still
        applies at the host"). If the host skipped it too, the warning would be
        describing a filter that ran nowhere at all."""
        rows = await self._rows(FILTER_UNREACHABLE)
        regions = sorted(
            str(r["orders.customers.regions.name"]) for r in rows
        )
        # order_tags 'rush' tags orders 1 (region Alpha) and 2 (region Zulu).
        assert regions == sorted([REGION_A_LOW, REGION_A_HIGH]), regions

    async def test_the_rerooted_cte_applies_its_own_copy_of_the_filter(
        self,
    ) -> None:
        """The half a filtered-DIMENSION test cannot reach (Codex).

        Grouping by the very column the filter names hides whether the CTE
        applied anything: the host keeps only Alpha, the join-back picks
        Alpha's row out of the CTE, and Alpha's aggregate is right either way.
        A filter on a DIFFERENT target attribute separates them — region Alpha
        survives, but with only its gold customer counted.

        So this fails BOTH ways: if the host stops applying the filter, extra
        regions come back; if the CTE stops applying it, Alpha's ``cs`` reads
        the unfiltered 1040 instead of 1000.
        """
        rows = await self._rows(FILTER_TARGET_ATTRIBUTE)
        by_region = {r["orders.customers.regions.name"]: r for r in rows}
        assert sorted(by_region) == sorted([REGION_A_LOW, REGION_A_HIGH]), (
            f"the host base did not narrow to the gold-tier rows: "
            f"{sorted(by_region)}"
        )
        assert by_region[REGION_A_LOW]["orders.cs"] == ALPHA_SPEND_GOLD, (
            f"Alpha's cross-model spend is {by_region[REGION_A_LOW]['orders.cs']}, "
            f"not {ALPHA_SPEND_GOLD} — the re-rooted CTE aggregated an "
            f"UNFILTERED target population (unfiltered total is "
            f"{ALPHA_SPEND_ALL})"
        )

    async def test_no_warning_when_every_filter_is_reachable(self) -> None:
        """A reachable predicate misclassified as unreachable still produces
        right-looking rows, because the host applies it either way. The warning
        is the only place that mistake surfaces."""
        for flt in (FILTER_REACHABLE, FILTER_TARGET_ATTRIBUTE, FILTER_HOST_LOCAL):
            _, dropped = await self._rows_and_warnings(flt)
            assert not dropped, (
                f"{flt!r} is evaluable by the re-rooted CTE but was reported "
                f"dropped: {[str(w.message) for w in dropped]}"
            )

    async def test_the_unreachable_filter_warns_exactly_once(self) -> None:
        rows, dropped = await self._rows_and_warnings(FILTER_UNREACHABLE)
        assert len(dropped) == 1, [str(w.message) for w in dropped]
        assert "order_tags" in str(dropped[0].message)
        # And the promise the warning makes — "it still applies at the host" —
        # holds: only the two 'rush'-tagged orders' regions survive.
        regions = sorted(str(r["orders.customers.regions.name"]) for r in rows)
        assert regions == sorted([REGION_A_LOW, REGION_A_HIGH]), regions


class TestRerootedAggregateRefFilter:
    """The HAVING half, which the WHERE-phase tests above cannot reach (Codex).

    An AGGREGATE-phase predicate over the isolated aggregate
    (``customers.spend:sum > 500``) is NOT host-evaluable: the aggregate does
    not live in ``_base`` at all. So unlike a ROW-phase filter it must STAY
    routed to the CTE, and clearing ``having_filter_ids`` alongside
    ``where_filter_ids`` would be over-clearing.

    Deleting only the ``having_filter_ids`` half of the fix leaves every other
    test in this module green, which is why this class exists.
    """

    def test_the_aggregate_ref_filter_stays_routed(self) -> None:
        plan = _sole_plan(FILTER_AGGREGATE_REF)
        assert plan.rerooted_plan is not None, "the fixture stopped re-rooting"
        assert plan.applied_filter_ids, "the predicate is applied nowhere"

    async def test_it_raises_rather_than_returning_leaked_rows(self) -> None:
        """This shape is NOT yet supported end to end (stage 7b.12: a
        cross-model aggregate ref in a filter routes via the per-plan CTE, not
        inline HAVING), and it must keep saying so.

        The un-blanking that this fix corrects turned that loud
        ``NotImplementedError`` into a silent answer — three regions returned,
        two of them carrying a NULL measure, for a query whose whole point was
        to keep only the groups above a threshold. A wrong answer is strictly
        worse than an unsupported one.
        """
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            query = _query(FILTER_AGGREGATE_REF)
            with pytest.raises(NotImplementedError) as exc:
                await engine.execute(query)
        assert "not inline HAVING" in str(exc.value), exc.value


class TestRowPhaseFiltersAlwaysApplyAtTheHost:
    """B6, second instance — the same defect on the FORWARD route, found by
    following Codex's prediction that a per-plan fix leaves a cross-plan hole.

    The generator unions ``where_filter_ids`` over EVERY cross-model plan into
    one ``routed_ids`` set and skips those at the host base. So one plan's
    routing decided what the host did about a filter another plan needed —
    and the ``_cm_`` join-back is a LEFT JOIN, which propagates a value but
    never an exclusion. A host row whose group the CTE filtered away does not
    disappear; it arrives with a NULL measure.

    Present identically at the merge base, so this is not a regression from the
    re-rooting work — it is the same bug class one route over, and the fix is
    that a ROW-phase predicate is applied in BOTH places. It is host-evaluable
    by construction, and double-applying costs nothing: the CTE's copy narrows
    the aggregate, the host's copy narrows the rows.
    """

    @staticmethod
    async def _rows(query: SlayerQuery):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UnreachableFilterDroppedWarning)
                return (await engine.execute(query)).data

    #: One RE-ROOTED plan (``customers``, because the region dimension sits a
    #: hop past it) and one FORWARD plan (``regions``, for which that same
    #: dimension IS the forward path). The filter is reachable from both, so
    #: the forward plan routes it to WHERE while the re-rooted plan clears —
    #: and the union used to make the host skip it for both.
    _MIXED_ROUTES = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="name", model="customers.regions")],
        measures=[
            {"formula": "amount:sum", "name": "rev"},
            {"formula": "customers.spend:sum", "name": "cs"},
            {"formula": "customers.regions.population:sum", "name": "pop"},
        ],
        filters=[FILTER_REACHABLE],
    )

    def test_the_fixture_really_mixes_the_two_routes(self) -> None:
        """Vacuity guard: the whole point is one re-rooted plan beside one
        forward plan that routes a filter. If both re-rooted, the union would
        be empty and the row assertion below would pass for the wrong reason."""
        plans = plan_query(
            query=self._MIXED_ROUTES, bundle=dev1747_bundle(),
        ).cross_model_aggregate_plans
        rerooted = [p for p in plans if p.rerooted_plan is not None]
        forward_routing = [
            p for p in plans if p.rerooted_plan is None and p.where_filter_ids
        ]
        assert rerooted, f"no plan re-rooted: {[p.target_model for p in plans]}"
        assert forward_routing, (
            f"no forward plan routes a filter to its CTE, so there is no "
            f"cross-plan union to test: "
            f"{[(p.target_model, p.where_filter_ids) for p in plans]}"
        )

    async def test_one_plans_routing_does_not_unfilter_the_host(self) -> None:
        rows = await self._rows(self._MIXED_ROUTES)
        regions = [r["orders.customers.regions.name"] for r in rows]
        assert regions == [REGION_A_LOW], (
            f"the forward plan's routing made the host skip the filter, so "
            f"regions it excludes came back: {regions}"
        )

    async def test_a_row_phase_filter_matches_the_no_cross_model_answer(
        self,
    ) -> None:
        """P-G, stated over rows rather than SQL: adding a cross-model measure
        must not change which rows a filter keeps. That is the invariant the
        skip broke, and it is what makes "apply it in both places" the right
        rule rather than a patch — the host behaves as it always would.
        """
        plain = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="name", model="customers.regions")],
            measures=[{"formula": "amount:sum", "name": "rev"}],
            filters=[FILTER_REACHABLE],
        )
        plain_rows = await self._rows(plain)
        mixed_rows = await self._rows(self._MIXED_ROUTES)
        assert (
            [r["orders.customers.regions.name"] for r in plain_rows]
            == [r["orders.customers.regions.name"] for r in mixed_rows]
        ), f"plain={plain_rows} mixed={mixed_rows}"
        assert (
            [r["orders.rev"] for r in plain_rows]
            == [r["orders.rev"] for r in mixed_rows]
        ), "the sibling local measure changed when a cross-model one was added"

    async def test_applying_at_the_host_does_not_fan_out_a_sibling(self) -> None:
        """The hazard this fix could plausibly introduce, ruled out: the host's
        copy of a filter on a 1:N path (``order_tags``, where order 1 carries
        THREE tags) pulls that join into the base FROM, which is exactly how a
        sibling ``amount:sum`` gets multiplied.

        It does not, and the reason is structural rather than lucky — the host
        applies host filters on joined paths this way already; the cross-model
        case now simply agrees with it. Group A must read 24.0, not 46.0
        (11*3 + 13, the value a three-way fan-out on order 1 would produce).
        """
        rows = await self._rows(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "order_tags.id:count", "name": "tags"},
            ],
            filters=["order_tags.name == 'rush'"],
        ))
        by_status = {r["orders.status"]: r for r in rows}
        assert sorted(by_status) == ["A"], (
            f"'rush' tags only orders 1 and 2, both status A: {sorted(by_status)}"
        )
        assert by_status["A"]["orders.rev"] == GROUP_A_AMOUNT, (
            f"the sibling local measure fanned out: "
            f"{by_status['A']['orders.rev']} != {GROUP_A_AMOUNT}"
        )
