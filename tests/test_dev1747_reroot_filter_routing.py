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
from slayer.engine import stage_planner
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


def _attaches(*filters: str):
    # DEV-1836: the ``_query`` cross-model aggregate (a hop past its target)
    # now routes to a TARGET-rooted regroup producer rooted at ``customers``,
    # not the legacy re-rooted ``CrossModelAggregatePlan``.
    plan = plan_query(query=_query(*filters), bundle=dev1747_bundle())
    attaches = [
        a for a in plan.regroup_attach_plans
        if a.producer_root_model == "customers"
    ]
    assert attaches, "no target-rooted cross-model producer was planned"
    return attaches


def _sole_attach(*filters: str):
    attaches = _attaches(*filters)
    assert len(attaches) == 1, (
        f"expected one cross-model producer, got {len(attaches)}"
    )
    return attaches[0]


# ---------------------------------------------------------------------------
# Group 1 — routing survives the reroot (D6)
# ---------------------------------------------------------------------------
class TestRoutingSurvivesReroot:
    def test_the_plan_is_actually_rerooted(self) -> None:
        """Guard for the rest of the module: if the shape stops routing to a
        target-rooted producer, every assertion below becomes vacuous."""
        attach = _sole_attach(FILTER_REACHABLE)
        assert attach.producer_plan.source_relation == "customers"

    def test_reachable_filter_is_routed_not_blanked(self) -> None:
        # DEV-1836: a filter reachable from the producer root inherits INTO the
        # producer sub-plan (re-rooted) and is not dropped.
        attach = _sole_attach(FILTER_REACHABLE)
        assert attach.producer_plan.filters_by_phase, (
            "the reachable filter did not inherit into the producer sub-plan"
        )
        assert not attach.dropped_filter_warnings, (
            f"a reachable filter was reported dropped: "
            f"{attach.dropped_filter_warnings}"
        )

    def test_host_local_filter_is_dropped_and_warned(self) -> None:
        """DEV-1836 class-(c) divergence (divergences.md): a host-local ROW
        filter (``status``) is unreachable from the target root ``customers``,
        so it is dropped from the producer and warned — the host base still
        applies it to the local measure, but the cross-model aggregate is
        computed over the un-narrowed target population."""
        attach = _sole_attach(FILTER_HOST_LOCAL)
        assert not attach.producer_plan.filters_by_phase
        assert attach.dropped_filter_warnings

    def test_routing_lists_are_not_cleared_wholesale(self) -> None:
        """A reachable filter present alongside a host-local one must still
        inherit into the producer."""
        attach = _sole_attach(FILTER_REACHABLE, FILTER_HOST_LOCAL)
        assert attach.producer_plan.filters_by_phase, (
            "the reachable filter did not inherit even though it is present"
        )

    def test_mixed_filters_route_independently(self) -> None:
        attach = _sole_attach(FILTER_REACHABLE, FILTER_HOST_LOCAL, FILTER_UNREACHABLE)
        assert attach.producer_plan.filters_by_phase, "the reachable filter did not inherit"
        assert attach.dropped_filter_warnings, "the unreachable filter did not warn"


# ---------------------------------------------------------------------------
# Group 1b — classified EXACTLY once, in the CTE's coordinate system (D6)
# ---------------------------------------------------------------------------
def _classifier_spy(monkeypatch) -> list:
    """Record every ``_cross_model_inherited_filters`` call and its arguments.

    DEV-1836: the target-rooted producer inherits/drops filters through
    ``stage_planner._cross_model_inherited_filters`` (the legacy
    ``classify_host_filter`` route is not taken for this now-migrated shape).
    Called once per producer, so a per-CTE classification count comes from it.
    """

    calls: list = []
    original = stage_planner._cross_model_inherited_filters

    def _recording(**kwargs):
        result = original(**kwargs)
        calls.append({**kwargs, "result": result})
        return result

    monkeypatch.setattr(
        stage_planner, "_cross_model_inherited_filters", _recording,
    )
    return calls


class TestClassifiedExactlyOnce:
    def test_each_host_filter_is_classified_once_per_cte(self, monkeypatch) -> None:
        """One cross-model aggregate ⇒ one producer ⇒ the inheritance pass runs
        once, and each host ROW filter is presented to it exactly once."""
        calls = _classifier_spy(monkeypatch)
        _sole_attach(FILTER_REACHABLE, FILTER_HOST_LOCAL, FILTER_UNREACHABLE)
        assert calls, "the inheritance pass was never called — spy is vacuous"
        assert len(calls) == 1, (
            f"one producer must classify its filters once, not {len(calls)}×"
        )
        per_filter: dict = {}
        for bf, _text in calls[0]["base_filters"]:
            fid = str(bf.value_key)
            per_filter[fid] = per_filter.get(fid, 0) + 1
        assert per_filter and set(per_filter.values()) == {1}, (
            f"host filters were presented more than once: {per_filter}"
        )

    def test_the_classifier_sees_the_cte_actual_root(self, monkeypatch) -> None:
        """The re-rooted producer is rooted at ``customers``, so the inheritance
        pass must be asked about ``target_path == ("customers",)`` and the
        matching root model — classifying against the FORWARD path is how a
        reachable filter ends up judged unreachable (and vice versa)."""
        calls = _classifier_spy(monkeypatch)
        _sole_attach(FILTER_REACHABLE)
        assert calls, "the inheritance pass was never called — spy is vacuous"
        assert {call["target_path"] for call in calls} == {("customers",)}
        assert {call["root_model"].name for call in calls} == {"customers"}

    def test_the_classifier_receives_the_structural_summary(
        self, monkeypatch,
    ) -> None:
        """The decision input must carry the unreachable filter itself — an
        inheritance pass handed an EMPTY filter list would drop nothing and pass
        the warning tests below by accident."""
        calls = _classifier_spy(monkeypatch)
        attach = _sole_attach(FILTER_UNREACHABLE)
        texts = {
            text for call in calls for _bf, text in call["base_filters"]
        }
        assert FILTER_UNREACHABLE in texts, (
            f"the unreachable filter never reached the inheritance pass; "
            f"saw {texts}"
        )
        # …and the machinery actually ran the reachability decision on it.
        assert attach.dropped_filter_warnings, "the unreachable filter was not dropped"


# ---------------------------------------------------------------------------
# Group 2 — unreachable warns instead of vanishing (B6 / §5.5)
# ---------------------------------------------------------------------------
class TestUnreachableWarns:
    def test_unreachable_filter_produces_a_warning_on_the_plan(self) -> None:
        attach = _sole_attach(FILTER_UNREACHABLE)
        assert attach.dropped_filter_warnings, (
            "an unreachable filter was dropped from the producer with no "
            "warning — the B6 defect"
        )

    def test_warning_carries_the_original_filter_text(self) -> None:
        """§5.5 payload fidelity — a warning that does not name the user's own
        filter text cannot be acted on."""
        warning = _sole_attach(FILTER_UNREACHABLE).dropped_filter_warnings[0]
        assert FILTER_UNREACHABLE in warning.filter_text

    def test_warning_carries_a_reason(self) -> None:
        warning = _sole_attach(FILTER_UNREACHABLE).dropped_filter_warnings[0]
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
                # is the silent drop, not catching per se. Require the re-raise
                # at the handler's TOP level: a raise buried in a conditional
                # branch still leaves a swallow path.
                reraises = any(isinstance(s, ast.Raise) for s in node.body)
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
            stage_planner, "_cross_model_inherited_filters", _explode, raising=True,
        )
        with pytest.raises(RuntimeError, match="planner exploded"):
            _attaches(FILTER_REACHABLE)


# ---------------------------------------------------------------------------
# Group 4 — D7: dims still drop, but structurally
# ---------------------------------------------------------------------------
class TestUnreachableDimensionsStillDrop:
    def test_unreachable_dimension_is_dropped_without_a_warning(self) -> None:
        """DEV-1836: an unreachable DIMENSION broadcasts (its own warning),
        but it must not raise a dropped-FILTER warning (D7)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[
                ColumnRef(name="name", model="customers.regions"),
                ColumnRef(name="name", model="order_tags"),
            ],
            measures=[{"formula": "amount:sum", "name": "rev"}, _CROSS_MODEL_MEASURE],
        )
        plan = plan_query(query=query, bundle=dev1747_bundle())
        attach = next(
            a for a in plan.regroup_attach_plans
            if a.producer_root_model == "customers"
        )
        assert attach.broadcast_dimensions, (
            "the unreachable dimension should broadcast off the producer grain"
        )
        assert not attach.dropped_filter_warnings, (
            "a dropped DIMENSION must not raise a dropped-FILTER warning (D7)"
        )

    def test_reachable_dimensions_still_form_the_grain(self) -> None:
        attach = _sole_attach()
        assert attach.join_pairs, (
            "the producer lost its grain — the join-back would broadcast"
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
            assert _sole_attach(flt).producer_plan.source_relation == "customers", (
                f"the {label} fixture no longer roots its producer at the "
                f"target, so its row assertions no longer test re-rooting"
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
        is the only place that mistake surfaces. DEV-1836: ``FILTER_HOST_LOCAL``
        is intentionally NOT here — it is unreachable from the target root and
        now drops+warns (see ``test_host_local_filter_is_dropped_and_warned``)."""
        for flt in (FILTER_REACHABLE, FILTER_TARGET_ATTRIBUTE):
            _, dropped = await self._rows_and_warnings(flt)
            assert not dropped, (
                f"{flt!r} is evaluable by the producer but was reported "
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
    """The aggregate-phase half, which the ROW-phase tests above cannot reach.

    An AGGREGATE-phase predicate over the isolated aggregate
    (``customers.spend:sum > 500``) is NOT host-evaluable: the aggregate does
    not live in ``_base`` at all. DEV-1836 attaches it as an OUTER-SELECT WHERE
    on the producer's value (divergences.md pattern #3), so this shape — a loud
    ``NotImplementedError`` before — now executes and restricts rows uniformly.
    """

    def test_the_aggregate_ref_filter_stays_routed(self) -> None:
        # The predicate is applied at the OUTER SELECT WHERE (on the attached
        # producer value), and the producer still roots at the target.
        plan = plan_query(query=_query(FILTER_AGGREGATE_REF), bundle=dev1747_bundle())
        assert any(
            a.producer_root_model == "customers"
            for a in plan.regroup_attach_plans
        ), "the aggregate-ref shape stopped rooting its producer at the target"
        assert plan.outer_where_filter_ids, "the predicate is applied nowhere"

    async def test_it_restricts_rows_by_the_aggregate_predicate(self) -> None:
        """DEV-1836 class-(c): the aggregate-phase predicate now restricts rows
        uniformly — only region Alpha's ``customers.spend:sum`` clears 500, so
        it is the sole surviving group (the others are dropped, not returned
        with a NULL measure)."""
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            query = _query(FILTER_AGGREGATE_REF)
            rows = (await engine.execute(query)).data
        regions = [r["orders.customers.regions.name"] for r in rows]
        assert regions == [REGION_A_LOW], regions
        assert rows[0]["orders.cs"] == ALPHA_SPEND_ALL, rows[0]


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
        """Vacuity guard: the whole point is two cross-model producers rooted at
        DIFFERENT targets (``customers`` a hop past the region dimension, and
        ``regions`` for which that dimension is the forward path). If they
        collapsed to one, the row assertions below would test nothing."""
        attaches = plan_query(
            query=self._MIXED_ROUTES, bundle=dev1747_bundle(),
        ).regroup_attach_plans
        roots = {a.producer_root_model for a in attaches}
        assert {"customers", "regions"} <= roots, (
            f"expected producers rooted at both customers and regions, got {roots}"
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
