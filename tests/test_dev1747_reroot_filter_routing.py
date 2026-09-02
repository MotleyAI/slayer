"""No silent filter drops when a cross-model CTE is re-rooted at its target (B6/D6/D7)."""
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
import inspect

#: Cross-model aggregate + a dimension a hop past the target → planner re-roots the CTE at ``customers``.
_CROSS_MODEL_MEASURE = {"formula": "customers.spend:sum", "name": "cs"}

#: Reachable from the re-rooted target (``customers -> regions``).
FILTER_REACHABLE = "customers.regions.name == 'Alpha'"
#: Purely host-local — filters host rows, stays at the host base, never warns.
FILTER_HOST_LOCAL = "status == 'A'"
#: Off the target's graph (``orders -> order_tags``) — unreachable from a CTE rooted at ``customers``.
FILTER_UNREACHABLE = "order_tags.name == 'rush'"
#: Reachable from both scopes; changes the aggregate inside a surviving group (Alpha ``cs`` 1040→1000) — separates CTE-applied from host-applied.
FILTER_TARGET_ATTRIBUTE = "customers.tier == 'gold'"
#: Aggregate-phase predicate over the isolated aggregate; not host-evaluable, so it must STAY routed to the CTE.
FILTER_AGGREGATE_REF = "customers.spend:sum > 500"

#: A HOST-ROOTED shape (``cte_root_model == "orders"``): order by a derived crossing column — the DEV-1503/1709 helpers live only on this route.
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


class TestRoutingSurvivesReroot:
    def test_the_plan_is_actually_rerooted(self) -> None:
        """Vacuity guard: if the shape stops routing to a target-rooted producer, the rest of the module is vacuous."""
        attach = _sole_attach(FILTER_REACHABLE)
        assert attach.producer_plan.source_relation == "customers"

    def test_reachable_filter_is_routed_not_blanked(self) -> None:
        attach = _sole_attach(FILTER_REACHABLE)
        assert attach.producer_plan.filters_by_phase, (
            "the reachable filter did not inherit into the producer sub-plan"
        )
        assert not attach.dropped_filter_warnings, (
            f"a reachable filter was reported dropped: "
            f"{attach.dropped_filter_warnings}"
        )

    def test_host_local_filter_is_dropped_and_warned(self) -> None:
        """Host-local ROW filter is unreachable from target root ``customers`` → dropped from the producer and warned; the host base still applies it locally."""
        attach = _sole_attach(FILTER_HOST_LOCAL)
        assert not attach.producer_plan.filters_by_phase
        assert attach.dropped_filter_warnings

    def test_routing_lists_are_not_cleared_wholesale(self) -> None:
        attach = _sole_attach(FILTER_REACHABLE, FILTER_HOST_LOCAL)
        assert attach.producer_plan.filters_by_phase, (
            "the reachable filter did not inherit even though it is present"
        )

    def test_mixed_filters_route_independently(self) -> None:
        attach = _sole_attach(FILTER_REACHABLE, FILTER_HOST_LOCAL, FILTER_UNREACHABLE)
        assert attach.producer_plan.filters_by_phase, "the reachable filter did not inherit"
        assert attach.dropped_filter_warnings, "the unreachable filter did not warn"


def _classifier_spy(monkeypatch) -> list:
    """Record every ``_cross_model_inherited_filters`` call and its arguments (once per producer)."""

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
        assert per_filter, "no host filters were presented at all"
        assert len(per_filter) == 3, (
            f"not all host filters were presented: {per_filter}"
        )
        assert set(per_filter.values()) == {1}, (
            f"host filters were presented more than once: {per_filter}"
        )

    def test_the_classifier_sees_the_cte_actual_root(self, monkeypatch) -> None:
        """The inheritance pass must classify against ``target_path == ("customers",)``; the FORWARD path would misjudge reachability."""
        calls = _classifier_spy(monkeypatch)
        _sole_attach(FILTER_REACHABLE)
        assert calls, "the inheritance pass was never called — spy is vacuous"
        assert {call["target_path"] for call in calls} == {("customers",)}
        assert {call["root_model"].name for call in calls} == {"customers"}

    def test_the_classifier_receives_the_structural_summary(
        self, monkeypatch,
    ) -> None:
        """The decision input must carry the unreachable filter itself, else the warning tests below pass vacuously."""
        calls = _classifier_spy(monkeypatch)
        attach = _sole_attach(FILTER_UNREACHABLE)
        texts = {
            text for call in calls for _bf, text in call["base_filters"]
        }
        assert FILTER_UNREACHABLE in texts, (
            f"the unreachable filter never reached the inheritance pass; "
            f"saw {texts}"
        )
        assert attach.dropped_filter_warnings, "the unreachable filter was not dropped"


class TestUnreachableWarns:
    def test_unreachable_filter_produces_a_warning_on_the_plan(self) -> None:
        attach = _sole_attach(FILTER_UNREACHABLE)
        assert attach.dropped_filter_warnings, (
            "an unreachable filter was dropped from the producer with no "
            "warning — the B6 defect"
        )

    def test_warning_carries_the_original_filter_text(self) -> None:
        warning = _sole_attach(FILTER_UNREACHABLE).dropped_filter_warnings[0]
        assert FILTER_UNREACHABLE in warning.filter_text

    def test_warning_carries_a_reason(self) -> None:
        warning = _sole_attach(FILTER_UNREACHABLE).dropped_filter_warnings[0]
        assert warning.reason, "the warning carries no reason at all"
        assert "reach" in warning.reason.lower(), warning.reason

    async def test_exactly_one_warning_per_filter_per_execute(self) -> None:
        """The boundary dedups per filter identity: two cross-model measures classify one filter twice, user sees it once."""
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
        """Under ``-W error`` the drop must stop execution, not silently return fewer rows."""
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
        """Identity is per filter, not per text bucket: two different unreachable filters warn separately."""
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


class TestInternalFailuresRaise:


    def test_no_bare_except_in_the_reroot_path(self) -> None:
        """No swallowing ``except`` in the reroot functions: a broad tuple, ``except Exception``, or bare ``except:`` all reintroduce the B6 defect."""
        broad = {"Exception", "BaseException", "ValueError"}
        checked = 0
        for name in (
            "_cross_model_inherited_filters",
            "_attributable_from_root",
            "_reroot_from_root",
        ):
            target = getattr(stage_planner, name, None)
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
                # A broad catch that re-raises at the handler's top level doesn't swallow — the B6 defect is the silent drop.
                reraises = any(isinstance(s, ast.Raise) for s in node.body)
                assert not (names & broad) or reraises, (
                    f"{name} SWALLOWS {sorted(names & broad)} without re-raising "
                    f"— a swallowed broad error is exactly the B6 defect"
                )
        assert checked == 3, "not every reroot function was checked"

    def test_planner_failure_propagates_rather_than_warning(self, monkeypatch) -> None:

        boom = RuntimeError("planner exploded")

        def _explode(**_kwargs):
            raise boom

        monkeypatch.setattr(
            stage_planner, "_cross_model_inherited_filters", _explode, raising=True,
        )
        with pytest.raises(RuntimeError, match="planner exploded"):
            _attaches(FILTER_REACHABLE)


class TestUnreachableDimensionsStillDrop:
    def test_unreachable_dimension_is_dropped_without_a_warning(self) -> None:
        """An unreachable DIMENSION broadcasts but must not raise a dropped-FILTER warning (D7)."""
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


class TestFilteredLocalDispatchAccounting:
    """A ``grain="host"`` wrap (path is its crossing) must take the HOST-rooted route; a target-rooted producer would degenerate to a scalar CROSS JOIN (D2)."""

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

    def test_host_grain_wrap_dispatches_to_the_host_rooted_route(self) -> None:
        plan = plan_query(
            query=self._grouped_joined_order(), bundle=dev1747_bundle(),
        )
        (attach,) = [
            a for a in plan.regroup_attach_plans
            if a.attach_phase == "combined"
        ]
        assert attach.producer_root_model is None
        (sub,) = attach.substitutions
        assert sub.original_key.grain == "host"
        assert sub.original_key.source.path == ("customers", "regions"), (
            "the wrap lost its path on the way to the host-rooted route"
        )

    def test_a_target_grain_aggregate_does_not_take_that_route(self) -> None:
        """The contrast: a genuine cross-model measure must keep the TARGET-rooted producer, or its value silently becomes per-host-group."""
        plan = plan_query(
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
        roots = {a.producer_root_model for a in plan.regroup_attach_plans}
        assert len(plan.regroup_attach_plans) == 1, plan.regroup_attach_plans
        assert roots == {"customers"}, (
            f"the target-grain aggregate must plan ONE target-rooted producer; "
            f"got producer roots {roots}"
        )

    def test_the_path_counts_as_the_crossing_input(self) -> None:
        plan = plan_query(
            query=self._grouped_joined_order(), bundle=dev1747_bundle(),
        )
        attaches = [
            a for a in plan.regroup_attach_plans
            if a.attach_phase == "combined" and a.producer_root_model is None
        ]
        assert attaches, "no host-rooted producer was planned for the wrap"


class TestRerootedFilterStillNarrowsTheHost:
    """A re-rooted plan has no forward CTE, so ``where``/``having`` routing ids must NOT tell the host base to skip a host-evaluable filter — else excluded rows return with a NULL measure. Row-level tests; the plan-level ones passed throughout."""

    @staticmethod
    async def _rows_and_warnings(*filters: str):
        """Rows plus every warning; warnings are the only signal a reachable predicate was misclassified (rows still look right)."""
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
        """Vacuity guard: if the planner stopped re-rooting and used a forward plan, the row assertions below would pass while testing nothing."""
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
        """A reachable filter must narrow the host to one region group; without the fix it silently became "annotate, don't exclude"."""
        rows = await self._rows(FILTER_REACHABLE)
        regions = [r["orders.customers.regions.name"] for r in rows]
        assert regions == [REGION_A_LOW], (
            f"the re-rooted CTE applied the filter but the host base did not, "
            f"so excluded regions came back: {regions}"
        )

    async def test_the_excluded_regions_do_not_come_back_as_nulls(self) -> None:
        """The symptom is a row present with a NULL measure — a count assertion alone would not catch it."""
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
        """The surviving group must keep its own values; an over-filtering fix would empty the result and still pass the two tests above."""
        rows = await self._rows(FILTER_REACHABLE)
        assert len(rows) == 1, rows
        assert rows[0]["orders.rev"] == 11.0, (
            f"the surviving group's own measure changed: {rows[0]}"
        )
        assert rows[0]["orders.cs"] == ALPHA_SPEND_ALL, (
            f"the cross-model measure changed: {rows[0]}"
        )

    async def test_a_host_local_filter_still_narrows_the_host(self) -> None:
        """Control: a host-local filter never had routing ids, so it is unaffected — ``status == 'A'`` keeps group A's two orders across two regions."""
        rows = await self._rows(FILTER_HOST_LOCAL)
        regions = sorted(
            str(r["orders.customers.regions.name"]) for r in rows
        )
        assert regions == sorted([REGION_A_LOW, REGION_A_HIGH]), regions
        assert sum(r["orders.rev"] for r in rows) == GROUP_A_AMOUNT, rows

    async def test_an_unreachable_filter_still_narrows_the_host(self) -> None:
        """An unreachable filter must still apply at the host — that is the promise the dropped-filter warning makes."""
        rows = await self._rows(FILTER_UNREACHABLE)
        regions = sorted(
            str(r["orders.customers.regions.name"]) for r in rows
        )
        assert regions == sorted([REGION_A_LOW, REGION_A_HIGH]), regions

    async def test_the_rerooted_cte_applies_its_own_copy_of_the_filter(
        self,
    ) -> None:
        """A filter on a DIFFERENT target attribute separates host from CTE: extra regions return if the host stops filtering; Alpha's ``cs`` reads 1040 not 1000 if the CTE does."""
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
        """A misclassified-reachable predicate still yields right-looking rows; the warning is the only tell. ``FILTER_HOST_LOCAL`` is excluded — it drops+warns now."""
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
        regions = sorted(str(r["orders.customers.regions.name"]) for r in rows)
        assert regions == sorted([REGION_A_LOW, REGION_A_HIGH]), regions


class TestRerootedAggregateRefFilter:
    """An aggregate-phase predicate over the isolated aggregate is not host-evaluable; it attaches as an OUTER-SELECT WHERE on the producer value and restricts rows uniformly."""

    def test_the_aggregate_ref_filter_stays_routed(self) -> None:
        plan = plan_query(query=_query(FILTER_AGGREGATE_REF), bundle=dev1747_bundle())
        assert any(
            a.producer_root_model == "customers"
            for a in plan.regroup_attach_plans
        ), "the aggregate-ref shape stopped rooting its producer at the target"
        assert plan.outer_where_filter_ids, "the predicate is applied nowhere"

    async def test_it_restricts_rows_by_the_aggregate_predicate(self) -> None:
        """Only Alpha's ``customers.spend:sum`` clears 500, so it is the sole surviving group — others dropped, not returned with a NULL measure."""
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
    """Same B6 defect on the FORWARD route: unioning ``where_filter_ids`` across plans let one plan's routing unfilter the host for another's filter. A ROW-phase predicate must apply in BOTH the CTE and the host base."""

    @staticmethod
    async def _rows(query: SlayerQuery):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "dev1747.db")
            seed_dev1747_sqlite(db)
            engine = await make_sqlite_engine(d, db)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UnreachableFilterDroppedWarning)
                return (await engine.execute(query)).data

    #: Mixes a re-rooted plan (``customers``) and a forward plan (``regions``); the filter is reachable from both, and the union used to make the host skip it.
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
        """Vacuity guard: needs two producers rooted at different targets (``customers`` and ``regions``); if they collapse, the rows below test nothing."""
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
        """Adding a cross-model measure must not change which rows a filter keeps — the invariant the host-skip broke."""
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
        """Ruled out: the host's copy of a filter on a 1:N path (``order_tags``) must not fan out a sibling ``amount:sum`` — group A reads 24.0, not the 46.0 a three-way fan-out gives."""
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
