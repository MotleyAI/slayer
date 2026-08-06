"""DEV-1747 §5.10 — ``OrderEntry`` enrichment at PLAN time.

Today ``OrderEntry`` carries only ``(slot_id, direction)``, so each of the
three renderers re-derives everything else at render time — and disagrees:

* ``_apply_order_limit_from_planned`` dispatches on the SLOT KIND;
* ``_resolve_combined_order_term`` runs a 5-way precedence chain
  (hidden-CTE ref → cross-model alias → outer-composite alias → outer-composite
  expression → bare → ``_base.``-qualified);
* ``_planned_order_by_sql`` builds text and knows about none of it.

§5.10 moves the classification into the plan: ``scope`` names WHERE the ordered
value lives, ``phase`` its phase, ``nulls`` the null-ordering policy. This
module asserts the PLAN — no SQL — because "plan decides, render emits" (P-D)
is only true if the decision is observable without rendering.

``scope`` and ``phase`` are REQUIRED with no default. A shape the planner
forgets to classify must fail loudly rather than fall through to the
``_base.``-qualified branch, which is how an order term silently attaches to
the wrong scope today.

Refs: DEV-1747 (D3, D5), DEV-1742 §5.10 / P-D.
"""
from __future__ import annotations

import pytest

from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.core.enums import TimeGranularity
from slayer.engine.planned import OrderEntry, OrderScope
from slayer.engine.stage_planner import plan_query
from tests._dev1747_fixtures import dev1747_bundle

_MEASURE = [{"formula": "amount:sum", "name": "rev"}]


def _plan(query: SlayerQuery):
    return plan_query(query=query, bundle=dev1747_bundle())


def _sole_entry(query: SlayerQuery) -> OrderEntry:
    plan = _plan(query)
    assert len(plan.order) == 1, (
        f"expected one order entry, got {len(plan.order)}"
    )
    return plan.order[0]


# ---------------------------------------------------------------------------
# Group 1 — the field contract
# ---------------------------------------------------------------------------
class TestOrderEntryShape:
    def test_scope_is_required(self) -> None:
        """No default. A planner path that forgets to classify must fail at
        construction, not silently order against ``_base``."""
        with pytest.raises(Exception):
            OrderEntry(slot_id="s1", direction="asc")  # type: ignore[call-arg]

    def test_nulls_defaults_to_dialect_default(self) -> None:
        from slayer.core.keys import Phase

        entry = OrderEntry(
            slot_id="s1", direction="asc",
            scope=OrderScope.HOST_BASE, phase=Phase.ROW,
        )
        assert entry.nulls == "default"

    def test_nulls_rejects_an_unknown_policy(self) -> None:
        from slayer.core.keys import Phase

        with pytest.raises(Exception):
            OrderEntry(
                slot_id="s1", direction="asc", scope=OrderScope.HOST_BASE,
                phase=Phase.ROW, nulls="sometimes",  # type: ignore[arg-type]
            )

    def test_direction_validation_still_applies(self) -> None:
        """The existing contract must survive the enrichment."""
        from slayer.core.keys import Phase

        with pytest.raises(Exception):
            OrderEntry(
                slot_id="s1", direction="ASC",  # type: ignore[arg-type]
                scope=OrderScope.HOST_BASE, phase=Phase.ROW,
            )


# ---------------------------------------------------------------------------
# Group 2 — scope classification per shape
# ---------------------------------------------------------------------------
class TestScopeClassification:
    def test_projected_dimension_is_host_base(self) -> None:
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="status"), direction="asc")],
        ))
        assert entry.scope is OrderScope.HOST_BASE

    def test_projected_measure_is_host_base(self) -> None:
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
        ))
        assert entry.scope is OrderScope.HOST_BASE

    def test_hidden_local_aggregate_is_host_base_hidden(self) -> None:
        """Materialised in the base but trimmed from the public projection —
        the distinction the ``bare_ids`` set encodes at render time today."""
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        ))
        assert entry.scope is OrderScope.HOST_BASE_HIDDEN

    def test_cross_model_aggregate_is_cross_model_cte(self) -> None:
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
        ))
        assert entry.scope is OrderScope.CROSS_MODEL_CTE

    def test_windowed_measure_is_windowed_cte(self) -> None:
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "amount:sum(window='90d')", "name": "w"}],
            order=[OrderItem(column=ColumnRef(name="w"), direction="desc")],
        ))
        assert entry.scope is OrderScope.WINDOWED_CTE

    def test_transform_measure_is_transform_step(self) -> None:
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            order=[OrderItem(column=ColumnRef(name="cs"), direction="asc")],
        ))
        assert entry.scope is OrderScope.TRANSFORM_STEP

    def test_composite_over_a_cross_model_operand_is_outer_composite(self) -> None:
        """``customers.spend:sum + amount:sum`` cannot be evaluated in ``_base``
        — one operand lives in a ``_cm_`` CTE — so it is rendered in the outer
        combined SELECT and ordered there.

        This is its own scope, not a variant of ``CROSS_MODEL_CTE``: the value
        has no CTE column to name, only an outer alias or a re-rendered
        expression, which is exactly the branch
        ``_resolve_combined_order_term``'s precedence chain gets wrong."""
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "customers.spend:sum + amount:sum", "name": "mix"}],
            order=[OrderItem(column=ColumnRef(name="mix"), direction="desc")],
        ))
        assert entry.scope is OrderScope.OUTER_COMPOSITE

    def test_hidden_composite_order_is_outer_composite_too(self) -> None:
        """The hidden variant: nothing projects the composite, so a scope that
        fell back to ``HOST_BASE`` would render it inline in ``_base`` and
        silently substitute a plain aggregate for the cross-model one."""
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(
                column="customers.spend:sum + amount:sum", direction="desc",
            )],
        ))
        assert entry.scope is OrderScope.OUTER_COMPOSITE

    def test_grouped_joined_wrap_is_cross_model_cte(self) -> None:
        """The DEV-1735 wrap lives in its own host-rooted CTE, so it resolves
        CTE-qualified — not as a bare ``_base`` alias."""
        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(
                column=ColumnRef(name="name", model="customers.regions"),
                direction="asc",
            )],
        ))
        assert entry.scope is OrderScope.CROSS_MODEL_CTE


# ---------------------------------------------------------------------------
# Group 3 — phase and direction ride through
# ---------------------------------------------------------------------------
class TestPhaseAndDirection:
    def test_row_target_carries_row_phase(self) -> None:
        from slayer.core.keys import Phase

        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="status"), direction="asc")],
        ))
        assert entry.phase is Phase.ROW

    def test_aggregate_target_carries_aggregate_phase(self) -> None:
        from slayer.core.keys import Phase

        entry = _sole_entry(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
        ))
        assert entry.phase is Phase.AGGREGATE

    def test_multiple_entries_keep_their_own_scope_and_direction(self) -> None:
        plan = _plan(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[
                OrderItem(column=ColumnRef(name="status"), direction="asc"),
                OrderItem(column=ColumnRef(name="rev"), direction="desc"),
                OrderItem(column=ColumnRef(name="created_at"), direction="asc"),
            ],
        ))
        assert [e.direction for e in plan.order] == ["asc", "desc", "asc"]
        assert [e.scope for e in plan.order] == [
            OrderScope.HOST_BASE, OrderScope.HOST_BASE, OrderScope.HOST_BASE_HIDDEN,
        ]


# ---------------------------------------------------------------------------
# Group 4 — the direction-aware wrap is decided at plan time (D10)
# ---------------------------------------------------------------------------
class TestDirectionAwareWrapIsPlanned:
    def _wrap_aggs(self, direction: str) -> list[str]:
        from slayer.core.keys import AggregateKey

        plan = _plan(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(
                column=ColumnRef(name="created_at"), direction=direction,
            )],
        ))
        return [
            s.key.agg for s in plan.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.hidden
        ]

    def test_ascending_plans_a_min_wrap(self) -> None:
        assert self._wrap_aggs("asc") == ["min"]

    def test_descending_plans_a_max_wrap(self) -> None:
        assert self._wrap_aggs("desc") == ["max"]

    def test_same_column_both_directions_plans_two_slots(self) -> None:
        """MIN(a) and MAX(a) are different values, so they must be different
        slots. Keying the order remap by key alone collapses them."""
        from slayer.core.keys import AggregateKey

        plan = _plan(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[
                OrderItem(column=ColumnRef(name="created_at"), direction="asc"),
                OrderItem(column=ColumnRef(name="created_at"), direction="desc"),
            ],
        ))
        aggs = sorted(
            s.key.agg for s in plan.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.hidden
        )
        assert aggs == ["max", "min"]
        assert len({e.slot_id for e in plan.order}) == 2


# ---------------------------------------------------------------------------
# Group 5 — the host-grain marker (D2)
# ---------------------------------------------------------------------------
class TestHostGrainMarker:
    def test_joined_order_wrap_is_marked_host_grain(self) -> None:
        from slayer.core.keys import AggregateKey

        plan = _plan(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(
                column=ColumnRef(name="name", model="customers.regions"),
                direction="asc",
            )],
        ))
        wraps = [
            s.key for s in plan.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.hidden
        ]
        assert wraps, "no hidden order wrap was planned"
        assert wraps[0].grain == "host"
        assert wraps[0].source.path == ("customers", "regions")

    def test_local_wrap_keeps_the_default_grain(self) -> None:
        from slayer.core.keys import AggregateKey

        plan = _plan(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        ))
        wraps = [
            s.key for s in plan.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.hidden
        ]
        assert wraps and wraps[0].grain == "target"

    def test_host_grain_and_target_grain_are_distinct_identities(self) -> None:
        """A user-declared ``customers.regions.name:max`` measure and the
        synthetic host-grain wrap mean different things (global vs per-group),
        so they must not intern onto one slot."""
        from slayer.core.keys import AggregateKey, ColumnKey

        source = ColumnKey(path=("customers", "regions"), leaf="name")
        target_rooted = AggregateKey(source=source, agg="max")
        host_rooted = AggregateKey(source=source, agg="max", grain="host")
        assert target_rooted != host_rooted
        assert hash(target_rooted) != hash(host_rooted)
        assert len({target_rooted, host_rooted}) == 2


# ---------------------------------------------------------------------------
# Group 6 — the revised Law-3 trigger, branch by branch (D2)
# ---------------------------------------------------------------------------
_GROUPED_HEAD = {
    "source_model": "orders",
    "dimensions": [ColumnRef(name="status")],
}

#: ``grain="host"`` + non-empty ``source.path`` — the NEW branch. Today the
#: trigger reads ``if not agg_path and not has_crossing_input: continue`` and
#: then routes any path-bearing key to a TARGET-rooted CTE.
_HOST_GRAIN_WITH_PATH = SlayerQuery(
    **_GROUPED_HEAD, measures=_MEASURE,
    order=[OrderItem(
        column=ColumnRef(name="name", model="customers.regions"), direction="asc",
    )],
)

#: Target-grain + path — the pre-existing cross-model case, unchanged.
_TARGET_GRAIN_WITH_PATH = SlayerQuery(
    **_GROUPED_HEAD,
    measures=[
        {"formula": "amount:sum", "name": "rev"},
        {"formula": "customers.spend:sum", "name": "cs"},
    ],
)

#: Host-grain, NO path, crossing input — the DEV-1709 branch that already
#: works. Included so the matrix proves D2 does not disturb it.
_HOST_GRAIN_CROSSING_INPUT = SlayerQuery(
    **_GROUPED_HEAD, measures=_MEASURE,
    order=[OrderItem(column=ColumnRef(name="cust_region"), direction="asc")],
)


class TestLaw3TriggerMatrix:
    """Each branch of the widened trigger, and where it routes.

    ``cte_root_model`` is the discriminator the planner already carries: set to
    the HOST model name for a host-rooted CTE, ``None`` for a target-rooted one.
    A host-grain wrap landing target-rooted is the scalar-CROSS-JOIN
    degeneration DEV-1735 describes, and it is invisible in a per-case test that
    only asserts "a plan exists".
    """

    @pytest.mark.parametrize(
        ("query", "expect_host_rooted"),
        [
            pytest.param(_HOST_GRAIN_WITH_PATH, True, id="host-grain+path"),
            pytest.param(_TARGET_GRAIN_WITH_PATH, False, id="target-grain+path"),
            pytest.param(
                _HOST_GRAIN_CROSSING_INPUT, True, id="host-grain+crossing-input",
            ),
        ],
    )
    def test_trigger_routes_each_branch(
        self, query: SlayerQuery, expect_host_rooted: bool,
    ) -> None:
        plan = _plan(query)
        assert plan.cross_model_aggregate_plans, (
            "the Law-3 trigger did not fire for this branch"
        )
        cma = plan.cross_model_aggregate_plans[0]
        assert (cma.cte_root_model is not None) is expect_host_rooted, (
            f"cte_root_model={cma.cte_root_model!r} — expected "
            f"{'host' if expect_host_rooted else 'target'}-rooted"
        )

    def test_isolation_disabled_renders_inline_instead_of_recursing(self) -> None:
        """The recursion guard. The host-rooted sub-plan contains the SAME
        crossing key, so if the trigger fired again inside it the planner would
        recurse without bound — which is why ``subplan_builder`` always passes
        ``disable_host_rooted_isolation=True``.

        Under that flag the key must render INLINE (base-pull), which is legal
        there because the CTE is the aggregate's own scope. Observable as: no
        cross-model plan at all.
        """
        plan = plan_query(
            query=_HOST_GRAIN_WITH_PATH,
            bundle=dev1747_bundle(),
            disable_host_rooted_isolation=True,
        )
        assert not plan.cross_model_aggregate_plans, (
            "a host-grain wrap still isolated under "
            "disable_host_rooted_isolation — the sub-plan would recurse"
        )

    def test_the_disabled_flag_does_not_suppress_target_rooted_plans(self) -> None:
        """The flag is scoped to HOST-rooted isolation. Suppressing genuine
        cross-model aggregates too would silently inline a joined SUM into the
        host base and multiply it by the join's fan-out."""
        plan = plan_query(
            query=_TARGET_GRAIN_WITH_PATH,
            bundle=dev1747_bundle(),
            disable_host_rooted_isolation=True,
        )
        assert plan.cross_model_aggregate_plans, (
            "disable_host_rooted_isolation wrongly suppressed a target-rooted "
            "cross-model plan"
        )
