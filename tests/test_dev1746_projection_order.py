"""DEV-1746 §5.2 — one ordered public projection (B7, B8, B11).

``PlannedQuery.projection`` already exists, is already in declaration order, and
already excludes hidden slots. What is missing is that **no renderer consumes
it**: the cross-model combined SELECT rebuilds an order out of four separate
grouped passes (host slots, then outer composites, then ``_cm_``, then ``_wm_``),
which is why a cross-model measure declared FIRST is emitted LAST. The generator
even admits it in a comment — "windowed columns are grouped after the ``_base``
projection rather than woven into ``planned_query.projection`` order". B7 makes
every renderer walk the plan's list verbatim.

Trimming hidden slots then stops being a mechanism at all: a hidden slot is
simply absent from ``projection``, which is what replaces the five bespoke
variants. One renderer-side assertion is kept as a belt because pydantic's
``model_copy(update=…)`` skips validation and rerooted plans use it (Codex D9).

**B8** — eight inner-stage projection sites carry their aliases as
``sorted(aliases)``; one still carries the comment "matches legacy
``_generate_with_computed:1607``", i.e. it is byte-parity ballast. They become
plan-ordered. The tests name measures so that alphabetical order and declaration
order DISAGREE (``zz_`` declared before ``aa_``), because a test whose two
candidate orders coincide proves nothing. Each site also fails closed: the new
ordered list must contain exactly the aliases the old flattening did, so a
reorder can never silently drop one (Codex D8).

**B11** — the base FROM's join order comes from a two-tier merge that exists to
reproduce legacy bytes ("→ byte-identical FROM"); it becomes the same
first-seen registration order the per-CTE path already uses. B11 is ratified as
**execution-identical**, so what is asserted here is the part that can regress:
the join SET is unchanged, every join still appears (including one discovered
late, from a filter rather than a projection), generation is deterministic, and
the results are right. The precise emitted ORDER is deliberately not pinned to a
literal here — both the old and new orders are deterministic and
execution-equivalent, so a literal pin would assert an implementation detail
rather than a contract; the order change itself is surfaced through the PR's
recompare churn list (Codex D6).
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.planned import PlannedQuery, ValueSlot
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import SQLGenerator
from slayer.sql.scope import ScopeFrame

from tests._cross_model_chain import (
    _countries,
    _customers_v2,
    _gen,
    _orders_x,
    _regions,
)
from tests._dev1746_fixtures import (
    base_cte_join_sequence,
    carried_alias_drops,
    carry_list_order_violations,
    make_sqlite_engine,
    outer_select_aliases,
    seed_dev1746_sqlite,
)
from tests._engine_helpers import (
    _engine_generate,
    _extract_cte_body,
    _join_aliases,
)


def _chain_bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_x(),
        referenced_models=[_customers_v2(), _regions(), _countries()],
    )


@pytest.fixture
async def exec_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "dev1746.db")
        seed_dev1746_sqlite(db_path)
        yield await make_sqlite_engine(os.path.join(d, "store"), db_path)


# --------------------------------------------------------------------------- #
# Shapes whose declaration order and current emitted order disagree.
# --------------------------------------------------------------------------- #
def _cm_declared_first_query() -> SlayerQuery:
    """A cross-model measure declared BEFORE a local one. Emitted today as
    ``status, local_second, cm_first``; B7 makes it ``status, cm_first,
    local_second``."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        measures=[
            ModelMeasure(formula="customers_v2.lifetime_value:sum", name="cm_first"),
            ModelMeasure(formula="amount:sum", name="local_second"),
        ],
    )


def _windowed_declared_first_query() -> SlayerQuery:
    """A windowed measure declared BEFORE a local one."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="amount:sum(window='90d')", name="w_first"),
            ModelMeasure(formula="amount:sum", name="local_second"),
        ],
    )


def _interleaved_query() -> SlayerQuery:
    """local, cross-model, local — an order no grouped-pass scheme can produce,
    because any such scheme emits all host slots before all ``_cm_`` ones."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        measures=[
            ModelMeasure(formula="amount:sum", name="a_local"),
            ModelMeasure(formula="customers_v2.lifetime_value:sum", name="b_cross"),
            ModelMeasure(formula="amount:avg", name="c_local"),
        ],
    )


def _all_slots(planned: PlannedQuery) -> List[ValueSlot]:
    """Every slot a projection id can point at.

    A transform (``cumsum(...)``) is a ``combined_expression_slot``, not a row
    or aggregate slot, so a lookup over only those two misses it.
    """
    return (
        list(planned.row_slots)
        + list(planned.aggregate_slots)
        + list(planned.combined_expression_slots)
    )


def _expected_projection_aliases(query: SlayerQuery) -> List[str]:
    """The public aliases the plan declares, in plan order.

    Derived from ``PlannedQuery.projection`` rather than hard-coded, so this
    expresses the actual contract ("renderers consume the plan's list verbatim")
    instead of a guess about how aliases are spelled.
    """
    planned = plan_query(query=query, bundle=_chain_bundle())
    slots = {s.id: s for s in _all_slots(planned)}
    out: List[str] = []
    for sid in planned.projection:
        slot = slots[sid]
        assert not slot.hidden, (
            f"hidden slot {sid} appeared in PlannedQuery.projection — the "
            f"public projection must contain only public slots."
        )
        out.append(slot.public_aliases[0] if slot.public_aliases else slot.public_name)
    return out


def _alias_suffixes(aliases: List[str]) -> List[str]:
    """Emitted aliases are host-rooted (``orders_x.status``); the plan names the
    trailing public part. Compare on that."""
    return [a.split(".")[-1] for a in aliases]


# =========================================================================== #
# B7 — declaration-order projection.
# =========================================================================== #
class TestB7DeclarationOrderProjection:

    async def test_cross_model_measure_keeps_its_declared_position(self) -> None:
        """NEW (B7): a cross-model measure declared first is emitted first."""
        sql = await _gen(_cm_declared_first_query(), dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["status", "cm_first", "local_second"], (
            f"emitted projection order {emitted} is not declaration order — the "
            f"cross-model measure was grouped after the host slots.\n\n{sql}"
        )

    async def test_windowed_measure_keeps_its_declared_position(self) -> None:
        """NEW (B7): the same for a windowed measure."""
        sql = await _gen(_windowed_declared_first_query(), dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["status", "created_at", "w_first", "local_second"], (
            f"emitted projection order {emitted} is not declaration order.\n\n{sql}"
        )

    async def test_interleaved_local_and_cross_model_measures(self) -> None:
        """The decisive shape: local, cross-model, local. No grouped-pass scheme
        can emit this order — only walking the plan's list can."""
        sql = await _gen(_interleaved_query(), dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["status", "a_local", "b_cross", "c_local"], (
            f"emitted {emitted}; a cross-model measure cannot be woven between "
            f"two host measures unless the renderer consumes the plan's "
            f"ordered projection.\n\n{sql}"
        )

    @pytest.mark.parametrize(
        "query_factory",
        [_cm_declared_first_query, _windowed_declared_first_query, _interleaved_query],
        ids=["cross_model", "windowed", "interleaved"],
    )
    async def test_emitted_order_equals_the_plans_projection_order(
        self, query_factory,
    ) -> None:
        """The contract itself, stated once: emitted order IS plan order."""
        query = query_factory()
        sql = await _gen(query, dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        expected = _alias_suffixes(_expected_projection_aliases(query))
        assert emitted == expected, (
            f"renderer did not consume PlannedQuery.projection verbatim:\n"
            f"  emitted:  {emitted}\n  plan:     {expected}\n\n{sql}"
        )

    async def test_response_column_order_follows_declaration_order(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """B7 is observable to callers: ``response.columns`` is read off the
        emitted outer SELECT, so its order changes with the projection."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="cm_first"),
                ModelMeasure(formula="amount:sum", name="local_second"),
            ],
        )
        resp = await exec_engine.execute(query)
        assert _alias_suffixes(list(resp.columns)) == [
            "status", "cm_first", "local_second",
        ], f"response column order: {resp.columns}"
        # Row keys follow the same order.
        assert _alias_suffixes(list(resp.data[0].keys())) == [
            "status", "cm_first", "local_second",
        ], f"row keys: {list(resp.data[0].keys())}"

    async def test_a_slot_that_is_both_projected_and_a_transform_operand(
        self,
    ) -> None:
        """A slot can be publicly projected AND consumed as a transform input.

        This is the shape where the projection's occurrence count and the slot's
        rendered columns could disagree, which is what the combined SELECT's
        leftover guard exists to catch.

        Three things have to line up for the test to mean anything, so each is
        asserted rather than assumed:

        * the measure and the transform's operand must be ONE slot (same key →
          same slot), otherwise there is no slot reached by both paths;
        * the query must take the CROSS-MODEL path, because the guard lives in
          the combined-SELECT builder — a purely local transform never reaches
          it;
        * the assertion must be on the COMBINED select (which becomes the
          transform chain's ``base`` CTE), because a leftover column appended
          there would be trimmed by the outer wrap and never show up in the
          outermost SELECT.
        """
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(
                    formula="customers_v2.lifetime_value:sum", name="ltv",
                ),
                ModelMeasure(
                    formula="cumsum(customers_v2.lifetime_value:sum)",
                    name="running",
                ),
            ],
        )
        planned = plan_query(query=query, bundle=_chain_bundle())

        # (1) One slot, reached by both.
        slots = {s.id: s for s in _all_slots(planned)}
        ltv_sid = next(
            sid for sid in planned.projection
            if slots[sid].public_name == "ltv"
        )
        running_sid = next(
            sid for sid in planned.projection
            if slots[sid].public_name == "running"
        )
        operand_key = slots[running_sid].key.input
        operand_sid = next(
            (sid for sid, s in slots.items() if s.key == operand_key), None,
        )
        assert operand_sid == ltv_sid, (
            f"precondition: the transform's operand must be the SAME slot as "
            f"the projected measure, or this shape does not exercise the "
            f"guard. operand={operand_sid} projected={ltv_sid}"
        )

        # (2) The cross-model path — the guard lives in that builder.
        assert planned.cross_model_aggregate_plans, (
            "precondition: this query must take the cross-model path"
        )
        assert planned.transform_layers, (
            "precondition: this query must carry a transform chain"
        )

        sql = await _gen(query, dialect="postgres")

        # (3) The combined SELECT — the transform chain's ``base`` CTE.
        # ``\bbase`` and not ``base``: the latter also matches the host ``_base``
        # CTE, which is a different scope and carries none of these columns.
        base_body = _extract_cte_body(sql, r"\bbase")
        # Parsed, not regexed: an ``AS "..."`` scan misses a column projected
        # without an alias and would also match a CAST's type name or an alias
        # inside a nested subquery.
        base_aliases = _alias_suffixes(outer_select_aliases(base_body))
        assert base_aliases == ["created_at", "ltv"], (
            f"the combined SELECT should carry exactly the created_at grain and "
            f"one column for the shared slot: {base_aliases}\n{base_body}"
        )
        assert base_aliases.count("ltv") == 1, (
            f"the combined SELECT carries {base_aliases.count('ltv')} columns "
            f"for the shared slot; a leftover would be appended here and then "
            f"trimmed by the outer wrap, invisible from the outermost SELECT.\n"
            f"{base_body}"
        )

        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["created_at", "ltv", "running"], (
            f"expected each declared name once, in declaration order: "
            f"{emitted}\n\n{sql}"
        )

    async def test_hidden_slots_are_absent_from_the_projection(self) -> None:
        """Unified trimming: an order-only aggregate never reaches the public
        projection because it is not in ``projection`` at all."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count", name="n")],
            order=[OrderItem(column="amount:sum", direction="desc")],
        )
        sql = await _gen(query, dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["status", "n"], (
            f"the hidden order-only aggregate leaked into the public "
            f"projection: {emitted}\n\n{sql}"
        )

    async def test_hidden_cross_model_order_slot_is_trimmed_but_orderable(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """A hidden CROSS-MODEL aggregate must still drive ORDER BY while being
        absent from the projection — the case the ``trim_hidden`` flag handled
        and which unified trimming must preserve."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.tier")],
            measures=[ModelMeasure(formula="*:count", name="n")],
            order=[OrderItem(column="customers.spend:sum", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert _alias_suffixes(list(resp.columns)) == ["tier", "n"], resp.columns
        # gold (1000.0) outranks the NULL tier group (325.0).
        assert [r["orders.customers.tier"] for r in resp.data] == ["gold", None], (
            f"hidden cross-model order slot did not drive the ordering: {resp.data}"
        )


# =========================================================================== #
# The projection invariant + its belt.
# =========================================================================== #
class TestProjectionInvariant:

    def test_a_hidden_slot_in_the_projection_is_rejected(self) -> None:
        """Validated at construction: the public projection is public-only."""
        planned = plan_query(
            query=_cm_declared_first_query(), bundle=_chain_bundle(),
        )
        hidden = [
            s for s in list(planned.row_slots) + list(planned.aggregate_slots)
            if s.hidden
        ]
        if not hidden:
            # Synthesise one so the invariant is tested even when this shape
            # happens to have no hidden slot.
            slot = planned.aggregate_slots[0].model_copy(
                update={"id": "hidden_x", "hidden": True,
                        "public_name": None, "public_aliases": []},
            )
            fields = dict(planned.__dict__)
            fields["aggregate_slots"] = list(planned.aggregate_slots) + [slot]
            fields["projection"] = list(planned.projection) + ["hidden_x"]
        else:
            fields = dict(planned.__dict__)
            fields["projection"] = list(planned.projection) + [hidden[0].id]
        with pytest.raises(ValueError, match="(?i)hidden"):
            PlannedQuery(**fields)

    def test_a_slot_may_repeat_once_per_declared_public_name(self) -> None:
        """C13: one key selected under two names IS listed twice, and each
        occurrence consumes the next alias. Pinned so the duplicate check below
        cannot be tightened into rejecting a legitimate plan."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="amount:sum(window='90d')", name="wa"),
                ModelMeasure(formula="amount:sum(window='90d')", name="wb"),
            ],
        )
        planned = plan_query(query=query, bundle=_chain_bundle())
        repeated = [
            sid for sid in set(planned.projection)
            if planned.projection.count(sid) > 1
        ]
        assert repeated, (
            f"expected one slot listed twice for the two names: "
            f"{planned.projection}"
        )
        slot = {s.id: s for s in _all_slots(planned)}[repeated[0]]
        assert len(slot.public_aliases) == planned.projection.count(repeated[0])

    def test_more_occurrences_than_declared_names_is_rejected(self) -> None:
        """One occurrence too many would emit the same column twice under the
        same name — the duplication that the per-occurrence alias cursor in the
        combined projection exists to prevent."""
        planned = plan_query(
            query=_cm_declared_first_query(), bundle=_chain_bundle(),
        )
        fields = dict(planned.__dict__)
        fields["projection"] = list(planned.projection) + [planned.projection[0]]
        with pytest.raises(ValueError, match="(?i)duplicate|public name"):
            PlannedQuery(**fields)

    def test_renderer_belt_catches_a_model_copy_that_skips_validation(
        self,
    ) -> None:
        """Codex D9: ``model_copy(update=…)`` bypasses validators, and rerooting
        uses it — so exactly one renderer-side assertion is kept. It must RAISE,
        never silently skip the offending slot."""
        planned = plan_query(
            query=_cm_declared_first_query(), bundle=_chain_bundle(),
        )
        slots = {
            s.id: s for s in list(planned.row_slots) + list(planned.aggregate_slots)
        }
        victim = planned.projection[0]
        broken_slot = slots[victim].model_copy(
            update={"hidden": True, "public_name": None, "public_aliases": []},
        )
        # Replace the slot ONLY in the collection that owns it. Putting it in
        # both would additionally corrupt slot classification, so the belt
        # could then fire for an unrelated reason and the test would pass
        # without proving anything about hidden-slot detection.
        update = {}
        if any(s.id == victim for s in planned.row_slots):
            update["row_slots"] = [
                broken_slot if s.id == victim else s for s in planned.row_slots
            ]
        else:
            update["aggregate_slots"] = [
                broken_slot if s.id == victim else s
                for s in planned.aggregate_slots
            ]
        corrupted = planned.model_copy(update=update)
        gen = SQLGenerator(dialect="postgres")
        with pytest.raises((AssertionError, ValueError)) as excinfo:
            gen.generate_from_planned(
                planned_query=corrupted, bundle=_chain_bundle(),
            )
        message = str(excinfo.value).lower()
        assert "hidden" in message, (
            "the belt fired, but not for the hidden slot — the message does not "
            f"mention it, so this may be an unrelated failure: {excinfo.value!r}"
        )


# =========================================================================== #
# The ``_wm_`` grain invariant (Codex D1).
# =========================================================================== #
class TestWindowedGrainInvariant:

    def test_windowed_plan_grain_always_contains_the_window_time_dimension(
        self,
    ) -> None:
        """Why the ``_wm_`` join-back can never be an empty-grain CROSS JOIN:
        the planner always includes the window's time dimension in the grain.
        Pinned at plan level so the render path does not need a special case."""
        planned = plan_query(
            query=_windowed_declared_first_query(), bundle=_chain_bundle(),
        )
        assert planned.windowed_aggregate_plans, "expected a windowed plan"
        for wm in planned.windowed_aggregate_plans:
            assert wm.grain_slot_ids, (
                "a windowed plan has an EMPTY grain — the outer join-back would "
                "degenerate to a CROSS JOIN and multiply rows."
            )
            assert wm.window_time_dimension_slot_id in wm.grain_slot_ids, (
                f"the window time dimension {wm.window_time_dimension_slot_id} "
                f"is not part of the grain {wm.grain_slot_ids}."
            )


# =========================================================================== #
# B8 — inner-stage projections in plan order.
# =========================================================================== #
class TestB8InnerStagePlanOrder:
    """Measures are named so alphabetical and declaration order DISAGREE:
    ``zz_running`` is declared before ``aa_total``."""

    def _transform_query(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="cumsum(amount:sum)", name="zz_running"),
                ModelMeasure(formula="amount:avg", name="aa_total"),
            ],
        )

    def _cross_model_transform_query(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="cumsum(amount:sum)", name="zz_running"),
                ModelMeasure(
                    formula="customers_v2.lifetime_value:sum", name="aa_cross",
                ),
            ],
        )

    def _time_shift_query(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="zz_prev"),
                ModelMeasure(formula="amount:avg", name="aa_total"),
            ],
        )

    def _consecutive_periods_query(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(
                    formula="consecutive_periods(amount:sum > 0)", name="zz_streak",
                ),
                ModelMeasure(formula="amount:avg", name="aa_total"),
            ],
        )

    @pytest.mark.parametrize(
        "factory_name",
        [
            "_transform_query",
            "_cross_model_transform_query",
            "_time_shift_query",
            "_consecutive_periods_query",
        ],
    )
    async def test_carried_aliases_follow_base_order_not_alphabetical(
        self, factory_name: str,
    ) -> None:
        """NEW (B8): every downstream stage carries its aliases in the order the
        base stage projects them (plan order), not ``sorted()``.

        The measures are named so the two orders disagree: the base projects
        ``created_at, aa_total, amount_sum`` while ``sorted()`` yields
        ``aa_total, amount_sum, created_at``.
        """
        query = getattr(self, factory_name)()
        sql = await _gen(query, dialect="postgres")
        violations = carry_list_order_violations(sql)
        assert not violations, (
            f"[{factory_name}] inner stage(s) carry aliases alphabetically "
            f"instead of in plan order:\n  " + "\n  ".join(violations)
            + f"\n\n{sql}"
        )

    @pytest.mark.parametrize(
        "factory_name",
        [
            "_transform_query",
            "_cross_model_transform_query",
            "_time_shift_query",
            "_consecutive_periods_query",
        ],
    )
    async def test_no_carried_alias_is_dropped(self, factory_name: str) -> None:
        """Codex D8 — fail closed, at BOTH ends.

        Comparing only the plan against the outermost SELECT would miss the
        failure mode that matters: an intermediate stage dropping a hidden
        input, a time key, or an aggregate operand that a LATER stage still
        references. So this asserts (a) every public alias survives to the
        projection, and (b) no stage omits an alias the next stage reads.
        """
        query = getattr(self, factory_name)()
        sql = await _gen(query, dialect="postgres")
        expected = set(_alias_suffixes(_expected_projection_aliases(query)))
        emitted = set(_alias_suffixes(outer_select_aliases(sql)))
        assert expected <= emitted, (
            f"[{factory_name}] aliases lost between plan and projection: "
            f"{sorted(expected - emitted)}\n\n{sql}"
        )
        drops = carried_alias_drops(sql)
        assert not drops, (
            f"[{factory_name}] an inner stage dropped an alias a later stage "
            f"still references:\n  " + "\n  ".join(drops) + f"\n\n{sql}"
        )

    async def test_local_transform_chain_executes_with_plan_ordered_stages(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """EXECUTED (Codex D5): reordering inner-stage projections must not
        change any value. Monthly totals are 15.0 (10+5) and 27.0 (20+7), so
        the running total is 15.0 then 42.0."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="cumsum(amount:sum)", name="zz_running"),
                ModelMeasure(formula="amount:sum", name="aa_total"),
            ],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.zz_running"] for r in resp.data] == pytest.approx(
            [15.0, 42.0],
        ), f"cumulative sum changed under the reorder: {resp.data}"
        assert [r["orders.aa_total"] for r in resp.data] == pytest.approx(
            [15.0, 27.0],
        ), resp.data

    async def test_cross_model_transform_chain_executes(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """EXECUTED (Codex D5), family 2 of 4: a transform chain that also
        carries a cross-model measure — the chain whose WITH assembly this PR
        rebuilds."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="cumsum(amount:sum)", name="zz_running"),
                ModelMeasure(formula="customers.spend:sum", name="aa_cross"),
            ],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.zz_running"] for r in resp.data] == pytest.approx(
            [15.0, 42.0],
        ), resp.data
        # The scalar cross-model total is the same on every row (no shared grain).
        assert [r["orders.aa_cross"] for r in resp.data] == pytest.approx(
            [1325.0, 1325.0],
        ), resp.data

    async def test_time_shift_chain_executes(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """EXECUTED (Codex D5), family 3 of 4: the ``sjoin_`` carry list."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="zz_prev"),
                ModelMeasure(formula="amount:sum", name="aa_total"),
            ],
        )
        resp = await exec_engine.execute(query)
        by_month = {
            str(r["orders.created_at"])[:7]: (
                r["orders.aa_total"], r["orders.zz_prev"],
            )
            for r in resp.data
        }
        assert by_month["2024-01"][0] == pytest.approx(15.0), resp.data
        assert by_month["2024-02"][0] == pytest.approx(27.0), resp.data
        # February's previous month is January's total; January has no
        # predecessor in the seeded data.
        assert by_month["2024-02"][1] == pytest.approx(15.0), (
            f"time_shift did not read the previous month: {resp.data}"
        )
        assert by_month["2024-01"][1] is None, resp.data

    async def test_consecutive_periods_chain_executes(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """EXECUTED (Codex D5), family 4 of 4: the ``cp_reset_`` carry list.
        Both seeded months are positive, so the streak runs 1 then 2."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(
                    formula="consecutive_periods(amount:sum > 0)", name="zz_streak",
                ),
                ModelMeasure(formula="amount:sum", name="aa_total"),
            ],
        )
        resp = await exec_engine.execute(query)
        assert [r["orders.zz_streak"] for r in resp.data] == [1, 2], (
            f"consecutive-period streak changed under the reorder: {resp.data}"
        )


# =========================================================================== #
# B11 — one deterministic FROM-join ordering mechanism.
# =========================================================================== #
class TestB11JoinOrdering:

    async def test_join_set_is_unchanged_for_a_projected_join(self) -> None:
        sql = await _gen(
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="customers_v2.status")],
                measures=[ModelMeasure(formula="amount:sum")],
            ),
            dialect="postgres",
        )
        assert _join_aliases(sql) == {"customers_v2"}, _join_aliases(sql)

    async def test_join_discovered_only_by_a_filter_is_still_emitted(self) -> None:
        """The late-registration case (Codex D6): a join that no projection
        mentions — it is discovered while rendering the filter — must survive
        the switch to registration-order collection."""
        sql = await _gen(
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum")],
                filters=["customers_v2.status == 'active'"],
            ),
            dialect="postgres",
        )
        assert "customers_v2" in _join_aliases(sql), (
            f"the filter-only join was dropped:\n{sql}"
        )

    async def test_filter_on_a_joined_derived_column_pulls_the_deeper_hop(
        self,
    ) -> None:
        """Regression: filtering on a JOINED model's crossing derived column.

        ``customers_v2.deep_pop`` is declared on the joined model as
        ``regions.population``. Both the filter renderer and the join scanner
        expanded it as if ``customers_v2`` were the query root, so the ref came
        out bare (``regions.population``) — which the scanner could not match to
        a join path, leaving the hop unjoined and the filter pointing at a table
        that is not in the FROM::

            FROM orders AS orders_x LEFT JOIN customers AS customers_v2 ...
            WHERE regions.population > 0        -- no such table

        Both sites now expand with ``is_root=False``, so the ref resolves to the
        full path alias and the scanner registers the hop. The two must stay in
        lockstep: discovery scans the SAME expansion the renderer emits.
        """
        sql = await _gen(
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum")],
                filters=["customers_v2.deep_pop > 0"],
            ),
            dialect="postgres",
        )
        assert "customers_v2__regions" in _join_aliases(sql), (
            f"the deeper hop was never joined:\n{sql}"
        )
        assert "customers_v2__regions.population" in sql, (
            f"the derived ref was not qualified to its path alias:\n{sql}"
        )

    async def test_multi_hop_joins_emit_parent_before_child(self) -> None:
        """A three-hop path must emit every hop, each after its parent — the
        structural requirement ANY ordering must satisfy, asserted over the
        full emitted sequence rather than one adjacent pair."""
        sql = await _gen(
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="customers_v2.deep_gdp")],
                measures=[ModelMeasure(formula="amount:sum")],
            ),
            dialect="postgres",
        )
        expected = [
            "customers_v2",
            "customers_v2__regions",
            "customers_v2__regions__countries",
        ]
        assert set(expected) <= _join_aliases(sql), _join_aliases(sql)
        sequence = base_cte_join_sequence(sql)
        positions = [sequence.index(name) for name in expected]
        assert positions == sorted(positions), (
            f"hops are not emitted parent-before-child: {sequence}\n\n{sql}"
        )

    async def test_generation_is_deterministic(self) -> None:
        """Whatever the order is, it must be the SAME order every run — a set
        would make emitted SQL vary between processes."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[
                ColumnRef(name="customers_v2.status"),
                ColumnRef(name="customers_v2.deep_pop"),
            ],
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["customers_v2.status != 'x'"],
        )
        first = await _gen(query, dialect="postgres")
        second = await _gen(query, dialect="postgres")
        assert first == second, "emitted SQL is not deterministic across runs"

    async def test_mixed_projection_and_filter_joins_all_present(self) -> None:
        """The shape where the two tiers of the old merge differ: the two-hop
        ``regions`` join arrives via a projected dimension while the one-hop
        ``customers_v2`` join is also named by a filter. Both must be emitted
        exactly once, whichever tier discovered them."""
        sql = await _gen(
            SlayerQuery(
                source_model="orders_x",
                dimensions=[ColumnRef(name="customers_v2.deep_pop")],
                measures=[ModelMeasure(formula="amount:sum")],
                filters=["customers_v2.status != 'x'"],
            ),
            dialect="postgres",
        )
        assert {"customers_v2", "customers_v2__regions"} <= _join_aliases(sql), sql
        assert sql.count("LEFT JOIN regions AS customers_v2__regions") == 1, (
            f"the regions hop was emitted more than once:\n{sql}"
        )

    async def test_executed_multi_join_query_is_correct(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """EXECUTED (Codex D5): join reordering is only safe if the rows are
        unchanged. Region 2's name is NULL, so this also covers a nullable
        two-hop dimension."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.regions.name")],
            measures=[ModelMeasure(formula="amount:sum", name="revenue")],
        )
        resp = await exec_engine.execute(query)
        by_region = {
            r["orders.customers.regions.name"]: r["orders.revenue"]
            for r in resp.data
        }
        # Orders 1,2 -> customer 100 -> region 1 ("West"): 10 + 20 = 30.
        # Orders 3,4 -> customer 101 -> region 2 (NULL):    5 +  7 = 12.
        assert by_region.get("West") == pytest.approx(30.0), resp.data
        assert by_region.get(None) == pytest.approx(12.0), resp.data

    async def test_join_order_is_stable_across_dialects(self) -> None:
        """One mechanism means the relative join order does not depend on the
        dialect — only identifier quoting does.

        The sequence is read out of the parsed JOIN nodes. Building it by
        testing known names for membership in the SQL string would return the
        *caller's* ordering every time and assert nothing.
        """
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.deep_pop")],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        sequences = {}
        for dialect in ("postgres", "sqlite", "duckdb"):
            sql = await _engine_generate(
                query=query, model=_orders_x(), dialect=dialect,
                extra_models=[_customers_v2(), _regions(), _countries()],
            )
            sequences[dialect] = base_cte_join_sequence(sql, dialect=dialect)
        assert all(seq for seq in sequences.values()), (
            f"no joins were found to compare: {sequences}"
        )
        assert len(set(map(tuple, sequences.values()))) == 1, sequences

    async def test_every_registered_scope_path_is_emitted_as_a_join(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """B11's safety property, stated in terms of the mechanism it adopts.

        Whatever the host scope registers must end up in the FROM. This is the
        half of B11 that can regress: switching the collector to
        ``join_paths.as_list()`` changes which list is authoritative, and a path
        present in one list but not the other becomes a missing join — invalid
        SQL rather than a cosmetic reordering.

        NOTE ON WHY THE *ORDER* IS NOT PINNED HERE. Today the host scope is not
        consulted at all for a plain joined dimension: ``ScopeFrame.resolve``
        is never called for that shape, and the dimension paths come from the
        row-slot walk. So the new ordering cannot be asserted against the scope
        before the implementation exists without presuming its internals — and
        the ratified statement for B11 is *execution-identical*, with the order
        change itself surfaced through the PR's recompare corpus. What is
        pinned here instead are the properties that must hold either way: the
        join SET, parent-before-child, determinism, and executed results.
        """
        registered: List[str] = []
        original = ScopeFrame._register_join_paths

        def _wrapped(self_frame, parsed):
            result = original(self_frame, parsed)
            registered.extend(
                "__".join(p) for p in self_frame.join_paths.as_list()
            )
            return result

        monkeypatch.setattr(
            ScopeFrame, "_register_join_paths", _wrapped, raising=True,
        )
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.status")],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            filters=["customers_v2.status != 'x'"],
        )
        sql = await _gen(query, dialect="postgres")
        emitted = set(_join_aliases(sql))
        missing = {p for p in registered if p not in emitted}
        assert not missing, (
            f"path(s) {sorted(missing)} were registered on a scope but never "
            f"emitted as joins — the FROM would reference an unbound table.\n\n"
            f"{sql}"
        )
