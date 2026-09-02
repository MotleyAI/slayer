"""One ordered public projection consumed by every renderer (B7/B8/B11)."""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.keys import TransformKey
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


def _cm_declared_first_query() -> SlayerQuery:
    """A cross-model measure declared BEFORE a local one; B7 keeps its position."""
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
    """Every slot a projection id can point at (row + aggregate + combined-expression)."""
    return (
        list(planned.row_slots)
        + list(planned.aggregate_slots)
        + list(planned.combined_expression_slots)
    )


def _expected_projection_aliases(query: SlayerQuery) -> List[str]:
    """The public aliases the plan declares, in plan order (derived from ``PlannedQuery.projection``, not hard-coded)."""
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
    """Emitted aliases are host-rooted (``orders_x.status``); compare on the trailing public part."""
    return [a.split(".")[-1] for a in aliases]


class TestB7DeclarationOrderProjection:

    async def test_cross_model_measure_keeps_its_declared_position(self) -> None:
        sql = await _gen(_cm_declared_first_query(), dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["status", "cm_first", "local_second"], (
            f"emitted projection order {emitted} is not declaration order — the "
            f"cross-model measure was grouped after the host slots.\n\n{sql}"
        )

    async def test_windowed_measure_keeps_its_declared_position(self) -> None:
        sql = await _gen(_windowed_declared_first_query(), dialect="postgres")
        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["status", "created_at", "w_first", "local_second"], (
            f"emitted projection order {emitted} is not declaration order.\n\n{sql}"
        )

    async def test_interleaved_local_and_cross_model_measures(self) -> None:
        """The decisive shape: local, cross-model, local — only walking the plan's list emits this order."""
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
        """``response.columns`` is read off the emitted outer SELECT, so its order follows the projection."""
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
        assert _alias_suffixes(list(resp.data[0].keys())) == [
            "status", "cm_first", "local_second",
        ], f"row keys: {list(resp.data[0].keys())}"

    async def test_a_slot_that_is_both_projected_and_a_transform_operand(
        self,
    ) -> None:
        """A slot both projected and consumed as a transform operand exercises the combined-SELECT leftover guard; the three preconditions are asserted below."""
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
        assert any(
            a.producer_root_model == "customers_v2"
            for a in planned.regroup_attach_plans
        ), "precondition: this query must take the cross-model path"
        assert planned.transform_layers, (
            "precondition: this query must carry a transform chain"
        )

        # The transform slot is projected but rendered by a later step CTE, so the combined loop skips it (no rendered columns) rather than flagging a drop.
        assert running_sid in planned.projection, planned.projection
        assert isinstance(slots[running_sid].key, TransformKey), (
            f"expected a transform slot, got {type(slots[running_sid].key).__name__}"
        )

        sql = await _gen(query, dialect="postgres")

        # (3) The combined SELECT (the chain's ``base`` CTE); ``\bbase`` avoids matching the host ``_base``.
        base_body = _extract_cte_body(sql, r"\bbase")
        base_aliases = _alias_suffixes(outer_select_aliases(base_body))
        assert base_aliases == ["created_at", "ltv"], (
            f"the combined SELECT must carry the grain and exactly ONE column "
            f"for the shared slot: {base_aliases}. A leftover would be appended "
            f"here and then trimmed by the outer wrap, invisible from the "
            f"outermost SELECT.\n{base_body}"
        )

        emitted = _alias_suffixes(outer_select_aliases(sql))
        assert emitted == ["created_at", "ltv", "running"], (
            f"expected each declared name once, in declaration order: "
            f"{emitted}\n\n{sql}"
        )

    @pytest.mark.parametrize("direction", ["asc", "desc"])
    async def test_combined_order_by_suppresses_the_tsql_nulls_emulation(
        self, direction: str,
    ) -> None:
        """The combined ORDER BY must go through ``_ordered`` to pin ``nulls_first`` and suppress the T-SQL NULLS-emulation CASE wrapper (whose alias mis-resolves); the wrapper appears on ASC only."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=[ColumnRef(name="customers_v2.status")],
            measures=[ModelMeasure(
                formula="customers_v2.lifetime_value:sum", name="ltv",
            )],
            order=[OrderItem(
                column="customers_v2.lifetime_value:sum", direction=direction,
            )],
        )
        sql = await _gen(query, dialect="tsql")
        assert "CASE WHEN" not in sql.upper(), (
            f"[{direction}] the T-SQL NULLS-emulation wrapper is back; its "
            f"bracketed alias does not resolve at the ORDER BY scope:\n{sql}"
        )

    async def test_hidden_slots_are_absent_from_the_projection(self) -> None:
        """Unified trimming: an order-only aggregate never reaches the public projection — it is simply not in ``projection``."""
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
        """A hidden cross-model aggregate must drive ORDER BY while absent from the projection."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.tier")],
            measures=[ModelMeasure(formula="*:count", name="n")],
            order=[OrderItem(column="customers.spend:sum", direction="desc")],
        )
        resp = await exec_engine.execute(query)
        assert _alias_suffixes(list(resp.columns)) == ["tier", "n"], resp.columns
        assert [r["orders.customers.tier"] for r in resp.data] == ["gold", None], (
            f"hidden cross-model order slot did not drive the ordering: {resp.data}"
        )


class TestProjectionInvariant:

    def test_a_hidden_slot_in_the_projection_is_rejected(self) -> None:
        planned = plan_query(
            query=_cm_declared_first_query(), bundle=_chain_bundle(),
        )
        hidden = [
            s for s in list(planned.row_slots) + list(planned.aggregate_slots)
            if s.hidden
        ]
        if not hidden:
            # Synthesise a hidden slot so the invariant is tested even when this shape has none.
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
        """One key selected under two names is listed twice, each consuming the next alias — a legitimate plan the duplicate check must not reject."""
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
        """One occurrence too many would emit the same column twice under the same name — rejected."""
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
        """``model_copy(update=…)`` bypasses validators (rerooting uses it), so one renderer-side belt must RAISE on a hidden slot in the projection."""
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
        # Replace the slot ONLY in the collection that owns it, or the belt could fire for an unrelated reason.
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
        bundle = _chain_bundle()
        gen = SQLGenerator(dialect="postgres")
        with pytest.raises((AssertionError, ValueError)) as excinfo:
            gen.generate_from_planned(planned_query=corrupted, bundle=bundle)
        message = str(excinfo.value).lower()
        assert "hidden" in message, (
            "the belt fired, but not for the hidden slot — the message does not "
            f"mention it, so this may be an unrelated failure: {excinfo.value!r}"
        )


class TestWindowedGrainInvariant:

    def test_windowed_plan_grain_always_contains_the_window_time_dimension(
        self,
    ) -> None:
        """The planner always includes the window's time dimension in the grain, so the windowed join-back is never an empty-grain CROSS JOIN (pinned on the attach)."""
        planned = plan_query(
            query=_windowed_declared_first_query(), bundle=_chain_bundle(),
        )
        windowed_attaches = [
            rp for rp in planned.regroup_attach_plans
            if rp.kernel.kind == "trailing-window"
        ]
        assert windowed_attaches, "expected a trailing-window kernel producer"
        for attach in windowed_attaches:
            assert attach.join_pairs, (
                "a windowed producer has an EMPTY grain — the outer join-back "
                "would degenerate to a CROSS JOIN and multiply rows."
            )
            grain_slot_ids = {sid for _, sid in attach.join_pairs}
            assert attach.kernel.bucket_slot_id in grain_slot_ids, (
                f"the window time dimension {attach.kernel.bucket_slot_id} "
                f"is not part of the grain {grain_slot_ids}."
            )


class TestB8InnerStagePlanOrder:
    """Measures named so alphabetical and declaration order disagree (``zz_running`` before ``aa_total``)."""

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
        """Every downstream stage carries its aliases in the base's plan order, not ``sorted()`` (the measure names make the two disagree)."""
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
        """Fail closed at both ends: every public alias survives to the projection, and no stage omits an alias the next stage reads."""
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
        """Executed: reordering inner-stage projections must not change values — running total 15.0 then 42.0."""
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
        """Executed: a transform chain that also carries a cross-model measure."""
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
        assert by_month["2024-02"][1] == pytest.approx(15.0), (
            f"time_shift did not read the previous month: {resp.data}"
        )
        assert by_month["2024-01"][1] is None, resp.data

    async def test_consecutive_periods_chain_executes(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """Executed: the ``cp_reset_`` carry list — both months positive, so the streak runs 1 then 2."""
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
        """A join no projection mentions — discovered while rendering the filter — must survive the switch to registration-order collection."""
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
        """Filtering on a joined model's crossing derived column: both the filter renderer and the join scanner must expand with ``is_root=False`` so the hop is joined."""
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
        """A three-hop path must emit every hop, each after its parent — asserted over the full sequence."""
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
        """Whatever the order, it must be the same every run — a set would vary between processes."""
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
        """A join arriving via a projected dimension and one named by a filter must each be emitted exactly once."""
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
        """Executed: join reordering is only safe if rows are unchanged; region 2's NULL name covers a nullable two-hop dimension."""
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
        assert by_region.get("West") == pytest.approx(30.0), resp.data
        assert by_region.get(None) == pytest.approx(12.0), resp.data

    async def test_join_order_is_stable_across_dialects(self) -> None:
        """Relative join order does not depend on the dialect, only quoting; the sequence is read from the parsed JOIN nodes, not string membership."""
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
        """Whatever the host scope registers must end up in the FROM; a path in one list but not the other becomes a missing join (order is not pinned — B11 is execution-identical)."""
        registered: List[str] = []
        original = ScopeFrame._register_join_paths

        def _wrapped(self_frame, parsed):
            result = original(self_frame, parsed)
            # Only the HOST scope; throwaway frames' join_paths are intentionally discarded and need not appear in the FROM.
            if self_frame.root_relation == "orders_x":
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
