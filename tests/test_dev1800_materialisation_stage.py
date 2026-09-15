"""DEV-1800 plan structure — the planner assigns every value one materialisation
stage (``BASE < PRODUCER < COMBINED < CHAIN(level) < POST``) and a needs-column
flag, and rejects a plan that references a later stage or leaves a value
unstaged. This is the harness for engine.arc42 P6 (phase/stage is a property of
the value, not re-derived from text at render time).

Stages are inspected off the ``PlannedQuery`` that ``plan_query`` produces, so
the assertions bind the planner's *output*, not any internal function name. The
detailed re-aggregation-vs-series SQL regime per shape is pinned by
``tests/test_dev1800_golden_sql.py`` (shifted-CTE vs window, byte for byte);
here ``series`` is checked only for the two anchor shapes.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.errors import MaterialisationStageError
from slayer.core.keys import (
    REGROUP_LEAF_PREFIX,
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    Phase,
    StarKey,
    TransformKey,
)
from slayer.engine.compile.projection import _iter_slot_deps
from slayer.engine.plan import plan_query
from slayer.ir.planned import PlannedQuery, Stage, StageKind, ValueSlot
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import generate_from_planned as render_planned

from tests._dev1800_fixtures import ModelMeasure, SlayerQuery, dev1800_models, signup_td

CM = "customers.spend:sum"


# --------------------------------------------------------------------------- #
# Planning + slot-walking helpers.
# --------------------------------------------------------------------------- #
def _bundle() -> ResolvedSourceBundle:
    models = dev1800_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _plan(*, source_model: str = "orders", **kw) -> PlannedQuery:
    kw.setdefault("time_dimensions", signup_td())
    return plan_query(query=SlayerQuery(source_model=source_model, **kw), bundle=_bundle())


def _all_slots(pq: PlannedQuery, *, recurse: bool = True) -> list:
    slots = list(pq.row_slots) + list(pq.aggregate_slots) + list(pq.combined_expression_slots)
    if recurse:
        for ap in pq.regroup_attach_plans:
            slots += _all_slots(ap.producer_plan, recurse=True)
    return slots


def _dep_slots(slot: ValueSlot, by_key: dict) -> list:
    out = []
    for dep_key in _iter_slot_deps(slot.key):
        if dep_key == slot.key:
            continue
        dep = by_key.get(dep_key)
        if dep is not None:
            out.append(dep)
    return out


def _transform_slots(pq: PlannedQuery) -> list:
    return [s for s in _all_slots(pq) if isinstance(s.key, TransformKey)]


def _composite_slots(pq: PlannedQuery) -> list:
    return [s for s in _all_slots(pq) if isinstance(s.key, ArithmeticKey)]


# A spread of shapes the ordering/staging invariants must hold across.
_INVARIANT_QUERIES = [
    dict(measures=[ModelMeasure(formula="amount:sum", name="a")]),
    dict(measures=[ModelMeasure(formula="change(amount:sum)", name="c")]),
    dict(measures=[ModelMeasure(formula="change(cumsum(amount:sum))", name="n")]),
    dict(measures=[ModelMeasure(formula=CM, name="s")]),
    dict(measures=[ModelMeasure(formula=f"{CM} + amount:sum", name="c")]),
    dict(measures=[ModelMeasure(formula=f"change({CM}) + amount:sum", name="c")]),
    dict(measures=[ModelMeasure(formula="consecutive_periods(amount:sum > 0)", name="p")]),
]


# --------------------------------------------------------------------------- #
# The Stage value type.
# --------------------------------------------------------------------------- #
class TestStageType:
    def test_kinds_exist(self) -> None:
        assert {k.name for k in StageKind} >= {"BASE", "PRODUCER", "COMBINED", "CHAIN", "POST"}

    def test_total_order(self) -> None:
        ascending = [
            Stage(kind=StageKind.BASE),
            Stage(kind=StageKind.PRODUCER),
            Stage(kind=StageKind.COMBINED),
            Stage(kind=StageKind.CHAIN, level=1),
            Stage(kind=StageKind.CHAIN, level=2),
            Stage(kind=StageKind.POST),
        ]
        for lo, hi in zip(ascending, ascending[1:]):
            assert lo < hi
            assert not (hi < lo)

    def test_frozen_hashable(self) -> None:
        assert Stage(kind=StageKind.BASE) == Stage(kind=StageKind.BASE)
        assert len({Stage(kind=StageKind.CHAIN, level=1), Stage(kind=StageKind.CHAIN, level=1)}) == 1


# --------------------------------------------------------------------------- #
# Every planned value carries exactly one stage (cross-model-aggregates spec).
# --------------------------------------------------------------------------- #
class TestEveryValueCarriesOneStage:
    @pytest.mark.parametrize("kw", _INVARIANT_QUERIES)
    def test_all_slots_staged_including_producers(self, kw) -> None:
        pq = _plan(**kw)
        for slot in _all_slots(pq, recurse=True):
            assert slot.stage is not None, f"{slot.id} ({type(slot.key).__name__}) is unstaged"
            assert isinstance(slot.stage, Stage)


# --------------------------------------------------------------------------- #
# Ordering invariants (both delta specs).
# --------------------------------------------------------------------------- #
class TestStageOrderingInvariants:
    @pytest.mark.parametrize("kw", _INVARIANT_QUERIES)
    def test_transform_strictly_later_than_its_inputs(self, kw) -> None:
        pq = _plan(**kw)
        by_key = {s.key: s for s in _all_slots(pq)}
        for tslot in _transform_slots(pq):
            for dep in _dep_slots(tslot, by_key):
                assert dep.stage < tslot.stage, (
                    f"transform {tslot.id} not strictly later than input {dep.id}")

    @pytest.mark.parametrize("kw", _INVARIANT_QUERIES)
    def test_composite_never_earlier_than_an_operand(self, kw) -> None:
        pq = _plan(**kw)
        by_key = {s.key: s for s in _all_slots(pq)}
        for cslot in _composite_slots(pq):
            for dep in _dep_slots(cslot, by_key):
                assert not (cslot.stage < dep.stage), (
                    f"composite {cslot.id} earlier than operand {dep.id}")

    def test_composite_over_a_transform_is_post(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula=f"change({CM}) + amount:sum", name="c")])
        by_key = {s.key: s for s in _all_slots(pq)}
        composites_with_chain_dep = [
            c for c in _composite_slots(pq)
            if any(d.stage.kind == StageKind.CHAIN for d in _dep_slots(c, by_key))
        ]
        assert composites_with_chain_dep
        for c in composites_with_chain_dep:
            assert c.stage.kind == StageKind.POST


# --------------------------------------------------------------------------- #
# Concrete stage assignments (empirically anchored shapes).
# --------------------------------------------------------------------------- #
class TestConcreteStages:
    def test_local_aggregate_is_base(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")], time_dimensions=[])
        [agg] = [s for s in pq.aggregate_slots if isinstance(s.key, AggregateKey)]
        assert agg.stage.kind == StageKind.BASE

    def test_time_shift_inner_is_chain_level_one(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="change(amount:sum)", name="c")])
        [ts] = [s for s in _transform_slots(pq) if s.key.op == "time_shift"]
        assert ts.stage.kind == StageKind.CHAIN
        assert ts.stage.level == 1

    def test_nested_transform_reaches_chain_level_two(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="change(cumsum(amount:sum))", name="n")])
        levels = {s.stage.level for s in _transform_slots(pq)}
        assert levels == {1, 2}

    def test_combined_attach_placeholder_is_producer(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula=f"{CM} + amount:sum", name="c")])
        [attach] = pq.regroup_attach_plans
        assert attach.attach_phase == "combined"
        placeholders = [
            s for s in _all_slots(pq)
            if isinstance(s.key, ColumnKey) and s.key.leaf.startswith(REGROUP_LEAF_PREFIX)
        ]
        assert placeholders
        for ph in placeholders:
            assert ph.stage.kind == StageKind.PRODUCER


# --------------------------------------------------------------------------- #
# needs_column — operands needed later are materialised as columns (spec).
# --------------------------------------------------------------------------- #
class TestNeedsColumn:
    def test_hidden_operand_of_failing_composite_is_projected(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula=f"change({CM}) + amount:sum", name="c")])
        hidden_local = [
            s for s in pq.aggregate_slots
            if isinstance(s.key, AggregateKey) and s.hidden
        ]
        assert hidden_local, "the hidden local operand should exist"
        for s in hidden_local:
            assert s.needs_column is True
        placeholders = [
            s for s in _all_slots(pq)
            if isinstance(s.key, ColumnKey) and s.key.leaf.startswith(REGROUP_LEAF_PREFIX)
        ]
        for ph in placeholders:
            assert ph.needs_column is True

    def test_projected_measure_needs_a_column(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")], time_dimensions=[])
        [agg] = [s for s in pq.aggregate_slots if isinstance(s.key, AggregateKey)]
        assert agg.needs_column is True


# --------------------------------------------------------------------------- #
# time_shift regime is a planner fact (D6). The full matrix is golden-pinned;
# here the two anchor shapes only.
# --------------------------------------------------------------------------- #
class TestSeriesRegime:
    def test_local_change_is_reaggregation(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="change(amount:sum)", name="c")])
        [ts] = [s for s in _transform_slots(pq) if s.key.op == "time_shift"]
        assert ts.series is False

    def test_predicate_uses_series(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="consecutive_periods(amount:sum > 0)", name="p")])
        [cp] = [s for s in _transform_slots(pq) if s.key.op == "consecutive_periods"]
        assert cp.series is True


# --------------------------------------------------------------------------- #
# The strict validator: a plan referencing a later stage, or leaving a value
# unstaged (recursively), is rejected before any SQL is generated.
# --------------------------------------------------------------------------- #
def _agg_key(col: str = "amount") -> AggregateKey:
    return AggregateKey(source=ColumnKey(path=(), leaf=col), agg="sum")


class TestValidatorRejects:
    def test_later_stage_reference_is_rejected(self) -> None:
        agg = _agg_key()
        arith = ArithmeticKey(op="+", operands=(agg, LiteralKey(value=1)))
        agg_slot = ValueSlot(id="a", key=agg, declared_name="amount_sum", hidden=True,
                             phase=Phase.AGGREGATE, stage=Stage(kind=StageKind.POST), needs_column=True)
        arith_slot = ValueSlot(id="c", key=arith, declared_name="c", public_name="c",
                               public_aliases=["c"], phase=Phase.AGGREGATE, stage=Stage(kind=StageKind.BASE))
        with pytest.raises((MaterialisationStageError, ValidationError)) as ei:
            PlannedQuery(source_relation="orders", aggregate_slots=[agg_slot, arith_slot], projection=["c"])
        assert "MaterialisationStageError" in str(ei.value) or isinstance(ei.value, MaterialisationStageError)

    def test_unstaged_slot_is_rejected(self) -> None:
        agg_slot = ValueSlot(id="a", key=_agg_key(), declared_name="amount_sum",
                             public_name="amount_sum", public_aliases=["amount_sum"],
                             phase=Phase.AGGREGATE, stage=None)
        with pytest.raises((MaterialisationStageError, ValidationError)):
            PlannedQuery(source_relation="orders", aggregate_slots=[agg_slot], projection=["a"])

    def test_unstaged_nested_producer_is_rejected(self) -> None:
        from slayer.ir.planned import RegroupAttachPlan

        unstaged = ValueSlot(id="p1", key=_agg_key("spend"), declared_name="spend_sum",
                             public_name="spend_sum", public_aliases=["spend_sum"],
                             phase=Phase.AGGREGATE, stage=None)
        producer = PlannedQuery.model_construct(
            source_relation="customers", row_slots=[], aggregate_slots=[unstaged],
            combined_expression_slots=[], regroup_attach_plans=[], transform_layers=[],
            masks=[], projection=["p1"], order=[],
        )
        host_agg = ValueSlot(id="a", key=_agg_key(), declared_name="amount_sum",
                             public_name="amount_sum", public_aliases=["amount_sum"],
                             phase=Phase.AGGREGATE, stage=Stage(kind=StageKind.BASE))
        with pytest.raises((MaterialisationStageError, ValidationError)):
            PlannedQuery(
                source_relation="orders", aggregate_slots=[host_agg],
                regroup_attach_plans=[RegroupAttachPlan(producer_plan=producer, alias_hint="p")],
                projection=["a"],
            )


class TestGeneratorBelt:
    def test_belt_refuses_a_plan_unstaged_via_model_copy_bypass(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")], time_dimensions=[])
        target = pq.aggregate_slots[0]
        bad = pq.model_copy(update={
            "aggregate_slots": [target.model_copy(update={"stage": None})]
        })
        with pytest.raises(MaterialisationStageError):
            render_planned(bad, bundle=_bundle(), dialect="postgres")


# --------------------------------------------------------------------------- #
# Slot-dependency traversal contract the staging pass relies on.
# --------------------------------------------------------------------------- #
class TestTraversalContract:
    def test_opaque_kind_raises(self) -> None:
        with pytest.raises(TypeError):
            list(_iter_slot_deps(object()))

    def test_aggregate_is_terminal(self) -> None:
        agg = _agg_key()
        assert list(_iter_slot_deps(agg)) == [agg]

    def test_star_and_literal_yield_no_slot(self) -> None:
        assert list(_iter_slot_deps(StarKey())) == []
        assert list(_iter_slot_deps(LiteralKey(value=1))) == []

    def test_arithmetic_recurses_into_operands(self) -> None:
        agg = _agg_key()
        arith = ArithmeticKey(op="+", operands=(agg, LiteralKey(value=1)))
        assert list(_iter_slot_deps(arith)) == [agg]

    def test_time_trunc_is_the_slot_not_its_column(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")])
        tt_slots = [s for s in pq.row_slots if type(s.key).__name__ == "TimeTruncKey"]
        assert tt_slots
        for s in tt_slots:
            assert list(_iter_slot_deps(s.key)) == [s.key]
