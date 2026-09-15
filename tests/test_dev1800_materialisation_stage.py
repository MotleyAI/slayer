"""DEV-1800 plan structure — the planner assigns every value one materialisation
stage (``BASE < PRODUCER < COMBINED < DERIVED(level)``) and a needs-column
flag, and rejects a plan that references a later stage or leaves a value
unstaged. This is the harness for engine.arc42 P6 (phase/stage is a property of
the value, not re-derived from text at render time).

Only transforms stratify (design D10/D11): a value reading a transform sits one
level above the deepest transform it reads, composites rendering inline — so a
value's stage is a function of its term alone.

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


def _transforms_read(key, by_key: dict) -> list:
    """Transform slots ``key`` reads, descending through composites whether or
    not interned; a transform or aggregate is terminal (D3 traversal)."""
    out: list = []

    def visit(k) -> None:
        if isinstance(k, TransformKey):
            slot = by_key.get(k)
            if slot is not None:
                out.append(slot)
            return
        if isinstance(k, AggregateKey):
            return
        for child in k.children():
            visit(child)

    for child in key.children():
        visit(child)
    return out


# A spread of shapes the ordering/staging invariants must hold across —
# measures, filter predicates and ORDER-BY keys, since every value the plan
# carries (mask and order slots included) must be staged.
_INVARIANT_QUERIES = [
    dict(measures=[ModelMeasure(formula="amount:sum", name="a")]),
    dict(measures=[ModelMeasure(formula="change(amount:sum)", name="c")]),
    dict(measures=[ModelMeasure(formula="change(cumsum(amount:sum))", name="n")]),
    dict(measures=[ModelMeasure(formula=CM, name="s")]),
    dict(measures=[ModelMeasure(formula=f"{CM} + amount:sum", name="c")]),
    dict(measures=[ModelMeasure(formula=f"change({CM}) + amount:sum", name="c")]),
    dict(measures=[ModelMeasure(formula="consecutive_periods(amount:sum > 0)", name="p")]),
    # filter predicate over a transform composite (a mask slot must be staged)
    dict(measures=[ModelMeasure(formula="amount:sum", name="a")],
         filters=[f"change({CM}) + amount:sum > 0"]),
    # a row-typed and a measure-typed mask together
    dict(measures=[ModelMeasure(formula="amount:sum", name="a")],
         filters=["amount > 5", "amount:sum > 50"]),
    # ORDER-BY key over a transform composite (an order slot must be staged)
    dict(measures=[ModelMeasure(formula="amount:sum", name="a")],
         order=[{"column": f"change({CM}) + amount:sum", "direction": "desc"}]),
    # the alternation class (D10): a transform over a composite over a transform,
    # beside the projected composite — the dev1859 stall shape
    dict(measures=[ModelMeasure(formula="change(amount:sum)", name="ch")],
         filters=["last(change(amount:sum)) < 0"]),
    dict(measures=[ModelMeasure(formula="last(change(cumsum(amount:sum)))", name="t")]),
]


# --------------------------------------------------------------------------- #
# The Stage value type.
# --------------------------------------------------------------------------- #
class TestStageType:
    def test_kinds_exist(self) -> None:
        # exact: CHAIN and POST are gone (D10 — one leveled derived stage)
        assert {k.name for k in StageKind} == {"BASE", "PRODUCER", "COMBINED", "DERIVED"}

    def test_total_order(self) -> None:
        ascending = [
            Stage(kind=StageKind.BASE),
            Stage(kind=StageKind.PRODUCER),
            Stage(kind=StageKind.COMBINED),
            Stage(kind=StageKind.DERIVED, level=1),
            Stage(kind=StageKind.DERIVED, level=2),
        ]
        for lo, hi in zip(ascending, ascending[1:]):
            assert lo < hi
            assert not (hi < lo)

    def test_frozen_hashable(self) -> None:
        assert Stage(kind=StageKind.BASE) == Stage(kind=StageKind.BASE)
        assert len({Stage(kind=StageKind.DERIVED, level=1), Stage(kind=StageKind.DERIVED, level=1)}) == 1


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
    def test_value_strictly_later_than_every_transform_it_reads(self, kw) -> None:
        pq = _plan(**kw)
        by_key = {s.key: s for s in _all_slots(pq)}
        for slot in _all_slots(pq):
            for t in _transforms_read(slot.key, by_key):
                assert t.stage < slot.stage, (
                    f"{slot.id} not strictly later than transform {t.id} it reads")

    @pytest.mark.parametrize("kw", _INVARIANT_QUERIES)
    def test_no_value_earlier_than_any_dependency(self, kw) -> None:
        pq = _plan(**kw)
        by_key = {s.key: s for s in _all_slots(pq)}
        for slot in _all_slots(pq):
            for dep in _dep_slots(slot, by_key):
                assert not (slot.stage < dep.stage), (
                    f"{slot.id} earlier than its dependency {dep.id}")

    def test_composite_over_a_transform_is_one_level_above_it(self) -> None:
        """A composite is not a relation boundary (D10): it sits one level above
        the deepest transform it reads, never in a terminal stage of its own."""
        pq = _plan(measures=[ModelMeasure(formula=f"change({CM}) + amount:sum", name="c")])
        by_key = {s.key: s for s in _all_slots(pq)}
        composites = [c for c in _composite_slots(pq) if _transforms_read(c.key, by_key)]
        assert composites
        for c in composites:
            deepest = max(t.stage.level for t in _transforms_read(c.key, by_key))
            assert c.stage == Stage(kind=StageKind.DERIVED, level=1 + deepest)


# --------------------------------------------------------------------------- #
# Concrete stage assignments (empirically anchored shapes).
# --------------------------------------------------------------------------- #
class TestConcreteStages:
    def test_local_aggregate_is_base(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")], time_dimensions=[])
        [agg] = [s for s in pq.aggregate_slots if isinstance(s.key, AggregateKey)]
        assert agg.stage.kind == StageKind.BASE

    def test_time_shift_inner_is_derived_level_one(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="change(amount:sum)", name="c")])
        [ts] = [s for s in _transform_slots(pq) if s.key.op == "time_shift"]
        assert ts.stage == Stage(kind=StageKind.DERIVED, level=1)

    def test_nested_transform_reaches_derived_level_two(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="change(cumsum(amount:sum))", name="n")])
        assert {s.stage.kind for s in _transform_slots(pq)} == {StageKind.DERIVED}
        assert {s.stage.level for s in _transform_slots(pq)} == {1, 2}

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

    def test_composite_over_a_combined_placeholder_is_combined(self) -> None:
        """No transform operand → the combined-SELECT expression is COMBINED,
        strictly later than the PRODUCER placeholder it reads (D1 F5)."""
        pq = _plan(measures=[ModelMeasure(formula=f"{CM} + amount:sum", name="c")])
        [comp] = [s for s in pq.aggregate_slots if isinstance(s.key, ArithmeticKey)]
        assert comp.stage.kind == StageKind.COMBINED

    def test_computed_dimension_is_base(self) -> None:
        pq = _plan(dimensions=[{"expression": "amount * 2", "name": "amt2"}],
                   measures=[ModelMeasure(formula="amount:sum", name="a")], time_dimensions=[])
        [dim] = [s for s in pq.row_slots if s.is_dimension and isinstance(s.key, ArithmeticKey)]
        assert dim.stage.kind == StageKind.BASE


# --------------------------------------------------------------------------- #
# Transparent-composite levels (D10/D11) — only transforms stratify.
# --------------------------------------------------------------------------- #
_LOCAL_STALL = dict(
    measures=[ModelMeasure(formula="change(amount:sum)", name="ch")],
    filters=["last(change(amount:sum)) < 0"],
)


class TestTransparentCompositeLevels:
    def test_last_over_change_shares_the_composites_level(self) -> None:
        pq = _plan(**_LOCAL_STALL)
        by_key = {s.key: s for s in _all_slots(pq)}
        [ts] = [s for s in _transform_slots(pq) if s.key.op == "time_shift"]
        [last] = [s for s in _transform_slots(pq) if s.key.op == "last"]
        assert ts.stage == Stage(kind=StageKind.DERIVED, level=1)
        assert last.stage == Stage(kind=StageKind.DERIVED, level=2)
        change_comps = [
            c for c in _composite_slots(pq)
            if _transforms_read(c.key, by_key)
            and all(t.key.op == "time_shift" for t in _transforms_read(c.key, by_key))
        ]
        assert change_comps, "the projected change composite should be interned"
        for c in change_comps:
            assert c.stage == Stage(kind=StageKind.DERIVED, level=2)

    def test_mask_level_is_one_above_the_last_transform(self) -> None:
        """A mask's level is a validation / column-necessity fact only (D5);
        its placement stays the global wrapper, pinned by execution tests."""
        pq = _plan(**_LOCAL_STALL)
        by_id = {s.id: s for s in _all_slots(pq)}
        [mask] = pq.masks
        assert by_id[mask.slot_id].stage == Stage(kind=StageKind.DERIVED, level=3)

    def test_measure_only_last_over_change_is_level_two(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="last(change(amount:sum))", name="t")])
        [last] = [s for s in _transform_slots(pq) if s.key.op == "last"]
        assert last.stage == Stage(kind=StageKind.DERIVED, level=2)

    def test_staging_is_a_function_of_the_term_alone(self) -> None:
        """The same term carries the same stage with and without ``change(x)``
        projected (interning parity)."""
        without = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")],
                        filters=["last(change(amount:sum)) < 0"])
        with_proj = _plan(measures=[ModelMeasure(formula="amount:sum", name="a"),
                                    ModelMeasure(formula="change(amount:sum)", name="ch")],
                          filters=["last(change(amount:sum)) < 0"])
        a = {s.key: s.stage for s in _all_slots(without)}
        b = {s.key: s.stage for s in _all_slots(with_proj)}
        shared = set(a) & set(b)
        assert shared
        diffs = {k: (a[k], b[k]) for k in shared if a[k] != b[k]}
        assert not diffs

    def test_deeper_alternation_stages_one_level_per_transform(self) -> None:
        pq = _plan(measures=[
            ModelMeasure(formula="change(cumsum(amount:sum))", name="ch"),
            ModelMeasure(formula="last(change(cumsum(amount:sum)))", name="t"),
        ])
        by_op = {s.key.op: s for s in _transform_slots(pq)}
        assert by_op["cumsum"].stage == Stage(kind=StageKind.DERIVED, level=1)
        assert by_op["time_shift"].stage == Stage(kind=StageKind.DERIVED, level=2)
        assert by_op["last"].stage == Stage(kind=StageKind.DERIVED, level=3)
        by_key = {s.key: s for s in _all_slots(pq)}
        comps = [c for c in _composite_slots(pq) if _transforms_read(c.key, by_key)]
        assert comps, "the projected change composite should be interned"
        for c in comps:
            assert c.stage == Stage(kind=StageKind.DERIVED, level=3)


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

    def test_measure_mask_over_hidden_last_marks_the_chain(self) -> None:
        """A measure mask over a hidden ``last(change(x))`` marks ``last`` (mask
        dep) and its ``time_shift`` (read by a later level) as columns."""
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")],
                   filters=["last(change(amount:sum)) < 0"])
        [last] = [s for s in _transform_slots(pq) if s.key.op == "last"]
        [ts] = [s for s in _transform_slots(pq) if s.key.op == "time_shift"]
        assert last.needs_column is True
        assert ts.needs_column is True

    def test_derived_order_target_materialises_as_a_column(self) -> None:
        pq = _plan(measures=[ModelMeasure(formula="amount:sum", name="a")],
                   order=[{"column": f"change({CM}) + amount:sum", "direction": "desc"}])
        by_id = {s.id: s for s in _all_slots(pq)}
        targets = [by_id[e.slot_id] for e in pq.order]
        [comp] = [s for s in targets if isinstance(s.key, ArithmeticKey)]
        assert comp.stage.kind == StageKind.DERIVED
        assert comp.needs_column is True


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
    def test_later_stage_reference_is_rejected_naming_the_value(self) -> None:
        agg = _agg_key()
        arith = ArithmeticKey(op="+", operands=(agg, LiteralKey(value=1)))
        agg_slot = ValueSlot(id="later_agg", key=agg, declared_name="amount_sum", hidden=True,
                             phase=Phase.AGGREGATE, stage=Stage(kind=StageKind.DERIVED, level=1),
                             needs_column=True)
        arith_slot = ValueSlot(id="outer_composite", key=arith, declared_name="c", public_name="c",
                               public_aliases=["c"], phase=Phase.AGGREGATE, stage=Stage(kind=StageKind.BASE))
        with pytest.raises((MaterialisationStageError, ValidationError)) as ei:
            PlannedQuery(source_relation="orders",
                         aggregate_slots=[agg_slot, arith_slot], projection=["outer_composite"])
        msg = str(ei.value)
        assert "MaterialisationStageError" in msg or isinstance(ei.value, MaterialisationStageError)
        # the typed error names the offending value (cross-model-aggregates spec)
        assert "outer_composite" in msg

    def test_equal_level_transform_over_transform_is_rejected(self) -> None:
        """D7 explicit strictness: a transform must be staged strictly later than
        every transform it reads — a hand-built equal-level plan is rejected."""
        inner = TransformKey(op="cumsum", input=_agg_key())
        outer = TransformKey(op="last", input=inner)
        inner_slot = ValueSlot(id="t_inner", key=inner, declared_name="i", hidden=True,
                               phase=Phase.POST, needs_column=True,
                               stage=Stage(kind=StageKind.DERIVED, level=1))
        outer_slot = ValueSlot(id="t_outer", key=outer, declared_name="o", public_name="o",
                               public_aliases=["o"], phase=Phase.POST,
                               stage=Stage(kind=StageKind.DERIVED, level=1))
        with pytest.raises((MaterialisationStageError, ValidationError)):
            PlannedQuery(source_relation="orders",
                         aggregate_slots=[inner_slot, outer_slot], projection=["t_outer"])

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
