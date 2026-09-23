"""Plan-structure pins for the shifted producer (design D2/D4/D5): one
``attach_phase="shifted"`` attach per non-series ``time_shift`` slot, per-leaf
re-evaluate / carry classification, interning, and the ``PlannedQuery``
validators over malformed shifted attaches."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.errors import MaterialisationStageError
from slayer.core.keys import AggregateKey, TransformKey, walk_value_keys
from slayer.engine.plan import plan_query
from slayer.ir.planned import (
    PlannedQuery,
    RegroupAttachPlan,
    regroup_producer_identity,
)
from slayer.core.query import SlayerQuery
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1832_fixtures import (
    ModelMeasure,
    dev1832_models,
    month_td,
    monthly_q,
)

SHARE = "amount:sum / amount:sum(partition_by=[ordered_at])"
REGION_SHARE = "amount:sum / amount:sum(partition_by=[region])"
SERIES = "time_shift(cumsum(amount:sum), -1)"


def _bundle(root_name: str = "monthly") -> ResolvedSourceBundle:
    models = dev1832_models()
    root = next(m for m in models if m.name == root_name)
    return ResolvedSourceBundle(source_model=root,
                                referenced_models=[m for m in models if m is not root])


def _plan(*formulas: str) -> PlannedQuery:
    return plan_query(query=monthly_q(
        dimensions=["region"], time_dimensions=month_td(),
        measures=[ModelMeasure(formula=f, name=f"t{i}") for i, f in enumerate(formulas)]),
        bundle=_bundle())


def _slots(pq: PlannedQuery) -> list:
    return [*pq.row_slots, *pq.aggregate_slots, *pq.combined_expression_slots]


def _shift_slots(pq: PlannedQuery, *, series: bool) -> list:
    return [s for s in _slots(pq) if isinstance(s.key, TransformKey)
            and s.key.op == "time_shift" and s.series is series]


def _shifted(pq: PlannedQuery) -> list:
    return [a for a in pq.regroup_attach_plans if a.attach_phase == "shifted"]


def _keys(pq: PlannedQuery) -> list:
    return [k for s in _slots(pq) for k in walk_value_keys(s.key)]


def _rebuild(pq: PlannedQuery, **update) -> PlannedQuery:
    """Re-run every ``PlannedQuery`` validator over ``pq`` with fields replaced."""
    fields = {name: getattr(pq, name) for name in type(pq).model_fields}
    return PlannedQuery(**{**fields, **update})


def _attach(a: RegroupAttachPlan, **update) -> RegroupAttachPlan:
    fields = {name: getattr(a, name) for name in type(a).model_fields}
    return RegroupAttachPlan(**{**fields, **update})


class TestShiftedAttachShape:
    def test_one_shifted_attach_per_non_series_slot(self) -> None:
        pq = _plan(f"time_shift({SHARE}, -1)")
        [slot] = _shift_slots(pq, series=False)
        [attach] = _shifted(pq)
        assert attach.shift_of == slot.id
        assert attach.substitutions == []
        assert attach.answer_slot_id in {s.id for s in _slots(attach.producer_plan)}

    def test_join_pairs_cover_the_complete_grain(self) -> None:
        pq = _plan(f"time_shift({SHARE}, -1)")
        [attach] = _shifted(pq)
        grain = {s.key for s in pq.row_slots if not s.hidden}
        assert len(grain) == 2  # region + month
        assert {k for k, _ in attach.join_pairs} == grain
        producer_ids = {s.id for s in _slots(attach.producer_plan)}
        assert {sid for _, sid in attach.join_pairs} <= producer_ids

    def test_join_pairs_cover_secondary_time_dimension(self) -> None:
        pq = plan_query(query=SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["status"],
            "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"},
                                {"dimension": "customers.signup_at", "granularity": "month"}],
            "main_time_dimension": "ordered_at",
            "measures": [{"formula": "time_shift(amount:sum / 2, -1)", "name": "t"}]}),
            bundle=_bundle("orders"))
        [attach] = _shifted(pq)
        grain = {s.key for s in pq.row_slots if not s.hidden}
        assert len(grain) == 3  # status + both month buckets
        assert {k for k, _ in attach.join_pairs} == grain

    def test_series_slot_has_no_shifted_attach(self) -> None:
        pq = _plan(SERIES)
        assert _shift_slots(pq, series=True)
        assert _shifted(pq) == []

    def test_mixed_regimes_in_one_query(self) -> None:
        pq = _plan(f"time_shift({SHARE}, -1)", SERIES)
        [slot] = _shift_slots(pq, series=False)
        [attach] = _shifted(pq)
        assert attach.shift_of == slot.id


class TestLeafClassification:
    def test_axis_bearing_partitioned_leaf_is_re_evaluated_at_its_grain(self) -> None:
        pq = _plan(f"time_shift({SHARE}, -1)")
        [outer_cm] = [a for a in pq.regroup_attach_plans if a.attach_phase == "combined"]
        [sub] = outer_cm.substitutions
        [shifted] = _shifted(pq)
        producer = shifted.producer_plan
        nested = [a for a in producer.regroup_attach_plans
                  if any(s.original_key == sub.original_key for s in a.substitutions)]
        assert len(nested) == 1
        assert nested[0] is not outer_cm
        assert len(nested[0].join_pairs) == 1  # keyed by [month] only
        assert sub.placeholder not in _keys(producer)

    def test_inline_local_leaf_is_re_evaluated(self) -> None:
        pq = _plan(f"time_shift({SHARE}, -1)")
        [shifted] = _shifted(pq)
        plain = [k for k in _keys(shifted.producer_plan)
                 if isinstance(k, AggregateKey) and k.agg == "sum" and k.partition_keys is None]
        assert plain

    def test_axis_free_leaf_is_carried_as_the_same_attach(self) -> None:
        pq = _plan(f"time_shift({REGION_SHARE}, -1)")
        [outer_cm] = [a for a in pq.regroup_attach_plans if a.attach_phase == "combined"]
        [sub] = outer_cm.substitutions
        [shifted] = _shifted(pq)
        producer = shifted.producer_plan
        assert any(a is outer_cm for a in producer.regroup_attach_plans)
        assert sub.placeholder in _keys(producer)

    def test_two_offsets_intern_to_one_producer(self) -> None:
        pq = _plan(f"time_shift({SHARE}, -1)", f"time_shift({SHARE}, -2)")
        attaches = _shifted(pq)
        assert len(attaches) == 2
        assert len({a.shift_of for a in attaches}) == 2
        assert len({regroup_producer_identity(a) for a in attaches}) == 1


class TestValidatorsRejectMalformedShiftedAttaches:
    def _valid(self):
        pq = _plan(f"time_shift({SHARE}, -1)", SERIES)
        [attach] = _shifted(pq)
        others = [a for a in pq.regroup_attach_plans if a is not attach]
        return pq, attach, others

    def test_well_formed_rebuild_passes(self) -> None:
        pq, attach, others = self._valid()
        _rebuild(pq, regroup_attach_plans=[*others, _attach(attach)])

    def test_orphan_shift_of_rejected(self) -> None:
        pq, attach, others = self._valid()
        plans = [*others, _attach(attach, shift_of="no_such_slot")]
        with pytest.raises(ValueError):
            _rebuild(pq, regroup_attach_plans=plans)

    def test_shift_of_non_transform_slot_rejected(self) -> None:
        pq, attach, others = self._valid()
        dim = next(s for s in pq.row_slots if not s.hidden)
        plans = [*others, _attach(attach, shift_of=dim.id)]
        with pytest.raises(ValueError):
            _rebuild(pq, regroup_attach_plans=plans)

    def test_series_targeting_shifted_attach_rejected(self) -> None:
        pq, attach, others = self._valid()
        [series] = _shift_slots(pq, series=True)
        plans = [*others, _attach(attach, shift_of=series.id)]
        with pytest.raises(ValueError):
            _rebuild(pq, regroup_attach_plans=plans)

    def test_duplicate_shifted_attach_rejected(self) -> None:
        pq, attach, others = self._valid()
        plans = [*others, _attach(attach), _attach(attach)]
        with pytest.raises(ValueError):
            _rebuild(pq, regroup_attach_plans=plans)

    def test_foreign_answer_slot_rejected(self) -> None:
        pq, attach, others = self._valid()
        plans = [*others, _attach(attach, answer_slot_id="no_such_slot")]
        with pytest.raises(ValueError):
            _rebuild(pq, regroup_attach_plans=plans)

    def test_non_empty_substitutions_rejected(self) -> None:
        pq, attach, others = self._valid()
        borrowed = next(a for a in others if a.substitutions).substitutions
        plans = [*others, _attach(attach, substitutions=list(borrowed))]
        with pytest.raises(ValueError):
            _rebuild(pq, regroup_attach_plans=plans)

    def test_stage_order_recurses_into_shifted_producer(self) -> None:
        pq, attach, others = self._valid()
        producer = attach.producer_plan
        [first, *rest] = producer.aggregate_slots
        broken = producer.model_copy(update={
            "aggregate_slots": [first.model_copy(update={"stage": None}), *rest]})
        plans = [*others, attach.model_copy(update={"producer_plan": broken})]
        # Pydantic wraps a validator's ValueError subclass in ValidationError.
        with pytest.raises((MaterialisationStageError, ValidationError)):
            _rebuild(pq, regroup_attach_plans=plans)
