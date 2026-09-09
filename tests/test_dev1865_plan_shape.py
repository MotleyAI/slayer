"""DEV-1865 — the typed mask plan representation.

Filter conjuncts compile to typed mask entries (stable slot id, field/measure
typing, stratum) over hidden slots, and ``OrderEntry`` drops its plan-side
``scope`` (classification relocates to emission-side lowering). The materialized
value equalling the measure's is pinned by the executed twin-query law in
``test_dev1865_value_parity``; here we pin the plan IR shape.
"""

from __future__ import annotations

from slayer.engine.planned import OrderEntry
from slayer.engine.stage_planner import plan_query

from tests._dev1840_fixtures import ModelMeasure as _CM
from tests._dev1840_fixtures import bundle as _dev1840_bundle
from tests._dev1840_fixtures import q as _dev1840_q
from tests._dev1865_fixtures import ModelMeasure, plan, q

_DEV1840_CM = _CM(formula="customers.spend:sum", name="cm")


def _dev1840_attach(planned, root):
    matches = [a for a in planned.regroup_attach_plans if a.producer_root_model == root]
    assert len(matches) == 1, [a.producer_root_model for a in planned.regroup_attach_plans]
    return matches[0]


def _slots_by_id(planned):
    slots = {}
    for name in ("row_slots", "aggregate_slots", "combined_expression_slots"):
        for slot in getattr(planned, name, []) or []:
            slots[slot.id] = slot
    return slots


def _typing(mask) -> str:
    return str(getattr(mask, "typing", "")).lower()


class TestMaskEntries:
    def test_planned_query_exposes_typed_masks_per_conjunct(self) -> None:
        planned = plan(q(
            dimensions=["region"],
            filters=["amount:sum > 100", "status = 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert hasattr(planned, "masks"), "PlannedQuery must carry typed mask entries"
        masks = planned.masks
        assert len(masks) == 2
        for mask in masks:
            assert getattr(mask, "slot_id", None) is not None
            assert hasattr(mask, "stratum")
        typings = {_typing(m) for m in masks}
        assert any("field" in t for t in typings)
        assert any("measure" in t for t in typings)

    def test_mask_slots_are_hidden(self) -> None:
        planned = plan(q(
            dimensions=["region"],
            filters=["amount:sum > 100", "status = 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert hasattr(planned, "masks")
        slots = _slots_by_id(planned)
        for mask in planned.masks:
            slot = slots.get(mask.slot_id)
            assert slot is not None, f"mask slot {mask.slot_id!r} not materialized"
            assert slot.hidden

    def test_mask_slot_ids_are_stable_across_planning(self) -> None:
        query = q(
            dimensions=["region"],
            filters=["amount:sum > 100", "status = 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        first, second = plan(query), plan(query)
        assert hasattr(first, "masks")
        assert [m.slot_id for m in first.masks] == [m.slot_id for m in second.masks]

    def test_field_conjunct_is_stratum_zero(self) -> None:
        planned = plan(q(
            dimensions=["region"],
            filters=["status = 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert hasattr(planned, "masks")
        (mask,) = planned.masks
        assert "field" in _typing(mask)
        assert getattr(mask, "stratum") == 0


class TestOrderEntryScopeRemoved:
    def test_scope_is_no_longer_a_plan_field(self) -> None:
        assert "scope" not in OrderEntry.model_fields


class TestDev1840DispositionUnchanged:
    """task 2.4 / axiom D: the reachability re-plumbing must not change the
    semi-join dispositions on the DEV-1840 fixture shapes."""

    def test_root_local_filter_stays_inline(self) -> None:
        att = _dev1840_attach(plan_query(query=_dev1840_q(
            dimensions=["customers.tier"], measures=[_DEV1840_CM],
            filters=["customers.tier = 'gold'"],
        ), bundle=_dev1840_bundle()), "customers")
        assert att.dropped_filter_warnings == []
        assert list(att.producer_plan.semi_join_filters) == []

    def test_unsafe_reverse_hop_pushes_one_semi_join(self) -> None:
        att = _dev1840_attach(plan_query(query=_dev1840_q(
            dimensions=["customers.tier"], measures=[_DEV1840_CM],
            filters=["channel = 'app'"],
        ), bundle=_dev1840_bundle()), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]
        assert [tuple(p) for p in sj.hops[0].join_pairs] == [("id", "customer_id")]
        assert len(sj.conjuncts) == 1
        assert any("channel" in (t or "") for t in sj.filter_texts)
