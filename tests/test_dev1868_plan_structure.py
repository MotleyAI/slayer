"""DEV-1868 plan-structure pins (design D2/D3/D7): producer count, substitution
ownership, attach phase, complete join grain; composites become combined slots;
the row-leaf time_shift rejection fires at plan time."""

from __future__ import annotations

import re

import pytest

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ScalarCallKey,
    walk_value_keys,
)
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1836_fixtures import ModelMeasure, dev1836_models, gen, month_td, q
from tests._dev1846_fixtures import SlayerQuery, dev1846_models
from tests._dev1868_fixtures import CM_PART_TIER, LAST_TIER


def _bundle36() -> ResolvedSourceBundle:
    models = dev1836_models()
    return ResolvedSourceBundle(source_model=models[0],
                                referenced_models=models[1:])


def _bundle46() -> ResolvedSourceBundle:
    models = dev1846_models()
    return ResolvedSourceBundle(source_model=models[0],
                                referenced_models=models[1:])


def _q46(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    kw.setdefault("time_dimensions",
                  [{"dimension": "ordered_at", "granularity": "month"}])
    return SlayerQuery(**kw)


class TestRankedProducerShape:
    def test_cross_model_last_installs_a_ranked_kernel(self) -> None:
        planned = plan_query(query=q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula=LAST_TIER, name="l")]),
            bundle=_bundle36())
        [attach] = planned.regroup_attach_plans
        assert attach.producer_root_model == "customers"
        assert attach.attach_phase == "combined"
        assert attach.kernel.kind == "ranked"
        assert len(attach.join_pairs) == 1  # the complete [tier] grain


class TestSharedInnerProducerShape:
    def test_one_producer_serves_transform_and_filter(self) -> None:
        planned = plan_query(query=q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filters=[f"{CM_PART_TIER} > 100"],
            measures=[ModelMeasure(formula=f"cumsum({CM_PART_TIER})", name="c")]),
            bundle=_bundle36())
        attaches = [a for a in planned.regroup_attach_plans
                    if a.producer_root_model == "customers"]
        assert len(attaches) == 1
        [attach] = attaches
        assert attach.attach_phase == "combined"
        assert attach.partition_display == ["tier"]  # the complete grain
        assert len(attach.join_pairs) == 1
        [sub] = attach.substitutions
        assert isinstance(sub.original_key, AggregateKey)
        slots = {s.id: s for s in [*planned.row_slots, *planned.aggregate_slots,
                                   *planned.combined_expression_slots]}

        def _reads_placeholder(slot) -> bool:
            return any(k == sub.placeholder for k in walk_value_keys(slot.key))

        transform_slots = [slots[sid] for layer in planned.transform_layers
                           for sid in layer.slot_ids]
        assert any(_reads_placeholder(s) for s in transform_slots)
        mask_slots = [slots[m.slot_id] for m in planned.masks]
        assert any(_reads_placeholder(s) for s in mask_slots)
        # The original cross-model aggregate survives in no slot.
        assert not [s.id for s in slots.values()
                    if any(k == sub.original_key for k in walk_value_keys(s.key))]

    async def test_emitted_sql_has_one_producer_relation(self) -> None:
        sql = await gen(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filters=[f"{CM_PART_TIER} > 100"],
            measures=[ModelMeasure(formula=f"cumsum({CM_PART_TIER})", name="c")]))
        assert len(set(re.findall(r"_cm_\w+", sql))) == 1, sql


class TestCompositesBecomeCombinedSlots:
    """D3: every cross-model composite shape plans as a placeholder-substituted
    AGGREGATE slot with target-rooted producers — none reaches the seam."""

    @pytest.mark.parametrize("formula, n_producers", [
        ("amount:sum / customers.spend:sum", 1),
        ("customers.spend:sum * 2", 1),
        ("round(customers.spend:sum / amount:sum, 2)", 1),
        ("customers.spend:sum - customers.spend:max", 2),
    ])
    def test_composite_plans_with_producers(
        self, formula: str, n_producers: int,
    ) -> None:
        planned = plan_query(query=q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula=formula, name="r")]),
            bundle=_bundle36())
        attaches = planned.regroup_attach_plans
        assert len(attaches) == n_producers
        assert all(a.producer_root_model == "customers" for a in attaches)
        composite_slots = [s for s in planned.aggregate_slots
                           if isinstance(s.key, (ArithmeticKey, ScalarCallKey))]
        assert len(composite_slots) == 1


class TestRowLeafRejectionAtPlanTime:
    """D7: the surviving time_shift row-leaf validation is a checker — it fires
    during planning, before any SQL generation."""

    def test_mixed_composite_rejected_by_plan_query(self) -> None:
        query = _q46(
            measures=[ModelMeasure(formula="time_shift(revenue:sum * weight, -1)",
                                   name="t")])
        bundle = _bundle46()
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=bundle)
        message = str(ei.value)
        assert "time_shift" in message
        assert re.search(r"(?i)row", message)
        assert "source_queries" in message

    def test_row_predicate_rejected_by_plan_query(self) -> None:
        query = _q46(
            measures=[ModelMeasure(formula="time_shift(store in ('A', 'B'), -1)",
                                   name="t")])
        bundle = _bundle46()
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=bundle)
        message = str(ei.value)
        assert "time_shift" in message
        assert re.search(r"(?i)row", message)
        assert "source_queries" in message
