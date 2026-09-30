"""DEV-1910 — plan shapes for the home-rooted association producer (design
decisions 2–6): home root, root-coordinate entity keys, reachable-unsafe conjunct
as a kept bound filter, nested attached-parameter producer.

Spec: queries/attribution-modes — "Distinct-entity association over the virtual model";
queries/cross-model-aggregates — "Producer filter routing".
"""

from __future__ import annotations

from slayer.core.keys import ColumnKey
from slayer.engine.plan import plan_query
from slayer.ir.planned import AssociationProducerKernel, RegroupAttachPlan
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1840_fixtures import bundle, dev1840_models
from tests._dev1841_fixtures import ModelMeasure, assoc_q
from tests._dev1900_fixtures import dev1900_models, orders_q

CM = ModelMeasure(formula="customers.spend:sum", name="cm")
HEADLINE = ("customers.spend:weighted_avg("
            "weight=sum(amount, partition_by=customers.regions.name))")


def _bundle1900():
    models = dev1900_models()
    return ResolvedSourceBundle(dialect="postgres", source_model=models[0], referenced_models=models[1:])


def _assoc(planned):
    (att,) = [a for a in planned.regroup_attach_plans
              if getattr(a.kernel, "kind", None) == "association"]
    return att


def _akernel(att) -> AssociationProducerKernel:
    assert isinstance(att.kernel, AssociationProducerKernel)
    return att.kernel


def _rerooted_paths(att, leaf: str):
    """Producer-side (home-rooted) grain key paths for ``leaf`` — the producer
    slot each join_pair points at, where the reroot shows."""
    slots = {s.id: s for s in att.producer_plan.row_slots}
    return [getattr(slots[sid].key, "path", None)
            for _, sid in att.join_pairs
            if sid in slots and getattr(slots[sid].key, "leaf", None) == leaf]


class TestHomeRooting:
    def test_producer_roots_at_the_home(self):
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM]), bundle=bundle()))
        assert att.producer_root_model == "customers"

    def test_entity_keys_are_in_root_coordinates(self):
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM]), bundle=bundle()))
        assert _akernel(att).entity_keys == [ColumnKey(path=(), leaf="id")]


class TestReachableConjunctIsInlined:
    def test_no_semi_join_conjunct_rides_as_a_bound_filter(self):
        """The weak-plans conjunct inlines (no semi-join); its text rides the
        attach's restricted-filter list so the informational entry is kept."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM],
                          filters=["customers.plans.level = 'basic'"]),
            bundle=bundle(dev1840_models(strong_plans=False))))
        assert list(att.producer_plan.semi_join_filters) == []
        assert any("basic" in t for t in att.association_restricted_filter_texts)


class TestTwoHopHome:
    def test_roots_at_regions_with_the_two_hop_reverse_path(self):
        """A metric homed two hops away roots at regions; status reroots through
        the two-hop reverse path ('customers', 'orders')."""
        att = _assoc(plan_query(
            query=orders_q(dimensions=["status"],
                           measures=[ModelMeasure(formula="customers.regions.pop:sum",
                                                  name="rp")],
                           to_many_handling="associate"),
            bundle=_bundle1900()))
        assert att.producer_root_model == "regions"
        assert ("customers", "orders") in _rerooted_paths(att, "status")


class TestSerialization:
    def test_new_fields_survive_a_pydantic_round_trip(self):
        """association_restricted_filter_texts (attach) round-trips through
        model_dump / model_validate."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM],
                          filters=["customers.plans.level = 'basic'"]),
            bundle=bundle(dev1840_models(strong_plans=False))))
        restored = RegroupAttachPlan.model_validate(att.model_dump())
        assert (restored.association_restricted_filter_texts
                == att.association_restricted_filter_texts)
        assert restored.association_restricted_filter_texts  # the basic conjunct


class TestNestedAttachedParameterProducer:
    def test_headline_nests_an_orders_rooted_producer(self):
        """DEV-1859 headline (decision 6): the customers-rooted body nests the
        amount-sum parameter's own orders-rooted producer, not a host-rooted one."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"],
                          measures=[ModelMeasure(formula=HEADLINE, name="w")]),
            bundle=bundle()))
        nested_roots = [n.producer_root_model
                        for n in att.producer_plan.regroup_attach_plans]
        assert "orders" in nested_roots
