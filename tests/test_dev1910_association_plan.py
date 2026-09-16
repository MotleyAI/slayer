"""DEV-1910 — plan shapes for the home-rooted association producer (design
decisions 2–6). The producer roots at the aggregate's home, its entity keys are
in root coordinates, a population-root-reached dimension is presence-guarded on
the reverse hop, a reachable-but-unsafe conjunct rides as a bound filter (no
semi-join) with its informational entry kept, and a cross-model attached
parameter nests its own target-rooted producer.

Spec: queries/attribution-modes — "Distinct-entity association semantics";
queries/cross-model-aggregates — "Producer filter routing".
"""

from __future__ import annotations

from slayer.core.keys import ColumnKey
from slayer.engine.plan import plan_query
from slayer.ir.planned import RegroupAttachPlan
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1840_fixtures import bundle, dev1840_models
from tests._dev1841_fixtures import ModelMeasure, assoc_q
from tests._dev1900_fixtures import BAD_POP, dev1900_models, orders_q

CM = ModelMeasure(formula="customers.spend:sum", name="cm")
HEADLINE = ("customers.spend:weighted_avg("
            "weight=sum(amount, partition_by=customers.regions.name))")


def _bundle1900():
    models = dev1900_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _assoc(planned):
    (att,) = [a for a in planned.regroup_attach_plans
              if getattr(a.kernel, "kind", None) == "association"]
    return att


def _present(att):
    return {(k.path, k.leaf) for k in att.kernel.present_keys}


def _rerooted_paths(att, leaf: str):
    return [vk.path for vk, _ in att.join_pairs
            if getattr(vk, "leaf", None) == leaf]


class TestHomeRooting:
    def test_producer_roots_at_the_home(self):
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM]), bundle=bundle()))
        assert att.producer_root_model == "customers"

    def test_entity_keys_are_in_root_coordinates(self):
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM]), bundle=bundle()))
        assert att.kernel.entity_keys == [ColumnKey(path=(), leaf="id")]


class TestReachableConjunctIsInlined:
    def test_no_semi_join_conjunct_rides_as_a_bound_filter(self):
        """The weak-plans conjunct inlines on the home-rooted joins (no
        semi-join); its text rides the attach's restricted-filter list so the
        informational entry is kept."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM],
                          filters=["customers.plans.level = 'basic'"]),
            bundle=bundle(dev1840_models(strong_plans=False))))
        assert list(att.producer_plan.semi_join_filters) == []
        assert any("basic" in t for t in att.association_restricted_filter_texts)


class TestPresenceKeys:
    def test_population_root_dimension_guards_the_back_hop(self):
        """status is reached only back through the population root — present_keys
        is the host-side join column of the reverse hop (orders.customer_id)."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM]), bundle=bundle()))
        assert _present(att) == {(("orders",), "customer_id")}

    def test_home_side_dimension_has_no_presence_key(self):
        """bad_pop is home-side (forward from the customers home) — no presence
        guard: the region with no events keeps its NULL cell."""
        att = _assoc(plan_query(
            query=orders_q(dimensions=[BAD_POP], measures=[CM],
                           to_many_handling="associate"),
            bundle=_bundle1900()))
        assert att.kernel.present_keys == []

    def test_composite_back_hop_guards_every_column(self):
        """A composite reverse hop (orders → stores) guards BOTH host-side join
        columns (all-components rule)."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"],
                          measures=[ModelMeasure(formula="stores.rent:sum", name="r")]),
            bundle=bundle()))
        assert _present(att) == {
            (("orders",), "store_co"), (("orders",), "store_no")}


class TestTwoHopHome:
    def test_roots_at_regions_with_the_two_hop_reverse_path(self):
        """A metric homed two hops away roots at regions; the population-root
        dimension reroots through the two-hop reverse path ('customers',
        'orders')."""
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
        """Task 3.1: present_keys (kernel) and association_restricted_filter_texts
        (attach) round-trip through model_dump / model_validate."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"], measures=[CM],
                          filters=["customers.plans.level = 'basic'"]),
            bundle=bundle(dev1840_models(strong_plans=False))))
        restored = RegroupAttachPlan.model_validate(att.model_dump())
        assert restored.kernel.present_keys == att.kernel.present_keys
        assert (restored.association_restricted_filter_texts
                == att.association_restricted_filter_texts)
        assert restored.kernel.present_keys  # status guards the back hop
        assert restored.association_restricted_filter_texts  # the basic conjunct


class TestNestedAttachedParameterProducer:
    def test_headline_nests_an_orders_rooted_producer(self):
        """DEV-1859 headline (decision 6): the customers-rooted association body
        nests the amount-sum parameter's own target-rooted producer at orders,
        rather than re-routing onto a host-rooted producer."""
        att = _assoc(plan_query(
            query=assoc_q(dimensions=["status"],
                          measures=[ModelMeasure(formula=HEADLINE, name="w")]),
            bundle=bundle()))
        nested_roots = [n.producer_root_model
                        for n in att.producer_plan.regroup_attach_plans]
        assert "orders" in nested_roots
