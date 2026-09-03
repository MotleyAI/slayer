"""DEV-1840 task 1.3 — first-reverse-hop grouping and the SemiJoinFilter IR
(design D3/D5).

One EXISTS per root-adjacent reverse hop; conjuncts of a group are AND-ed in
the same subquery; hop descriptors carry the oriented pairs and a stable
per-node identity so repeated models bind distinct aliases.
"""

from __future__ import annotations

from slayer.engine.query_engine import _walk_regroup_attaches
from slayer.engine.stage_planner import plan_query

from tests._dev1840_fixtures import (
    ModelMeasure,
    SPEND_BAND_170,
    bundle,
    dev1840_models,
    q,
)

M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")
RM = ModelMeasure(formula="stores.rent:sum", name="rm")


def _plan(query, models=None):
    return plan_query(query=query, bundle=bundle(models))


def _attach(planned, root):
    matches = [a for a in planned.regroup_attach_plans
               if a.producer_root_model == root]
    assert len(matches) == 1
    return matches[0]


def _pairs(hop):
    return [tuple(p) for p in hop.join_pairs]


class TestGrouping:
    def test_same_first_hop_conjuncts_share_one_exists(self):
        """Scenario: filters sharing a branch bind to the same related row."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["status = 'ok'", "channel = 'app'"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert len(sj.conjuncts) == 2
        texts = " ".join(t or "" for t in sj.filter_texts)
        assert "status" in texts and "channel" in texts

    def test_distinct_first_hops_get_independent_exists(self):
        att = _attach(_plan(
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["channel = 'web'", "customers.plans.level = 'basic'"]),
            models=dev1840_models(strong_plans=False),
        ), "customers")
        assert att.dropped_filter_warnings == []
        sjs = list(att.producer_plan.semi_join_filters)
        assert len(sjs) == 2
        assert {sj.hops[0].target_model for sj in sjs} == {"orders", "plans"}
        assert all(len(sj.conjuncts) == 1 for sj in sjs)

    def test_shared_first_hop_builds_the_union_tree(self):
        """Two conjuncts, one on orders and one a hop further on customers,
        share the first reverse hop: ONE group whose tree joins both."""
        att = _attach(_plan(q(
            dimensions=["status"], measures=[M, RM],
            filters=["channel = 'app'", "customers.tier = 'gold'"],
        )), "stores")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders", "customers"]
        assert _pairs(sj.hops[1]) == [("customer_id", "id")]
        assert len(sj.conjuncts) == 2


class TestHopDescriptors:
    def test_inverted_hop_is_oriented_root_to_target(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["channel = 'app'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert _pairs(sj.hops[0]) == [("id", "customer_id")]

    def test_composite_inverted_hop_keeps_every_pair(self):
        att = _attach(_plan(q(
            dimensions=["status"], measures=[M, RM],
            filters=["channel = 'app'"],
        )), "stores")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]
        assert _pairs(sj.hops[0]) == [("co", "store_co"), ("no", "store_no")]

    def test_repeated_model_nodes_carry_distinct_identity(self):
        """``cust_tier`` expands through orders back into customers: the tree
        holds a second customers node that must not alias the root."""
        att = _attach(_plan(q(
            dimensions=["status"], measures=[CM],
            filters=["cust_tier = 'gold'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders", "customers"]
        assert len({h.node_id for h in sj.hops}) == len(sj.hops)


class TestThreading:
    def test_nested_producer_carries_the_group(self):
        """The computed dimension's nested producer receives the pushdown."""
        planned = _plan(q(
            dimensions=[{"expression": SPEND_BAND_170, "name": "sband"}],
            measures=[M],
            filters=["channel = 'app'"],
        ))
        carrying = [
            att for att in _walk_regroup_attaches(planned)
            if att.producer_root_model == "customers"
            and list(att.producer_plan.semi_join_filters)
        ]
        assert carrying, "no customers-rooted producer carries the semi-join"
        for att in carrying:
            assert att.dropped_filter_warnings == []
