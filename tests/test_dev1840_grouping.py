"""First-reverse-hop grouping and the SemiJoinFilter IR
(design D3/D5).

One EXISTS per root-adjacent reverse hop; conjuncts of a group are AND-ed in
the same subquery; hop descriptors carry the oriented pairs and a stable
per-node identity so repeated datasets bind distinct aliases.
"""

from __future__ import annotations

from slayer.engine.plan import plan_query
from slayer.ir.planned import PlannedQuery, RegroupAttachPlan, SemiJoinHop, _walk_regroup_attaches
from slayer.core.query import QueryMetric

from tests._dev1840_fixtures import (
    SPEND_BAND_170,
    bundle,
    dev1840_datasets,
    q,
)

M = QueryMetric(metric="sum(orders.amount)", alias="m")
CM = QueryMetric(metric="sum(customers.spend)", alias="cm")
RM = QueryMetric(metric="sum(stores.rent)", alias="rm")


def _plan(query, datasets=None):
    return plan_query(query=query, bundle=bundle(datasets))


def _attach(planned: PlannedQuery, root: str) -> RegroupAttachPlan:
    matches = [a for a in planned.regroup_attach_plans
               if a.producer_root_dataset == root]
    assert len(matches) == 1
    return matches[0]


def _pairs(hop: SemiJoinHop) -> list[tuple[str, ...]]:
    return [tuple(p) for p in hop.join_pairs]


class TestGrouping:
    def test_same_first_hop_conjuncts_share_one_exists(self):
        """Scenario: filters sharing a branch bind to the same related row."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["orders.status = 'ok'", "orders.channel = 'app'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert len(sj.conjuncts) == 2
        texts = " ".join(t or "" for t in sj.filter_texts)
        assert "status" in texts
        assert "channel" in texts

    def test_distinct_first_hops_get_independent_exists(self):
        att = _attach(_plan(
            q(dimensions=["customers.tier"], metrics=[CM],
              filter=["orders.channel = 'web'", "customers.plans.level = 'basic'"]),
            datasets=dev1840_datasets(strong_plans=False),
        ), "customers")
        sjs = list(att.producer_plan.semi_join_filters)
        assert len(sjs) == 2
        assert {sj.hops[0].to for sj in sjs} == {"orders", "plans"}
        assert all(len(sj.conjuncts) == 1 for sj in sjs)

    def test_shared_first_hop_builds_the_union_tree(self):
        """Two conjuncts, one on orders and one a hop further on customers,
        share the first reverse hop: ONE group whose tree joins both."""
        att = _attach(_plan(q(
            dimensions=["orders.status"], metrics=[M, RM],
            filter=["orders.channel = 'app'", "customers.tier = 'gold'"],
            to_many_handling="broadcast",
        )), "stores")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders", "customers"]
        assert _pairs(sj.hops[1]) == [("customer_id", "id")]
        assert len(sj.conjuncts) == 2


class TestHopDescriptors:
    def test_inverted_hop_is_oriented_root_to_target(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["orders.channel = 'app'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert _pairs(sj.hops[0]) == [("id", "customer_id")]

    def test_composite_inverted_hop_keeps_every_pair(self):
        att = _attach(_plan(q(
            dimensions=["orders.status"], metrics=[M, RM],
            filter=["orders.channel = 'app'"],
            to_many_handling="broadcast",
        )), "stores")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders"]
        assert _pairs(sj.hops[0]) == [("co", "store_co"), ("no", "store_no")]

    def test_repeated_model_nodes_carry_distinct_identity(self):
        """``cust_tier`` expands through orders back into customers: the tree
        holds a second customers node that must not alias the root."""
        att = _attach(_plan(q(
            dimensions=["orders.status"], metrics=[CM],
            filter=["orders.cust_tier = 'gold'"],
            to_many_handling="broadcast",
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders", "customers"]
        assert len({h.node_id for h in sj.hops}) == len(sj.hops)


class TestThreading:
    def test_nested_producer_carries_the_group(self):
        """The computed dimension's nested producer receives the pushdown."""
        planned = _plan(q(
            dimensions=[{"field": SPEND_BAND_170, "alias": "sband"}],
            metrics=[M],
            filter=["orders.channel = 'app'"],
        ))
        carrying = [
            att for att in _walk_regroup_attaches(planned)
            if att.producer_root_dataset == "customers"
            and list(att.producer_plan.semi_join_filters)
        ]
        assert carrying, "no customers-rooted producer carries the semi-join"
