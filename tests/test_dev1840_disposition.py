"""Conjunct disposition (design D1/D2/D4).

Spec: openspec …/specs/queries/cross-model-aggregates — "Producer filter
inheritance". Each test names the delta scenario it pins. Contract: a pushed
group appears as ``producer_plan.semi_join_filters`` (hops carry
``target_model``, oriented ``join_pairs``, ``node_id``; the group carries
``conjuncts`` and ``filter_texts``).
"""

from __future__ import annotations

import pytest

from slayer.core.errors import AmbiguousRelationshipPathError
from slayer.engine.plan import plan_query
from slayer.ir.planned import PlannedQuery, RegroupAttachPlan, SemiJoinHop
from slayer.core.query import QueryMetric

from tests._dev1840_fixtures import (
    ambiguity_datasets,
    bundle,
    dev1840_datasets,
    gen,
    q,
    tq,
)

CM = QueryMetric(metric="sum(customers.spend)", alias="cm")
RM = QueryMetric(metric="sum(stores.rent)", alias="rm")
SM = QueryMetric(metric="sum(agents.score)", alias="sm")
RV = QueryMetric(metric="sum(reviews.stars)", alias="rv")


def _plan(query, datasets=None):
    return plan_query(query=query, bundle=bundle(datasets))


def _attach(planned: PlannedQuery, root: str) -> RegroupAttachPlan:
    matches = [a for a in planned.regroup_attach_plans
               if a.producer_root_dataset == root]
    assert len(matches) == 1, [a.producer_root_dataset
                               for a in planned.regroup_attach_plans]
    return matches[0]


def _pairs(hop: SemiJoinHop) -> list[tuple[str, ...]]:
    return [tuple(p) for p in hop.join_pairs]


class TestInlineUnchanged:
    def test_provably_safe_hop_stays_inline(self):
        """Scenario: provably safe filter paths keep the inline form."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["customers.regions.name = 'North'"],
        )), "customers")
        assert list(att.producer_plan.semi_join_filters) == []

    def test_root_local_stays_inline(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["customers.tier = 'gold'"],
        )), "customers")
        assert list(att.producer_plan.semi_join_filters) == []


class TestSemiJoinPushdown:
    def test_unsafe_reverse_hop_pushes(self):
        """Scenario: pushdown works without a declared reverse join —
        the stored forward edge is inverted, oriented root→orders."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["channel = 'app'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders"]
        assert _pairs(sj.hops[0]) == [("id", "customer_id")]
        assert len(sj.conjuncts) == 1
        assert any("channel" in (t or "") for t in sj.filter_texts)

    async def test_declared_reverse_edge_pushes_identically(self):
        """The edge declared on the customers side pushes exactly like the
        forward declaration — which model stores it is storage trivia
        (mirror parity)."""
        query = q(dimensions=["customers.tier"], metrics=[CM],
                  filter=["channel = 'app'"])
        att = _attach(_plan(
            query, datasets=dev1840_datasets(declare_reverse=True),
        ), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders"]
        assert _pairs(sj.hops[0]) == [("id", "customer_id")]
        assert await gen(query) == await gen(
            query, datasets=dev1840_datasets(declare_reverse=True),
        )

    def test_forward_unproven_hop_pushes(self):
        """An unproven hop needs no inversion: the stored forward edge is the
        correlation path."""
        att = _attach(_plan(
            q(dimensions=["customers.tier"], metrics=[CM],
              filter=["customers.plans.level = 'basic'"]),
            datasets=dev1840_datasets(strong_plans=False),
        ), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["plans"]
        assert _pairs(sj.hops[0]) == [("plan_code", "code")]

    def test_pure_cross_path_disjunction_pushes(self):
        """One branch, no root-local refs: the whole OR moves into the EXISTS."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["status = 'ok' OR channel = 'app'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert len(sj.conjuncts) == 1

    def test_pure_cross_path_negation_pushes(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["NOT (channel = 'app')"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders"]

    def test_root_local_mixing_under_conjunctive_comparison_pushes(self):
        """An atomic comparison may mix a root-local ref (correlated outer
        reference) with cross-path refs."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["customers.spend > amount"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders"]


class TestExpandedDependencies:
    def test_host_declared_column_reading_the_target_pushes(self):
        """Scenario: derived-column dependencies drive classification — the
        raw ref is host-local, its SQL reads customers across the unsafe hop;
        it must not inline."""
        att = _attach(_plan(q(
            dimensions=["status"], metrics=[CM],
            filter=["cust_tier = 'gold'"], to_many_handling="broadcast",
        )), "customers")
        sjs = list(att.producer_plan.semi_join_filters)
        assert len(sjs) == 1
        assert sjs[0].hops[0].to == "orders"

    def test_target_declared_column_crossing_1n_never_inlines(self):
        """The latent inline hole: ``customers.last_status`` is declared on
        the root but reads orders across the declared 1:N hop. Today it
        inlines and fans; it must classify by its expanded dependencies."""
        att = _attach(_plan(
            q(dimensions=["customers.tier"], metrics=[CM],
              filter=["customers.last_status = 'ok'"]),
            datasets=dev1840_datasets(declare_reverse=True),
        ), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.to for h in sj.hops] == ["orders"]


class TestBooleanTotalPushdown:
    """Out-of-scope conjuncts (OR/NOT mixing a local and a cross-path
    ref, several branches, or a cross-branch atom) flip from dropped-and-warned to
    a boolean-total semi-join on the producer."""

    def test_mixed_disjunction_pushes(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["customers.tier = 'gold' OR channel = 'app'"],
        )), "customers")
        (sj,) = att.producer_plan.semi_join_filters
        assert any("channel" in (t or "") for t in sj.filter_texts)

    def test_mixed_negation_pushes(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], metrics=[CM],
            filter=["NOT (customers.tier = 'gold' AND channel = 'app')"],
        )), "customers")
        assert att.producer_plan.semi_join_filters

    def test_cross_branch_disjunction_pushes(self):
        att = _attach(_plan(
            q(dimensions=["customers.tier"], metrics=[CM],
              filter=["channel = 'app' OR customers.plans.level = 'basic'"]),
            datasets=dev1840_datasets(strong_plans=False),
        ), "customers")
        assert att.producer_plan.semi_join_filters

    def test_cross_branch_atomic_comparison_pushes(self):
        """Cross-path refs spanning two join branches in ONE conjunct -> product."""
        att = _attach(_plan(
            q(dimensions=["customers.tier"], metrics=[CM],
              filter=["channel = customers.plans.level"]),
            datasets=dev1840_datasets(strong_plans=False),
        ), "customers")
        assert att.producer_plan.semi_join_filters


class TestExcludedConjuncts:
    def test_ambiguous_measure_hop_fails_closed(self):
        """Scenario: ambiguous hops fail closed — two unnamed edges
        tickets→agents make the measure's hop an error, retiring the silent
        first-match. Divergence class (d)."""
        query = tq(metrics=[SM], filter=["effort > 2"])
        models = ambiguity_datasets()
        with pytest.raises(AmbiguousRelationshipPathError) as ei:
            _plan(query, datasets=models)
        msg = str(ei.value)
        assert "opened_by" in msg
        assert "closed_by" in msg

    def test_ambiguous_filter_hop_fails_closed(self):
        """Scenario: ambiguous correlation hop fails closed — the measure's
        path is clean (tickets→reviews) but the filter crosses the ambiguous
        pair; drop+warn is retired. Divergence class (d)."""
        query = tq(metrics=[RV], filter=["agents.name = 'Ann'"])
        models = ambiguity_datasets()
        with pytest.raises(AmbiguousRelationshipPathError) as ei:
            _plan(query, datasets=models)
        msg = str(ei.value)
        assert "opened_by" in msg
        assert "closed_by" in msg
