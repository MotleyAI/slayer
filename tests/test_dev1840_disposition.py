"""DEV-1840 task 1.2 — three-way conjunct disposition (design D1/D2/D4).

Spec: openspec …/specs/queries/cross-model-aggregates — "Producer filter
inheritance". Each test names the delta scenario it pins. Contract: a pushed
group appears as ``producer_plan.semi_join_filters`` (hops carry
``target_model``, oriented ``join_pairs``, ``node_id``; the group carries
``conjuncts`` and ``filter_texts``); excluded conjuncts keep
``attach.dropped_filter_warnings``.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import AmbiguousJoinPathError
from slayer.engine.stage_planner import plan_query

from tests._dev1840_fixtures import (
    ModelMeasure,
    ambiguity_models,
    bundle,
    dev1840_models,
    gen,
    q,
    tq,
)

CM = ModelMeasure(formula="customers.spend:sum", name="cm")
RM = ModelMeasure(formula="stores.rent:sum", name="rm")
SM = ModelMeasure(formula="agents.score:sum", name="sm")
RV = ModelMeasure(formula="reviews.stars:sum", name="rv")


def _plan(query, models=None):
    return plan_query(query=query, bundle=bundle(models))


def _attach(planned, root):
    matches = [a for a in planned.regroup_attach_plans
               if a.producer_root_model == root]
    assert len(matches) == 1, [a.producer_root_model
                               for a in planned.regroup_attach_plans]
    return matches[0]


def _pairs(hop):
    return [tuple(p) for p in hop.join_pairs]


class TestInlineUnchanged:
    def test_provably_safe_hop_stays_inline(self):
        """Scenario: provably safe filter paths keep the inline form."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.regions.name = 'North'"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        assert list(att.producer_plan.semi_join_filters) == []

    def test_root_local_stays_inline(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.tier = 'gold'"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        assert list(att.producer_plan.semi_join_filters) == []


class TestSemiJoinPushdown:
    def test_unsafe_reverse_hop_pushes(self):
        """Scenario: pushdown works without a declared reverse join —
        the stored forward edge is inverted, oriented root→orders."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["channel = 'app'"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]
        assert _pairs(sj.hops[0]) == [("id", "customer_id")]
        assert len(sj.conjuncts) == 1
        assert any("channel" in (t or "") for t in sj.filter_texts)

    async def test_declared_reverse_edge_pushes_identically(self):
        """The edge declared on the customers side pushes exactly like the
        forward declaration — which model stores it is storage trivia
        (DEV-1853 mirror parity)."""
        query = q(dimensions=["customers.tier"], measures=[CM],
                  filters=["channel = 'app'"])
        att = _attach(_plan(
            query, models=dev1840_models(declare_reverse=True),
        ), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]
        assert _pairs(sj.hops[0]) == [("id", "customer_id")]
        assert await gen(query) == await gen(
            query, models=dev1840_models(declare_reverse=True),
        )

    def test_forward_unproven_hop_pushes(self):
        """An unproven hop needs no inversion: the stored forward edge is the
        correlation path."""
        att = _attach(_plan(
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["customers.plans.level = 'basic'"]),
            models=dev1840_models(strong_plans=False),
        ), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["plans"]
        assert _pairs(sj.hops[0]) == [("plan_code", "code")]

    def test_pure_cross_path_disjunction_pushes(self):
        """One branch, no root-local refs: the whole OR moves into the EXISTS."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["status = 'ok' OR channel = 'app'"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert len(sj.conjuncts) == 1

    def test_pure_cross_path_negation_pushes(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["NOT (channel = 'app')"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]

    def test_root_local_mixing_under_conjunctive_comparison_pushes(self):
        """An atomic comparison may mix a root-local ref (correlated outer
        reference) with cross-path refs."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.spend > amount"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]


class TestExpandedDependencies:
    def test_host_declared_column_reading_the_target_pushes(self):
        """Scenario: derived-column dependencies drive classification — the
        raw ref is host-local, its SQL reads customers across the unsafe hop;
        it must not inline."""
        att = _attach(_plan(q(
            dimensions=["status"], measures=[CM],
            filters=["cust_tier = 'gold'"],
        )), "customers")
        assert att.dropped_filter_warnings == []
        sjs = list(att.producer_plan.semi_join_filters)
        assert len(sjs) == 1
        assert sjs[0].hops[0].target_model == "orders"

    def test_target_declared_column_crossing_1n_never_inlines(self):
        """The latent inline hole: ``customers.last_status`` is declared on
        the root but reads orders across the declared 1:N hop. Today it
        inlines and fans; it must classify by its expanded dependencies."""
        att = _attach(_plan(
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["customers.last_status = 'ok'"]),
            models=dev1840_models(declare_reverse=True),
        ), "customers")
        assert att.dropped_filter_warnings == []
        (sj,) = att.producer_plan.semi_join_filters
        assert [h.target_model for h in sj.hops] == ["orders"]


class TestExcludedConjuncts:
    def test_mixed_disjunction_stays_dropped(self):
        """Scenario: mixed disjunction stays dropped and warned."""
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.tier = 'gold' OR channel = 'app'"],
        )), "customers")
        (w,) = att.dropped_filter_warnings
        assert "channel" in w.filter_text
        assert w.reason
        assert list(getattr(att.producer_plan, "semi_join_filters", ())) == []

    def test_mixed_negation_stays_dropped(self):
        att = _attach(_plan(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["NOT (customers.tier = 'gold' AND channel = 'app')"],
        )), "customers")
        (w,) = att.dropped_filter_warnings
        assert "channel" in w.filter_text
        assert list(getattr(att.producer_plan, "semi_join_filters", ())) == []

    def test_cross_branch_disjunction_stays_dropped(self):
        att = _attach(_plan(
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["channel = 'app' OR customers.plans.level = 'basic'"]),
            models=dev1840_models(strong_plans=False),
        ), "customers")
        (w,) = att.dropped_filter_warnings
        assert "channel" in w.filter_text
        assert list(getattr(att.producer_plan, "semi_join_filters", ())) == []

    def test_cross_branch_atomic_comparison_stays_dropped(self):
        """Cross-path refs spanning two join branches in ONE conjunct."""
        att = _attach(_plan(
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["channel = customers.plans.level"]),
            models=dev1840_models(strong_plans=False),
        ), "customers")
        (w,) = att.dropped_filter_warnings
        assert "channel" in w.filter_text
        assert list(getattr(att.producer_plan, "semi_join_filters", ())) == []

    def test_ambiguous_measure_hop_fails_closed(self):
        """Scenario: ambiguous hops fail closed — two unnamed edges
        tickets→agents make the measure's hop an error, retiring the silent
        first-match. DEV-1853 divergences.md class (d)."""
        with pytest.raises(AmbiguousJoinPathError) as ei:
            _plan(tq(measures=[SM], filters=["effort > 2"]),
                  models=ambiguity_models())
        msg = str(ei.value)
        assert "opened_by" in msg
        assert "closed_by" in msg

    def test_ambiguous_filter_hop_fails_closed(self):
        """Scenario: ambiguous correlation hop fails closed — the measure's
        path is clean (tickets→reviews) but the filter crosses the ambiguous
        pair; drop+warn is retired. DEV-1853 divergences.md class (d)."""
        with pytest.raises(AmbiguousJoinPathError) as ei:
            _plan(tq(measures=[RV], filters=["agents.name = 'Ann'"]),
                  models=ambiguity_models())
        msg = str(ei.value)
        assert "opened_by" in msg
        assert "closed_by" in msg
