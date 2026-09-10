"""DEV-1841 task 3.3 — filters route into the association producer by the
existing producer filter routing (root = host): host-attributable conjuncts
inline, unsafe-but-reachable conjuncts push by semi-join, out-of-scope conjuncts
stay dropped+warned. Executed values on SQLite + DuckDB.

The multi-branch routing MATRIX (which conjuncts inline vs push vs drop, across
several join branches, ambiguous hops) is DEV-1840's contract
(``test_dev1840_disposition.py``); the association producer reuses that routing
verbatim. These tests pin that the association producer participates in it and
that its membership is the semi-join semantics — including the NULL/absent-row
case (an entity with no passing related row is excluded).

Spec: openspec …/specs/queries/cross-model-aggregates — "Producer filter
routing", scenarios "Filter on a sibling fan-out branch restricts the
association correctly" and "Pushdown reaches every producer kind".
"""

from __future__ import annotations

import pytest

from tests._dev1840_fixtures import dev1840_models
from tests._dev1841_fixtures import (
    ASSOC_APP_SPEND_BY_STATUS,
    ASSOC_BASIC_SPEND_BY_STATUS,
    ModelMeasure,
    assoc_q,
    dropped_filter_warnings,
    make_exec_engine,
    status_key,
)

CM = ModelMeasure(formula="customers.spend:sum", name="cm")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_weak(request):
    async for engine in make_exec_engine(
        request, models=dev1840_models(strong_plans=False),
    ):
        yield request.param, engine


class TestFilterRoutingIntoAssociation:
    async def test_host_local_filter_applies_inline(self, exec_backend):
        """A host (orders) column filter restricts which rows enter the
        association before the per-cell distinct-entity aggregation."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"], measures=[CM], filters=["channel = 'app'"]))
        by = status_key(resp)
        for status, spend in ASSOC_APP_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.cm"]) == pytest.approx(spend)

    async def test_unsafe_reachable_filter_pushes_by_semijoin(
        self, exec_backend_weak,
    ):
        """Scenario: pushdown reaches the association producer — an unproven-hop
        conjunct restricts the association's membership by semi-join."""
        _, engine = exec_backend_weak
        resp = await engine.execute(assoc_q(
            dimensions=["status"], measures=[CM],
            filters=["customers.plans.level = 'basic'"]))
        by = status_key(resp)
        for status, spend in ASSOC_BASIC_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.cm"]) == pytest.approx(spend)

    async def test_null_sensitive_membership_excludes_absent_related_rows(
        self, exec_backend_weak,
    ):
        """Scenario (NULL-sensitive branch): membership is the semi-join
        semantics — an entity with no related row passing the predicate is
        excluded. c6 (an ok customer with no plan row) is dropped by
        ``plans.level != 'pro'``; a NULL-insensitive bug would add its 30 (270)."""
        _, engine = exec_backend_weak
        resp = await engine.execute(assoc_q(
            dimensions=["status"], measures=[CM],
            filters=["customers.plans.level != 'pro'"]))
        by = status_key(resp)
        assert float(by[("ok",)]["orders.cm"]) == pytest.approx(240.0)
        assert float(by[("ok",)]["orders.cm"]) != pytest.approx(270.0)
        assert float(by[("new",)]["orders.cm"]) == pytest.approx(100.0)

    async def test_out_of_scope_conjunct_stays_dropped_and_warns(
        self, exec_backend,
    ):
        """A mixed disjunction is outside pushdown scope: excluded from the
        association and reported by the dropped-filter warning."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"], measures=[CM],
            filters=["customers.tier = 'gold' OR channel = 'app'"]))
        (w,) = dropped_filter_warnings(resp)
        assert "channel" in w.filter_text
