"""DEV-1840 task 1.5 — metadata silence for pushed filters, strict narrowing
(design D8).

Spec: openspec …/specs/queries/cross-model-aggregates — "Strict mode" and the
metadata clauses of "Producer filter inheritance": pushed conjuncts are
silent and strict-clean; excluded conjuncts keep the established warning and
the strict error.
"""

from __future__ import annotations

import warnings as _warnings

import pytest

from slayer.core.errors import SlayerError, UnreachableFilterDroppedWarning

from tests._dev1840_fixtures import (
    ModelMeasure,
    SPEND_APP_BY_TIER,
    SPEND_BASIC_BY_TIER,
    ambiguity_models,
    dev1840_models,
    dropped_filter_warnings,
    make_exec_engine,
    q,
    rows_by,
    tq,
)


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


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_amb(request):
    async for engine in make_exec_engine(request, models=ambiguity_models()):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")
SM = ModelMeasure(formula="agents.score:sum", name="sm")


class TestPushedFiltersAreSilent:
    async def test_no_metadata_and_no_python_warning(self, exec_backend):
        """Scenario: unsafe filter no longer fans out — with no warning."""
        _, engine = exec_backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(q(
                dimensions=["customers.tier"], measures=[M, CM],
                filters=["channel = 'app'"],
            ))
        assert dropped_filter_warnings(resp) == []
        hits = [c for c in caught
                if issubclass(c.category, UnreachableFilterDroppedWarning)]
        assert hits == []

    async def test_forward_unproven_pushdown_is_silent_too(
        self, exec_backend_weak,
    ):
        _, engine = exec_backend_weak
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.plans.level = 'basic'"],
        ))
        assert dropped_filter_warnings(resp) == []


class TestStrictNarrows:
    async def test_strict_passes_on_a_pushable_filter(self, exec_backend):
        """Scenario: strict passes on a pushable filter."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            strict=True, dimensions=["customers.tier"], measures=[M, CM],
            filters=["channel = 'app'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in SPEND_APP_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier

    async def test_strict_passes_on_a_pushable_forward_hop(
        self, exec_backend_weak,
    ):
        _, engine = exec_backend_weak
        resp = await engine.execute(q(
            strict=True, dimensions=["customers.tier"], measures=[CM],
            filters=["customers.plans.level = 'basic'"],
        ))
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in SPEND_BASIC_BY_TIER.items():
            assert float(by[(tier,)]["orders.cm"]) == pytest.approx(spend), tier

    async def test_strict_still_errors_on_a_mixed_disjunction(self, exec_backend):
        """Scenario: strict still errors on an excluded filter — naming the
        filter and the remedy."""
        _, engine = exec_backend
        with pytest.raises(SlayerError) as ei:
            await engine.execute(q(
                strict=True, dimensions=["customers.tier"], measures=[CM],
                filters=["customers.tier = 'gold' OR channel = 'app'"],
            ))
        message = str(ei.value)
        assert "channel" in message
        assert "cardinality" in message or "unique" in message \
            or "remove" in message

    async def test_strict_still_errors_on_an_ambiguous_path(
        self, exec_backend_amb,
    ):
        _, engine = exec_backend_amb
        with pytest.raises(SlayerError) as ei:
            await engine.execute(
                tq(strict=True, measures=[SM], filters=["effort > 2"]),
            )
        assert "effort" in str(ei.value)

    async def test_strict_still_errors_on_an_unreachable_filter(
        self, exec_backend_amb,
    ):
        """Scenario: genuinely unreachable filter keeps the established
        behavior under strict."""
        _, engine = exec_backend_amb
        with pytest.raises(SlayerError) as ei:
            await engine.execute(
                tq(strict=True, measures=[SM], filters=["reviews.stars > 4"]),
            )
        message = str(ei.value)
        assert "stars" in message
        assert "cardinality" in message or "unique" in message \
            or "remove" in message


class TestExcludedFiltersKeepTheWarning:
    async def test_mixed_disjunction_warns(self, exec_backend):
        """Scenario: mixed disjunction stays dropped and warned."""
        _, engine = exec_backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(q(
                dimensions=["customers.tier"], measures=[CM],
                filters=["customers.tier = 'gold' OR channel = 'app'"],
            ))
        (w,) = dropped_filter_warnings(resp)
        assert "channel" in w.filter_text
        assert w.location
        assert w.reason
        hits = [c for c in caught
                if issubclass(c.category, UnreachableFilterDroppedWarning)]
        assert len(hits) == 1

    async def test_cross_branch_atomic_warns(self, exec_backend_weak):
        _, engine = exec_backend_weak
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["channel = customers.plans.level"],
        ))
        (w,) = dropped_filter_warnings(resp)
        assert "channel" in w.filter_text

    async def test_ambiguous_path_warns(self, exec_backend_amb):
        """Scenario: ambiguous reverse path stays dropped and warned."""
        _, engine = exec_backend_amb
        resp = await engine.execute(tq(measures=[SM], filters=["effort > 2"]))
        (w,) = dropped_filter_warnings(resp)
        assert "effort" in w.filter_text
        assert w.reason

    async def test_unreachable_filter_warns(self, exec_backend_amb):
        """Scenario: genuinely unreachable filter keeps the established
        behavior — reviews is reachable only through the ambiguous hop."""
        _, engine = exec_backend_amb
        resp = await engine.execute(
            tq(measures=[SM], filters=["reviews.stars > 4"]),
        )
        (w,) = dropped_filter_warnings(resp)
        assert "stars" in w.filter_text
        assert w.reason
