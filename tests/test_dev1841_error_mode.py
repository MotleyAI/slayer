"""DEV-1841 task 2.2 — ``to_many_handling: "error"`` replaces the strict gate:
every silent-semantics event the strict flag rejected still errors, now for
local aggregates too; pushed filters and attributable queries still pass.

Spec: openspec …/specs/queries/attribution-modes — "Error mode refuses silent
semantics".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import AmbiguousJoinPathError, SlayerError

from tests._dev1840_fixtures import ambiguity_models
from tests._dev1841_fixtures import (
    BCAST_SPEND_CROSS,
    ModelMeasure,
    SlayerQuery,
    bcast_q,
    cust_q,
    error_q,
    make_exec_engine,
    rows_by,
)

M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")
SM = ModelMeasure(formula="agents.score:sum", name="sm")
SP = ModelMeasure(formula="spend:sum", name="sp")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_amb(request):
    async for engine in make_exec_engine(request, models=ambiguity_models()):
        yield request.param, engine


class TestErrorModeRefusesBroadcast:
    async def test_cross_model_broadcast_errors(self, exec_backend):
        """Scenario: broadcast-would-happen errors — cross-model."""
        _, engine = exec_backend
        query = error_q(dimensions=["status"], measures=[M, CM])
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert "cm" in msg or "spend" in msg
        assert "status" in msg
        assert "cardinality" in msg or "unique" in msg or "associate" in msg

    async def test_local_broadcast_errors(self, exec_backend):
        """Scenario: broadcast-would-happen errors — local (the new coverage:
        a fanning local aggregate errors instead of silently multiplying)."""
        _, engine = exec_backend
        query = cust_q(dimensions=["orders.status"], measures=[SP],
                       to_many_handling="error")
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        assert "status" in str(ei.value)


class TestErrorModeRefusesExcludedFilters:
    async def test_excluded_filter_errors(self, exec_backend):
        """Scenario: excluded filter errors — a mixed disjunction stays dropped
        and error mode turns it into an error naming the filter."""
        _, engine = exec_backend
        query = error_q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.tier = 'gold' OR channel = 'app'"])
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        assert "channel" in str(ei.value)

    async def test_ambiguous_hop_errors_in_error_mode(self, exec_backend_amb):
        """An ambiguous correlation hop fails closed in every mode — not an
        error-mode concern, but it must not be masked by it."""
        _, engine = exec_backend_amb
        query = error_q(source_model="tickets", measures=[SM], filters=["effort > 2"])
        with pytest.raises(AmbiguousJoinPathError):
            await engine.execute(query)


class TestErrorModePasses:
    async def test_pushed_filter_and_clean_query_pass(self, exec_backend):
        """Scenario: pushed filter and clean query pass — a semi-join-pushed
        filter plus a fully attributable grain succeeds, values identical to
        the broadcast run."""
        _, engine = exec_backend
        kw = dict(dimensions=["customers.tier"], measures=[M, CM],
                  filters=["channel = 'app'"])
        strict_resp = await engine.execute(error_q(**kw))
        lenient_resp = await engine.execute(bcast_q(**kw))
        strict_by = rows_by(strict_resp, "orders.customers.tier")
        lenient_by = rows_by(lenient_resp, "orders.customers.tier")
        assert set(strict_by) == set(lenient_by)
        for key, row in strict_by.items():
            assert row["orders.cm"] == lenient_by[key]["orders.cm"]

    async def test_fully_attributable_passes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(
            error_q(dimensions=["customers.tier"], measures=[M, CM]))
        assert {r["orders.customers.tier"] for r in resp.data} >= {"gold", "silver"}

    async def test_explicit_partition_broadcast_does_not_error(self, exec_backend):
        """Scenario: explicit ``partition_by=`` broadcasting must not error even
        in error mode (requested grain, by design)."""
        _, engine = exec_backend
        resp = await engine.execute(error_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=[])", name="total")]))
        for row in resp.data:
            assert float(row["orders.total"]) == pytest.approx(BCAST_SPEND_CROSS)


class TestStrictParity:
    async def test_error_mode_matches_old_strict_semantics(self, exec_backend):
        """The strict flag's behavior is absorbed unchanged: attributable →
        pass with identical values, broadcast → error."""
        _, engine = exec_backend
        ok = await engine.execute(
            error_q(dimensions=["customers.tier"], measures=[CM]))
        assert ok.data
        broadcast_query = error_q(dimensions=["status"], measures=[CM])
        with pytest.raises((SlayerError, ValueError)):
            await engine.execute(broadcast_query)

    async def test_migrated_strict_query_executes_in_error_mode(self, exec_backend):
        """Scenario: stored strict queries migrate and EXECUTE with the mapped
        semantics — a v3 ``strict: true`` payload loads as error mode and errors
        on a broadcast, closing the migration→execution loop."""
        _, engine = exec_backend
        migrated = SlayerQuery.model_validate({
            "version": 3, "source_model": "orders", "strict": True,
            "dimensions": ["status"],
            "measures": [{"formula": "customers.spend:sum", "name": "cm"}],
        })
        assert migrated.to_many_handling == "error"
        with pytest.raises((SlayerError, ValueError)):
            await engine.execute(migrated)
