"""DEV-1841 task 3.4 — associate-mode eligibility: windowed / first-last
combinations and a root without a unique key fail with a clear typed
``SlayerError`` (specified behavior, outside the NotImplementedError ratchet).

Spec: openspec …/specs/queries/attribution-modes — "Association eligibility and
input handling", scenarios "Windowed or first/last combination fails closed"
and "Root without a unique key fails closed".
"""

from __future__ import annotations

import re

import pytest

from slayer.core.errors import SlayerError

from tests._dev1841_fixtures import (
    ColumnRef,
    ModelMeasure,
    TimeDimension,
    TimeGranularity,
    assoc_q,
    column_default_agg_models,
    crossing_input_models,
    error_q,
    keyless_root_models,
    make_exec_engine,
)

CM = ModelMeasure(formula="customers.spend:sum", name="cm")
#: Must name the association concept — a generic ranked/time-dimension rejection
#: (which would fire before association eligibility) must NOT satisfy it.
_COMBO = re.compile(r"associat|attribut|distinct.entity|combination", re.IGNORECASE)
_SIGNUP_TD = [TimeDimension(
    dimension=ColumnRef(name="customers.signup_at"),
    granularity=TimeGranularity.MONTH)]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_keyless(request):
    async for engine in make_exec_engine(request, models=keyless_root_models()):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_crossing(request):
    async for engine in make_exec_engine(request, models=crossing_input_models()):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_coldef(request):
    async for engine in make_exec_engine(request, models=column_default_agg_models()):
        yield request.param, engine


class TestUnsupportedCombinations:
    async def test_first_last_with_association_fails(self, exec_backend):
        """Scenario: windowed or first/last combination fails closed — a
        first/last aggregate that also needs association is a typed error. The
        signup_at time dimension makes the ranking otherwise valid, so the sole
        failure is the unsupported association combination."""
        _, engine = exec_backend
        query = assoc_q(
            dimensions=["status"], time_dimensions=_SIGNUP_TD,
            measures=[ModelMeasure(formula="customers.spend:last", name="lastspend")])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(query)
        assert _COMBO.search(str(ei.value))

    async def test_window_with_association_fails(self, exec_backend):
        """A ``window=`` aggregate that also needs association is a typed
        error — never a silently wrong value."""
        _, engine = exec_backend
        query = assoc_q(
            dimensions=["status"], time_dimensions=_SIGNUP_TD,
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='90d')", name="win")])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(query)
        assert _COMBO.search(str(ei.value))


class TestMissingUniqueKey:
    async def test_root_without_unique_key_fails(self, exec_backend_keyless):
        """Scenario: root without a unique key fails closed — association cannot
        dedup entities, so it errors naming the model and the remedy."""
        _, engine = exec_backend_keyless
        query = assoc_q(dimensions=["status"], measures=[CM])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert "customers" in msg
        assert "key" in msg.lower()


class TestUnsafeInputInAssociate:
    """Input safety is mode-invariant: a crossing-hop input rejected on the broadcast/error path is rejected before association too."""

    VIP = ModelMeasure(formula="customers.vip_spend:sum", name="vip")

    async def test_crossing_input_rejected_in_associate(self, exec_backend_crossing):
        _, engine = exec_backend_crossing
        error_query = error_q(dimensions=["status"], measures=[self.VIP])
        assoc_query = assoc_q(dimensions=["status"], measures=[self.VIP])
        # Reference: the broadcast/error path already rejects the unsafe input.
        with pytest.raises(ValueError, match="unproven join hop"):
            await engine.execute(error_query)
        # The associate path must inherit the identical rejection.
        with pytest.raises(ValueError, match="unproven join hop"):
            await engine.execute(assoc_query)


class TestColumnReferenceParameter:
    """A column-reference aggregate parameter can't ride the deduped ``_base``, so association rejects it loudly (explicit kwarg and definition default)."""

    WAVG = ModelMeasure(
        formula="customers.spend:weighted_avg(weight=customers.spend)", name="wavg")
    WSUM = ModelMeasure(formula="customers.spend:wsum", name="wsum")

    async def test_column_param_rejected_in_associate(self, exec_backend):
        _, engine = exec_backend
        query = assoc_q(dimensions=["status"], measures=[self.WAVG])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(query)
        assert "parameter" in str(ei.value).lower()

    async def test_column_default_param_rejected_in_associate(self, exec_backend_coldef):
        _, engine = exec_backend_coldef
        query = assoc_q(dimensions=["status"], measures=[self.WSUM])
        with pytest.raises(SlayerError) as ei:
            await engine.execute(query)
        assert "parameter" in str(ei.value).lower()
