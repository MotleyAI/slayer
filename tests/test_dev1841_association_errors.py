"""Associate-mode eligibility: windowed / first-last combinations fail with a clear typed
``SlayerError``; a root without a unique key associates through its row identity.

Spec: queries/attribution-modes — "Association eligibility, input handling and row identity".
"""

from __future__ import annotations

import re

import pytest

from slayer.core.errors import SlayerError

from tests._dev1841_fixtures import (
    ASSOC_SPEND_BY_STATUS,
    ModelMeasure,
    TimeDimension,
    TimeGranularity,
    assoc_q,
    column_default_agg_models,
    crossing_input_models,
    error_q,
    keyless_root_models,
    make_exec_engine,
    status_key,
)
from tests._dev1892_fixtures import (
    ASSOC_WAVG_SPEND_BY_STATUS,
    ASSOC_WSUM_BY_STATUS,
)

CM = ModelMeasure(formula="customers.spend:sum", name="cm")
#: Must name the association concept — a generic ranked/time-dimension rejection
#: (which would fire before association eligibility) must NOT satisfy it.
_COMBO = re.compile(r"associat|attribut|distinct.entity|combination", re.IGNORECASE)
_SIGNUP_TD = [TimeDimension(
    dimension="customers.signup_at",
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
    async def test_root_without_unique_key_associates(self, exec_backend_keyless):
        """A keyless root is deduplicated by its per-query row identity."""
        _, engine = exec_backend_keyless
        resp = await engine.execute(assoc_q(dimensions=["status"], measures=[CM]))
        by = status_key(resp)
        for status, expected in ASSOC_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.cm"]) == pytest.approx(expected)


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
    """A column-reference parameter is picked once per distinct entity: the explicit
    weight and the definition default both execute."""

    WAVG = ModelMeasure(
        formula="customers.spend:weighted_avg(weight=customers.spend)", name="wavg")
    WSUM = ModelMeasure(formula="customers.spend:wsum", name="wsum")

    async def test_explicit_column_param_executes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(dimensions=["status"], measures=[self.WAVG]))
        by = status_key(resp)
        for status, expected in ASSOC_WAVG_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.wavg"]) == pytest.approx(expected)

    async def test_column_default_param_executes(self, exec_backend_coldef):
        _, engine = exec_backend_coldef
        resp = await engine.execute(assoc_q(dimensions=["status"], measures=[self.WSUM]))
        by = status_key(resp)
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(by[(status,)]["orders.wsum"]) == pytest.approx(expected)
