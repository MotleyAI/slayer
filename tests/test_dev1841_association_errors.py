"""Associate-mode eligibility: windowed / first-last aggregates associate; a root without a unique key associates
through its row identity.

Spec: queries/attribution-modes — "Association eligibility, input handling and row identity".
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1914_fixtures import LAST_BY_STATUS_SIGNUP, WINDOW_90D_BY_STATUS_SIGNUP, known_status_signup
from tests._dev1841_fixtures import (
    ASSOC_SPEND_BY_STATUS,
    TimeDimension,
    TimeGranularity,
    assoc_q,
    field_default_agg_datasets,
    crossing_input_datasets,
    error_q,
    keyless_root_datasets,
    make_exec_engine,
    status_key,
)
from tests._dev1892_fixtures import (
    ASSOC_WAVG_SPEND_BY_STATUS,
    ASSOC_WSUM_BY_STATUS,
)

CM = QueryMetric(metric="sum(customers.spend)", alias="cm")
_SIGNUP_TD = [TimeDimension(
    field="customers.signup_at",
    granularity=TimeGranularity.MONTH)]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_keyless(request):
    async for engine in make_exec_engine(request, datasets=keyless_root_datasets()):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_crossing(request):
    async for engine in make_exec_engine(request, datasets=crossing_input_datasets()):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend_coldef(request):
    async for engine in make_exec_engine(request, datasets=field_default_agg_datasets()):
        yield request.param, engine


class TestRankedAndWindowedCombinations:
    async def test_first_last_with_association_associates(self, exec_backend):
        """A first/last aggregate that needs association picks among each cell's distinct entities."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["orders.status"], time_dimensions=_SIGNUP_TD,
            metrics=[QueryMetric(metric="last(customers.spend)", alias="lastspend")]))
        assert known_status_signup(resp, "lastspend") == LAST_BY_STATUS_SIGNUP

    async def test_window_with_association_associates(self, exec_backend):
        """A ``window=`` aggregate that needs association counts each entity once per frame."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["orders.status"], time_dimensions=_SIGNUP_TD,
            metrics=[QueryMetric(metric="sum(customers.spend, window='90d')", alias="win")]))
        assert known_status_signup(resp, "win") == WINDOW_90D_BY_STATUS_SIGNUP


class TestMissingUniqueKey:
    async def test_root_without_unique_key_associates(self, exec_backend_keyless):
        """A keyless root is deduplicated by its per-query row identity."""
        _, engine = exec_backend_keyless
        resp = await engine.execute(assoc_q(dimensions=["orders.status"], metrics=[CM]))
        by = status_key(resp)
        for status, expected in ASSOC_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["cm"]) == pytest.approx(expected)


class TestUnsafeInputInAssociate:
    """Input safety is mode-invariant: a crossing-hop input rejected on the broadcast/error path is rejected before association too."""

    VIP = QueryMetric(metric="sum(customers.vip_spend)", alias="vip")

    async def test_crossing_input_rejected_in_associate(self, exec_backend_crossing):
        _, engine = exec_backend_crossing
        error_query = error_q(dimensions=["orders.status"], metrics=[self.VIP])
        assoc_query = assoc_q(dimensions=["orders.status"], metrics=[self.VIP])
        # Reference: the broadcast/error path already rejects the unsafe input.
        with pytest.raises(ValueError, match="unproven relationship hop"):
            await engine.execute(error_query)
        # The associate path must inherit the identical rejection.
        with pytest.raises(ValueError, match="unproven relationship hop"):
            await engine.execute(assoc_query)


class TestColumnReferenceParameter:
    """A field-reference parameter is picked once per distinct entity: the explicit
    weight and the definition default both execute."""

    WAVG = QueryMetric(
        metric="weighted_avg(customers.spend, weight=customers.spend)", alias="wavg")
    WSUM = QueryMetric(metric="wsum(customers.spend)", alias="wsum")

    async def test_explicit_column_param_executes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(dimensions=["orders.status"], metrics=[self.WAVG]))
        by = status_key(resp)
        for status, expected in ASSOC_WAVG_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["wavg"]) == pytest.approx(expected)

    async def test_column_default_param_executes(self, exec_backend_coldef):
        _, engine = exec_backend_coldef
        resp = await engine.execute(assoc_q(dimensions=["orders.status"], metrics=[self.WSUM]))
        by = status_key(resp)
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(by[(status,)]["wsum"]) == pytest.approx(expected)
