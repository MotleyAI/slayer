"""DEV-1900 — definition defaults join the home-rule candidates (gap 4).

The home dataset is chosen over every resolved input, definition defaults
included. A source ``customers.regions.pop`` whose aggregation defaults the
weight to ``customers.spend`` has a valid shallower home (``customers``
determines ``pop`` to-one and ``spend`` locally); the explicit spelling already
widens the home, so the definition default must produce the identical plan and
value. A reverse-hop default with no forward home stays failing closed
(reverse-hop cancellation is DEV-1908).
"""

from __future__ import annotations

import pytest

from tests._dev1900_fixtures import (
    HOME_WIDEN_VALUE,
    ModelMeasure,
    SlayerQuery,
    home_path_models,
    make_exec_engine,
    orders_q,
)

EXPLICIT = ModelMeasure(
    formula="customers.regions.pop:wsum_cust_spend(weight=customers.spend)", name="w")
DEFAULT = ModelMeasure(formula="customers.regions.pop:wsum_cust_spend", name="w")


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request, models=home_path_models()):
        yield e


class TestDefinitionDefaultJoinsHomeCandidates:
    async def test_explicit_weight_widens_the_home(self, engine):
        """Reference: an explicit weight=customers.spend already roots the
        aggregate at the shallower customers home."""
        resp = await engine.execute(orders_q(measures=[EXPLICIT]))
        assert float(resp.data[0]["orders.w"]) == pytest.approx(HOME_WIDEN_VALUE)

    async def test_definition_default_matches_the_explicit(self, engine):
        """Subject: the non-overridden default must join the home candidates the
        same way — identical value to spelling it explicitly."""
        resp = await engine.execute(orders_q(measures=[DEFAULT]))
        assert float(resp.data[0]["orders.w"]) == pytest.approx(HOME_WIDEN_VALUE)


class TestReverseHopDefaultFailsClosed:
    async def test_back_hop_default_stays_failing_closed(self, engine):
        """Rooted at regions there is no forward home for the customers.spend
        default; reverse-hop cancellation is DEV-1908, so it must fail closed."""
        q = SlayerQuery(
            source_model="regions",
            measures=[ModelMeasure(formula="pop:wsum_cust_spend", name="w")])
        with pytest.raises(ValueError, match="unproven join hop"):
            await engine.execute(q)
