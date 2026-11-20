"""DEV-1900 — definition defaults join the home-rule candidates (gap 4).

The home dataset is chosen over every resolved input, definition defaults
included. A source ``customers.regions.pop`` whose aggregation defaults the
weight to ``customers.spend`` has a valid shallower home (``customers``
determines ``pop`` to-one and ``spend`` locally); the explicit spelling already
widens the home, so the definition default must produce the identical plan and
value. A reverse-hop default homes on the dataset across that hop.
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1900_fixtures import (
    HOME_WIDEN_VALUE,
    SlayerQuery,
    home_path_datasets,
    make_exec_engine,
    orders_q,
)

EXPLICIT = QueryMetric(
    metric="wsum_cust_spend(customers.regions.pop, weight=customers.spend)", alias="w")
DEFAULT = QueryMetric(metric="wsum_cust_spend(customers.regions.pop)", alias="w")


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request, datasets=home_path_datasets()):
        yield e


class TestDefinitionDefaultJoinsHomeCandidates:
    async def test_explicit_weight_widens_the_home(self, engine):
        """Reference: an explicit weight=customers.spend already roots the
        aggregate at the shallower customers home."""
        resp = await engine.execute(orders_q(metrics=[EXPLICIT]))
        assert float(resp.data[0]["w"]) == pytest.approx(HOME_WIDEN_VALUE)

    async def test_definition_default_matches_the_explicit(self, engine):
        """Subject: the non-overridden default must join the home candidates the
        same way — identical value to spelling it explicitly."""
        resp = await engine.execute(orders_q(metrics=[DEFAULT]))
        assert float(resp.data[0]["w"]) == pytest.approx(HOME_WIDEN_VALUE)


class TestReverseHopDefaultHomesOnCustomers:
    async def test_back_hop_default_homes_on_customers(self, engine):
        """Rooted at regions, the customers.spend default homes on customers, which determines its region's pop:
        the value equals the same aggregate rooted at customers."""
        from_regions = await engine.execute(SlayerQuery(
            population="regions", metrics=[QueryMetric(metric="wsum_cust_spend(regions.pop)", alias="w")]))
        from_customers = await engine.execute(SlayerQuery(
            population="customers", metrics=[QueryMetric(metric="sum(regions.pop * customers.spend)", alias="w")]))
        assert float(from_regions.data[0]["w"]) == pytest.approx(
            float(from_customers.data[0]["w"]))
