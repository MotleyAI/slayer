"""Keyless cross-dataset partitioned aggregates in filter/order position.

A computed dimension may band a cross-dataset partitioned aggregate whose
partition key is not a query dimension (keyless grain). Filtering on that same
aggregate row-routes like the local shape; ordering by the dimension's name
sorts by the banded value; consuming the keyless aggregate in a second
(metric / raw ORDER BY) role fails with the partition-key error. Run on
SQLite + DuckDB.
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1865_fixtures import q

# gold spend=150, silver spend=200 → only silver clears 175 ('hi').
CM_BAND = (
    "CASE WHEN sum(customers.spend, partition_by=customers.tier) > 175 "
    "THEN 'hi' ELSE 'lo' END"
)
KEYLESS_AGG = "sum(customers.spend, partition_by=customers.tier)"
RK_REGION = "customers.regions.name"




class TestKeylessFilterRowRoutes:
    async def test_filter_on_own_keyless_aggregate_executes(self, exec_engine) -> None:
        dims = [{"field": CM_BAND, "alias": "band"}, "customers.regions.name"]
        unfiltered = (await exec_engine.execute(q(
            dimensions=dims,
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
        ))).data
        filtered = (await exec_engine.execute(q(
            dimensions=dims,
            filter=[f"{KEYLESS_AGG} > 175"],
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
        ))).data
        assert 0 < len(filtered) <= len(unfiltered)
        # Row-routed against the attached tier-total: only silver rows survive → 'hi'.
        assert all(r["band"] == "hi" for r in filtered)
        # Exact: the lone surviving customer is c2 (silver, RegN, spend 200),
        # value-preserving against the unfiltered (hi, RegN) group.
        by = {(r["band"], r[RK_REGION]): r for r in filtered}
        assert set(by) == {("hi", "RegN")}
        unfiltered_by = {(r["band"], r[RK_REGION]): r for r in unfiltered}
        assert float(by[("hi", "RegN")]["sp"]) == pytest.approx(
            float(unfiltered_by[("hi", "RegN")]["sp"])
        )
        assert float(by[("hi", "RegN")]["sp"]) == pytest.approx(200.0)


class TestKeylessOrderByDimensionName:
    async def test_order_by_computed_dim_name_executes(self, exec_engine) -> None:
        dims = [{"field": CM_BAND, "alias": "band"}, "customers.regions.name"]
        resp = await exec_engine.execute(q(
            dimensions=dims,
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
            order=[{"field": "band", "direction": "asc"}],
        ))
        bands = [r["band"] for r in resp.data]
        assert bands == sorted(bands)


class TestKeylessDualRoleRejected:
    async def test_second_role_needs_partition_key_among_dims(self, exec_engine) -> None:
        # The keyless aggregate banded by the computed dim, also selected as a
        # metric while its partition key (tier) is absent → partition-key error.
        query = q(
            dimensions=[{"field": CM_BAND, "alias": "band"}, "customers.regions.name"],
            metrics=[QueryMetric(metric=KEYLESS_AGG, alias="dual")],
        )
        with pytest.raises(
            ValueError,
            match="partition_by field 'customers.tier' is not a query dimension",
        ):
            await exec_engine.execute(query)
