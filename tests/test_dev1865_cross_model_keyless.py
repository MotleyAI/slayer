"""DEV-1865 — keyless cross-model partitioned aggregates in filter/order position.

A computed dimension may band a cross-model partitioned aggregate whose
partition key is not a query dimension (keyless grain). Filtering on that same
aggregate row-routes like the local shape; ordering by the dimension's name
sorts by the banded value; consuming the keyless aggregate in a second
(measure / raw ORDER BY) role fails with the partition-key error. Run on
SQLite + DuckDB.
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import ModelMeasure, q

# gold spend=150, silver spend=200 → only silver clears 175 ('hi').
CM_BAND = (
    "CASE WHEN customers.spend:sum(partition_by=customers.tier) > 175 "
    "THEN 'hi' ELSE 'lo' END"
)
KEYLESS_AGG = "customers.spend:sum(partition_by=customers.tier)"
RK_REGION = "orders.customers.regions.name"




class TestKeylessFilterRowRoutes:
    async def test_filter_on_own_keyless_aggregate_executes(self, exec_engine) -> None:
        dims = [{"expression": CM_BAND, "name": "band"}, "customers.regions.name"]
        unfiltered = (await exec_engine.execute(q(
            dimensions=dims,
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
        ))).data
        filtered = (await exec_engine.execute(q(
            dimensions=dims,
            filters=[f"{KEYLESS_AGG} > 175"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
        ))).data
        assert 0 < len(filtered) <= len(unfiltered)
        # Row-routed against the attached tier-total: only silver rows survive → 'hi'.
        assert all(r["orders.band"] == "hi" for r in filtered)
        # Exact: the lone surviving customer is c2 (silver, RegN, spend 200),
        # value-preserving against the unfiltered (hi, RegN) group.
        by = {(r["orders.band"], r[RK_REGION]): r for r in filtered}
        assert set(by) == {("hi", "RegN")}
        unfiltered_by = {(r["orders.band"], r[RK_REGION]): r for r in unfiltered}
        assert float(by[("hi", "RegN")]["orders.sp"]) == pytest.approx(
            float(unfiltered_by[("hi", "RegN")]["orders.sp"])
        )
        assert float(by[("hi", "RegN")]["orders.sp"]) == pytest.approx(200.0)


class TestKeylessOrderByDimensionName:
    async def test_order_by_computed_dim_name_executes(self, exec_engine) -> None:
        dims = [{"expression": CM_BAND, "name": "band"}, "customers.regions.name"]
        resp = await exec_engine.execute(q(
            dimensions=dims,
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
            order=[{"column": "band", "direction": "asc"}],
        ))
        bands = [r["orders.band"] for r in resp.data]
        assert bands == sorted(bands)


class TestKeylessDualRoleRejected:
    async def test_second_role_needs_partition_key_among_dims(self, exec_engine) -> None:
        # The keyless aggregate banded by the computed dim, also selected as a
        # measure while its partition key (tier) is absent → partition-key error.
        query = q(
            dimensions=[{"expression": CM_BAND, "name": "band"}, "customers.regions.name"],
            measures=[ModelMeasure(formula=KEYLESS_AGG, name="dual")],
        )
        with pytest.raises(
            ValueError,
            match="partition_by column 'customers.tier' is not a query dimension",
        ):
            await exec_engine.execute(query)
