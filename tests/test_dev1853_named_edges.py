"""DEV-1853 — edge names as path segments, in either direction.

A path token matching an incident edge's name traverses that edge from either
endpoint; result keys keep the path as typed; downstream consumers (time
dimensions, ordering, filters, Mode-A SQL) resolve the terminal model from
the resolved edge, never by reading the token as a model name.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1836_fixtures import broadcast_warnings
from tests._dev1853_fixtures import (
    PARALLEL_BILL_TIERS,
    PARALLEL_BILLING_AMOUNTS,
    PARALLEL_BILLING_MONTH_AMOUNTS,
    PARALLEL_BILLING_NAMES,
    PARALLEL_LAST_SPEND_BY_SIGNUP,
    PARALLEL_REROOT_BY_TIER,
    PARALLEL_SHIPPING_NAMES,
    parallel_engine,
    rows_set,
)


@pytest.fixture
async def named_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=True)


class TestNamedHopResolvesAnAmbiguousPair:
    async def test_billing_dimension_and_result_key(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["billing_customer.name"]))
        assert "orders.billing_customer.name" in resp.data[0]
        assert rows_set(resp, "orders.billing_customer.name") == \
            PARALLEL_BILLING_NAMES

    async def test_shipping_edge_is_distinct(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["shipping_customer.name"]))
        assert rows_set(resp, "orders.shipping_customer.name") == \
            PARALLEL_SHIPPING_NAMES


class TestNamesAreDirectionAgnostic:
    async def test_reverse_measure_through_the_billing_edge(
        self, named_engine,
    ) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="customers", dimensions=["name"],
            measures=[{"formula": "billing_customer.amount:sum", "name": "b"}]))
        got = {r["customers.name"]: r["customers.b"] for r in resp.data}
        assert got == PARALLEL_BILLING_AMOUNTS


class TestNamedTerminalMetadata:
    async def test_time_dimension_grouping_and_ordering(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="billing_customer.signup_at"),
                granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "amount:sum", "name": "t"}],
            order=[{"column": "billing_customer.signup_at",
                    "direction": "asc"}]))
        key = "orders.billing_customer.signup_at"
        assert key in resp.data[0]
        months = [str(r[key])[:7] for r in resp.data]
        assert months == sorted(PARALLEL_BILLING_MONTH_AMOUNTS)
        got = {m: r["orders.t"] for m, r in zip(months, resp.data)}
        assert got == PARALLEL_BILLING_MONTH_AMOUNTS
        # Response metadata resolves the terminal column from the resolved
        # edge — the label lives on customers.signup_at.
        meta = resp.attributes.dimensions.get(key)
        assert meta is not None and meta.label == "Signup date"

    async def test_cross_model_aggregate_reroots_over_the_named_edge(
        self, named_engine,
    ) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["billing_customer.tier"],
            measures=[{"formula": "billing_customer.spend:sum",
                       "name": "cs"}]))
        got = {r["orders.billing_customer.tier"]: r["orders.cs"]
               for r in resp.data}
        assert got == PARALLEL_REROOT_BY_TIER
        assert not broadcast_warnings(resp)


class TestNamedEdgeInRankedProducer:
    async def test_last_transform_over_the_named_edge(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders",
            measures=[{"formula":
                       "billing_customer.spend:last(billing_customer.signup_at)",
                       "name": "l"}]))
        assert len(resp.data) == 1
        assert resp.data[0]["orders.l"] == \
            pytest.approx(PARALLEL_LAST_SPEND_BY_SIGNUP)


class TestNamedEdgeInModeA:
    async def test_model_sql_column_over_a_named_edge(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["bill_tier"]))
        assert rows_set(resp, "orders.bill_tier") == PARALLEL_BILL_TIERS


class TestNamedEdgeInFilters:
    async def test_filter_through_the_billing_edge(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["status"],
            filters=["billing_customer.tier = 'gold'"]))
        # Orders billed to Alice (gold): o1 ok, o2 new.
        assert rows_set(resp, "orders.status") == {("ok",), ("new",)}
