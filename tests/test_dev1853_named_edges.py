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

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.models import Column, ModelMeasure
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
        assert meta is not None
        assert meta.label == "Signup date"

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


class TestNamedEdgeValidationConsumers:
    """Validation/metadata consumers resolve path tokens through the shared
    walker — a named-edge token must not silently skip them."""

    @staticmethod
    async def _augment_customers(engine: SlayerQueryEngine) -> None:
        """Add a filtered column, an agg restriction, and a reverse measure."""
        storage = engine.storage
        cust = await storage.get_model("customers", data_source="test")
        assert cust is not None
        cust.columns = [*cust.columns, Column(
            name="gold_spend", sql="spend", filter="tier = 'gold'",
            type=DataType.DOUBLE), Column(
            name="tier_uc", sql="UPPER(tier)", type=DataType.TEXT), Column(
            name="signup_day", sql="signup_at", type=DataType.TIMESTAMP)]
        for col in cust.columns:
            if col.name == "spend":
                col.allowed_aggregations = ["sum"]
        cust.measures = [*cust.measures, ModelMeasure(
            name="total_b", formula="billing_customer.amount:sum")]
        await storage.save_model(cust)

    async def test_column_filter_applies_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders",
            measures=[{"formula": "billing_customer.gold_spend:sum",
                       "name": "g"}]))
        # Gold customers only (Alice 100 + Cara 60), not the full 310.
        assert resp.data[0]["orders.g"] == 160.0

    async def test_disallowed_aggregation_rejected_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "billing_customer.spend:avg", "name": "a"}])
        with pytest.raises(AggregationNotAllowedError):
            await named_engine.execute(query)

    async def test_round_trip_measure_rejected_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "billing_customer.total_b", "name": "r"}])
        with pytest.raises(ValueError, match="Round-trip"):
            await named_engine.execute(query)

    async def test_derived_dimension_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["billing_customer.tier_uc"]))
        assert rows_set(resp, "orders.billing_customer.tier_uc") == \
            {("GOLD",), ("SILVER",)}

    async def test_derived_time_dimension_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="billing_customer.signup_day"),
                granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "amount:sum", "name": "t"}]))
        months = {str(r["orders.billing_customer.signup_day"])[:7]: r["orders.t"]
                  for r in resp.data}
        assert months == PARALLEL_BILLING_MONTH_AMOUNTS

    async def test_order_by_a_non_projected_named_edge_column(
        self, named_engine,
    ) -> None:
        """The hidden host-grain MIN/MAX order wrap walks the named edge."""
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "t"}],
            order=[{"column": "billing_customer.name", "direction": "desc"}]))
        # Desc by each group's max billing name: ok (Bob) before new (Alice).
        assert [r["orders.status"] for r in resp.data] == ["ok", "new"]
        assert [r["orders.t"] for r in resp.data] == [40.0, 20.0]
