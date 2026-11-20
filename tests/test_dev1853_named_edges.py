"""Edge names as path segments, in either direction.

A path token matching an incident edge's name traverses that edge from either
endpoint; result keys keep the path as typed; downstream consumers (time
dimensions, ordering, filters, Mode-A SQL) resolve the terminal dataset from
the resolved edge, never by reading the token as a dataset name.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetMetric
from slayer.core.query import SlayerQuery, TimeDimension
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
            population="orders", dimensions=["orders.billing_customer.name"]))
        assert "orders.billing_customer.name" in resp.data[0]
        assert rows_set(resp, "orders.billing_customer.name") == \
            PARALLEL_BILLING_NAMES

    async def test_shipping_edge_is_distinct(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            population="orders", dimensions=["orders.shipping_customer.name"]))
        assert rows_set(resp, "orders.shipping_customer.name") == \
            PARALLEL_SHIPPING_NAMES


class TestNamesAreDirectionAgnostic:
    async def test_reverse_measure_through_the_billing_edge(
        self, named_engine,
    ) -> None:
        resp = await named_engine.execute(SlayerQuery.model_validate({
            "population": "customers",
            "dimensions": ["customers.name"],
            "metrics": [{"metric": "sum(customers.billing_customer.amount)", "alias": "b"}],
        }))
        got = {r["customers.name"]: r["b"] for r in resp.data}
        assert got == PARALLEL_BILLING_AMOUNTS


class TestNamedTerminalMetadata:
    async def test_time_dimension_grouping_and_ordering(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [TimeDimension(
                field="orders.billing_customer.signup_at",
                granularity=TimeGranularity.MONTH)],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "t"}],
            "order": [{"field": "orders.billing_customer.signup_at",
                    "direction": "asc"}],
        }))
        key = "orders.billing_customer.signup_at"
        assert key in resp.data[0]
        months = [str(r[key])[:7] for r in resp.data]
        assert months == sorted(PARALLEL_BILLING_MONTH_AMOUNTS)
        got = {m: r["t"] for m, r in zip(months, resp.data)}
        assert got == PARALLEL_BILLING_MONTH_AMOUNTS
        # Response metadata resolves the terminal field from the resolved
        # edge — the label lives on customers.signup_at.
        meta = resp.attributes.dimensions.get(key)
        assert meta is not None
        assert meta.label == "Signup date"

    async def test_cross_model_aggregate_reroots_over_the_named_edge(
        self, named_engine,
    ) -> None:
        resp = await named_engine.execute(SlayerQuery(
            population="orders", dimensions=["orders.billing_customer.tier"],
            metrics=[{"metric": "sum(orders.billing_customer.spend)",
                       "alias": "cs"}]))
        got = {r["orders.billing_customer.tier"]: r["cs"]
               for r in resp.data}
        assert got == PARALLEL_REROOT_BY_TIER
        assert not broadcast_warnings(resp)


class TestNamedEdgeInRankedProducer:
    async def test_last_transform_over_the_named_edge(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            population="orders",
            metrics=[{"metric":
                       "last(orders.billing_customer.spend, orders.billing_customer.signup_at)",
                       "alias": "l"}]))
        assert len(resp.data) == 1
        assert resp.data[0]["l"] == \
            pytest.approx(PARALLEL_LAST_SPEND_BY_SIGNUP)


class TestNamedEdgeInModeA:
    async def test_model_sql_column_over_a_named_edge(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            population="orders", dimensions=["orders.bill_tier"]))
        assert rows_set(resp, "orders.bill_tier") == PARALLEL_BILL_TIERS


class TestNamedEdgeInFilters:
    async def test_filter_through_the_billing_edge(self, named_engine) -> None:
        resp = await named_engine.execute(SlayerQuery(
            population="orders", dimensions=["orders.status"],
            filter=["orders.billing_customer.tier = 'gold'"]))
        # Orders billed to Alice (gold): o1 ok, o2 new.
        assert rows_set(resp, "orders.status") == {("ok",), ("new",)}


class TestNamedEdgeValidationConsumers:
    """Validation/metadata consumers resolve path tokens through the shared
    walker — a named-edge token must not silently skip them."""

    @staticmethod
    async def _augment_customers(engine: SlayerQueryEngine) -> None:
        """Add a filtered field, an agg restriction, and a reverse metric."""
        storage = engine.storage
        cust = await storage.get_dataset("customers", data_source="test")
        assert cust is not None
        cust.fields = [*cust.fields, DatasetField(
            name="gold_spend", sql="spend", filter="tier = 'gold'",
            datatype=DataType.FLOAT), DatasetField(
            name="tier_uc", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="UPPER(tier)")]), datatype=DataType.STRING), DatasetField(
            name="signup_day", sql="signup_at", datatype=DataType.DATETIME)]
        for col in cust.fields:
            if col.name == "spend":
                col.allowed_aggregations = ["sum"]
        cust.metrics = [*cust.metrics, DatasetMetric(
            name="total_b", expression="sum(billing_customer.amount)")]
        await storage.save_dataset(cust)

    async def test_column_filter_applies_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        resp = await named_engine.execute(SlayerQuery(
            population="orders",
            metrics=[{"metric": "sum(orders.billing_customer.gold_spend)",
                       "alias": "g"}]))
        # Gold customers only (Alice 100 + Cara 60), not the full 310.
        assert resp.data[0]["g"] == 160.0

    async def test_disallowed_aggregation_rejected_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "avg(orders.billing_customer.spend)", "alias": "a"}],
        })
        with pytest.raises(AggregationNotAllowedError):
            await named_engine.execute(query)

    async def test_round_trip_measure_composes_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "orders.billing_customer.total_b", "alias": "r"}]})
        # A second ``orders`` occurrence over every order (10 + 20 + 30), as from ``customers``.
        resp = await named_engine.execute(query)
        assert resp.data[0]["r"] == 60.0

    async def test_derived_dimension_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        resp = await named_engine.execute(SlayerQuery(
            population="orders", dimensions=["orders.billing_customer.tier_uc"]))
        assert rows_set(resp, "orders.billing_customer.tier_uc") == \
            {("GOLD",), ("SILVER",)}

    async def test_derived_time_dimension_over_the_named_edge(
        self, named_engine,
    ) -> None:
        await self._augment_customers(named_engine)
        resp = await named_engine.execute(SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [TimeDimension(
                field="orders.billing_customer.signup_day",
                granularity=TimeGranularity.MONTH)],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "t"}],
        }))
        months = {str(r["orders.billing_customer.signup_day"])[:7]: r["t"]
                  for r in resp.data}
        assert months == PARALLEL_BILLING_MONTH_AMOUNTS

    async def test_order_by_a_non_projected_named_edge_column(
        self, named_engine,
    ) -> None:
        """The hidden host-grain MIN/MAX order wrap walks the named edge."""
        resp = await named_engine.execute(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "t"}],
            "order": [{"field": "orders.billing_customer.name", "direction": "desc"}],
        }))
        # Desc by each group's max billing name: ok (Bob) before new (Alice).
        assert [r["orders.status"] for r in resp.data] == ["ok", "new"]
        assert [r["t"] for r in resp.data] == [40.0, 20.0]
