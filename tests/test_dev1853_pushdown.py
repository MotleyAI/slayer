"""DEV-1853 — filter pushdown over the shared walker.

Semi-join correlation resolves through the same bidirectional traversal as
every other hop: no declared reverse join is needed, an ambiguous correlation
hop errors in both modes (never dropped), a named edge carries the
correlation, and reverse round-trip saved measures stay rejected.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1836_fixtures import dropped_filter_warnings
from tests._dev1853_fixtures import (
    CHAIN_PUSHDOWN_OK_SPEND,
    PARALLEL_PUSHDOWN_BILLING_OK_POP,
    chain_engine,
    parallel_engine,
)


@pytest.fixture
async def fwd_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await chain_engine(d)


@pytest.fixture
async def unnamed_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=False)


@pytest.fixture
async def named_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=True)


class TestPushdownWithoutDeclaredReverseJoin:
    async def test_semi_join_over_the_inverted_default_left_edge(
        self, fwd_engine,
    ) -> None:
        resp = await fwd_engine.execute(SlayerQuery(
            source_model="orders", filters=["status = 'ok'"],
            measures=[{"formula": "customers.spend:sum", "name": "cs"}]))
        assert len(resp.data) == 1
        assert resp.data[0]["orders.cs"] == pytest.approx(CHAIN_PUSHDOWN_OK_SPEND)
        assert not dropped_filter_warnings(resp)


class TestAmbiguousCorrelationFailsClosed:
    def _query(self, *, to_many_handling: str = "broadcast") -> SlayerQuery:
        return SlayerQuery(
            source_model="customers",
            measures=[{"formula": "regions.pop:sum", "name": "rp"}],
            filters=["orders.status = 'ok'"],
            to_many_handling=to_many_handling)

    async def test_lenient_mode_errors_instead_of_drop_and_warn(
        self, unnamed_engine,
    ) -> None:
        query = self._query()
        with pytest.raises(AmbiguousJoinPathError) as ei:
            await unnamed_engine.execute(query)
        msg = str(ei.value)
        assert "billing_customer_id" in msg
        assert "shipping_customer_id" in msg

    async def test_error_mode_errors_the_same_way(self, unnamed_engine) -> None:
        query = self._query(to_many_handling="error")
        with pytest.raises(AmbiguousJoinPathError):
            await unnamed_engine.execute(query)


class TestNamedEdgeTargetPathPushesDown:
    async def test_host_filter_pushes_into_a_named_edge_producer(
        self, named_engine,
    ) -> None:
        # The aggregate's target path IS the named edge; the reverse
        # correlation must invert that exact edge, not re-parse "orders"
        # (which is ambiguous across the billing/shipping pair).
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders", filters=["status = 'ok'"],
            measures=[{"formula": "billing_customer.spend:sum",
                       "name": "cs"}]))
        assert len(resp.data) == 1
        # Billing customers with an ok order: Alice (o1) 100 + Bob (o3) 150.
        assert resp.data[0]["orders.cs"] == pytest.approx(250.0)
        assert not dropped_filter_warnings(resp)

    async def test_filter_sharing_the_named_edge_prefix_binds_to_the_chain(
        self, named_engine,
    ) -> None:
        # The filter ref rides the aggregate's named-edge prefix (D3): it must
        # bind to the reverse chain node, not resolve the token as a model.
        resp = await named_engine.execute(SlayerQuery(
            source_model="orders",
            measures=[{"formula": "billing_customer.regions.pop:sum",
                       "name": "rp"}],
            filters=["billing_customer.tier = 'gold'"]))
        assert len(resp.data) == 1
        # Gold billing customers with orders: Alice — North counts once.
        assert resp.data[0]["orders.rp"] == pytest.approx(100.0)


class TestNamedEdgeCarriesTheCorrelation:
    async def test_semi_join_values_through_the_billing_edge(
        self, named_engine,
    ) -> None:
        # Customers with an ok billing order: Alice, Bob — both North.
        # North's pop counts once (never join-multiplied); South is excluded.
        resp = await named_engine.execute(SlayerQuery(
            source_model="customers",
            measures=[{"formula": "regions.pop:sum", "name": "rp"}],
            filters=["billing_customer.status = 'ok'"]))
        assert len(resp.data) == 1
        assert resp.data[0]["customers.rp"] == \
            pytest.approx(PARALLEL_PUSHDOWN_BILLING_OK_POP)
        assert not dropped_filter_warnings(resp)


class TestReverseRoundTripStaysRejected:
    async def test_saved_measure_crossing_back_over_the_reverse_hop(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await chain_engine(d)
            storage = engine.storage
            customers = await storage.get_model("customers", data_source="test")
            assert customers is not None
            customers.measures = [
                *customers.measures,
                ModelMeasure(name="order_total", formula="orders.amount:sum"),
            ]
            await storage.save_model(customers)
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "customers.order_total", "name": "x"}])
            with pytest.raises(ValueError) as ei:
                await engine.execute(query, dry_run=True)
            message = str(ei.value)
            assert "order_total" in message
            assert "orders" in message
            low = message.lower()
            assert ("circular" in low or "round" in low or "revisit" in low
                    or "again" in low)
