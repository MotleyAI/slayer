"""DEV-1853 — ambiguous hops fail closed in both directions.

Two or more edges between the same pair of models make a bare model-name hop
an error naming every candidate; the silent forward first-match is retired.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.models import Column
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1853_fixtures import parallel_engine


@pytest.fixture
async def unnamed_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        yield await parallel_engine(d, named=False)


def _assert_names_both_candidates(msg: str) -> None:
    assert "billing_customer_id" in msg
    assert "shipping_customer_id" in msg


class TestParallelEdgesFailClosed:
    async def test_forward_dimension_no_longer_first_match(
        self, unnamed_engine,
    ) -> None:
        query = SlayerQuery(
            source_model="orders", dimensions=["customers.name"])
        with pytest.raises(AmbiguousJoinPathError) as ei:
            await unnamed_engine.execute(query)
        _assert_names_both_candidates(str(ei.value))

    async def test_reverse_hop_names_the_same_candidates(
        self, unnamed_engine,
    ) -> None:
        query = SlayerQuery(
            source_model="customers", dimensions=["orders.status"])
        with pytest.raises(AmbiguousJoinPathError) as ei:
            await unnamed_engine.execute(query)
        _assert_names_both_candidates(str(ei.value))

    async def test_error_carries_the_candidate_edges(self, unnamed_engine) -> None:
        query = SlayerQuery(
            source_model="orders", dimensions=["customers.name"])
        with pytest.raises(AmbiguousJoinPathError) as ei:
            await unnamed_engine.execute(query)
        assert len(ei.value.candidates) == 2

    async def test_filter_path_fails_closed(self, unnamed_engine) -> None:
        query = SlayerQuery(
            source_model="orders", dimensions=["status"],
            filters=["customers.tier = 'gold'"])
        with pytest.raises(AmbiguousJoinPathError):
            await unnamed_engine.execute(query)

    async def test_measure_path_fails_closed(self, unnamed_engine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.spend:sum", "name": "s"}])
        with pytest.raises(AmbiguousJoinPathError):
            await unnamed_engine.execute(query)


class TestModeAAmbiguity:
    async def test_model_sql_over_an_ambiguous_pair_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await parallel_engine(d, named=False)
            storage = engine.storage
            orders = await storage.get_model("orders", data_source="test")
            assert orders is not None
            orders.columns = [
                *orders.columns,
                Column(name="cust_tier", sql="customers.tier"),
            ]
            await storage.save_model(orders)
            query = SlayerQuery(source_model="orders", dimensions=["cust_tier"])
            with pytest.raises(AmbiguousJoinPathError):
                await engine.execute(query)
