"""DEV-1853 — ambiguous hops fail closed in both directions.

Two or more edges between the same pair of models make a bare model-name hop
an error naming every candidate; the silent forward first-match is retired.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.errors import AmbiguousRelationshipPathError, DerivedFieldAmbiguousError
from slayer.core.models import DatasetField
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
            population="orders", dimensions=["customers.name"])
        with pytest.raises(AmbiguousRelationshipPathError) as ei:
            await unnamed_engine.execute(query)
        _assert_names_both_candidates(str(ei.value))

    async def test_reverse_hop_names_the_same_candidates(
        self, unnamed_engine,
    ) -> None:
        query = SlayerQuery(
            population="customers", dimensions=["orders.status"])
        with pytest.raises(AmbiguousRelationshipPathError) as ei:
            await unnamed_engine.execute(query)
        _assert_names_both_candidates(str(ei.value))

    async def test_error_carries_the_candidate_edges(self, unnamed_engine) -> None:
        query = SlayerQuery(
            population="orders", dimensions=["customers.name"])
        with pytest.raises(AmbiguousRelationshipPathError) as ei:
            await unnamed_engine.execute(query)
        assert len(ei.value.candidates) == 2

    async def test_filter_path_fails_closed(self, unnamed_engine) -> None:
        query = SlayerQuery(
            population="orders", dimensions=["status"],
            filter=["customers.tier = 'gold'"])
        with pytest.raises(AmbiguousRelationshipPathError):
            await unnamed_engine.execute(query)

    async def test_measure_path_fails_closed(self, unnamed_engine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "sum(customers.spend)", "name": "s"}],
        })
        with pytest.raises(AmbiguousRelationshipPathError):
            await unnamed_engine.execute(query)


class TestModeAAmbiguity:
    async def test_model_sql_over_an_ambiguous_pair_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await parallel_engine(d, named=False)
            storage = engine.storage
            orders = await storage.get_dataset("orders", data_source="test")
            assert orders is not None
            orders.fields = [
                *orders.fields,
                DatasetField(name="cust_tier", sql="customers.tier"),
            ]
            with pytest.raises(DerivedFieldAmbiguousError):
                await storage.save_dataset(orders)
            # A definition stored without validation (migration write-back) still fails closed at query time.
            await storage.save_dataset(orders, _validate=False)
            query = SlayerQuery(population="orders", dimensions=["cust_tier"])
            with pytest.raises(AmbiguousRelationshipPathError):
                await engine.execute(query)
