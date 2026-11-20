"""Query-backed datasets carry provable uniqueness.

Spec: openspec …/specs/models/join-cardinality — "Query-backed models carry
their provable uniqueness". ``create_dataset_from_query`` stamps the result
grain's uniqueness only when the backing query provably deduplicates it:
it aggregates, or is dimension-only with ``distinct_dimension_values=True``.
"""

from __future__ import annotations

import pytest

from slayer.core.models import DatasetRelationship
from slayer.core.relationship_walker import provably_to_one

from slayer.core.query import QueryMetric

from tests._dev1836_fixtures import (
    AMOUNT_BY_STATUS,
    make_exec_engine,
    orders_dataset,
    q,
)
from slayer.core.models import SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite"])
async def engine(request):
    async for eng in make_exec_engine(request):
        yield eng


def _pk_names(dataset: SlayerDataset) -> set[str]:
    return {c.name for c in dataset.fields if c.primary_key}


class TestStamping:
    async def test_aggregated_query_stamps_composite_grain(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(
            q(dimensions=["orders.status", "orders.channel"],
              metrics=[QueryMetric(metric="sum(orders.amount)", alias="total")]),
            "vm_agg",
        )
        assert _pk_names(model) == {"status", "channel"}

    async def test_dimension_only_distinct_stamps_grain(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(
            q(dimensions=["orders.status"]), "vm_dims",
        )
        # A single-field grain is unique, not a primary key (not an identifier).
        assert _pk_names(model) == set()
        assert {c.name for c in model.fields if c.unique} == {"status"}

    async def test_duplicate_preserving_query_stamps_nothing(self, engine: SlayerQueryEngine) -> None:
        """F2 negative — ``distinct_dimension_values=False`` preserves
        duplicates, so no uniqueness claim may be stamped."""
        model = await engine.create_dataset_from_query(
            q(dimensions=["orders.status"], distinct_dimension_values=False),
            "vm_dup",
        )
        assert _pk_names(model) == set()
        assert not any(c.unique for c in model.fields)


class TestStampedModelProvesJoins:
    """Scenario: an aggregated backing query proves N:1 joins onto it."""

    async def test_join_onto_complete_grain_is_provable(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(
            q(dimensions=["orders.status", "orders.channel"],
              metrics=[QueryMetric(metric="sum(orders.amount)", alias="total")]),
            "vm_kpis",
        )
        full = DatasetRelationship(
            to="vm_kpis",
            from_columns=["status", "channel"], to_columns=["status", "channel"],
        )
        assert provably_to_one(edge=full, target_dataset=model)

    async def test_join_onto_partial_grain_stays_unproven(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(
            q(dimensions=["orders.status", "orders.channel"],
              metrics=[QueryMetric(metric="sum(orders.amount)", alias="total")]),
            "vm_kpis2",
        )
        partial = DatasetRelationship(
            to="vm_kpis2", from_columns=["status"], to_columns=["status"],
        )
        assert not provably_to_one(edge=partial, target_dataset=model)

    async def test_metric_through_stamped_grain_keeps_exact_values(
        self, engine: SlayerQueryEngine,
    ) -> None:
        """Scenario's executed clause: a cross-dataset metric over the proven
        relationship onto the query-backed dataset keeps exact per-dimension values."""
        await engine.create_dataset_from_query(
            q(dimensions=["orders.status", "orders.channel"],
              metrics=[QueryMetric(metric="sum(orders.amount)", alias="total")]),
            "vm_grain",
        )
        host = orders_dataset()
        host.name = "orders_kpi"
        host.relationships.append(DatasetRelationship(
            to="vm_grain",
            from_columns=["status", "channel"], to_columns=["status", "channel"],
        ))
        await engine.storage.save_dataset(host, _validate=False)
        resp = await engine.execute(q(
            population="orders_kpi", dimensions=["orders_kpi.status"],
            metrics=[QueryMetric(metric="sum(vm_grain.total)", alias="t")],
        ))
        got = {r["orders_kpi.status"]: float(r["t"])
               for r in resp.data}
        assert len(resp.data) == 2
        assert got == pytest.approx(AMOUNT_BY_STATUS)

    async def test_unstamped_model_proves_nothing(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(
            q(dimensions=["orders.status"], distinct_dimension_values=False),
            "vm_dup2",
        )
        join = DatasetRelationship(to="vm_dup2", from_columns=["status"], to_columns=["status"])
        assert not provably_to_one(edge=join, target_dataset=model)
