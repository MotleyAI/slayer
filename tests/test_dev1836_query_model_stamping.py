"""DEV-1836 task 1.2 — DEV-1689: query-backed models carry provable uniqueness
(design D5).

Spec: openspec …/specs/models/join-cardinality — "Query-backed models carry
their provable uniqueness". ``create_model_from_query`` stamps the result
grain's uniqueness only when the backing query provably deduplicates it:
it aggregates, or is dimension-only with ``distinct_dimension_values=True``.
"""

from __future__ import annotations

import pytest

from slayer.core.models import ModelJoin
from slayer.engine.join_safety import provably_to_one

from tests._dev1836_fixtures import (
    AMOUNT_BY_STATUS,
    ModelMeasure,
    make_exec_engine,
    orders_model,
    q,
)


@pytest.fixture(params=["sqlite"])
async def engine(request):
    async for eng in make_exec_engine(request):
        yield eng


def _pk_names(model) -> set[str]:
    return {c.name for c in model.columns if c.primary_key}


class TestStamping:
    async def test_aggregated_query_stamps_composite_grain(self, engine) -> None:
        model = await engine.create_model_from_query(
            q(dimensions=["status", "channel"],
              measures=[ModelMeasure(formula="amount:sum", name="total")]),
            "vm_agg",
        )
        assert _pk_names(model) == {"status", "channel"}

    async def test_dimension_only_distinct_stamps_grain(self, engine) -> None:
        model = await engine.create_model_from_query(
            q(dimensions=["status"]), "vm_dims",
        )
        assert _pk_names(model) == {"status"}

    async def test_duplicate_preserving_query_stamps_nothing(self, engine) -> None:
        """F2 negative — ``distinct_dimension_values=False`` preserves
        duplicates, so no uniqueness claim may be stamped."""
        model = await engine.create_model_from_query(
            q(dimensions=["status"], distinct_dimension_values=False),
            "vm_dup",
        )
        assert _pk_names(model) == set()
        assert not any(c.unique for c in model.columns)


class TestStampedModelProvesJoins:
    """Scenario: an aggregated backing query proves N:1 joins onto it."""

    async def test_join_onto_complete_grain_is_provable(self, engine) -> None:
        model = await engine.create_model_from_query(
            q(dimensions=["status", "channel"],
              measures=[ModelMeasure(formula="amount:sum", name="total")]),
            "vm_kpis",
        )
        full = ModelJoin(
            target_model="vm_kpis",
            join_pairs=[["status", "status"], ["channel", "channel"]],
        )
        assert provably_to_one(join=full, target_model=model)

    async def test_join_onto_partial_grain_stays_unproven(self, engine) -> None:
        model = await engine.create_model_from_query(
            q(dimensions=["status", "channel"],
              measures=[ModelMeasure(formula="amount:sum", name="total")]),
            "vm_kpis2",
        )
        partial = ModelJoin(
            target_model="vm_kpis2", join_pairs=[["status", "status"]],
        )
        assert not provably_to_one(join=partial, target_model=model)

    async def test_metric_through_stamped_grain_keeps_exact_values(
        self, engine,
    ) -> None:
        """Scenario's executed clause: a cross-model metric over the proven
        join onto the query-backed model keeps exact per-dimension values."""
        await engine.create_model_from_query(
            q(dimensions=["status", "channel"],
              measures=[ModelMeasure(formula="amount:sum", name="total")]),
            "vm_grain",
        )
        host = orders_model()
        host.name = "orders_kpi"
        host.joins.append(ModelJoin(
            target_model="vm_grain",
            join_pairs=[["status", "status"], ["channel", "channel"]],
        ))
        await engine.storage.save_model(host, _validate=False)
        resp = await engine.execute(q(
            source_model="orders_kpi", dimensions=["status"],
            measures=[ModelMeasure(formula="vm_grain.total:sum", name="t")],
        ))
        got = {r["orders_kpi.status"]: float(r["orders_kpi.t"])
               for r in resp.data}
        assert len(resp.data) == 2
        assert got == pytest.approx(AMOUNT_BY_STATUS)

    async def test_unstamped_model_proves_nothing(self, engine) -> None:
        model = await engine.create_model_from_query(
            q(dimensions=["status"], distinct_dimension_values=False),
            "vm_dup2",
        )
        join = ModelJoin(target_model="vm_dup2", join_pairs=[["status", "status"]])
        assert not provably_to_one(join=join, target_model=model)
