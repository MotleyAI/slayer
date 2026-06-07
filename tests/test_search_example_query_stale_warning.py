"""DEV-1428: example_query stale-Memory.query warning.

When an example_queries result's attached ``Memory.query`` references an
entity that has since vanished, the search service emits a warning naming
the memory and still surfaces the memory + its stored query unchanged.
"""

from __future__ import annotations

import pytest

from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend


@pytest.fixture
def storage(mydb_orders_storage: StorageBackend) -> StorageBackend:
    return mydb_orders_storage


class TestExampleQueryStaleWarning:
    async def test_stale_query_emits_warning_but_surfaces_memory(
        self, storage: StorageBackend,
    ) -> None:
        # Save a memory carrying a query referencing the existing column,
        # then remove the referenced column so the query is now stale.
        attached = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        )
        seed = await storage.save_memory(
            learning="paid revenue",
            entities=["mydb.orders.amount"],
            query=attached,
        )
        # Drop the referenced column, leaving the memory's Memory.query
        # dangling.
        existing = await storage.get_model("orders", data_source="mydb")
        assert existing is not None
        updated = existing.model_copy(
            update={"columns": [c for c in existing.columns if c.name != "amount"]}
        )
        await storage.save_model(updated)

        svc = SearchService(storage=storage)
        resp = await svc.search(question="paid revenue", max_results=20)
        example_queries = [h for h in resp.results if h.kind == "memory" and h.query is not None]
        if not example_queries:
            resp = await svc.search(question="revenue", max_results=20)
            example_queries = [h for h in resp.results if h.kind == "memory" and h.query is not None]
        # The query-bearing memory must still surface.
        eq_ids = [eq.id for eq in example_queries]
        assert str(seed.id) in eq_ids, (
            f"expected memory {seed.id!r} in example_queries; got {eq_ids}"
        )
        # And its attached query must be unchanged (we don't rewrite).
        eq = next(e for e in example_queries if e.id == str(seed.id))
        assert eq.query.source_model == "orders"
        assert eq.query.measures is not None
        assert eq.query.measures[0].formula == "amount:sum"
        # And a warning naming the memory must be present.
        memory_warnings = [
            w for w in resp.warnings if "memory:" in w and "stale" in w.lower()
        ]
        assert memory_warnings, (
            f"expected a stale-query warning, got: {resp.warnings}"
        )
