"""DEV-1428: ``SearchService.search`` is lenient on unresolved refs.

Unresolved entity / memory refs in ``search(entities=...)`` become
warnings rather than raising. ``resolved_input_entities`` shows only
survivors.
"""

from __future__ import annotations

import pytest

from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend


@pytest.fixture
def storage(mydb_orders_storage: StorageBackend) -> StorageBackend:
    return mydb_orders_storage


class TestSearchLenientValidation:
    async def test_unknown_entity_becomes_warning(
        self, storage: StorageBackend,
    ) -> None:
        svc = SearchService(storage=storage)
        resp = await svc.search(
            entities=["mydb.orders.amount", "mydb.orders.does_not_exist"],
        )
        # Unknown entity should NOT raise; should appear as a warning.
        assert any("does_not_exist" in w for w in resp.warnings)
        # Survivor present in resolved.
        assert "mydb.orders.amount" in resp.resolved_input_entities
        assert "mydb.orders.does_not_exist" not in resp.resolved_input_entities

    async def test_unknown_memory_ref_becomes_warning(
        self, storage: StorageBackend,
    ) -> None:
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=["memory:nonexistent"])
        assert any("memory:nonexistent" in w for w in resp.warnings)
        assert resp.resolved_input_entities == []

    async def test_known_memory_ref_resolves(
        self, storage: StorageBackend,
    ) -> None:
        seed = await storage.save_memory(
            learning="seed", entities=["mydb.orders"],
        )
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=[f"memory:{seed.id}"])
        assert f"memory:{seed.id}" in resp.resolved_input_entities

    # ---- DEV-1513: warning text for named-entity surfacing drops -----------

    async def test_off_datasource_named_entity_warning_text(
        self, storage: StorageBackend,
    ) -> None:
        """DEV-1513: a resolved entity not rooted at ``datasource`` is
        dropped from the entities bucket with a specific warning shape."""
        from slayer.core.models import DatasourceConfig, SlayerModel, Column

        await storage.save_datasource(
            DatasourceConfig(name="otherdb", type="sqlite", database=":memory:"),
        )
        await storage.save_model(SlayerModel(
            name="catalog",
            sql_table="catalog",
            data_source="otherdb",
            columns=[Column(name="id", sql="id", primary_key=True)],
        ))
        svc = SearchService(storage=storage)
        resp = await svc.search(
            entities=["otherdb.catalog"],
            datasource="mydb",
            max_results=20,
        )
        entity_hits = [h for h in resp.results if h.kind != "memory"]
        assert entity_hits == []
        assert any(
            "otherdb.catalog" in w
            and "not rooted at datasource 'mydb'" in w
            and "dropped from entities bucket" in w
            for w in resp.warnings
        )
