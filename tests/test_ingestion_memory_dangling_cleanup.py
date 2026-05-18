"""DEV-1428: defense-in-depth ingest-time dangling-ref cleanup.

``_refresh_memories_for_datasource`` walks each memory and drops refs
that no longer resolve in storage. Transient lookup failures must NOT
drop refs (raise treated as "ref intact"). Stale ``Memory.query``
emits an ``IngestionError`` rather than rewriting the query.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


# Live-datasource introspection raises one of these when the seeded
# SQLite file is empty / missing the ``orders`` table the test model
# claims to back. The test's intent is to validate the memory cleanup
# pass, not the ingest itself, so we suppress these specific errors
# (and re-raise everything else, so assertion / logic regressions are
# never masked — CodeRabbit review on PR #130).
_EXPECTED_INGEST_FAILURES = (SAOperationalError, sqlite3.OperationalError)


@pytest.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        await s.save_datasource(
            DatasourceConfig(
                name="mydb", type="sqlite",
                database=os.path.join(tmpdir, "live.db"),
            )
        )
        await s.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="mydb",
                columns=[
                    Column(name="id", sql="id", primary_key=True),
                    Column(name="amount", sql="amount"),
                ],
            )
        )
        yield s


class TestIngestDanglingRefCleanup:
    async def test_residual_refs_cleaned(
        self, storage: StorageBackend,
    ) -> None:
        # Hand-inject a memory with a stale model ref.
        m = await storage.save_memory(
            learning="x",
            entities=["mydb.orders.amount", "mydb.deleted_model"],
        )
        # Run the datasource ingest pass; cleanup happens inline.
        try:
            ds = await storage.get_datasource("mydb")
            assert ds is not None
            await ingest_datasource_idempotent(
                storage=storage, datasource=ds,
            )
        except _EXPECTED_INGEST_FAILURES:
            # Ingestion may fail because of missing live table, but the
            # memory cleanup pass should still run / surface independently.
            pass
        loaded = await storage.get_memory(m.id)
        assert "mydb.orders.amount" in loaded.entities
        # The residual cleanup pass should strip the stale ref.
        assert "mydb.deleted_model" not in loaded.entities

    async def test_transient_failure_keeps_refs(
        self, storage: StorageBackend, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A would-be-stale ref under a transient lookup failure must be
        preserved — a raise during existence-check is treated as "ref
        intact" so transient infra hiccups don't drop data."""
        m = await storage.save_memory(
            learning="x",
            # ``mydb.deleted_model`` would normally get stripped, but
            # under a faulting lookup the cleanup pass must abstain.
            entities=["mydb.orders.amount", "mydb.deleted_model"],
        )
        ds = await storage.get_datasource("mydb")
        assert ds is not None
        # Force every cleanup-side get_model lookup to raise — the
        # cleanup pass must treat the raise as "ref intact". Patch the
        # specific storage instance rather than the ABC so the
        # JoinSyncStorage wrapper's override is the one we replace.
        async def _raise(*args, **kwargs):
            raise RuntimeError("transient")

        monkeypatch.setattr(storage, "get_model", _raise)
        try:
            await ingest_datasource_idempotent(
                storage=storage, datasource=ds,
            )
        except (_EXPECTED_INGEST_FAILURES + (RuntimeError,)):
            # The patched ``get_model`` raises RuntimeError; the live
            # ingest can additionally hit the missing-table sqlite
            # errors. Anything else means an assertion regression and
            # MUST fail the test.
            pass
        # Restore so the post-test get_memory lookup works.
        monkeypatch.undo()
        loaded = await storage.get_memory(m.id)
        # Both refs survive: cleanup never had a definitive answer.
        assert "mydb.orders.amount" in loaded.entities
        assert "mydb.deleted_model" in loaded.entities

    async def test_stale_query_emits_ingestion_error(
        self, storage: StorageBackend,
    ) -> None:
        # Memory carries Memory.query referencing the existing column;
        # drop the column to make the query stale.
        attached = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        )
        await storage.save_memory(
            learning="x",
            entities=["mydb.orders.amount"],
            query=attached,
        )
        existing = await storage.get_model("orders", data_source="mydb")
        assert existing is not None
        await storage.save_model(
            existing.model_copy(
                update={
                    "columns": [c for c in existing.columns if c.name != "amount"],
                },
            )
        )
        ds = await storage.get_datasource("mydb")
        assert ds is not None
        # Capture the seeded memory id by listing — only one memory exists.
        seeded = (await storage.list_memories())[0]
        result = await ingest_datasource_idempotent(
            storage=storage, datasource=ds,
        )
        # An IngestionError tagged ``memory:<id>`` should be present.
        memory_errors = [
            e for e in result.errors if "memory:" in e.model_name
        ]
        assert memory_errors, (
            f"expected an IngestionError for stale Memory.query; "
            f"got: {result.errors}"
        )
        # The stale Memory.query must be LEFT ALONE — warning-only policy
        # per the plan. Agents who discover the broken query re-save it.
        reloaded = await storage.get_memory(seeded.id)
        assert reloaded.query is not None
        assert reloaded.query.source_model == "orders"
        assert reloaded.query.measures is not None
        assert reloaded.query.measures[0].formula == "amount:sum"
