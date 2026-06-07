"""Tests for idempotent ``ingest_datasource_idempotent``. See DEV-1356.

The function:

* Adds new columns / joins / tables that appeared in the live schema since
  the last ingest.
* Never overwrites existing column / join definitions.
* Skips ``sql``-mode and query-backed models.
* Returns a combined ``IdempotentIngestResult`` with ``additions``,
  ``to_delete`` (verbatim ``validate_models`` output), and per-model
  ``errors``.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable, List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.engine.ingestion import (
    _refresh_datasource_embeddings,
    ingest_datasource_idempotent,
)
from slayer.engine.schema_drift import (
    IdempotentIngestResult,
    ModelAddition,
    WholeModelDelete,
    EditModelDelete,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _create_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            region TEXT NOT NULL
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            customer_id INTEGER REFERENCES customers(id)
        );
        INSERT INTO customers VALUES (1, 'US'), (2, 'EU');
        INSERT INTO orders VALUES (1, 100.0, 'completed', 1);
        """
    )
    conn.commit()
    conn.close()


async def _setup(workspace: Path, *, persist_models: bool = True) -> tuple:
    db_path = str(workspace / "live.db")
    _create_schema(db_path)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
    await storage.save_datasource(ds)
    if persist_models:
        # Run ingest once so that subsequent calls test idempotency.
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
    return storage, ds, db_path


def _addition_for(name: str, additions: Iterable[ModelAddition]):
    for a in additions:
        if a.model_name == name:
            return a
    return None


# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_re_run_on_identical_schema_is_no_op(self, workspace: Path) -> None:
        storage, ds, _ = await _setup(workspace)
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert isinstance(result, IdempotentIngestResult)
        # Nothing new
        for addition in result.additions:
            assert addition.created is False
            assert addition.new_columns == []
            assert addition.new_joins == []
        assert result.to_delete == []
        assert result.errors == []


class TestAdditive:
    async def test_new_column_appends_to_existing_model(
        self, workspace: Path
    ) -> None:
        storage, ds, db_path = await _setup(workspace)
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders ADD COLUMN delivery_address TEXT")
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("orders", result.additions)
        assert addition is not None
        assert "delivery_address" in addition.new_columns
        assert addition.created is False
        # Persisted model now has the new column
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert any(c.name == "delivery_address" for c in loaded.columns)

    async def test_new_table_creates_model(self, workspace: Path) -> None:
        storage, ds, db_path = await _setup(workspace)
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)"
        )
        conn.execute("INSERT INTO products VALUES (1, 'A1')")
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("products", result.additions)
        assert addition is not None
        assert addition.created is True
        assert {"id", "sku"} <= set(addition.new_columns)
        loaded = await storage.get_model("products", data_source="ds")
        assert loaded is not None
        assert {c.name for c in loaded.columns} == {"id", "sku"}

    async def test_new_fk_creates_join(self, workspace: Path) -> None:
        # Build a DB with no FK initially, then add one. SQLite doesn't
        # support adding FK to an existing table via ALTER, so this test
        # builds a scenario where the orders.customer_id exists but no
        # FK was defined initially.
        db_path = str(workspace / "live.db")
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT NOT NULL);
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                customer_id INTEGER
            );
            INSERT INTO customers VALUES (1, 'US');
            INSERT INTO orders VALUES (1, 100.0, 1);
            """
        )
        conn.commit()
        conn.close()
        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        # Persist orders model with no joins
        await storage.save_model(
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="ds",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                    ),
                    Column(name="region", sql="region", type=DataType.TEXT),
                ],
            )
        )
        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                    ),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                    Column(
                        name="customer_id",
                        sql="customer_id",
                        type=DataType.DOUBLE,
                    ),
                ],
                joins=[],
            )
        )
        # Recreate orders with an FK now via DROP + CREATE (SQLite has no
        # ALTER to add FKs).
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE orders_new (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO orders_new SELECT * FROM orders;
            DROP TABLE orders;
            ALTER TABLE orders_new RENAME TO orders;
            """
        )
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert any(j.target_model == "customers" for j in loaded.joins)
        addition = _addition_for("orders", result.additions)
        assert addition is not None
        assert "customers" in addition.new_joins


class TestPreservation:
    async def test_existing_column_metadata_preserved(self, workspace: Path) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Mutate the persisted orders model: customize amount's metadata
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        for c in loaded.columns:
            if c.name == "amount":
                c.description = "Order amount in USD"
                c.label = "Amount"
                c.format = NumberFormat(type=NumberFormatType.CURRENCY)
                c.meta = {"unit": "usd"}
        await storage.save_model(loaded)

        # Add a new live column to trigger the additive pass.
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders ADD COLUMN extra TEXT")
        conn.commit()
        conn.close()

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        loaded2 = await storage.get_model("orders", data_source="ds")
        assert loaded2 is not None
        amount = next(c for c in loaded2.columns if c.name == "amount")
        # Untouched: customizations preserved
        assert amount.description == "Order amount in USD"
        assert amount.label == "Amount"
        assert amount.format is not None
        assert amount.format.type == NumberFormatType.CURRENCY
        assert amount.meta == {"unit": "usd"}
        # New column appended
        assert any(c.name == "extra" for c in loaded2.columns)


class TestSkipNonSqlTableModes:
    async def test_sql_mode_model_skipped(self, workspace: Path) -> None:
        storage, ds, _ = await _setup(workspace, persist_models=False)
        await storage.save_model(
            SlayerModel(
                name="custom_orders",
                sql="SELECT id, amount FROM orders WHERE amount > 0",
                data_source="ds",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                    ),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            )
        )
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # custom_orders is sql-mode — additive pass must not touch it.
        loaded = await storage.get_model("custom_orders", data_source="ds")
        assert loaded is not None
        assert loaded.sql is not None
        assert _addition_for("custom_orders", result.additions) is None

    async def test_query_backed_model_skipped(self, workspace: Path) -> None:
        storage, ds, _ = await _setup(workspace)
        # Save a query-backed model that references orders. Use the engine
        # so the cache populates correctly.
        from slayer.engine.query_engine import SlayerQueryEngine

        engine = SlayerQueryEngine(storage=storage)
        await engine.create_model_from_query(
            query=SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum", "name": "total"}],
            ),
            name="orders_summary",
        )
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert _addition_for("orders_summary", result.additions) is None
        # And the persisted query-backed model is unchanged.
        loaded = await storage.get_model("orders_summary", data_source="ds")
        assert loaded is not None
        assert loaded.source_queries is not None


class TestCombinedReturnShape:
    async def test_type_drift_surfaces_in_to_delete_not_additions(
        self, workspace: Path
    ) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Persisted orders.amount = NUMBER. Mutate live so it returns text:
        # SQLite has dynamic typing, so we drop and recreate as TEXT.
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE orders_new (
                id INTEGER PRIMARY KEY,
                amount TEXT NOT NULL,
                status TEXT NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO orders_new (id, amount, status, customer_id)
                SELECT id, CAST(amount AS TEXT), status, customer_id FROM orders;
            DROP TABLE orders;
            ALTER TABLE orders_new RENAME TO orders;
            """
        )
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # No additions for this drift — the additive pass simply skips the
        # name (it is taken in the persisted model) and lets validate_models
        # surface the bucket mismatch.
        addition = _addition_for("orders", result.additions)
        assert addition is None or "amount" not in addition.new_columns
        # And the to_delete payload lists the bucket-mismatched column.
        orders_drops = [
            e for e in result.to_delete if e.model_name == "orders"
        ]
        assert orders_drops, "expected validate_models to flag orders.amount"
        assert isinstance(orders_drops[0], (EditModelDelete, WholeModelDelete))


class TestErrorIsolation:
    async def test_per_model_save_failure_captured_in_errors(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Add two new tables so the additive pass tries to save two models.
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE a_new (id INTEGER PRIMARY KEY, x TEXT)")
        conn.execute("CREATE TABLE b_new (id INTEGER PRIMARY KEY, y TEXT)")
        conn.commit()
        conn.close()

        # Patch save_model on the storage to fail for "a_new" only.
        original_save = storage.save_model

        async def flaky_save(model):
            if model.name == "a_new":
                raise RuntimeError("disk full")
            return await original_save(model)

        monkeypatch.setattr(storage, "save_model", flaky_save)

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # One success (b_new), one failure (a_new)
        assert _addition_for("b_new", result.additions) is not None
        assert any(e.model_name == "a_new" for e in result.errors)


class TestMemoryEmbeddingRefresh:
    """DEV-1416: `_refresh_datasource_embeddings` must walk memories whose
    entities are rooted at the datasource, in addition to models and the
    datasource doc. Failures attribute to the offending memory by id."""

    @staticmethod
    def _enable_channel(monkeypatch: pytest.MonkeyPatch) -> List[List[str]]:
        """Override the conftest autouse fixture for this test. Returns a
        list that captures every ``embed_batch`` call's text payload."""
        monkeypatch.setattr(embedding_client, "is_available", lambda: True)
        calls: List[List[str]] = []

        async def fake_embed_batch(  # NOSONAR(S7503) — must be `async def` to match the patched embed_batch signature
            texts: List[str], *, model: Optional[str] = None,
        ) -> List[Optional[List[float]]]:
            calls.append(list(texts))
            return [[0.1, 0.2, 0.3] for _ in texts]

        monkeypatch.setattr(
            "slayer.search.retrievers.embeddings.embed_batch", fake_embed_batch,
        )
        return calls

    async def test_refreshes_only_memories_rooted_at_datasource(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage, ds, _ = await _setup(workspace)
        # Second datasource for negative-case memories. We don't actually
        # ingest it — it just needs to exist as a config for the entity
        # strings to be distinguishable from `ds`.
        other = DatasourceConfig(name="other_ds", type="sqlite", database=":memory:")
        await storage.save_datasource(other)

        m_in_ds = await storage.save_memory(
            learning="rooted at ds",
            entities=[f"{ds.name}.orders.amount"],
        )
        m_in_other = await storage.save_memory(
            learning="rooted at other_ds only",
            entities=["other_ds.customers.name"],
        )
        m_spanning = await storage.save_memory(
            learning="spans both",
            entities=[f"{ds.name}.customers.region", "other_ds.regions.country"],
        )

        self._enable_channel(monkeypatch)

        warnings = await _refresh_datasource_embeddings(
            datasource_name=ds.name, storage=storage,
        )
        assert warnings == []

        rows = await storage.list_embeddings(
            embedding_model_name=embedding_client.current_model(),
        )
        memory_ids = {
            r.canonical_id for r in rows if r.entity_kind == "memory"
        }
        assert f"memory:{m_in_ds.id}" in memory_ids
        assert f"memory:{m_spanning.id}" in memory_ids
        assert f"memory:{m_in_other.id}" not in memory_ids

    async def test_second_pass_is_noop_when_content_unchanged(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage, ds, _ = await _setup(workspace)
        saved = await storage.save_memory(
            learning="stable",
            entities=[f"{ds.name}.orders.amount"],
        )
        calls = self._enable_channel(monkeypatch)

        await _refresh_datasource_embeddings(
            datasource_name=ds.name, storage=storage,
        )
        first_pass_call_count = len(calls)
        first_pass_total_texts = sum(len(c) for c in calls)

        # No content changed → no new embed calls.
        await _refresh_datasource_embeddings(
            datasource_name=ds.name, storage=storage,
        )
        assert len(calls) == first_pass_call_count
        assert sum(len(c) for c in calls) == first_pass_total_texts

        # And the memory row is present after both passes.
        rows = await storage.list_embeddings(
            embedding_model_name=embedding_client.current_model(),
        )
        assert any(
            r.canonical_id == f"memory:{saved.id}" for r in rows
        )

    async def test_per_memory_failure_surfaces_with_memory_id_in_ingest_result(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage, ds, _ = await _setup(workspace)
        saved = await storage.save_memory(
            learning="will fail to embed",
            entities=[f"{ds.name}.orders.amount"],
        )
        self._enable_channel(monkeypatch)

        # Force `refresh_memory` to return the per-row failure warning
        # shape that `_apply_pending` would normally emit when
        # `embed_batch` returns None.
        async def failing_refresh(self, memory):  # NOSONAR(S7503) — replaces async upsert_memory
            return [
                f"embedding refresh failed for memory:{memory.id}; "
                f"skipped (search will still find this entity via tantivy + BM25)."
            ]

        monkeypatch.setattr(
            "slayer.search.retrievers.embeddings."
            "EmbeddingRetriever.upsert_memory",
            failing_refresh,
        )

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage,
        )
        memory_errors = [
            e for e in result.errors
            if e.model_name == f"memory:{saved.id}"
        ]
        assert memory_errors, (
            f"expected an IngestionError with "
            f"model_name='memory:{saved.id}'; got {result.errors!r}"
        )
        assert memory_errors[0].data_source == ds.name
        assert memory_errors[0].error.startswith("embedding refresh:")
        assert f"memory:{saved.id}" in memory_errors[0].error

    async def test_refresh_memory_raise_is_caught_and_tagged(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The defensive try/except in the memory loop must convert a
        raise from ``refresh_memory`` into a tagged ``(model_name,
        error_text)`` tuple — never propagate."""
        storage, ds, _ = await _setup(workspace)
        saved = await storage.save_memory(
            learning="raises on refresh",
            entities=[f"{ds.name}.orders.amount"],
        )
        self._enable_channel(monkeypatch)

        async def raising_refresh(self, memory):  # NOSONAR(S7503) — replaces async upsert_memory
            raise RuntimeError("boom")

        monkeypatch.setattr(
            "slayer.search.retrievers.embeddings."
            "EmbeddingRetriever.upsert_memory",
            raising_refresh,
        )

        warnings = await _refresh_datasource_embeddings(
            datasource_name=ds.name, storage=storage,
        )
        assert any(
            model_name == f"memory:{saved.id}" and "boom" in err
            for model_name, err in warnings
        ), warnings

    async def test_extra_not_installed_silent_noop_on_memory_path(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When ``is_available()`` returns False (extra not installed or
        no API key), the memory loop must be silent — no warnings, no
        embedding rows."""
        storage, ds, _ = await _setup(workspace)
        await storage.save_memory(
            learning="unused", entities=[f"{ds.name}.orders.amount"],
        )
        # The autouse `_disable_embedding_channel_by_default` fixture
        # already forces is_available=False; assert explicitly for
        # clarity.
        assert embedding_client.is_available() is False

        called: List[List[str]] = []

        async def should_not_be_called(  # NOSONAR(S7503) — must be `async def` to match the patched embed_batch signature
            texts: List[str], *, model: Optional[str] = None,
        ) -> List[Optional[List[float]]]:
            called.append(list(texts))
            return [None for _ in texts]

        monkeypatch.setattr(
            "slayer.search.retrievers.embeddings.embed_batch", should_not_be_called,
        )

        warnings = await _refresh_datasource_embeddings(
            datasource_name=ds.name, storage=storage,
        )
        assert warnings == []
        assert called == []
        rows = await storage.list_embeddings(
            embedding_model_name=embedding_client.current_model(),
        )
        assert [r for r in rows if r.entity_kind == "memory"] == []

    async def test_save_embeddings_failure_during_memory_persist_attributes_to_memory_id(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Codex Finding 1: when ``save_embeddings`` raises while
        persisting a memory's embedding, the resulting warning carries
        the memory's canonical id so ``ingest_datasource_idempotent``
        can route the failure to ``IngestionError(model_name="memory:<id>")``
        rather than the unattributed ``model_name=""`` bucket."""
        storage, ds, _ = await _setup(workspace)
        saved = await storage.save_memory(
            learning="row that will fail to persist",
            entities=[f"{ds.name}.orders.amount"],
        )
        self._enable_channel(monkeypatch)

        # Patch the storage backend so save_embeddings raises only for
        # the memory canonical id; model + datasource-doc rows persist
        # normally so we can pin that this is the only failure.
        original_save = storage.save_embeddings

        async def selective_save(rows):
            if any(r.canonical_id == f"memory:{saved.id}" for r in rows):
                raise RuntimeError("disk full")
            await original_save(rows)

        monkeypatch.setattr(storage, "save_embeddings", selective_save)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage,
        )
        memory_errors = [
            e for e in result.errors
            if e.model_name == f"memory:{saved.id}"
        ]
        assert memory_errors, (
            f"expected an IngestionError with "
            f"model_name='memory:{saved.id}'; got {result.errors!r}"
        )
        assert "disk full" in memory_errors[0].error
        assert f"memory:{saved.id}" in memory_errors[0].error

    async def test_list_memories_failure_warns_and_continues(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A raise inside ``storage.list_memories`` must be captured as a
        single warning tuple — the datasource pass must still complete
        without propagating."""
        storage, ds, _ = await _setup(workspace)
        self._enable_channel(monkeypatch)

        async def boom(self, *, entities=None):  # NOSONAR(S7503) — async list_memories signature
            raise RuntimeError("memories table missing")

        monkeypatch.setattr(
            "slayer.storage.base.StorageBackend.list_memories", boom,
        )

        warnings = await _refresh_datasource_embeddings(
            datasource_name=ds.name, storage=storage,
        )
        assert any(
            "memories table missing" in err for _, err in warnings
        ), warnings


class TestExcludeTables:
    async def test_excluded_tables_not_touched(self, workspace: Path) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Add a column to orders, but exclude orders from the pass.
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders ADD COLUMN extra TEXT")
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, exclude_tables=["orders"]
        )
        # No addition for orders
        assert _addition_for("orders", result.additions) is None
        # to_delete also doesn't reference orders
        assert not any(e.model_name == "orders" for e in result.to_delete)
        # Persisted orders model is unchanged (no extra column appended)
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert not any(c.name == "extra" for c in loaded.columns)
