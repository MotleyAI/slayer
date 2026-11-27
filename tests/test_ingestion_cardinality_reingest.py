"""Idempotent re-ingest must persist metadata-only cardinality/unique fills."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.live_schema.schema_drift import IdempotentIngestResult
from slayer.database.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


async def _ingest(*, datasource: DatasourceConfig, **kwargs: Any) -> IdempotentIngestResult:
    client = SlayerSQLClient(datasource=datasource)
    try:
        return await ingest_datasource_idempotent(datasource=datasource, client=client, **kwargs)
    finally:
        await client.aclose()
        client.close()


def _create_schema(db_path: str) -> None:
    with transaction(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE,
                region TEXT NOT NULL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO customers VALUES (1, 'a@x.com', 'US');
            INSERT INTO orders VALUES (1, 100.0, 1);
            """
        )


async def _setup(workspace: Path) -> tuple[YAMLStorage, DatasourceConfig]:
    db_path = str(workspace / "live.db")
    _create_schema(db_path)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
    await storage.save_datasource(ds)
    await _ingest(datasource=ds, storage=storage)
    return storage, ds


def _order_relationship(dataset: SlayerDataset):
    return next(j for j in dataset.relationships if j.to == "customers")


class TestFillsMetadataAndPersists:
    async def test_reingest_refills_none_cardinality_and_persists(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # Simulate legacy persisted data: relationship without a cardinality.
        orders = await storage.get_dataset("orders", data_source="ds")
        assert orders is not None
        orders.relationships[0] = _order_relationship(orders).model_copy(update={"cardinality": None})
        await storage.save_dataset(orders)

        await _ingest(datasource=ds, storage=storage)

        reloaded = await storage.get_dataset("orders", data_source="ds")
        assert reloaded is not None
        # Refilled AND persisted (reloaded from storage proves the save).
        assert _order_relationship(reloaded).cardinality is RelationshipCardinality.MANY_TO_ONE

    async def test_reingest_sets_unique_and_persists(self, workspace: Path) -> None:
        storage, ds = await _setup(workspace)

        customers = await storage.get_dataset("customers", data_source="ds")
        assert customers is not None
        email = next(c for c in customers.fields if c.name == "email")
        idx = customers.fields.index(email)
        customers.fields[idx] = email.model_copy(update={"unique": False})
        customers.enforced_keys = [k for k in customers.enforced_keys if k != ["email"]]  # a legacy store records none
        await storage.save_dataset(customers)

        await _ingest(datasource=ds, storage=storage)

        reloaded = await storage.get_dataset("customers", data_source="ds")
        assert reloaded is not None
        email = next(c for c in reloaded.fields if c.name == "email")
        assert email.unique is True


class TestAdditiveContract:
    async def test_reingest_does_not_overwrite_user_cardinality(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # A deliberate user override that disagrees with the structural guess.
        orders = await storage.get_dataset("orders", data_source="ds")
        assert orders is not None
        orders.relationships[0] = _order_relationship(orders).model_copy(
            update={"cardinality": RelationshipCardinality.ONE_TO_ONE}
        )
        await storage.save_dataset(orders)

        await _ingest(datasource=ds, storage=storage)

        reloaded = await storage.get_dataset("orders", data_source="ds")
        assert reloaded is not None
        assert _order_relationship(reloaded).cardinality is RelationshipCardinality.ONE_TO_ONE

    async def test_reingest_does_not_downgrade_user_unique(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # User marked a field unique that has no DB constraint.
        customers = await storage.get_dataset("customers", data_source="ds")
        assert customers is not None
        region = next(c for c in customers.fields if c.name == "region")
        idx = customers.fields.index(region)
        customers.fields[idx] = region.model_copy(update={"unique": True})
        await storage.save_dataset(customers)

        await _ingest(datasource=ds, storage=storage)

        reloaded = await storage.get_dataset("customers", data_source="ds")
        assert reloaded is not None
        region = next(c for c in reloaded.fields if c.name == "region")
        assert region.unique is True


class TestLegacyJoinTargetNormalisation:
    """A ``__`` relationship target is a LEGAL dataset name now, so a relationship to a
    faithfully-named ``__`` dataset is preserved across re-ingest — never rewritten
    to a sanitized spelling. (Genuine old-world healing — a raw target with no
    dataset but a sanitized one that does — is handled by the [C2] adopt-stored-name
    pre-pass in ``ingest_datasource_idempotent`` plus ``_repair_legacy_relationship_targets``.)
    """

    async def test_dunder_join_target_is_preserved_across_reingest(
        self, workspace: Path
    ) -> None:
        db = str(workspace / "legacy.db")
        with transaction(db) as conn:
            conn.executescript(
                """
                CREATE TABLE reports__patient__drug (id INTEGER PRIMARY KEY);
                CREATE TABLE visits (
                    id INTEGER PRIMARY KEY,
                    report_id INTEGER REFERENCES reports__patient__drug(id)
                );
                """
            )

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db)
        await storage.save_datasource(ds)
        await _ingest(datasource=ds, storage=storage)

        # The faithful target is what fresh ingest already wrote.
        visits = await storage.get_dataset("visits", data_source="ds")
        assert [j.to for j in visits.relationships] == ["reports__patient__drug"]

        await _ingest(datasource=ds, storage=storage)

        reloaded = await storage.get_dataset("visits", data_source="ds")
        assert [j.to for j in reloaded.relationships] == ["reports__patient__drug"]

    async def test_colliding_legacy_targets_do_not_break_reingest(
        self, workspace: Path
    ) -> None:
        """Two legacy targets can sanitize to the same name.

        Repairing on the name alone would point both at `a_b`, and the
        duplicate-target guard in `_merge_joins_strict` would then turn a
        tolerated (if dangling) store into a hard re-ingest failure. Only the
        relationship whose pairs match the fresh one is the relationship the bug produced.

        Looped, so this also covers idempotence: a repaired target sanitizes
        to itself and later runs are no-ops.
        """
        db = str(workspace / "collide.db")
        with transaction(db) as conn:
            conn.executescript(
                """
                CREATE TABLE a_b (id INTEGER PRIMARY KEY);
                CREATE TABLE src (
                    id INTEGER PRIMARY KEY,
                    x INTEGER REFERENCES a_b(id)
                );
                """
            )

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db)
        await storage.save_datasource(ds)
        await storage.save_dataset(SlayerDataset(
            name="src", sql_table="src", data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="x", datatype=DataType.INTEGER),
            ],
            relationships=[
                # Matches the fresh relationship's pairs — this is the one to repair.
                DatasetRelationship(to="a__b", from_columns=["x"], to_columns=["id"]),
                # Sanitizes to the same name but has different pairs, so it is
                # somebody else's relationship and must be left alone.
                DatasetRelationship(to="a___b", from_columns=["id"], to_columns=["id"]),
            ],
        ))

        for _ in range(3):
            await _ingest(datasource=ds, storage=storage)
            reloaded = await storage.get_dataset("src", data_source="ds")
            targets = [j.to for j in reloaded.relationships]
            assert targets == ["a_b", "a___b"], targets
            assert len(targets) == len(set(targets))
