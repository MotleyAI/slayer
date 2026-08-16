"""Idempotent re-ingest must persist metadata-only cardinality/unique fills."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from slayer.core.enums import JoinCardinality
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource_idempotent
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
    conn.commit()
    conn.close()


async def _setup(workspace: Path) -> tuple:
    db_path = str(workspace / "live.db")
    _create_schema(db_path)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
    await storage.save_datasource(ds)
    await ingest_datasource_idempotent(datasource=ds, storage=storage)
    return storage, ds


def _order_join(model):
    return next(j for j in model.joins if j.target_model == "customers")


class TestFillsMetadataAndPersists:
    async def test_reingest_refills_none_cardinality_and_persists(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # Simulate legacy persisted data: join without a cardinality.
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = _order_join(orders).model_copy(update={"cardinality": None})
        await storage.save_model(orders)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("orders", data_source="ds")
        # Refilled AND persisted (reloaded from storage proves the save).
        assert _order_join(reloaded).cardinality is JoinCardinality.MANY_TO_ONE

    async def test_reingest_sets_unique_and_persists(self, workspace: Path) -> None:
        storage, ds = await _setup(workspace)

        customers = await storage.get_model("customers", data_source="ds")
        email = next(c for c in customers.columns if c.name == "email")
        idx = customers.columns.index(email)
        customers.columns[idx] = email.model_copy(update={"unique": False})
        await storage.save_model(customers)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("customers", data_source="ds")
        email = next(c for c in reloaded.columns if c.name == "email")
        assert email.unique is True


class TestAdditiveContract:
    async def test_reingest_does_not_overwrite_user_cardinality(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # A deliberate user override that disagrees with the structural guess.
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = _order_join(orders).model_copy(
            update={"cardinality": JoinCardinality.ONE_TO_ONE}
        )
        await storage.save_model(orders)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("orders", data_source="ds")
        assert _order_join(reloaded).cardinality is JoinCardinality.ONE_TO_ONE

    async def test_reingest_does_not_downgrade_user_unique(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # User marked a column unique that has no DB constraint.
        customers = await storage.get_model("customers", data_source="ds")
        region = next(c for c in customers.columns if c.name == "region")
        idx = customers.columns.index(region)
        customers.columns[idx] = region.model_copy(update={"unique": True})
        await storage.save_model(customers)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("customers", data_source="ds")
        region = next(c for c in reloaded.columns if c.name == "region")
        assert region.unique is True


class TestLegacyJoinTargetNormalisation:
    """A join persisted with the live object name self-heals on re-ingest.

    Model names cannot contain ``__``, so such a target can never resolve —
    it is repaired rather than kept alongside the corrected join.
    """

    async def test_legacy_double_underscore_target_is_rewritten(
        self, workspace: Path
    ) -> None:
        import sqlite3

        from slayer.core.models import DatasourceConfig
        from slayer.storage.yaml_storage import YAMLStorage

        db = str(workspace / "legacy.db")
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE reports__patient__drug (id INTEGER PRIMARY KEY);
            CREATE TABLE visits (
                id INTEGER PRIMARY KEY,
                report_id INTEGER REFERENCES reports__patient__drug(id)
            );
            """
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        # Rewind to the pre-fix state: the join names the live object.
        visits = await storage.get_model("visits", data_source="ds")
        visits.joins[0].target_model = "reports__patient__drug"
        await storage.save_model(visits)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("visits", data_source="ds")
        assert [j.target_model for j in reloaded.joins] == ["reports_patient_drug"]

    async def test_colliding_legacy_targets_do_not_break_reingest(
        self, workspace: Path
    ) -> None:
        """Two legacy targets can sanitize to the same name.

        Repairing on the name alone would point both at `a_b`, and the
        duplicate-target guard in `_merge_joins_strict` would then turn a
        tolerated (if dangling) store into a hard re-ingest failure.
        """
        import sqlite3

        from slayer.core.enums import DataType
        from slayer.core.models import (
            Column,
            DatasourceConfig,
            ModelJoin,
            SlayerModel,
        )
        from slayer.storage.yaml_storage import YAMLStorage

        db = str(workspace / "collide.db")
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE a_b (id INTEGER PRIMARY KEY);
            CREATE TABLE src (
                id INTEGER PRIMARY KEY,
                x INTEGER REFERENCES a_b(id)
            );
            """
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db)
        await storage.save_datasource(ds)
        await storage.save_model(SlayerModel(
            name="src", sql_table="src", data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="x", type=DataType.INT),
            ],
            joins=[
                ModelJoin(target_model="a__b", join_pairs=[["x", "id"]]),
                ModelJoin(target_model="a___b", join_pairs=[["id", "id"]]),
            ],
        ))

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("src", data_source="ds")
        targets = [j.target_model for j in reloaded.joins]
        assert len(targets) == len(set(targets)), f"duplicate targets: {targets}"

    async def test_repair_is_idempotent(self, workspace: Path) -> None:
        """A repaired target sanitizes to itself, so later re-ingests no-op."""
        import sqlite3

        from slayer.core.models import DatasourceConfig
        from slayer.storage.yaml_storage import YAMLStorage

        db = str(workspace / "idem.db")
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE reports__patient__drug (id INTEGER PRIMARY KEY);
            CREATE TABLE visits (
                id INTEGER PRIMARY KEY,
                report_id INTEGER REFERENCES reports__patient__drug(id)
            );
            """
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        visits = await storage.get_model("visits", data_source="ds")
        visits.joins[0].target_model = "reports__patient__drug"
        await storage.save_model(visits)

        for _ in range(3):
            await ingest_datasource_idempotent(datasource=ds, storage=storage)
            reloaded = await storage.get_model("visits", data_source="ds")
            assert [j.target_model for j in reloaded.joins] == [
                "reports_patient_drug"
            ]
