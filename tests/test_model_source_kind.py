"""Persist what kind of database object backs a model."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import (
    _additive_merge_existing,
    ingest_datasource,
    ingest_datasource_idempotent,
    introspect_table_to_model,
    list_ingestable_objects,
)
from slayer.storage.migrations import CURRENT_VERSIONS, migrate
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _ds(workspace: Path, script: str, name: str = "live.db") -> tuple[str, DatasourceConfig]:
    db_path = str(workspace / name)
    conn = sqlite3.connect(db_path)
    conn.executescript(script)
    conn.commit()
    conn.close()
    return db_path, DatasourceConfig(name="ds", type="sqlite", database=db_path)


_TABLE_AND_VIEW = """
    CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT);
    INSERT INTO orders VALUES (1, 10.0, 'ok');
    CREATE VIEW v_orders AS SELECT id, amount, status FROM orders;
"""


# ---------------------------------------------------------------------------
# classification at ingest
# ---------------------------------------------------------------------------


class TestClassification:
    def test_table_and_view_are_classified(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _TABLE_AND_VIEW)
        by_name = {m.name: m for m in ingest_datasource(datasource=ds)}
        assert by_name["orders"].source_kind == "table"
        assert by_name["v_orders"].source_kind == "view"

    def test_materialized_view_is_classified(self) -> None:
        """no SQLite/DuckDB matview support, so this is mock-only."""
        insp = MagicMock(spec=sa.engine.Inspector)
        insp.get_table_names.return_value = []
        insp.get_view_names.return_value = []
        insp.get_materialized_view_names.return_value = ["mv_orders"]

        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert [(o.name, o.kind) for o in objects] == [
            ("mv_orders", "materialized_view")
        ]

    def test_introspect_table_to_model_defaults_to_none(
        self, workspace: Path
    ) -> None:
        """The dbt and OSI converters call this without a kind and must be unaffected."""
        db_path, ds = _ds(workspace, _TABLE_AND_VIEW)
        engine = sa.create_engine(f"sqlite:///{db_path}")
        try:
            model = introspect_table_to_model(
                sa_engine=engine,
                inspector=sa.inspect(engine),
                table_name="orders",
                schema=None,
                data_source="ds",
            )
            assert model.source_kind is None
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# 30, 33: persistence round-trip
# ---------------------------------------------------------------------------


class TestPersistence:
    async def test_round_trips_through_yaml_storage(self, workspace: Path) -> None:
        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        model = SlayerModel(
            name="v_orders",
            sql_table="v_orders",
            data_source="ds",
            source_kind="view",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        await storage.save_model(model)
        loaded = await storage.get_model("v_orders", data_source="ds")
        assert loaded is not None
        assert loaded.source_kind == "view"

    async def test_round_trips_through_sqlite_storage(self, workspace: Path) -> None:
        storage = SQLiteStorage(db_path=str(workspace / "slayer.db"))
        model = SlayerModel(
            name="mv_orders",
            sql_table="mv_orders",
            data_source="ds",
            source_kind="materialized_view",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        await storage.save_model(model)
        loaded = await storage.get_model("mv_orders", data_source="ds")
        assert loaded is not None
        assert loaded.source_kind == "materialized_view"

    def test_defaults_to_none(self) -> None:
        """Hand-authored and sql-mode models have no live object, so None is correct."""
        assert (
            SlayerModel(name="m", sql_table="t", data_source="ds").source_kind
            is None
        )
        assert (
            SlayerModel(
                name="m", sql="SELECT 1", data_source="ds"
            ).source_kind
            is None
        )

    def test_rejects_an_unknown_kind(self) -> None:
        with pytest.raises(ValidationError):
            SlayerModel(
                name="m", sql_table="t", data_source="ds", source_kind="temp_table"
            )


# ---------------------------------------------------------------------------
# migration
# ---------------------------------------------------------------------------


class TestMigration:
    def test_current_version_is_at_least_8(self) -> None:
        current = CURRENT_VERSIONS["SlayerModel"]
        assert current >= 8  # v8 added source_kind; DEV-1743 bumped to 9
        assert SlayerModel(
            name="m", sql_table="t", data_source="ds"
        ).version == current

    def test_v7_payload_migrates_without_raising(self) -> None:
        """migrate() raises when a step has no converter, so v8_migration.py is mandatory."""
        payload = {
            "version": 7,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "ds",
            "columns": [],
        }
        migrated = migrate("SlayerModel", payload)
        assert migrated["version"] == CURRENT_VERSIONS["SlayerModel"]

    def test_v7_payload_loads_with_unknown_source_kind(self) -> None:
        """A pre-existing model cannot know what backed it — None is correct."""
        model = SlayerModel.model_validate(
            {
                "version": 7,
                "name": "orders",
                "sql_table": "orders",
                "data_source": "ds",
                "columns": [],
            }
        )
        assert model.source_kind is None
        assert model.version == CURRENT_VERSIONS["SlayerModel"]

    def test_migration_chain_walks_from_v1(self) -> None:
        """no gap anywhere in the chain."""
        payload = {"name": "orders", "data_source": "ds", "version": 1}
        assert migrate("SlayerModel", payload)["version"] == CURRENT_VERSIONS["SlayerModel"]


# ---------------------------------------------------------------------------
# the refresh exception to the additive contract
# ---------------------------------------------------------------------------


class TestSourceKindRefresh:
    async def test_view_to_table_flip_refreshes(self, workspace: Path) -> None:
        """A view→table flip with identical columns must bypass the early-return and save gate."""
        db_path, ds = _ds(
            workspace,
            """
            CREATE TABLE base (id INTEGER PRIMARY KEY, amount REAL);
            CREATE VIEW foo AS SELECT id, amount FROM base;
            """,
        )
        storage = YAMLStorage(base_dir=str(workspace / "storage"))

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        first = await storage.get_model("foo", data_source="ds")
        assert first is not None
        assert first.source_kind == "view"

        # dbt's `+materialized: table` — same name, same columns, now a table.
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            DROP VIEW foo;
            CREATE TABLE foo (id INTEGER PRIMARY KEY, amount REAL);
            """
        )
        conn.commit()
        conn.close()

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        second = await storage.get_model("foo", data_source="ds")
        assert second is not None
        assert second.source_kind == "table", (
            "source_kind went stale — the refresh must also bypass the "
            "early return and the save gate, not just model_copy(update=...)"
        )

    async def test_table_to_view_flip_refreshes(self, workspace: Path) -> None:
        """symmetric."""
        db_path, ds = _ds(
            workspace,
            """
            CREATE TABLE src (id INTEGER PRIMARY KEY, amount REAL);
            CREATE TABLE foo (id INTEGER PRIMARY KEY, amount REAL);
            """,
        )
        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        first = await storage.get_model("foo", data_source="ds")
        assert first is not None
        assert first.source_kind == "table"

        conn = sqlite3.connect(db_path)
        conn.executescript(
            "DROP TABLE foo; CREATE VIEW foo AS SELECT id, amount FROM src;"
        )
        conn.commit()
        conn.close()

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        second = await storage.get_model("foo", data_source="ds")
        assert second is not None
        assert second.source_kind == "view"

    async def test_refresh_reports_the_change(self, workspace: Path) -> None:
        """A kind change with no column change must still surface as a ModelAddition."""
        db_path, ds = _ds(
            workspace,
            """
            CREATE TABLE base (id INTEGER PRIMARY KEY, amount REAL);
            CREATE VIEW foo AS SELECT id, amount FROM base;
            """,
        )
        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        conn = sqlite3.connect(db_path)
        conn.executescript(
            "DROP VIEW foo; CREATE TABLE foo (id INTEGER PRIMARY KEY, amount REAL);"
        )
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = next(a for a in result.additions if a.model_name == "foo")
        assert addition.kind_change == "view → table"

    async def test_none_never_erases_a_known_value(self, workspace: Path) -> None:
        """A fresh model with kind None must not wipe a persisted value."""
        persisted = SlayerModel(
            name="foo",
            sql_table="foo",
            data_source="ds",
            source_kind="view",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        fresh = SlayerModel(
            name="foo",
            sql_table="foo",
            data_source="ds",
            source_kind=None,
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        outcome = _additive_merge_existing(persisted=persisted, fresh=fresh)
        assert outcome.kind_changed is False
        assert outcome.merged.source_kind == "view"

    def test_no_op_merge_preserves_the_early_return_contract(self) -> None:
        """An identical re-ingest must still short-circuit."""
        model = SlayerModel(
            name="foo",
            sql_table="foo",
            data_source="ds",
            source_kind="table",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        outcome = _additive_merge_existing(
            persisted=model, fresh=model.model_copy(deep=True)
        )
        assert outcome.merged is model
        assert outcome.new_columns == []
        assert outcome.new_joins == []
        assert outcome.widened_columns == []
        assert outcome.kind_changed is False

    async def test_identical_reingest_is_still_a_no_op(
        self, workspace: Path
    ) -> None:
        """End-to-end: adding source_kind must not turn every re-ingest into a write."""
        _, ds = _ds(workspace, _TABLE_AND_VIEW)
        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        for addition in result.additions:
            assert addition.created is False
            assert addition.new_columns == []
            assert addition.kind_change is None
