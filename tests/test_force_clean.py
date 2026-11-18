"""Tests for ``apply_drift_deletes`` + ``slayer validate-models --force-clean``.

See DEV-1356 stage 4.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    SlayerDataset,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import (
    ApplyDriftResult,
    DeleteReason,
    EditDatasetDelete,
    RemoveSpec,
    ToDeleteEntry,
    WholeDatasetDelete,
)
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


async def _setup(workspace: Path) -> tuple[SlayerQueryEngine, str]:
    db_path = str(workspace / "live.db")
    with transaction(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT NOT NULL);
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO customers VALUES (1, 'US');
            INSERT INTO orders VALUES (1, 100.0, 'completed', 1);
            """
        )

    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_dataset(
        SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
            ],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(
                    name="customer_id", sql="customer_id", datatype=DataType.FLOAT
                ),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage), db_path


# ---------------------------------------------------------------------------
# apply_drift_deletes
# ---------------------------------------------------------------------------


class TestApplyDriftDeletes:
    async def test_applies_edit_model_delete(self, workspace: Path) -> None:
        engine, _ = await _setup(workspace)
        deletes: list[ToDeleteEntry] = [
            EditDatasetDelete(
                dataset_name="orders",
                data_source="ds",
                remove=RemoveSpec(fields=["status"]),
                reasons=[DeleteReason(target="column:status", reason="dropped")],
            )
        ]
        result = await engine.apply_drift_deletes(deletes)
        assert isinstance(result, ApplyDriftResult)
        assert len(result.applied) == 1
        assert result.applied[0].dataset_name == "orders"
        assert result.errors == []
        # Persisted model now lacks 'status'
        loaded = await engine.storage.get_dataset("orders", data_source="ds")
        assert loaded is not None
        assert not any(c.name == "status" for c in loaded.fields)

    async def test_applies_whole_model_delete(self, workspace: Path) -> None:
        engine, _ = await _setup(workspace)
        deletes: list[ToDeleteEntry] = [
            WholeDatasetDelete(
                dataset_name="orders",
                data_source="ds",
                reasons=[DeleteReason(target="model:orders", reason="missing")],
            )
        ]
        result = await engine.apply_drift_deletes(deletes)
        assert len(result.applied) == 1
        assert result.applied[0].tool == "delete_dataset"
        # Persisted model gone
        assert await engine.storage.get_dataset("orders", data_source="ds") is None

    async def test_arbitrary_input_order(self, workspace: Path) -> None:
        engine, _ = await _setup(workspace)
        deletes = [
            EditDatasetDelete(
                dataset_name="orders",
                data_source="ds",
                remove=RemoveSpec(fields=["status"]),
            ),
            EditDatasetDelete(
                dataset_name="customers",
                data_source="ds",
                remove=RemoveSpec(fields=["region"]),
            ),
        ]
        result = await engine.apply_drift_deletes(deletes)
        assert len(result.applied) == 2
        assert result.errors == []

    async def test_per_entry_failure_captured(self, workspace: Path) -> None:
        engine, _ = await _setup(workspace)
        # Inject a save failure on the orders edit.
        original_save = engine.storage.save_dataset

        async def flaky(model: SlayerDataset) -> None:
            if model.name == "orders":
                raise RuntimeError("disk full")
            return await original_save(model)

        with patch.object(engine.storage, "save_dataset", autospec=True, side_effect=flaky):
            deletes = [
                EditDatasetDelete(
                    dataset_name="orders",
                    data_source="ds",
                    remove=RemoveSpec(fields=["status"]),
                ),
                EditDatasetDelete(
                    dataset_name="customers",
                    data_source="ds",
                    remove=RemoveSpec(fields=["region"]),
                ),
            ]
            result = await engine.apply_drift_deletes(deletes)
        assert any(e.dataset_name == "orders" for e in result.errors)
        assert any(a.dataset_name == "customers" for a in result.applied)

    async def test_residual_populated_from_post_apply_validate(
        self, workspace: Path
    ) -> None:
        engine, db_path = await _setup(workspace)
        # Drop one column externally; apply drift deletes; another column
        # is dropped in between → post-apply validate sees residual drift.
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE customers DROP COLUMN region")

        # Apply only the orders.status drop (which doesn't exist as drift —
        # but the test purpose is residual after apply, not the drift
        # itself). Arrange: pre-drop "status" persisted column to make
        # apply meaningful.
        deletes = [
            EditDatasetDelete(
                dataset_name="orders",
                data_source="ds",
                remove=RemoveSpec(fields=["status"]),
            ),
        ]
        result = await engine.apply_drift_deletes(deletes)
        # Residual should include customers.region drop discovered post-apply.
        residual_models = {e.dataset_name for e in result.residual}
        assert "customers" in residual_models


# ---------------------------------------------------------------------------
# CLI integration — slayer validate-models --force-clean
# ---------------------------------------------------------------------------


def _run_cli(args, *, input_text: str = "", workspace: Path) -> subprocess.CompletedProcess:
    # DEV-1815: retained as the one end-to-end subprocess smoke (state-writing +
    # interactive stdin); the non-interactive validate-models tests below run
    # in-process. Keeps coverage of the console-script/process boundary.
    return subprocess.run(
        [sys.executable, "-m", "slayer.cli", *args, "--storage", str(workspace / "storage")],
        input=input_text,
        capture_output=True,
        text=True,
        timeout=60,
    )


class TestForceCleanCLI:
    @pytest.fixture
    def cli_workspace(self, workspace):
        # Build the same sqlite + storage as _setup, but synchronously so
        # subprocess CLI can read it.
        async def _build():
            return await _setup(workspace)

        run_sync(_build())
        return workspace

    def test_default_no_flags_prints_diff_and_exits_zero(self, cli_workspace) -> None:
        # Drop a column externally first
        db_path = str(cli_workspace / "live.db")
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE customers DROP COLUMN region")

        storage_arg = str(cli_workspace / "storage")
        result = run_cli_in_process(
            ["validate-datasets", "--datasource", "ds", "--storage", storage_arg]
        )
        assert result.returncode == 0
        assert "region" in result.stdout

    def test_force_clean_yes_applies_without_prompt(self, cli_workspace) -> None:
        db_path = str(cli_workspace / "live.db")
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE customers DROP COLUMN region")

        storage_arg = str(cli_workspace / "storage")
        result = run_cli_in_process(
            ["validate-datasets", "--datasource", "ds", "--force-clean", "--yes",
             "--storage", storage_arg]
        )
        assert result.returncode == 0
        # After apply, the diff is empty — re-run validate-models and confirm.
        follow_up = run_cli_in_process(
            ["validate-datasets", "--datasource", "ds", "--storage", storage_arg]
        )
        assert follow_up.returncode == 0
        assert "no drift detected" in follow_up.stdout.lower()

    def test_force_clean_n_aborts(self, cli_workspace) -> None:
        db_path = str(cli_workspace / "live.db")
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE customers DROP COLUMN region")

        result = _run_cli(
            ["validate-datasets", "--datasource", "ds", "--force-clean"],
            input_text="n\n",
            workspace=cli_workspace,
        )
        assert result.returncode == 0, result.stderr
        # Aborted — region should still be on the persisted model
        async def _check():
            engine = SlayerQueryEngine(
                storage=YAMLStorage(base_dir=str(cli_workspace / "storage"))
            )
            return await engine.storage.get_dataset("customers", data_source="ds")

        loaded = run_sync(_check())
        assert loaded is not None
        # The region column is still persisted (apply was aborted).
        assert any(c.name == "region" for c in loaded.fields)
