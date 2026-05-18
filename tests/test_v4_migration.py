"""Tests for the v3 → v4 SlayerModel schema bump and storage layout migration.

v4 namespaces SlayerModel storage by ``(data_source, name)`` instead of the
flat global ``name`` key used in v1–v3. The schema migration:

* requires ``data_source`` to be non-empty on every SlayerModel dict;
* moves YAML files from ``models/<name>.yaml`` into
  ``models/<data_source>/<name>.yaml`` on first open;
* alters the SQLite ``models`` table from a single-column PK on ``name`` to a
  composite PK on ``(data_source, name)``, copying ``data_source`` out of the
  JSON blob in the process.

When the legacy file/row carries an empty ``data_source``:

* if **exactly one** datasource is registered, the migration auto-fills it
  (the only consistent answer);
* otherwise the migration **hard-fails** with an actionable message — the
  user must edit the file or pre-populate ``data_source`` before reopening.
"""

import json
import os
import sqlite3

import pytest
import yaml

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage import migrations as mig
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# --- Schema bump -----------------------------------------------------------


def test_current_slayer_model_version_is_v4() -> None:
    """The bump itself: storage writes v4, period."""
    assert mig.CURRENT_VERSIONS["SlayerModel"] == 6


def test_slayer_model_default_version_is_v4() -> None:
    """A freshly-constructed SlayerModel carries the bumped version."""
    m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
    assert m.version == 6


def test_slayer_model_dump_writes_v4() -> None:
    m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
    assert m.model_dump(mode="json", exclude_none=True)["version"] == 6


# --- Pure dict converter ----------------------------------------------------


def test_v3_to_v4_converter_passes_through_when_data_source_set() -> None:
    out = mig.migrate("SlayerModel", {
        "version": 3,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "warehouse",
    })
    assert out["version"] == 6
    assert out["data_source"] == "warehouse"


def test_v3_to_v4_converter_rejects_empty_data_source() -> None:
    """An orphan model dict can't be migrated standalone — the converter has
    no list-of-datasources context, so it can't auto-fill. The migration must
    refuse rather than silently invent a datasource value."""
    with pytest.raises(ValueError, match="data_source"):
        mig.migrate("SlayerModel", {
            "version": 3,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "",
        })


def test_v3_to_v4_converter_rejects_missing_data_source() -> None:
    with pytest.raises(ValueError, match="data_source"):
        mig.migrate("SlayerModel", {
            "version": 3,
            "name": "orders",
            "sql_table": "orders",
        })


def test_pydantic_rejects_empty_data_source_at_v4() -> None:
    """The non-empty rule is also enforced at the model layer, so callers
    that bypass the dict converter (e.g., construct SlayerModel directly)
    can't slip an orphan past."""
    with pytest.raises(ValueError, match="data_source"):
        SlayerModel(name="orders", sql_table="orders", data_source="")


# --- YAMLStorage layout migration ------------------------------------------


async def test_yaml_legacy_flat_file_migrates_to_nested(tmp_path) -> None:
    """A v3-shaped flat ``models/<name>.yaml`` opens cleanly on a v4 storage
    and lives under ``models/<data_source>/<name>.yaml`` after migration.
    """
    base = str(tmp_path)
    # DEV-1361 storage-driven type refinement requires the datasource entry
    # to be present when the migrated dict has refineable DOUBLE base columns.
    # A live SQLite stub satisfies that contract for this layout-migration test.
    live_db_path = os.path.join(base, "live.db")
    with sqlite3.connect(live_db_path) as live:
        live.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        live.commit()
    ds_dir = os.path.join(base, "datasources")
    os.makedirs(ds_dir, exist_ok=True)
    with open(os.path.join(ds_dir, "warehouse.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump(
            {"name": "warehouse", "type": "sqlite", "database": live_db_path, "version": 1},
            f,
        )

    legacy_models_dir = os.path.join(base, "models")
    os.makedirs(legacy_models_dir, exist_ok=True)
    with open(os.path.join(legacy_models_dir, "orders.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({
            "version": 3,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "warehouse",
            "columns": [{"name": "id", "type": "number", "primary_key": True}],
        }, f)

    storage = YAMLStorage(base_dir=base)

    loaded = await storage.get_model("orders", data_source="warehouse")
    assert loaded is not None
    assert loaded.data_source == "warehouse"
    assert loaded.version == 6

    # Old flat file is gone; new namespaced file exists.
    assert not os.path.exists(os.path.join(legacy_models_dir, "orders.yaml"))
    assert os.path.exists(os.path.join(legacy_models_dir, "warehouse", "orders.yaml"))


async def test_yaml_orphan_with_single_datasource_auto_assigned(tmp_path) -> None:
    """One DatasourceConfig in storage ⇒ orphan v3 file gets that as its
    ``data_source`` (the only consistent answer)."""
    base = str(tmp_path)
    # Pre-create a single datasource the migration can resolve to.
    ds_dir = os.path.join(base, "datasources")
    os.makedirs(ds_dir, exist_ok=True)
    with open(os.path.join(ds_dir, "only_ds.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({"name": "only_ds", "type": "postgres"}, f)

    legacy_models_dir = os.path.join(base, "models")
    os.makedirs(legacy_models_dir, exist_ok=True)
    with open(os.path.join(legacy_models_dir, "orders.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({
            "version": 3,
            "name": "orders",
            "sql_table": "orders",
            # data_source intentionally absent (orphan).
        }, f)

    storage = YAMLStorage(base_dir=base)

    loaded = await storage.get_model("orders", data_source="only_ds")
    assert loaded is not None
    assert loaded.data_source == "only_ds"


def test_yaml_orphan_with_multiple_datasources_hard_fails(tmp_path) -> None:
    """≥2 datasources ⇒ the migration can't pick one, so it raises and refuses
    to open the storage. Error message must name the orphan files so the user
    can fix them by hand."""
    base = str(tmp_path)
    ds_dir = os.path.join(base, "datasources")
    os.makedirs(ds_dir, exist_ok=True)
    for n in ("a", "b"):
        with open(os.path.join(ds_dir, f"{n}.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump({"name": n, "type": "postgres"}, f)

    legacy_models_dir = os.path.join(base, "models")
    os.makedirs(legacy_models_dir, exist_ok=True)
    with open(os.path.join(legacy_models_dir, "orders.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({"version": 3, "name": "orders", "sql_table": "orders"}, f)

    with pytest.raises(ValueError, match=r"orders.yaml|data_source"):
        YAMLStorage(base_dir=base)


def test_yaml_orphan_with_no_datasources_hard_fails(tmp_path) -> None:
    """Zero datasources ⇒ no plausible default; same hard fail as ≥2."""
    base = str(tmp_path)
    legacy_models_dir = os.path.join(base, "models")
    os.makedirs(legacy_models_dir, exist_ok=True)
    with open(os.path.join(legacy_models_dir, "orders.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({"version": 3, "name": "orders", "sql_table": "orders"}, f)

    with pytest.raises(ValueError, match=r"orders.yaml|data_source"):
        YAMLStorage(base_dir=base)


def test_yaml_migration_refuses_to_overwrite_existing_namespaced_model(tmp_path) -> None:
    """If ``models/<data_source>/<name>.yaml`` already exists when the
    flat→nested migration runs (e.g. partial/manual migration, or a rerun
    after an interrupted open), the migrator must raise rather than
    silently overwriting the namespaced file. See PR #92 thread #11.
    """
    base = str(tmp_path)
    legacy_models_dir = os.path.join(base, "models")
    os.makedirs(legacy_models_dir, exist_ok=True)
    # Existing v4 file under the namespaced layout — this is what the
    # migrator must NOT clobber.
    ds_dir = os.path.join(legacy_models_dir, "warehouse")
    os.makedirs(ds_dir)
    with open(os.path.join(ds_dir, "orders.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({
            "version": 4,
            "name": "orders",
            "sql_table": "orders_v4",
            "data_source": "warehouse",
            "columns": [{"name": "id", "type": "number", "primary_key": True}],
        }, f)
    # Conflicting flat file with the same (data_source, name) key.
    with open(os.path.join(legacy_models_dir, "orders.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        yaml.dump({
            "version": 3,
            "name": "orders",
            "sql_table": "orders_v3_flat",
            "data_source": "warehouse",
        }, f)

    with pytest.raises(ValueError, match=r"orders|exist"):
        YAMLStorage(base_dir=base)

    # Source flat file is still present (migrator refused without
    # destroying input) so the user can resolve manually.
    assert os.path.exists(os.path.join(legacy_models_dir, "orders.yaml"))
    # The pre-existing v4 file is unchanged.
    with open(os.path.join(ds_dir, "orders.yaml")) as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
        on_disk = yaml.safe_load(f)
    assert on_disk["sql_table"] == "orders_v4"


async def test_yaml_already_migrated_layout_is_no_op(tmp_path) -> None:
    """Opening a v4-shaped layout twice must not re-migrate (idempotent)."""
    base = str(tmp_path)
    storage = YAMLStorage(base_dir=base)
    # Save a datasource and a model so the second open has real state to scan.
    await storage.save_datasource(DatasourceConfig(name="warehouse", type="postgres"))
    await storage.save_model(SlayerModel(name="orders", sql_table="orders", data_source="warehouse"))

    # Re-open at the same base dir; should find no flat files, do nothing.
    storage2 = YAMLStorage(base_dir=base)
    loaded = await storage2.get_model("orders", data_source="warehouse")
    assert loaded is not None


# --- SQLiteStorage schema migration ----------------------------------------


def _create_legacy_sqlite_models_table(db_path: str) -> None:
    """Re-create the v3 schema (single-column PK on ``name``)."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE models (name TEXT PRIMARY KEY, data TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE datasources (name TEXT PRIMARY KEY, data TEXT NOT NULL)"
        )


async def test_sqlite_legacy_schema_migrates_to_composite_pk(tmp_path) -> None:
    """A v3 SQLite DB with single-PK ``models`` table is migrated in-place.
    After migration the table has a composite PK on ``(data_source, name)``
    and existing rows survive with their data_source extracted from the
    JSON blob."""
    db_path = str(tmp_path / "slayer.db")
    _create_legacy_sqlite_models_table(db_path)
    blob = json.dumps({
        "version": 3,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "warehouse",
    })
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO models (name, data) VALUES (?, ?)", ("orders", blob))

    storage = SQLiteStorage(db_path=db_path)

    loaded = await storage.get_model("orders", data_source="warehouse")
    assert loaded is not None
    assert loaded.data_source == "warehouse"
    assert loaded.version == 6

    # New schema: composite PK on (data_source, name).
    with sqlite3.connect(db_path) as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(models)").fetchall()]
        pk_cols = [
            r[1] for r in conn.execute("PRAGMA table_info(models)").fetchall() if r[5] > 0
        ]
    assert "data_source" in cols
    assert set(pk_cols) == {"data_source", "name"}


async def test_sqlite_legacy_schema_orphan_with_single_datasource_auto_assigned(tmp_path) -> None:
    db_path = str(tmp_path / "slayer.db")
    _create_legacy_sqlite_models_table(db_path)
    blob_orphan = json.dumps({
        "version": 3,
        "name": "orders",
        "sql_table": "orders",
        # No data_source.
    })
    ds_blob = json.dumps({"name": "only_ds", "type": "postgres", "version": 1})
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO models (name, data) VALUES (?, ?)", ("orders", blob_orphan))
        conn.execute("INSERT INTO datasources (name, data) VALUES (?, ?)", ("only_ds", ds_blob))

    storage = SQLiteStorage(db_path=db_path)

    loaded = await storage.get_model("orders", data_source="only_ds")
    assert loaded is not None
    assert loaded.data_source == "only_ds"


def test_sqlite_legacy_schema_orphan_with_multiple_datasources_hard_fails(tmp_path) -> None:
    db_path = str(tmp_path / "slayer.db")
    _create_legacy_sqlite_models_table(db_path)
    blob_orphan = json.dumps({"version": 3, "name": "orders", "sql_table": "orders"})
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO models (name, data) VALUES (?, ?)", ("orders", blob_orphan))
        for n in ("a", "b"):
            conn.execute(
                "INSERT INTO datasources (name, data) VALUES (?, ?)",
                (n, json.dumps({"name": n, "type": "postgres", "version": 1})),
            )

    with pytest.raises(ValueError, match=r"orders|data_source"):
        SQLiteStorage(db_path=db_path)


async def test_sqlite_already_migrated_schema_is_no_op(tmp_path) -> None:
    """Opening a v4 SQLite twice must be idempotent."""
    db_path = str(tmp_path / "slayer.db")
    storage = SQLiteStorage(db_path=db_path)
    await storage.save_datasource(DatasourceConfig(name="warehouse", type="postgres"))
    await storage.save_model(SlayerModel(name="orders", sql_table="orders", data_source="warehouse"))

    storage2 = SQLiteStorage(db_path=db_path)
    loaded = await storage2.get_model("orders", data_source="warehouse")
    assert loaded is not None
