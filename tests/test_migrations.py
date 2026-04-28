"""Tests for the schema migration registry and read-time converters."""

import json
import os
import sqlite3
import tempfile

import pytest
import yaml

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.storage import migrations as mig
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# --- Pure migrate() unit tests --------------------------------------------


def test_migrate_unknown_entity_raises() -> None:
    with pytest.raises(KeyError):
        mig.migrate("NotAnEntity", {"version": 1})


def test_migrate_passes_non_dict_through() -> None:
    sentinel = object()
    assert mig.migrate("SlayerModel", sentinel) is sentinel


def test_migrate_v1_noop_stamps_version() -> None:
    out = mig.migrate("SlayerModel", {"name": "foo"})
    assert out["version"] == 1
    assert out["name"] == "foo"


def test_migrate_forward_version_passes_through() -> None:
    """A dict from a newer SLayer should not be downgraded or rejected."""
    out = mig.migrate("SlayerModel", {"version": 99, "name": "foo", "future": True})
    assert out["version"] == 99
    assert out["future"] is True


def test_migrate_missing_handler_raises(monkeypatch) -> None:
    """If CURRENT_VERSIONS jumps ahead but no migration is registered, fail loudly."""
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 2)
    with pytest.raises(RuntimeError, match="No migration registered"):
        mig.migrate("SlayerModel", {"version": 1, "name": "foo"})


def test_migrate_chain_runs_in_order(monkeypatch) -> None:
    """Synthetic v0 → v1 → v2 chain to verify ordering and version stamping."""
    # Pretend SlayerModel has bumped to v3, with two migrations to register.
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 3)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 1)
    def _v1_to_v2(data: dict) -> dict:
        data["step1"] = True
        return data

    @mig.register_migration("SlayerModel", 2)
    def _v2_to_v3(data: dict) -> dict:
        assert data.get("step1") is True  # ordering guarantee
        data["step2"] = True
        return data

    out = mig.migrate("SlayerModel", {"version": 1, "name": "foo"})
    assert out["version"] == 3
    assert out["step1"] is True
    assert out["step2"] is True


def test_register_migration_rejects_duplicates(monkeypatch) -> None:
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 7)
    def _first(data: dict) -> dict:
        return data

    with pytest.raises(ValueError, match="Duplicate migration"):

        @mig.register_migration("SlayerModel", 7)
        def _second(data: dict) -> dict:
            return data


# --- Pydantic-level integration tests -------------------------------------


def test_slayer_model_validates_v1_dict() -> None:
    m = SlayerModel.model_validate({"name": "orders"})
    assert m.version == 1
    assert m.name == "orders"


def test_slayer_model_dump_includes_version() -> None:
    m = SlayerModel(name="orders")
    dumped = m.model_dump(mode="json", exclude_none=True)
    assert dumped["version"] == 1


def test_slayer_model_v0_dict_migrates_via_validator(monkeypatch) -> None:
    """Synthetic v0 → v1 to prove the model_validator(mode='before') hook runs."""
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 2)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 1)
    def _v1_to_v2(data: dict) -> dict:
        # Stash a marker in meta so we can verify post-validation that the
        # converter actually ran on the inbound dict.
        data.setdefault("meta", {})["migrated"] = True
        return data

    m = SlayerModel.model_validate({"version": 1, "name": "orders"})
    assert m.version == 2
    assert m.meta == {"migrated": True}


def test_datasource_config_validates_v1_dict() -> None:
    ds = DatasourceConfig.model_validate({"name": "pg", "type": "postgres"})
    assert ds.version == 1


def test_datasource_config_user_alias_still_works() -> None:
    """Ensure the existing user→username alias still applies post-migration."""
    ds = DatasourceConfig.model_validate(
        {"name": "pg", "type": "postgres", "user": "alice"}
    )
    assert ds.username == "alice"
    assert ds.version == 1


def test_slayer_query_validates_v1_dict() -> None:
    q = SlayerQuery.model_validate({"source_model": "orders"})
    assert q.version == 1


def test_slayer_query_dump_includes_version() -> None:
    q = SlayerQuery(source_model="orders")
    assert q.model_dump(mode="json", exclude_none=True)["version"] == 1


# --- End-to-end: any backend should benefit from migrations ---------------


async def test_yaml_storage_migrates_legacy_model_on_load(monkeypatch) -> None:
    """Write a v0-shaped YAML directly and confirm YAMLStorage upgrades it."""
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 2)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 1)
    def _v1_to_v2(data: dict) -> dict:
        data.setdefault("meta", {})["upgraded"] = True
        return data

    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        legacy_path = os.path.join(storage.models_dir, "orders.yaml")
        with open(legacy_path, "w") as f:
            yaml.dump({"version": 1, "name": "orders"}, f)

        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == 2
        assert loaded.meta == {"upgraded": True}


async def test_sqlite_storage_migrates_legacy_model_on_load(monkeypatch) -> None:
    """Same end-to-end path, but via SQLiteStorage — proves the hook is at the
    Pydantic layer and not tied to YAML I/O."""
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 2)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 1)
    def _v1_to_v2(data: dict) -> dict:
        data.setdefault("meta", {})["upgraded"] = True
        return data

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "slayer.db")
        storage = SQLiteStorage(db_path=db_path)
        legacy_blob = json.dumps({"version": 1, "name": "orders"})
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO models (name, data) VALUES (?, ?)",
                ("orders", legacy_blob),
            )

        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == 2
        assert loaded.meta == {"upgraded": True}


async def test_yaml_round_trip_preserves_version() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_model(SlayerModel(name="orders"))
        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == 1

        # And confirm it actually hit the file as `version: 1`.
        with open(os.path.join(storage.models_dir, "orders.yaml")) as f:
            on_disk = yaml.safe_load(f)
        assert on_disk["version"] == 1
