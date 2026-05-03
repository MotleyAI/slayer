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
    """A SlayerModel dict with no version starts at 1 and walks to the current."""
    out = mig.migrate("SlayerModel", {"name": "foo"})
    assert out["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
    assert out["name"] == "foo"


def test_migrate_does_not_mutate_input_dict() -> None:
    """migrate() must never mutate the caller's payload."""
    payload = {"name": "foo"}  # no "version" key
    out = mig.migrate("SlayerModel", payload)
    assert "version" not in payload
    assert out["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
    assert out is not payload


def test_migrate_forward_version_passes_through() -> None:
    """A dict from a newer SLayer should not be downgraded or rejected."""
    out = mig.migrate("SlayerModel", {"version": 99, "name": "foo", "future": True})
    assert out["version"] == 99
    assert out["future"] is True


def test_migrate_missing_handler_raises(monkeypatch) -> None:
    """If CURRENT_VERSIONS jumps ahead but no migration is registered, fail loudly."""
    # Bump past the highest registered migration to force a gap.
    target = max(mig.CURRENT_VERSIONS.values()) + 5
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", target)
    with pytest.raises(RuntimeError, match="No migration registered"):
        mig.migrate("SlayerModel", {"version": target - 1, "name": "foo"})


def test_migrate_chain_runs_in_order(monkeypatch) -> None:
    """Synthetic chain to verify ordering and version stamping.

    Registers two synthetic migrations *above* the real ones so we don't
    collide with the real v1→v2 SlayerModel converter.
    """
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 4)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 2)
    def _v2_to_v3(data: dict) -> dict:
        data["step1"] = True
        return data

    @mig.register_migration("SlayerModel", 3)
    def _v3_to_v4(data: dict) -> dict:
        assert data.get("step1") is True  # ordering guarantee
        data["step2"] = True
        return data

    out = mig.migrate("SlayerModel", {"version": 2, "name": "foo"})
    assert out["version"] == 4
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
    m = SlayerModel.model_validate({"name": "orders", "sql_table": "orders"})
    assert m.version == mig.CURRENT_VERSIONS["SlayerModel"]
    assert m.name == "orders"


def test_slayer_model_dump_includes_version() -> None:
    m = SlayerModel(name="orders", sql_table="orders")
    dumped = m.model_dump(mode="json", exclude_none=True)
    assert dumped["version"] == mig.CURRENT_VERSIONS["SlayerModel"]


def test_slayer_model_synthetic_migration_runs_via_validator(monkeypatch) -> None:
    """Prove the model_validator(mode='before') hook walks the chain.

    Registers a migration *above* the real ones so we don't double-migrate.
    """
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", 3)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", 2)
    def _v2_to_v3(data: dict) -> dict:
        # Stash a marker in meta so we can verify post-validation that the
        # converter actually ran on the inbound dict.
        data.setdefault("meta", {})["migrated"] = True
        return data

    m = SlayerModel.model_validate(
        {"version": 2, "name": "orders", "sql_table": "orders"}
    )
    assert m.version == 3
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
    assert q.version == mig.CURRENT_VERSIONS["SlayerQuery"]


def test_slayer_query_dump_includes_version() -> None:
    q = SlayerQuery(source_model="orders")
    assert q.model_dump(mode="json", exclude_none=True)["version"] == mig.CURRENT_VERSIONS["SlayerQuery"]


# --- End-to-end: any backend should benefit from migrations ---------------


async def test_yaml_storage_migrates_legacy_model_on_load(monkeypatch) -> None:
    """Write a synthetic-version YAML directly and confirm YAMLStorage upgrades it.

    Registers a synthetic migration *above* the real ones, then writes a model
    file at that intermediate version to prove the hook runs through every
    backend.
    """
    next_version = mig.CURRENT_VERSIONS["SlayerModel"] + 1
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", next_version)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", next_version - 1)
    def _synthetic(data: dict) -> dict:
        data.setdefault("meta", {})["upgraded"] = True
        return data

    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        legacy_path = os.path.join(storage.models_dir, "orders.yaml")
        with open(legacy_path, "w") as f:
            yaml.dump(
                {"version": next_version - 1, "name": "orders", "sql_table": "orders"},
                f,
            )

        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == next_version
        assert loaded.meta == {"upgraded": True}


async def test_sqlite_storage_migrates_legacy_model_on_load(monkeypatch) -> None:
    """Same end-to-end path, but via SQLiteStorage — proves the hook is at the
    Pydantic layer and not tied to YAML I/O."""
    next_version = mig.CURRENT_VERSIONS["SlayerModel"] + 1
    monkeypatch.setitem(mig.CURRENT_VERSIONS, "SlayerModel", next_version)
    monkeypatch.setattr(mig, "_REGISTRY", dict(mig._REGISTRY))

    @mig.register_migration("SlayerModel", next_version - 1)
    def _synthetic(data: dict) -> dict:
        data.setdefault("meta", {})["upgraded"] = True
        return data

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "slayer.db")
        storage = SQLiteStorage(db_path=db_path)
        legacy_blob = json.dumps(
            {"version": next_version - 1, "name": "orders", "sql_table": "orders"}
        )
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO models (name, data) VALUES (?, ?)",
                ("orders", legacy_blob),
            )

        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == next_version
        assert loaded.meta == {"upgraded": True}


async def test_yaml_round_trip_preserves_version() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_model(SlayerModel(name="orders", sql_table="orders"))
        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == mig.CURRENT_VERSIONS["SlayerModel"]

        # And confirm it actually hit the file at the current version.
        with open(os.path.join(storage.models_dir, "orders.yaml")) as f:
            on_disk = yaml.safe_load(f)
        assert on_disk["version"] == mig.CURRENT_VERSIONS["SlayerModel"]


# --- v1 → v2 migration: dim+measure → columns + fields → measures ---------


def test_model_v1_to_v2_dimensions_only() -> None:
    """A v1 model with only dimensions migrates to v2 columns; measures empty."""
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "orders",
        "sql_table": "orders",
        "dimensions": [
            {"name": "status", "type": "string"},
            {"name": "id", "type": "number", "primary_key": True},
        ],
    })
    assert m.version == 2
    assert [c.name for c in m.columns] == ["status", "id"]
    assert m.columns[0].type.value == "string"
    assert m.columns[1].primary_key is True
    assert m.measures == []


def test_model_v1_to_v2_measures_only() -> None:
    """A v1 model with only measures migrates to v2 columns with NUMBER default."""
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "orders",
        "sql_table": "orders",
        "measures": [
            {"name": "revenue", "sql": "amount"},
            {"name": "high_value", "sql": "amount", "filter": "amount > 100",
             "allowed_aggregations": ["sum"]},
        ],
    })
    assert m.version == 2
    assert [c.name for c in m.columns] == ["revenue", "high_value"]
    assert all(c.type.value == "number" for c in m.columns)
    assert all(c.primary_key is False for c in m.columns)
    assert m.columns[1].filter == "amount > 100"
    assert m.columns[1].allowed_aggregations == ["sum"]
    assert m.measures == []


def test_model_v1_to_v2_dim_and_measure() -> None:
    """Both lists merge into columns; order preserved (dimensions first)."""
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "orders",
        "sql_table": "orders",
        "dimensions": [{"name": "status"}],
        "measures": [{"name": "revenue", "sql": "amount"}],
    })
    assert [c.name for c in m.columns] == ["status", "revenue"]
    assert m.measures == []


def test_model_v1_to_v2_collision_raises() -> None:
    """A v1 model with a name in both dimensions and measures raises a clear error."""
    with pytest.raises(ValueError, match="name collision"):
        SlayerModel.model_validate({
            "version": 1,
            "name": "orders",
            "dimensions": [{"name": "amount", "type": "number"}],
            "measures": [{"name": "amount", "sql": "amount"}],
        })


def test_model_v1_to_v2_legacy_type_alias() -> None:
    """Old `type: sum` on a Measure becomes allowed_aggregations=['sum']."""
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "orders",
        "sql_table": "orders",
        "measures": [{"name": "revenue", "sql": "amount", "type": "sum"}],
    })
    col = m.columns[0]
    assert col.name == "revenue"
    assert col.type.value == "number"  # default after stripping legacy `type: sum`
    assert col.allowed_aggregations == ["sum"]


def test_model_v1_to_v2_legacy_type_alias_respects_explicit_whitelist() -> None:
    """If user already set allowed_aggregations, the legacy type doesn't overwrite it."""
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "orders",
        "sql_table": "orders",
        "measures": [{
            "name": "revenue",
            "sql": "amount",
            "type": "sum",
            "allowed_aggregations": ["sum", "avg"],
        }],
    })
    assert m.columns[0].allowed_aggregations == ["sum", "avg"]


def test_model_v1_detector_handles_non_list_measures() -> None:
    """Malformed `measures` (e.g. a dict) must not crash the v1 detector — it
    should fall through so Pydantic raises the regular validation error."""
    with pytest.raises(Exception) as exc_info:
        SlayerModel.model_validate({
            "version": 1,
            "name": "orders",
            "measures": {"name": "revenue"},  # dict, not list
        })
    # The error should come from Pydantic validation, not a KeyError: 0
    # raised inside the v1 detector while subscripting raw_measures[0].
    assert not isinstance(exc_info.value, KeyError) or exc_info.value.args != (0,)


def test_model_v2_input_is_noop() -> None:
    """A v2 dict passes through migrate() unchanged at the version walker."""
    m = SlayerModel.model_validate({
        "version": 2,
        "name": "orders",
        "sql_table": "orders",
        "columns": [{"name": "status", "type": "string"}],
        "measures": [],
    })
    assert m.version == 2
    assert [c.name for c in m.columns] == ["status"]


def test_model_forward_version_passes_through() -> None:
    """A v3 dict (future) passes through; Pydantic ignores extras."""
    m = SlayerModel.model_validate({
        "version": 99,
        "name": "orders",
        "sql_table": "orders",
        "columns": [{"name": "status", "type": "string"}],
        "measures": [],
        "future_field": "ignored",
    })
    assert m.version == 99


def test_query_v1_to_v2_fields_renamed() -> None:
    """v1 SlayerQuery `fields` becomes v2 `measures`."""
    q = SlayerQuery.model_validate({
        "version": 1,
        "source_model": "orders",
        "fields": [{"formula": "revenue:sum", "name": "rev"}],
    })
    assert q.version == 2
    assert q.measures is not None
    assert q.measures[0].formula == "revenue:sum"
    assert q.measures[0].name == "rev"


def test_query_v1_to_v2_both_fields_and_measures_raises() -> None:
    """A v1 query with both keys is unmigratable."""
    with pytest.raises(ValueError, match="both 'fields' and 'measures'"):
        SlayerQuery.model_validate({
            "version": 1,
            "source_model": "orders",
            "fields": [{"formula": "revenue:sum"}],
            "measures": [{"formula": "revenue:avg"}],
        })


def test_query_v1_to_v2_inline_model_extension() -> None:
    """ModelExtension nested in source_model gets its dim/measures merged."""
    q = SlayerQuery.model_validate({
        "version": 1,
        "source_model": {
            "source_name": "orders",
            "dimensions": [{"name": "region", "type": "string"}],
            "measures": [{"name": "revenue", "sql": "amount"}],
        },
        "fields": [{"formula": "revenue:sum"}],
    })
    assert q.version == 2
    sm = q.source_model
    assert isinstance(sm, dict)
    assert sm["source_name"] == "orders"
    assert [c["name"] for c in sm["columns"]] == ["region", "revenue"]
    assert sm["measures"] == []


def test_query_v1_to_v2_inline_slayer_model_dict_is_left_for_model_migration() -> None:
    """An inline SlayerModel dict isn't pre-migrated by the query converter.

    SlayerQuery.source_model is typed as ``object`` so Pydantic doesn't recurse
    into it during query validation. The inline dict only gets migrated when
    the engine later runs ``SlayerModel.model_validate(sm)``. This test pins
    that boundary: the query keeps the dict verbatim, and feeding the same
    dict through SlayerModel produces the v2 shape.
    """
    inline = {
        "version": 1,
        "name": "orders",
        "data_source": "demo",
        "sql_table": "orders",
        "dimensions": [{"name": "status"}],
        "measures": [{"name": "revenue", "sql": "amount"}],
    }
    q = SlayerQuery.model_validate({
        "version": 1,
        "source_model": dict(inline),
        "fields": [{"formula": "revenue:sum"}],
    })
    # Query migration leaves the inline dict alone (still has v1 keys).
    assert q.measures is not None  # `fields` rename worked
    assert isinstance(q.source_model, dict)

    # When the engine validates the same inline dict as a SlayerModel, the
    # model-level migration runs and produces v2 shape.
    m = SlayerModel.model_validate(inline)
    assert m.version == 2
    assert [c.name for c in m.columns] == ["status", "revenue"]


def test_model_v1_to_v2_source_queries_nested_rename() -> None:
    """``source_queries`` entries are migrated in place at model-load time
    so re-saving the model doesn't persist the v1 ``fields`` key.
    """
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "saved",
        "data_source": "demo",
        "source_queries": [{
            "version": 1,
            "source_model": "orders",
            "fields": [{"formula": "revenue:sum", "name": "rev"}],
        }],
    })
    assert m.source_queries is not None
    assert len(m.source_queries) == 1
    # source_queries entries are parsed into SlayerQuery instances by
    # SlayerModel's before-validator after the v1→v2 rename runs.
    inner = m.source_queries[0]
    assert isinstance(inner, SlayerQuery)
    assert inner.measures is not None
    assert inner.measures[0].formula == "revenue:sum"
    assert inner.measures[0].name == "rev"
    assert inner.version == 2


def test_model_v1_to_v2_source_queries_with_inline_extension() -> None:
    """A nested SlayerQuery whose source_model is an inline ModelExtension
    also gets recursively migrated (extension dimensions+measures merged).
    """
    m = SlayerModel.model_validate({
        "version": 1,
        "name": "saved",
        "data_source": "demo",
        "source_queries": [{
            "version": 1,
            "source_model": {
                "source_name": "orders",
                "dimensions": [{"name": "status", "type": "string"}],
                "measures": [{"name": "revenue", "sql": "amount", "type": "number"}],
            },
            "fields": [{"formula": "revenue:sum"}],
        }],
    })
    inner = m.source_queries[0]
    assert isinstance(inner, SlayerQuery)
    # source_model on a SlayerQuery is typed as ``object`` and stays a dict
    # (ModelExtension shape) for the engine to interpret later.
    src = inner.source_model
    assert isinstance(src, dict)
    assert "dimensions" not in src
    # Merged into columns (status from dimensions + revenue from measures)
    col_names = sorted(c["name"] for c in src["columns"])
    assert col_names == ["revenue", "status"]


def test_model_v2_input_with_source_queries_preserved() -> None:
    """v2 input with already-v2 source_queries entries is left alone."""
    m = SlayerModel.model_validate({
        "version": 2,
        "name": "saved",
        "data_source": "demo",
        "columns": [],
        "measures": [],
        "source_queries": [{
            "version": 2,
            "source_model": "orders",
            "measures": [{"formula": "revenue:sum", "name": "rev"}],
        }],
    })
    inner = m.source_queries[0]
    assert isinstance(inner, SlayerQuery)
    assert inner.measures is not None
    assert inner.measures[0].formula == "revenue:sum"
    assert inner.measures[0].name == "rev"


async def test_v1_yaml_round_trip_to_v2() -> None:
    """Hand-write a v1 YAML, load via storage, observe v2 shape on disk after save."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        legacy_path = os.path.join(storage.models_dir, "orders.yaml")
        with open(legacy_path, "w") as f:
            yaml.dump({
                "version": 1,
                "name": "orders",
                "data_source": "demo",
                "sql_table": "orders",
                "dimensions": [{"name": "status", "type": "string"}],
                "measures": [{"name": "revenue", "sql": "amount"}],
            }, f)

        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == 2
        assert [c.name for c in loaded.columns] == ["status", "revenue"]
        assert loaded.measures == []

        # Re-save and confirm v2 on disk.
        await storage.save_model(loaded)
        with open(legacy_path) as f:
            on_disk = yaml.safe_load(f)
        assert on_disk["version"] == 2
        assert "columns" in on_disk
        assert "dimensions" not in on_disk


async def test_v1_sqlite_round_trip_to_v2() -> None:
    """Same round-trip, but via SQLiteStorage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "slayer.db")
        storage = SQLiteStorage(db_path=db_path)
        legacy_blob = json.dumps({
            "version": 1,
            "name": "orders",
            "data_source": "demo",
            "sql_table": "orders",
            "dimensions": [{"name": "status", "type": "string"}],
            "measures": [{"name": "revenue", "sql": "amount"}],
        })
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO models (name, data) VALUES (?, ?)",
                ("orders", legacy_blob),
            )

        loaded = await storage.get_model("orders")
        assert loaded is not None
        assert loaded.version == 2
        assert [c.name for c in loaded.columns] == ["status", "revenue"]
