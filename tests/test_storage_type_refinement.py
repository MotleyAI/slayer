"""DEV-1361: storage-driven type refinement on load.

The v4→v5 dict migrator does coarse rename only (``number`` → ``DOUBLE``).
A separate sync helper, ``slayer.storage.type_refinement.refine_dict_with_live_schema``,
introspects the model's datasource and refines ``DOUBLE`` → ``INT`` for base
columns whose live SQL type is integer. Storage backends call this helper
during ``get_model`` and write back the refined dict so subsequent loads are
free.

Hard-fail behavior: if the datasource is unreachable, the SQLAlchemy connect
error propagates out of ``get_model``. Same effective behavior as a query
against the DS would produce.
"""

import os
import sqlite3
import tempfile
from typing import Any
from unittest.mock import patch

import pytest
import sqlalchemy as sa
import yaml

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.storage import migrations as mig
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_with_int_double_text():
    """A real SQLite file with three columns of distinct types so live
    introspection produces a meaningful Dict[str, DataType].
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "live.db")
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, amount REAL, name TEXT, qty INTEGER)"
            )
            conn.commit()
        finally:
            conn.close()
        yield {
            "tmpdir": tmpdir,
            "db_path": db_path,
            "table": "items",
        }


@pytest.fixture
def storage_with_v4_model(sqlite_with_int_double_text):
    """A YAMLStorage backend with a hand-written v4 model on disk pointing at
    the ``items`` table. ``id`` and ``qty`` are INT in the live DB but stored
    as legacy ``number`` in the YAML — refinement should narrow them to
    ``INT`` on first load. ``amount`` is REAL in the live DB → stays
    ``DOUBLE``. ``name`` is TEXT in the live DB → stays ``TEXT`` (no
    refinement attempted for non-numeric).
    """
    base = sqlite_with_int_double_text["tmpdir"]
    table = sqlite_with_int_double_text["table"]
    db_path = sqlite_with_int_double_text["db_path"]

    # Datasource — points the SQLite file.
    datasources_dir = os.path.join(base, "datasources")
    os.makedirs(datasources_dir, exist_ok=True)
    with open(os.path.join(datasources_dir, "live.yaml"), "w") as f:
        yaml.dump(
            {
                "name": "live",
                "type": "sqlite",
                "database": db_path,
                "version": 1,
            },
            f,
        )

    # v4 model file at the v4 namespaced layout. All numeric columns use the
    # legacy ``number`` value; the migrator coarsens them to ``DOUBLE`` on
    # load, then introspection refines INT-backed ones back to ``INT``.
    models_dir = os.path.join(base, "models", "live")
    os.makedirs(models_dir, exist_ok=True)
    model_path = os.path.join(models_dir, "items.yaml")
    with open(model_path, "w") as f:
        yaml.dump(
            {
                "version": 4,
                "name": "items",
                "sql_table": table,
                "data_source": "live",
                "columns": [
                    {"name": "id", "sql": "id", "type": "number", "primary_key": True},
                    {"name": "amount", "sql": "amount", "type": "number"},
                    {"name": "name", "sql": "name", "type": "string"},
                    {"name": "qty", "sql": "qty", "type": "number"},
                    # Derived (non-base) numeric column: refinement must leave
                    # this alone because its sql isn't a bare identifier.
                    {"name": "double_amount", "sql": "items.amount * 2", "type": "number"},
                ],
            },
            f,
        )

    storage = YAMLStorage(base_dir=base)
    yield {
        "storage": storage,
        "base": base,
        "model_path": model_path,
        "db_path": db_path,
    }


# ---------------------------------------------------------------------------
# refine_dict_with_live_schema — pure helper unit tests
# ---------------------------------------------------------------------------


class TestRefineDictWithLiveSchema:
    """Direct unit-tests on the helper function so a regression in the
    refinement rule shows up without YAMLStorage round-trips."""

    def _ds_for(self, db_path: str) -> DatasourceConfig:
        return DatasourceConfig(
            name="live",
            type="sqlite",
            database=db_path,
        )

    def test_refines_double_to_int_when_live_is_int(self, sqlite_with_int_double_text) -> None:
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "id", "sql": "id", "type": "DOUBLE", "primary_key": True},
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is True
        assert d["columns"][0]["type"] == "INT"
        assert d["columns"][1]["type"] == "INT"

    def test_leaves_double_for_real_columns(self, sqlite_with_int_double_text) -> None:
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "amount", "sql": "amount", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is False
        assert d["columns"][0]["type"] == "DOUBLE"

    def test_skips_text_and_other_types(self, sqlite_with_int_double_text) -> None:
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "name", "sql": "name", "type": "TEXT"},
                {"name": "amount", "sql": "amount", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        refine_dict_with_live_schema(d, ds)
        assert d["columns"][0]["type"] == "TEXT"
        assert d["columns"][1]["type"] == "DOUBLE"

    def test_skips_non_base_derived_columns(self, sqlite_with_int_double_text) -> None:
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                # Non-bare sql: this is a derived column whose live type is
                # unknown — refinement must NOT touch it even if id is INT.
                {"name": "double_id", "sql": "id * 2", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is False
        assert d["columns"][0]["type"] == "DOUBLE"

    def test_skips_query_backed_models(self) -> None:
        """Models without ``sql_table`` (e.g. query-backed) must short-circuit."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "rollup",
            "data_source": "live",
            "source_queries": [{"source_model": "items", "measures": [{"formula": "qty:sum"}]}],
            "columns": [
                {"name": "qty_sum", "sql": "qty_sum", "type": "DOUBLE"},
            ],
        }
        ds = DatasourceConfig(name="live", type="sqlite", database=":memory:")
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is False
        assert d["columns"][0]["type"] == "DOUBLE"

    def test_skips_sql_mode_models(self) -> None:
        """Models in ``sql`` source-mode (explicit subquery) must short-circuit."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "rollup",
            "sql": "SELECT * FROM items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        ds = DatasourceConfig(name="live", type="sqlite", database=":memory:")
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is False
        assert d["columns"][0]["type"] == "DOUBLE"

    def test_unreachable_datasource_propagates(self) -> None:
        """DS unreachable → SQLAlchemy connect error propagates. Hard-fail per
        DEV-1361 plan."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        # An unreachable Postgres URL → connect raises during introspection.
        ds = DatasourceConfig(
            name="live",
            type="postgres",
            host="127.0.0.1",
            port=1,  # closed
            database="nope",
            username="nobody",
            password="nope",  # NOSONAR(S2068) — test fixture, not a real credential; targets a closed port to assert hard-fail
        )
        with pytest.raises(sa.exc.OperationalError):
            refine_dict_with_live_schema(d, ds)


# ---------------------------------------------------------------------------
# YAMLStorage end-to-end: refinement + write-back on first load
# ---------------------------------------------------------------------------


class TestYamlStorageRefinementOnLoad:
    async def test_first_load_refines_int_columns(self, storage_with_v4_model) -> None:
        loaded = await storage_with_v4_model["storage"].get_model("items", data_source="live")
        assert loaded is not None
        # Map name -> type for ergonomic assertions.
        types = {c.name: c.type for c in loaded.columns}
        assert types["id"] == DataType.INT
        assert types["qty"] == DataType.INT
        assert types["amount"] == DataType.DOUBLE
        assert types["name"] == DataType.TEXT
        # Non-base derived column stays at DOUBLE (no refinement attempted).
        assert types["double_amount"] == DataType.DOUBLE

    async def test_first_load_writes_back_v5_with_refined_types(
        self, storage_with_v4_model
    ) -> None:
        await storage_with_v4_model["storage"].get_model("items", data_source="live")
        # Re-read raw YAML; the storage layer writes back at CURRENT_VERSIONS,
        # whatever that currently is. Pre-DEV-1480 this was hard-coded to 6;
        # post-bump it's whatever migrations.py declares.
        with open(storage_with_v4_model["model_path"]) as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            raw = yaml.safe_load(f)
        assert raw["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        types_by_name = {c["name"]: c["type"] for c in raw["columns"]}
        assert types_by_name["id"] == "INT"
        assert types_by_name["qty"] == "INT"
        assert types_by_name["amount"] == "DOUBLE"
        assert types_by_name["name"] == "TEXT"
        assert types_by_name["double_amount"] == "DOUBLE"

    async def test_second_load_does_not_introspect(self, storage_with_v4_model) -> None:
        """After write-back, the on-disk dict is v5 with refined types. The
        migrator chain returns early (version >= current) and the storage
        backend's refinement-helper invocation gate doesn't trigger."""
        # First load: triggers refinement.
        await storage_with_v4_model["storage"].get_model("items", data_source="live")

        # Now spy on _live_schema_for_datasource — it must NOT be called the
        # second time around, because the model on disk is now v5 with refined
        # types and the migrator chain returns immediately.
        with patch(
            "slayer.engine.schema_drift._live_schema_for_datasource",
            wraps=_unreachable,
        ) as spy:
            await storage_with_v4_model["storage"].get_model("items", data_source="live")
            spy.assert_not_called()

    async def test_missing_datasource_entry_raises_on_v4_load(
        self, sqlite_with_int_double_text
    ) -> None:
        """If the v4 model exists but its referenced datasource entry is gone,
        ``get_model`` must raise — silently skipping refinement and writing the
        v5 dict back would freeze base integer columns at ``DOUBLE`` forever
        (next load short-circuits on the version check).
        """
        base = sqlite_with_int_double_text["tmpdir"]
        # No datasources/ dir → get_datasource("live") returns None.
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": 4,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [
                        {"name": "id", "sql": "id", "type": "number"},
                    ],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        with pytest.raises(ValueError, match="datasource 'live' is unavailable"):
            await storage.get_model("items", data_source="live")

    async def test_unreachable_datasource_propagates_through_get_model(
        self, sqlite_with_int_double_text, monkeypatch
    ) -> None:
        """If introspection fails during first-load refinement, the error
        propagates out of get_model — the v4 model isn't silently kept at
        coarse types."""
        from slayer.engine import schema_drift

        base = sqlite_with_int_double_text["tmpdir"]
        # Datasource record on disk; pointing it at the SQLite file is fine
        # for the loader's get_datasource path.
        datasources_dir = os.path.join(base, "datasources")
        os.makedirs(datasources_dir, exist_ok=True)
        with open(os.path.join(datasources_dir, "live.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {"name": "live", "type": "sqlite", "database": sqlite_with_int_double_text["db_path"], "version": 1},
                f,
            )
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": 4,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [
                        {"name": "id", "sql": "id", "type": "number"},
                    ],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)

        def _boom(*, datasource: Any, schema: Any = None) -> Any:
            raise sa.exc.OperationalError("simulated", None, Exception("connect refused"))  # NOSONAR(S112) — Exception(...) is the cause-of arg for the simulated SQLAlchemy connect error

        monkeypatch.setattr(schema_drift, "_live_schema_for_datasource", _boom)
        with pytest.raises(sa.exc.OperationalError):
            await storage.get_model("items", data_source="live")


def _unreachable(**kw):  # used as a wraps target only — the spy.assert_not_called check fires first
    raise AssertionError("Unexpectedly called _live_schema_for_datasource on a v5 model")


# ---------------------------------------------------------------------------
# CLI: slayer storage migrate-types
# ---------------------------------------------------------------------------


class TestCliMigrateTypes:
    """The CLI subcommand exposes the same refinement step as a batch /
    inspectable tool. ``--dry-run`` reports planned refinements without
    writing; without it, refinements are persisted."""

    async def test_dry_run_reports_without_writing(  # NOSONAR(S7503) — pytest-asyncio test body; capsys fixture wired in async context
        self, storage_with_v4_model, capsys
    ) -> None:
        from slayer.cli import _run_storage  # introduced in Phase 2.9

        args = _build_args(
            command="storage",
            subcommand="migrate-types",
            storage=storage_with_v4_model["base"],
            models_dir=None,
            dry_run=True,
            data_source=None,
        )
        _run_storage(args)
        # On-disk YAML must remain at v4 (no write-back during dry-run).
        with open(storage_with_v4_model["model_path"]) as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            raw = yaml.safe_load(f)
        assert raw["version"] == 4
        # Output should mention the planned refinements.
        out = capsys.readouterr().out
        assert "id" in out
        assert "INT" in out

    async def test_apply_writes_refinements(self, storage_with_v4_model) -> None:  # NOSONAR(S7503) — pytest-asyncio test body; sync run via _run_storage
        from slayer.cli import _run_storage

        args = _build_args(
            command="storage",
            subcommand="migrate-types",
            storage=storage_with_v4_model["base"],
            models_dir=None,
            dry_run=False,
            data_source=None,
        )
        _run_storage(args)
        with open(storage_with_v4_model["model_path"]) as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            raw = yaml.safe_load(f)
        assert raw["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        types_by_name = {c["name"]: c["type"] for c in raw["columns"]}
        assert types_by_name["id"] == "INT"
        assert types_by_name["qty"] == "INT"

    async def test_missing_datasource_raises_for_refineable_model(self, tmp_path) -> None:  # NOSONAR(S7503) — pytest-asyncio test body; sync run via _run_storage
        """Mirror of the ABC's raise: the CLI must fail loudly rather than
        silently report 'nothing to refine' for a v4 model whose datasource
        entry has been removed."""
        from slayer.cli import _refine_one_model_for_cli

        base = str(tmp_path)
        # Lay down a v4 YAML model with a refineable DOUBLE base column but
        # no datasources/<name>.yaml file alongside it.
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": 4,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [{"name": "id", "sql": "id", "type": "number"}],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        with pytest.raises(ValueError, match="datasource 'live' is unavailable"):
            _refine_one_model_for_cli(
                inner=storage, ds_name="live", model_name="items", dry_run=True,
            )

    async def test_missing_datasource_silent_for_text_only_model(self, tmp_path) -> None:  # NOSONAR(S7503) — pytest-asyncio test body; sync run via _run_storage
        """Models with no refineable DOUBLE base columns (text-only here) must
        load through the CLI without requiring a live datasource entry."""
        from slayer.cli import _refine_one_model_for_cli

        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        with open(os.path.join(models_dir, "events.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": 4,
                    "name": "events",
                    "sql_table": "events",
                    "data_source": "live",
                    "columns": [{"name": "tag", "sql": "tag", "type": "string"}],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        # No raise — returns False because nothing needed refinement.
        result = _refine_one_model_for_cli(
            inner=storage, ds_name="live", model_name="events", dry_run=True,
        )
        assert result is False


def _build_args(**kw):
    from types import SimpleNamespace

    return SimpleNamespace(**kw)
