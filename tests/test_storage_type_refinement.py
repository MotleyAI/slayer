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
from pathlib import Path
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

    DEV-1538: SQLite-aware refinement is probe-driven, so the fixture
    inserts integer rows into ``id`` and ``qty`` (the columns the tests
    expect to narrow DOUBLE → INT). Without data, the probe returns
    ``None`` and the narrowing wouldn't fire.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "live.db")
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, amount REAL, name TEXT, qty INTEGER)"
            )
            # DEV-1538: integer rows so the probe positively certifies INT
            # for id and qty. amount has no rows here on purpose — that
            # gives the probe no evidence and exercises the
            # "REAL-declared, empty storage" path that
            # ``test_leaves_double_for_real_columns`` pins.
            for i in range(1, 4):
                conn.execute(
                    "INSERT INTO items (id, qty) VALUES (?, ?)",
                    (i, i * 10),
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
        coarse types.

        DEV-1538 update: the SQLite branch of ``refine_dict_with_live_schema``
        opens its own SA engine via ``sa.create_engine`` (rather than going
        through ``_live_schema_for_datasource`` like the non-SQLite path).
        Patch the SQLite probe's engine factory so a connect failure
        propagates identically.
        """
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

        from slayer.storage import type_refinement

        def _boom(*_args, **_kw):
            raise sa.exc.OperationalError("simulated", None, Exception("connect refused"))  # NOSONAR(S112) — Exception(...) is the cause-of arg for the simulated SQLAlchemy connect error

        # SQLite refinement runs via sa.create_engine inside type_refinement;
        # the same engine factory that any consumer would use must surface
        # the connect failure.
        monkeypatch.setattr(type_refinement.sa, "create_engine", _boom)
        with pytest.raises(sa.exc.OperationalError):
            await storage.get_model("items", data_source="live")


def _unreachable(**kw):  # used as a wraps target only — the spy.assert_not_called check fires first
    raise AssertionError("Unexpectedly called _live_schema_for_datasource on a v5 model")


# ---------------------------------------------------------------------------
# DEV-1538: load-time SQLite affinity probe on legacy-dict migration.
#
# The v5 refinement is wrong on SQLite because it trusts the declared
# affinity. The DEV-1538 fix:
#
# 1. SQLite branch in ``refine_dict_with_live_schema`` runs the per-column
#    value probe for every persisted INT base column AND skips the existing
#    DOUBLE → INT narrowing entirely.
# 2. Already-v7 dicts on SQLite remain untouched on load — DEV-1538 only
#    affects the migration-write-back path. Re-ingest is the auto-heal for
#    v7 models.
# 3. The narrowing is unchanged on non-SQLite datasources.
# ---------------------------------------------------------------------------


def _create_sqlite_with_int_storage(db_path: str, values: list) -> None:
    """Build a SQLite file with one INTEGER-declared column holding the
    given per-row typed values (preserves storage classes)."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)"
        )
        for i, v in enumerate(values, start=1):
            conn.execute("INSERT INTO items VALUES (?, ?)", (i, v))
        conn.commit()
    finally:
        conn.close()


class TestRefineSqliteAffinityProbe:
    """DEV-1538: ``refine_dict_with_live_schema`` SQLite branch."""

    def _ds_for(self, db_path: str) -> DatasourceConfig:
        return DatasourceConfig(name="live", type="sqlite", database=db_path)

    def test_sqlite_int_with_real_storage_widens_to_double(
        self, tmp_path: Path
    ) -> None:
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7, 0.9])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {
                    "name": "qty", "sql": "qty",
                    "type": "INT",
                    "format": {"type": "integer"},
                },
            ],
        }
        ds = self._ds_for(db_path)
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is True
        assert d["columns"][0]["type"] == "DOUBLE"
        # Auto-default integer format is updated to FLOAT.
        assert d["columns"][0]["format"]["type"] == "float"

    def test_sqlite_double_narrows_to_int_when_probe_certifies_int(
        self, tmp_path: Path
    ) -> None:
        """On SQLite, the DEV-1361 DOUBLE → INT narrowing is now probe-
        verified rather than declared-type-driven. With all-integer
        storage, the probe certifies INT and the narrowing fires (the
        DEV-1361 contract is preserved on SQLite when the probe agrees).
        """
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 2, 3])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(db_path)
        refine_dict_with_live_schema(d, ds)
        assert d["columns"][0]["type"] == "INT"

    def test_sqlite_double_stays_double_when_probe_says_double(
        self, tmp_path: Path
    ) -> None:
        """A persisted DOUBLE column stays DOUBLE when the probe sees REAL
        values — the DEV-1361 narrowing would have flipped it to INT based
        on the declared affinity, but the probe knows better."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(db_path)
        refine_dict_with_live_schema(d, ds)
        assert d["columns"][0]["type"] == "DOUBLE"

    def test_sqlite_double_stays_double_when_probe_returns_none(
        self, tmp_path: Path
    ) -> None:
        """A None probe verdict (failure or saturation) is conservative:
        the persisted DOUBLE stays DOUBLE."""
        from unittest.mock import patch
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 2, 3])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        ds = self._ds_for(db_path)
        with patch(
            "slayer.sql.sqlite_introspect.probe_sqlite_integer_column",
            return_value=None,
        ):
            refine_dict_with_live_schema(d, ds)
        assert d["columns"][0]["type"] == "DOUBLE"

    def test_postgres_still_narrows_double_to_int(self) -> None:
        """Sanity: the DEV-1361 narrowing rule is preserved for non-SQLite
        datasources. (Postgres datasource is unreachable here, but the
        SQLite-only carve-out must not apply.)"""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "DOUBLE"},
            ],
        }
        # Use a closed-port Postgres URL; the connect must raise (not
        # silently no-op like the SQLite skip would).
        ds = DatasourceConfig(
            name="live",
            type="postgres",
            host="127.0.0.1",
            port=1,
            database="nope",
            username="nobody",
            password="nope",  # NOSONAR(S2068)
        )
        with pytest.raises(sa.exc.OperationalError):
            refine_dict_with_live_schema(d, ds)

    def test_sqlite_int_with_pure_int_storage_stays_int(
        self, tmp_path: Path
    ) -> None:
        """All-INTEGER storage → probe returns INT → no widening, type
        preserved."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 2, 3])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "INT"},
            ],
        }
        ds = self._ds_for(db_path)
        refine_dict_with_live_schema(d, ds)
        assert d["columns"][0]["type"] == "INT"

    def test_custom_format_preserved_on_widening(self, tmp_path: Path) -> None:
        """A user-set custom format on a widening column is left untouched."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {
                    "name": "qty", "sql": "qty",
                    "type": "INT",
                    "format": {
                        "type": "currency",
                        "symbol": "€",
                        "precision": 3,
                    },
                },
            ],
        }
        ds = self._ds_for(db_path)
        refine_dict_with_live_schema(d, ds)
        assert d["columns"][0]["type"] == "DOUBLE"
        # Custom format preserved verbatim.
        assert d["columns"][0]["format"]["type"] == "currency"
        assert d["columns"][0]["format"]["symbol"] == "€"
        assert d["columns"][0]["format"]["precision"] == 3

    def test_sqlite_int_with_text_storage_widens_to_text(
        self, tmp_path: Path
    ) -> None:
        """Persisted INT column whose live storage is non-coercible TEXT
        widens to TEXT and the auto-default integer format is cleared."""
        from slayer.storage.type_refinement import refine_dict_with_live_schema

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, "abc", "xyz"])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {
                    "name": "qty", "sql": "qty",
                    "type": "INT",
                    "format": {"type": "integer"},
                },
            ],
        }
        ds = self._ds_for(db_path)
        changed = refine_dict_with_live_schema(d, ds)
        assert changed is True
        assert d["columns"][0]["type"] == "TEXT"
        # Auto-default integer format must be cleared on TEXT widening.
        assert d["columns"][0].get("format") in (None, {})


class TestHasRefineableColumnsSqliteIntBranch:
    """DEV-1538: SQLite-INT widening is gated by ``has_sqlite_widenable_columns``
    (best-effort, no DS hard-fail). DEV-1361 DOUBLE narrowing stays gated by
    ``has_refineable_columns`` (DS required). The split was added after Codex
    caught a regression where the broadened predicate made non-SQLite legacy
    INT-only dicts hard-fail on missing DS — pre-DEV-1538 they didn't."""

    def test_sqlite_int_base_column_is_widenable_not_refineable(self) -> None:
        from slayer.storage.type_refinement import (
            has_refineable_columns,
            has_sqlite_widenable_columns,
        )

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "qty", "sql": "qty", "type": "INT"},
            ],
        }
        # INT base columns are *widenable* (best-effort), not *refineable*
        # (mandatory DS); this asymmetry keeps non-SQLite legacy INT-only
        # dicts loadable when the datasource entry is gone.
        assert has_sqlite_widenable_columns(d) is True
        assert has_refineable_columns(d) is False

    def test_double_base_column_is_refineable_not_widenable(self) -> None:
        from slayer.storage.type_refinement import (
            has_refineable_columns,
            has_sqlite_widenable_columns,
        )

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "columns": [
                {"name": "amount", "sql": "amount", "type": "DOUBLE"},
            ],
        }
        assert has_refineable_columns(d) is True
        assert has_sqlite_widenable_columns(d) is False

    async def test_sqlite_legacy_dict_with_int_loads_when_datasource_missing(
        self, tmp_path: Path, caplog
    ) -> None:
        """DEV-1538: when a legacy dict has ONLY INT base columns (no
        DOUBLE), a missing datasource is NOT a hard fail. The persisted
        INT is a safe default — re-ingest will heal any mis-typed columns
        once the DS is back. A WARNING is logged so the skip is visible.

        This is the corrected behavior after Codex caught a regression
        where the original predicate broadening made non-SQLite legacy
        INT-only dicts hard-fail on missing DS (pre-DEV-1538 they didn't).
        """
        import logging
        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        # Use a recent legacy version that genuinely carries the modern
        # "INT" type token. (Pre-DEV-1361 v4 dicts only know the legacy
        # "number" / "string" tokens — pinning "version": 4 with "type":
        # "INT" wouldn't represent any real on-disk model shape.)
        legacy_version = mig.CURRENT_VERSIONS["SlayerModel"] - 1
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": legacy_version,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [
                        {"name": "qty", "sql": "qty", "type": "INT"},
                    ],
                },
                f,
            )
        # Don't register the datasource — load should succeed (skip probe,
        # log warning), NOT hard-fail like a DOUBLE-base-column dict would.
        storage = YAMLStorage(base_dir=base)
        with caplog.at_level(logging.WARNING, logger="slayer.storage.base"):
            loaded = await storage.get_model("items", data_source="live")
        assert loaded is not None
        col = next(c for c in loaded.columns if c.name == "qty")
        # Persisted INT preserved as the safe default.
        assert col.type is DataType.INT
        # Warning emitted naming the model + datasource.
        msgs = [r.getMessage() for r in caplog.records]
        assert any("items" in m and "live" in m for m in msgs), msgs

    async def test_legacy_dict_with_double_still_raises_when_datasource_missing(
        self, tmp_path: Path
    ) -> None:
        """The DEV-1361 hard-fail contract is preserved: a legacy dict with
        ANY DOUBLE base column still hard-fails when the datasource is
        missing (live introspection is required to narrow safely)."""
        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        legacy_version = mig.CURRENT_VERSIONS["SlayerModel"] - 1
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": legacy_version,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [
                        # DOUBLE base column → DEV-1361 narrowing required.
                        {"name": "amount", "sql": "amount", "type": "DOUBLE"},
                    ],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        with pytest.raises(ValueError, match="datasource 'live' is unavailable"):
            await storage.get_model("items", data_source="live")


class TestV7SqliteModelNotAutoRepairedOnLoad:
    """DEV-1538 non-goal: already-current-version SQLite models with the
    wrong INT type are NOT auto-repaired on load. Re-ingest is the
    auto-heal path. Loading must not trigger the probe."""

    async def test_current_version_sqlite_model_untouched_on_load(
        self, tmp_path: Path
    ) -> None:
        from unittest.mock import patch

        from slayer.storage import migrations as mig

        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7, 0.9])

        # Write a current-version dict on disk (no migration will run).
        base = str(tmp_path / "storage")
        datasources_dir = os.path.join(base, "datasources")
        os.makedirs(datasources_dir, exist_ok=True)
        with open(os.path.join(datasources_dir, "live.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "name": "live",
                    "type": "sqlite",
                    "database": db_path,
                    "version": 1,
                },
                f,
            )
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": mig.CURRENT_VERSIONS["SlayerModel"],
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [
                        {
                            "name": "id", "sql": "id",
                            "type": "INT", "primary_key": True,
                        },
                        # WRONG persisted type: live storage is REAL but
                        # persisted INT. Load must NOT widen it.
                        {"name": "qty", "sql": "qty", "type": "INT"},
                    ],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)

        # The probe must NOT be called on a current-version dict load.
        with patch(
            "slayer.sql.sqlite_introspect.probe_sqlite_integer_column",
            side_effect=AssertionError(
                "probe must not run on already-current-version dict load"
            ),
        ):
            loaded = await storage.get_model("items", data_source="live")
        # Type stays INT because the probe never ran.
        col = next(c for c in loaded.columns if c.name == "qty")
        assert col.type is DataType.INT


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
