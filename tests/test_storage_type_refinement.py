"""Storage-driven type refinement on load.

The v4→v5 dict migrator does coarse rename only (``number`` → ``DOUBLE``).
A separate sync helper, ``slayer.live_schema.type_refinement.refine_dict_with_live_schema``,
introspects the dataset's datasource and refines ``DOUBLE`` → ``INT`` for base
fields whose live SQL type is integer. Storage backends call this helper
during ``get_dataset`` and write back the refined dict so subsequent loads are
free.

Hard-fail behavior: if the datasource is unreachable, the SQLAlchemy connect
error propagates out of ``get_model``. Same effective behavior as a query
against the DS would produce.
"""

from collections.abc import AsyncGenerator
from typing import TypedDict
import logging
import os
from slayer.storage.sqlite_conn import transaction
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError
import yaml

from slayer.cli import _refine_one_dataset_for_cli, _run_storage
from slayer.core.enums import DataType
from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.database.client import DatabaseUnavailable, SlayerSQLClient
from slayer.storage import migrations as mig
from slayer.live_schema.type_refinement import (
    has_refineable_fields,
    has_sqlite_widenable_fields,
    refine_dict_with_live_schema,
)
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.base import StorageBackend

# Newest on-disk schema still live-refined on load.
_NEWEST_REFINED_VERSION = 7


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


async def _refine(d: dict, ds: DatasourceConfig) -> bool:
    """``refine_dict_with_live_schema`` through a fresh client, closed afterwards."""
    client = SlayerSQLClient(datasource=ds)
    try:
        return await refine_dict_with_live_schema(d, ds, client)
    finally:
        await client.aclose()
        client.close()


@pytest.fixture
def sqlite_with_int_double_text():
    """A real SQLite file with three columns of distinct types so live
    introspection produces a meaningful Dict[str, DataType].

    SQLite-aware refinement is probe-driven, so the fixture
    inserts integer rows into ``id`` and ``qty`` (the columns the tests
    expect to narrow DOUBLE → INT). Without data, the probe returns
    ``None`` and the narrowing wouldn't fire.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "live.db")
        with transaction(db_path) as conn:
            conn.execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, amount REAL, name TEXT, qty INTEGER)"
            )
            # Integer rows so the probe positively certifies INT
            # for id and qty. amount has no rows here on purpose — that
            # gives the probe no evidence and exercises the
            # "REAL-declared, empty storage" path that
            # ``test_leaves_double_for_real_columns`` pins.
            for i in range(1, 4):
                conn.execute(
                    "INSERT INTO items (id, qty) VALUES (?, ?)",
                    (i, i * 10),
                )
        yield {
            "tmpdir": tmpdir,
            "db_path": db_path,
            "table": "items",
        }


class V4Store(TypedDict):
    storage: StorageBackend
    base: str
    model_path: str
    db_path: str


@pytest.fixture
async def storage_with_v4_model(sqlite_with_int_double_text) -> AsyncGenerator[V4Store, None]:
    """A YAMLStorage backend with a hand-written v4 dataset on disk pointing at
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

    # v4 dataset file at the v4 namespaced layout. All numeric fields use the
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
                    # Derived (non-base) numeric field: refinement must leave
                    # this alone because its sql isn't a bare identifier.
                    {"name": "double_amount", "sql": "items.amount * 2", "type": "number"},
                ],
            },
            f,
        )

    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=base))
    yield V4Store(storage=engine.storage, base=base, model_path=model_path, db_path=db_path)
    await engine.aclose()
    engine.close()


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

    async def test_refines_double_to_int_when_live_is_int(self, sqlite_with_int_double_text) -> None:
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "id", "expression": "id", "datatype": "Float", "primary_key": True},
                {"name": "qty", "expression": "qty", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        changed = await _refine(d, ds)
        assert changed is True
        assert d["fields"][0]["datatype"] == "Integer"
        assert d["fields"][1]["datatype"] == "Integer"

    async def test_leaves_double_for_real_columns(self, sqlite_with_int_double_text) -> None:
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "amount", "expression": "amount", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        changed = await _refine(d, ds)
        assert changed is False
        assert d["fields"][0]["datatype"] == "Float"

    async def test_skips_text_and_other_types(self, sqlite_with_int_double_text) -> None:
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "name", "expression": "name", "datatype": "String"},
                {"name": "amount", "expression": "amount", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        await _refine(d, ds)
        assert d["fields"][0]["datatype"] == "String"
        assert d["fields"][1]["datatype"] == "Float"

    async def test_skips_non_base_derived_columns(self, sqlite_with_int_double_text) -> None:
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                # Non-bare sql: this is a derived field whose live type is
                # unknown — refinement must NOT touch it even if id is INT.
                {"name": "double_id", "expression": "id * 2", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(sqlite_with_int_double_text["db_path"])
        changed = await _refine(d, ds)
        assert changed is False
        assert d["fields"][0]["datatype"] == "Float"

    async def test_skips_query_backed_models(self) -> None:
        """Datasets without ``sql_table`` (e.g. query-backed) must short-circuit."""
        d = {
            "name": "rollup",
            "data_source": "live",
            "source_queries": [{"population": "items", "metrics": [{"metric": "sum(items.qty)"}]}],
            "fields": [
                {"name": "qty_sum", "expression": "qty_sum", "datatype": "Float"},
            ],
        }
        ds = DatasourceConfig(name="live", type="sqlite", database=":memory:")
        changed = await _refine(d, ds)
        assert changed is False
        assert d["fields"][0]["datatype"] == "Float"

    async def test_skips_sql_mode_models(self) -> None:
        """Datasets in ``sql`` source-mode (explicit subquery) must short-circuit."""
        d = {
            "name": "rollup",
            "sql": "SELECT * FROM items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Float"},
            ],
        }
        ds = DatasourceConfig(name="live", type="sqlite", database=":memory:")
        changed = await _refine(d, ds)
        assert changed is False
        assert d["fields"][0]["datatype"] == "Float"

    async def test_unreachable_datasource_propagates(self) -> None:
        """DS unreachable → SQLAlchemy connect error propagates (hard-fail)."""
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Float"},
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
        with pytest.raises(DatabaseUnavailable):
            await _refine(d, ds)


# ---------------------------------------------------------------------------
# YAMLStorage end-to-end: refinement + write-back on first load
# ---------------------------------------------------------------------------


class TestYamlStorageRefinementOnLoad:
    async def test_first_load_refines_int_columns(self, storage_with_v4_model: V4Store) -> None:
        loaded = await storage_with_v4_model["storage"].get_dataset("items", data_source="live")
        assert loaded is not None
        # Map name -> type for ergonomic assertions.
        types = {c.name: c.datatype for c in loaded.fields}
        assert types["id"] == DataType.INTEGER
        assert types["qty"] == DataType.INTEGER
        assert types["amount"] == DataType.FLOAT
        assert types["name"] == DataType.STRING
        # Non-base derived field stays at DOUBLE (no refinement attempted).
        assert types["double_amount"] == DataType.FLOAT

    async def test_first_load_writes_back_v5_with_refined_types(
        self, storage_with_v4_model: V4Store
    ) -> None:
        await storage_with_v4_model["storage"].get_dataset("items", data_source="live")
        # Re-read raw YAML; the storage layer writes back at CURRENT_VERSIONS,
        # whatever migrations.py declares.
        with open(storage_with_v4_model["model_path"]) as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            raw = yaml.safe_load(f)
        assert raw["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        types_by_name = {c["name"]: c["datatype"] for c in raw["fields"]}
        assert types_by_name["id"] == "Integer"
        assert types_by_name["qty"] == "Integer"
        assert types_by_name["amount"] == "Float"
        assert types_by_name["name"] == "String"
        assert types_by_name["double_amount"] == "Float"

    async def test_second_load_does_not_introspect(self, storage_with_v4_model: V4Store) -> None:
        """After write-back, the on-disk dict is v5 with refined types. The
        migrator chain returns early (version >= current) and the storage
        backend's refinement-helper invocation gate doesn't trigger."""
        # First load: triggers refinement.
        await storage_with_v4_model["storage"].get_dataset("items", data_source="live")

        # Now spy on _live_schema_for_datasource — it must NOT be called the
        # second time around, because the dataset on disk is now v5 with refined
        # types and the migrator chain returns immediately.
        with patch(
            "slayer.live_schema.schema_drift._live_schema_for_datasource",
            wraps=_unreachable,
        ) as spy:
            await storage_with_v4_model["storage"].get_dataset("items", data_source="live")
            spy.assert_not_called()

    async def test_missing_datasource_entry_raises_on_v4_load(
        self, sqlite_with_int_double_text
    ) -> None:
        """If the v4 dataset exists but its referenced datasource entry is gone,
        ``get_dataset`` must raise — silently skipping refinement and writing the
        v5 dict back would freeze base integer fields at ``DOUBLE`` forever
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
            await storage.get_dataset("items", data_source="live")

    async def test_unreachable_datasource_propagates_through_get_model(
        self, sqlite_with_int_double_text, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If introspection fails during first-load refinement, the error
        propagates out of get_dataset — the v4 dataset isn't silently kept at
        coarse types.

        The SQLite branch of ``refine_dict_with_live_schema`` opens its own SA
        engine via ``engine_factory.get_engine``. Patch the factory directly so a
        connect failure propagates identically.
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
        engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=base))

        def _boom(*_args, **_kw):
            raise OperationalError("simulated", None, Exception("connection refused"))  # NOSONAR(S112) — Exception(...) is the cause-of arg for the simulated SQLAlchemy connect error

        # Every engine consumer goes through engine_factory.get_engine;
        # patching it surfaces the same connect failure the legacy
        # type_refinement.sa.create_engine patch used to.
        engine_factory.reset_cache()
        monkeypatch.setattr(engine_factory, "get_engine", _boom)
        try:
            with pytest.raises(StoredDocumentLoadError) as ei:
                await engine.storage.get_dataset("items", data_source="live")
        finally:
            await engine.aclose()
            engine.close()
        assert isinstance(ei.value.__cause__, DatabaseUnavailable)


def _unreachable(**kw):  # used as a wraps target only — the spy.assert_not_called check fires first
    raise AssertionError("Unexpectedly called _live_schema_for_datasource on a v5 model")


# ---------------------------------------------------------------------------
# Load-time SQLite affinity probe on legacy-dict migration.
#
# The v5 refinement is wrong on SQLite because it trusts the declared
# affinity. The fix:
#
# 1. SQLite branch in ``refine_dict_with_live_schema`` runs the per-field
#    value probe for every persisted INT base field AND skips the existing
#    DOUBLE → INT narrowing entirely.
# 2. Already-v7 dicts on SQLite remain untouched on load — the probe only
#    affects the migration-write-back path. Re-ingest is the auto-heal for
#    v7 datasets.
# 3. The narrowing is unchanged on non-SQLite datasources.
# ---------------------------------------------------------------------------


def _create_sqlite_with_int_storage(db_path: str, values: list) -> None:
    """Build a SQLite file with one INTEGER-declared column holding the
    given per-row typed values (preserves storage classes)."""
    with transaction(db_path) as conn:
        conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)"
        )
        for i, v in enumerate(values, start=1):
            conn.execute("INSERT INTO items VALUES (?, ?)", (i, v))


class TestRefineSqliteAffinityProbe:
    """``refine_dict_with_live_schema`` SQLite branch."""

    def _ds_for(self, db_path: str) -> DatasourceConfig:
        return DatasourceConfig(name="live", type="sqlite", database=db_path)

    async def test_sqlite_int_with_real_storage_widens_to_double(
        self, tmp_path: Path
    ) -> None:
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7, 0.9])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {
                    "name": "qty", "expression": "qty",
                    "datatype": "Integer",
                    "format": {"type": "integer"},
                },
            ],
        }
        ds = self._ds_for(db_path)
        changed = await _refine(d, ds)
        assert changed is True
        assert d["fields"][0]["datatype"] == "Float"
        # Auto-default integer format is updated to FLOAT.
        assert d["fields"][0]["format"]["type"] == "float"

    async def test_sqlite_double_narrows_to_int_when_probe_certifies_int(
        self, tmp_path: Path
    ) -> None:
        """On SQLite, the DOUBLE → INT narrowing is probe-verified
        rather than declared-type-driven. With all-integer storage, the
        probe certifies INT and the narrowing fires (the narrowing
        contract is preserved on SQLite when the probe agrees).
        """
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 2, 3])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(db_path)
        await _refine(d, ds)
        assert d["fields"][0]["datatype"] == "Integer"

    async def test_sqlite_double_stays_double_when_probe_says_double(
        self, tmp_path: Path
    ) -> None:
        """A persisted DOUBLE field stays DOUBLE when the probe sees REAL
        values — the DOUBLE → INT narrowing would have flipped it to INT based
        on the declared affinity, but the probe knows better."""
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(db_path)
        await _refine(d, ds)
        assert d["fields"][0]["datatype"] == "Float"

    async def test_sqlite_double_stays_double_when_probe_returns_none(
        self, tmp_path: Path
    ) -> None:
        """A None probe verdict (failure or saturation) is conservative:
        the persisted DOUBLE stays DOUBLE."""
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 2, 3])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Float"},
            ],
        }
        ds = self._ds_for(db_path)
        with patch(
            "slayer.database.sqlite_introspect.probe_sqlite_integer_column",
            return_value=None,
        ):
            await _refine(d, ds)
        assert d["fields"][0]["datatype"] == "Float"

    async def test_postgres_still_narrows_double_to_int(self) -> None:
        """Sanity: the DOUBLE → INT narrowing rule is preserved for non-SQLite
        datasources. (Postgres datasource is unreachable here, but the
        SQLite-only carve-out must not apply.)"""
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Float"},
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
        with pytest.raises(DatabaseUnavailable):
            await _refine(d, ds)

    async def test_sqlite_int_with_pure_int_storage_stays_int(
        self, tmp_path: Path
    ) -> None:
        """All-INTEGER storage → probe returns INT → no widening, type
        preserved."""
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 2, 3])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Integer"},
            ],
        }
        ds = self._ds_for(db_path)
        await _refine(d, ds)
        assert d["fields"][0]["datatype"] == "Integer"

    async def test_custom_format_preserved_on_widening(self, tmp_path: Path) -> None:
        """A user-set custom format on a widening field is left untouched."""
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {
                    "name": "qty", "expression": "qty",
                    "datatype": "Integer",
                    "format": {
                        "type": "currency",
                        "symbol": "€",
                        "precision": 3,
                    },
                },
            ],
        }
        ds = self._ds_for(db_path)
        await _refine(d, ds)
        assert d["fields"][0]["datatype"] == "Float"
        # Custom format preserved verbatim.
        assert d["fields"][0]["format"]["type"] == "currency"
        assert d["fields"][0]["format"]["symbol"] == "€"
        assert d["fields"][0]["format"]["precision"] == 3

    async def test_sqlite_int_with_text_storage_widens_to_text(
        self, tmp_path: Path
    ) -> None:
        """Persisted INT field whose live storage is non-coercible TEXT
        widens to TEXT and the auto-default integer format is cleared."""
        db_path = str(tmp_path / "live.db")
        _create_sqlite_with_int_storage(db_path, [1, "abc", "xyz"])

        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {
                    "name": "qty", "expression": "qty",
                    "datatype": "Integer",
                    "format": {"type": "integer"},
                },
            ],
        }
        ds = self._ds_for(db_path)
        changed = await _refine(d, ds)
        assert changed is True
        assert d["fields"][0]["datatype"] == "String"
        # Auto-default integer format must be cleared on TEXT widening.
        assert d["fields"][0].get("format") in (None, {})


class TestHasRefineableColumnsSqliteIntBranch:
    """SQLite-INT widening is gated by ``has_sqlite_widenable_columns``
    (best-effort, no DS hard-fail). DOUBLE narrowing stays gated by
    ``has_refineable_columns`` (DS required). The split was added after Codex
    caught a regression where the broadened predicate made non-SQLite legacy
    INT-only dicts hard-fail on missing DS — before the probe they didn't."""

    def test_sqlite_int_base_column_is_widenable_not_refineable(self) -> None:
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "qty", "expression": "qty", "datatype": "Integer"},
            ],
        }
        # INT base fields are *widenable* (best-effort), not *refineable*
        # (mandatory DS); this asymmetry keeps non-SQLite legacy INT-only
        # dicts loadable when the datasource entry is gone.
        assert has_sqlite_widenable_fields(d) is True
        assert has_refineable_fields(d) is False

    def test_double_base_column_is_refineable_not_widenable(self) -> None:
        d = {
            "name": "items",
            "sql_table": "items",
            "data_source": "live",
            "fields": [
                {"name": "amount", "expression": "amount", "datatype": "Float"},
            ],
        }
        assert has_refineable_fields(d) is True
        assert has_sqlite_widenable_fields(d) is False

    async def test_sqlite_legacy_dict_with_int_loads_when_datasource_missing(
        self, tmp_path: Path, caplog
    ) -> None:
        """When a legacy dict has ONLY INT base fields (no
        DOUBLE), a missing datasource is NOT a hard fail. The persisted
        INT is a safe default — re-ingest will heal any mis-typed fields
        once the DS is back. A WARNING is logged so the skip is visible.

        This is the corrected behavior after Codex caught a regression
        where the original predicate broadening made non-SQLite legacy
        INT-only dicts hard-fail on missing DS (before the probe they didn't).
        """
        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        # Use a recent legacy version that genuinely carries the modern
        # "INT" type token. (v4 dicts only know the legacy
        # "number" / "string" tokens — pinning "version": 4 with "type":
        # "INT" wouldn't represent any real on-disk dataset shape.)
        legacy_version = _NEWEST_REFINED_VERSION
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
        # log warning), NOT hard-fail like a DOUBLE-base-field dict would.
        storage = YAMLStorage(base_dir=base)
        with caplog.at_level(logging.WARNING, logger="slayer.storage.base"):
            loaded = await storage.get_dataset("items", data_source="live")
        assert loaded is not None
        col = next(c for c in loaded.fields if c.name == "qty")
        # Persisted INT preserved as the safe default.
        assert col.datatype is DataType.INTEGER
        # Warning emitted naming the dataset + datasource.
        msgs = [r.getMessage() for r in caplog.records]
        assert any("items" in m and "live" in m for m in msgs), msgs

    async def test_legacy_dict_with_double_still_raises_when_datasource_missing(
        self, tmp_path: Path
    ) -> None:
        """The hard-fail contract is preserved: a legacy dict with
        ANY DOUBLE base field still hard-fails when the datasource is
        missing (live introspection is required to narrow safely)."""
        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        legacy_version = _NEWEST_REFINED_VERSION
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": legacy_version,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [
                        # DOUBLE base field → narrowing required.
                        {"name": "amount", "sql": "amount", "type": "DOUBLE"},
                    ],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        with pytest.raises(ValueError, match="datasource 'live' is unavailable"):
            await storage.get_dataset("items", data_source="live")

    async def test_post_refinement_schema_migrates_without_datasource(
        self, tmp_path: Path
    ) -> None:
        """A dict-only bump never needs the live datasource."""
        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": _NEWEST_REFINED_VERSION + 1,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [{"name": "amount", "sql": "amount", "type": "DOUBLE"}],
                },
                f,
            )
        loaded = await YAMLStorage(base_dir=base).get_dataset("items", data_source="live")
        assert loaded is not None
        assert loaded.version == mig.CURRENT_VERSIONS["SlayerModel"]
        assert loaded.fields[0].datatype is DataType.FLOAT


class TestV7SqliteModelNotAutoRepairedOnLoad:
    """Non-goal: already-current-version SQLite datasets with the
    wrong INT type are NOT auto-repaired on load. Re-ingest is the
    auto-heal path. Loading must not trigger the probe."""

    async def test_current_version_sqlite_model_untouched_on_load(
        self, tmp_path: Path
    ) -> None:
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
                    "fields": [
                        {
                            "name": "id", "expression": "id",
                            "datatype": "Integer", "primary_key": True,
                        },
                        # WRONG persisted type: live storage is REAL but
                        # persisted INT. Load must NOT widen it.
                        {"name": "qty", "expression": "qty", "datatype": "Integer"},
                    ],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)

        # The probe must NOT be called on a current-version dict load.
        with patch(
            "slayer.database.sqlite_introspect.probe_sqlite_integer_column",
            side_effect=AssertionError(
                "probe must not run on already-current-version dict load"
            ),
        ):
            loaded = await storage.get_dataset("items", data_source="live")
        # Type stays INT because the probe never ran.
        col = next(c for c in loaded.fields if c.name == "qty")
        assert col.datatype is DataType.INTEGER


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
        args = _build_args(
            command="storage",
            subcommand="migrate-types",
            storage=storage_with_v4_model["base"],
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
        assert "Integer" in out

    async def test_apply_writes_refinements(self, storage_with_v4_model) -> None:  # NOSONAR(S7503) — pytest-asyncio test body; sync run via _run_storage
        args = _build_args(
            command="storage",
            subcommand="migrate-types",
            storage=storage_with_v4_model["base"],
            dry_run=False,
            data_source=None,
        )
        _run_storage(args)
        with open(storage_with_v4_model["model_path"]) as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            raw = yaml.safe_load(f)
        assert raw["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        types_by_name = {c["name"]: c["datatype"] for c in raw["fields"]}
        assert types_by_name["id"] == "Integer"
        assert types_by_name["qty"] == "Integer"

    async def test_missing_datasource_raises_for_refineable_model(self, tmp_path) -> None:  # NOSONAR(S7503) — pytest-asyncio test body; sync run via _run_storage
        """Mirror of the ABC's raise: the CLI must fail loudly rather than
        silently report 'nothing to refine' for a v4 dataset whose datasource
        entry has been removed."""
        base = str(tmp_path)
        # Lay down a v4 YAML dataset with a refineable DOUBLE base field but
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
        engine = SlayerQueryEngine(storage=storage)
        with pytest.raises(ValueError, match="datasource 'live' is unavailable"):
            await _refine_one_dataset_for_cli(
                inner=storage, engine=engine, ds_name="live", dataset_name="items", dry_run=True,
            )

    async def test_missing_datasource_silent_for_text_only_model(self, tmp_path) -> None:  # NOSONAR(S7503) — pytest-asyncio test body; sync run via _run_storage
        """Datasets with no refineable DOUBLE base fields (text-only here) must
        load through the CLI without requiring a live datasource entry."""
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
        engine = SlayerQueryEngine(storage=storage)
        # No raise — returns False because nothing needed refinement.
        result = await _refine_one_dataset_for_cli(
            inner=storage, engine=engine, ds_name="live", dataset_name="events", dry_run=True,
        )
        assert result is False

    def test_sqlite_int_only_legacy_dict_widens_via_cli(
        self, tmp_path, capsys,
    ) -> None:
        """``slayer storage migrate-types`` must also surface
        SQLite INT-only legacy datasets for widening (not just DOUBLE-only
        ones — the post-split CLI gate would otherwise skip them). The
        CLI prints a before/after diff for the widened field."""
        base = str(tmp_path)
        # Real SQLite DB with REAL storage for the INT-declared column.
        db_path = os.path.join(base, "live.db")
        _create_sqlite_with_int_storage(db_path, [1, 0.5, 0.7, 0.9])
        datasources_dir = os.path.join(base, "datasources")
        os.makedirs(datasources_dir, exist_ok=True)
        with open(os.path.join(datasources_dir, "live.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {"name": "live", "type": "sqlite", "database": db_path, "version": 1},
                f,
            )
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        legacy_version = _NEWEST_REFINED_VERSION
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": legacy_version,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [{"name": "qty", "sql": "qty", "type": "INT"}],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        engine = SlayerQueryEngine(storage=storage)
        result = engine.run_sync(_refine_one_dataset_for_cli(
            inner=storage, engine=engine, ds_name="live", dataset_name="items", dry_run=True,
        ))
        engine.close()
        assert result is True
        out = capsys.readouterr().out
        assert "qty" in out
        assert "Integer" in out
        assert "Float" in out

    def test_missing_datasource_skips_silently_for_sqlite_int_only(
        self, tmp_path, capsys,
    ) -> None:
        """Best-effort: an INT-only SQLite legacy dict with no
        registered datasource must NOT raise — the CLI logs a skip and
        returns False (re-run after restoring the datasource). The DOUBLE
        contract is independent and tested separately."""
        base = str(tmp_path)
        models_dir = os.path.join(base, "models", "live")
        os.makedirs(models_dir, exist_ok=True)
        legacy_version = _NEWEST_REFINED_VERSION
        with open(os.path.join(models_dir, "items.yaml"), "w") as f:  # NOSONAR(S7493) — test fixture: sync I/O is fine
            yaml.dump(
                {
                    "version": legacy_version,
                    "name": "items",
                    "sql_table": "items",
                    "data_source": "live",
                    "columns": [{"name": "qty", "sql": "qty", "type": "INT"}],
                },
                f,
            )
        storage = YAMLStorage(base_dir=base)
        engine = SlayerQueryEngine(storage=storage)
        result = engine.run_sync(_refine_one_dataset_for_cli(
            inner=storage, engine=engine, ds_name="live", dataset_name="items", dry_run=True,
        ))
        engine.close()
        assert result is False
        # Skip message printed on stderr.
        err = capsys.readouterr().err
        assert "datasource 'live' unavailable" in err
        assert "items" in err


def _build_args(**kw):
    return SimpleNamespace(**kw)
