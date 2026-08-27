"""DEV-1743 — ingestion preserves ``__`` in object names (WP6 / D3).

Fresh ingests keep faithful names (``stg_jaffle_shop__orders``), instead of
sanitising them to ``stg_jaffle_shop_orders`` as they must today because of the
ban this issue lifts. ``sanitize_model_name`` is KEPT (it becomes the re-ingest
fallback-matching spelling), so its unit behavior is an invariant lock.

Fail-first: the fresh-ingest assertions expect the preserved ``__`` name, which
the current sanitiser strips.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
    sanitize_model_name,
)
from slayer.engine.ingestion import (
    ingest_datasource,
    ingest_datasource_idempotent,
    ingest_datasource_report,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _sqlite_ds(workspace: Path, script: str) -> DatasourceConfig:
    db_path = str(workspace / "live.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(script)
    conn.commit()
    conn.close()
    return DatasourceConfig(name="ds", type="sqlite", database=db_path)


def _skipped_names(report) -> set[str]:
    return {s.table_name for s in report.skipped}


# --------------------------------------------------------------------------- #
# INVARIANT: sanitize_model_name is unchanged (kept for fallback matching).
# --------------------------------------------------------------------------- #
class TestSanitizerUnchanged:
    @pytest.mark.parametrize(("raw", "expected"), [
        ("reports__patient__drug", "reports_patient_drug"),
        ("a__b", "a_b"),
        ("a___b", "a_b"),
        ("plain", "plain"),
    ])
    def test_still_collapses_runs(self, raw: str, expected: str) -> None:
        assert sanitize_model_name(raw) == expected


# --------------------------------------------------------------------------- #
# FAIL-FIRST: fresh ingest PRESERVES the __ name.
# --------------------------------------------------------------------------- #
class TestFreshIngestPreservesDunder:
    def test_dunder_table_modelled_under_faithful_name(self, workspace: Path) -> None:
        ds = _sqlite_ds(
            workspace,
            "CREATE TABLE stg_jaffle_shop__orders (id INTEGER PRIMARY KEY, x TEXT);",
        )
        models = ingest_datasource(datasource=ds)
        names = {m.name for m in models}
        assert "stg_jaffle_shop__orders" in names, (
            f"fresh ingest must preserve the __ name; got {names}"
        )
        model = next(m for m in models if m.name == "stg_jaffle_shop__orders")
        assert model.sql_table == "stg_jaffle_shop__orders"

    def test_dunder_view_preserved(self, workspace: Path) -> None:
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE base (id INTEGER PRIMARY KEY, x TEXT);
            CREATE VIEW stg__nested__thing AS SELECT id, x FROM base;
            """,
        )
        models = ingest_datasource(datasource=ds)
        assert "stg__nested__thing" in {m.name for m in models}


# --------------------------------------------------------------------------- #
# FAIL-FIRST: a__b and a_b are now DISTINCT models — no collision, no skip.
# (The old collision policy dissolves once __ is a legal name.)
# --------------------------------------------------------------------------- #
class TestNoSpuriousCollision:
    _SCRIPT = """
        CREATE TABLE a__b (id INTEGER PRIMARY KEY, viaview TEXT);
        CREATE TABLE a_b (id INTEGER PRIMARY KEY, real_col TEXT);
    """

    def test_both_names_are_distinct_models(self, workspace: Path) -> None:
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)
        names = {m.name for m in report.models}
        assert "a__b" in names
        assert "a_b" in names
        # Neither is skipped — they no longer collapse onto one model name.
        assert "a__b" not in _skipped_names(report)


# --------------------------------------------------------------------------- #
# FAIL-FIRST: an FK to a __-named table targets the preserved model name.
# --------------------------------------------------------------------------- #
class TestJoinTargetsPreservedName:
    def test_fk_targets_the_dunder_model_name(self, workspace: Path) -> None:
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE reports__patient__drug (id INTEGER PRIMARY KEY);
            CREATE TABLE visits (
                id INTEGER PRIMARY KEY,
                report_id INTEGER REFERENCES reports__patient__drug(id)
            );
            """,
        )
        models = {m.name: m for m in ingest_datasource(datasource=ds)}
        assert "reports__patient__drug" in models
        visits = models["visits"]
        assert [j.target_model for j in visits.joins] == ["reports__patient__drug"]
        assert visits.joins[0].target_model in models


# --------------------------------------------------------------------------- #
# Re-ingest matching (D3 / [C2]): exact-then-sanitized, stored-name-wins, with
# the "same live object" guard.
# --------------------------------------------------------------------------- #
def _exec(db_path: str, script: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(script)
    conn.commit()
    conn.close()


class TestReingestMatching:
    @pytest.mark.asyncio
    async def test_new_dunder_object_stays_distinct(self, workspace: Path) -> None:
        """FAIL-FIRST: a genuinely new ``a__b`` object is a distinct model, not
        merged into an existing ``a_b`` (they are different live tables)."""
        db_path = str(workspace / "live.db")
        _exec(db_path, "CREATE TABLE a_b (id INTEGER PRIMARY KEY, x TEXT);")
        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # A new, genuinely different object appears.
        _exec(db_path, "CREATE TABLE a__b (id INTEGER PRIMARY KEY, y TEXT);")
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        a_b = await storage.get_model("a_b", data_source="ds")
        a__b = await storage.get_model("a__b", data_source="ds")
        assert a_b is not None
        assert a__b is not None, (
            "a new __-named object must become its own model, not collapse "
            "onto the sanitized a_b"
        )

    @pytest.mark.asyncio
    async def test_distinct_live_twin_not_collapsed_by_stored_legacy(
        self, workspace: Path
    ) -> None:
        """FAIL-FIRST (A2): when BOTH ``a__b`` and ``a_b`` are live objects and a
        stored legacy ``a_b`` points at ``a__b``, the re-ingest rename pre-pass
        must NOT adopt ``a__b`` → ``a_b`` — that would collapse two distinct live
        tables onto one model. The sanitized-is-a-live-object guard prevents it."""
        db_path = str(workspace / "live.db")
        _exec(
            db_path,
            """
            CREATE TABLE a__b (id INTEGER PRIMARY KEY, viaview TEXT);
            CREATE TABLE a_b (id INTEGER PRIMARY KEY, real_col TEXT);
            """,
        )
        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        # Seed the old-world legacy model whose sql_table is the OTHER live object.
        await storage.save_model(SlayerModel(
            name="a_b", data_source="ds", sql_table="a__b",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="x", type=DataType.TEXT)],
        ))
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        a__b = await storage.get_model("a__b", data_source="ds")
        a_b = await storage.get_model("a_b", data_source="ds")
        assert a__b is not None, "the live a__b table must keep its own model"
        assert a_b is not None, "the live a_b table must keep its own model"
        # Distinct live objects → distinct models; a__b is not collapsed away.
        assert a__b.sql_table == "a__b"

    @pytest.mark.asyncio
    async def test_stored_sanitized_name_wins_no_duplicate(
        self, workspace: Path
    ) -> None:
        """INVARIANT LOCK: a pre-existing (old-world) stored model ``a_b`` whose
        ``sql_table`` IS the live ``a__b`` object is not duplicated on re-ingest —
        the stored name wins, no churn."""
        db_path = str(workspace / "live.db")
        _exec(db_path, "CREATE TABLE a__b (id INTEGER PRIMARY KEY, x TEXT);")
        storage = YAMLStorage(base_dir=str(workspace / "store"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        # Seed the old-world sanitized model pointing at the real object.
        await storage.save_model(SlayerModel(
            name="a_b", data_source="ds", sql_table="a__b",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="x", type=DataType.TEXT)],
        ))
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert await storage.get_model("a_b", data_source="ds") is not None
        # No duplicate under the faithful spelling.
        assert await storage.get_model("a__b", data_source="ds") is None
