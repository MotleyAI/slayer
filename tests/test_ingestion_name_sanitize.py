"""One bad table name must not abort the whole ingest.

dlt flattens nested JSON into ``__``-named child tables, which model names
reserve for join-path aliases. The ``ValidationError`` was raised inside the
table loop, which runs wholly before the per-table isolation downstream — so
the run died before creating a single model.

Fix: sanitize ``__`` into the model name (``sql_table`` keeps the real name),
plus a ``try/except`` backstop for everything else.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.models import DatasourceConfig, SlayerModel, sanitize_model_name
from slayer.engine.ingestion import (
    IngestionScanReport,
    SkippedTable,
    ingest_datasource,
    ingest_datasource_report,
)


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


def _skipped_names(report: IngestionScanReport) -> set[str]:
    return {s.table_name for s in report.skipped}


# ---------------------------------------------------------------------------
# the sanitizer itself
# ---------------------------------------------------------------------------


class TestSanitizer:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("reports__patient__drug", "reports_patient_drug"),
            ("a__b", "a_b"),
            # A naive str.replace("__", "_") is non-overlapping left-to-right
            # and leaves "a__b" here — which still fails validation.
            ("a___b", "a_b"),
            ("a____b", "a_b"),
            ("a_____b", "a_b"),
            ("a_b", "a_b"),
            ("plain", "plain"),
            ("__leading", "_leading"),
            ("trailing__", "trailing_"),
            ("__", "_"),
        ],
    )
    def test_collapses_underscore_runs(self, raw: str, expected: str) -> None:
        assert sanitize_model_name(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        ["a__b", "a___b", "reports__patient__drug", "__", "a__b__c__d"],
    )
    def test_result_never_contains_dunder(self, raw: str) -> None:
        assert "__" not in sanitize_model_name(raw)

    @pytest.mark.parametrize("raw", ["a__b", "a___b", "plain", "__x"])
    def test_is_idempotent(self, raw: str) -> None:
        once = sanitize_model_name(raw)
        assert sanitize_model_name(once) == once

    def test_result_is_a_valid_model_name(self) -> None:
        """The whole point: the output must pass the validator that rejected
        the input."""
        name = sanitize_model_name("reports__patient__drug")
        model = SlayerModel(name=name, sql_table="reports__patient__drug",
                            data_source="ds")
        assert model.name == "reports_patient_drug"

    def test_does_not_touch_other_reserved_characters(self) -> None:
        """Only ``__`` is sanitized. Dots stay put — a dotted table name makes
        ``sql_table`` itself ambiguous with schema qualification, so those go
        down the skip path instead."""
        assert sanitize_model_name("weird.table") == "weird.table"
        assert sanitize_model_name("odd:name") == "odd:name"


# ---------------------------------------------------------------------------
# dunder tables are modelled, and no longer poison the run
# ---------------------------------------------------------------------------


class TestDunderTableIngestion:
    def test_dunder_table_is_modelled_under_a_sanitized_name(
        self, workspace: Path
    ) -> None:
        """sql_table keeps the real object name so queries still
        resolve."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE reports__patient__drug (
                id INTEGER PRIMARY KEY,
                drug_name TEXT
            );
            """,
        )
        models = ingest_datasource(datasource=ds)
        model = next(m for m in models if m.name == "reports_patient_drug")
        assert model.sql_table == "reports__patient__drug"

    def test_one_bad_name_no_longer_kills_the_run(self, workspace: Path) -> None:
        """THE headline regression. Before the fix this raised a
        ValidationError and produced zero models."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL);
            CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT);
            CREATE TABLE reports__patient__drug (id INTEGER PRIMARY KEY, x TEXT);
            """,
        )
        models = ingest_datasource(datasource=ds)
        names = {m.name for m in models}
        assert "orders" in names
        assert "customers" in names
        assert "reports_patient_drug" in names

    def test_dunder_view_is_also_sanitized(self, workspace: Path) -> None:
        """dlt child tables are sometimes exposed as views."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE base (id INTEGER PRIMARY KEY, x TEXT);
            CREATE VIEW stg__nested__thing AS SELECT id, x FROM base;
            """,
        )
        models = ingest_datasource(datasource=ds)
        model = next(m for m in models if m.name == "stg_nested_thing")
        assert model.sql_table == "stg__nested__thing"

    def test_schema_qualified_dunder_table_keeps_qualified_sql_table(
        self, workspace: Path
    ) -> None:
        """The schema prefix must survive sanitization untouched — only the
        model name changes."""
        ds = _sqlite_ds(
            workspace,
            "CREATE TABLE reports__patient (id INTEGER PRIMARY KEY, x TEXT);",
        )
        report = ingest_datasource_report(datasource=ds, schema="main")
        model = next(m for m in report.models if m.name == "reports_patient")
        assert model.sql_table == "main.reports__patient"


# ---------------------------------------------------------------------------
# collision policy
# ---------------------------------------------------------------------------


class TestCollisionPolicy:
    _SCRIPT = """
        CREATE TABLE a__b (id INTEGER PRIMARY KEY, viaview TEXT);
        CREATE TABLE a_b (id INTEGER PRIMARY KEY, real_col TEXT);
    """

    def test_real_table_wins_and_dunder_table_is_skipped(
        self, workspace: Path
    ) -> None:
        """an unsanitized name always beats a sanitized one, so the
        model named ``a_b`` is the one actually called ``a_b`` in the DB."""
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)

        model = next(m for m in report.models if m.name == "a_b")
        assert model.sql_table == "a_b"
        assert "a__b" in _skipped_names(report)

    def test_collision_skip_records_a_reason(self, workspace: Path) -> None:
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)
        entry = next(s for s in report.skipped if s.table_name == "a__b")
        assert isinstance(entry, SkippedTable)
        assert "collision" in entry.reason.lower()
        assert "a_b" in entry.reason

    def test_collision_outcome_is_order_independent(self, workspace: Path) -> None:
        """determinism. Reversing the scan order must not flip which
        object gets the name; otherwise re-ingest churns models."""
        from slayer.engine import ingestion as ingestion_module

        ds = _sqlite_ds(workspace, self._SCRIPT)
        real = ingestion_module.list_ingestable_objects

        def _reversed(**kwargs):
            return list(reversed(real(**kwargs)))

        forward = ingest_datasource_report(datasource=ds)
        ingestion_module.list_ingestable_objects = _reversed
        try:
            backward = ingest_datasource_report(datasource=ds)
        finally:
            ingestion_module.list_ingestable_objects = real

        def _mapping(report):
            return {m.name: m.sql_table for m in report.models}

        assert _mapping(forward) == _mapping(backward)
        assert _skipped_names(forward) == _skipped_names(backward)

    def test_no_numeric_suffix_disambiguation(self, workspace: Path) -> None:
        """Suffixes were rejected: they are unstable across runs as the table
        set changes, orphaning models and churning drift."""
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)
        assert not any(m.name.endswith(("_2", "_3")) for m in report.models)

    def test_two_dunder_names_collapsing_to_one_skips_the_second(
        self, workspace: Path
    ) -> None:
        """``a__b`` and ``a___b`` both sanitize to ``a_b`` with no real ``a_b``
        present — the first keeps it, the second is skipped."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE a__b (id INTEGER PRIMARY KEY, x TEXT);
            CREATE TABLE a___b (id INTEGER PRIMARY KEY, y TEXT);
            """,
        )
        report = ingest_datasource_report(datasource=ds)
        assert len([m for m in report.models if m.name == "a_b"]) == 1
        assert len(report.skipped) == 1


# ---------------------------------------------------------------------------
# the try/except backstop and the pre-loop FK guard
# ---------------------------------------------------------------------------


class TestSkipBackstop:
    def test_unmodellable_object_is_skipped_not_fatal(
        self, workspace: Path, monkeypatch
    ) -> None:
        """anything that fails per-object construction is skipped
        with the rest of the run intact."""
        from slayer.engine import ingestion as ingestion_module

        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE good_one (id INTEGER PRIMARY KEY, x TEXT);
            CREATE TABLE bad_one (id INTEGER PRIMARY KEY, y TEXT);
            CREATE TABLE good_two (id INTEGER PRIMARY KEY, z TEXT);
            """,
        )
        real = ingestion_module._columns_to_model

        def _explode(*args, **kwargs):
            if kwargs.get("sql_table") == "bad_one" or "bad_one" in repr(args):
                raise ValueError("synthetic per-object failure")
            return real(*args, **kwargs)

        monkeypatch.setattr(ingestion_module, "_columns_to_model", _explode)

        report = ingest_datasource_report(datasource=ds)
        names = {m.name for m in report.models}
        assert "good_one" in names
        assert "good_two" in names
        assert "bad_one" in _skipped_names(report)

    def test_fk_introspection_failure_does_not_abort_the_run(
        self, workspace: Path, monkeypatch
    ) -> None:
        """``_get_fk_relationships`` and the FK-collection loop both
        run BEFORE the per-object try/except, so an unguarded raise there
        would kill the run regardless of the backstop. Views legitimately have
        no FKs and some dialects raise rather than returning []."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL);
            CREATE VIEW v_orders AS SELECT id, amount FROM orders;
            """,
        )

        real_inspect = sa.inspect

        def _patched_inspect(target):
            insp = real_inspect(target)
            original = insp.get_foreign_keys

            def _maybe_raise(table_name, schema=None, **kwargs):
                if table_name == "v_orders":
                    raise sa.exc.NoSuchTableError("views have no FKs here")
                return original(table_name, schema=schema, **kwargs)

            insp.get_foreign_keys = _maybe_raise
            return insp

        monkeypatch.setattr(
            "slayer.engine.ingestion.sa.inspect", _patched_inspect
        )

        report = ingest_datasource_report(datasource=ds)
        names = {m.name for m in report.models}
        assert "orders" in names
        assert "v_orders" in names


# ---------------------------------------------------------------------------
# the wrapper contract for existing callers
# ---------------------------------------------------------------------------


class TestWrapperContract:
    def test_ingest_datasource_still_returns_a_list_of_models(
        self, workspace: Path
    ) -> None:
        """Three call sites still expect a plain list: demo/jaffle_shop.py,
        cli.py datasources-create, and cli.py create-demo."""
        ds = _sqlite_ds(
            workspace, "CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT);"
        )
        models = ingest_datasource(datasource=ds)
        assert isinstance(models, list)
        assert all(isinstance(m, SlayerModel) for m in models)

    def test_report_carries_models_skipped_and_objects(
        self, workspace: Path
    ) -> None:
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT);
            CREATE VIEW v_orders AS SELECT id, x FROM orders;
            """,
        )
        report = ingest_datasource_report(datasource=ds)
        assert isinstance(report, IngestionScanReport)
        assert {m.name for m in report.models} == {"orders", "v_orders"}
        assert report.skipped == []
        assert {o.name for o in report.objects} == {"orders", "v_orders"}

    def test_empty_schema_reports_no_objects(self, workspace: Path) -> None:
        """Distinguishing 'schema was empty' from 'everything was skipped' is
        what drives the CLI's exit-1-with-hint path."""
        db_path = str(workspace / "empty.db")
        sqlite3.connect(db_path).close()
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        report = ingest_datasource_report(datasource=ds)
        assert report.models == []
        assert report.skipped == []
        assert report.objects == []
