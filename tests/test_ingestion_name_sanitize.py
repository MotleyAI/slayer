"""One bad table name must not abort the whole ingest."""
from __future__ import annotations

import logging
import sqlite3
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.models import DatasourceConfig, SlayerModel, sanitize_model_name
from slayer.sql import engine_factory
from slayer.engine.ingestion import (
    IngestionScanReport,
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
            # naive str.replace("__","_") leaves "a__b" here — still invalid
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
        """Output must pass the validator that rejected the input."""
        name = sanitize_model_name("reports__patient__drug")
        model = SlayerModel(name=name, sql_table="reports__patient__drug",
                            data_source="ds")
        assert model.name == "reports_patient_drug"

    def test_does_not_touch_other_reserved_characters(self) -> None:
        """Only ``__`` is sanitized; dots stay put and go down the skip path."""
        assert sanitize_model_name("weird.table") == "weird.table"
        assert sanitize_model_name("odd:name") == "odd:name"


# ---------------------------------------------------------------------------
# dunder tables are modelled, and no longer poison the run
# ---------------------------------------------------------------------------


class TestDunderTableIngestion:
    def test_dunder_table_is_modelled_under_its_faithful_name(
        self, workspace: Path
    ) -> None:
        """DEV-1743: the ``__`` ban is lifted — the model keeps the faithful
        object name, and ``sql_table`` matches."""
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
        model = next(m for m in models if m.name == "reports__patient__drug")
        assert model.sql_table == "reports__patient__drug"

    def test_one_bad_name_no_longer_kills_the_run(self, workspace: Path) -> None:
        """Headline regression: before the fix this produced zero models."""
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
        assert "reports__patient__drug" in names

    def test_dunder_view_is_preserved(self, workspace: Path) -> None:
        """dlt child tables are sometimes exposed as views; ``__`` is kept."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE base (id INTEGER PRIMARY KEY, x TEXT);
            CREATE VIEW stg__nested__thing AS SELECT id, x FROM base;
            """,
        )
        models = ingest_datasource(datasource=ds)
        model = next(m for m in models if m.name == "stg__nested__thing")
        assert model.sql_table == "stg__nested__thing"

    def test_schema_qualified_dunder_table_keeps_qualified_sql_table(
        self, workspace: Path
    ) -> None:
        """The schema prefix stays on ``sql_table``; the model keeps the faithful
        ``__`` name."""
        ds = _sqlite_ds(
            workspace,
            "CREATE TABLE reports__patient (id INTEGER PRIMARY KEY, x TEXT);",
        )
        report = ingest_datasource_report(datasource=ds, schema="main")
        model = next(m for m in report.models if m.name == "reports__patient")
        assert model.sql_table == "main.reports__patient"


# ---------------------------------------------------------------------------
# collision policy
# ---------------------------------------------------------------------------


class TestCollisionPolicy:
    """DEV-1743: ``__`` is a legal model-name character now, so ``a__b`` and
    ``a_b`` (and ``a___b``) are DISTINCT models — the old sanitize-collapse
    collision between them no longer exists. A genuine collision is now only two
    objects sharing one raw name across different schemas (covered in
    ``test_ingestion_schema_qualification.py``)."""

    _SCRIPT = """
        CREATE TABLE a__b (id INTEGER PRIMARY KEY, viaview TEXT);
        CREATE TABLE a_b (id INTEGER PRIMARY KEY, real_col TEXT);
    """

    def test_faithful_and_sanitized_names_are_distinct_models(
        self, workspace: Path
    ) -> None:
        """``a__b`` keeps its faithful name; ``a_b`` is its own model; no skip."""
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)

        by_name = {m.name: m for m in report.models}
        assert by_name["a__b"].sql_table == "a__b"
        assert by_name["a_b"].sql_table == "a_b"
        assert "a__b" not in _skipped_names(report)

    def test_no_collision_skip_for_distinct_faithful_names(
        self, workspace: Path
    ) -> None:
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)
        assert report.skipped == []

    def test_outcome_is_order_independent(
        self, workspace: Path, monkeypatch
    ) -> None:
        """Reversing the scan order must not change the (distinct) result."""
        from slayer.engine import ingestion as ingestion_module

        ds = _sqlite_ds(workspace, self._SCRIPT)
        real = ingestion_module.list_ingestable_objects

        def _reversed(**kwargs):
            return list(reversed(real(**kwargs)))

        forward = ingest_datasource_report(datasource=ds)
        monkeypatch.setattr(ingestion_module, "list_ingestable_objects", _reversed)
        backward = ingest_datasource_report(datasource=ds)

        def _mapping(report):
            return {m.name: m.sql_table for m in report.models}

        assert _mapping(forward) == _mapping(backward)
        assert _skipped_names(forward) == _skipped_names(backward)

    def test_no_numeric_suffix_disambiguation(self, workspace: Path) -> None:
        """Numeric suffixes were rejected: unstable across runs, they churn drift."""
        ds = _sqlite_ds(workspace, self._SCRIPT)
        report = ingest_datasource_report(datasource=ds)
        assert not any(m.name.endswith(("_2", "_3")) for m in report.models)

    _TWO_DUNDER = """
        CREATE TABLE a__b (id INTEGER PRIMARY KEY, x TEXT);
        CREATE TABLE a___b (id INTEGER PRIMARY KEY, y TEXT);
    """

    def test_two_dunder_names_are_distinct_models(
        self, workspace: Path
    ) -> None:
        """``a__b`` and ``a___b`` are different objects → different faithful
        models; nothing collapses, nothing is skipped."""
        ds = _sqlite_ds(workspace, self._TWO_DUNDER)
        report = ingest_datasource_report(datasource=ds)
        names = {m.name for m in report.models}
        assert {"a__b", "a___b"} <= names
        assert report.skipped == []

    def test_two_dunder_names_result_is_order_independent(
        self, workspace: Path, monkeypatch
    ) -> None:
        """Both distinct models appear regardless of scan order."""
        from slayer.engine import ingestion as ingestion_module

        ds = _sqlite_ds(workspace, self._TWO_DUNDER)
        real = ingestion_module.list_ingestable_objects

        def _reversed(**kwargs):
            return list(reversed(real(**kwargs)))

        forward = ingest_datasource_report(datasource=ds)
        monkeypatch.setattr(ingestion_module, "list_ingestable_objects", _reversed)
        backward = ingest_datasource_report(datasource=ds)

        def _mapping(report):
            return {m.name: m.sql_table for m in report.models}

        assert _mapping(forward) == _mapping(backward)
        assert _skipped_names(forward) == _skipped_names(backward) == set()


# ---------------------------------------------------------------------------
# the try/except backstop and the pre-loop FK guard
# ---------------------------------------------------------------------------


class TestSkipBackstop:
    def test_unmodellable_object_is_skipped_not_fatal(
        self, workspace: Path, monkeypatch
    ) -> None:
        """Anything that fails per-object construction is skipped, run intact."""
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
        """FK introspection runs before the per-object backstop, and some dialects raise on views."""
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


class TestEngineDisposal:
    def test_dispose_failure_does_not_mask_the_real_error(
        self, workspace: Path, monkeypatch
    ) -> None:
        """A raising dispose in the finally must not mask the in-flight error."""
        from slayer.engine import ingestion as ingestion_module

        ds = _sqlite_ds(
            workspace, "CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT);"
        )

        class _Boom(Exception):
            pass

        disposed: list[bool] = []

        class _ExplodingEngine:
            def dispose(self):
                disposed.append(True)
                raise RuntimeError("dispose blew up")

        monkeypatch.setattr(
            "slayer.sql.engine_factory.get_engine",
            lambda _cfg: _ExplodingEngine(),
        )
        monkeypatch.setattr(
            ingestion_module.sa,
            "inspect",
            lambda *_a, **_k: (_ for _ in ()).throw(_Boom("the real failure")),
        )

        with pytest.raises(_Boom, match="the real failure"):
            ingest_datasource_report(datasource=ds)
        assert disposed, "dispose must still be attempted"

    def test_dispose_failure_does_not_fail_a_successful_ingest(
        self, workspace: Path, monkeypatch
    ) -> None:
        """With no in-flight error, a raising dispose must not fail the ingest."""
        ds = _sqlite_ds(
            workspace, "CREATE TABLE orders (id INTEGER PRIMARY KEY, x TEXT);"
        )

        real_get_engine = engine_factory.get_engine
        disposed: list[bool] = []
        real_dispose = None

        def _wrap(cfg):
            nonlocal real_dispose
            engine = real_get_engine(cfg)
            real_dispose = engine.dispose

            def _explode():
                disposed.append(True)
                raise RuntimeError("dispose blew up")

            monkeypatch.setattr(engine, "dispose", _explode)
            return engine

        monkeypatch.setattr(engine_factory, "get_engine", _wrap)

        try:
            report = ingest_datasource_report(datasource=ds)
            assert {m.name for m in report.models} == {"orders"}
            assert disposed, "dispose must still be attempted"
        finally:
            # engine_factory caches engines; without this the pool holds the file open
            if real_dispose is not None:
                real_dispose()

    def test_dispose_failure_is_logged_at_warning(
        self, workspace: Path, caplog
    ) -> None:
        """A dispose failure must be visible above DEBUG."""
        from slayer.engine.ingestion import _dispose_quietly

        class _ExplodingEngine:
            def dispose(self):
                raise RuntimeError("dispose blew up")

        with caplog.at_level(logging.WARNING, logger="slayer.engine.ingestion"):
            _dispose_quietly(_ExplodingEngine())

        assert any(
            r.levelno >= logging.WARNING and "dispose failed" in r.getMessage()
            for r in caplog.records
        )


class TestWrapperContract:
    def test_ingest_datasource_still_returns_a_list_of_models(
        self, workspace: Path
    ) -> None:
        """Existing call sites still expect a plain list of models."""
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
        """Empty schema vs everything-skipped drives the CLI's exit-1 hint path."""
        db_path = str(workspace / "empty.db")
        sqlite3.connect(db_path).close()
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        report = ingest_datasource_report(datasource=ds)
        assert report.models == []
        assert report.skipped == []
        assert report.objects == []


# ---------------------------------------------------------------------------
# FK targets follow the sanitized model name
# ---------------------------------------------------------------------------


class TestJoinTargetsUseModelNames:
    """A join must name the persisted MODEL, not the live object.

    DEV-1743: model names keep ``__``, so an FK pointing at
    ``reports__patient__drug`` binds to the faithful ``reports__patient__drug``.
    """

    def test_fk_to_dunder_table_targets_the_faithful_model_name(
        self, workspace: Path
    ) -> None:
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
        # The whole point: the target resolves to a model that exists.
        assert visits.joins[0].target_model in models

    def test_join_is_dropped_when_the_target_has_no_model(
        self, workspace: Path
    ) -> None:
        """An out-of-scope target is not ingested, so a join to it is dropped
        rather than left dangling (DEV-1743: no longer a sanitize-collision, but
        the exclude filter still yields a target with no model)."""
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE a__b (id INTEGER PRIMARY KEY);
            CREATE TABLE refs_it (
                id INTEGER PRIMARY KEY,
                x INTEGER REFERENCES a__b(id)
            );
            """,
        )
        report = ingest_datasource_report(datasource=ds, exclude_tables=["a__b"])
        models = {m.name: m for m in report.models}
        assert "a__b" not in models

        refs_it = models["refs_it"]
        assert refs_it.joins == []
        assert all(j.target_model in models for j in refs_it.joins)


class TestEmptyJoinListIsNotNoJoinList:
    """An empty join list means every join was dropped, not "none generated".

    Conflating the two sent the fallback to introspect the skipped object and
    emit `a__b.label` — a column name the SQL generator reads as a join path.
    (`_columns_to_model` drops dotted names, so no bad model reached storage;
    the cost was pointless introspection against an object with no model.)
    """

    def _fixture(self, workspace: Path):
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE a_b (id INTEGER PRIMARY KEY);
            CREATE TABLE a__b (id INTEGER PRIMARY KEY, label TEXT);
            CREATE TABLE refs_it (
                id INTEGER PRIMARY KEY,
                x INTEGER REFERENCES a__b(id)
            );
            """,
        )
        sa_engine = engine_factory.get_engine(ds.resolve_env_vars())
        return sa_engine, sa.inspect(sa_engine)

    def _introspect(self, workspace: Path, joins):
        from slayer.engine.ingestion import _introspect_query_columns_via_inspector

        sa_engine, inspector = self._fixture(workspace)
        return [
            c.name
            for c in _introspect_query_columns_via_inspector(
                sa_engine=sa_engine,
                inspector=inspector,
                table_name="refs_it",
                ref=None,
                rollup_sql=None,
                referenced_tables={"a__b"},
                fk_columns_by_table={"refs_it": {"x"}},
                joins=joins,
            )
        ]

    def test_empty_joins_introspects_no_referenced_table(
        self, workspace: Path
    ) -> None:
        assert self._introspect(workspace, []) == ["id", "x"]

    def test_none_joins_still_falls_back(self, workspace: Path) -> None:
        """`None` means joins were never generated — the fallback must stay."""
        assert self._introspect(workspace, None) == [
            "id", "x", "a__b.id", "a__b.label",
        ]


class TestSanitizedNamesDoNotLeakIntoColumns:
    """A join target's name must never leak into the source model's COLUMNS as a
    dotted/qualified column — join paths belong in ``joins``, not column names."""

    def test_no_dotted_columns_when_target_is_a_faithful_dunder_model(
        self, workspace: Path
    ) -> None:
        ds = _sqlite_ds(
            workspace,
            """
            CREATE TABLE a__b (id INTEGER PRIMARY KEY, label TEXT);
            CREATE TABLE refs_it (
                id INTEGER PRIMARY KEY,
                x INTEGER REFERENCES a__b(id)
            );
            """,
        )
        models = {m.name: m for m in ingest_datasource(datasource=ds)}
        refs_it = models["refs_it"]
        # DEV-1743: a__b is a valid model now, so the FK becomes a real join
        # (not dropped) targeting the faithful name.
        assert [j.target_model for j in refs_it.joins] == ["a__b"]
        # No qualified column names leaked in from the join target.
        assert all("." not in c.name for c in refs_it.columns)
