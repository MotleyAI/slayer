"""Recognised ELT/migration housekeeping tables ingest hidden, not skipped — modelled and queryable but absent from every listing surface, with reporting that reflects post-merge state rather than the raw scan verdict."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import (
    InternalTable,
    SkippedTable,
    _print_ingest_drift_and_errors,
    ingest_datasource,
    ingest_datasource_idempotent,
    ingest_datasource_report,
)
from slayer.engine.internal_tables import internal_table_rule
from slayer.engine.schema_drift import (
    IdempotentIngestResult,
    ModelAddition,
    validate_datasource,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _ds(workspace: Path, script: str, name: str = "live.db") -> tuple[str, DatasourceConfig]:
    db_path = str(workspace / name)
    conn = sqlite3.connect(db_path)
    conn.executescript(script)
    conn.commit()
    conn.close()
    return db_path, DatasourceConfig(name="ds", type="sqlite", database=db_path)


# A real table plus the dlt trio and an Alembic bookkeeping table.
_MIXED = """
    CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT);
    INSERT INTO orders VALUES (1, 10.0, 'ok');
    CREATE TABLE _dlt_loads (load_id TEXT, schema_name TEXT, status INTEGER);
    INSERT INTO _dlt_loads VALUES ('1721', 'openfda', 0);
    CREATE TABLE _dlt_pipeline_state (version INTEGER, state TEXT);
    CREATE TABLE _dlt_version (version INTEGER, schema TEXT);
    CREATE TABLE alembic_version (version_num TEXT);
"""

_INTERNAL_NAMES = {"_dlt_loads", "_dlt_pipeline_state", "_dlt_version", "alembic_version"}


async def _storage_with(workspace: Path, ds: DatasourceConfig) -> YAMLStorage:
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    await storage.save_datasource(ds)
    return storage


# ---------------------------------------------------------------------------
# The matcher, in isolation
# ---------------------------------------------------------------------------


class TestMatcherPrefixes:
    """Both surviving prefixes are vendor-reserved in every warehouse, so a prefix match is safe and needs no dialect."""

    @pytest.mark.parametrize(
        "name",
        ["_dlt_loads", "_dlt_pipeline_state", "_dlt_version", "_dlt_some_future_table"],
    )
    def test_dlt_prefix(self, name: str) -> None:
        assert internal_table_rule(name) == "dlt"

    @pytest.mark.parametrize(
        "name",
        ["_airbyte_destination_state", "_airbyte_raw_users", "_airbyte_raw_orders"],
    )
    def test_airbyte_prefix_covers_state_and_raw(self, name: str) -> None:
        assert internal_table_rule(name) == "airbyte"


class TestMatcherExactNames:
    @pytest.mark.parametrize(
        "name,tool",
        [
            ("_fivetran_audit", "fivetran"),
            ("_fivetran_audit_warning", "fivetran"),
            ("flyway_schema_history", "flyway"),
            ("schema_version", "flyway"),
            ("databasechangelog", "liquibase"),
            ("databasechangeloglock", "liquibase"),
            ("alembic_version", "alembic"),
            ("django_migrations", "django"),
            ("schema_migrations", "rails"),
            ("ar_internal_metadata", "rails"),
            ("sequelizemeta", "sequelize"),
            ("pgmigrations", "node-pg-migrate"),
            ("__efmigrationshistory", "entity-framework"),
            ("__migrationhistory", "entity-framework"),
            ("knex_migrations", "knex"),
            ("knex_migrations_lock", "knex"),
        ],
    )
    def test_exact_name_maps_to_tool(self, name: str, tool: str) -> None:
        assert internal_table_rule(name) == tool


class TestMatcherCaseInsensitivity:
    """Liquibase upper-cases and EF Core/Sequelize camel-case, so the matcher must be case-insensitive."""

    @pytest.mark.parametrize(
        "name,tool",
        [
            ("DATABASECHANGELOG", "liquibase"),
            ("DatabaseChangeLog", "liquibase"),
            ("DATABASECHANGELOGLOCK", "liquibase"),
            ("__EFMigrationsHistory", "entity-framework"),
            ("__MigrationHistory", "entity-framework"),
            ("SequelizeMeta", "sequelize"),
            ("Alembic_Version", "alembic"),
            ("_DLT_LOADS", "dlt"),
            ("_AirByte_Raw_Users", "airbyte"),
        ],
    )
    def test_case_variants_match(self, name: str, tool: str) -> None:
        assert internal_table_rule(name) == tool


class TestMatcherNegatives:
    @pytest.mark.parametrize(
        "name",
        [
            "orders",
            "customers",
            "dlt_loads",              # no leading underscore — a real table
            "airbyte_raw_x",          # ditto
            "my_schema_migrations",   # exact rules must not match as a suffix
            "schema_migrations_v2",   # nor as a prefix
            "spatial_ref_sys",        # PostGIS deliberately excluded
            "geometry_columns",
            "geography_columns",
            "raster_columns",
            "pg_stat_statements",     # not an engine-reserved namespace
        ],
    )
    def test_real_tables_do_not_match(self, name: str) -> None:
        assert internal_table_rule(name) is None

    @pytest.mark.parametrize("name", ["_sdc_batched_at", "_sdc_received_at"])
    def test_singer_sdc_is_column_level_not_a_table_rule(self, name: str) -> None:
        """Singer's `_sdc_` surface is columns on real tables, so there is deliberately no table rule for it."""
        assert internal_table_rule(name) is None

    @pytest.mark.parametrize("name", ["_fivetran_synced", "_fivetran_deleted"])
    def test_fivetran_column_names_are_not_table_rules(self, name: str) -> None:
        """Only Fivetran's two audit TABLES are rules; there is no `_fivetran_` prefix rule since everything else is a column."""
        assert internal_table_rule(name) is None

    def test_empty_name_does_not_match(self) -> None:
        assert internal_table_rule("") is None


class TestNoSqlitePrefixRule:
    """There is deliberately no `sqlite_` prefix rule: SQLite's own internals never reach the scan, and on any other engine `sqlite_`-named tables are ordinary data."""

    @pytest.mark.parametrize(
        "name",
        ["sqlite_sequence", "sqlite_stat1", "sqlite_stat4", "sqlite_backup"],
    )
    def test_sqlite_prefixed_names_do_not_match(self, name: str) -> None:
        assert internal_table_rule(name) is None

    def test_sqlite_internals_never_reach_the_scan(self, workspace: Path) -> None:
        """AUTOINCREMENT's real `sqlite_sequence` table is in `sqlite_master` yet never becomes a candidate."""
        db_path, ds = _ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL);
            INSERT INTO orders (amount) VALUES (1.0);
            """,
        )
        conn = sqlite3.connect(db_path)
        live = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
        conn.close()
        assert "sqlite_sequence" in live  # the engine really did create it

        report = ingest_datasource_report(datasource=ds)
        assert {o.name for o in report.objects} == {"orders"}
        assert {m.name for m in report.models} == {"orders"}
        assert report.hidden_internals == []
        assert report.skipped == []

    def test_a_user_table_named_sqlite_something_is_impossible_on_sqlite(
        self, workspace: Path
    ) -> None:
        """On SQLite nobody can create a `sqlite_`-named table, so the rule would have no legitimate target there."""
        with pytest.raises(sqlite3.OperationalError, match="reserved for internal use"):
            _ds(workspace, "CREATE TABLE sqlite_backup (id INTEGER PRIMARY KEY);")


# ---------------------------------------------------------------------------
# Classification during the scan
# ---------------------------------------------------------------------------


class TestScanClassification:
    def test_internals_are_hidden_and_real_tables_are_not(
        self, workspace: Path
    ) -> None:
        _, ds = _ds(workspace, _MIXED)
        by_name = {m.name: m for m in ingest_datasource(datasource=ds)}

        # Nothing is omitted — hidden, not skipped.
        assert set(by_name) == {"orders"} | _INTERNAL_NAMES

        assert by_name["orders"].hidden is False
        for name in _INTERNAL_NAMES:
            assert by_name[name].hidden is True, name

    def test_breadcrumb_records_the_matching_tool(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        by_name = {m.name: m for m in ingest_datasource(datasource=ds)}

        assert by_name["_dlt_loads"].meta == {"internal_table": "dlt"}
        assert by_name["alembic_version"].meta == {"internal_table": "alembic"}

    def test_real_tables_get_no_breadcrumb(self, workspace: Path) -> None:
        """A non-matching model must not grow an empty meta dict that would show up in every ingested YAML."""
        _, ds = _ds(workspace, _MIXED)
        by_name = {m.name: m for m in ingest_datasource(datasource=ds)}
        assert by_name["orders"].meta is None

    def test_report_lists_each_hidden_internal(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(datasource=ds)

        assert {h.table_name for h in report.hidden_internals} == _INTERNAL_NAMES
        by_table = {h.table_name: h for h in report.hidden_internals}
        assert by_table["_dlt_version"].tool == "dlt"
        assert by_table["alembic_version"].tool == "alembic"
        assert all(h.kind == "table" for h in report.hidden_internals)

    def test_report_omits_real_tables(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(datasource=ds)
        assert "orders" not in {h.table_name for h in report.hidden_internals}

    def test_matching_view_is_hidden_and_labelled_view(
        self, workspace: Path
    ) -> None:
        """A view named like an internal classifies the same way and keeps its kind for the report."""
        _, ds = _ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL);
            CREATE VIEW _dlt_v AS SELECT id FROM orders;
            """,
        )
        report = ingest_datasource_report(datasource=ds)
        by_name = {m.name: m for m in report.models}

        assert by_name["_dlt_v"].hidden is True
        entry = next(h for h in report.hidden_internals if h.table_name == "_dlt_v")
        assert entry.kind == "view"
        assert entry.tool == "dlt"


class TestMatchesTheLiveObjectName:
    def test_dunder_internal_keeps_its_faithful_name(
        self, workspace: Path
    ) -> None:
        """DEV-1743: the internal-table rule matches the live object name, and the
        model keeps that faithful ``__`` name (no sanitization); the report carries
        both, now equal."""
        _, ds = _ds(
            workspace,
            "CREATE TABLE _dlt_loads__x (id INTEGER PRIMARY KEY, v TEXT);",
        )
        report = ingest_datasource_report(datasource=ds)
        by_name = {m.name: m for m in report.models}

        assert "_dlt_loads__x" in by_name
        assert by_name["_dlt_loads__x"].hidden is True

        entry = next(iter(report.hidden_internals))
        assert entry.table_name == "_dlt_loads__x"
        assert entry.model_name == "_dlt_loads__x"


class TestSqlitePrefixOnAnotherEngine:
    def test_sqlite_named_table_stays_visible_on_duckdb(
        self, workspace: Path
    ) -> None:
        """On DuckDB `sqlite_backup` stays visible while a sibling dlt table is still hidden."""
        pytest.importorskip("duckdb")
        import duckdb

        db_path = str(workspace / "live.duckdb")
        con = duckdb.connect(db_path)
        con.execute("CREATE TABLE sqlite_backup (id INTEGER PRIMARY KEY)")
        con.execute("CREATE TABLE _dlt_loads (load_id VARCHAR)")
        con.close()

        ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
        report = ingest_datasource_report(datasource=ds)
        by_name = {m.name: m for m in report.models}

        assert by_name["sqlite_backup"].hidden is False
        assert by_name["sqlite_backup"].meta is None
        assert by_name["_dlt_loads"].hidden is True
        assert {h.table_name for h in report.hidden_internals} == {"_dlt_loads"}


class TestSkipAndHideAreDisjoint:
    def test_construction_failure_reports_skipped_only(
        self, workspace: Path, monkeypatch
    ) -> None:
        """A per-object build failure lands in `skipped`, not `hidden_internals` — no model was produced to hide."""
        from slayer.engine import ingestion as ing

        real = ing._build_one_model

        def _boom(**kwargs):
            if kwargs["obj"].name == "_dlt_version":
                raise RuntimeError("introspection exploded")
            return real(**kwargs)

        monkeypatch.setattr(ing, "_build_one_model", _boom)

        _, ds = _ds(workspace, _MIXED)
        report = ing.ingest_datasource_report(datasource=ds)

        assert "_dlt_version" in {s.table_name for s in report.skipped}
        assert "_dlt_version" not in {h.table_name for h in report.hidden_internals}
        assert "_dlt_version" not in {m.name for m in report.models}
        # A sibling internal still classifies — the failure is isolated.
        assert "_dlt_loads" in {h.table_name for h in report.hidden_internals}

    def test_distinct_dunder_internals_are_both_hidden_not_skipped(
        self, workspace: Path
    ) -> None:
        """DEV-1743: ``_dlt_loads_x`` and ``_dlt_loads__x`` no longer collapse, so
        both are distinct hidden internals — neither is skipped, and skip/hide
        stay disjoint (nothing skipped)."""
        _, ds = _ds(
            workspace,
            """
            CREATE TABLE _dlt_loads_x (id INTEGER PRIMARY KEY, v TEXT);
            CREATE TABLE _dlt_loads__x (id INTEGER PRIMARY KEY, v TEXT);
            """,
        )
        report = ingest_datasource_report(datasource=ds)

        skipped_names = {s.table_name for s in report.skipped}
        hidden_names = {h.table_name for h in report.hidden_internals}

        assert {"_dlt_loads_x", "_dlt_loads__x"} <= hidden_names
        assert not (skipped_names & hidden_names)


# ---------------------------------------------------------------------------
# --surface-internals
# ---------------------------------------------------------------------------


class TestLiveNameSurvivesToTheReport:
    """The scan carries the live object name through to the report, since reconstructing it from a dotted ``sql_table`` would lose the match."""

    def test_dotted_schema_still_classifies(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(datasource=ds)
        # Simulate the dotted-schema shape and re-check what a consumer sees.
        entry = next(
            t for t in report.internal_tables if t.table_name == "_dlt_loads"
        )
        assert entry.tool == "dlt"

        from slayer.engine.ingestion import _bare_table_name
        from slayer.engine.internal_tables import internal_table_rule

        # The reconstruction that used to back the idempotent report loses it.
        assert internal_table_rule(
            _bare_table_name("project.dataset._dlt_loads")
        ) is None
        # The carried live name does not.
        assert internal_table_rule(entry.table_name) == "dlt"

    async def test_dotted_schema_model_is_still_reported(
        self, workspace: Path
    ) -> None:
        """A hidden internal reaches the idempotent report even when its persisted ``sql_table`` is multi-part."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        await storage.save_model(
            loaded.model_copy(update={"sql_table": "project.dataset._dlt_loads"})
        )

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert "_dlt_loads" in {h.table_name for h in result.hidden_internals}


class TestSurfaceInternals:
    def test_flag_records_the_classification_anyway(
        self, workspace: Path
    ) -> None:
        """`internal_tables` is populated regardless of the flag, so a surfaced run can still report an earlier run's hidden model."""
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(datasource=ds, surface_internals=True)

        assert {t.table_name for t in report.internal_tables} == _INTERNAL_NAMES
        assert all(t.hidden is False for t in report.internal_tables)

    def test_flag_ingests_internals_visible(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        by_name = {
            m.name: m
            for m in ingest_datasource(datasource=ds, surface_internals=True)
        }
        for name in _INTERNAL_NAMES:
            assert by_name[name].hidden is False, name

    def test_flag_writes_no_breadcrumb(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        by_name = {
            m.name: m
            for m in ingest_datasource(datasource=ds, surface_internals=True)
        }
        assert by_name["_dlt_loads"].meta is None

    def test_flag_empties_the_scan_report_list(self, workspace: Path) -> None:
        """The list means "what we hid" — nothing was hidden, so it is empty."""
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(datasource=ds, surface_internals=True)
        assert report.hidden_internals == []

    def test_flag_does_not_change_which_models_exist(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        hidden_run = {m.name for m in ingest_datasource(datasource=ds)}
        surfaced_run = {
            m.name
            for m in ingest_datasource(datasource=ds, surface_internals=True)
        }
        assert hidden_run == surfaced_run


# ---------------------------------------------------------------------------
# Interaction with --include / --exclude
# ---------------------------------------------------------------------------


class TestIncludeExclude:
    def test_excluded_internal_is_absent_from_models_and_report(
        self, workspace: Path
    ) -> None:
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(
            datasource=ds, exclude_tables=["_dlt_loads"]
        )
        assert "_dlt_loads" not in {m.name for m in report.models}
        assert "_dlt_loads" not in {h.table_name for h in report.hidden_internals}

    def test_explicit_include_still_hides(self, workspace: Path) -> None:
        """`--include`/`--exclude` choose which objects are scanned; visibility is a separate axis, so an explicit include still hides."""
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(
            datasource=ds, include_tables=["orders", "_dlt_loads"]
        )
        by_name = {m.name: m for m in report.models}

        assert set(by_name) == {"orders", "_dlt_loads"}
        assert by_name["_dlt_loads"].hidden is True
        assert {h.table_name for h in report.hidden_internals} == {"_dlt_loads"}

    def test_include_plus_surface_internals_shows_it(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(
            datasource=ds,
            include_tables=["_dlt_loads"],
            surface_internals=True,
        )
        assert report.models[0].hidden is False


# ---------------------------------------------------------------------------
# Idempotency — and, above all, what gets REPORTED on a re-ingest
# ---------------------------------------------------------------------------


class TestIdempotencyAndReporting:
    async def test_second_run_keeps_internals_hidden(self, workspace: Path) -> None:
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        assert loaded.hidden is True

    async def test_steady_state_run_still_reports_hidden_internals(
        self, workspace: Path
    ) -> None:
        """A no-op re-ingest produces no `ModelAddition`, so the hidden-internals section is scan-level to stay reported."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        second = await ingest_datasource_idempotent(datasource=ds, storage=storage)

        assert {h.table_name for h in second.hidden_internals} == _INTERNAL_NAMES

    async def test_user_unhidden_internal_is_not_reported_as_hidden(
        self, workspace: Path
    ) -> None:
        """The report describes the post-merge model, so a user-unhidden internal is not reported as hidden."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        await storage.save_model(loaded.model_copy(update={"hidden": False}))

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reported = {h.table_name for h in result.hidden_internals}
        assert "_dlt_loads" not in reported
        # The others are untouched and still reported.
        assert "_dlt_version" in reported

    async def test_user_unhidden_internal_stays_visible(
        self, workspace: Path
    ) -> None:
        """Re-ingest must never fight a deliberate un-hide; the model stays visible."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        await storage.save_model(loaded.model_copy(update={"hidden": False}))

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        again = await storage.get_model("_dlt_loads", data_source="ds")
        assert again is not None
        assert again.hidden is False

    async def test_preexisting_visible_model_is_never_retro_hidden(
        self, workspace: Path
    ) -> None:
        """A pre-existing visible internal is never retro-hidden, since that would mutate user-owned config."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await storage.save_model(
            SlayerModel(
                name="_dlt_loads",
                data_source="ds",
                sql_table="_dlt_loads",
                columns=[Column(name="load_id", sql="load_id", type=DataType.TEXT)],
            )
        )

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        assert loaded.hidden is False
        # And the report agrees — it describes effective state, not the scan.
        assert "_dlt_loads" not in {h.table_name for h in result.hidden_internals}

    async def test_user_authored_sql_model_is_untouched_and_reported_honestly(
        self, workspace: Path
    ) -> None:
        """A user-authored sql-mode model is left untouched, and the report reads effective storage state."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await storage.save_model(
            SlayerModel(
                name="_dlt_loads",
                data_source="ds",
                sql="SELECT load_id FROM _dlt_loads",
                columns=[Column(name="load_id", sql="load_id", type=DataType.TEXT)],
            )
        )

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        assert loaded.sql == "SELECT load_id FROM _dlt_loads"
        assert loaded.hidden is False
        assert "_dlt_loads" not in {h.table_name for h in result.hidden_internals}

    async def test_dunder_internal_model_uses_faithful_name(
        self, workspace: Path
    ) -> None:
        """DEV-1743: a ``__``-named internal table keeps its faithful model name
        (no sanitization), so table_name == model_name in the report."""
        _, ds = _ds(
            workspace,
            "CREATE TABLE _dlt_loads__x (id INTEGER PRIMARY KEY, v TEXT);",
        )
        storage = await _storage_with(workspace, ds)

        first = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        second = await ingest_datasource_idempotent(datasource=ds, storage=storage)

        for result in (first, second):
            entry = next(iter(result.hidden_internals))
            assert entry.table_name == "_dlt_loads__x"
            assert entry.model_name == "_dlt_loads__x"

    async def test_surface_internals_does_not_unhide_an_existing_model(
        self, workspace: Path
    ) -> None:
        """The flag controls creation, not mutation, so it does not unhide an existing model and still lists it."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, surface_internals=True
        )

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        assert loaded.hidden is True
        assert "_dlt_loads" in {h.table_name for h in result.hidden_internals}

    async def test_user_edits_to_an_internal_model_survive_re_ingest(
        self, workspace: Path
    ) -> None:
        """The breadcrumb must not clobber a user's own meta keys on merge."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        loaded = await storage.get_model("_dlt_loads", data_source="ds")
        assert loaded is not None
        await storage.save_model(
            loaded.model_copy(
                update={
                    "description": "load bookkeeping",
                    "meta": {**(loaded.meta or {}), "owner": "data-eng"},
                }
            )
        )

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        again = await storage.get_model("_dlt_loads", data_source="ds")
        assert again is not None
        assert again.description == "load bookkeeping"
        assert again.meta is not None
        assert again.meta["owner"] == "data-eng"
        assert again.meta["internal_table"] == "dlt"


class TestColumnsToModelKwargs:
    def test_meta_is_propagated_verbatim(self) -> None:
        """`_columns_to_model` passes `meta` through untouched, since the caller merges the breadcrumb."""
        from slayer.engine.ingestion import IntrospectedColumn, _columns_to_model

        model = _columns_to_model(
            name="t",
            columns=[IntrospectedColumn(name="id", type=DataType.INT, primary_key=True)],
            data_source="ds",
            sql_table="t",
            hidden=True,
            meta={"internal_table": "dlt", "provenance": "scan"},
        )
        assert model.hidden is True
        assert model.meta == {"internal_table": "dlt", "provenance": "scan"}

    def test_defaults_leave_the_model_untouched(self) -> None:
        """The dbt hidden-import path calls this without the new kwargs."""
        from slayer.engine.ingestion import IntrospectedColumn, _columns_to_model

        model = _columns_to_model(
            name="t",
            columns=[IntrospectedColumn(name="id", type=DataType.INT, primary_key=True)],
            data_source="ds",
            sql_table="t",
        )
        assert model.hidden is False
        assert model.meta is None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _result_with_hidden(**overrides) -> IdempotentIngestResult:
    base = dict(
        additions=[
            ModelAddition(
                model_name="orders", data_source="ds", created=True,
                new_columns=["id"],
            )
        ],
        to_delete=[],
        errors=[],
        skipped=[],
        hidden_internals=[
            InternalTable(
                table_name="_dlt_loads", model_name="_dlt_loads",
                tool="dlt", kind="table",
            ),
            InternalTable(
                table_name="alembic_version", model_name="alembic_version",
                tool="alembic", kind="table",
            ),
        ],
    )
    base.update(overrides)
    return IdempotentIngestResult(**base)


class TestRenderer:
    def test_section_renders_a_full_line_per_table(self, capsys) -> None:
        """Asserted as whole lines: a substring check on `dlt` would pass even if the `tool` field were dropped."""
        _print_ingest_drift_and_errors(_result_with_hidden())
        out = capsys.readouterr().out

        assert "Hidden (2)" in out
        assert "  - _dlt_loads: dlt" in out
        assert "  - alembic_version: alembic" in out

    def test_line_names_the_model_when_it_differs(self, capsys) -> None:
        """The advice takes the MODEL name, so a `__`-sanitized line must name the model the user can act on."""
        _print_ingest_drift_and_errors(
            _result_with_hidden(
                hidden_internals=[
                    InternalTable(
                        table_name="_dlt_loads__x", model_name="_dlt_loads_x",
                        tool="dlt", kind="table",
                    )
                ]
            )
        )
        out = capsys.readouterr().out
        assert "  - _dlt_loads__x (model: _dlt_loads_x): dlt" in out

    def test_line_stays_bare_when_names_match(self, capsys) -> None:
        """The common case must not grow a redundant parenthetical."""
        _print_ingest_drift_and_errors(_result_with_hidden())
        out = capsys.readouterr().out
        assert "  - _dlt_loads: dlt" in out
        assert "(model:" not in out

    def test_section_explains_both_escape_hatches(self, capsys) -> None:
        """`--surface-internals` only affects newly created models, so the section also points at `edit_model`."""
        _print_ingest_drift_and_errors(_result_with_hidden(), data_source="ds")
        out = capsys.readouterr().out

        assert "--surface-internals" in out
        assert "hidden=false" in out

    def test_hint_is_datasource_qualified(self, capsys) -> None:
        """The hint is datasource-qualified because a bare internal name collides across every dlt-loaded database."""
        _print_ingest_drift_and_errors(_result_with_hidden(), data_source="warehouse")
        out = capsys.readouterr().out

        assert 'edit_model("<model>", data_source="warehouse", hidden=false)' in out

    def test_hint_degrades_when_the_datasource_is_unknown(self, capsys) -> None:
        """When the caller omits the datasource, the hint degrades gracefully rather than printing `data_source="None"`."""
        _print_ingest_drift_and_errors(_result_with_hidden())
        out = capsys.readouterr().out

        assert 'edit_model("<model>", hidden=false)' in out
        assert "data_source=" not in out

    def test_nothing_printed_when_no_internals(self, capsys) -> None:
        _print_ingest_drift_and_errors(_result_with_hidden(hidden_internals=[]))
        assert "Hidden" not in capsys.readouterr().out

    def test_tolerates_a_result_lacking_the_attribute(self, capsys) -> None:
        """The renderer tolerates a result missing the attribute, matching the defensiveness `skipped` already gets."""
        legacy = SimpleNamespace(to_delete=[], errors=[], skipped=[])
        _print_ingest_drift_and_errors(legacy)
        assert "Hidden" not in capsys.readouterr().out

    def test_accepts_a_scan_report(self, workspace: Path, capsys) -> None:
        """The renderer accepts an `IngestionScanReport`, which lacks `to_delete` and `errors` entirely."""
        _, ds = _ds(workspace, _MIXED)
        report = ingest_datasource_report(datasource=ds)

        _print_ingest_drift_and_errors(report)

        out = capsys.readouterr().out
        assert "Hidden (4)" in out
        assert "  - _dlt_loads: dlt" in out

    def test_hidden_and_skipped_sections_coexist(self, capsys) -> None:
        result = _result_with_hidden(
            skipped=[
                SkippedTable(table_name="weird__name", reason="name collision")
            ]
        )
        _print_ingest_drift_and_errors(result)
        out = capsys.readouterr().out

        assert "Skipped (1)" in out
        assert "Hidden (2)" in out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _args(workspace: Path, **overrides) -> SimpleNamespace:
    base = dict(
        datasource="ds",
        schema=None,
        include=None,
        exclude=None,
        include_views=True,
        surface_internals=False,
        storage=str(workspace / "storage"),
        models_dir=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _async_return(value):
    async def _call(*_args, **_kwargs):
        return value
    return _call


def _patch_ingest(monkeypatch, result, captured: dict | None = None) -> None:
    async def _fake(**kwargs):
        if captured is not None:
            captured.update(kwargs)
        return result

    monkeypatch.setattr(
        "slayer.engine.ingestion.ingest_datasource_idempotent", _fake
    )
    monkeypatch.setattr(
        "slayer.cli._resolve_storage",
        lambda _args: SimpleNamespace(
            get_datasource=_async_return(
                SimpleNamespace(name="ds", type="sqlite", database=":memory:")
            )
        ),
    )


class TestCliExitCodes:
    def test_hidden_internals_alone_exit_zero(
        self, workspace: Path, monkeypatch, capsys
    ) -> None:
        """Hiding is the intended outcome with nothing for the user to fix, so it exits 0 unlike a skip."""
        from slayer.cli import _run_ingest

        _patch_ingest(monkeypatch, _result_with_hidden())
        _run_ingest(_args(workspace))

        out = capsys.readouterr().out
        assert "Hidden (2)" in out

    def test_skipped_alongside_hidden_still_exits_one(
        self, workspace: Path, monkeypatch
    ) -> None:
        from slayer.cli import _run_ingest

        _patch_ingest(
            monkeypatch,
            _result_with_hidden(
                skipped=[
                    SkippedTable(table_name="weird__name", reason="name collision")
                ]
            ),
        )
        # `args` built outside the block so only one call inside it can raise (Sonar S5778).
        args = _args(workspace)
        with pytest.raises(SystemExit) as exc:
            _run_ingest(args)
        assert exc.value.code == 1


class TestCliFlagThreading:
    def test_flag_defaults_to_false(
        self, workspace: Path, monkeypatch
    ) -> None:
        from slayer.cli import _run_ingest

        captured: dict = {}
        _patch_ingest(monkeypatch, _result_with_hidden(), captured)
        _run_ingest(_args(workspace))
        assert captured["surface_internals"] is False

    def test_flag_reaches_the_engine(
        self, workspace: Path, monkeypatch
    ) -> None:
        from slayer.cli import _run_ingest

        captured: dict = {}
        _patch_ingest(monkeypatch, _result_with_hidden(), captured)
        _run_ingest(_args(workspace, surface_internals=True))
        assert captured["surface_internals"] is True


class TestParserWiring:
    """The parser is built inline in ``main()``, so drive it through ``main()`` with a stubbed handler."""

    @staticmethod
    def _capture(monkeypatch, argv: list[str], handler: str) -> SimpleNamespace:
        import sys

        captured: dict[str, SimpleNamespace] = {}

        def _stub(args, *_rest, **_kwargs):
            captured["args"] = args

        monkeypatch.setattr(f"slayer.cli.{handler}", _stub)
        monkeypatch.setattr(sys, "argv", ["slayer", *argv])

        from slayer.cli import main

        main()
        return captured["args"]

    def test_ingest_defaults_the_flag_off(self, monkeypatch) -> None:
        args = self._capture(
            monkeypatch, ["ingest", "--datasource", "ds"], "_run_ingest"
        )
        assert args.surface_internals is False

    def test_ingest_accepts_the_flag(self, monkeypatch) -> None:
        args = self._capture(
            monkeypatch,
            ["ingest", "--datasource", "ds", "--surface-internals"],
            "_run_ingest",
        )
        assert args.surface_internals is True

    def test_datasources_create_accepts_the_flag(self, monkeypatch) -> None:
        args = self._capture(
            monkeypatch,
            [
                "datasources", "create", "sqlite:///x.db",
                "--ingest", "--surface-internals",
            ],
            "_run_datasources_create",
        )
        assert args.surface_internals is True


class TestDatasourcesCreateReporting:
    """`datasources create --ingest` now reports hidden internals and skips instead of silently hiding models."""

    @staticmethod
    def _create_args(workspace: Path, db_path: str, **overrides) -> SimpleNamespace:
        base = dict(
            connection_string=f"sqlite:///{db_path}",
            name="ds",
            description=None,
            ingest=True,
            include=None,
            exclude=None,
            schema=None,
            include_views=True,
            surface_internals=False,
            yes=True,
            storage=str(workspace / "storage"),
            models_dir=None,
        )
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_hidden_section_printed(self, workspace: Path, capsys) -> None:
        from slayer.cli import _run_datasources_create

        db_path, _ = _ds(workspace, _MIXED)
        storage = YAMLStorage(base_dir=str(workspace / "storage"))

        _run_datasources_create(self._create_args(workspace, db_path), storage)

        out = capsys.readouterr().out
        assert "Hidden (4)" in out
        assert "_dlt_loads" in out
        assert "alembic_version" in out

    def test_skipped_section_printed(self, workspace: Path, capsys) -> None:
        """The report form closes the pre-existing gap where this path swallowed
        skips entirely. DEV-1743: ``__`` is no longer a skip cause, so the skip
        is driven by the reserved ``__slayer_`` prefix instead."""
        from slayer.cli import _run_datasources_create

        db_path, _ = _ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY);
            CREATE TABLE __slayer_reserved (id INTEGER PRIMARY KEY);
            """,
        )
        storage = YAMLStorage(base_dir=str(workspace / "storage"))

        _run_datasources_create(self._create_args(workspace, db_path), storage)

        out = capsys.readouterr().out
        assert "Skipped (1)" in out
        assert "__slayer_reserved" in out

    def test_flag_surfaces_them(self, workspace: Path, capsys) -> None:
        from slayer.cli import _run_datasources_create

        db_path, _ = _ds(workspace, _MIXED)
        storage = YAMLStorage(base_dir=str(workspace / "storage"))

        _run_datasources_create(
            self._create_args(workspace, db_path, surface_internals=True), storage
        )

        assert "Hidden" not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# REST
# ---------------------------------------------------------------------------


def _api_client(workspace: Path, db_path: str):
    from fastapi.testclient import TestClient

    from slayer.api.server import create_app

    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    client = TestClient(create_app(storage=storage))
    client.post(
        "/datasources",
        json={"name": "ds", "type": "sqlite", "database": db_path},
    )
    return client


class TestRest:
    def test_request_defaults_the_flag_off(self) -> None:
        from slayer.api.server import IngestRequest

        assert IngestRequest(datasource="ds").surface_internals is False

    def test_request_accepts_the_flag(self) -> None:
        from slayer.api.server import IngestRequest

        assert (
            IngestRequest(datasource="ds", surface_internals=True).surface_internals
            is True
        )

    def test_route_forwards_the_flag(self, workspace: Path, monkeypatch) -> None:
        """The route actually forwards the flag, not just accepts it on `IngestRequest`."""
        captured: dict = {}

        async def _fake(**kwargs):
            captured.update(kwargs)
            return IdempotentIngestResult()

        monkeypatch.setattr(
            "slayer.engine.ingestion.ingest_datasource_idempotent", _fake
        )
        db_path, _ = _ds(workspace, _MIXED)
        client = _api_client(workspace, db_path)

        client.post(
            "/ingest", json={"datasource": "ds", "surface_internals": True}
        )
        assert captured["surface_internals"] is True

    def test_route_defaults_the_flag_off(
        self, workspace: Path, monkeypatch
    ) -> None:
        captured: dict = {}

        async def _fake(**kwargs):
            captured.update(kwargs)
            return IdempotentIngestResult()

        monkeypatch.setattr(
            "slayer.engine.ingestion.ingest_datasource_idempotent", _fake
        )
        db_path, _ = _ds(workspace, _MIXED)
        client = _api_client(workspace, db_path)

        client.post("/ingest", json={"datasource": "ds"})
        assert captured["surface_internals"] is False

    def test_body_carries_hidden_internals_at_200(self, workspace: Path) -> None:
        """The 200 body carries hidden internals; they must never turn the response into a 422, matching `skipped`."""
        db_path, _ = _ds(workspace, _MIXED)
        client = _api_client(workspace, db_path)

        resp = client.post("/ingest", json={"datasource": "ds"})
        assert resp.status_code == 200

        body = resp.json()
        assert {h["table_name"] for h in body["hidden_internals"]} == _INTERNAL_NAMES
        assert {h["tool"] for h in body["hidden_internals"]} == {"dlt", "alembic"}

    def test_re_ingest_with_flag_does_not_unhide(self, workspace: Path) -> None:
        """The creation-only rule holds over REST too; the body reports the model as still hidden."""
        db_path, _ = _ds(workspace, _MIXED)
        client = _api_client(workspace, db_path)

        client.post("/ingest", json={"datasource": "ds"})
        resp = client.post(
            "/ingest", json={"datasource": "ds", "surface_internals": True}
        )

        assert resp.status_code == 200
        assert "_dlt_loads" in {
            h["table_name"] for h in resp.json()["hidden_internals"]
        }
        assert "_dlt_loads" not in {
            m["name"] for m in client.get("/models").json()
        }

    def test_hidden_internals_absent_from_the_models_endpoint(
        self, workspace: Path
    ) -> None:
        db_path, _ = _ds(workspace, _MIXED)
        client = _api_client(workspace, db_path)
        client.post("/ingest", json={"datasource": "ds"})

        listed = {m["name"] for m in client.get("/models").json()}
        assert listed == {"orders"}


# ---------------------------------------------------------------------------
# What hiding actually buys, end to end
# ---------------------------------------------------------------------------


async def _models_in(storage: YAMLStorage) -> list[SlayerModel]:
    names = await storage.list_models(data_source="ds")
    return [
        m
        for n in names
        if (m := await storage.get_model(n, data_source="ds")) is not None
    ]


class TestVisibilitySurfaces:
    async def test_nothing_is_omitted_from_storage(self, workspace: Path) -> None:
        """Hidden, not skipped — every object still becomes a model."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        assert {m.name for m in await _models_in(storage)} == (
            {"orders"} | _INTERNAL_NAMES
        )

    async def test_absent_from_mcp_models_summary(self, workspace: Path) -> None:
        """Hidden internals are absent from the real MCP `models_summary` tool."""
        from slayer.mcp.server import create_mcp_server

        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        mcp = create_mcp_server(storage=storage)
        blocks, _ = await mcp.call_tool(
            name="models_summary", arguments={"datasource_name": "ds"}
        )
        summary = blocks[0].text

        assert "orders" in summary
        for name in _INTERNAL_NAMES:
            assert name not in summary, name

    async def test_absent_from_the_bi_catalog(self, workspace: Path) -> None:
        """pg_facade and Flight both build through ``FacadeCatalog``, so one assertion covers both."""
        from slayer.facade.catalog import build_catalog

        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        catalog = build_catalog(
            models_by_datasource={"ds": await _models_in(storage)}
        )
        exposed = {t.name for s in catalog.schemas for t in s.tables}
        assert exposed == {"orders"}

    async def test_absent_from_the_search_index(self, workspace: Path) -> None:
        from slayer.search.index import build_in_memory_corpus

        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        corpus = build_in_memory_corpus(
            memories=[], models=await _models_in(storage), datasources=["ds"],
        )
        indexed = set(corpus.canonical_to_kind)
        assert "ds.orders" in indexed
        assert not any(name in c for c in indexed for name in _INTERNAL_NAMES)

    async def test_hidden_internal_is_still_queryable_by_name(
        self, workspace: Path
    ) -> None:
        """A hidden internal is still queryable when deliberately targeted by name — the point of hidden over skipped."""
        from slayer.engine.query_engine import SlayerQueryEngine

        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        engine = SlayerQueryEngine(storage=storage)
        resp = await engine.execute(
            SlayerQuery(
                source_model="_dlt_loads",
                measures=[{"formula": "*:count", "name": "n"}],
            )
        )
        assert resp.row_count == 1
        assert next(iter(resp.data[0].values())) == 1


class TestJoinsIntoHiddenModels:
    async def test_visible_model_joins_to_a_hidden_one(
        self, workspace: Path
    ) -> None:
        """Hiding must not filter join targets, since a silent drop would return different rows without raising."""
        from slayer.engine.query_engine import SlayerQueryEngine

        _, ds = _ds(
            workspace,
            """
            CREATE TABLE _dlt_loads (
                id INTEGER PRIMARY KEY,
                schema_name TEXT NOT NULL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                load_id INTEGER REFERENCES _dlt_loads(id)
            );
            INSERT INTO _dlt_loads VALUES (1, 'openfda'), (2, 'other');
            INSERT INTO orders VALUES (1, 10.0, 1), (2, 20.0, 1), (3, 5.0, 2);
            """,
        )
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        hidden = await storage.get_model("_dlt_loads", data_source="ds")
        assert hidden is not None
        assert hidden.hidden is True

        engine = SlayerQueryEngine(storage=storage)
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=["_dlt_loads.schema_name"],
                measures=[{"formula": "amount:sum", "name": "total"}],
            )
        )
        # The hidden model's name in the result key is evidence the join resolved.
        assert resp.row_count == 2
        assert set(resp.data[0]) == {
            "orders._dlt_loads.schema_name",
            "orders.total",
        }
        assert {
            row["orders._dlt_loads.schema_name"]: row["orders.total"]
            for row in resp.data
        } == {"openfda": 30.0, "other": 5.0}


class TestDriftScope:
    async def test_hidden_internal_is_validated_like_any_other_model(
        self, workspace: Path
    ) -> None:
        """A hidden internal is still validated like any other model, so its schema drift is surfaced."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        conn = sqlite3.connect(str(workspace / "live.db"))
        conn.execute("DROP TABLE _dlt_version")
        conn.commit()
        conn.close()

        names = await storage.list_models(data_source="ds")
        models = [
            m
            for n in names
            if (m := await storage.get_model(n, data_source="ds")) is not None
        ]
        to_delete = await validate_datasource(datasource=ds, models=models)

        entry = next(e for e in to_delete if e.model_name == "_dlt_version")
        assert entry.tool == "delete_model"
        assert entry.data_source == "ds"


# ---------------------------------------------------------------------------
# The MCP ingest tool's own report
# ---------------------------------------------------------------------------


class TestMcpIngestReporting:
    """`ingest_datasource_models` has its own renderer, which must report hidden internals and skips to the agent that ran the ingest."""

    async def _ingest_via_mcp(self, storage, *, schema_name: str = "") -> str:
        from slayer.mcp.server import create_mcp_server

        mcp = create_mcp_server(storage=storage)
        blocks, _ = await mcp.call_tool(
            name="ingest_datasource_models",
            arguments={"datasource_name": "ds", "schema_name": schema_name},
        )
        return blocks[0].text

    async def test_first_ingest_reports_hidden_internals(
        self, workspace: Path
    ) -> None:
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        out = await self._ingest_via_mcp(storage)

        assert "Hidden (4)" in out
        assert "- _dlt_loads: dlt" in out
        assert "- alembic_version: alembic" in out
        # And the real table is still reported as created, not swallowed.
        assert "orders" in out

    async def test_steady_state_re_ingest_still_reports_them(
        self, workspace: Path
    ) -> None:
        """A no-op re-ingest still reports hidden internals rather than returning "already in sync"."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        await self._ingest_via_mcp(storage)
        second = await self._ingest_via_mcp(storage)

        assert "already in sync" not in second
        assert "Hidden (4)" in second
        assert "- _dlt_version: dlt" in second

    async def test_report_uses_the_faithful_dunder_name(
        self, workspace: Path
    ) -> None:
        """DEV-1743: a ``__``-named internal table keeps its faithful model name,
        so the report shows the bare name with no ``(model: …)`` annotation (the
        model no longer differs from the live object)."""
        _, ds = _ds(
            workspace, "CREATE TABLE _dlt_loads__x (id INTEGER PRIMARY KEY);"
        )
        storage = await _storage_with(workspace, ds)

        out = await self._ingest_via_mcp(storage)

        assert "- _dlt_loads__x: dlt" in out
        assert "(model:" not in out

    async def test_report_points_at_the_escape_hatch(
        self, workspace: Path
    ) -> None:
        """The agent-facing hint names `edit_model`, not the CLI-only `--surface-internals` flag."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        out = await self._ingest_via_mcp(storage)

        assert "edit_model" in out
        assert "hidden=false" in out
        assert "--surface-internals" not in out

    async def test_hint_names_the_ingested_datasource(
        self, workspace: Path
    ) -> None:
        """The hint names the ingested datasource, since an unqualified `edit_model` could un-hide a different datasource's model."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        out = await self._ingest_via_mcp(storage)

        assert 'edit_model("<model>", data_source="ds", hidden=false)' in out

    async def test_two_datasources_share_internal_names(
        self, workspace: Path
    ) -> None:
        """Two dlt pipelines in one store both produce `_dlt_loads`, so the hint must name which one was ingested."""
        _, ds_a = _ds(workspace, _MIXED, name="a.db")
        db_b = str(workspace / "b.db")
        conn = sqlite3.connect(db_b)
        conn.executescript(_MIXED)
        conn.commit()
        conn.close()
        ds_b = DatasourceConfig(name="second", type="sqlite", database=db_b)

        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        await storage.save_datasource(ds_a)
        await storage.save_datasource(ds_b)
        await ingest_datasource_idempotent(datasource=ds_a, storage=storage)
        await ingest_datasource_idempotent(datasource=ds_b, storage=storage)

        # Same model name really does exist under both datasources.
        for name in ("ds", "second"):
            model = await storage.get_model("_dlt_loads", data_source=name)
            assert model is not None and model.hidden is True, name

        from slayer.mcp.server import create_mcp_server

        mcp = create_mcp_server(storage=storage)
        blocks, _ = await mcp.call_tool(
            name="ingest_datasource_models",
            arguments={"datasource_name": "second", "schema_name": ""},
        )
        out = blocks[0].text

        assert 'data_source="second"' in out
        assert 'data_source="ds"' not in out

    async def test_skipped_objects_are_reported_too(
        self, workspace: Path
    ) -> None:
        """Skips are reported here too. DEV-1743: ``__`` no longer causes a skip,
        so the reserved ``__slayer_`` prefix drives the skip instead."""
        _, ds = _ds(
            workspace,
            """
            CREATE TABLE orders (id INTEGER PRIMARY KEY);
            CREATE TABLE __slayer_reserved (id INTEGER PRIMARY KEY);
            """,
        )
        storage = await _storage_with(workspace, ds)

        out = await self._ingest_via_mcp(storage)

        assert "Skipped (1)" in out
        assert "__slayer_reserved" in out
        # No `--exclude` in sight: this tool has no such argument.
        assert "--exclude" not in out

    async def test_nothing_printed_when_there_are_no_internals(
        self, workspace: Path
    ) -> None:
        _, ds = _ds(workspace, "CREATE TABLE orders (id INTEGER PRIMARY KEY);")
        storage = await _storage_with(workspace, ds)

        out = await self._ingest_via_mcp(storage)

        assert "Hidden" not in out
        assert "Skipped" not in out

    async def test_empty_schema_message_survives(self, workspace: Path) -> None:
        """Widening the early-return guard must not cost the empty-schema hint that tells an agent to try another schema."""
        _, ds = _ds(workspace, "CREATE TABLE placeholder (id INTEGER);")
        storage = await _storage_with(workspace, ds)
        conn = sqlite3.connect(str(workspace / "live.db"))
        conn.execute("DROP TABLE placeholder")
        conn.commit()
        conn.close()

        out = await self._ingest_via_mcp(storage)

        assert "Hidden" not in out
        assert "already in sync" not in out

    def test_renderer_tolerates_a_result_lacking_the_attributes(self) -> None:
        """The renderer tolerates a result lacking the attributes, mirroring the CLI renderer's defensiveness."""
        from slayer.mcp.server import (
            _render_hidden_internals_section,
            _render_skipped_section,
        )

        legacy = SimpleNamespace(additions=[], to_delete=[], errors=[])
        assert _render_skipped_section(
            list(getattr(legacy, "skipped", None) or [])
        ) == []
        assert _render_hidden_internals_section(
            list(getattr(legacy, "hidden_internals", None) or [])
        ) == []

    def test_section_renders_whole_lines(self) -> None:
        """Asserted as whole lines: a substring check on `dlt` would pass even if the `tool` field were dropped."""
        from slayer.mcp.server import _render_hidden_internals_section

        lines = _render_hidden_internals_section([
            InternalTable(
                table_name="_dlt_loads", model_name="_dlt_loads",
                tool="dlt", kind="table",
            ),
        ])

        assert "Hidden (1) — recognised ELT/migration internals " \
               "(excluded from models_summary; still queryable by name):" in lines
        assert "- _dlt_loads: dlt" in lines
