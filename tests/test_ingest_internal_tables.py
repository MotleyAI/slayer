"""Recognised ELT/migration housekeeping tables ingest hidden (DEV-1759).

An unfiltered ingest modelled `_dlt_loads` / `_dlt_pipeline_state` /
`_dlt_version` as first-class semantic models. The model list is the menu
handed to an agent over MCP, so junk entries cost tokens in every session and
invite wrong turns — agents do not share the human instinct that an
underscore-prefixed table is off-limits.

Hidden, not skipped: DEV-1741 spent a release killing silent omissions, and
`_dlt_loads` legitimately answers "when did this last load?" when targeted by
name. So the table is modelled, queryable, and joinable — just absent from
every listing surface.

The sharp edge is REPORTING, and it has two faces. The rule fires while
building the scanned candidate, but `_additive_merge_existing` preserves the
persisted model's `hidden`, so an implementation that reports what the SCAN
classified rather than what SURVIVED THE MERGE lies twice over: it calls a
user-unhidden model hidden, and it goes silent about a still-hidden model on
the very run where `--surface-internals` was passed to see it. Hence the
deliberate split — `IngestionScanReport.hidden_internals` is what the scan
constructed (correct for the fresh path, which never touches storage) while
`IdempotentIngestResult.hidden_internals` is effective post-merge state.
`TestIdempotencyAndReporting` is what pins that apart.
"""
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
    HiddenInternal,
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


# A real table plus the dlt trio and an Alembic bookkeeping table — the shape
# the reporter actually hit on a dlt-loaded DuckDB.
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
    """Three namespaces are reserved by contract, so a prefix match is safe."""

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

    @pytest.mark.parametrize(
        "name", ["sqlite_sequence", "sqlite_stat1", "sqlite_stat4"]
    )
    def test_sqlite_prefix(self, name: str) -> None:
        assert internal_table_rule(name) == "sqlite"


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
    """Liquibase upper-cases, EF Core and Sequelize camel-case. A
    case-sensitive matcher silently misses every one of them."""

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
            ("SQLite_Sequence", "sqlite"),
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
        """Singer's `_sdc_` surface is columns on real tables. A table rule
        would match nothing real, so there deliberately isn't one — and if one
        is ever added it must be a considered decision, not a drive-by."""
        assert internal_table_rule(name) is None

    @pytest.mark.parametrize("name", ["_fivetran_synced", "_fivetran_deleted"])
    def test_fivetran_column_names_are_not_table_rules(self, name: str) -> None:
        """Same for Fivetran: only the two audit TABLES are rules. There is no
        `_fivetran_` prefix rule, because everything else the vendor writes
        into a destination schema is a column."""
        assert internal_table_rule(name) is None

    def test_empty_name_does_not_match(self) -> None:
        assert internal_table_rule("") is None


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
        """A non-matching model must not grow an empty meta dict — that would
        show up in every ingested YAML for no reason."""
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
        """Views are ingested since DEV-1741, so a view named like an internal
        must classify the same way — and keep its kind for the report."""
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
    def test_sanitized_model_name_does_not_decide_the_match(
        self, workspace: Path
    ) -> None:
        """Model names cannot contain `__`, so DEV-1741 sanitizes a dlt child
        table `_dlt_loads__x` down to `_dlt_loads_x`. Matching the sanitized
        name means matching a derived string; the rule must see the live one,
        and the report must carry both so the user can find the table."""
        _, ds = _ds(
            workspace,
            "CREATE TABLE _dlt_loads__x (id INTEGER PRIMARY KEY, v TEXT);",
        )
        report = ingest_datasource_report(datasource=ds)
        by_name = {m.name: m for m in report.models}

        assert "_dlt_loads_x" in by_name
        assert by_name["_dlt_loads_x"].hidden is True

        entry = next(iter(report.hidden_internals))
        assert entry.table_name == "_dlt_loads__x"
        assert entry.model_name == "_dlt_loads_x"


class TestSkipAndHideAreDisjoint:
    def test_construction_failure_reports_skipped_only(
        self, workspace: Path, monkeypatch
    ) -> None:
        """The other way an object lands in `skipped`: the per-object
        try/except around `_build_one_model`. Classifying before that call
        returns would report a table as hidden that produced no model at all."""
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

    def test_collision_skipped_object_is_not_also_reported_hidden(
        self, workspace: Path
    ) -> None:
        """`_assign_model_names` reserves unsanitized names first, so a real
        `_dlt_loads_x` beats the sanitized `_dlt_loads__x` and the latter is
        skipped. Classifying before that decision would report one object in
        both `skipped` and `hidden_internals` — two contradictory verdicts on
        the same table."""
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

        assert "_dlt_loads__x" in skipped_names
        assert not (skipped_names & hidden_names)
        # The winner still classifies normally.
        assert "_dlt_loads_x" in hidden_names


# ---------------------------------------------------------------------------
# --surface-internals
# ---------------------------------------------------------------------------


class TestSurfaceInternals:
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
        """`--include` and `--exclude` choose WHICH objects are scanned;
        visibility is a separate axis with its own flag. Making an explicit
        include surface the model would mean the first ingest's invocation
        shape silently decides the model's visibility forever, and would make
        REST (which has no flags) behave differently from the CLI."""
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
        """A no-op re-ingest produces no `ModelAddition`, so labelling the
        `Created:` line would go silent here — precisely the silence class
        DEV-1741 set out to kill. The section is scan-level for this reason."""
        _, ds = _ds(workspace, _MIXED)
        storage = await _storage_with(workspace, ds)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        second = await ingest_datasource_idempotent(datasource=ds, storage=storage)

        assert {h.table_name for h in second.hidden_internals} == _INTERNAL_NAMES

    async def test_user_unhidden_internal_is_not_reported_as_hidden(
        self, workspace: Path
    ) -> None:
        """The report must describe the model that SURVIVED THE MERGE. The
        additive merge preserves the persisted `hidden=False`, so reporting
        what the scan classified would call a visible model hidden."""
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
        """Creation-only: re-ingest must never fight a deliberate un-hide.
        `_dlt_loads` answers freshness questions, which the issue explicitly
        wants to keep working."""
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
        """A store ingested before this feature keeps its visible internals.
        Retro-hiding would mutate user-owned config on an unrelated run."""
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
        """`_process_one_table` early-returns for sql-mode models, so nothing
        merges. The re-derived report must still read effective storage state
        rather than assume the scan's verdict applied."""
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

    async def test_sanitized_model_is_reported_with_both_names(
        self, workspace: Path
    ) -> None:
        """The post-merge re-derivation has to look the model up by
        `model_name`. Keying it on `table_name` would find nothing for a
        `__`-sanitized table, so a correctly-hidden model would silently
        vanish from the report."""
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
            assert entry.model_name == "_dlt_loads_x"

    async def test_surface_internals_does_not_unhide_an_existing_model(
        self, workspace: Path
    ) -> None:
        """The flag controls creation, not mutation — and the report must say
        so by still listing the model. Emptying the list here would print
        nothing on the exact run where the user asked to see these."""
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
        """`_columns_to_model` must pass `meta` through untouched — the
        breadcrumb is merged by the caller, so a constructor that rebuilt or
        replaced the dict here would drop keys. (Persisted-meta preservation
        across re-ingest is covered by
        `test_user_edits_to_an_internal_model_survive_re_ingest`.)"""
        from slayer.engine.ingestion import _columns_to_model

        model = _columns_to_model(
            name="t",
            columns=[("id", DataType.INT, True, False, None)],
            data_source="ds",
            sql_table="t",
            hidden=True,
            meta={"internal_table": "dlt", "provenance": "scan"},
        )
        assert model.hidden is True
        assert model.meta == {"internal_table": "dlt", "provenance": "scan"}

    def test_defaults_leave_the_model_untouched(self) -> None:
        """The dbt hidden-import path calls this without the new kwargs."""
        from slayer.engine.ingestion import _columns_to_model

        model = _columns_to_model(
            name="t",
            columns=[("id", DataType.INT, True, False, None)],
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
            HiddenInternal(
                table_name="_dlt_loads", model_name="_dlt_loads",
                tool="dlt", kind="table",
            ),
            HiddenInternal(
                table_name="alembic_version", model_name="alembic_version",
                tool="alembic", kind="table",
            ),
        ],
    )
    base.update(overrides)
    return IdempotentIngestResult(**base)


class TestRenderer:
    def test_section_renders_a_full_line_per_table(self, capsys) -> None:
        """Asserted as whole lines on purpose: `"dlt" in out` is satisfied by
        the substring inside `_dlt_loads`, so a renderer that dropped the
        `tool` field entirely would still pass a naive containment check."""
        _print_ingest_drift_and_errors(_result_with_hidden())
        out = capsys.readouterr().out

        assert "Hidden (2)" in out
        assert "  - _dlt_loads: dlt" in out
        assert "  - alembic_version: alembic" in out

    def test_section_explains_both_escape_hatches(self, capsys) -> None:
        """`--surface-internals` alone is a half-truth on a re-ingest: it only
        affects models this run CREATES. A user staring at an already-hidden
        model needs to be pointed at `edit_model` instead."""
        _print_ingest_drift_and_errors(_result_with_hidden())
        out = capsys.readouterr().out

        assert "--surface-internals" in out
        assert "hidden=false" in out

    def test_nothing_printed_when_no_internals(self, capsys) -> None:
        _print_ingest_drift_and_errors(_result_with_hidden(hidden_internals=[]))
        assert "Hidden" not in capsys.readouterr().out

    def test_tolerates_a_result_lacking_the_attribute(self, capsys) -> None:
        """Matches the defensiveness `skipped` already gets — the renderer is
        called with duck-typed results from more than one code path."""
        legacy = SimpleNamespace(to_delete=[], errors=[], skipped=[])
        _print_ingest_drift_and_errors(legacy)
        assert "Hidden" not in capsys.readouterr().out

    def test_accepts_a_scan_report(self, workspace: Path, capsys) -> None:
        """`datasources create --ingest` renders an `IngestionScanReport`,
        which has no `to_delete` and no `errors` at all. The renderer reaches
        both directly today, so it must become defensive on every field or
        that path dies with an AttributeError that has nothing to do with
        classification."""
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
        """Hiding is the intended outcome. Unlike a skip — which is a
        capability failure with `--exclude` as its remedy — there is nothing
        for the user to fix, so nagging with exit 1 would be noise."""
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
        with pytest.raises(SystemExit) as exc:
            _run_ingest(_args(workspace))
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
    """The parser is built inline in ``main()``, so drive it through ``main()``
    with a stubbed handler that captures the parsed args."""

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
    """That path called ``ingest_datasource`` (models only), so it could not
    report anything. Left alone it would silently hide four models — and it
    was already silent about DEV-1741's skips."""

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
        """Switching this path to the report form closes the pre-existing
        DEV-1741 gap where it swallowed skips entirely."""
        from slayer.cli import _run_datasources_create

        db_path, _ = _ds(
            workspace,
            """
            CREATE TABLE a_b (id INTEGER PRIMARY KEY);
            CREATE TABLE a__b (id INTEGER PRIMARY KEY);
            """,
        )
        storage = YAMLStorage(base_dir=str(workspace / "storage"))

        _run_datasources_create(self._create_args(workspace, db_path), storage)

        out = capsys.readouterr().out
        assert "Skipped (1)" in out
        assert "a__b" in out

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
        """Adding the field to `IngestRequest` without wiring it into the
        route would satisfy every other REST assertion here."""
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
        """The 200 body is how a REST client learns what was hidden — there is
        no exit code or log for it to read. Hidden internals must never turn
        the response into a 422, matching the `skipped` contract."""
        db_path, _ = _ds(workspace, _MIXED)
        client = _api_client(workspace, db_path)

        resp = client.post("/ingest", json={"datasource": "ds"})
        assert resp.status_code == 200

        body = resp.json()
        assert {h["table_name"] for h in body["hidden_internals"]} == _INTERNAL_NAMES
        assert {h["tool"] for h in body["hidden_internals"]} == {"dlt", "alembic"}

    def test_re_ingest_with_flag_does_not_unhide(self, workspace: Path) -> None:
        """The creation-only rule holds over REST too, and the body must
        report the model as still hidden rather than going quiet."""
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
        """The issue's own acceptance criterion, exercised through the real
        MCP tool rather than by re-applying our own `hidden` filter."""
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
        """pg_facade and Flight both build through ``FacadeCatalog``, so one
        assertion covers both wire protocols."""
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
        """The whole reason this issue chose hidden over skipped: `_dlt_loads`
        answers "when did this last load?" when deliberately targeted."""
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
        """Hiding must not filter join targets. A silent drop here would not
        raise — it would return different rows, which is the worst failure
        mode available."""
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
        assert hidden is not None and hidden.hidden is True

        engine = SlayerQueryEngine(storage=storage)
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=["_dlt_loads.schema_name"],
                measures=[{"formula": "amount:sum", "name": "total"}],
            )
        )
        # Result keys are ``model.column``, and a joined dimension keeps its
        # full path — so the hidden model's name appearing in the key is itself
        # evidence the join resolved rather than being dropped.
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
        """`_scoped_models_for_validation` does not filter on `hidden`, so a
        hidden internal must still surface drift. If it silently fell out of
        scope, its persisted schema would rot unnoticed."""
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
