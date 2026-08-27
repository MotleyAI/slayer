"""Schema-qualified ingestion (DEV-1758), behavioral end-to-end on DuckDB.

DuckDB is the only exposed Tier-1 dialect whose ``Inspector`` sweeps every
schema for a ``schema=None`` listing, so it is where the regression is visible
and where the fix must hold. Real temp ``.duckdb`` files (unit-suite, not
integration — same pattern as ``test_cube_js_e2e_duckdb.py``).

Fixture DBs are module-scoped (built once); storage is per-test. The
attached-catalog battery works at the engine / scope / fallback level because a
SLayer datasource points at a single file and cannot ``ATTACH`` a second
catalog — the "current catalog only" guarantee is a property of scope
resolution over an inspector that happens to see attached catalogs.
"""
from __future__ import annotations

import pytest
import sqlalchemy as sa

duckdb = pytest.importorskip("duckdb")

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import (
    ingest_datasource_idempotent,
    ingest_datasource_report,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_scope import (
    SchemaEnumerationError,
    SchemaRef,
    resolve_ingest_scope,
)
from slayer.storage.yaml_storage import YAMLStorage


# ===========================================================================
# Fixtures
# ===========================================================================


def _duck(path: str, statements: list[str]) -> None:
    con = duckdb.connect(path)
    try:
        for stmt in statements:
            con.execute(stmt)
    finally:
        con.close()


@pytest.fixture(scope="module")
def basic_db(tmp_path_factory) -> str:
    """One default-schema table + one non-default-schema table (the repro)."""
    path = str(tmp_path_factory.mktemp("basic") / "fda.duckdb")
    _duck(path=path, statements=[
        "CREATE TABLE in_default(x INTEGER)",
        "INSERT INTO in_default VALUES (1)",
        "CREATE SCHEMA openfda_rest",
        "CREATE TABLE openfda_rest.reports(id INTEGER, val INTEGER)",
        "INSERT INTO openfda_rest.reports VALUES (1, 10), (2, 20)",
    ])
    return path


@pytest.fixture(scope="module")
def same_name_db(tmp_path_factory) -> str:
    """``reports`` in three schemas with DIFFERENT columns — the D2 fixture."""
    path = str(tmp_path_factory.mktemp("same_name") / "sn.duckdb")
    _duck(path=path, statements=[
        "CREATE TABLE reports(a INTEGER)",              # main (default)
        "CREATE SCHEMA s2",
        "CREATE TABLE s2.reports(b INTEGER, c INTEGER)",
        "CREATE SCHEMA s3",
        "CREATE TABLE s3.reports(d INTEGER)",
    ])
    return path


@pytest.fixture(scope="module")
def fk_db(tmp_path_factory) -> str:
    """Parent/child FK entirely inside a non-default schema."""
    path = str(tmp_path_factory.mktemp("fk") / "fk.duckdb")
    _duck(path=path, statements=[
        "CREATE SCHEMA ofr",
        "CREATE TABLE ofr.parent(id INTEGER PRIMARY KEY)",
        "CREATE TABLE ofr.child(id INTEGER PRIMARY KEY, "
        "parent_id INTEGER REFERENCES ofr.parent(id))",
    ])
    return path


@pytest.fixture(scope="module")
def sanitize_collision_db(tmp_path_factory) -> str:
    """``s1.a__b`` sanitises to model ``a_b``; ``s2.a_b`` is the exact name."""
    path = str(tmp_path_factory.mktemp("sanit") / "sc.duckdb")
    _duck(path=path, statements=[
        "CREATE SCHEMA s1",
        "CREATE TABLE s1.a__b(x INTEGER)",
        "CREATE SCHEMA s2",
        "CREATE TABLE s2.a_b(y INTEGER)",
    ])
    return path


@pytest.fixture(scope="module")
def loser_fk_db(tmp_path_factory) -> str:
    """A non-default table with an FK to a name-collision LOSER.

    ``main.x`` (default) wins model ``x``; ``s1.x`` loses. ``s1.y`` has an FK to
    ``s1.x`` — since the loser has no model, ``y``'s join must be dropped, and
    it must never resolve cross-schema to ``main.x``.
    """
    path = str(tmp_path_factory.mktemp("loserfk") / "lf.duckdb")
    _duck(path=path, statements=[
        "CREATE TABLE x(id INTEGER PRIMARY KEY)",         # main.x (winner)
        "CREATE SCHEMA s1",
        "CREATE TABLE s1.x(id INTEGER PRIMARY KEY)",      # loses model name 'x'
        "CREATE TABLE s1.y(id INTEGER PRIMARY KEY, "
        "x_id INTEGER REFERENCES s1.x(id))",
    ])
    return path


@pytest.fixture(scope="module")
def legacy_db(tmp_path_factory) -> str:
    """``orders`` in the default schema AND a non-default one (validate-models)."""
    path = str(tmp_path_factory.mktemp("legacy") / "lg.duckdb")
    _duck(path=path, statements=[
        "CREATE TABLE orders(id INTEGER)",                # main.orders
        "CREATE SCHEMA analytics",
        "CREATE TABLE analytics.orders(id INTEGER, note TEXT)",
    ])
    return path


@pytest.fixture(scope="module")
def comment_db(tmp_path_factory) -> str:
    """Non-default table carrying a table comment and a column comment."""
    path = str(tmp_path_factory.mktemp("comment") / "cm.duckdb")
    _duck(path=path, statements=[
        "CREATE SCHEMA ofr",
        "CREATE TABLE ofr.reports(id INTEGER, val INTEGER)",
        "COMMENT ON TABLE ofr.reports IS 'openfda report rows'",
        "COMMENT ON COLUMN ofr.reports.val IS 'the measured value'",
        # A same-named default-schema table with a DIFFERENT comment, to prove
        # the non-default comment is not read from the wrong schema.
        "CREATE TABLE reports(id INTEGER, val INTEGER)",
        "COMMENT ON COLUMN reports.val IS 'DEFAULT SCHEMA COMMENT'",
    ])
    return path


# --- attached-catalog engine (built at the SQLAlchemy level) ---------------


@pytest.fixture(scope="module")
def attached_paths(tmp_path_factory) -> tuple[str, str]:
    """Primary catalog ``att_main`` + a second catalog deliberately named
    ``aaa`` (sorts BEFORE ``att_main`` so 'default wins' can't be confused with
    'lowest sorted wins').

    Cross-catalog twins that make the catalog-safety guarantees observable:
    - ``main.shared`` exists in both catalogs with DIFFERENT columns (m vs o) →
      the column-union trap.
    - ``main.pktbl(id PK)`` exists in both → DuckDB gives both the same
      auto-generated PK constraint name, so a schema-only join duplicates it.
    - ``main.commented(v)`` exists in both with DIFFERENT comments on ``v`` →
      the DEV-1809 cross-catalog comment-bleed trap.
    - ``only_in_other`` lives ONLY in ``aaa`` → a bare-token retry from
      ``att_main`` would wrongly find it.
    """
    d = tmp_path_factory.mktemp("attached")
    main_path = str(d / "att_main.duckdb")
    aaa_path = str(d / "aaa.duckdb")
    _duck(path=main_path, statements=[
        "CREATE TABLE in_default(x INTEGER)",
        "CREATE TABLE shared(m INTEGER)",
        "CREATE TABLE pktbl(id INTEGER PRIMARY KEY)",
        "CREATE TABLE commented(v INTEGER)",
        "COMMENT ON COLUMN commented.v IS 'att_main comment'",
        "CREATE SCHEMA openfda_rest",
        "CREATE TABLE openfda_rest.reports(id INTEGER PRIMARY KEY, val INTEGER)",
    ])
    _duck(path=aaa_path, statements=[
        "CREATE TABLE only_in_other(y INTEGER)",
        "CREATE TABLE shared(o INTEGER)",
        "CREATE TABLE pktbl(id INTEGER PRIMARY KEY)",
        "CREATE TABLE commented(v INTEGER)",
        "COMMENT ON COLUMN commented.v IS 'aaa comment'",
    ])
    return main_path, aaa_path


def _open_attached(paths: tuple[str, str]) -> sa.Engine:
    main_path, aaa_path = paths
    engine = sa.create_engine(f"duckdb:///{main_path}")

    @sa.event.listens_for(engine, "connect")
    def _attach(dbapi_conn, _rec):  # noqa: ANN001
        dbapi_conn.execute(f"ATTACH IF NOT EXISTS '{aaa_path}' AS aaa")

    return engine


# --- shared helpers --------------------------------------------------------


def _ds(path: str, **kw) -> DatasourceConfig:
    return DatasourceConfig(name="ds", type="duckdb", database=path, **kw)


def _sql_tables(report) -> dict[str, str]:
    return {m.name: m.sql_table for m in report.models}


def _skip_labels(report) -> list[str]:
    """Every skip label, as a LIST — same-labelled losers must not collapse."""
    return [s.table_name for s in report.skipped]


async def _query_count(storage: YAMLStorage, model_name: str) -> int:
    engine = SlayerQueryEngine(storage=storage)
    resp = await engine.execute(
        SlayerQuery(source_model=model_name,
                    measures=[{"formula": "*:count", "name": "n"}])
    )
    assert resp.row_count == 1, resp.data
    return next(iter(resp.data[0].values()))


# ===========================================================================
# The reported regression, end to end through the typed pipeline
# ===========================================================================


class TestReportedRegression:
    def test_bare_ingest_covers_the_default_schema_only(self, basic_db) -> None:
        """D-1: bare ingest models only the connection default; the
        non-default ``reports`` is NOT created, and the scope names it as a
        schema the user could opt into."""
        report = ingest_datasource_report(datasource=_ds(basic_db))
        assert _sql_tables(report) == {"in_default": "in_default"}

    async def test_explicit_schema_qualifies_and_is_queryable(self, basic_db, tmp_path) -> None:
        """The issue's repro: ``--schema openfda_rest`` yields a qualified
        ``sql_table`` and a real ``*:count`` returns rows (was: table-not-found)."""
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)

        report = ingest_datasource_report(datasource=ds, schemas=["openfda_rest"])
        assert _sql_tables(report)["reports"] == "openfda_rest.reports"

        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["openfda_rest"]
        )
        assert await _query_count(storage, "reports") == 2

    async def test_all_schemas_makes_both_queryable(self, basic_db, tmp_path) -> None:
        """``--all-schemas``: default stays bare, non-default is qualified, both
        resolve at query time."""
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, all_schemas=True
        )
        in_default = await storage.get_model("in_default", data_source="ds")
        reports = await storage.get_model("reports", data_source="ds")
        assert in_default.sql_table == "in_default"
        assert reports.sql_table == "openfda_rest.reports"
        assert await _query_count(storage, "in_default") == 1
        assert await _query_count(storage, "reports") == 2


# ===========================================================================
# D2 — column corruption across same-named tables
# ===========================================================================


class TestColumnCorruption:
    def test_bare_ingest_never_unions_same_named_columns(self, same_name_db) -> None:
        """The default ``reports`` must carry exactly its own column, never the
        union of every schema's ``reports``."""
        report = ingest_datasource_report(datasource=_ds(same_name_db))
        reports = next(m for m in report.models if m.name == "reports")
        assert {c.name for c in reports.columns} == {"a"}

    def test_explicit_non_default_gets_its_own_columns(self, same_name_db) -> None:
        report = ingest_datasource_report(datasource=_ds(same_name_db), schemas=["s2"])
        reports = next(m for m in report.models if m.name == "reports")
        assert {c.name for c in reports.columns} == {"b", "c"}
        assert reports.sql_table == "s2.reports"


# ===========================================================================
# Attached catalogs — current catalog only, never union, never empty
# ===========================================================================


class TestAttachedCatalogs:
    def test_get_schema_names_are_catalog_qualified(self, attached_paths) -> None:
        """Sanity: the fixture actually attached, and DuckDB qualifies tokens."""
        engine = _open_attached(attached_paths)
        try:
            names = set(sa.inspect(engine).get_schema_names())
            assert {"att_main.main", "aaa.main", "att_main.openfda_rest"} <= names
        finally:
            engine.dispose()

    def test_all_schemas_covers_only_the_current_catalog(self, attached_paths) -> None:
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            scope = resolve_ingest_scope(
                inspector=insp, sa_engine=engine, requested=None,
                all_schemas=True, datasource_schema=None,
            )
            names = {r.name for r in scope.schemas}
            assert "main" in names
            assert "openfda_rest" in names
            assert all(r.catalog == "att_main" for r in scope.schemas)
            # aaa is a foreign catalog → reported as skipped, never silent, with
            # an actionable reason that names the catalog and the fix.
            aaa_skip = next(s for s in scope.skipped if s.token == "aaa.main")
            assert "catalog" in aaa_skip.reason.lower()
            assert "aaa" in aaa_skip.reason           # names the offending catalog
        finally:
            engine.dispose()

    def test_bare_scope_does_not_reach_into_the_attached_catalog(self, attached_paths) -> None:
        """The default ref must be the QUALIFIED default token, so discovery
        under it does not sweep ``aaa``."""
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            scope = resolve_ingest_scope(
                inspector=insp, sa_engine=engine, requested=None,
                all_schemas=False, datasource_schema=None,
            )
            assert len(scope.schemas) == 1
            assert scope.schemas[0].token == "att_main.main"
            assert scope.schemas[0].is_default is True
        finally:
            engine.dispose()

    def test_column_fallback_accepts_a_qualified_token(self, attached_paths) -> None:
        from slayer.engine.introspect_utils import _get_columns_fallback
        engine = _open_attached(attached_paths)
        try:
            ref = SchemaRef(catalog="att_main", name="openfda_rest")
            cols = {c["name"] for c in _get_columns_fallback(engine, "reports", ref)}
            assert cols == {"id", "val"}
        finally:
            engine.dispose()

    def test_column_fallback_never_unions_across_catalogs(self, attached_paths) -> None:
        """``shared`` lives in both catalogs; the qualified token yields only the
        current catalog's column, never the ``['m','o']`` union."""
        from slayer.engine.introspect_utils import _get_columns_fallback
        engine = _open_attached(attached_paths)
        try:
            ref = SchemaRef(catalog="att_main", name="main")
            cols = {c["name"] for c in _get_columns_fallback(engine, "shared", ref)}
            assert cols == {"m"}
        finally:
            engine.dispose()

    def test_primary_key_survives_a_qualified_token(self, attached_paths) -> None:
        from slayer.engine.ingestion import _get_pk_constraint_fallback
        engine = _open_attached(attached_paths)
        try:
            ref = SchemaRef(catalog="att_main", name="openfda_rest")
            pk = _get_pk_constraint_fallback(engine, "reports", ref)
            assert pk["constrained_columns"] == ["id"]
        finally:
            engine.dispose()

    def test_qualified_token_never_retries_bare(self, attached_paths) -> None:
        """The strongest no-bare-retry proof: ``only_in_other`` exists ONLY in
        ``aaa``. A correct qualified probe of ``att_main.main`` finds nothing; a
        bare retry would wrongly reach into the attached catalog and return
        ``[y]``. The result MUST be empty (§3.2b: never retry bare)."""
        from slayer.engine.introspect_utils import _get_columns_fallback
        engine = _open_attached(attached_paths)
        try:
            ref = SchemaRef(catalog="att_main", name="main")
            cols = [c["name"] for c in _get_columns_fallback(engine, "only_in_other", ref)]
            assert cols == []
        finally:
            engine.dispose()

    def test_pk_fallback_joins_on_the_catalog_too(self, attached_paths) -> None:
        """``pktbl(id PK)`` exists in BOTH catalogs' ``main`` and DuckDB gives
        both the SAME auto-generated constraint name. A schema-only join
        duplicates the PK column (``['id','id']``); the three-way catalog join
        keeps it to the current catalog's single ``id``."""
        from slayer.engine.ingestion import _get_pk_constraint_fallback
        engine = _open_attached(attached_paths)
        try:
            ref = SchemaRef(catalog="att_main", name="main")
            pk = _get_pk_constraint_fallback(engine, "pktbl", ref)
            assert pk["constrained_columns"] == ["id"]
        finally:
            engine.dispose()

    def test_comment_fallback_does_not_bleed_across_catalogs(self, attached_paths) -> None:
        """DEV-1809: ``commented.v`` carries a DIFFERENT comment in each catalog.
        The qualified token must read only the current catalog's comment (needs
        ``duckdb_columns().database_name = :catalog``)."""
        from slayer.engine.introspect_utils import _get_column_comments_fallback
        engine = _open_attached(attached_paths)
        try:
            ref = SchemaRef(catalog="att_main", name="main")
            comments = _get_column_comments_fallback(engine, "commented", ref)
            assert comments.get("v") == "att_main comment"
        finally:
            engine.dispose()

    def test_column_fallback_ref_none_never_unions(self, attached_paths) -> None:
        """Defensive branch: with ``ref=None`` and ``shared`` present in several
        (catalog, schema) groups, the fallback must NOT silently union. It may
        resolve to the default group ({m}) or refuse (ValueError) — never
        ``{m, o}`` (§3.10, lowest-sorted-wins explicitly rejected)."""
        from slayer.engine.introspect_utils import _get_columns_fallback
        engine = _open_attached(attached_paths)
        try:
            try:
                cols = {c["name"] for c in _get_columns_fallback(engine, "shared", None)}
            except ValueError:
                return                 # refusing to guess is acceptable
            assert cols == {"m"}       # if it resolves, the default group; never the union
        finally:
            engine.dispose()

    def test_discovery_ref_none_does_not_sweep_the_attached_catalog(
        self, attached_paths
    ) -> None:
        """``list_ingestable_objects(ref=None)`` resolves None to the QUALIFIED
        default ref before listing, so it never returns ``aaa``'s objects."""
        from slayer.engine.ingestion import list_ingestable_objects
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            names = {o.name for o in list_ingestable_objects(inspector=insp, ref=None)}
            assert "only_in_other" not in names
            assert "in_default" in names
        finally:
            engine.dispose()

    def test_introspect_table_schema_none_resolves_the_qualified_default(
        self, attached_paths
    ) -> None:
        """schema=None probes the qualified default: {m} not {m,o}, bare sql_table."""
        from slayer.engine.ingestion import introspect_table_to_model
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            model = introspect_table_to_model(
                sa_engine=engine, inspector=insp, table_name="shared",
                schema=None, data_source="ds",
            )
            assert {c.name for c in model.columns} == {"m"}
            assert model.sql_table == "shared"      # default stays bare
        finally:
            engine.dispose()

    def test_introspect_table_schema_none_never_unions_a_bare_result(
        self, attached_paths, monkeypatch
    ) -> None:
        """Even when the Inspector RETURNS a bare-schema union, schema=None probes
        the qualified default and keeps only {m}."""
        from slayer.engine.ingestion import introspect_table_to_model
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            real_get_columns = insp.get_columns

            def _bare_returns_union(table_name, schema=None, **kw):
                if schema is None:
                    # Simulate an Inspector that sweeps every attached catalog for
                    # a bare request and unions the twins.
                    return [
                        {"name": "m", "type": sa.INTEGER()},
                        {"name": "o", "type": sa.INTEGER()},
                    ]
                return real_get_columns(table_name, schema=schema, **kw)

            monkeypatch.setattr(insp, "get_columns", _bare_returns_union)
            model = introspect_table_to_model(
                sa_engine=engine, inspector=insp, table_name="shared",
                schema=None, data_source="ds",
            )
            assert {c.name for c in model.columns} == {"m"}
        finally:
            engine.dispose()

    def test_requested_foreign_catalog_is_skipped_not_ingested(self, attached_paths) -> None:
        """Codex-review: explicitly requesting a foreign attached catalog
        (``--schema aaa.main``) drops it to ``skipped`` and out of scope, like
        the all_schemas enumeration filter — never ingesting a catalog this
        datasource does not own."""
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            scope = resolve_ingest_scope(
                inspector=insp, sa_engine=engine, requested=["aaa.main"],
                all_schemas=False, datasource_schema=None,
            )
            assert scope.schemas == []
            assert any(s.token == "aaa.main" for s in scope.skipped)
        finally:
            engine.dispose()


# ===========================================================================
# Scope resolution — precedence, dedup, requested-discipline
# ===========================================================================


class TestScopeResolution:
    def _scope(self, path, **kw):
        ds = _ds(path)
        from slayer.sql import engine_factory
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        insp = sa.inspect(engine)
        defaults = dict(requested=None, all_schemas=False, datasource_schema=None)
        defaults.update(kw)
        return resolve_ingest_scope(inspector=insp, sa_engine=engine, **defaults)

    def test_bare_resolves_to_the_default_and_hints_the_rest(self, basic_db) -> None:
        scope = self._scope(basic_db)
        assert [r.name for r in scope.schemas] == ["main"]
        assert scope.schemas[0].is_default is True
        assert "openfda_rest" in scope.other_schemas

    def test_persisted_schema_name_is_explicit_and_verbatim(self, basic_db) -> None:
        scope = self._scope(basic_db, datasource_schema="main")
        (ref,) = scope.schemas
        assert ref.explicit is True
        assert ref.qualify("t") == "main.t"

    def test_multi_request_is_not_verbatim(self, basic_db) -> None:
        scope = self._scope(basic_db, requested=["main", "openfda_rest"])
        by_name = {r.name: r for r in scope.schemas}
        assert by_name["main"].explicit is False
        assert by_name["main"].qualify("t") == "t"               # default bare
        assert by_name["openfda_rest"].qualify("t") == "openfda_rest.t"

    def test_single_request_is_verbatim(self, basic_db) -> None:
        scope = self._scope(basic_db, requested=["openfda_rest"])
        (ref,) = scope.schemas
        assert ref.explicit is True
        assert ref.qualify("t") == "openfda_rest.t"

    def test_resolved_tokens_are_deduplicated(self, basic_db) -> None:
        """``main`` given twice (or once bare + once as its default) collapses to
        a single scope entry — the double-scan that marked models for deletion."""
        scope = self._scope(basic_db, requested=["main", "main"])
        assert len(scope.schemas) == 1

    def test_multi_request_reports_no_hint(self, basic_db) -> None:
        scope = self._scope(basic_db, requested=["main", "openfda_rest"])
        assert scope.other_schemas == []

    def test_requested_system_schema_is_dropped(self, basic_db) -> None:
        """An explicitly requested system schema is dropped, not scanned."""
        scope = self._scope(basic_db, requested=["information_schema"])
        assert scope.schemas == []
        assert any("information_schema" in (s.token or "") for s in scope.skipped)

    def test_persisted_system_schema_is_dropped(self, basic_db) -> None:
        """A persisted system schema_name is dropped too (datasource branch)."""
        scope = self._scope(basic_db, datasource_schema="pg_catalog")
        assert scope.schemas == []
        assert any("pg_catalog" in (s.token or "") for s in scope.skipped)

    def test_all_schemas_raises_when_enumeration_fails(
        self, basic_db, monkeypatch
    ) -> None:
        """--all-schemas raises rather than returning a silent empty scope."""
        ds = _ds(basic_db)
        from slayer.sql import engine_factory
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        insp = sa.inspect(engine)

        def _boom() -> list[str]:
            raise RuntimeError("metadata permission revoked")

        monkeypatch.setattr(insp, "get_schema_names", _boom)
        with pytest.raises(SchemaEnumerationError):
            resolve_ingest_scope(
                inspector=insp, sa_engine=engine, requested=None,
                all_schemas=True, datasource_schema=None,
            )

    def test_default_scope_survives_enumeration_failure(
        self, basic_db, monkeypatch
    ) -> None:
        """The implicit default request still resolves when enumeration fails."""
        ds = _ds(basic_db)
        from slayer.sql import engine_factory
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        insp = sa.inspect(engine)

        def _boom() -> list[str]:
            raise RuntimeError("metadata permission revoked")

        monkeypatch.setattr(insp, "get_schema_names", _boom)
        scope = resolve_ingest_scope(
            inspector=insp, sa_engine=engine, requested=None,
            all_schemas=False, datasource_schema=None,
        )
        assert len(scope.schemas) == 1
        assert scope.schemas[0].is_default is True


class TestSkippedSchemasReported:
    """DEV-1758 (Codex): a requested schema dropped from scope (system /
    foreign catalog) is surfaced in the report, not a silent empty ingest."""

    def test_report_surfaces_a_requested_system_schema(self, basic_db) -> None:
        report = ingest_datasource_report(
            datasource=_ds(basic_db), schemas=["information_schema"]
        )
        assert report.models == []
        assert any(
            "information_schema" in s.token for s in report.skipped_schemas
        )


# ===========================================================================
# Collisions — one deterministic phase, winners-only structures
# ===========================================================================


def _reversed_scan(monkeypatch):
    """Reverse the per-schema discovery order to prove order-independence."""
    from slayer.engine import ingestion as mod
    real = mod.list_ingestable_objects

    def _rev(**kwargs):
        return list(reversed(real(**kwargs)))

    monkeypatch.setattr(mod, "list_ingestable_objects", _rev)


class TestCollisions:
    def test_default_schema_wins_and_every_loser_is_skipped(self, same_name_db) -> None:
        """main wins; BOTH s2 and s3 losers are reported — and under a
        multi-schema scan the labels are fully disambiguated (§3.5)."""
        report = ingest_datasource_report(datasource=_ds(same_name_db), all_schemas=True)
        reports = [m for m in report.models if m.name == "reports"]
        assert len(reports) == 1
        assert {c.name for c in reports[0].columns} == {"a"}      # main won
        labels = _skip_labels(report)
        assert "s2.reports" in labels                              # qualified label
        assert "s3.reports" in labels                             # BOTH losers, no collapse

    def test_non_default_collision_picks_the_lower_schema(self, same_name_db) -> None:
        report = ingest_datasource_report(datasource=_ds(same_name_db), schemas=["s2", "s3"])
        reports = next(m for m in report.models if m.name == "reports")
        assert reports.sql_table == "s2.reports"                  # s2 < s3
        assert "s3.reports" in _skip_labels(report)

    def test_collision_winner_is_independent_of_discovery_order(
        self, same_name_db, monkeypatch
    ) -> None:
        """Reverse ONLY the discovery order (requested order held fixed) so a
        failure isolates to discovery nondeterminism."""
        forward = ingest_datasource_report(datasource=_ds(same_name_db), schemas=["s2", "s3"])
        _reversed_scan(monkeypatch)
        backward = ingest_datasource_report(datasource=_ds(same_name_db), schemas=["s2", "s3"])
        assert _sql_tables(forward)["reports"] == _sql_tables(backward)["reports"]
        # Direct list comparison: the skip labels must be emitted in the same
        # deterministic order regardless of discovery order (the property
        # ``_collision_sort_key`` guarantees).
        assert _skip_labels(forward) == _skip_labels(backward)

    def test_collision_winner_is_independent_of_requested_order(self, same_name_db) -> None:
        forward = ingest_datasource_report(datasource=_ds(same_name_db), schemas=["s2", "s3"])
        backward = ingest_datasource_report(datasource=_ds(same_name_db), schemas=["s3", "s2"])
        assert _sql_tables(forward)["reports"] == _sql_tables(backward)["reports"]

    def test_faithful_and_sanitized_names_do_not_collide_across_schemas(
        self, sanitize_collision_db
    ) -> None:
        """DEV-1743: ``s1.a__b`` keeps its faithful name ``a__b``; ``s2.a_b`` is
        the distinct ``a_b`` — the old sanitize-collapse collision is gone, so
        both are modelled and neither is skipped."""
        report = ingest_datasource_report(
            datasource=_ds(sanitize_collision_db), all_schemas=True
        )
        tables = _sql_tables(report)
        assert tables["a__b"] == "s1.a__b"
        assert tables["a_b"] == "s2.a_b"
        assert not any("a__b" in lbl for lbl in _skip_labels(report))

    def test_single_schema_output_is_unqualified(self, same_name_db) -> None:
        """Bare ingest of one schema keeps ``sql_table`` and skip labels bare —
        no cross-schema disambiguation when there is nothing to disambiguate."""
        report = ingest_datasource_report(datasource=_ds(same_name_db))
        assert _sql_tables(report) == {"reports": "reports"}
        assert report.skipped == []

    def test_faithful_and_sanitized_names_are_distinct_in_one_schema(
        self, tmp_path
    ) -> None:
        """DEV-1743: within one schema ``a_b`` and ``a__b`` are distinct models —
        no collision, nothing skipped."""
        path = str(tmp_path / "one.duckdb")
        _duck(path=path, statements=[
            "CREATE TABLE a_b(real_col INTEGER)",
            "CREATE TABLE a__b(x INTEGER)",
        ])
        report = ingest_datasource_report(datasource=_ds(path))
        assert {"a_b", "a__b"} <= {m.name for m in report.models}
        assert _skip_labels(report) == []


class TestCollisionTieBreaks:
    def test_distinct_dunder_names_in_one_schema_do_not_tie(self, tmp_path) -> None:
        """DEV-1743: ``s2.a__b`` and ``s2.a___b`` are different faithful models —
        no sanitize-collapse tie, both modelled, nothing skipped."""
        path = str(tmp_path / "tie.duckdb")
        _duck(path=path, statements=[
            "CREATE SCHEMA s2",
            "CREATE TABLE s2.a__b(x INTEGER)",
            "CREATE TABLE s2.a___b(y INTEGER)",
        ])
        report = ingest_datasource_report(datasource=_ds(path), schemas=["s2"])
        tables = _sql_tables(report)
        assert tables["a__b"] == "s2.a__b"
        assert tables["a___b"] == "s2.a___b"
        assert _skip_labels(report) == []


class TestWinnersOnlyStructures:
    def test_join_to_a_collision_loser_is_dropped(self, loser_fk_db) -> None:
        """``s1.y``'s FK targets ``s1.x`` which lost the model name to
        ``main.x``; the join must be dropped, never repointed cross-schema."""
        report = ingest_datasource_report(datasource=_ds(loser_fk_db), all_schemas=True)
        models = {m.name: m for m in report.models}
        y = models["y"]
        assert y.joins == []


# ===========================================================================
# Foreign keys in a non-default schema
# ===========================================================================


class TestForeignKeysAreSchemaAware:
    def test_join_is_generated_for_a_non_default_schema(self, fk_db) -> None:
        report = ingest_datasource_report(datasource=_ds(fk_db), schemas=["ofr"])
        models = {m.name: m for m in report.models}
        child = models["child"]
        assert child.sql_table == "ofr.child"
        assert [j.target_model for j in child.joins] == ["parent"]
        assert child.joins[0].join_pairs == [["parent_id", "id"]]


# ===========================================================================
# Self-heal (D-3): add a missing qualifier, never rewrite one
# ===========================================================================


class TestSelfHeal:
    async def _persist_bare(self, storage, ds, columns, *, extra=None):
        model = SlayerModel(
            name="reports", sql_table="reports", data_source="ds",
            columns=columns,
        )
        if extra:
            model = model.model_copy(update=extra)
        await storage.save_datasource(ds)
        await storage.save_model(model)

    async def test_missing_qualifier_is_healed(self, basic_db, tmp_path) -> None:
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        # Pretend an older run stored ``reports`` unqualified with hand metadata.
        col = Column(name="id", sql="id", type=DataType.INT, description="hand id")
        await self._persist_bare(storage, ds, [col],
                                 extra={"description": "hand table"})
        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["openfda_rest"]
        )
        healed = await storage.get_model("reports", data_source="ds")
        assert healed.sql_table == "openfda_rest.reports"
        # Hand-authored metadata survives the heal.
        assert healed.description == "hand table"
        assert next(c for c in healed.columns if c.name == "id").description == "hand id"
        # The change is reported.
        assert any(getattr(a, "sql_table_change", None) == "reports → openfda_rest.reports"
                   for a in result.additions)

    async def test_existing_qualifier_is_never_rewritten(self, basic_db, tmp_path) -> None:
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        model = SlayerModel(
            name="reports", sql_table="prod.reports", data_source="ds",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        await storage.save_datasource(ds)
        await storage.save_model(model)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["openfda_rest"]
        )
        kept = await storage.get_model("reports", data_source="ds")
        assert kept.sql_table == "prod.reports"

    async def test_qualifier_repair_alone_triggers_a_save(self, basic_db, tmp_path) -> None:
        """Isolate the short-circuit: persist a model whose columns/description
        already match the fresh introspection, so the ONLY difference is the
        missing qualifier. It must still be saved and reported — an
        implementation that ignores ``sql_table_change`` in the save gate fails."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        await storage.save_datasource(ds)
        # openfda_rest.reports is (id INT, val INT); mirror it exactly but bare.
        await storage.save_model(SlayerModel(
            name="reports", sql_table="reports", data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.INT,
                       format={"type": "integer"}),
                Column(name="val", sql="val", type=DataType.INT,
                       format={"type": "integer"}),
            ],
        ))
        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["openfda_rest"]
        )
        healed = await storage.get_model("reports", data_source="ds")
        assert healed.sql_table == "openfda_rest.reports"        # saved
        assert any(getattr(a, "sql_table_change", None)
                   == "reports → openfda_rest.reports"
                   for a in result.additions)                    # reported

    async def test_default_schema_qualified_twin_is_a_repair_not_a_conflict(
        self, same_name_db, tmp_path
    ) -> None:
        """A fresh DEFAULT-schema-qualified ``main.reports`` (from explicit
        ``--schema main``) heals a persisted bare ``reports`` rather than being
        treated as a cross-schema conflict — main IS the default (§3.6)."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(same_name_db)
        await storage.save_datasource(ds)
        await storage.save_model(SlayerModel(
            name="reports", sql_table="reports", data_source="ds",
            columns=[Column(name="a", sql="a", type=DataType.INT)],
        ))
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["main"],   # verbatim → main.reports
        )
        healed = await storage.get_model("reports", data_source="ds")
        assert healed.sql_table == "main.reports"
        assert {c.name for c in healed.columns} == {"a"}         # still main's table


class TestAdditionRendering:
    def test_updated_line_names_the_qualifier_repair(self) -> None:
        """A qualifier repair adds no columns, so the ``Updated:`` line must be
        driven by ``sql_table_change`` alone (§3.7 rendering)."""
        import io
        from slayer.engine.ingestion import _print_ingest_addition
        from slayer.engine.schema_drift import ModelAddition
        addition = ModelAddition(
            model_name="reports", data_source="ds", created=False,
            sql_table_change="reports → openfda_rest.reports",
        )
        buf = io.StringIO()
        _print_ingest_addition(addition, file=buf)
        out = buf.getvalue()
        assert "reports → openfda_rest.reports" in out


# ===========================================================================
# Cross-schema merge guard
# ===========================================================================


class TestCrossSchemaMergeGuard:
    async def test_bare_default_model_not_repointed_by_a_non_default_twin(
        self, same_name_db, tmp_path
    ) -> None:
        """Persisted bare ``reports`` (main) must not merge/heal with
        ``s2.reports`` — different physical table."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(same_name_db)
        await storage.save_datasource(ds)
        await storage.save_model(SlayerModel(
            name="reports", sql_table="reports", data_source="ds",
            columns=[Column(name="a", sql="a", type=DataType.INT)],
        ))
        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["s2"]
        )
        kept = await storage.get_model("reports", data_source="ds")
        assert kept.sql_table == "reports"
        assert {c.name for c in kept.columns} == {"a"}
        assert any("reports" in s.table_name for s in result.skipped)

    async def test_guard_fails_closed_when_default_membership_unknown(
        self, same_name_db, tmp_path, monkeypatch
    ) -> None:
        """If the default-schema object listing fails, membership is UNKNOWN and
        the merge is refused (never repoint on a guess)."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        from slayer.engine import ingestion as mod

        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(same_name_db)
        await storage.save_datasource(ds)
        await storage.save_model(SlayerModel(
            name="reports", sql_table="reports", data_source="ds",
            columns=[Column(name="a", sql="a", type=DataType.INT)],
        ))

        real = mod.list_ingestable_objects

        def _maybe_raise(**kwargs):
            ref = kwargs.get("ref")
            if ref is not None and getattr(ref, "is_default", False):
                raise sa.exc.OperationalError("boom", {}, Exception())
            return real(**kwargs)

        monkeypatch.setattr(mod, "list_ingestable_objects", _maybe_raise)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schemas=["s2"]
        )
        kept = await storage.get_model("reports", data_source="ds")
        assert kept.sql_table == "reports"
        assert {c.name for c in kept.columns} == {"a"}


# ===========================================================================
# validate-models — the data-loss guard (D-6)
# ===========================================================================


class TestValidateModels:
    async def _validate(self, ds, models):
        from slayer.engine.schema_drift import validate_datasource
        return await validate_datasource(datasource=ds, models=models)

    async def test_no_whole_model_delete_after_multi_schema_ingest(
        self, basic_db, tmp_path
    ) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, all_schemas=True
        )
        models = [
            await storage.get_model("in_default", data_source="ds"),
            await storage.get_model("reports", data_source="ds"),
        ]
        to_delete = await self._validate(ds, models)
        assert not [e for e in to_delete if e.tool == "delete_model"]

    async def test_legacy_bare_model_survives_a_new_same_named_schema(
        self, legacy_db, tmp_path
    ) -> None:
        """A pre-existing unqualified ``orders`` must still resolve to the
        default-schema live table even though ``analytics.orders`` now exists —
        the contested bare alias resolves the way the database would."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(legacy_db)
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        await storage.save_model(model)
        to_delete = await self._validate(ds, [model])
        assert not [e for e in to_delete if e.tool == "delete_model"]

    async def test_missing_non_default_table_is_reported_deleted_not_masked(
        self, tmp_path
    ) -> None:
        """Codex-review regression: a model qualified to a non-default table
        that no longer exists must be reported for deletion even when a
        same-named table exists in the DEFAULT schema. The pre-fix
        ``_resolve_live_table`` last-one fallback silently diffed against the
        default twin (``analytics.orders`` → ``orders``) and hid the drop."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        path = str(tmp_path / "drop.duckdb")
        _duck(path=path, statements=["CREATE TABLE orders(id INTEGER)"])   # default only; no analytics
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(path)
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="orders", sql_table="analytics.orders", data_source="ds",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        await storage.save_model(model)
        to_delete = await self._validate(ds, [model])
        assert [e for e in to_delete if e.tool == "delete_model"]

    async def test_failed_schema_listing_does_not_mass_delete(
        self, basic_db, tmp_path, monkeypatch
    ) -> None:
        """CodeRabbit: if one in-scope schema fails to list, validate must NOT
        report that schema's models for deletion — a transient error would hand
        ``--force-clean`` the whole schema. The partial live map is refused
        (IntrospectionUnavailable), so no drift verdict is produced."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        from slayer.engine import ingestion as mod
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="reports", sql_table="openfda_rest.reports", data_source="ds",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        await storage.save_model(model)
        real = mod.list_ingestable_objects

        def _maybe_raise(**kwargs):
            ref = kwargs.get("ref")
            if ref is not None and getattr(ref, "name", None) == "openfda_rest":
                raise sa.exc.OperationalError("boom", {}, Exception())
            return real(**kwargs)

        monkeypatch.setattr(mod, "list_ingestable_objects", _maybe_raise)
        to_delete = await self._validate(ds, [model])
        assert not [e for e in to_delete if e.tool == "delete_model"]

    async def test_qualified_model_diffs_against_the_correct_twin(
        self, same_name_db, tmp_path
    ) -> None:
        """A model qualified to ``s2.reports`` (cols b,c) must diff against that
        table, not ``main.reports`` (col a) — else it reads every column as
        dropped and whole-deletes."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(same_name_db)
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="reports", sql_table="s2.reports", data_source="ds",
            columns=[
                Column(name="b", sql="b", type=DataType.INT),
                Column(name="c", sql="c", type=DataType.INT),
            ],
        )
        await storage.save_model(model)
        to_delete = await self._validate(ds, [model])
        assert not [e for e in to_delete if e.tool == "delete_model"]

    async def test_pinned_schema_name_still_covers_other_own_schemas(
        self, legacy_db, tmp_path
    ) -> None:
        """A model qualified to a non-pinned own schema isn't false-deleted."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(legacy_db, schema_name="main")
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="analytics_orders", sql_table="analytics.orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.INT),
                Column(name="note", sql="note", type=DataType.TEXT),
            ],
        )
        await storage.save_model(model)
        to_delete = await self._validate(ds, [model])
        assert not [e for e in to_delete if e.tool == "delete_model"]

    async def test_validate_falls_back_when_enumeration_fails(
        self, legacy_db, tmp_path, monkeypatch
    ) -> None:
        """Enumeration fails, yet a pinned-schema model still gets a real drift
        verdict via the configured-schema fallback, not a silent skip."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        from slayer.engine import schema_drift as drift_mod
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(legacy_db, schema_name="main")
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.INT),
                Column(name="ghost", sql="ghost", type=DataType.INT),  # not live
            ],
        )
        await storage.save_model(model)

        real = drift_mod.resolve_ingest_scope

        def _no_enumerate(**kwargs):
            if kwargs.get("all_schemas"):
                raise SchemaEnumerationError("simulated least-privilege connection")
            return real(**kwargs)

        monkeypatch.setattr(drift_mod, "resolve_ingest_scope", _no_enumerate)
        to_delete = await self._validate(ds, [model])
        assert to_delete                                              # verdict, not a skip
        assert not [e for e in to_delete if e.tool == "delete_model"]  # table resolved

    async def test_fallback_covers_a_non_pinned_model_schema(
        self, legacy_db, tmp_path, monkeypatch
    ) -> None:
        """Enumeration fails, but a model qualified to a NON-pinned own schema
        still resolves — the fallback scopes to the models' own schemas, so it is
        not false-deleted."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        from slayer.engine import schema_drift as drift_mod
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(legacy_db, schema_name="main")
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="analytics_orders", sql_table="analytics.orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.INT),
                Column(name="note", sql="note", type=DataType.TEXT),
            ],
        )
        await storage.save_model(model)

        real = drift_mod.resolve_ingest_scope

        def _no_enumerate(**kwargs):
            if kwargs.get("all_schemas"):
                raise SchemaEnumerationError("simulated least-privilege connection")
            return real(**kwargs)

        monkeypatch.setattr(drift_mod, "resolve_ingest_scope", _no_enumerate)
        to_delete = await self._validate(ds, [model])
        assert not [e for e in to_delete if e.tool == "delete_model"]

    async def test_fallback_default_stays_bare_keyed_past_catalog_token(
        self, tmp_path, monkeypatch
    ) -> None:
        """Fallback: a model token equal to the catalog-qualified default
        (``cat.main.x``) must not un-bare-key the default, or a bare model
        alongside another schema gets false-deleted."""
        from slayer.core.models import Column
        from slayer.core.enums import DataType
        from slayer.engine import schema_drift as drift_mod
        path = str(tmp_path / "cat.duckdb")           # catalog "cat", default "main"
        _duck(path=path, statements=[
            "CREATE TABLE orders(id INTEGER)",
            "CREATE TABLE customers(id INTEGER)",
            "CREATE SCHEMA analytics",
            "CREATE TABLE analytics.foo(id INTEGER)",
        ])
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(path)
        await storage.save_datasource(ds)
        models = [
            SlayerModel(name="orders", sql_table="orders", data_source="ds",
                        columns=[Column(name="id", sql="id", type=DataType.INT)]),
            SlayerModel(name="customers", sql_table="cat.main.customers",
                        data_source="ds",
                        columns=[Column(name="id", sql="id", type=DataType.INT)]),
            SlayerModel(name="foo", sql_table="analytics.foo", data_source="ds",
                        columns=[Column(name="id", sql="id", type=DataType.INT)]),
        ]
        for m in models:
            await storage.save_model(m)

        real = drift_mod.resolve_ingest_scope

        def _no_enumerate(**kwargs):
            if kwargs.get("all_schemas"):
                raise SchemaEnumerationError("simulated least-privilege connection")
            return real(**kwargs)

        monkeypatch.setattr(drift_mod, "resolve_ingest_scope", _no_enumerate)
        to_delete = await self._validate(ds, models)
        assert not [e for e in to_delete if e.tool == "delete_model"]


class TestLiveTableResolution:
    """``_resolve_live_table`` walks full → last-two → unquoted (§3.7), so a
    model written before its catalog was known still resolves — but it never
    strips a qualifier down to a bare same-named object (Codex review), which
    would let a dropped non-default table masquerade as its default twin."""

    def _live(self):
        from slayer.engine.schema_drift import LiveTable
        return LiveTable(columns={}, pk_columns=set(), fk_relationships=[])

    def test_three_part_resolves_via_last_two_segments(self) -> None:
        from slayer.engine.schema_drift import _resolve_live_table
        live = self._live()
        assert _resolve_live_table(
            sql_table="cat.schema.tbl", live_tables={"schema.tbl": live}
        ) is live

    def test_bare_model_resolves_against_the_bare_key(self) -> None:
        """A legacy unqualified model finds its default-schema live table (which
        ``_collect_live_tables`` keys bare)."""
        from slayer.engine.schema_drift import _resolve_live_table
        live = self._live()
        assert _resolve_live_table(
            sql_table="orders", live_tables={"orders": live}
        ) is live

    def test_default_qualified_model_resolves_against_the_qualified_key(self) -> None:
        """An explicit ``main.orders`` resolves because the default schema is
        dual-keyed (bare AND qualified) in the live map."""
        from slayer.engine.schema_drift import _resolve_live_table
        live = self._live()
        assert _resolve_live_table(
            sql_table="main.orders", live_tables={"main.orders": live}
        ) is live

    def test_missing_qualified_table_does_not_fall_through_to_a_bare_twin(self) -> None:
        """Codex-review regression: a qualified ``analytics.orders`` whose live
        table is GONE must resolve to None (→ WholeModelDelete), never to a
        same-named default ``orders`` twin — which would silently hide the drop.
        The pre-fix ``full → last-two → last-one`` walk returned the bare twin."""
        from slayer.engine.schema_drift import _resolve_live_table
        live = self._live()
        assert _resolve_live_table(
            sql_table="analytics.orders", live_tables={"orders": live}
        ) is None

    def test_quoted_identifier_is_unquoted(self) -> None:
        from slayer.engine.schema_drift import _resolve_live_table
        live = self._live()
        assert _resolve_live_table(
            sql_table='prod."Company"', live_tables={"prod.Company": live}
        ) is live

    def test_configured_schema_live_map_carries_the_catalog(self, basic_db) -> None:
        """Codex-review: the validate-models live-schema path for a configured
        schema must carry the current catalog on DuckDB, else the bare token
        re-arms the cross-catalog sweep. Pre-fix the ref's catalog was None."""
        from slayer.engine.schema_drift import _live_schema_refs
        engine = sa.create_engine(f"duckdb:///{basic_db}")
        try:
            insp = sa.inspect(engine)
            refs = _live_schema_refs(
                inspector=insp, sa_engine=engine, datasource=_ds(basic_db),
                schema="openfda_rest",
            )
            assert len(refs) == 1
            assert refs[0].catalog is not None
            assert refs[0].token == f"{refs[0].catalog}.openfda_rest"
        finally:
            engine.dispose()


# ===========================================================================
# DEV-1809 composition — comments from a non-default schema, no cross-bleed
# ===========================================================================


class TestCommentComposition:
    def test_comments_are_read_from_the_named_schema(self, comment_db) -> None:
        report = ingest_datasource_report(datasource=_ds(comment_db), schemas=["ofr"])
        reports = next(m for m in report.models if m.name == "reports")
        assert reports.description == "openfda report rows"
        val = next(c for c in reports.columns if c.name == "val")
        assert val.description == "the measured value"

    def test_comments_do_not_bleed_from_the_default_schema(self, comment_db) -> None:
        """The non-default ``val`` comment must win — never the default
        schema's same-named column comment."""
        report = ingest_datasource_report(datasource=_ds(comment_db), schemas=["ofr"])
        reports = next(m for m in report.models if m.name == "reports")
        val = next(c for c in reports.columns if c.name == "val")
        assert val.description != "DEFAULT SCHEMA COMMENT"


# ===========================================================================
# First-dot parsers — 3-part names through the probing paths (§3.8)
# ===========================================================================


class TestFirstDotParsers:
    """The persisted-``sql_table`` parsers split on the FINAL dot, so a 3-part
    ``catalog.schema.table`` keeps its catalog with the schema token (§3.8).
    (The shared ``split_sql_table`` is pinned in test_schema_scope.py; here we
    pin the ``type_refinement`` entry point the plan commits to keeping — the
    SQLite integer-probe / drift-probe input.) A first-dot split would probe
    schema ``c`` / table ``s.t`` and silently match nothing."""

    def test_type_refinement_parser_keeps_the_catalog(self) -> None:
        from slayer.storage.type_refinement import _parse_sql_table_with_default_schema
        ds = DatasourceConfig(name="s", type="duckdb", database=":memory:")
        assert _parse_sql_table_with_default_schema("proj.dataset.tbl", ds) == (
            "proj.dataset", "tbl"
        )


# ===========================================================================
# Forced-filter column-presence probe across catalogs (query_engine)
# ===========================================================================


class TestForcedFilterProbe:
    def test_probe_reads_the_current_catalog_column(self, attached_paths) -> None:
        """The forced-filter presence probe goes through ``_safe_get_columns``;
        with same-named tables across catalogs it must see the current
        catalog's columns, never the union."""
        from slayer.engine.introspect_utils import _safe_get_columns
        engine = _open_attached(attached_paths)
        try:
            insp = sa.inspect(engine)
            ref = SchemaRef(catalog="att_main", name="main")
            cols = {c["name"] for c in _safe_get_columns(insp, engine, "shared", ref)}
            assert cols == {"m"}
        finally:
            engine.dispose()


# ===========================================================================
# Hint eligibility
# ===========================================================================


class TestHint:
    async def test_hint_fires_for_a_persisted_schema_name(self, basic_db, tmp_path) -> None:
        """A datasource whose ``schema_name`` was set once still gets told when a
        new schema appears (eligibility independent of ``explicit``)."""
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db, schema_name="main")
        await storage.save_datasource(ds)
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert result.schema_hint is not None
        assert "openfda_rest" in result.schema_hint

    async def test_no_hint_under_all_schemas(self, basic_db, tmp_path) -> None:
        """``--all-schemas`` already covers everything, so there is nothing to
        hint about (eligibility: exactly one schema in scope)."""
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        await storage.save_datasource(ds)
        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, all_schemas=True
        )
        assert result.schema_hint is None


# ===========================================================================
# include / exclude filtering, per schema, BEFORE collision resolution (§3.5)
# ===========================================================================


class TestIncludeExcludeAcrossSchemas:
    def test_include_matches_the_bare_name_in_every_schema(self, same_name_db) -> None:
        """``include_tables=['reports']`` keeps the ``reports`` twin in each
        scanned schema; collision resolution then picks the winner. Filtering
        must NOT drop the default winner just because a non-default twin shares
        the name."""
        report = ingest_datasource_report(
            datasource=_ds(same_name_db), all_schemas=True,
            include_tables=["reports"],
        )
        reports = next(m for m in report.models if m.name == "reports")
        assert {c.name for c in reports.columns} == {"a"}        # main still wins

    def test_exclude_removes_the_name_in_every_schema(self, same_name_db) -> None:
        report = ingest_datasource_report(
            datasource=_ds(same_name_db), all_schemas=True,
            exclude_tables=["reports"],
        )
        assert "reports" not in {m.name for m in report.models}
        assert report.skipped == []                              # excluded, not skipped


# ===========================================================================
# schema_name persistence + precedence (D-7)
# ===========================================================================


def _run_cli(monkeypatch, argv: list[str]) -> None:
    """Drive the real ``slayer`` CLI by argv (``main`` reads ``sys.argv``)."""
    from slayer import cli
    monkeypatch.setattr("sys.argv", ["slayer", *argv])
    cli.main()


class TestSchemaNamePersistence:
    def test_create_persists_schema_name_and_bare_ingest_uses_it(
        self, basic_db, tmp_path, monkeypatch
    ) -> None:
        """``datasources create --schema X --ingest`` persists ``schema_name``;
        a subsequent bare ``slayer ingest`` scans X (D-3 fix, D-7)."""
        from slayer.async_utils import run_sync
        store = str(tmp_path / "store")
        storage = YAMLStorage(base_dir=store)

        _run_cli(monkeypatch, [
            "datasources", "--storage", store, "create",
            f"duckdb:///{basic_db}", "--name", "ds",
            "--schema", "openfda_rest", "--ingest", "--yes",
        ])
        stored = run_sync(storage.get_datasource("ds"))
        assert stored.schema_name == "openfda_rest"

        # A bare re-ingest against the stored config uses the persisted schema.
        report = ingest_datasource_report(datasource=stored)
        assert _sql_tables(report).get("reports") == "openfda_rest.reports"

    def test_all_schemas_does_not_persist_schema_name(
        self, basic_db, tmp_path, monkeypatch
    ) -> None:
        from slayer.async_utils import run_sync
        store = str(tmp_path / "store")
        storage = YAMLStorage(base_dir=store)
        _run_cli(monkeypatch, [
            "datasources", "--storage", store, "create",
            f"duckdb:///{basic_db}", "--name", "ds",
            "--all-schemas", "--ingest", "--yes",
        ])
        stored = run_sync(storage.get_datasource("ds"))
        assert stored.schema_name is None

    def test_explicit_schema_beats_persisted_beats_default(self, basic_db) -> None:
        ds = _ds(basic_db, schema_name="main")
        # explicit request overrides the persisted schema_name
        rep = ingest_datasource_report(datasource=ds, schemas=["openfda_rest"])
        assert _sql_tables(rep).get("reports") == "openfda_rest.reports"
        # with no request, the persisted schema_name is used (main → only in_default)
        rep2 = ingest_datasource_report(datasource=ds)
        assert set(_sql_tables(rep2)) == {"in_default"}


# ===========================================================================
# Engine / REST / MCP / CLI conflict + parity
# ===========================================================================


_CONFLICTS = [
    {"schema": "a", "schemas": ["b"]},
    {"schema": "a", "all_schemas": True},
    {"schemas": ["b"], "all_schemas": True},
]


class TestEngineConflicts:
    """The shared validator rejects conflicts at EVERY engine entry point."""

    @pytest.mark.parametrize("kwargs", _CONFLICTS)
    def test_report_rejects_conflicts(self, basic_db, kwargs) -> None:
        ds = _ds(basic_db)
        with pytest.raises(ValueError):
            ingest_datasource_report(datasource=ds, **kwargs)

    @pytest.mark.parametrize("kwargs", _CONFLICTS)
    def test_ingest_datasource_rejects_conflicts(self, basic_db, kwargs) -> None:
        from slayer.engine.ingestion import ingest_datasource
        ds = _ds(basic_db)
        with pytest.raises(ValueError):
            ingest_datasource(datasource=ds, **kwargs)

    @pytest.mark.parametrize("kwargs", _CONFLICTS)
    async def test_idempotent_rejects_conflicts(self, basic_db, tmp_path, kwargs) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        ds = _ds(basic_db)
        await storage.save_datasource(ds)
        with pytest.raises(ValueError):
            await ingest_datasource_idempotent(datasource=ds, storage=storage, **kwargs)


class TestRestParity:
    def test_conflicting_scope_is_a_validation_error(self) -> None:
        from pydantic import ValidationError
        from slayer.api.server import IngestRequest
        with pytest.raises(ValidationError):
            IngestRequest(datasource="ds", schema_name="a", schemas=["b"])
        with pytest.raises(ValidationError):
            IngestRequest(datasource="ds", all_schemas=True, schemas=["b"])

    def test_legacy_schema_name_still_accepted(self) -> None:
        from slayer.api.server import IngestRequest
        req = IngestRequest(datasource="ds", schema_name="public")
        assert req.schema_name == "public"

    async def test_http_ingest_qualifies_and_conflicts_are_422(self, basic_db, tmp_path) -> None:
        """The route actually applies the scope (qualifies) and returns 422 on a
        conflicting body — not just the request model in isolation."""
        from httpx import ASGITransport, AsyncClient
        from slayer.api.server import create_app
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(_ds(basic_db))
        app = create_app(storage=storage)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://t") as client:
            ok = await client.post("/ingest", json={
                "datasource": "ds", "schemas": ["openfda_rest"],
            })
            assert ok.status_code == 200, ok.text
            reports = await storage.get_model("reports", data_source="ds")
            assert reports is not None
            assert reports.sql_table == "openfda_rest.reports"

            bad = await client.post("/ingest", json={
                "datasource": "ds", "schema_name": "a", "schemas": ["b"],
            })
            assert bad.status_code == 422


class TestMcpParity:
    async def _server(self, storage):
        from slayer.mcp.server import create_mcp_server
        return create_mcp_server(storage=storage)

    async def _text(self, server, name, arguments):
        blocks, _ = await server.call_tool(name=name, arguments=arguments)
        return blocks[0].text

    async def test_ingest_tool_accepts_schemas(self, basic_db, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(_ds(basic_db))
        server = await self._server(storage)
        await self._text(server, "ingest_datasource_models",
                         {"datasource_name": "ds", "schemas": "openfda_rest"})
        reports = await storage.get_model("reports", data_source="ds")
        assert reports is not None
        assert reports.sql_table == "openfda_rest.reports"

    async def test_ingest_tool_reports_conflicting_scope(self, basic_db, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(_ds(basic_db))
        server = await self._server(storage)
        text = await self._text(server, "ingest_datasource_models",
                                {"datasource_name": "ds", "schemas": "a", "all_schemas": True})
        assert "all_schemas" in text                             # actionable error string
        assert "schema" in text

    async def test_create_datasource_persists_and_ingests_schema(self, basic_db, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        server = await self._server(storage)
        await self._text(server, "create_datasource", {
            "name": "ds", "type": "duckdb", "database": basic_db,
            "schema_name": "openfda_rest",
        })
        stored = await storage.get_datasource("ds")
        assert stored.schema_name == "openfda_rest"
        reports = await storage.get_model("reports", data_source="ds")
        assert reports is not None
        assert reports.sql_table == "openfda_rest.reports"

    async def test_create_datasource_validates_before_persisting(self, basic_db, tmp_path) -> None:
        """A conflicting scope is rejected as an error string AND the datasource
        is NOT persisted (validator runs before the save, §3.9)."""
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        server = await self._server(storage)
        text = await self._text(server, "create_datasource", {
            "name": "ds", "type": "duckdb", "database": basic_db,
            "schemas": "a", "all_schemas": True,
        })
        assert "all_schemas" in text
        assert "schema" in text
        assert await storage.get_datasource("ds") is None        # nothing persisted

    async def test_show_tables_does_not_sweep_non_default_schemas(self, basic_db, tmp_path) -> None:
        """MCP ``datasources show``'s table listing routes through scope
        resolution, so DuckDB no longer sweeps every schema (§3.9)."""
        from slayer.mcp.server import _fetch_tables
        # DEV-1750: _fetch_tables returns IngestableObjects (name + kind).
        objects, err = _fetch_tables(ds=_ds(basic_db), schema_name=None)
        assert err is None
        assert [o.name for o in objects] == ["in_default"]       # not [..., "reports"]


class TestCliArgumentParsing:
    """``--schema`` and ``--all-schemas`` are mutually exclusive. Assert the
    rejection is the *mutual-exclusion* one ("not allowed with"), not the
    unknown-argument one — so the test fails for the right reason before the
    flag exists."""

    def test_ingest_rejects_schema_with_all_schemas(self, monkeypatch, capsys) -> None:
        with pytest.raises(SystemExit):
            _run_cli(monkeypatch, ["ingest", "--datasource", "d",
                                   "--schema", "a", "--all-schemas"])
        assert "not allowed with" in capsys.readouterr().err

    def test_datasources_create_rejects_schema_with_all_schemas(
        self, monkeypatch, capsys
    ) -> None:
        with pytest.raises(SystemExit):
            _run_cli(monkeypatch, ["datasources", "create", "duckdb:///x.db",
                                   "--schema", "a", "--all-schemas"])
        assert "not allowed with" in capsys.readouterr().err


# ===========================================================================
# Non-regression — SQLite / default schema stay byte-identical
# ===========================================================================


class TestUnqualifiedNonRegression:
    def test_sqlite_ingest_stays_unqualified(self, tmp_path) -> None:
        import sqlite3
        db = str(tmp_path / "app.db")
        con = sqlite3.connect(db)
        con.executescript("CREATE TABLE orders(id INTEGER PRIMARY KEY, amount REAL);")
        con.commit()
        con.close()
        ds = DatasourceConfig(name="s", type="sqlite", database=db)
        report = ingest_datasource_report(datasource=ds)
        assert _sql_tables(report) == {"orders": "orders"}

    def test_duckdb_default_schema_stays_unqualified(self, basic_db) -> None:
        report = ingest_datasource_report(datasource=_ds(basic_db))
        assert _sql_tables(report) == {"in_default": "in_default"}
