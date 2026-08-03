"""Tests for slayer.facade.catalog_sql — DuckDB-backed catalog SQL executor (DEV-1558).

Replaces canned-row catalog matching with arbitrary SQL execution against an
in-memory DuckDB materialising the catalog corpus. These tests pin the corpus
shape, the AST pre-rewrite pass, the function stubs, the result-type coercion,
and the executor cache. The Metabase v0.62 golden-corpus tests live in
``test_catalog_sql_metabase_golden.py``.
"""

from __future__ import annotations

import logging

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.facade.catalog import (
    FacadeCatalog,
    build_catalog,
    build_catalog_grouped_by_schema,
)
from slayer.facade.catalog_sql import (
    KNOWN_SYSTEM_OIDS,
    CatalogRelation,
    CatalogSqlExecutor,
    build_catalog_relations,
    executor_for,
    is_catalog_only,
    stable_oid,
)
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.facade.translator import TranslationError


# --- fixtures ---------------------------------------------------------------


def _parse(sql: str):
    return sqlglot.parse_one(sql, dialect="postgres")


def _demo_catalog() -> FacadeCatalog:
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        description="Demo orders table",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE, description="Revenue cents"),
            Column(name="status", type=DataType.TEXT, label="Status"),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        measures=[
            ModelMeasure(
                name="aov", formula="revenue:sum / *:count", type=DataType.DOUBLE,
                description="Average order value",
            ),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["id", "id"]])],
    )
    customers = SlayerModel(
        name="customers",
        data_source="jaffle",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
        ],
    )
    # Production builds catalogs grouped by postgres_schema (default "public"),
    # so the demo catalog uses the same path — datasource "jaffle" folds under
    # schema "public".
    return build_catalog_grouped_by_schema(
        models_by_datasource={"jaffle": [orders, customers]}
    )


def _executor(catalog: FacadeCatalog | None = None, *, datasource: str = "jaffle") -> CatalogSqlExecutor:
    return CatalogSqlExecutor(catalog=catalog or _demo_catalog(), datasource=datasource)


def _run(sql: str, *, executor: CatalogSqlExecutor | None = None) -> RowBatch:
    ex = executor or _executor()
    return ex.execute(parsed=_parse(sql), sql=sql)


# --- corpus shape -----------------------------------------------------------


def test_build_catalog_relations_lists_every_required_relation() -> None:
    relations = build_catalog_relations(_demo_catalog())
    names = {r.name for r in relations}
    expected = {
        # pg_catalog tables (existing 6 + 9 new).
        "pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_proc", "pg_settings",
        "pg_description", "pg_stat_user_tables", "pg_enum", "pg_tables", "pg_views",
        "pg_matviews", "pg_constraint", "pg_index", "pg_attrdef",
        # information_schema (materialised under _is_ prefix per the AST rewrite plan).
        "_is_columns", "_is_table_constraints", "_is_key_column_usage",
        "_is_schemata", "_is_tables", "_is_metrics", "_is_dimensions",
    }
    assert expected <= names


def test_pg_description_table_description_row_has_objsubid_zero() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_description"].rows
    table_descs = [r for r in rows if r["objsubid"] == 0 and r["description"] == "Demo orders table"]
    assert len(table_descs) == 1
    assert table_descs[0]["classoid"] == 1259  # pg_class system OID


def test_pg_description_column_rows_use_one_based_objsubid() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_description"].rows
    # 'Revenue cents' should be one of the column-description rows on orders,
    # with objsubid > 0 (the column position).
    matches = [r for r in rows if r["description"] == "Revenue cents"]
    assert len(matches) == 1
    assert matches[0]["objsubid"] >= 1
    assert matches[0]["classoid"] == 1259


def test_pg_description_model_measure_description_present() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_description"].rows
    descs = {r["description"] for r in rows}
    assert "Average order value" in descs


def test_pg_stat_user_tables_n_live_tup_is_null() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_stat_user_tables"].rows
    assert {r["relname"] for r in rows} == {"orders", "customers"}
    assert all(r["n_live_tup"] is None for r in rows)


@pytest.mark.parametrize("relname", [
    "pg_enum", "pg_views", "pg_matviews", "pg_constraint", "pg_index", "pg_attrdef",
    "_is_table_constraints", "_is_key_column_usage",
])
def test_relations_designed_empty_are_empty(relname: str) -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    assert relations[relname].rows == []


def test_pg_tables_one_row_per_table_typed_model() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_tables"].rows
    assert {r["tablename"] for r in rows} == {"orders", "customers"}
    assert all(r["schemaname"] == "public" for r in rows)


def test_view_typed_models_surface_as_pg_views() -> None:
    """Codex review: SQL-backed models (``table_type='VIEW'``) must
    surface as views to view-aware clients — emitted with
    ``relkind='v'`` in ``pg_class`` AND one row in ``pg_views``. They
    are NOT in ``pg_tables`` (M1)."""
    sql_model = SlayerModel(
        name="custom_view", data_source="jaffle", sql="SELECT 1 AS id",
        columns=[Column(name="id", type=DataType.INT)],
    )
    cat = build_catalog_grouped_by_schema(models_by_datasource={"jaffle": [sql_model]})
    relations = {r.name: r for r in build_catalog_relations(cat)}
    # pg_tables filters out VIEW-typed models (M1).
    assert relations["pg_tables"].rows == []
    # pg_matviews unused — materialized-view abstraction not implemented.
    assert relations["pg_matviews"].rows == []
    # pg_class advertises relkind='v' for VIEW-typed models.
    cls_row = next(r for r in relations["pg_class"].rows if r["relname"] == "custom_view")
    assert cls_row["relkind"] == "v"
    # pg_views has one row per VIEW-typed model.
    view_rows = relations["pg_views"].rows
    assert len(view_rows) == 1
    assert view_rows[0]["viewname"] == "custom_view"
    assert view_rows[0]["schemaname"] == "public"


def test_table_typed_models_stay_as_pg_class_r() -> None:
    """Regression: normal ``sql_table``-mode models stay relkind='r' and
    do not bleed into pg_views."""
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    for row in relations["pg_class"].rows:
        assert row["relkind"] == "r"
    assert relations["pg_views"].rows == []


def test_is_columns_has_postgres_shape_plus_slayer_extensions() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    cols = {c.name for c in relations["_is_columns"].columns}
    pg_required = {
        "table_catalog", "table_schema", "table_name", "column_name",
        "ordinal_position", "column_default", "is_nullable", "data_type",
        "udt_schema", "udt_name", "is_identity", "is_generated",
    }
    slayer_extensions = {"column_kind", "description", "label"}
    assert pg_required <= cols
    assert slayer_extensions <= cols


def test_is_columns_udt_name_mapping_per_datatype() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    by_col = {(r["table_name"], r["column_name"]): r for r in relations["_is_columns"].rows}
    assert by_col[("orders", "id")]["udt_name"] == "int8"
    assert by_col[("orders", "status")]["udt_name"] == "text"
    assert by_col[("orders", "revenue")]["udt_name"] == "float8"
    assert by_col[("orders", "ordered_at")]["udt_name"] == "timestamp"


def test_pg_attrdef_pg_index_pg_constraint_carry_required_columns() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    attrdef_cols = {c.name for c in relations["pg_attrdef"].columns}
    index_cols = {c.name for c in relations["pg_index"].columns}
    constraint_cols = {c.name for c in relations["pg_constraint"].columns}
    assert {"adrelid", "adnum", "adbin"} <= attrdef_cols
    assert {"indrelid", "indexrelid", "indkey", "indisprimary", "indnkeyatts"} <= index_cols
    assert {"conrelid", "confrelid", "connamespace", "contype", "conkey", "confkey"} <= constraint_cols


def test_empty_catalog_relations_project_metabase_referenced_columns() -> None:
    """Each empty catalog relation must declare every column Metabase v0.62
    queries reference, so DuckDB binds the SELECT successfully and returns
    zero rows (rather than erroring with 'column not found').  Smoke-test
    by selecting the actual referenced columns from the corpus."""
    # Drawn from the corpus statements (#12, #14, #15) — these are the columns
    # Metabase joins/filters/projects on the empty relations.
    queries = [
        ("pg_views", "SELECT schemaname, viewname, viewowner, definition FROM pg_views"),
        ("pg_matviews", "SELECT schemaname, matviewname, matviewowner, "
                        "hasindexes, ispopulated FROM pg_matviews"),
        ("pg_constraint", "SELECT oid, conname, contype, conrelid, confrelid, "
                          "connamespace, conkey, confkey FROM pg_constraint"),
        ("pg_index", "SELECT indexrelid, indrelid, indkey, indisprimary, "
                     "indnkeyatts FROM pg_index"),
        ("pg_attrdef", "SELECT oid, adrelid, adnum, adbin FROM pg_attrdef"),
        ("information_schema.table_constraints",
         "SELECT constraint_name, constraint_schema, constraint_name, table_schema, "
         "table_name, constraint_type FROM information_schema.table_constraints"),
        ("information_schema.key_column_usage",
         "SELECT constraint_name, table_schema, table_name, column_name "
         "FROM information_schema.key_column_usage"),
    ]
    ex = _executor()
    for _label, sql in queries:
        batch = ex.execute(parsed=_parse(sql), sql=sql)
        assert batch.rows == [], f"{_label!r} should be empty but returned {batch.rows!r}"


def test_pg_type_carries_typnotnull_typbasetype_typtypmod() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    cols = {c.name for c in relations["pg_type"].columns}
    assert {"typnotnull", "typbasetype", "typtypmod", "typtype"} <= cols


# --- is_catalog_only --------------------------------------------------------


@pytest.mark.parametrize("sql", [
    "SELECT * FROM pg_catalog.pg_namespace",
    "SELECT * FROM pg_namespace",
    "SELECT * FROM information_schema.columns",
    "SELECT * FROM information_schema.tables",
    # Tableless SELECTs are catalog-only only when they explicitly
    # reference a catalog function (CR review feedback).
    "SELECT current_database()",
    "SELECT pg_catalog.current_database()",
    "SELECT 'pg_class'::regclass",
    "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
    "ON c.relnamespace = n.oid",
])
def test_is_catalog_only_true_for_catalog_or_tableless(sql: str) -> None:
    assert is_catalog_only(_parse(sql)) is True


@pytest.mark.parametrize("sql", [
    # Tableless SELECTs without any catalog reference no longer route
    # through the executor — the probe matcher handles SELECT 1 etc.,
    # and routing unknown tableless SQL through DuckDB would expand the
    # facade's accepted SQL surface (CR review feedback).
    "SELECT 1",
    "SELECT 1 + 1",
    "SELECT NOW()",
])
def test_is_catalog_only_false_for_unannotated_tableless(sql: str) -> None:
    assert is_catalog_only(_parse(sql)) is False


def test_is_catalog_only_false_for_user_model() -> None:
    assert is_catalog_only(_parse("SELECT revenue_sum FROM orders")) is False


def test_is_catalog_only_false_for_mixed() -> None:
    # FROM a catalog table joined to a user model is NOT catalog-only.
    sql = "SELECT * FROM pg_catalog.pg_class c JOIN orders o ON c.relname = o.status"
    assert is_catalog_only(_parse(sql)) is False


def test_is_catalog_only_skips_cte_names() -> None:
    # #8 (table_privileges CTE) shape — CTE table reference must not disqualify.
    sql = """
    WITH table_privileges AS (
        SELECT schemaname, tablename FROM pg_catalog.pg_tables
    )
    SELECT t.* FROM table_privileges t
    """
    assert is_catalog_only(_parse(sql)) is True


def test_is_catalog_only_cte_body_user_table_still_false() -> None:
    # A CTE that references a user model in its body — outer 'x' is exempt
    # because it's CTE-defined, but the CTE BODY's reference to `orders`
    # must still disqualify (otherwise an exemption-everything implementation
    # would mark all unknown refs as catalog-only).
    sql = "WITH x AS (SELECT * FROM orders) SELECT * FROM x"
    assert is_catalog_only(_parse(sql)) is False


def test_is_catalog_only_unknown_table_false() -> None:
    # Guard against an over-broad implementation that treats every Table node
    # as if it were CTE-defined.
    assert is_catalog_only(_parse("SELECT * FROM totally_unknown_thing")) is False


@pytest.mark.parametrize("name", ["columns", "tables", "schemata"])
def test_is_catalog_only_bare_info_schema_names_disallowed(name: str) -> None:
    # DEV-1558 Codex review: bare info-schema names like `columns` /
    # `tables` shadow user models with the same name. Only the qualified
    # form `information_schema.columns` routes to the catalog executor;
    # bare `columns` flows down to the regular SLayer translator.
    assert is_catalog_only(_parse(f"SELECT * FROM {name}")) is False


def test_is_catalog_only_catalog_qualified_information_schema_resolves() -> None:
    assert is_catalog_only(_parse("SELECT * FROM slayer.information_schema.columns")) is True


def test_is_catalog_only_catalog_qualified_pg_catalog_resolves() -> None:
    assert is_catalog_only(_parse("SELECT * FROM slayer.pg_catalog.pg_class")) is True


def test_is_catalog_only_foreign_catalog_disallowed() -> None:
    # A non-slayer outer catalog is never our catalog SQL.
    assert is_catalog_only(_parse("SELECT * FROM other.pg_catalog.pg_class")) is False


def test_catalog_qualified_information_schema_runs() -> None:
    # `slayer.information_schema.columns` should run cleanly — the outer
    # catalog qualifier strips off and `information_schema.columns`
    # rewrites to `_is_columns`.
    batch = _run("SELECT table_name FROM slayer.information_schema.columns "
                 "WHERE table_name = 'orders' LIMIT 5")
    assert all(r["table_name"] == "orders" for r in batch.rows)


def test_is_catalog_only_skips_subquery_aliases() -> None:
    # Subquery alias 'sub' is not a catalog table either, but its inner FROM is.
    sql = """
    SELECT * FROM (SELECT relname FROM pg_catalog.pg_class) sub
    """
    assert is_catalog_only(_parse(sql)) is True


# --- projection alias case preservation -------------------------------------


def test_quoted_alias_case_preserved() -> None:
    batch = _run('SELECT nspname AS "TABLE_SCHEM" FROM pg_catalog.pg_namespace')
    assert [c.name for c in batch.columns] == ["TABLE_SCHEM"]
    assert {r["TABLE_SCHEM"] for r in batch.rows} == {"public", "pg_catalog"}


def test_unquoted_alias_lowercase_per_duckdb_default() -> None:
    batch = _run("SELECT nspname AS table_schem FROM pg_catalog.pg_namespace")
    assert [c.name for c in batch.columns] == ["table_schem"]


# --- AST rewrites -----------------------------------------------------------


def test_regclass_static_known_resolves() -> None:
    batch = _run("SELECT 'pg_class'::regclass AS oid")
    assert batch.rows == [{"oid": 1259}]


def test_regclass_static_user_table_resolves_to_table_oid() -> None:
    # 'orders' resolves to the deterministic stable_oid('jaffle', 'orders').
    batch = _run("SELECT 'orders'::regclass AS oid")
    rows = batch.rows
    assert rows[0]["oid"] != 0  # non-trivial OID


def test_regclass_static_unknown_resolves_to_zero() -> None:
    batch = _run("SELECT 'nope.does_not_exist'::regclass AS oid")
    assert batch.rows == [{"oid": 0}]


def test_regclass_dynamic_via_udf() -> None:
    # Row-dependent regclass cast — #12's CAST(FORMAT('%I.%I', ...) AS REGCLASS).
    batch = _run(
        "SELECT CAST(FORMAT('%I.%I', 'public', 'orders') AS REGCLASS) AS oid"
    )
    assert batch.rows[0]["oid"] != 0


def test_regclass_well_known_system_oids_map_contract() -> None:
    # Contract pinned: pg_description.classoid = 1259 → 'pg_class'::regclass must = 1259.
    # The full set Codex's prior round required is enumerated here so a
    # partial implementation cannot pass.
    assert KNOWN_SYSTEM_OIDS["pg_class"] == 1259
    assert KNOWN_SYSTEM_OIDS["pg_namespace"] == 2615
    assert KNOWN_SYSTEM_OIDS["pg_attribute"] == 1249
    assert KNOWN_SYSTEM_OIDS["pg_type"] == 1247
    assert KNOWN_SYSTEM_OIDS["pg_proc"] == 1255
    assert KNOWN_SYSTEM_OIDS["pg_description"] == 2609
    assert KNOWN_SYSTEM_OIDS["pg_constraint"] == 2606
    assert KNOWN_SYSTEM_OIDS["pg_index"] == 2610
    assert KNOWN_SYSTEM_OIDS["pg_attrdef"] == 2604


def test_regclass_dynamic_udf_is_row_dependent() -> None:
    # A constant-folding implementation that special-cases FORMAT(literal, literal)
    # would still pass test_regclass_dynamic_via_udf. Use VALUES so the cast input
    # is genuinely row-dependent.
    batch = _run(
        "SELECT slayer_regclass_oid(FORMAT('%I.%I', schema, tbl)) AS oid "
        "FROM (VALUES ('public', 'orders'), ('public', 'customers'), "
        "             ('public', 'nope')) AS v(schema, tbl)"
    )
    rows = batch.rows
    assert len(rows) == 3
    # Two known → non-zero OIDs that match pg_class; one unknown → 0.
    nonzero = [r["oid"] for r in rows[:2]]
    assert all(n != 0 for n in nonzero)
    assert rows[2]["oid"] == 0


def test_regproc_cast_to_zero() -> None:
    batch = _run("SELECT 'any_proc'::regproc AS oid")
    assert batch.rows == [{"oid": 0}]


def test_regtype_cast_unknown_resolves_to_zero() -> None:
    batch = _run("SELECT 'any_unknown_type'::regtype AS oid")
    assert batch.rows == [{"oid": 0}]


@pytest.mark.parametrize(("typename", "expected_oid"), [
    ("int8", 20),
    ("text", 25),
    ("bool", 16),
    ("float8", 701),
    ("date", 1082),
    ("timestamp", 1114),
])
def test_regtype_cast_known_resolves_to_oid(typename: str, expected_oid: int) -> None:
    """Codex review: known PostgreSQL type names resolve via
    ``::regtype`` cast to the underlying ``pg_type.oid`` so catalog
    queries like ``WHERE oid = 'int8'::regtype`` filter correctly."""
    batch = _run(f"SELECT '{typename}'::regtype AS oid")
    assert batch.rows == [{"oid": expected_oid}]


def test_regtype_cast_filter_against_pg_type() -> None:
    """End-to-end: ``WHERE oid = 'int8'::regtype`` matches the int8
    row in pg_type."""
    batch = _run(
        "SELECT typname FROM pg_catalog.pg_type WHERE oid = 'int8'::regtype"
    )
    assert batch.rows == [{"typname": "int8"}]


def test_pg_catalog_qualifier_stripped() -> None:
    # pg_catalog.pg_class and bare pg_class produce identical results.
    qualified = _run("SELECT relname FROM pg_catalog.pg_class ORDER BY relname")
    bare = _run("SELECT relname FROM pg_class ORDER BY relname")
    assert qualified.rows == bare.rows


def test_information_schema_qualifier_resolves() -> None:
    batch = _run("SELECT table_name FROM information_schema.columns "
                 "WHERE table_name = 'orders' ORDER BY ordinal_position")
    table_names = {r["table_name"] for r in batch.rows}
    assert table_names == {"orders"}


@pytest.mark.parametrize("expr", ["current_database()", "current_catalog"])
def test_current_database_and_catalog_resolve_to_datasource(expr: str) -> None:
    batch = _run(f"SELECT {expr} AS db")
    assert batch.rows == [{"db": "jaffle"}]


@pytest.mark.parametrize("expr", ["current_schema()", "current_schema"])
def test_current_schema_resolves_to_public(expr: str) -> None:
    """DuckDB's built-in current_schema returns 'main'; we must rewrite
    to 'public' to match the single schema the pg facade advertises
    (CR/Codex review)."""
    batch = _run(f"SELECT {expr} AS s")
    assert batch.rows == [{"s": "public"}]


@pytest.mark.parametrize("sql", [
    "SELECT current_role",
    "SELECT current_user",
    "SELECT current_catalog",
])
def test_tableless_context_function_routes_to_executor_via_translator(sql: str) -> None:
    """CR/Codex round 15: ``SELECT current_role`` (and the niladic
    siblings) are parsed by sqlglot as bare Column refs without a
    FROM clause. The translator must classify them as catalog-only
    (via is_catalog_only) so they reach the executor instead of
    falling through to the SLayer model-query path with a 'no FROM
    clause' error."""
    parsed = _parse(sql)
    assert is_catalog_only(parsed) is True


def test_information_schema_qualified_column_projection_resolves() -> None:
    """Codex round 16: a fully-qualified
    ``information_schema.columns.column_name`` column ref must rename
    its table-qualifier to ``_is_columns`` to match the FROM-side
    strip, not just drop the ``information_schema`` qualifier."""
    batch = _run(
        "SELECT information_schema.columns.column_name "
        "FROM information_schema.columns "
        "WHERE information_schema.columns.table_name = 'orders'"
    )
    assert batch.rows
    cols = {r["column_name"] for r in batch.rows}
    # The demo catalog's ``orders`` model defines four dimension columns;
    # all four must surface so a partial qualifier-strip would fail loudly.
    assert {"id", "revenue", "status", "ordered_at"} <= cols


def test_pg_catalog_qualified_column_projection_resolves() -> None:
    """Codex round 15: a catalog query that fully qualifies a Column
    ref (e.g. ``SELECT pg_catalog.pg_namespace.nspname FROM
    pg_catalog.pg_namespace``) is legal Postgres SQL. The FROM-side
    strip already turns the table into bare ``pg_namespace``; the
    column-side strip must remove the matching catalog qualifier from
    the projection so DuckDB binds cleanly."""
    batch = _run(
        "SELECT pg_catalog.pg_namespace.nspname FROM pg_catalog.pg_namespace "
        "WHERE pg_catalog.pg_namespace.nspname = 'public'"
    )
    names = {r["nspname"] for r in batch.rows}
    assert names == {"public"}


def test_pg_catalog_four_part_qualified_column_resolves() -> None:
    """Round 20 follow-up (Codex review): a 4-part column ref
    ``slayer.pg_catalog.pg_namespace.nspname`` keeps a stale ``slayer``
    catalog qualifier on the column AST even after the schema part is
    stripped, breaking DuckDB binding against ``main.pg_namespace``.
    ``_strip_column_schema_qualifiers`` must drop the catalog qualifier
    symmetrically with the FROM-side ``_strip_schema_qualifiers``."""
    batch = _run(
        "SELECT slayer.pg_catalog.pg_namespace.nspname "
        "FROM slayer.pg_catalog.pg_namespace "
        "WHERE slayer.pg_catalog.pg_namespace.nspname = 'public'"
    )
    assert {r["nspname"] for r in batch.rows} == {"public"}


def test_information_schema_four_part_qualified_column_resolves() -> None:
    """Companion to the pg_catalog case: a 4-part column ref through
    ``information_schema`` must lose both the outer catalog qualifier
    AND have its table-side qualifier rewritten to the ``_is_<X>``
    materialised form."""
    batch = _run(
        "SELECT slayer.information_schema.columns.column_name "
        "FROM slayer.information_schema.columns "
        "WHERE slayer.information_schema.columns.table_name = 'orders' "
        "ORDER BY slayer.information_schema.columns.ordinal_position "
        "LIMIT 1"
    )
    assert batch.rows and "column_name" in batch.rows[0]


def test_pg_catalog_query_filters_by_current_schema() -> None:
    """End-to-end shape exercising the current_schema substitution
    inside a catalog query (mixes both rewrite paths)."""
    batch = _run(
        "SELECT nspname FROM pg_catalog.pg_namespace "
        "WHERE nspname = current_schema()"
    )
    assert batch.rows == [{"nspname": "public"}]


def test_current_database_full_tree_walk_inside_projection() -> None:
    # #13 shape: current_database() inside a SELECT projection of a JOINed query.
    sql = (
        "SELECT current_database() AS current_database, n.nspname "
        "FROM pg_catalog.pg_namespace n WHERE n.nspname = 'public'"
    )
    batch = _run(sql)
    assert batch.rows[0]["current_database"] == "jaffle"


def test_catalog_union_all_routes_to_executor() -> None:
    """Live-Metabase repro: corpus #12 (describe-fields) is a top-level
    ``UNION ALL`` between an info-schema branch and a pg_catalog
    branch. The translator must route ``exp.Union`` through the
    catalog executor when every leaf Table resolves to a catalog
    relation — previously the Select-only gate raised
    ``Unsupported statement: Union`` before the executor branch
    ever fired."""
    from slayer.facade.translator import (
        PgCatalogResult, translate,
    )
    sql = (
        "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
        "INNER JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
        "WHERE n.nspname = 'public' "
        "UNION ALL "
        "SELECT 'public' AS nspname, t.table_name AS relname "
        "FROM information_schema.tables t"
    )
    catalog = _demo_catalog()
    result = translate(sql, catalog, dialect="postgres",
                      catalog_sql_executor=_executor(catalog))
    assert isinstance(result, PgCatalogResult)
    # Both branches contribute rows; orders + customers from the
    # pg_class half, plus the info-schema tables half.
    relnames = {r["relname"] for r in result.batch.rows}
    assert "orders" in relnames
    assert "customers" in relnames


@pytest.mark.parametrize("call", [
    "pg_catalog.current_database()",
    "pg_catalog.current_catalog",
    "pg_catalog.current_user",
    "pg_catalog.current_schema()",
])
def test_pg_catalog_qualified_context_functions_substitute(call: str) -> None:
    """Postgres clients commonly qualify built-ins via ``pg_catalog.``.
    The whole ``Dot(pg_catalog, <ctx-fn>)`` node must collapse to the
    literal — otherwise the substitution leaves the outer Dot wrapping
    a string and DuckDB parses ``pg_catalog.'jaffle'``."""
    batch = _run(f"SELECT {call} AS v")
    val = batch.rows[0]["v"]
    if "current_database" in call or "current_catalog" in call:
        assert val == "jaffle"
    elif "current_user" in call:
        assert val == "slayer"
    elif "current_schema" in call:
        # current_schema() is handled by the pg-facade probe matcher in
        # production, but inside the executor we just need the AST
        # rewrite to not crash. Any string value is acceptable here.
        assert isinstance(val, str)


@pytest.mark.parametrize("expr", ["current_user", "session_user", "current_role"])
def test_current_user_role_resolve_to_slayer(expr: str) -> None:
    batch = _run(f"SELECT {expr} AS u")
    assert batch.rows == [{"u": "slayer"}]


def test_current_schemas_true_indexed_rewrites_to_public() -> None:
    batch = _run("SELECT (current_schemas(true))[1] AS first_schema")
    assert batch.rows == [{"first_schema": "public"}]


def test_current_schemas_bare_returns_single_element_list() -> None:
    """CR/Codex review: the bare ``current_schemas(...)`` call (no
    bracket index) must return ``['public']`` — the only schema the
    facade advertises. Without this rewrite DuckDB would emit its
    internal schema list."""
    batch = _run("SELECT current_schemas(true) AS s")
    assert batch.rows == [{"s": ["public"]}]


def test_current_schemas_non_first_index_returns_null() -> None:
    """CR review: ``current_schemas(true)[2]`` must NOT collapse to
    'public' — the indexed-rewrite only fires for ``[1]``. The bare
    ``current_schemas(...)`` rewrite leaves the call as
    ``LIST_VALUE('public')`` (a single-element list); indexing with
    ``[2]`` then returns NULL (out of bounds) under DuckDB."""
    batch = _run("SELECT (current_schemas(true))[2] AS s")
    assert batch.rows == [{"s": None}]


def test_is_schemata_folds_datasources_to_public_by_default() -> None:
    """DEV-1594: datasources without a custom ``postgres_schema`` fold into a
    single ``public`` schema, so ``information_schema.schemata`` has one row
    even when multiple datasources back the catalog."""
    cat = build_catalog_grouped_by_schema(models_by_datasource={
        "dsA": [SlayerModel(
            name="t1", data_source="dsA", sql_table="t1",
            columns=[Column(name="x", type=DataType.INT)],
        )],
        "dsB": [SlayerModel(
            name="t2", data_source="dsB", sql_table="t2",
            columns=[Column(name="y", type=DataType.INT)],
        )],
    })
    relations = {r.name: r for r in build_catalog_relations(cat, datasource="dsX")}
    rows = relations["_is_schemata"].rows
    assert len(rows) == 1
    assert rows[0] == {"catalog_name": "dsX", "schema_name": "public"}


def test_is_schemata_lists_custom_postgres_schemas() -> None:
    """A datasource with a custom ``postgres_schema`` surfaces as its own
    schema row alongside ``public``."""
    cat = build_catalog_grouped_by_schema(
        models_by_datasource={
            "dsA": [SlayerModel(
                name="t1", data_source="dsA", sql_table="t1",
                columns=[Column(name="x", type=DataType.INT)],
            )],
            "dsB": [SlayerModel(
                name="t2", data_source="dsB", sql_table="t2",
                columns=[Column(name="y", type=DataType.INT)],
            )],
        },
        schema_by_datasource={"dsB": "warehouse"},
    )
    relations = {r.name: r for r in build_catalog_relations(cat, datasource="dsX")}
    schemas = {r["schema_name"] for r in relations["_is_schemata"].rows}
    assert schemas == {"public", "warehouse"}
    # t2 reports its real schema in information_schema.tables.
    t2 = next(r for r in relations["_is_tables"].rows if r["table_name"] == "t2")
    assert t2["table_schema"] == "warehouse"
    # pg_namespace lists the custom schema with a distinct (non-public) oid.
    ns = {r["nspname"]: r["oid"] for r in relations["pg_namespace"].rows}
    assert "warehouse" in ns and "public" in ns
    assert ns["warehouse"] != ns["public"]
    # pg_class points t2 at the warehouse namespace.
    t2_class = next(r for r in relations["pg_class"].rows if r["relname"] == "t2")
    assert t2_class["relnamespace"] == ns["warehouse"]


def test_information_schema_schema_name_is_public_not_datasource() -> None:
    """Codex round 14: PostgreSQL clients filter
    ``information_schema.schemata`` / ``information_schema.tables`` by
    ``current_schema()`` (which returns ``'public'``) or by joining
    against ``pg_namespace`` (where the schema is also ``'public'``).
    The schema columns in our INFORMATION_SCHEMA rows must therefore
    be ``'public'`` too — not the SLayer-side datasource name."""
    relations = {
        r.name: r for r in build_catalog_relations(_demo_catalog(), datasource="jaffle")
    }
    assert all(r["schema_name"] == "public" for r in relations["_is_schemata"].rows)
    assert all(r["table_schema"] == "public" for r in relations["_is_tables"].rows)
    assert all(r["schema_name"] == "public" for r in relations["_is_metrics"].rows)
    assert all(r["schema_name"] == "public" for r in relations["_is_dimensions"].rows)
    # _is_columns was already 'public' since round 1.
    assert all(r["table_schema"] == "public" for r in relations["_is_columns"].rows)


def test_information_schema_catalog_name_is_datasource() -> None:
    """CR/Codex review: PostgreSQL clients filter
    ``information_schema.*`` by ``current_database()`` (which the AST
    rewrite substitutes with the connection's datasource). The
    ``table_catalog`` / ``catalog_name`` columns must therefore carry the
    same datasource value — not the hardcoded ``slayer`` catalog name."""
    relations = {
        r.name: r for r in build_catalog_relations(_demo_catalog(), datasource="jaffle")
    }
    cols_rows = relations["_is_columns"].rows
    assert cols_rows
    assert all(r["table_catalog"] == "jaffle" for r in cols_rows)
    tables_rows = relations["_is_tables"].rows
    assert tables_rows
    assert all(r["table_catalog"] == "jaffle" for r in tables_rows)
    schemata_rows = relations["_is_schemata"].rows
    assert schemata_rows
    assert all(r["catalog_name"] == "jaffle" for r in schemata_rows)
    metrics_rows = relations["_is_metrics"].rows
    # CR review: assert non-empty before all(...) — all([]) is True.
    assert metrics_rows
    assert all(r["catalog_name"] == "jaffle" for r in metrics_rows)
    dims_rows = relations["_is_dimensions"].rows
    assert dims_rows
    assert all(r["catalog_name"] == "jaffle" for r in dims_rows)


@pytest.mark.parametrize("dangerous_fn", [
    "read_text", "read_csv", "read_blob", "read_parquet",
])
def test_executor_blocks_filesystem_readers(dangerous_fn: str) -> None:
    """Codex round 9 critical: an authenticated catalog query must not
    be able to read local files via DuckDB's filesystem built-ins.

    The DuckDB session is locked into
    ``enable_external_access = false`` at construction; every FS / HTTP
    reader raises a BinderException at bind time, which the executor
    surfaces as a ``TranslationError``."""
    sql = (
        f"SELECT {dangerous_fn}('/etc/hostname') AS x "
        "FROM pg_catalog.pg_class LIMIT 1"
    )
    with pytest.raises(TranslationError):
        _run(sql)


def test_executor_cannot_re_enable_external_access() -> None:
    """``lock_configuration`` (where available) prevents a clever
    catalog-shaped query from re-enabling external access mid-session
    via ``SET enable_external_access = true``."""
    # The executor's `_translate` rejects unsupported statements, and a
    # bare SET would be classified as a NoOp at the translator level
    # anyway — but route a raw `SET` through the executor's `execute`
    # path directly to pin DuckDB's behaviour.
    ex = _executor()
    parsed = _parse("SET enable_external_access = true")
    with pytest.raises(TranslationError):
        ex.execute(parsed=parsed, sql="SET enable_external_access = true")


def test_executor_distinguishes_duplicate_output_column_names() -> None:
    """CR/Codex review: PostgreSQL allows duplicate output column names
    (``SELECT oid AS x, relname AS x FROM pg_class``). The previous
    row-as-dict shape collapsed them. The executor now disambiguates
    per-row keys via a ``__N`` suffix on the second-and-later
    occurrences, while the wire-visible RowDescription preserves the
    duplicate name; the wire emitter uses the position-aware keys."""
    batch = _run('SELECT 1 AS x, 2 AS x')
    # RowDescription preserves the duplicate name.
    assert [c.name for c in batch.columns] == ["x", "x"]
    # The row stores both values under disambiguated keys.
    assert len(batch.rows) == 1
    row = batch.rows[0]
    assert set(row.values()) == {1, 2}
    # The position-aware key list is exposed for the wire emitter.
    keys = getattr(batch, "_row_keys", None)
    assert keys == ["x", "x__2"]


def test_is_metrics_emits_jdbc_type_names() -> None:
    """The SLayer ``INFORMATION_SCHEMA.METRICS`` extension preserves the
    JDBC-style type contract from the pre-DEV-1558 ``match_info_schema``
    path (DOUBLE / BIGINT / TIMESTAMP rather than Postgres-internal
    float8 / int8 / timestamp)."""
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["_is_metrics"].rows
    types = {r["data_type"] for r in rows if r["data_type"] is not None}
    # No Postgres-internal spellings.
    assert "float8" not in types
    assert "int8" not in types
    assert "timestamp" not in types
    # Has JDBC-style spellings for what the demo carries.
    assert types & {"DOUBLE", "BIGINT", "TIMESTAMP"}


def test_is_dimensions_emits_jdbc_type_names() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["_is_dimensions"].rows
    types = {r["data_type"] for r in rows if r["data_type"] is not None}
    assert "float8" not in types
    assert "int8" not in types
    assert types & {"DOUBLE", "BIGINT", "TIMESTAMP"}


@pytest.mark.parametrize(("op", "pattern", "value", "matches"), [
    ("~", "^pg_", "pg_class", True),
    ("~", "^pg_", "orders", False),
    ("!~", "^pg_", "orders", True),
    ("!~", "^pg_", "pg_class", False),
    ("~*", "PG_", "pg_class", True),
    ("!~*", "PG_", "orders", True),
])
def test_regex_operators_rewritten(op: str, pattern: str, value: str, matches: bool) -> None:
    sql = f"SELECT 1 AS x WHERE '{value}' {op} '{pattern}'"
    batch = _run(sql)
    assert (len(batch.rows) == 1) is matches


# --- function stubs ---------------------------------------------------------


@pytest.mark.parametrize("call", [
    "has_table_privilege('orders', 'SELECT')",
    "has_table_privilege(current_user, 'orders', 'SELECT')",
    "has_table_privilege(10::int, 'SELECT')",
    "has_any_column_privilege('orders', 'SELECT')",
    "has_any_column_privilege(current_user, 'orders', 'SELECT')",
    "has_schema_privilege('public', 'USAGE')",
    "has_schema_privilege(current_user, 'public', 'USAGE')",
    "pg_table_is_visible(1259)",
])
def test_privilege_and_visibility_stubs_return_true(call: str) -> None:
    batch = _run(f"SELECT {call} AS r")
    assert batch.rows == [{"r": True}]


def test_pg_get_userbyid_returns_slayer() -> None:
    batch = _run("SELECT pg_get_userbyid(10) AS u")
    assert batch.rows == [{"u": "slayer"}]


def test_format_type_known_oid_returns_typname() -> None:
    batch = _run("SELECT format_type(25, NULL) AS t")  # 25 = OID_TEXT  # NOSONAR(S125) — value annotation, not commented-out code
    assert batch.rows == [{"t": "text"}]


def test_format_type_unknown_oid_falls_back_to_text() -> None:
    batch = _run("SELECT format_type(99999, NULL) AS t")
    assert batch.rows == [{"t": "text"}]


def test_obj_description_returns_table_description() -> None:
    # orders has description "Demo orders table"
    batch = _run(
        "SELECT obj_description("
        "    'orders'::regclass) AS d"
    )
    assert batch.rows[0]["d"] == "Demo orders table"


def test_col_description_returns_column_description() -> None:
    # revenue has description "Revenue cents"; col_description(oid, position).
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rev_attnum = next(
        r["attnum"] for r in relations["pg_attribute"].rows
        if r["attname"] == "revenue"
    )
    sql = f"SELECT col_description('orders'::regclass, {rev_attnum}) AS d"
    batch = _run(sql)
    assert batch.rows[0]["d"] == "Revenue cents"


def test_pg_get_expr_returns_null() -> None:
    batch = _run("SELECT pg_get_expr('text', 0) AS e")
    assert batch.rows == [{"e": None}]


def test_pg_total_relation_size_zero() -> None:
    batch = _run("SELECT pg_total_relation_size(1259) AS s")
    assert batch.rows == [{"s": 0}]


def test_pg_expandarray_stub_struct_pack_yields_null_fields() -> None:
    # _pg_expandarray returns a struct with `n` and `x` fields per Codex B2 fix.
    batch = _run("SELECT (_pg_expandarray('1 2 3')).n AS n, (_pg_expandarray('1 2 3')).x AS x")
    assert batch.rows[0]["n"] is None
    assert batch.rows[0]["x"] is None


def test_pg_expandarray_through_empty_pg_index_yields_zero_rows() -> None:
    # End-to-end shape of corpus #14: pg_index empty + _pg_expandarray stub ⇒ 0 rows.
    sql = """
    SELECT result.PK_NAME
    FROM (
        SELECT n.nspname, ct.relname,
               (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
               ci.relname AS PK_NAME,
               i.indnkeyatts AS KEY_COUNT
        FROM pg_catalog.pg_class ct
        JOIN pg_catalog.pg_attribute a ON ct.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON ct.relnamespace = n.oid
        JOIN pg_catalog.pg_index i ON a.attrelid = i.indrelid
        JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
        WHERE n.nspname = 'public' AND i.indisprimary
    ) result
    """
    batch = _run(sql)
    assert batch.rows == []


# --- result-type coercion ---------------------------------------------------


def test_result_types_within_six_datatypes() -> None:
    batch = _run(
        "SELECT 1::INT8 AS i, 1.0::DOUBLE AS d, true AS b, "
        "DATE '2026-01-01' AS dt, TIMESTAMP '2026-01-01' AS ts, 'x' AS t"
    )
    type_by_name = {c.name: c.type for c in batch.columns}
    assert type_by_name["i"] == DataType.INT
    assert type_by_name["d"] == DataType.DOUBLE
    assert type_by_name["b"] == DataType.BOOLEAN
    assert type_by_name["dt"] == DataType.DATE
    assert type_by_name["ts"] == DataType.TIMESTAMP
    assert type_by_name["t"] == DataType.TEXT


def test_unmapped_duckdb_type_falls_back_to_text() -> None:
    batch = _run("SELECT CAST('00000000-0000-0000-0000-000000000000' AS UUID) AS u")
    # UUID isn't one of the six coarse types → falls back to TEXT.
    assert batch.columns[0].type == DataType.TEXT


# --- error handling ---------------------------------------------------------


def test_unknown_function_translation_error_carries_sql(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        with pytest.raises(TranslationError) as excinfo:
            _run("SELECT this_function_does_not_exist()")
    assert "this_function_does_not_exist" in str(excinfo.value).lower()
    # The offending SQL is in the WARNING log.
    assert any("this_function_does_not_exist" in r.getMessage() for r in caplog.records)


def test_translate_wraps_catalog_executor_error_with_warning(caplog) -> None:
    """The plan: executor errors → TranslationError(...) after logger.warning.
    Test the translate() entry point, not the executor directly, so the
    wrapping behaviour is the asserted contract."""
    from slayer.facade.translator import translate
    catalog = _demo_catalog()
    sql = "SELECT no_such_fn() FROM pg_catalog.pg_namespace"
    with caplog.at_level(logging.WARNING):
        with pytest.raises(TranslationError):
            translate(sql, catalog, dialect="postgres",
                      catalog_sql_executor=_executor(catalog))
    # WARNING log carries the offending SQL.
    assert any(sql in r.getMessage() for r in caplog.records)


# --- executor_for cache -----------------------------------------------------


def test_executor_for_returns_same_instance_for_identical_catalog() -> None:
    cat1 = _demo_catalog()
    cat2 = _demo_catalog()
    a = executor_for(cat1)
    b = executor_for(cat2)
    assert a is b


def test_executor_for_invalidates_on_catalog_change() -> None:
    cat1 = _demo_catalog()
    a = executor_for(cat1)
    # Add a new model and rebuild — same datasource, but with an additional table.
    extended = build_catalog(models_by_datasource={"jaffle": [
        SlayerModel(
            name="orders", data_source="jaffle", sql_table="orders",
            description="Demo orders table",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="revenue", type=DataType.DOUBLE),
                Column(name="status", type=DataType.TEXT),
                Column(name="ordered_at", type=DataType.TIMESTAMP),
            ],
            measures=[ModelMeasure(name="aov", formula="revenue:sum / *:count", type=DataType.DOUBLE)],
        ),
        SlayerModel(
            name="customers", data_source="jaffle", sql_table="customers",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
            ],
        ),
        SlayerModel(
            name="payments", data_source="jaffle", sql_table="payments",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        ),
    ]})
    b = executor_for(extended)
    assert a is not b


def test_executor_for_distinguishes_column_order() -> None:
    """CR review: column position drives ``attnum`` / ``ordinal_position``
    / ``pg_description.objsubid``, so reordering a model's columns under
    the same name set MUST produce a new executor — otherwise the cache
    serves stale catalog metadata."""
    model_a = SlayerModel(
        name="t", data_source="ds_order_test", sql_table="t",
        columns=[
            Column(name="x", type=DataType.INT),
            Column(name="y", type=DataType.TEXT),
        ],
    )
    model_b = SlayerModel(
        name="t", data_source="ds_order_test", sql_table="t",
        columns=[
            # Same column names, swapped order.
            Column(name="y", type=DataType.TEXT),
            Column(name="x", type=DataType.INT),
        ],
    )
    cat_a = build_catalog(models_by_datasource={"ds_order_test": [model_a]})
    cat_b = build_catalog(models_by_datasource={"ds_order_test": [model_b]})
    assert executor_for(cat_a) is not executor_for(cat_b)


def test_executor_for_evicts_at_four_entries() -> None:
    # Build 6 distinct catalogs (different data source names) and confirm FIFO.
    instances = []
    for idx in range(6):
        cat = build_catalog(models_by_datasource={f"ds{idx}": [
            SlayerModel(
                name="t", data_source=f"ds{idx}", sql_table="t",
                columns=[Column(name="x", type=DataType.INT)],
            ),
        ]})
        instances.append(executor_for(cat))
    # First two should have been evicted; re-fetching them returns NEW instances.
    cat0 = build_catalog(models_by_datasource={"ds0": [
        SlayerModel(
            name="t", data_source="ds0", sql_table="t",
            columns=[Column(name="x", type=DataType.INT)],
        ),
    ]})
    refetched_0 = executor_for(cat0)
    assert refetched_0 is not instances[0]


# --- CatalogRelation dataclass-like shape -----------------------------------


def test_catalog_relation_is_pydantic_basemodel_not_dataclass() -> None:
    # Project convention: no dataclasses; everything goes through Pydantic.
    from pydantic import BaseModel
    assert issubclass(CatalogRelation, BaseModel)


# --- migrated coverage from the deleted tests/pg_facade/test_pg_catalog.py --


def test_stable_oid_is_deterministic_and_positive() -> None:
    a = stable_oid("jaffle", "orders")
    b = stable_oid("jaffle", "orders")
    assert a == b
    assert 0 <= a <= 0x7FFFFFFF
    assert stable_oid("jaffle", "orders") != stable_oid("jaffle", "customers")


def test_stable_oid_matches_crc32_not_builtin_hash() -> None:
    # Pin the exact crc32-derived value so a regression to the per-process
    # salted builtin hash() (which would NOT be stable across restarts) fails.
    import zlib
    assert stable_oid("jaffle", "orders") == zlib.crc32(b"jaffle.orders") & 0x7FFFFFFF


def test_pg_attribute_oids_match_datatype() -> None:
    from slayer.pg_facade.protocol import OID_FLOAT8, OID_TEXT
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    # Demo catalog folds datasource "jaffle" under schema "public", so table
    # OIDs are namespaced by the schema name.
    orders_oid = stable_oid("public", "orders")
    orders_attrs = {
        r["attname"]: r for r in relations["pg_attribute"].rows
        if r["attrelid"] == orders_oid
    }
    assert orders_attrs["status"]["atttypid"] == OID_TEXT
    assert orders_attrs["revenue"]["atttypid"] == OID_FLOAT8
    nums = sorted(r["attnum"] for r in orders_attrs.values())
    assert nums == list(range(1, len(nums) + 1))


def test_pg_type_covers_six_oids() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    oids = {r["oid"] for r in relations["pg_type"].rows}
    assert oids == {16, 20, 25, 701, 1082, 1114}
    by_oid = {r["oid"]: r for r in relations["pg_type"].rows}
    assert by_oid[25]["typname"] == "text"
    assert by_oid[20]["typname"] == "int8"


def test_pg_proc_is_empty() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    assert relations["pg_proc"].rows == []


def test_pg_settings_has_core_params() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    names = {r["name"] for r in relations["pg_settings"].rows}
    assert {"server_version", "client_encoding", "TimeZone"} <= names


def test_pg_namespace_has_public_and_pg_catalog() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_namespace"].rows
    names = {r["nspname"] for r in rows}
    assert names == {"public", "pg_catalog"}
    by_name = {r["nspname"]: r for r in rows}
    assert by_name["pg_catalog"]["oid"] == 11
    assert by_name["public"]["oid"] == 2200


def test_pg_class_one_row_per_model_relkind_r() -> None:
    relations = {r.name: r for r in build_catalog_relations(_demo_catalog())}
    rows = relations["pg_class"].rows
    by_name = {r["relname"]: r for r in rows}
    assert set(by_name) == {"orders", "customers"}
    assert all(r["relkind"] == "r" for r in rows)
    assert by_name["orders"]["relnatts"] > 0


def test_where_clause_now_honored_against_pg_class() -> None:
    # Old behaviour: WHERE ignored. New behaviour: WHERE filters rows.
    batch = _run("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'orders'")
    assert {r["relname"] for r in batch.rows} == {"orders"}


# --- psql backslash-command coverage stubs (DEV-XXXX) -----------------------


class TestPsqlBackslashCommandCoverage:
    """The exact ``\\d`` JOIN shape that triggered the original bug —
    joins pg_class + pg_namespace + pg_am. Fails on any drop of a stub
    or the routing allowlist."""

    def test_psql_backslash_d_join_through_pg_am_succeeds(self) -> None:
        sql = (
            "SELECT n.nspname AS schema_name, c.relname AS name, c.relkind "
            "FROM pg_catalog.pg_class AS c "
            "LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_catalog.pg_am AS am ON am.oid = c.relam "
            "WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', '') "
            "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY 1, 2"
        )
        batch = _run(sql)
        assert {r["name"] for r in batch.rows} == {"orders", "customers"}


class TestCatalogExtensibilityHook:
    """``build_catalog_relations(..., extra_relations=...)`` lets an embedder
    override a default catalog table (e.g. project real per-tenant rows
    into ``pg_roles``) or add a new one. Override is by table-name match."""

    def test_extra_relations_overrides_default_pg_roles(self) -> None:
        custom = CatalogRelation(
            name="pg_roles",
            columns=[
                FacadeColumn(name="oid", type=DataType.INT),
                FacadeColumn(name="rolname", type=DataType.TEXT),
                FacadeColumn(name="rolcanlogin", type=DataType.BOOLEAN),
            ],
            rows=[
                {"oid": 100, "rolname": "tenant_a", "rolcanlogin": True},
                {"oid": 101, "rolname": "tenant_b", "rolcanlogin": True},
            ],
        )
        relations = build_catalog_relations(
            _demo_catalog(), datasource="jaffle", extra_relations=[custom],
        )
        by_name = {r.name: r for r in relations}
        assert len(by_name["pg_roles"].rows) == 2
        assert {r["rolname"] for r in by_name["pg_roles"].rows} == {"tenant_a", "tenant_b"}

    def test_extra_relations_adds_new_table_when_no_default(self) -> None:
        custom = CatalogRelation(
            name="pg_extension",  # not in defaults
            columns=[FacadeColumn(name="extname", type=DataType.TEXT)],
            rows=[{"extname": "plpgsql"}],
        )
        relations = build_catalog_relations(
            _demo_catalog(), extra_relations=[custom],
        )
        names = {r.name for r in relations}
        assert "pg_extension" in names

    def test_executor_for_with_extras_bypasses_cache(self) -> None:
        """Extras opt out of caching (digesting row payloads would defeat
        the purpose of the fingerprint-based cache)."""
        cat = _demo_catalog()
        a = executor_for(cat, datasource="jaffle")
        b = executor_for(cat, datasource="jaffle")
        assert a is b  # cached default
        custom = CatalogRelation(
            name="pg_roles",
            columns=[FacadeColumn(name="rolname", type=DataType.TEXT)],
            rows=[{"rolname": "tenant_a"}],
        )
        with_extras = executor_for(cat, datasource="jaffle", extra_relations=[custom])
        assert with_extras is not a

    def test_extra_relations_override_visible_at_executor(self) -> None:
        """End-to-end: an override flows through ``executor_for`` so SQL
        run against the catalog DuckDB sees the new rows."""
        custom = CatalogRelation(
            name="pg_roles",
            columns=[FacadeColumn(name="rolname", type=DataType.TEXT)],
            rows=[{"rolname": "tenant_a"}, {"rolname": "tenant_b"}],
        )
        ex = executor_for(_demo_catalog(), datasource="jaffle", extra_relations=[custom])
        batch = ex.execute(
            parsed=_parse("SELECT rolname FROM pg_catalog.pg_roles ORDER BY rolname"),
            sql="SELECT rolname FROM pg_catalog.pg_roles ORDER BY rolname",
        )
        assert [r["rolname"] for r in batch.rows] == ["tenant_a", "tenant_b"]


# --- routing-allowlist parity (regression: keep _PG_CATALOG_NAMES in lockstep)


class TestCatalogRoutingAllowlistParity:
    """``is_catalog_only`` consults a hardcoded allowlist (``_PG_CATALOG_NAMES``)
    to decide whether a query routes to the catalog executor or falls through
    to user-table resolution. Adding a builder without adding the name to the
    allowlist makes the new table silently invisible at the routing layer —
    the user gets "Unknown schema: 'pg_catalog'" even though the executor
    knows about the table. Lock the two in step."""

    def test_every_pg_builder_is_in_the_routing_allowlist(self) -> None:
        from slayer.facade.catalog_sql import _PG_CATALOG_NAMES
        # Every relation the builder list emits whose name starts with `pg_`
        # must appear in the allowlist. (information_schema relations are
        # built under `_is_<name>` and use a separate allowlist.)
        relations = build_catalog_relations(_demo_catalog())
        pg_names = {r.name for r in relations if r.name.startswith("pg_")}
        missing = pg_names - _PG_CATALOG_NAMES
        assert not missing, (
            f"Built relation(s) {sorted(missing)} not in _PG_CATALOG_NAMES — "
            f"queries against them route to user-table resolution and fail "
            f"with 'Unknown schema: pg_catalog'."
        )

    @pytest.mark.parametrize("table", ["pg_am", "pg_roles", "pg_database"])
    def test_is_catalog_only_recognises_new_pg_tables(self, table: str) -> None:
        from slayer.facade.catalog_sql import is_catalog_only
        sql = f"SELECT * FROM pg_catalog.{table}"
        assert is_catalog_only(_parse(sql)) is True
        # Bare names too (without the pg_catalog. qualifier).
        bare = f"SELECT * FROM {table}"
        assert is_catalog_only(_parse(bare)) is True

    def test_psql_backslash_d_query_is_catalog_only(self) -> None:
        """The actual psql-emitted ``\\d`` shape (multi-JOIN through
        pg_class + pg_namespace + pg_am) must route to the catalog
        executor — not to user-table resolution."""
        from slayer.facade.catalog_sql import is_catalog_only
        sql = (
            "SELECT n.nspname, c.relname, c.relkind "
            "FROM pg_catalog.pg_class c "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam "
            "WHERE c.relkind IN ('r', 'v') "
            "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY 1, 2"
        )
        assert is_catalog_only(_parse(sql)) is True


# --- Bare-the-call rewrite for unknown ``pg_catalog.<fn>`` ------------------


class TestUnknownSchemaQualifiedCallRewrite:
    """Unknown ``pg_catalog.<fn>(...)`` / ``information_schema.<fn>(...)``
    calls have their schema qualifier stripped — otherwise DuckDB parses
    the Dot as ``<schema>.<attr>`` (a column reference), which surfaces a
    misleading "column not found in FROM clause" Binder error. The fix
    lets native DuckDB functions resolve cleanly and surfaces a sensible
    "function not found" for truly-unknown names."""

    def test_psql_backslash_l_query_executes(self) -> None:
        """psql's ``\\l`` calls ``pg_catalog.array_to_string`` (native to
        DuckDB but not in our stub maps). Pre-fix: silently failed with
        a misleading column-reference error."""
        sql = (
            "SELECT d.datname AS \"Name\", "
            "pg_catalog.pg_get_userbyid(d.datdba) AS \"Owner\", "
            "pg_catalog.pg_encoding_to_char(d.encoding) AS \"Encoding\", "
            "d.datcollate AS \"Collate\", "
            "pg_catalog.array_to_string(d.datacl, e'\\n') AS \"Access privileges\" "
            "FROM pg_catalog.pg_database AS d ORDER BY 1"
        )
        batch = _run(sql)
        assert len(batch.rows) == 1
        row = batch.rows[0]
        assert row["Name"] == "jaffle"   # _demo_catalog's datasource
        assert row["Owner"] == "slayer"
        assert row["Encoding"] == "UTF8"

    def test_unknown_schema_qualified_call_is_bared(self) -> None:
        """``pg_catalog.<fn>`` not in our stub maps is rewritten to a bare
        call so DuckDB can resolve native functions transparently."""
        # ``upper`` is native to DuckDB, NOT in _CONSTANT_STUB_LITERALS
        # or _LOOKUP_STUB_NAMES. Pre-fix would fail with "column not found".
        batch = _run("SELECT pg_catalog.upper('hello') AS u")
        assert batch.rows == [{"u": "HELLO"}]


# --- Review #4: regclass_map registers the real (schema, table) key ---------


class TestRegclassMapCustomSchema:
    """Pre-fix the regclass map only knew ``public.<table>`` and bare
    ``<table>``. For custom postgres_schema datasources, Metabase's
    ``COL_DESCRIPTION(FORMAT('%I.%I', table_schema, table_name)::regclass, …)``
    looked up ``people.employees`` and got OID 0, silently dropping all
    column descriptions in the BI tool."""

    def test_regclass_map_includes_real_schema_qualified_form(self) -> None:
        # Build a catalog whose datasource has a custom postgres_schema.
        cat = build_catalog_grouped_by_schema(
            models_by_datasource={"hr": [
                SlayerModel(name="employees", data_source="hr", sql_table="employees",
                            columns=[Column(name="id", type=DataType.INT, primary_key=True)]),
            ]},
            schema_by_datasource={"hr": "people"},
        )
        ex = CatalogSqlExecutor(catalog=cat, datasource="hr")
        # All three lookup forms resolve to the same non-zero OID — the
        # real ``people.employees`` form is what Metabase's COL_DESCRIPTION
        # uses, and was missing pre-fix.
        oid_qualified = ex._regclass_map.get("people.employees")
        oid_public = ex._regclass_map.get("public.employees")
        oid_bare = ex._regclass_map.get("employees")
        assert oid_qualified is not None and oid_qualified > 0
        assert oid_qualified == oid_public == oid_bare


# --- Review #3: public.<table> fallback narrowed --------------------------


class TestPublicSchemaFallbackGuard:
    """The ``schema='public'`` fall-back now only fires when the catalog
    has a single schema. With multiple schemas (custom postgres_schema in
    use), ``public.<table>`` raises "Unknown schema" instead of silently
    crossing isolation. The single-schema case (the common Metabase
    deployment) keeps working."""

    def test_public_fallback_works_with_single_schema(self) -> None:
        """Backward-compat: one-datasource catalog accepts ``public.X``
        as an alias for whatever the real schema name happens to be."""
        from slayer.facade.translator import translate
        sql = "SELECT id FROM public.orders"
        result = translate(sql, _demo_catalog(), dialect="postgres")
        assert result.facade_table.name == "orders"

    def test_public_fallback_rejected_with_multiple_schemas(self) -> None:
        """Isolation: with two datasources / two schemas, ``public.X``
        can't reach across — caller must qualify with the real schema."""
        from slayer.facade.translator import translate, TranslationError
        cat_a = build_catalog(models_by_datasource={"sales": [
            SlayerModel(name="orders", data_source="sales", sql_table="orders",
                        columns=[Column(name="id", type=DataType.INT, primary_key=True)]),
        ]})
        cat_b = build_catalog(models_by_datasource={"hr": [
            SlayerModel(name="people", data_source="hr", sql_table="people",
                        columns=[Column(name="id", type=DataType.INT, primary_key=True)]),
        ]})
        # Merge them into one multi-schema catalog (mimics the
        # postgres_schema-per-datasource layout).
        from slayer.facade.catalog import FacadeCatalog as _FC
        multi = _FC(
            catalog_name=cat_a.catalog_name,
            schemas=list(cat_a.schemas) + list(cat_b.schemas),
        )
        with pytest.raises(TranslationError, match="Unknown schema"):
            translate("SELECT id FROM public.orders", multi, dialect="postgres")


# --- Postgres schema-qualified operator + COLLATE stripping -----------------


class TestSchemaQualifiedOperatorRewrite:
    """psql emits ``OPERATOR(pg_catalog.<op>)`` for ``\\du <pattern>`` /
    ``\\d <pattern>`` etc. plus ``COLLATE pg_catalog.default`` alongside.
    DuckDB has no OPERATOR() syntax and no ``pg_catalog`` collation
    namespace; both must be rewritten to bare DuckDB equivalents."""

    @pytest.mark.parametrize("op,pattern,expected", [
        ("~", "^slayer$", [{"rolname": "slayer"}]),         # case-sensitive match
        ("~*", "^SLAYER$", [{"rolname": "slayer"}]),        # case-insensitive match
        ("!~", "^slayer$", []),                             # negated: default row matches
        ("!~*", "^SLAYER$", []),                            # negated case-insensitive
    ])
    def test_pg_catalog_regex_operator(
        self, op: str, pattern: str, expected: list,
    ) -> None:
        """The four schema-qualified regex operator variants map to
        ``regexp_matches`` / ``NOT regexp_matches``."""
        batch = _run(
            f"SELECT rolname FROM pg_catalog.pg_roles "
            f"WHERE rolname OPERATOR(pg_catalog.{op}) '{pattern}'"
        )
        assert batch.rows == expected

    def test_collate_clause_stripped(self) -> None:
        # ``COLLATE pg_catalog.default`` is dropped; the match proceeds
        # byte-wise, which is the right semantic for ASCII catalog names.
        batch = _run(
            "SELECT rolname FROM pg_catalog.pg_roles "
            "WHERE rolname OPERATOR(pg_catalog.~) '^slayer$' COLLATE pg_catalog.default"
        )
        assert batch.rows == [{"rolname": "slayer"}]

    def test_psql_backslash_du_pattern_query_executes(self) -> None:
        """The exact ``\\du users`` shape combining both rewrites."""
        sql = (
            "SELECT r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole, "
            "r.rolcreatedb, r.rolcanlogin, r.rolconnlimit, r.rolvaliduntil, "
            "r.rolreplication, r.rolbypassrls "
            "FROM pg_catalog.pg_roles AS r "
            "WHERE r.rolname OPERATOR(pg_catalog.~) '^(users)$' "
            "COLLATE pg_catalog.default ORDER BY 1"
        )
        # Executes cleanly — the default ``slayer`` role doesn't match
        # ``^(users)$`` so we get zero rows, not an error.
        batch = _run(sql)
        assert batch.rows == []


# --- CAST(x AS pg_catalog.<type>) rewrite -----------------------------------


class TestSchemaQualifiedCastRewrite:
    """psql's ``\\d <table>`` casts to Postgres-specific ``pg_catalog.<type>``
    names (``pg_catalog.regtype``, ``pg_catalog.text``, ``pg_catalog.oid``…).
    DuckDB rejects those; each must be rewritten to a DuckDB equivalent."""

    def test_pg_catalog_text_becomes_varchar(self) -> None:
        batch = _run("SELECT CAST('hello' AS pg_catalog.text) AS s")
        assert batch.rows == [{"s": "hello"}]

    def test_pg_catalog_oid_becomes_bigint(self) -> None:
        # Plain ``oid`` is a bare integer type — no OID-lookup semantics.
        batch = _run("SELECT CAST(100 AS pg_catalog.oid) AS v")
        assert batch.rows == [{"v": 100}]

    def test_pg_catalog_regclass_string_lookup(self) -> None:
        """``'pg_class'::pg_catalog.regclass`` normalizes to bare
        ``regclass`` and gets the OID-lookup semantics of the existing
        pass — critical for psql's ``\\dx`` which classoid-checks with
        ``'pg_catalog.pg_extension'::pg_catalog.regclass``. Pre-fix, the
        pg_catalog qualifier caused this to coerce silently to BIGINT,
        losing the lookup."""
        batch = _run("SELECT CAST('pg_class' AS pg_catalog.regclass) AS o")
        # ``pg_class`` is in KNOWN_SYSTEM_OIDS with the canonical OID 1259.
        assert batch.rows == [{"o": 1259}]

    def test_pg_catalog_regtype_string_lookup(self) -> None:
        # ``int8`` is a known type — resolves to its wire OID.
        from slayer.pg_facade.protocol import OID_INT8
        batch = _run("SELECT CAST('int8' AS pg_catalog.regtype) AS o")
        assert batch.rows == [{"o": OID_INT8}]

    def test_pg_catalog_regproc_returns_zero(self) -> None:
        # regproc is unconditionally 0 — matches bare ``::regproc``.
        batch = _run("SELECT CAST('some_fn' AS pg_catalog.regproc) AS r")
        assert batch.rows == [{"r": 0}]

    def test_unknown_pg_catalog_type_falls_back_to_varchar(self) -> None:
        # ``some_unknown_type`` won't be in the map; fallback is VARCHAR.
        batch = _run("SELECT CAST('x' AS pg_catalog.some_unknown_type) AS v")
        assert batch.rows == [{"v": "x"}]

    def test_bare_type_names_still_work(self) -> None:
        # The rewriter is scoped to USERDEFINED types with dotted kinds;
        # a bare ``CAST(... AS text)`` must be untouched.
        batch = _run("SELECT CAST(42 AS text) AS s")
        assert batch.rows == [{"s": "42"}]


class TestPgClassPsqlDescribeCompleteness:
    """The exact ``\\d <table>`` shape combining pg_class + pg_am JOIN +
    schema-qualified CAST + self-JOIN through reltoastrelid. Reads every
    column psql's describe query touches — will fail if any of them are
    missing from ``pg_class``."""

    def test_psql_backslash_d_query_end_to_end(self) -> None:
        sql = (
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, "
            "c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, "
            "FALSE AS relhasoids, c.relispartition, '', c.reltablespace, "
            "CASE WHEN c.reloftype = 0 THEN '' "
            "ELSE CAST(CAST(c.reloftype AS pg_catalog.regtype) AS pg_catalog.text) END, "
            "c.relpersistence, c.relreplident, am.amname "
            "FROM pg_catalog.pg_class AS c "
            "LEFT JOIN pg_catalog.pg_class AS tc ON (c.reltoastrelid = tc.oid) "
            "LEFT JOIN pg_catalog.pg_am AS am ON (c.relam = am.oid) "
            "WHERE c.relname = 'orders'"
        )
        batch = _run(sql)
        assert len(batch.rows) == 1
        row = batch.rows[0]
        assert row["relkind"] == "r"
        assert row["amname"] == "heap"
        assert row["relreplident"] == "d"


# --- pg_collation stub + typcollation completeness -------------------------


class TestPgCollationAndTypcollation:
    """psql's ``\\d <table>`` runs a sub-select through pg_collation JOIN
    pg_type to detect non-default collations. Pre-fix: pg_collation
    wasn't in the routing allowlist, and pg_type lacked ``typcollation``
    — the query fell through to user-table resolution and yielded
    "Unknown schema: 'pg_catalog'", or a misleading DuckDB "Referenced
    table 't' not found" (which really means a missing column)."""

    def test_pg_collation_routes_and_returns_empty(self) -> None:
        batch = _run("SELECT * FROM pg_catalog.pg_collation")
        # Empty is the right shape — every column uses the default
        # collation (attcollation = 0), so the JOIN yields no rows.
        assert batch.rows == []

    def test_psql_backslash_d_attribute_subselect(self) -> None:
        """The correlated ``pg_collation``/``pg_type`` sub-select from
        ``\\d <table>`` executes end-to-end (previously failed on the
        missing pg_type.typcollation column with a misleading binder
        error)."""
        sql = (
            "SELECT a.attname, "
            "(SELECT c.collname FROM pg_catalog.pg_collation AS c, "
            "pg_catalog.pg_type AS t "
            "WHERE c.oid = a.attcollation AND t.oid = a.atttypid "
            "AND a.attcollation <> t.typcollation) AS attcollation "
            "FROM pg_catalog.pg_attribute AS a "
            "WHERE a.attrelid IN "
            "(SELECT oid FROM pg_catalog.pg_class WHERE relname = 'orders')"
        )
        batch = _run(sql)
        # Rows returned — one per column of the orders model.
        assert len(batch.rows) > 0
        # The correlated attcollation sub-select is NULL for every row
        # (every column uses the default collation).
        assert all(r["attcollation"] is None for r in batch.rows)


# --- Empty stubs for the psql "\d <table>" fan-out -------------------------


class TestEmptyCatalogStubs:
    """psql's ``\\d <table>`` runs 6-10 sub-queries against catalog tables
    for triggers / inheritance / publications / partitioning / view
    rewrite / extensions. SLayer has none of these by construction, so
    every stub is legitimately empty — matching real Postgres."""

    _STUB_TABLES = [
        "pg_trigger", "pg_inherits",
        "pg_publication", "pg_publication_rel", "pg_publication_tables",
        "pg_partitioned_table", "pg_rewrite", "pg_extension",
    ]

    @pytest.mark.parametrize("table", _STUB_TABLES)
    def test_stub_is_routed_and_returns_empty(self, table: str) -> None:
        # Routing parity is covered structurally by
        # ``test_every_pg_builder_is_in_the_routing_allowlist``; this
        # only re-verifies the end-to-end execute path per stub.
        batch = _run(f"SELECT * FROM pg_catalog.{table}")
        assert batch.rows == []

    def test_psql_backslash_d_policy_subquery(self) -> None:
        """Real ``\\d`` RLS sub-query — the shape that motivated the
        ``pg_policy`` stub in the first place."""
        sql = (
            "SELECT pol.polname, pol.polpermissive, "
            "pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) "
            "FROM pg_catalog.pg_policy AS pol WHERE pol.polrelid = 999"
        )
        assert _run(sql).rows == []

    def test_psql_backslash_d_trigger_subquery(self) -> None:
        """The "Triggers:" section query psql emits per relation."""
        sql = (
            "SELECT t.tgname, t.tgenabled, t.tgisinternal "
            "FROM pg_catalog.pg_trigger AS t "
            "WHERE t.tgrelid = 999 AND NOT t.tgisinternal ORDER BY 1"
        )
        assert _run(sql).rows == []

    def test_psql_backslash_dx_extension_query(self) -> None:
        """``\\dx`` — list installed extensions. Empty for SLayer."""
        sql = (
            "SELECT e.extname, e.extversion, n.nspname, c.description "
            "FROM pg_catalog.pg_extension e "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace "
            "LEFT JOIN pg_catalog.pg_description c "
            "ON c.objoid = e.oid AND c.classoid = 'pg_catalog.pg_extension'::pg_catalog.regclass "
            "ORDER BY 1"
        )
        assert _run(sql).rows == []


# --- On-the-fly synthesis for unknown catalog tables -----------------------


class TestSynthesisFallback:
    """When a query references ``pg_catalog.<x>`` / ``information_schema.<x>``
    for a table we haven't explicitly cataloged, synthesize an empty
    relation inline instead of erroring. Columns are discovered from
    same-query column refs. Never fires for user-schema refs — those
    still surface as loud "unknown table" errors."""

    def test_unknown_pg_catalog_table_synthesized_empty(self, caplog) -> None:
        """``pg_statistic_ext`` — a real one we hit in the wild that
        wasn't in the explicit stub set."""
        with caplog.at_level(logging.WARNING, logger="slayer.facade.catalog_sql"):
            batch = _run(
                "SELECT stxname, stxrelid FROM pg_catalog.pg_statistic_ext "
                "WHERE stxrelid = 100"
            )
        assert batch.rows == []
        # WARN log fired with the friendly diagnostic.
        assert any(
            "pg_statistic_ext" in r.getMessage()
            for r in caplog.records
        )

    def test_unknown_information_schema_table_synthesized_empty(self) -> None:
        """Same pattern for information_schema — psql / Metabase touch
        obscure ones like ``information_schema.character_sets``."""
        batch = _run(
            "SELECT character_set_name FROM information_schema.character_sets"
        )
        assert batch.rows == []

    def test_column_discovery_via_alias(self) -> None:
        """Column refs qualified by an alias (``e.something``) are
        discovered and appear in the synthetic relation's column set."""
        batch = _run(
            "SELECT e.custom_column, e.other_col "
            "FROM pg_catalog.pg_totally_fictional AS e"
        )
        # DuckDB is happy — the synthetic table exposes both columns.
        assert batch.rows == []

    def test_bare_column_ref_still_discovered(self) -> None:
        """Column refs without a table qualifier (bare ``foo``) are picked
        up when the FROM table has no alias — the common shape psql uses."""
        batch = _run(
            "SELECT some_col FROM pg_catalog.pg_absent WHERE another_col = 42"
        )
        assert batch.rows == []

    def test_user_schema_typo_still_errors_loudly(self) -> None:
        """Safety: an unknown ``public.<x>`` reference must NOT be
        synthesized — that would mask user-table typos silently. Only
        pg_catalog / information_schema refs get the graceful fallback."""
        with pytest.raises(TranslationError):
            _run("SELECT * FROM public.no_such_table_here")

    def test_bare_unknown_name_not_synthesized(self) -> None:
        """A bare ``foo`` (no schema qualifier) that doesn't match any
        known pg_catalog name is NOT synthesized. Falls through to normal
        user-table resolution and errors loudly."""
        with pytest.raises(TranslationError):
            _run("SELECT * FROM totally_made_up_bare_name")

    def test_synthesis_with_pg_catalog_helper_stub_calls(self, caplog) -> None:
        """The full ``\\d`` pg_statistic_ext query from the wild:
        combines synthesis + regclass casts + the new pg_get_* helper
        stubs + ANY() array subscript."""
        sql = (
            "SELECT oid, CAST(stxrelid AS pg_catalog.regclass), "
            "CAST(CAST(stxnamespace AS pg_catalog.regnamespace) AS pg_catalog.text) AS nsp, "
            "stxname, pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns, "
            "'d' = ANY(stxkind) AS ndist_enabled, "
            "stxstattarget FROM pg_catalog.pg_statistic_ext "
            "WHERE stxrelid = 100 ORDER BY nsp, stxname"
        )
        batch = _run(sql)
        assert batch.rows == []


# --- Unknown pg_* function fallback ----------------------------------------


class TestUnknownPgFunctionFallback:
    """Any lingering ``pg_*`` Anonymous call not covered by a known stub
    gets rewritten to NULL. Prevents whack-a-mole every time a new BI
    tool release calls yet another obscure Postgres helper."""

    def test_unknown_pg_helper_becomes_null(self, caplog) -> None:
        """``pg_relation_is_publishable`` — a real psql-emitted helper
        we didn't stub explicitly."""
        with caplog.at_level(logging.WARNING, logger="slayer.facade.catalog_sql"):
            batch = _run(
                "SELECT pg_catalog.pg_relation_is_publishable(100) AS v"
            )
        assert batch.rows == [{"v": None}]
        assert any(
            "pg_relation_is_publishable" in r.getMessage()
            for r in caplog.records
        )

    def test_bare_unknown_pg_call_also_stubbed(self, caplog) -> None:
        """Bare ``pg_foo(...)`` (no schema qualifier) also stubs to NULL —
        real queries mix qualified and bare forms."""
        with caplog.at_level(logging.WARNING, logger="slayer.facade.catalog_sql"):
            batch = _run("SELECT pg_some_random_helper('x', 42) AS v")
        assert batch.rows == [{"v": None}]

    def test_known_stubs_still_take_precedence(self) -> None:
        """The fallback runs AFTER the explicit-stub passes —
        ``pg_get_userbyid`` still returns 'slayer', not NULL."""
        batch = _run("SELECT pg_catalog.pg_get_userbyid(10) AS u")
        assert batch.rows == [{"u": "slayer"}]

    def test_non_pg_prefixed_unknown_function_still_errors(self) -> None:
        """Safety: user-defined SQL functions (no ``pg_`` prefix) are
        left alone — typos in user queries surface loudly."""
        with pytest.raises(TranslationError):
            _run("SELECT some_unknown_udf(42) AS v")


class TestRegclassCastOnNumericExpr:
    """``CAST(<numeric_column> AS regclass)`` — psql's ``\\d`` emits this
    pattern for inheritance / partitioning queries (``CAST(c.oid AS
    regclass)``). Pre-fix, the rewrite wrapped it in
    ``slayer_regclass_oid(<expr>)`` which is a VARCHAR-only UDF — Binder
    Error on any BIGINT arg. Now: pass through unchanged (the OID value
    itself is the most useful answer we can produce for our minimal
    catalog)."""

    def test_regclass_cast_on_numeric_column_passes_through(self) -> None:
        # Uses pg_class.oid which is INT in our schema.
        batch = _run("SELECT CAST(c.oid AS pg_catalog.regclass) AS r FROM pg_catalog.pg_class AS c")
        # Rows come back — values are the oid values themselves.
        assert len(batch.rows) > 0
        assert all(isinstance(r["r"], int) for r in batch.rows)

    def test_regclass_cast_on_string_literal_still_looks_up(self) -> None:
        """Regression: the literal-lookup case still works (this is what
        the UDF was built for). ``'pg_class'::regclass → 1259``."""
        batch = _run("SELECT CAST('pg_class' AS pg_catalog.regclass) AS r")
        assert batch.rows == [{"r": 1259}]



class TestPR213ReviewFixes:
    """Regression tests for the four CodeRabbit findings on PR #213."""

    def test_regclass_cast_from_numeric_literal_passes_through(self) -> None:
        """``CAST(0 AS pg_catalog.regclass)`` = InvalidOid in Postgres.
        Pre-fix, this fell through to the VARCHAR-only UDF and errored
        on the INTEGER argument. Now: pass through unchanged (numeric
        literals join column refs on the fast path)."""
        batch = _run("SELECT CAST(0 AS pg_catalog.regclass) AS r")
        assert batch.rows == [{"r": 0}]

    @pytest.mark.parametrize("pg_type", [
        "regoper", "regoperator", "regprocedure",
    ])
    def test_extended_reg_family_casts_return_zero(self, pg_type: str) -> None:
        """``regoper`` / ``regoperator`` / ``regprocedure`` are in
        ``_PG_REG_TYPES`` (normalized to bare types) but pre-fix,
        ``_rewrite_regproc_regtype_casts`` only knew ``regproc`` and
        ``regtype`` — the others fell through as unsupported types."""
        batch = _run(f"SELECT CAST('anything' AS pg_catalog.{pg_type}) AS r")
        assert batch.rows == [{"r": 0}]

    def test_information_schema_synthesis_uses_is_alias(self) -> None:
        """For synthesized ``information_schema.<x>`` tables without a
        user alias, the subquery alias must be ``_is_<x>`` because
        ``_strip_column_schema_qualifiers`` later rewrites bare-column
        qualifiers to that name. Pre-fix, alias/qualifier mismatched
        and DuckDB missed the columns."""
        # ``character_sets`` isn't in the info-schema stub set — synthesized.
        # This asserts the synthesis wiring: rewrite must resolve.
        batch = _run(
            "SELECT character_set_name FROM information_schema.character_sets"
        )
        assert batch.rows == []

    def test_synthesized_column_with_quote_in_name_does_not_break_sql(self) -> None:
        """Column identifiers containing ``"`` must be escaped before
        embedding into the synth SQL — otherwise a malicious or oddly-
        named identifier could malform the parse."""
        # Sqlglot happily parses double-quoted identifiers with escaped
        # inner quotes (``"foo""bar"``). Use one via an unknown catalog.
        batch = _run(
            'SELECT "foo""bar" FROM pg_catalog.pg_totally_unknown'
        )
        # Executes cleanly, returns empty rows with the odd column name.
        assert batch.rows == []
