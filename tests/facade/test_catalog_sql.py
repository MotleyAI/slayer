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
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.catalog_sql import (
    KNOWN_SYSTEM_OIDS,
    CatalogRelation,
    CatalogSqlExecutor,
    build_catalog_relations,
    executor_for,
    is_catalog_only,
    stable_oid,
)
from slayer.facade.rows import RowBatch
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
    return build_catalog(models_by_datasource={"jaffle": [orders, customers]})


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
    cat = build_catalog(models_by_datasource={"jaffle": [sql_model]})
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
    assert "id" in cols or "revenue" in cols


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
    assert all(r["catalog_name"] == "jaffle" for r in metrics_rows)
    dims_rows = relations["_is_dimensions"].rows
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
    orders_oid = stable_oid("jaffle", "orders")
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
