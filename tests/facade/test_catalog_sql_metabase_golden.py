"""Golden-corpus tests against the captured Metabase v0.62.1.5 schema-sync SQL.

The corpus (`fixtures/metabase_v0_62_sync_corpus.sql`) was captured live from a
Metabase v0.62.1.5 container talking to a real Postgres 16 with 2 demo tables.
20 distinct statements + their observed bound parameter values. These tests
run each non-no-op catalog statement through the new ``CatalogSqlExecutor``
and assert (a) it reaches the executor (i.e. the translator returns a
``PgCatalogResult``), (b) the result column names match Metabase's expected
projection list (case-sensitive, including pgjdbc aliases like ``TABLE_SCHEM``),
(c) for content-driven queries (#8, #9, #12, #13, #15) the row count and a
representative row contain what Metabase needs to render the demo's tables and
descriptions. SET/SHOW statements assert ``NoOpResult`` via the existing
translator path (not the executor).
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.catalog_sql import CatalogSqlExecutor, executor_for
from slayer.facade.translator import (
    NoOpResult,
    PgCatalogResult,
    ProbeResult,
    translate,
)


FIXTURE = Path(__file__).parent / "fixtures" / "metabase_v0_62_sync_corpus.sql"


def _demo_catalog() -> FacadeCatalog:
    """A 2-table catalog mirroring the captured Postgres (public.orders + public.customers)."""
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        description="Demo orders table",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="total", type=DataType.DOUBLE, description="Order total"),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )
    customers = SlayerModel(
        name="customers", data_source="jaffle", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="email", type=DataType.TEXT, description="Customer email"),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, customers]})


def _seven_table_demo_catalog() -> FacadeCatalog:
    """7-table jaffle-shop-shaped catalog so #9 asserts ``count == 7``."""
    models = [
        SlayerModel(
            name=tn, data_source="jaffle", sql_table=tn,
            description=f"jaffle {tn}",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        )
        for tn in ("customers", "orders", "products", "stores", "supplies",
                   "items", "tweets")
    ]
    return build_catalog(models_by_datasource={"jaffle": models})


def _executor(catalog: FacadeCatalog) -> CatalogSqlExecutor:
    return executor_for(catalog)


def _pg_probe_matcher(catalog: FacadeCatalog):
    """Build a probe matcher that matches what the pg facade installs at
    runtime (DEV-1556 added SHOW + current_catalog/current_user/etc.).
    The corpus tests rely on this so SHOW statements are routed to a
    ProbeResult rather than to the noop classifier."""
    from slayer.pg_facade.identity import version_string
    from slayer.pg_facade.probes import match_pg_probe
    from slayer.facade.probe_queries import match_probe as facade_match_probe
    datasource = catalog.schemas[0].name if catalog.schemas else catalog.catalog_name

    def matcher(parsed):
        pg = match_pg_probe(parsed, datasource=datasource,
                            version_str=version_string())
        if pg is not None:
            return pg
        return facade_match_probe(parsed)
    return matcher


def _translate(sql: str, catalog: FacadeCatalog):
    return translate(sql, catalog, dialect="postgres",
                     probe_matcher=_pg_probe_matcher(catalog),
                     catalog_sql_executor=_executor(catalog))


# --- corpus loading ---------------------------------------------------------


def _load_corpus_statements() -> List[str]:
    """Yield each ``-- #N ...`` block from the captured corpus as a single string."""
    text = FIXTURE.read_text(encoding="utf-8")
    statements: List[str] = []
    current: List[str] = []
    capturing = False
    for line in text.splitlines():
        if line.startswith("-- #") and " first seen at " in line:
            if current:
                statements.append("\n".join(current).strip())
            current = []
            capturing = False
            continue
        if line.startswith("-- ====") or line.startswith("--"):
            # Header / footer / appendix comment — skip
            continue
        capturing = True
        current.append(line)
    if current and capturing:
        statements.append("\n".join(current).strip())
    # Strip trailing semicolons and empty entries.
    return [s.rstrip(";").strip() for s in statements if s.strip() and not s.strip().startswith("--")]


def test_corpus_fixture_contains_twenty_statements() -> None:
    """Pin the corpus shape so accidental fixture edits surface."""
    assert FIXTURE.exists()
    statements = _load_corpus_statements()
    assert len(statements) == 20, statements


# The fixture contains statements that are excluded from the fixture-load
# smoke test, for two reasons:
#   #8  Metabase's table-privileges CTE aliases columns ``AS select``,
#        ``AS update``, ``AS delete`` — Postgres tolerates SQL-keyword aliases
#        without quoting, sqlglot does not. The shape is covered explicitly
#        in ``test_corpus_08_table_privileges_cte`` with renamed aliases.
#   #19 `SELECT '=== GUI QUESTION ===' AS marker` — divider injected via
#        ``psql -c`` by the capture script to mark the boundary between sync
#        traffic and the GUI question. Not Metabase-emitted.
EXCLUDED_FIXTURE_INDICES = {8, 19}


def test_fixture_statements_translate_without_crashing() -> None:
    """Smoke test — every fixture statement (minus the divider) must reach
    a `TranslatorResult` of some kind via the unified translate() entry
    point. Catches drift between the corpus and the inline hand-written tests
    above (which substitute observed param values; the smoke test does NOT —
    so #N statements containing `$1` will skip if the substitution can't be
    made)."""
    statements = _load_corpus_statements()
    catalog = _demo_catalog()
    failures: list[tuple[int, str]] = []
    for idx, sql in enumerate(statements, start=1):
        if idx in EXCLUDED_FIXTURE_INDICES:
            continue
        if "$" in sql:
            # Bound-parameter statements: the pg facade's _handle_bind does the
            # literal substitution before translate(), so a raw $N here would
            # never reach the executor. Skip — the inline tests cover these.
            continue
        try:
            _translate(sql, catalog)
        except Exception as exc:  # noqa: BLE001 — we want any failure surfaced
            failures.append((idx, f"{type(exc).__name__}: {exc}"))
    assert not failures, "Fixture statements failed translate():\n" + \
        "\n".join(f"  #{n}: {msg}" for n, msg in failures)


# --- NoOp statements (SET / SHOW) go through translator, not executor -------


@pytest.mark.parametrize(("sql", "tag"), [
    ("SET application_name = 'x'", "SET"),
    ("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED", "SET"),
    ("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", "SET"),
])
def test_corpus_set_statements_route_to_noop_result(sql: str, tag: str) -> None:
    """SET goes through `_classify_noop_root`; the executor never sees it."""
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, NoOpResult)
    assert result.command_tag == tag


@pytest.mark.parametrize("sql", [
    "SHOW TRANSACTION ISOLATION LEVEL",
    "show timezone",
])
def test_corpus_show_statements_route_to_probe_result(sql: str) -> None:
    """SHOW is intercepted by the probe matcher (DEV-1556) before the
    executor; result is a ProbeResult with a single-row setting payload."""
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, ProbeResult)
    assert len(result.batch.rows) == 1


def test_corpus_select_1_routes_to_probe_with_one_row() -> None:
    # SELECT 1 — handled by the probe matcher (returns a single one-row batch
    # carrying the integer 1). Tighter than `is not None` so a broken impl
    # returning an empty result would still fail.
    result = _translate("SELECT 1", _demo_catalog())
    assert isinstance(result, ProbeResult)
    assert len(result.batch.rows) == 1
    assert list(result.batch.rows[0].values()) == [1]


def test_corpus_select_current_catalog_routes_to_probe() -> None:
    # DEV-1556 added a probe for `select current_catalog`; the datasource
    # name (`jaffle`) surfaces verbatim.
    result = _translate("select current_catalog", _demo_catalog())
    assert isinstance(result, ProbeResult)
    assert result.batch.rows == [{"current_catalog": "jaffle"}]


# --- catalog SQL via the executor ------------------------------------------


def test_corpus_07_get_schemas_pgjdbc() -> None:
    sql = (
        'SELECT nspname AS "TABLE_SCHEM", current_database() AS "TABLE_CATALOG" '
        "FROM pg_catalog.pg_namespace "
        "WHERE nspname <> 'pg_toast' "
        "AND (nspname !~ '^pg_temp_' OR nspname = (pg_catalog.current_schemas(true))[1]) "
        "AND (nspname !~ '^pg_toast_temp_' OR nspname = "
        "    replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) "
        'ORDER BY "TABLE_SCHEM"'
    )
    catalog = _demo_catalog()
    result = _translate(sql, catalog)
    assert isinstance(result, PgCatalogResult)
    # pgjdbc projects TABLE_SCHEM and TABLE_CATALOG with quoted aliases.
    assert [c.name for c in result.batch.columns] == ["TABLE_SCHEM", "TABLE_CATALOG"]
    # public is always present; pg_catalog filtered out by `nspname !~ '^pg_'`? No — the
    # corpus query keeps pg_catalog because the filter targets pg_toast and pg_temp,
    # not pg_catalog. So both rows surface.
    schemas = {r["TABLE_SCHEM"] for r in result.batch.rows}
    assert "public" in schemas
    # current_database() rewritten to the datasource name.
    assert all(r["TABLE_CATALOG"] == "jaffle" for r in result.batch.rows)


def test_corpus_08_table_privileges_cte() -> None:
    # The corpus's #8 — the table-privileges CTE.  Validates the CTE branch of
    # is_catalog_only AND the privilege-stub macros.
    sql = """
    WITH table_privileges AS (
        SELECT
            NULL as role,
            t.schemaname as schema,
            t.objectname as table,
            pg_catalog.has_any_column_privilege(current_user,
                '"' || replace(t.schemaname, '"', '""') || '"' || '.' ||
                '"' || replace(t.objectname, '"', '""') || '"', 'update') as update_priv,
            pg_catalog.has_any_column_privilege(current_user,
                '"' || replace(t.schemaname, '"', '""') || '"' || '.' ||
                '"' || replace(t.objectname, '"', '""') || '"', 'select') as select_priv,
            pg_catalog.has_table_privilege(current_user,
                '"' || replace(t.schemaname, '"', '""') || '"' || '.' ||
                '"' || replace(t.objectname, '"', '""') || '"', 'delete') as delete_priv
        FROM (
            SELECT schemaname, tablename AS objectname FROM pg_catalog.pg_tables
            UNION
            SELECT schemaname, viewname AS objectname FROM pg_catalog.pg_views
            UNION
            SELECT schemaname, matviewname AS objectname FROM pg_catalog.pg_matviews
        ) t
        WHERE t.schemaname !~ '^pg_'
          AND t.schemaname <> 'information_schema'
          AND pg_catalog.has_schema_privilege(current_user, t.schemaname, 'usage')
    )
    SELECT t.* FROM table_privileges t
    """
    catalog = _demo_catalog()
    result = _translate(sql, catalog)
    assert isinstance(result, PgCatalogResult)
    rows = result.batch.rows
    # Two rows (orders + customers).
    assert {r["table"] for r in rows} == {"orders", "customers"}
    # Privilege stubs return True.
    assert all(r["update_priv"] is True for r in rows)
    assert all(r["select_priv"] is True for r in rows)
    assert all(r["delete_priv"] is True for r in rows)


def test_corpus_09_describe_tables_join_seven_tables() -> None:
    sql = """
    SELECT "n"."nspname" AS "schema",
           "c"."relname" AS "name",
           CASE "c"."relkind"
                WHEN 'r' THEN 'TABLE'
                WHEN 'p' THEN 'PARTITIONED TABLE'
                WHEN 'v' THEN 'VIEW'
                WHEN 'f' THEN 'FOREIGN TABLE'
                WHEN 'm' THEN 'MATERIALIZED VIEW'
                ELSE NULL END AS "type",
           "d"."description" AS "description",
           NULLIF("stat"."n_live_tup", 0) AS "estimated_row_count"
    FROM "pg_catalog"."pg_class" AS "c"
    INNER JOIN "pg_catalog"."pg_namespace" AS "n" ON "c"."relnamespace" = "n"."oid"
    LEFT JOIN "pg_catalog"."pg_description" AS "d"
        ON ("c"."oid" = "d"."objoid") AND ("d"."objsubid" = 0)
        AND ("d"."classoid" = 'pg_class'::regclass)
    LEFT JOIN "pg_stat_user_tables" AS "stat"
        ON ("n"."nspname" = "stat"."schemaname") AND ("c"."relname" = "stat"."relname")
    WHERE ("c"."relnamespace" = "n"."oid")
      AND ("n"."nspname" !~ '^pg_')
      AND ("n"."nspname" <> 'information_schema')
      AND c.relkind in ('r', 'p', 'v', 'f', 'm')
      AND ("n"."nspname" IN ('public'))
    ORDER BY "type" ASC, "schema" ASC, "name" ASC
    """
    catalog = _seven_table_demo_catalog()
    result = _translate(sql, catalog)
    assert isinstance(result, PgCatalogResult)
    rows = result.batch.rows
    assert len(rows) == 7  # seven demo models
    assert {r["name"] for r in rows} == {
        "customers", "orders", "products", "stores", "supplies", "items", "tweets",
    }
    # n_live_tup is NULL → estimated_row_count NULL.
    assert all(r["estimated_row_count"] is None for r in rows)
    # Description present (per model.description = "jaffle <name>").
    assert all(r["description"].startswith("jaffle") for r in rows)
    # Aliased columns preserved exactly.
    assert [c.name for c in result.batch.columns] == [
        "schema", "name", "type", "description", "estimated_row_count",
    ]


def test_corpus_11_pg_enum_zero_rows() -> None:
    sql = ("SELECT nspname, typname FROM pg_type t "
           "JOIN pg_namespace n ON n.oid = t.typnamespace "
           "WHERE t.oid IN (SELECT DISTINCT enumtypid FROM pg_enum e)")
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, PgCatalogResult)
    assert result.batch.rows == []
    # Columns still surface so Metabase's RowDescription has the right shape.
    assert [c.name for c in result.batch.columns] == ["nspname", "typname"]


def test_corpus_12_describe_fields_columns_join() -> None:
    sql = """
    SELECT "c"."column_name" AS "name",
           "c"."udt_name" AS "database-type",
           "c"."ordinal_position" - 1 AS "database-position",
           "c"."table_schema" AS "table-schema",
           "c"."table_name" AS "table-name",
           "pk"."column_name" IS NOT NULL AS "pk?",
           COL_DESCRIPTION(
               CAST(CAST(FORMAT('%I.%I',
                   CAST("c"."table_schema" AS TEXT),
                   CAST("c"."table_name" AS TEXT)) AS REGCLASS) AS OID),
               "c"."ordinal_position"
           ) AS "field-comment",
           "is_nullable" = 'YES' AS "database-is-nullable"
    FROM "information_schema"."columns" AS "c"
    LEFT JOIN (
        SELECT "tc"."table_schema", "tc"."table_name", "kc"."column_name"
        FROM "information_schema"."table_constraints" AS "tc"
        INNER JOIN "information_schema"."key_column_usage" AS "kc"
            ON ("tc"."constraint_name" = "kc"."constraint_name")
            AND ("tc"."table_schema" = "kc"."table_schema")
            AND ("tc"."table_name" = "kc"."table_name")
        WHERE "tc"."constraint_type" = 'PRIMARY KEY'
    ) AS "pk"
        ON ("c"."table_schema" = "pk"."table_schema")
        AND ("c"."table_name" = "pk"."table_name")
        AND ("c"."column_name" = "pk"."column_name")
    WHERE c.table_schema !~ '^information_schema|catalog_history|pg_'
      AND ("c"."table_schema" IN ('public'))
    """
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, PgCatalogResult)
    rows = result.batch.rows
    # Columns for both demo tables surface.
    table_cols = {(r["table-name"], r["name"]) for r in rows}
    assert ("orders", "total") in table_cols
    assert ("customers", "email") in table_cols
    # COL_DESCRIPTION returns the column description for `total`.
    total_row = next(r for r in rows if r["table-name"] == "orders" and r["name"] == "total")
    assert total_row["field-comment"] == "Order total"
    # Aliases preserved.
    expected_aliases = ["name", "database-type", "database-position", "table-schema",
                        "table-name", "pk?", "field-comment", "database-is-nullable"]
    assert [c.name for c in result.batch.columns] == expected_aliases


def test_corpus_13_pgjdbc_get_columns_smoke() -> None:
    # We don't pin every projection — just confirm it executes and surfaces
    # at least one column row for each demo table; column descriptions flow
    # through pg_description (LEFT JOIN on objoid/objsubid).
    sql = """
    SELECT * FROM (
        SELECT current_database() AS current_database,
               n.nspname, c.relname, a.attname, a.atttypid,
               a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
               a.atttypmod, a.attlen, t.typtypmod,
               row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
               nullif(a.attidentity, '') as attidentity,
               nullif(a.attgenerated, '') as attgenerated,
               pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
               dsc.description, t.typbasetype, t.typtype
        FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
        JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
        JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
        LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
        LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
        LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
        LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
        WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped
          AND n.nspname LIKE 'public' AND c.relname LIKE 'orders'
    ) c
    WHERE true
    ORDER BY nspname, c.relname, attnum
    """
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, PgCatalogResult)
    rows = result.batch.rows
    assert len(rows) > 0
    # adsrc is the pg_get_expr stub → NULL.
    assert all(r["adsrc"] is None for r in rows)
    # current_database rewritten to datasource.
    assert all(r["current_database"] == "jaffle" for r in rows)
    # description from pg_description flowing through LEFT JOIN.
    descs = {r["description"] for r in rows if r["attname"] == "total"}
    assert "Order total" in descs


def test_corpus_14_pgjdbc_get_primary_keys_zero_rows() -> None:
    """#14 uses _pg_expandarray + empty pg_index → 0 rows by design (Q4-a)."""
    sql = """
    SELECT result.TABLE_CAT AS "TABLE_CAT",
           result.TABLE_SCHEM AS "TABLE_SCHEM",
           result.TABLE_NAME AS "TABLE_NAME",
           result.COLUMN_NAME AS "COLUMN_NAME",
           result.KEY_SEQ AS "KEY_SEQ",
           result.PK_NAME AS "PK_NAME"
    FROM (
        SELECT current_database() AS TABLE_CAT,
               n.nspname AS TABLE_SCHEM,
               ct.relname AS TABLE_NAME,
               a.attname AS COLUMN_NAME,
               (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
               ci.relname AS PK_NAME,
               information_schema._pg_expandarray(i.indkey) AS KEYS,
               a.attnum AS A_ATTNUM,
               i.indnkeyatts as KEY_COUNT
        FROM pg_catalog.pg_class ct
        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
        JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
        WHERE true AND n.nspname = 'public' AND ct.relname = 'orders' AND i.indisprimary
    ) result
    WHERE result.A_ATTNUM = (result.KEYS).x AND result.KEY_SEQ <= KEY_COUNT
    ORDER BY result.table_name, result.pk_name, result.key_seq
    """
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, PgCatalogResult)
    assert result.batch.rows == []
    # Projection column names with case-sensitive aliases preserved.
    assert [c.name for c in result.batch.columns] == [
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME",
    ]


def test_corpus_15_fk_introspection_zero_rows() -> None:
    sql = """
    SELECT "fk_ns"."nspname" AS "fk-table-schema",
           "fk_table"."relname" AS "fk-table-name",
           "fk_column"."attname" AS "fk-column-name",
           "pk_ns"."nspname" AS "pk-table-schema",
           "pk_table"."relname" AS "pk-table-name",
           "pk_column"."attname" AS "pk-column-name"
    FROM "pg_constraint" AS "c"
    INNER JOIN "pg_class" AS "fk_table" ON "c"."conrelid" = "fk_table"."oid"
    INNER JOIN "pg_namespace" AS "fk_ns" ON "c"."connamespace" = "fk_ns"."oid"
    INNER JOIN "pg_attribute" AS "fk_column" ON "c"."conrelid" = "fk_column"."attrelid"
    INNER JOIN "pg_class" AS "pk_table" ON "c"."confrelid" = "pk_table"."oid"
    INNER JOIN "pg_namespace" AS "pk_ns" ON "pk_table"."relnamespace" = "pk_ns"."oid"
    INNER JOIN "pg_attribute" AS "pk_column" ON "c"."confrelid" = "pk_column"."attrelid"
    WHERE fk_ns.nspname !~ '^information_schema|catalog_history|pg_'
      AND ("c"."contype" = 'f')
      AND ("fk_column"."attnum" = ANY(c.conkey))
      AND ("pk_column"."attnum" = ANY(c.confkey))
      AND ("fk_ns"."nspname" IN ('public'))
    ORDER BY "fk-table-schema" ASC, "fk-table-name" ASC
    """
    result = _translate(sql, _demo_catalog())
    assert isinstance(result, PgCatalogResult)
    assert result.batch.rows == []  # pg_constraint empty
    assert [c.name for c in result.batch.columns] == [
        "fk-table-schema", "fk-table-name", "fk-column-name",
        "pk-table-schema", "pk-table-name", "pk-column-name",
    ]


def test_corpus_18_field_type_lookup_substitutes_slayer_oid() -> None:
    """Corpus #18 has a hardcoded Postgres OID `16384` — that's the OID
    Metabase observed back from #13 against the REAL Postgres.  Inside SLayer
    the equivalent OID is the deterministic ``stable_oid('jaffle', 'orders')``
    value, which is what Metabase would have read from SLayer's pg_class in
    a live #13 run.  So the test substitutes SLayer's OID BEFORE sending the
    query — exactly what Metabase does end-to-end.  No in-SLayer rewriting
    of the captured literal 16384 happens; the captured literal is a property
    of the original Postgres, not of SLayer's catalog."""
    catalog = _demo_catalog()
    from slayer.facade.catalog_sql import build_catalog_relations
    relations = {r.name: r for r in build_catalog_relations(catalog)}
    orders_oid = next(r["oid"] for r in relations["pg_class"].rows if r["relname"] == "orders")

    sql = f"""
    SELECT c.oid, a.attnum, a.attname, c.relname, n.nspname,
           a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS notnull_flag,
           a.attidentity != '' OR pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%' AS is_seq
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
    JOIN pg_catalog.pg_attribute a ON (c.oid = a.attrelid)
    JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
    LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum)
    JOIN (SELECT {orders_oid} AS oid, 2 AS attnum
          UNION ALL SELECT {orders_oid}, 3
          UNION ALL SELECT {orders_oid}, 4) vals
        ON (c.oid = vals.oid AND a.attnum = vals.attnum)
    WHERE c.oid in ({orders_oid})
    """
    result = _translate(sql, catalog)
    assert isinstance(result, PgCatalogResult)
    assert len(result.batch.rows) == 3
    assert all(r["relname"] == "orders" for r in result.batch.rows)
