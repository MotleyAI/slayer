"""Integration tests for the Postgres facade (DEV-1486).

Boots a real ``slayer pg-serve``-equivalent asyncio server (in a background
thread with its own event loop) backed by the bundled Jaffle Shop demo, and
drives it with the pure-Python ``asyncpg`` client — exercising startup, auth,
the extended/binary protocol, transactions, and concurrency end-to-end.
"""

from __future__ import annotations

import asyncio
from typing import Iterator, Tuple

import pytest

from tests.integration._pg_serve_helpers import DEMO_DATASOURCE, start_pg_demo_server

pytestmark = pytest.mark.integration

asyncpg = pytest.importorskip("asyncpg")


@pytest.fixture(scope="module")
def pg_demo_server() -> Iterator[Tuple[str, int]]:
    loop, thread, host, port = start_pg_demo_server(token=None)
    try:
        yield host, port
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


@pytest.fixture(scope="module")
def pg_demo_server_with_token() -> Iterator[Tuple[str, int, str]]:
    token = "s3cret"
    loop, thread, host, port = start_pg_demo_server(token=token)
    try:
        yield host, port, token
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


async def _connect(host: str, port: int, *, database: str = DEMO_DATASOURCE, password: str = "x"):  # NOSONAR(S2068) — test credential
    return await asyncpg.connect(
        host=host, port=port, user="tester", password=password, database=database,
        timeout=10,
    )


# --- connect / identity ------------------------------------------------------


async def test_connect_and_current_database(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        assert await conn.fetchval("SELECT current_database()") == DEMO_DATASOURCE
    finally:
        await conn.close()


async def test_unknown_database_rejected(pg_demo_server) -> None:
    host, port = pg_demo_server
    with pytest.raises(asyncpg.InvalidCatalogNameError):
        await _connect(host, port, database="nope")


async def test_select_one(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


async def test_version_string(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        version = await conn.fetchval("SELECT version()")
        assert version.startswith("PostgreSQL 14.0 (SLayer Postgres facade")
    finally:
        await conn.close()


# --- introspection -----------------------------------------------------------


async def test_information_schema_metrics(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            "SELECT * FROM INFORMATION_SCHEMA.METRICS WHERE table_name = 'orders'"
        )
        # DEV-1558: WHERE is now honored server-side; result is filtered.
        assert rows  # at least one row
        assert all(r["table_name"] == "orders" for r in rows)
        assert any(r["metric_name"] == "row_count" for r in rows)
    finally:
        await conn.close()


async def test_pg_namespace(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_namespace")
        assert {r["nspname"] for r in rows} == {"public", "pg_catalog"}
    finally:
        await conn.close()


async def test_pg_class_orders_present(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch("SELECT * FROM pg_catalog.pg_class WHERE relname = 'orders'")
        # DEV-1558: WHERE is now honored; only `orders` comes back.
        assert {r["relname"] for r in rows} == {"orders"}
        assert rows[0]["relkind"] == "r"
    finally:
        await conn.close()


async def test_pg_attribute_has_orders_columns(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        oid = await conn.fetchval(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'orders'"
        )
        # DEV-1558: WHERE is now honored server-side. Inline the OID rather
        # than binding $1: the pg facade types unannounced $N parameters as
        # TEXT (per asyncpg's wire expectation), and asyncpg refuses to
        # encode an int through a TEXT parameter.
        rows = await conn.fetch(
            f"SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = {oid}"
        )
        assert len(rows) > 0
        assert all(r["attrelid"] == oid for r in rows)
    finally:
        await conn.close()


# --- DEV-1558 Metabase v0.62 schema-sync queries (live asyncpg) -------------


async def test_metabase_get_tables_4way_join(pg_demo_server) -> None:
    """Corpus #9 — Metabase's describe-tables join across pg_class +
    pg_namespace + pg_description + pg_stat_user_tables. All demo tables
    surface with descriptions; estimated_row_count is NULL."""
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            '''
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
            INNER JOIN "pg_catalog"."pg_namespace" AS "n"
                ON "c"."relnamespace" = "n"."oid"
            LEFT JOIN "pg_catalog"."pg_description" AS "d"
                ON ("c"."oid" = "d"."objoid") AND ("d"."objsubid" = 0)
                AND ("d"."classoid" = 'pg_class'::regclass)
            LEFT JOIN "pg_stat_user_tables" AS "stat"
                ON ("n"."nspname" = "stat"."schemaname")
                AND ("c"."relname" = "stat"."relname")
            WHERE ("n"."nspname" !~ '^pg_')
              AND ("n"."nspname" <> 'information_schema')
              AND c.relkind in ('r', 'p', 'v', 'f', 'm')
              AND ("n"."nspname" IN ('public'))
            ORDER BY "type" ASC, "schema" ASC, "name" ASC
            '''
        )
        assert len(rows) >= 1
        assert all(r["estimated_row_count"] is None for r in rows)
        assert all(r["schema"] == "public" for r in rows)
        # Columns came back with the aliased names exactly.
        assert set(rows[0].keys()) == {
            "schema", "name", "type", "description", "estimated_row_count",
        }
    finally:
        await conn.close()


async def test_metabase_describe_fields_columns_join(pg_demo_server) -> None:
    """Corpus #12 — describe-fields via information_schema.columns +
    table_constraints + key_column_usage with the COL_DESCRIPTION
    REGCLASS double-cast."""
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            '''
            SELECT "c"."column_name" AS "name",
                   "c"."udt_name" AS "database-type",
                   "c"."table_schema" AS "table-schema",
                   "c"."table_name" AS "table-name",
                   COL_DESCRIPTION(
                       CAST(CAST(FORMAT('%I.%I',
                           CAST("c"."table_schema" AS TEXT),
                           CAST("c"."table_name" AS TEXT)) AS REGCLASS) AS OID),
                       "c"."ordinal_position"
                   ) AS "field-comment"
            FROM "information_schema"."columns" AS "c"
            WHERE c.table_schema !~ '^information_schema|catalog_history|pg_'
              AND ("c"."table_schema" IN ('public'))
            '''
        )
        assert rows
        # Every row's table-schema is 'public'.
        assert all(r["table-schema"] == "public" for r in rows)
    finally:
        await conn.close()


async def test_metabase_table_privileges_cte(pg_demo_server) -> None:
    """Corpus #8 — the table_privileges CTE. Exercises CTE recognition
    in is_catalog_only + the privilege stub macros."""
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            '''
            WITH table_privileges AS (
                SELECT
                    NULL as role,
                    t.schemaname as schema,
                    t.objectname as table,
                    pg_catalog.has_any_column_privilege(current_user,
                        t.schemaname || '.' || t.objectname, 'select') as select_priv,
                    pg_catalog.has_table_privilege(current_user,
                        t.schemaname || '.' || t.objectname, 'delete') as delete_priv
                FROM (
                    SELECT schemaname, tablename AS objectname FROM pg_catalog.pg_tables
                    UNION
                    SELECT schemaname, viewname AS objectname FROM pg_catalog.pg_views
                    UNION
                    SELECT schemaname, matviewname AS objectname FROM pg_catalog.pg_matviews
                ) t
                WHERE t.schemaname !~ '^pg_'
                  AND t.schemaname <> 'information_schema'
            )
            SELECT t.* FROM table_privileges t
            '''
        )
        assert rows
        assert all(r["select_priv"] is True for r in rows)
        assert all(r["delete_priv"] is True for r in rows)
    finally:
        await conn.close()


async def test_metabase_fingerprint_three_part_qualified_column(pg_demo_server) -> None:
    """Corpus #17 shape — Metabase fingerprint queries against the model
    path use ``"public"."orders"."customer_id"`` three-part qualified column
    refs. The translator must strip the schema/table prefix."""
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            '''
            SELECT "public"."orders"."id" AS "id"
            FROM "public"."orders" LIMIT 5
            '''
        )
        assert rows
        assert all("id" in r.keys() for r in rows)
    finally:
        await conn.close()


async def test_metabase_fingerprint_substring_wrapper(pg_demo_server) -> None:
    """Corpus #16 shape — Metabase fingerprint queries wrap text columns in
    SUBSTRING(col, 1, 1234). The translator silently drops the wrapper and
    projects the bare column under the user's alias."""
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            '''
            SELECT SUBSTRING("public"."customers"."name", 1, 1234) AS "name_sub"
            FROM "public"."customers" LIMIT 5
            '''
        )
        assert rows
        assert all("name_sub" in r.keys() for r in rows)
    finally:
        await conn.close()


# --- semantic-model queries --------------------------------------------------


async def test_row_count_metric(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        count = await conn.fetchval("SELECT row_count FROM orders")
        assert isinstance(count, int)
        assert count > 0
    finally:
        await conn.close()


async def test_count_star_aggregate_sql_mapping(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        # COUNT(*) maps to the *:count measure (aggregate-SQL mapping).
        count = await conn.fetchval("SELECT COUNT(*) FROM orders")
        assert isinstance(count, int)
        assert count > 0
    finally:
        await conn.close()


async def test_time_grain_group_by(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch(
            "SELECT month(ordered_at) AS m, row_count FROM orders "
            "GROUP BY m ORDER BY m"
        )
        assert len(rows) > 0
    finally:
        await conn.close()


async def test_cross_model_dimension(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        rows = await conn.fetch("SELECT customers.name, row_count FROM orders")
        assert len(rows) > 0
        # The projected column keeps its dotted BI-flat name.
        assert "customers.name" in rows[0].keys()
    finally:
        await conn.close()


async def test_select_star_rejected(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        with pytest.raises(asyncpg.PostgresError) as exc_info:
            await conn.fetch("SELECT * FROM orders")
        assert "SELECT *" in str(exc_info.value)
    finally:
        await conn.close()


async def test_dml_rejected(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        with pytest.raises(asyncpg.PostgresError):
            await conn.execute("INSERT INTO orders VALUES (1)")
    finally:
        await conn.close()


async def test_cast_unsupported_coercion_returns_postgres_error(pg_demo_server) -> None:
    """DEV-1566: CAST(<TEXT col> AS INT) is outside the admitted-coercion
    allowlist (truncation/parse semantics differ from Postgres). The
    translator surfaces a clean PostgresError with the strict-allowlist
    message — not an internal connection crash at wire-encode time."""
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        with pytest.raises(asyncpg.PostgresError) as exc_info:
            await conn.fetch("SELECT CAST(name AS INT) FROM customers LIMIT 1")
        assert "Unsupported CAST" in str(exc_info.value)
    finally:
        await conn.close()


# --- parameterised query (literal substitution) ------------------------------


async def test_parameterised_query_substitutes(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        # asyncpg sends $1 via the extended protocol; the facade substitutes a
        # literal before translating. WHERE is ignored for INFORMATION_SCHEMA,
        # so this just proves the bind/substitute path runs without error.
        rows = await conn.fetch(
            "SELECT * FROM INFORMATION_SCHEMA.METRICS WHERE table_name = $1", "orders",
        )
        assert any(r["table_name"] == "orders" for r in rows)
    finally:
        await conn.close()


# --- transactions ------------------------------------------------------------


async def test_transaction_block(pg_demo_server) -> None:
    host, port = pg_demo_server
    conn = await _connect(host, port)
    try:
        async with conn.transaction():
            assert await conn.fetchval("SELECT 1") == 1
        # After the block the connection is reusable (tx returned to idle).
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


# --- concurrency -------------------------------------------------------------


async def test_concurrent_connections(pg_demo_server) -> None:
    host, port = pg_demo_server

    async def _one() -> int:
        conn = await _connect(host, port)
        try:
            return await conn.fetchval("SELECT 1")
        finally:
            await conn.close()

    results = await asyncio.gather(*[_one() for _ in range(10)])
    assert results == [1] * 10


# --- auth --------------------------------------------------------------------


async def test_auth_positive(pg_demo_server_with_token) -> None:
    host, port, token = pg_demo_server_with_token
    conn = await _connect(host, port, password=token)
    try:
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


async def test_auth_wrong_password(pg_demo_server_with_token) -> None:
    host, port, _token = pg_demo_server_with_token
    with pytest.raises(asyncpg.InvalidPasswordError):
        await _connect(host, port, password="wrong")  # NOSONAR(S2068) — test credential
