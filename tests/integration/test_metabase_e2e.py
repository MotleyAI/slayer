"""Live-Metabase end-to-end suite for the pg-facade (DEV-1562).

Drives a real Metabase v0.62.1.5 container (in Docker) against two real
``slayer pg-serve`` instances and asserts every regression vector from the
DEV-1558 3-hour live debugging session — plus the broader behaviour matrix
described in the spec.

All tests are marked ``metabase_e2e`` + ``integration``. The suite skips
cleanly when Docker is unavailable or the container fails to come up.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import datetime as dt
import logging
import time
from typing import Any, Dict, List

import pytest

asyncpg = pytest.importorskip("asyncpg")
psycopg2 = pytest.importorskip("psycopg2")
requests = pytest.importorskip("requests")

from tests.integration.conftest_metabase import (  # noqa: E402
    MetabaseE2EEnv,
    encode_mbql_query,
    encode_native_query,
)

pytestmark = [pytest.mark.metabase_e2e, pytest.mark.integration]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _asyncpg_connect(host: str, port: int, *, password: str = "x", database: str = "jaffle_shop"):
    return await asyncpg.connect(
        host=host, port=port, user="tester", password=password, database=database, timeout=10,
    )


async def _scalar(host: str, port: int, sql: str) -> Any:
    conn = await _asyncpg_connect(host, port)
    try:
        return await conn.fetchval(sql)
    finally:
        await conn.close()


def _dataset_rows(payload: Dict[str, Any]) -> List[List[Any]]:
    return payload.get("data", {}).get("rows", []) or []


def _dataset_cols(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return payload.get("data", {}).get("cols", []) or []


# ---------------------------------------------------------------------------
# A. Bootstrap (6 tests — base 4 + A.5 token-auth + A.6 log volume)
# ---------------------------------------------------------------------------


def test_setup_token_captured(metabase_e2e_env: MetabaseE2EEnv) -> None:
    assert metabase_e2e_env.session_token
    assert len(metabase_e2e_env.session_token) > 16


def test_datasource_registration_returns_id(metabase_e2e_env: MetabaseE2EEnv) -> None:
    assert isinstance(metabase_e2e_env.client.db_id, int)
    assert metabase_e2e_env.client.db_id > 0
    assert metabase_e2e_env.token_db_id > 0
    assert metabase_e2e_env.token_db_id != metabase_e2e_env.client.db_id


def test_sync_settles_returns_at_least_seven_tables(metabase_e2e_env: MetabaseE2EEnv) -> None:
    md = metabase_e2e_env.client.database_metadata()
    table_names = {t["name"] for t in md.get("tables", [])}
    expected = {"customers", "items", "orders", "products", "stores", "supplies", "tweets"}
    assert expected.issubset(table_names), f"missing: {expected - table_names}"


def test_sync_schema_idempotent(metabase_e2e_env: MetabaseE2EEnv) -> None:
    metabase_e2e_env.client.sync_schema()
    md_first = metabase_e2e_env.client.database_metadata()
    metabase_e2e_env.client.sync_schema()
    # Give Metabase a moment to settle the second sync.
    time.sleep(2)
    md_second = metabase_e2e_env.client.database_metadata()
    assert len(md_first["tables"]) == len(md_second["tables"])


def test_metabase_authenticates_with_token(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """A.5 — Metabase round-trips a query through the token-protected pg-serve."""
    token_client = metabase_e2e_env.make_client(metabase_e2e_env.token_db_id)
    md = token_client.database_metadata()
    assert len(md.get("tables", [])) >= 7
    orders_id = token_client.table_id_by_name("orders")
    payload = token_client.dataset(encode_mbql_query(
        source_table=orders_id, aggregation=[["count"]],
    ))
    rows = _dataset_rows(payload)
    assert rows and rows[0][0] > 0


def test_sync_log_volume_within_budget(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """A.6 — pg-serve sync should not spam WARN-level hygiene messages.

    DEV-1558 bug 4: 170+ WARN lines per sync. Budget here is generous (20)
    so incidental warnings don't trip the test; a true regression fires.

    Self-validating: also asserts the log-capture buffer is non-empty after
    the sync, so the test can't false-pass when the handler isn't wired up.
    """
    total_before = len(metabase_e2e_env.log_records)
    warn_before = sum(1 for r in metabase_e2e_env.log_records if r.levelno >= logging.WARNING)
    metabase_e2e_env.client.sync_schema()
    time.sleep(3)
    total_after = len(metabase_e2e_env.log_records)
    warn_after = sum(1 for r in metabase_e2e_env.log_records if r.levelno >= logging.WARNING)
    assert total_after > total_before, (
        "log capture appears inactive: sync_schema emitted no records — "
        "the WARN budget assertion below would false-pass without this guard"
    )
    delta = warn_after - warn_before
    assert delta < 20, f"sync_schema produced {delta} WARN+ records (budget 20)"


# ---------------------------------------------------------------------------
# B. Catalog introspection (8 tests — base 5 + B.6 TABLE_SCHEM + B.7 first/last absent + B.8 objsubid)
# ---------------------------------------------------------------------------


def test_orders_field_list_complete(metabase_e2e_env: MetabaseE2EEnv) -> None:
    orders_id = metabase_e2e_env.client.table_id_by_name("orders")
    md = metabase_e2e_env.client.table_metadata(orders_id)
    field_names = {f["name"] for f in md.get("fields", [])}
    expected = {"id", "customer_id", "ordered_at", "store_id", "subtotal", "tax_paid", "order_total"}
    assert expected.issubset(field_names), f"missing: {expected - field_names}"


def test_field_oid_to_metabase_type_mapping(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """OID → Metabase base_type mapping. DATE→type/Date, INT→type/BigInteger, etc."""
    orders_id = metabase_e2e_env.client.table_id_by_name("orders")
    fields = {f["name"]: f for f in metabase_e2e_env.client.table_metadata(orders_id)["fields"]}
    assert fields["ordered_at"]["base_type"] == "type/Date"
    # order_total / subtotal / tax_paid are DOUBLE → type/Float (Metabase's float bucket)
    assert fields["order_total"]["base_type"] in {"type/Float", "type/Decimal"}
    # id / customer_id / store_id are VARCHAR → type/Text
    assert fields["id"]["base_type"] == "type/Text"

    # stores table covers DATE more strongly, and (depending on schema) BOOLEAN/DOUBLE
    stores_id = metabase_e2e_env.client.table_id_by_name("stores")
    stores_fields = {f["name"]: f for f in metabase_e2e_env.client.table_metadata(stores_id)["fields"]}
    assert stores_fields["opened_at"]["base_type"] == "type/Date"
    assert stores_fields["tax_rate"]["base_type"] in {"type/Float", "type/Decimal"}


def _wait_until(predicate, *, timeout_s: float = 20, interval_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


def test_hidden_columns_not_surfaced(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.3 — flip ``Column.hidden=True`` on the SLayer model, re-sync,
    assert the column disappears from Metabase's view of the table. Restore
    in ``finally`` so the rest of the suite is unaffected.
    """
    from slayer.async_utils import run_sync

    storage = metabase_e2e_env.pg_no_token_storage
    assert storage is not None, "pg_no_token storage handle not wired through fixture"
    client = metabase_e2e_env.client

    model = run_sync(storage.get_model(name="products", data_source="jaffle_shop"))
    target = next(c for c in model.columns if c.name == "description")
    original = target.hidden
    target.hidden = True
    run_sync(storage.save_model(model))
    try:
        client.sync_schema()
        products_id = client.table_id_by_name("products")

        def _hidden_gone() -> bool:
            fields = client.table_metadata(products_id).get("fields", [])
            return all(f["name"] != "description" for f in fields)

        assert _wait_until(_hidden_gone, timeout_s=30), \
            "hidden column 'description' still surfaced by Metabase after sync"
    finally:
        target.hidden = original
        run_sync(storage.save_model(model))
        try:
            client.sync_schema()
        except Exception:
            pass


def test_descriptions_surface_in_metadata(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.4 — set ``Column.description`` on the SLayer model, re-sync, assert
    it flows through ``pg_description`` into Metabase's field metadata.
    """
    from slayer.async_utils import run_sync

    storage = metabase_e2e_env.pg_no_token_storage
    assert storage is not None
    client = metabase_e2e_env.client

    model = run_sync(storage.get_model(name="orders", data_source="jaffle_shop"))
    target = next(c for c in model.columns if c.name == "order_total")
    original_desc = target.description
    marker = "DEV-1562 e2e description marker"
    target.description = marker
    run_sync(storage.save_model(model))
    try:
        client.sync_schema()
        orders_id = client.table_id_by_name("orders")

        def _desc_visible() -> bool:
            md = client.table_metadata(orders_id)
            fld = next(
                (f for f in md.get("fields", []) if f["name"] == "order_total"),
                None,
            )
            return bool(fld and fld.get("description") == marker)

        assert _wait_until(_desc_visible, timeout_s=30), \
            f"description {marker!r} did not surface in Metabase metadata"
    finally:
        target.description = original_desc
        run_sync(storage.save_model(model))
        try:
            client.sync_schema()
        except Exception:
            pass


def test_four_part_qualified_refs_handled(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.5 — sync settles without error (round 20d regression vector).

    Metabase's catalog probes during sync include forms like
    `slayer.public.orders.<col>`. A regression here would surface as sync
    failure; a fresh sync_schema that returns 200 confirms the path holds.
    """
    r = requests.post(
        f"{metabase_e2e_env.base_url}/api/database/{metabase_e2e_env.client.db_id}/sync_schema",
        headers={"X-Metabase-Session": metabase_e2e_env.session_token},
        timeout=60,
    )
    assert r.status_code == 200


async def test_pg_namespace_table_schem_column_name(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.6 — pgjdbc expects ``TABLE_SCHEM`` as the schema column name.

    The original DEV-1558 bug 1 was a lookup failure on this exact alias.
    Drive the canonical query directly via asyncpg.
    """
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        rows = await conn.fetch(
            'SELECT nspname AS "TABLE_SCHEM" FROM pg_namespace ORDER BY nspname'
        )
        assert rows, "pg_namespace returned no rows"
        # asyncpg lower-cases unquoted column names; the quoted alias must survive.
        keys = list(rows[0].keys())
        assert "TABLE_SCHEM" in keys, f"expected TABLE_SCHEM alias preserved; got {keys}"
    finally:
        await conn.close()


def test_first_last_not_exposed_on_timeless_models(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.7 — DEV-1558 bug 5. ``first`` / ``last`` aggregations must not be
    callable on models without a time dimension.

    Scope note: Metabase derives per-field aggregation availability from
    ``base_type``, not from per-field catalog metadata, so the canonical
    "exposed in the picker" assertion isn't available end-to-end. The
    contract that matters from a regression standpoint is the call itself:
    if a regression re-enables ``first()`` on a timeless model, the dataset
    POST will COMPLETE; this test asserts it does NOT.
    """
    client = metabase_e2e_env.client
    # ``products`` has no time dimension in the demo schema.
    products_id = client.table_id_by_name("products")
    md = client.table_metadata(products_id)
    for f in md["fields"]:
        assert f.get("semantic_type") not in {"type/SerializedJSON"}  # sanity guard
    payload = client.post_raw(
        "/api/dataset",
        {
            "database": client.db_id,
            **encode_mbql_query(
                source_table=products_id,
                aggregation=[["first", ["field", client.field_id_by_name("products", "price"), None]]],
            ),
        },
    )
    body = payload.json()
    # Either Metabase rejects the MBQL (4xx) or pg-serve returns an error
    # envelope; in both cases ``status`` is not "completed".
    assert payload.status_code >= 400 or body.get("status") != "completed"


async def test_pg_description_objsubid_empty_string_predicate(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.8 — DEV-1558 bug 2: literal ``''`` against the INT ``objsubid`` column.

    Metabase's pgjdbc-driven catalog probes emit ``WHERE objsubid = ''``
    against the INT column; a regression returns a typed-sentinel
    conversion error. The contract is the query parses + executes cleanly.
    """
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        rows = await conn.fetch("SELECT * FROM pg_description WHERE objsubid = ''")
        assert isinstance(rows, list)
    finally:
        await conn.close()


async def test_union_all_catalog_query_routed(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """B.9 — DEV-1558 bug 3. Corpus #12 catalog query parses as
    ``exp.Union``, not ``exp.Select``; the router missed it before round 19.
    Drive a UNION-ALL probe to pin the routing path.
    """
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        rows = await conn.fetch(
            "SELECT relname AS name, 'r' AS kind FROM pg_class WHERE relkind = 'r' "
            "UNION ALL "
            "SELECT relname AS name, 'v' AS kind FROM pg_class WHERE relkind = 'v'"
        )
        # Routing was the issue, not result accuracy — assert the query
        # actually returned a value (list, possibly empty) without raising.
        assert isinstance(rows, list)
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# C. Raw preview / SELECT * (4 tests — base 3 + C.4 DATE round-trip via Metabase)
# ---------------------------------------------------------------------------


def test_dataset_source_table_returns_rows(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    payload = client.dataset(encode_mbql_query(source_table=orders_id, limit=10))
    rows = _dataset_rows(payload)
    cols = _dataset_cols(payload)
    assert 1 <= len(rows) <= 10
    assert len(cols) == 7  # orders has 7 columns


def test_empty_result_filter_returns_cleanly(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    fid = client.field_id_by_name("orders", "id")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        filter=["=", ["field", fid, None], "definitely-not-an-id"],
    ))
    rows = _dataset_rows(payload)
    assert rows == []


def test_wide_row_serialises(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """Every column on ``items`` (a join-table) and ``orders`` must serialise."""
    client = metabase_e2e_env.client
    for table in ("orders", "items", "stores"):
        tid = client.table_id_by_name(table)
        payload = client.dataset(encode_mbql_query(source_table=tid, limit=1))
        rows = _dataset_rows(payload)
        cols = _dataset_cols(payload)
        assert rows, f"no rows for {table}"
        assert len(rows[0]) == len(cols), f"{table}: row width {len(rows[0])} != cols {len(cols)}"


def test_date_column_clean_round_trip_via_metabase(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """C.4 — DEV-1558 bug 8: DATE encoder serialising datetime as
    ``"2024-06-01 00:00:00"`` broke pgjdbc's ``TimestampUtils.toLocalDate``.
    Metabase IS a pgjdbc client; assert response DATE values parse cleanly.
    """
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    payload = client.dataset(encode_mbql_query(source_table=orders_id, limit=5))
    cols = _dataset_cols(payload)
    rows = _dataset_rows(payload)
    date_col_idx = next(i for i, c in enumerate(cols) if c["name"] == "ordered_at")
    for row in rows:
        v = row[date_col_idx]
        assert isinstance(v, str), f"DATE value should be ISO string, got {type(v).__name__}: {v!r}"
        # Pgjdbc-friendly form: no time component.
        assert "T" not in v and " " not in v, f"DATE value carries time suffix: {v!r}"
        # Parse cleanly as YYYY-MM-DD.
        dt.date.fromisoformat(v)


# ---------------------------------------------------------------------------
# D. Aggregations (7)
# ---------------------------------------------------------------------------


AGG_CASES = [
    ("count", [["count"]], "SELECT COUNT(*) FROM orders"),
    ("sum", [["sum", ["field", "order_total", None]]], "SELECT SUM(order_total) FROM orders"),
    ("avg", [["avg", ["field", "order_total", None]]], "SELECT AVG(order_total) FROM orders"),
    ("min", [["min", ["field", "order_total", None]]], "SELECT MIN(order_total) FROM orders"),
    ("max", [["max", ["field", "order_total", None]]], "SELECT MAX(order_total) FROM orders"),
    (
        "distinct",
        [["distinct", ["field", "customer_id", None]]],
        "SELECT COUNT(DISTINCT customer_id) FROM orders",
    ),
    ("count_star", [["count"]], "SELECT COUNT(*) FROM orders"),
]


@pytest.mark.parametrize("agg_name,mbql_agg,native_sql", AGG_CASES, ids=[c[0] for c in AGG_CASES])
async def test_aggregation_matches_direct_sql(
    metabase_e2e_env: MetabaseE2EEnv, agg_name: str, mbql_agg: list, native_sql: str,
) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    # Replace symbolic field names with real ids.
    resolved_agg = []
    for entry in mbql_agg:
        if len(entry) == 2 and isinstance(entry[1], list) and len(entry[1]) == 3 and entry[1][0] == "field":
            fid = client.field_id_by_name("orders", entry[1][1])
            resolved_agg.append([entry[0], ["field", fid, entry[1][2]]])
        else:
            resolved_agg.append(entry)
    payload = client.dataset(encode_mbql_query(source_table=orders_id, aggregation=resolved_agg))
    rows = _dataset_rows(payload)
    assert rows, f"empty result for {agg_name}"
    mb_value = rows[0][0]

    host, port = metabase_e2e_env.pg_no_token
    direct = await _scalar(host, port, native_sql)
    if isinstance(direct, (int,)) and isinstance(mb_value, (int,)):
        assert mb_value == direct, f"{agg_name}: metabase={mb_value} direct={direct}"
    else:
        assert abs(float(mb_value) - float(direct)) < 1e-6, (
            f"{agg_name}: metabase={mb_value} direct={direct}"
        )


# ---------------------------------------------------------------------------
# E. Time grains (7 tests — 6 grains + E.7 native CAST/DATE_TRUNC)
# ---------------------------------------------------------------------------


GRAINS = ["hour", "day", "week", "month", "quarter", "year"]


@pytest.mark.parametrize("grain", GRAINS)
def test_time_grain_breakout(metabase_e2e_env: MetabaseE2EEnv, grain: str) -> None:
    """E.1-E.6 — temporal-unit breakout on a DATE column. The ``month`` case
    specifically pins the round-20 CAST-unwrap fix (DEV-1558 bug 6).
    """
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    fid = client.field_id_by_name("orders", "ordered_at")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        breakout=[["field", fid, {"temporal-unit": grain}]],
        limit=200,
    ))
    rows = _dataset_rows(payload)
    assert rows, f"no rows for grain={grain}"
    # First bucket should parse as ISO date / datetime.
    first_bucket = rows[0][0]
    assert isinstance(first_bucket, str)
    # All buckets should be parseable as date or datetime.
    for row in rows[:5]:
        bucket = row[0]
        # Try date first then ISO datetime.
        try:
            dt.date.fromisoformat(bucket.split("T")[0])
        except ValueError:
            pytest.fail(f"bucket {bucket!r} not parseable for grain={grain}")


def test_native_sql_cast_date_trunc_as_date(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """E.7 — DEV-1558 bug 6. Metabase's DATE-column wrapping emits
    ``CAST(DATE_TRUNC(...) AS DATE)``; pin the exact SQL shape.

    Function-name note: the Linear ticket refers to this as
    ``CAST(TIMESTAMP_TRUNC(...) AS DATE)``. ``TIMESTAMP_TRUNC`` is a
    BigQuery-only spelling; Metabase against the Postgres connector emits
    ``DATE_TRUNC``. The regression vector is the CAST-around-time-grain
    wrapper, not the inner function name — both unwrap through the same
    validator path.
    """
    client = metabase_e2e_env.client
    sql = (
        "SELECT CAST(DATE_TRUNC('month', ordered_at) AS DATE) AS bucket, "
        "COUNT(*) AS n FROM orders GROUP BY 1 ORDER BY 1"
    )
    payload = client.dataset(encode_native_query(sql))
    rows = _dataset_rows(payload)
    assert rows
    assert len(rows[0]) == 2


# ---------------------------------------------------------------------------
# F. Filters (8)
# ---------------------------------------------------------------------------


def test_filter_categorical_equality(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    products_id = client.table_id_by_name("products")
    type_fid = client.field_id_by_name("products", "type")
    # Discover an actual type value first.
    discover = client.dataset(encode_mbql_query(source_table=products_id, limit=1))
    type_idx = next(i for i, c in enumerate(_dataset_cols(discover)) if c["name"] == "type")
    sample_value = _dataset_rows(discover)[0][type_idx]
    payload = client.dataset(encode_mbql_query(
        source_table=products_id,
        filter=["=", ["field", type_fid, None], sample_value],
    ))
    rows = _dataset_rows(payload)
    assert rows
    for row in rows:
        assert row[type_idx] == sample_value


def test_filter_in_list(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    products_id = client.table_id_by_name("products")
    type_fid = client.field_id_by_name("products", "type")
    # Pick two distinct type values.
    all_types = client.dataset(encode_mbql_query(
        source_table=products_id,
        breakout=[["field", type_fid, None]],
    ))
    type_vals = [row[0] for row in _dataset_rows(all_types)[:2]]
    assert len(type_vals) >= 2, "need at least 2 distinct product types"
    payload = client.dataset(encode_mbql_query(
        source_table=products_id,
        filter=["=", ["field", type_fid, None], *type_vals],
    ))
    rows = _dataset_rows(payload)
    assert rows
    cols = _dataset_cols(payload)
    type_idx = next(i for i, c in enumerate(cols) if c["name"] == "type")
    seen = {row[type_idx] for row in rows}
    assert seen.issubset(set(type_vals))


def test_filter_numeric_range(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    ot_fid = client.field_id_by_name("orders", "order_total")
    # > 0 filter
    payload_gt = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        filter=[">", ["field", ot_fid, None], 0],
    ))
    n_gt = _dataset_rows(payload_gt)[0][0]
    assert n_gt > 0
    # BETWEEN filter
    payload_between = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        filter=["between", ["field", ot_fid, None], 0, 1_000_000],
    ))
    n_between = _dataset_rows(payload_between)[0][0]
    assert n_between == n_gt


def test_filter_date_range(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    od_fid = client.field_id_by_name("orders", "ordered_at")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        filter=["between", ["field", od_fid, {"base-type": "type/Date"}], "2024-06-01", "2024-12-31"],
    ))
    rows = _dataset_rows(payload)
    # Either there are rows in that window or there aren't; the contract is
    # the query completed without error and returned an integer count.
    assert rows and isinstance(rows[0][0], int)


def test_filter_is_null_and_is_not_null(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    ot_fid = client.field_id_by_name("orders", "order_total")
    payload_isnull = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        filter=["is-null", ["field", ot_fid, None]],
    ))
    payload_notnull = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        filter=["not-null", ["field", ot_fid, None]],
    ))
    n_null = _dataset_rows(payload_isnull)[0][0]
    n_notnull = _dataset_rows(payload_notnull)[0][0]
    assert n_null == 0  # order_total is NOT NULL in jaffle schema
    assert n_notnull > 0


def test_filter_like_ilike_contains(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    products_id = client.table_id_by_name("products")
    name_fid = client.field_id_by_name("products", "name")
    discover = client.dataset(encode_mbql_query(source_table=products_id, limit=1))
    name_idx = next(i for i, c in enumerate(_dataset_cols(discover)) if c["name"] == "name")
    sample = _dataset_rows(discover)[0][name_idx]
    if not isinstance(sample, str) or len(sample) < 2:
        pytest.skip("no usable sample product name for LIKE test")
    fragment = sample[:2]
    for op_name in ("starts-with", "ends-with", "contains"):
        payload = client.dataset(encode_mbql_query(
            source_table=products_id,
            aggregation=[["count"]],
            filter=[op_name, ["field", name_fid, None], fragment],
        ))
        n = _dataset_rows(payload)[0][0]
        assert isinstance(n, int)
        if op_name == "starts-with":
            assert n >= 1


def test_filter_having_on_aggregate(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    cust_fid = client.field_id_by_name("orders", "customer_id")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        breakout=[["field", cust_fid, None]],
        filter=[">", ["aggregation", 0], 1],
    ))
    rows = _dataset_rows(payload)
    # If there are customers with multiple orders, we get rows; if not, []
    # but either way the query must complete.
    for row in rows:
        assert row[1] > 1


async def test_filter_categorical_comma_bearing_value(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """F.8 — filter literal containing a comma must not break SQL escaping."""
    client = metabase_e2e_env.client
    host, port = metabase_e2e_env.pg_no_token
    sample_value = await _scalar(
        host, port, "SELECT content FROM tweets WHERE content LIKE '%,%' LIMIT 1"
    )
    if sample_value is None:
        pytest.skip("no comma-bearing tweet content in this jafgen dataset")
    tweets_id = client.table_id_by_name("tweets")
    content_fid = client.field_id_by_name("tweets", "content")
    payload = client.dataset(encode_mbql_query(
        source_table=tweets_id,
        aggregation=[["count"]],
        filter=["=", ["field", content_fid, None], sample_value],
    ))
    n = _dataset_rows(payload)[0][0]
    assert n >= 1


# ---------------------------------------------------------------------------
# G. ORDER BY / LIMIT (4 tests — base 3 + G.4 native canonical-alias)
# ---------------------------------------------------------------------------


def test_order_by_dimension_asc_and_desc(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    products_id = client.table_id_by_name("products")
    price_fid = client.field_id_by_name("products", "price")
    asc = client.dataset(encode_mbql_query(
        source_table=products_id,
        **{"order-by": [["asc", ["field", price_fid, None]]]},
        limit=5,
    ))
    desc = client.dataset(encode_mbql_query(
        source_table=products_id,
        **{"order-by": [["desc", ["field", price_fid, None]]]},
        limit=5,
    ))
    price_idx_asc = next(i for i, c in enumerate(_dataset_cols(asc)) if c["name"] == "price")
    asc_prices = [row[price_idx_asc] for row in _dataset_rows(asc)]
    desc_prices = [row[price_idx_asc] for row in _dataset_rows(desc)]
    assert asc_prices == sorted(asc_prices)
    assert desc_prices == sorted(desc_prices, reverse=True)


def test_order_by_aggregate(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    cust_fid = client.field_id_by_name("orders", "customer_id")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        breakout=[["field", cust_fid, None]],
        **{"order-by": [["desc", ["aggregation", 0]]]},
        limit=5,
    ))
    rows = _dataset_rows(payload)
    counts = [row[1] for row in rows]
    assert counts == sorted(counts, reverse=True)


def test_order_by_time_grain_expression(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """G.3 — round-20b canonical-alias fix.

    Metabase emits the unaliased CAST(DATE_TRUNC(...)) form in ORDER BY when
    you sort on a time-grain breakout.
    """
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    od_fid = client.field_id_by_name("orders", "ordered_at")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        breakout=[["field", od_fid, {"temporal-unit": "month"}]],
        **{"order-by": [["asc", ["field", od_fid, {"temporal-unit": "month"}]]]},
    ))
    rows = _dataset_rows(payload)
    assert rows
    buckets = [row[0] for row in rows]
    assert buckets == sorted(buckets)


def test_native_sql_order_by_canonical_alias(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """G.4 — direct native-SQL pin for the canonical-alias ORDER BY shape.

    Mirrors the exact SQL that surfaced DEV-1558 bug 7.
    """
    client = metabase_e2e_env.client
    sql = (
        "SELECT DATE_TRUNC('month', ordered_at) AS ordered_at, COUNT(*) AS n "
        "FROM orders GROUP BY DATE_TRUNC('month', ordered_at) "
        "ORDER BY DATE_TRUNC('month', ordered_at)"
    )
    payload = client.dataset(encode_native_query(sql))
    rows = _dataset_rows(payload)
    assert rows
    # Ensure rows are ordered.
    buckets = [row[0] for row in rows]
    assert buckets == sorted(buckets)


# ---------------------------------------------------------------------------
# H. Joins (3)
# ---------------------------------------------------------------------------


def test_join_single_hop_project_joined_column(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    stores_id = client.table_id_by_name("stores")
    store_id_fid = client.field_id_by_name("orders", "store_id")
    stores_pk_fid = client.field_id_by_name("stores", "id")
    stores_name_fid = client.field_id_by_name("stores", "name")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        fields=[["field", stores_name_fid, {"join-alias": "Stores"}]],
        joins=[{
            "source-table": stores_id,
            "alias": "Stores",
            "condition": ["=", ["field", store_id_fid, None], ["field", stores_pk_fid, {"join-alias": "Stores"}]],
            "fields": "none",
        }],
        limit=5,
    ))
    rows = _dataset_rows(payload)
    assert rows
    for row in rows:
        assert isinstance(row[0], str) and row[0]


def test_join_filter_on_joined_column(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    stores_id = client.table_id_by_name("stores")
    store_id_fid = client.field_id_by_name("orders", "store_id")
    stores_pk_fid = client.field_id_by_name("stores", "id")
    stores_name_fid = client.field_id_by_name("stores", "name")
    discover = client.dataset(encode_mbql_query(source_table=stores_id, limit=1))
    name_idx = next(i for i, c in enumerate(_dataset_cols(discover)) if c["name"] == "name")
    sample = _dataset_rows(discover)[0][name_idx]
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["count"]],
        joins=[{
            "source-table": stores_id,
            "alias": "Stores",
            "condition": ["=", ["field", store_id_fid, None], ["field", stores_pk_fid, {"join-alias": "Stores"}]],
            "fields": "none",
        }],
        filter=["=", ["field", stores_name_fid, {"join-alias": "Stores"}], sample],
    ))
    rows = _dataset_rows(payload)
    assert rows
    assert rows[0][0] >= 0


def test_join_aggregate_on_joined_column(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    orders_id = client.table_id_by_name("orders")
    stores_id = client.table_id_by_name("stores")
    store_id_fid = client.field_id_by_name("orders", "store_id")
    stores_pk_fid = client.field_id_by_name("stores", "id")
    stores_tax_fid = client.field_id_by_name("stores", "tax_rate")
    payload = client.dataset(encode_mbql_query(
        source_table=orders_id,
        aggregation=[["avg", ["field", stores_tax_fid, {"join-alias": "Stores"}]]],
        joins=[{
            "source-table": stores_id,
            "alias": "Stores",
            "condition": ["=", ["field", store_id_fid, None], ["field", stores_pk_fid, {"join-alias": "Stores"}]],
            "fields": "none",
        }],
    ))
    rows = _dataset_rows(payload)
    assert rows
    val = rows[0][0]
    assert val is None or float(val) >= 0


# ---------------------------------------------------------------------------
# I. Field-value dropdowns (2)
# ---------------------------------------------------------------------------


def test_field_values_returns_categorical_column(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    fid = client.field_id_by_name("products", "type")
    payload = client.field_values(fid)
    assert "values" in payload
    assert isinstance(payload["values"], list)


def test_field_values_response_shape_valid(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    fid = client.field_id_by_name("supplies", "perishable")
    payload = client.field_values(fid)
    assert isinstance(payload.get("values"), list)
    assert "has_more_values" in payload


# ---------------------------------------------------------------------------
# J. Transactions & multi-statement (3, asyncpg against no-token pg-serve)
# ---------------------------------------------------------------------------


async def test_tx_begin_select_commit_single_q(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        # asyncpg executes simple-Q statements via .execute(); chained statements
        # in one string are accepted iff pg-serve emits exactly one final
        # ReadyForQuery — otherwise asyncpg's protocol state machine raises.
        await conn.execute("BEGIN; SELECT 1; COMMIT;")
    finally:
        await conn.close()


async def test_tx_error_blocks_until_rollback(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        await conn.execute("BEGIN")
        try:
            await conn.execute("SELECT 1 FROM nonexistent_table_dev_1562")
        except Exception:
            pass
        # In aborted state the server must reject subsequent statements with 25P02
        # until ROLLBACK.
        with pytest.raises(Exception) as excinfo:
            await conn.execute("SELECT 1")
        assert "25P02" in str(excinfo.value) or "aborted" in str(excinfo.value).lower()
        await conn.execute("ROLLBACK")
        # After rollback the session must accept queries again.
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


async def test_set_application_name_succeeds(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        await conn.execute("SET application_name = 'dev-1562-test'")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# K. Wire-format coverage (4)
# ---------------------------------------------------------------------------


async def test_date_asyncpg_binary_round_trip(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        val = await conn.fetchval("SELECT ordered_at FROM orders LIMIT 1")
        assert isinstance(val, dt.date)
    finally:
        await conn.close()


def test_date_psycopg_text_round_trip(metabase_e2e_env: MetabaseE2EEnv) -> None:
    """K.2 — DEV-1558 bug 8: text-format DATE OID must decode as a date.

    The regression was the facade emitting ``"YYYY-MM-DD HH:MM:SS"`` for the
    DATE OID in text mode, breaking pgjdbc's ``TimestampUtils.toLocalDate``.
    psycopg2's default DATE parser is built to the same ``YYYY-MM-DD``
    expectation, so a regressed encoder would either trip a parse error
    here or return a string the parser silently coerces wrong. Asserting
    the parsed value is a ``datetime.date`` pins the contract.
    """
    host, port = metabase_e2e_env.pg_no_token
    import psycopg2

    conn = psycopg2.connect(
        host=host, port=port, dbname="jaffle_shop", user="tester", password="x",
        connect_timeout=10,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT ordered_at FROM orders LIMIT 1")
        row = cur.fetchone()
        assert row is not None
        val = row[0]
        # Strict: psycopg gave us back exactly a date (not a datetime, not str).
        assert isinstance(val, dt.date) and not isinstance(val, dt.datetime), (
            f"DATE OID decoded as {type(val).__name__}: {val!r} — bug 8 regressed?"
        )
    finally:
        conn.close()


async def test_timestamp_round_trip_both_formats(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        # No native TIMESTAMP column in jaffle; coerce a DATE through TIMESTAMP.
        val_bin = await conn.fetchval("SELECT CAST(ordered_at AS TIMESTAMP) FROM orders LIMIT 1")
        assert isinstance(val_bin, dt.datetime)
    finally:
        await conn.close()

    import psycopg2
    conn2 = psycopg2.connect(
        host=host, port=port, dbname="jaffle_shop", user="tester", password="x",
        connect_timeout=10,
    )
    try:
        cur = conn2.cursor()
        cur.execute("SELECT CAST(ordered_at AS TIMESTAMP) FROM orders LIMIT 1")
        ts_row = cur.fetchone()
        assert ts_row is not None
        val_text = ts_row[0]
        assert isinstance(val_text, dt.datetime)
    finally:
        conn2.close()


async def test_boolean_double_int_round_trip_both_formats(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token
    conn = await _asyncpg_connect(host, port)
    try:
        # DOUBLE
        d = await conn.fetchval("SELECT order_total FROM orders LIMIT 1")
        assert isinstance(d, float)
        # INT (count is INT)
        i = await conn.fetchval("SELECT COUNT(*) FROM orders")
        assert isinstance(i, int)
        # BOOLEAN — emit a literal
        b = await conn.fetchval("SELECT TRUE")
        assert b is True
    finally:
        await conn.close()

    import psycopg2
    conn2 = psycopg2.connect(
        host=host, port=port, dbname="jaffle_shop", user="tester", password="x",
        connect_timeout=10,
    )
    try:
        cur = conn2.cursor()
        cur.execute("SELECT order_total FROM orders LIMIT 1")
        r1 = cur.fetchone()
        assert r1 is not None and isinstance(r1[0], float)
        cur.execute("SELECT COUNT(*) FROM orders")
        r2 = cur.fetchone()
        assert r2 is not None and isinstance(r2[0], int)
        cur.execute("SELECT TRUE")
        r3 = cur.fetchone()
        assert r3 is not None and r3[0] is True
    finally:
        conn2.close()


# ---------------------------------------------------------------------------
# L. Error paths (3)
# ---------------------------------------------------------------------------


def test_unsupported_sql_returns_error_envelope(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    payload = client.post_raw("/api/dataset", {
        "database": client.db_id,
        **encode_native_query("INSERT INTO orders (id) VALUES ('x')"),
    })
    # Should NOT be a 5xx — Metabase surfaces this as a query-processor error.
    assert payload.status_code < 500
    body = payload.json()
    # Either an error in the body or a non-completed status.
    assert "error" in body or body.get("status") != "completed"


async def test_bad_password_returns_28P01(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port, _token = metabase_e2e_env.pg_token
    with pytest.raises(Exception) as exc:
        await _asyncpg_connect(host, port, password="wrong-password")
    # asyncpg surfaces sqlstate on InvalidPasswordError.
    err = exc.value
    sqlstate = getattr(err, "sqlstate", None) or str(err)
    assert "28P01" in str(sqlstate) or isinstance(err, asyncpg.InvalidPasswordError)


async def test_nonexistent_database_returns_3D000(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port, token = metabase_e2e_env.pg_token
    with pytest.raises(Exception) as exc:
        await asyncpg.connect(
            host=host, port=port, user="tester", password=token,
            database="bogus-not-a-datasource", timeout=10,
        )
    err = exc.value
    sqlstate = getattr(err, "sqlstate", None) or str(err)
    assert "3D000" in str(sqlstate) or isinstance(err, asyncpg.InvalidCatalogNameError)


# ---------------------------------------------------------------------------
# M. Concurrency (2)
# ---------------------------------------------------------------------------


def test_concurrent_dataset_requests(metabase_e2e_env: MetabaseE2EEnv) -> None:
    client = metabase_e2e_env.client
    table_names = ["orders", "customers", "products", "stores", "items", "tweets"]
    table_ids = [client.table_id_by_name(n) for n in table_names]

    def run_one(tid: int) -> int:
        payload = client.dataset(encode_mbql_query(source_table=tid, aggregation=[["count"]]))
        return _dataset_rows(payload)[0][0]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(run_one, tid) for tid in table_ids]
        results = [f.result(timeout=60) for f in futures]
    assert len(results) == 6
    for r in results:
        assert isinstance(r, int) and r >= 0


async def test_asyncpg_concurrent_connections(metabase_e2e_env: MetabaseE2EEnv) -> None:
    host, port = metabase_e2e_env.pg_no_token

    async def one(idx: int) -> int:
        conn = await _asyncpg_connect(host, port)
        try:
            # Each connection runs a different query to ensure no state bleed.
            return await conn.fetchval(f"SELECT {idx} + COUNT(*) FROM orders")
        finally:
            await conn.close()

    results = await asyncio.gather(*[one(i) for i in range(8)])
    assert len(results) == 8
    assert len(set(results)) == 8, "concurrent connections returned duplicate values"
