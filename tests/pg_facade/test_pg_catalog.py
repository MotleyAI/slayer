"""Tests for slayer.pg_facade.pg_catalog — minimum-viable pg_catalog.* (DEV-1486)."""

from __future__ import annotations

import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.facade.catalog import build_catalog
from slayer.pg_facade.pg_catalog import (
    SUPPORTED_PG_CATALOG_TABLES,
    match_pg_catalog,
    stable_oid,
)
from slayer.pg_facade.protocol import OID_FLOAT8, OID_TEXT


def _parse(sql: str):
    return sqlglot.parse_one(sql, dialect="postgres")


def _catalog():
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        measures=[
            ModelMeasure(name="aov", formula="revenue:sum / *:count", type=DataType.DOUBLE),
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


def test_non_pg_catalog_query_returns_none() -> None:
    assert match_pg_catalog(_parse("SELECT revenue_sum FROM orders"), _catalog()) is None
    assert match_pg_catalog(_parse("SELECT 1"), _catalog()) is None


def test_supported_tables_set() -> None:
    assert SUPPORTED_PG_CATALOG_TABLES == {
        "pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_proc", "pg_settings",
    }


def test_pg_namespace_has_public_and_pg_catalog() -> None:
    batch = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_namespace"), _catalog())
    assert batch is not None
    names = {r["nspname"] for r in batch.rows}
    # `public` for user models; `pg_catalog` so pg_type.typnamespace=11 resolves.
    assert names == {"public", "pg_catalog"}
    by_name = {r["nspname"]: r for r in batch.rows}
    assert by_name["pg_catalog"]["oid"] == 11


def test_pg_class_one_row_per_model_relkind_r() -> None:
    batch = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_class"), _catalog())
    assert batch is not None
    by_name = {r["relname"]: r for r in batch.rows}
    assert set(by_name) == {"orders", "customers"}
    assert all(r["relkind"] == "r" for r in batch.rows)
    assert by_name["orders"]["relnatts"] > 0


def test_pg_attribute_oids_match_datatype() -> None:
    cat = _catalog()
    orders_oid = stable_oid("jaffle", "orders")
    batch = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_attribute"), cat)
    assert batch is not None
    orders_attrs = {
        r["attname"]: r for r in batch.rows if r["attrelid"] == orders_oid
    }
    # A TEXT dimension → text OID; a DOUBLE metric → float8 OID.
    assert orders_attrs["status"]["atttypid"] == OID_TEXT
    assert orders_attrs["revenue_sum"]["atttypid"] == OID_FLOAT8
    # attnum is 1-based and sequential.
    nums = sorted(r["attnum"] for r in orders_attrs.values())
    assert nums == list(range(1, len(nums) + 1))


def test_pg_type_covers_six_oids() -> None:
    batch = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_type"), _catalog())
    assert batch is not None
    oids = {r["oid"] for r in batch.rows}
    assert oids == {16, 20, 25, 701, 1082, 1114}
    by_oid = {r["oid"]: r for r in batch.rows}
    assert by_oid[25]["typname"] == "text"
    assert by_oid[20]["typname"] == "int8"


def test_pg_proc_is_empty() -> None:
    batch = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_proc"), _catalog())
    assert batch is not None
    assert batch.rows == []


def test_pg_settings_has_core_params() -> None:
    batch = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_settings"), _catalog())
    assert batch is not None
    names = {r["name"] for r in batch.rows}
    assert {"server_version", "client_encoding", "TimeZone"} <= names


def test_where_is_ignored_returns_all_rows() -> None:
    batch = match_pg_catalog(
        _parse("SELECT * FROM pg_catalog.pg_class WHERE relname = 'orders'"), _catalog()
    )
    assert batch is not None
    # WHERE ignored — both models still present.
    assert {r["relname"] for r in batch.rows} == {"orders", "customers"}


def test_bare_name_and_pg_catalog_qualified_both_resolve() -> None:
    cat = _catalog()
    qualified = match_pg_catalog(_parse("SELECT * FROM pg_catalog.pg_namespace"), cat)
    bare = match_pg_catalog(_parse("SELECT * FROM pg_namespace"), cat)
    assert qualified is not None and bare is not None
    assert qualified.rows == bare.rows


def test_foreign_schema_does_not_match() -> None:
    assert match_pg_catalog(_parse("SELECT * FROM otherschema.pg_class"), _catalog()) is None


def test_stable_oid_is_deterministic_and_positive() -> None:
    a = stable_oid("jaffle", "orders")
    b = stable_oid("jaffle", "orders")
    assert a == b
    assert 0 <= a <= 0x7FFFFFFF
    # Different identifiers yield different OIDs (no salt across runs).
    assert stable_oid("jaffle", "orders") != stable_oid("jaffle", "customers")


def test_stable_oid_matches_crc32_not_builtin_hash() -> None:
    # Pin the exact crc32-derived value so a regression to the per-process
    # salted builtin hash() (which would NOT be stable across restarts) fails.
    import zlib

    assert stable_oid("jaffle", "orders") == zlib.crc32(b"jaffle.orders") & 0x7FFFFFFF
