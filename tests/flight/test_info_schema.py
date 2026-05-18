"""Tests for slayer.flight.info_schema — INFORMATION_SCHEMA.* responses."""

from __future__ import annotations

import pyarrow as pa
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.flight.catalog import build_catalog
from slayer.flight.info_schema import (
    SUPPORTED_INFO_SCHEMA_TABLES,
    match_info_schema,
)


def _parse(sql: str):
    return sqlglot.parse_one(sql)


def _demo_catalog():
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE, description="revenue cents"),
            Column(name="status", type=DataType.TEXT, label="Status"),
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


def test_supported_tables_set() -> None:
    assert SUPPORTED_INFO_SCHEMA_TABLES == {
        "METRICS", "DIMENSIONS", "SCHEMATA", "TABLES", "COLUMNS",
    }


def test_non_info_schema_select_returns_none() -> None:
    assert match_info_schema(parsed=_parse("SELECT * FROM orders"), catalog=_demo_catalog()) is None
    assert match_info_schema(parsed=_parse("SELECT 1"), catalog=_demo_catalog()) is None


def test_unknown_info_schema_table_returns_none() -> None:
    """Unrecognised INFORMATION_SCHEMA.<X> still falls through to the next
    pipeline step rather than being silently treated as a Flight table."""
    assert match_info_schema(parsed=_parse("SELECT * FROM information_schema.bogus"), catalog=_demo_catalog()) is None


def test_foreign_catalog_information_schema_returns_none() -> None:
    """``other.INFORMATION_SCHEMA.METRICS`` must not silently return SLayer
    metadata — falling through to the table-resolution path lets it raise
    the standard ``Unknown catalog`` error."""
    assert match_info_schema(
        parsed=_parse("SELECT * FROM other.INFORMATION_SCHEMA.METRICS"),
        catalog=_demo_catalog(),
    ) is None
    # The SLayer catalog name itself is still accepted.
    assert match_info_schema(
        parsed=_parse("SELECT * FROM slayer.INFORMATION_SCHEMA.METRICS"),
        catalog=_demo_catalog(),
    ) is not None


def test_metrics_table_shape_and_content() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.METRICS"), catalog=cat)
    assert table is not None
    assert table.schema.names == [
        "catalog_name", "schema_name", "table_name", "metric_name",
        "description", "data_type", "label",
    ]
    rows = table.to_pylist()
    # At least one row per non-hidden model.
    by_table = {(r["table_name"], r["metric_name"]) for r in rows}
    assert ("orders", "row_count") in by_table
    assert ("orders", "aov") in by_table
    assert ("orders", "revenue_sum") in by_table
    assert ("customers", "row_count") in by_table
    # Joined-path metric also surfaces.
    assert any(
        r["table_name"] == "orders" and r["metric_name"] == "customers.row_count"
        for r in rows
    )


def test_dimensions_table_shape() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM information_schema.dimensions"), catalog=cat)
    assert table is not None
    assert table.schema.names == [
        "catalog_name", "schema_name", "table_name", "dimension_name",
        "description", "data_type", "label", "is_time",
    ]
    rows = table.to_pylist()
    by_name = {(r["table_name"], r["dimension_name"]): r for r in rows}
    assert ("orders", "ordered_at") in by_name
    assert by_name[("orders", "ordered_at")]["is_time"] is True
    assert by_name[("orders", "status")]["is_time"] is False
    assert ("orders", "customers.region") in by_name


def test_tables_table_shape() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.TABLES"), catalog=cat)
    assert table is not None
    assert table.schema.names == [
        "table_catalog", "table_schema", "table_name", "table_type",
    ]
    rows = table.to_pylist()
    table_names = {r["table_name"] for r in rows}
    assert table_names == {"orders", "customers"}
    types = {r["table_name"]: r["table_type"] for r in rows}
    assert types == {"orders": "TABLE", "customers": "TABLE"}


def test_schemata_table() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA"), catalog=cat)
    assert table is not None
    assert table.schema.names == ["catalog_name", "schema_name"]
    rows = table.to_pylist()
    assert rows == [{"catalog_name": "slayer", "schema_name": "jaffle"}]


def test_columns_table_flattens_metrics_and_dimensions() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"), catalog=cat)
    assert table is not None
    assert table.schema.names == [
        "table_catalog", "table_schema", "table_name", "column_name",
        "ordinal_position", "data_type", "is_nullable", "column_kind",
    ]
    rows = table.to_pylist()
    kinds_by_col = {
        (r["table_name"], r["column_name"]): r["column_kind"] for r in rows
    }
    assert kinds_by_col[("orders", "status")] == "DIMENSION"
    assert kinds_by_col[("orders", "ordered_at")] == "DIMENSION"
    assert kinds_by_col[("orders", "row_count")] == "METRIC"
    assert kinds_by_col[("orders", "aov")] == "METRIC"
    # Ordinal positions are sequential within each (catalog, schema, table).
    for (sch, tbl), ords in _group_ordinals(rows).items():
        assert ords == list(range(1, len(ords) + 1)), f"{(sch, tbl)} ordinals: {ords}"


def _group_ordinals(rows: list[dict]) -> dict[tuple[str, str], list[int]]:
    grouped: dict[tuple[str, str], list[int]] = {}
    for r in rows:
        key = (r["table_schema"], r["table_name"])
        grouped.setdefault(key, []).append(r["ordinal_position"])
    return grouped


def test_case_insensitive_information_schema_match() -> None:
    cat = _demo_catalog()
    for sql in [
        "SELECT * FROM INFORMATION_SCHEMA.METRICS",
        "SELECT * FROM information_schema.metrics",
        "SELECT * FROM Information_Schema.Metrics",
    ]:
        table = match_info_schema(parsed=_parse(sql), catalog=cat)
        assert table is not None, f"failed to match: {sql}"
        assert table.schema.names[0] == "catalog_name"


def test_metric_data_type_renders_as_jdbc_string() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.METRICS"), catalog=cat)
    rows = table.to_pylist()
    aov = next(r for r in rows if r["table_name"] == "orders" and r["metric_name"] == "aov")
    assert aov["data_type"] == "DOUBLE"
    revenue_sum = next(
        r for r in rows
        if r["table_name"] == "orders" and r["metric_name"] == "revenue_sum"
    )
    assert revenue_sum["data_type"] == "DOUBLE"
    row_count_row = next(
        r for r in rows
        if r["table_name"] == "orders" and r["metric_name"] == "row_count"
    )
    assert row_count_row["data_type"] == "BIGINT"


def test_dimension_data_type_renders_as_jdbc_string() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.DIMENSIONS"), catalog=cat)
    rows = table.to_pylist()
    ordered_at = next(
        r for r in rows
        if r["table_name"] == "orders" and r["dimension_name"] == "ordered_at"
    )
    assert ordered_at["data_type"] == "TIMESTAMP"


def test_metrics_table_is_pyarrow_table_with_correct_dtypes() -> None:
    cat = _demo_catalog()
    table = match_info_schema(parsed=_parse("SELECT * FROM INFORMATION_SCHEMA.METRICS"), catalog=cat)
    assert isinstance(table, pa.Table)
    assert table.schema.field("data_type").type == pa.utf8()
