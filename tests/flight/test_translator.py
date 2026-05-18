"""Tests for slayer.flight.translator — SQL → SlayerQuery (DEV-1390 §6)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.flight.catalog import FlightCatalog, build_catalog
from slayer.flight.translator import (
    InfoSchemaResult,
    NoOpResult,
    ProbeResult,
    QueryResult,
    READ_ONLY_MESSAGE,
    TranslationError,
    translate,
)


def _catalog() -> FlightCatalog:
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
            ModelMeasure(name="aov", formula="revenue:sum / *:count",
                         type=DataType.DOUBLE),
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


def _multi_schema_catalog() -> FlightCatalog:
    """Two datasources, one with a unique model name and one with a shared name."""
    a_only = SlayerModel(
        name="unique_a", data_source="dsA", sql_table="unique_a",
        columns=[Column(name="x", type=DataType.INT)],
    )
    shared_a = SlayerModel(
        name="shared", data_source="dsA", sql_table="shared",
        columns=[Column(name="x", type=DataType.INT)],
    )
    shared_b = SlayerModel(
        name="shared", data_source="dsB", sql_table="shared",
        columns=[Column(name="y", type=DataType.INT)],
    )
    return build_catalog(models_by_datasource={"dsA": [a_only, shared_a], "dsB": [shared_b]})


# --- result-type dispatch ----------------------------------------------------


def test_probe_query_returns_probe_result() -> None:
    result = translate(sql="SELECT 1", catalog=_catalog())
    assert isinstance(result, ProbeResult)
    assert result.table.to_pylist() == [{"1": 1}]


def test_info_schema_returns_info_schema_result() -> None:
    result = translate(sql="SELECT * FROM INFORMATION_SCHEMA.METRICS", catalog=_catalog())
    assert isinstance(result, InfoSchemaResult)
    assert result.table.num_rows > 0


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN",
        "START TRANSACTION",
        "COMMIT",
        "ROLLBACK",
        "SET timezone = 'UTC'",
    ],
)
def test_no_op_statements(sql: str) -> None:
    result = translate(sql=sql, catalog=_catalog())
    assert isinstance(result, NoOpResult)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO orders VALUES (1)",
        "UPDATE orders SET id = 2",
        "DELETE FROM orders",
        "CREATE TABLE x (a INT)",
        "DROP TABLE orders",
        "ALTER TABLE orders ADD COLUMN foo INT",
    ],
)
def test_dml_ddl_rejected_read_only(sql: str) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_catalog())
    assert READ_ONLY_MESSAGE in str(exc_info.value)


def test_select_star_on_flight_table_rejected() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT * FROM orders", catalog=_catalog())
    assert "SELECT *" in str(exc_info.value)
    assert "INFORMATION_SCHEMA.METRICS" in str(exc_info.value)


def test_parse_error_translates() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT FROM WHERE", catalog=_catalog())
    assert "parse error" in str(exc_info.value).lower()


# --- table resolution --------------------------------------------------------


def test_schema_qualified_lookup() -> None:
    result = translate(sql="SELECT revenue_sum FROM jaffle.orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.flight_table.name == "orders"
    assert result.schema_name == "jaffle"


def test_catalog_qualified_lookup() -> None:
    result = translate(sql="SELECT revenue_sum FROM slayer.jaffle.orders", catalog=_catalog())
    assert isinstance(result, QueryResult)


def test_bare_name_unique_match() -> None:
    result = translate(sql="SELECT x FROM unique_a", catalog=_multi_schema_catalog())
    assert isinstance(result, QueryResult)
    assert result.flight_table.name == "unique_a"
    assert result.schema_name == "dsA"


def test_bare_name_ambiguous_errors() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT x FROM shared", catalog=_multi_schema_catalog())
    assert "Ambiguous" in str(exc_info.value)
    assert "dsA.shared" in str(exc_info.value)
    assert "dsB.shared" in str(exc_info.value)


def test_bare_name_unknown_errors() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT 1 FROM nope", catalog=_catalog())
    assert "Unknown table" in str(exc_info.value)


def test_unknown_catalog_errors() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT id FROM elsewhere.jaffle.orders", catalog=_catalog())
    assert "Unknown catalog" in str(exc_info.value)


# --- projection translation --------------------------------------------------


def test_simple_metric_and_dimension() -> None:
    result = translate(sql="SELECT revenue_sum, status FROM jaffle.orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    # One measure, one dimension.
    assert result.query.measures is not None and len(result.query.measures) == 1
    assert result.query.measures[0].formula == "revenue:sum"
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["status"]
    # Column-name mapping in projection order.
    mapping = dict(result.column_name_mapping)
    assert mapping == {
        "orders.revenue_sum": "revenue_sum",
        "orders.status": "status",
    }


def test_row_count_metric_maps_to_star_count() -> None:
    result = translate(sql="SELECT row_count FROM orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "*:count"


def test_saved_measure_aov_maps_to_bare_name() -> None:
    result = translate(sql="SELECT aov, status FROM orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    formulas = [m.formula for m in result.query.measures]
    assert "aov" in formulas


def test_cross_model_dotted_dimension() -> None:
    result = translate(sql="SELECT revenue_sum, customers.region FROM orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["customers.region"]
    mapping = dict(result.column_name_mapping)
    assert mapping["orders.customers.region"] == "customers.region"


def test_unknown_projection_item_errors() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT bogus FROM orders", catalog=_catalog())
    assert "Unknown projection item" in str(exc_info.value)


def test_as_alias_renames_projected_column() -> None:
    result = translate(sql="SELECT revenue_sum AS rs FROM orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.rs": "rs"}
    # The SLayerQuery measure carries the alias as its `name`.
    assert result.query.measures is not None
    assert result.query.measures[0].name == "rs"


# --- time-grain wrapping -----------------------------------------------------


def test_month_wrapper_creates_time_dimension() -> None:
    result = translate(sql="SELECT revenue_sum, month(ordered_at) FROM orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    td = result.query.time_dimensions[0]
    assert td.granularity == TimeGranularity.MONTH
    assert td.dimension.full_name == "ordered_at"


def test_date_trunc_creates_time_dimension() -> None:
    result = translate(sql="SELECT date_trunc('month', ordered_at), revenue_sum FROM orders", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_time_grain_on_non_time_column_errors() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT month(status) FROM orders", catalog=_catalog())
    assert "not a time column" in str(exc_info.value)


# --- WHERE translation -------------------------------------------------------


def test_between_lifts_to_date_range() -> None:
    result = translate(sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2024-12-31"]
    # WHERE is fully absorbed — no verbatim filter.
    assert not result.query.filters


def test_half_open_gte_lifts_to_date_range_lo() -> None:
    result = translate(sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01'", catalog=_catalog())
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", None]


def test_combined_half_open_gte_and_lte_set_both_bounds() -> None:
    result = translate(sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'", catalog=_catalog())
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2025-01-01"]


def test_non_time_filter_passes_through_verbatim() -> None:
    result = translate(sql="SELECT revenue_sum, status FROM orders WHERE status = 'completed'", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["status = 'completed'"]


def test_not_equal_rewrites_to_dsl_neq() -> None:
    result = translate(sql="SELECT revenue_sum, status FROM orders WHERE status != 'cancelled'", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["status <> 'cancelled'"]


def test_metric_in_where_passes_through_for_having() -> None:
    result = translate(sql="SELECT revenue_sum, status FROM orders WHERE revenue_sum > 1000", catalog=_catalog())
    assert isinstance(result, QueryResult)
    # Engine auto-routes metric refs to HAVING; translator just emits.
    assert result.query.filters == ["revenue_sum > 1000"]


# --- GROUP BY / ORDER BY / LIMIT / OFFSET ------------------------------------


def test_group_by_matching_derived_set_passes() -> None:
    result = translate(sql="SELECT revenue_sum, status FROM orders GROUP BY status", catalog=_catalog())
    assert isinstance(result, QueryResult)


def test_group_by_omission_is_lenient() -> None:
    # User forgot to GROUP BY `customers.region` — translator silently
    # honours the projection.
    result = translate(sql="SELECT revenue_sum, status, customers.region FROM orders "
        "GROUP BY status", catalog=_catalog())
    assert isinstance(result, QueryResult)


def test_group_by_extra_item_errors_strict() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT revenue_sum, status FROM orders GROUP BY status, customers.region", catalog=_catalog())
    assert "customers.region" in str(exc_info.value)
    assert "not in the projection" in str(exc_info.value)


def test_order_by_by_projected_metric_name() -> None:
    result = translate(sql="SELECT revenue_sum, status FROM orders ORDER BY revenue_sum DESC", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "revenue_sum"
    assert result.query.order[0].direction == "desc"


def test_order_by_unknown_column_errors() -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT revenue_sum, status FROM orders ORDER BY missing ASC", catalog=_catalog())
    assert "not in the projection" in str(exc_info.value)


def test_limit_and_offset_pass_through() -> None:
    result = translate(sql="SELECT revenue_sum FROM orders LIMIT 100 OFFSET 50", catalog=_catalog())
    assert isinstance(result, QueryResult)
    assert result.query.limit == 100
    assert result.query.offset == 50
