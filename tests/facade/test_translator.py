"""Tests for slayer.facade.translator — SQL → SlayerQuery (DEV-1390 §6, DEV-1486).

The translator is shared between the Flight SQL and Postgres facades. The
mapping is identical for both, so the structural tests are parametrised over
``dialect in (None, "postgres")``. Postgres-specific behaviour (aggregate-SQL
mapping, command_tag, dialect-only parse acceptance) is exercised explicitly.
"""

from __future__ import annotations

import logging

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.translator import (
    AGG_OVER_MEASURE_MESSAGE,
    InfoSchemaResult,
    NoOpResult,
    ProbeResult,
    QueryResult,
    READ_ONLY_MESSAGE,
    TranslationError,
    translate,
)


@pytest.fixture(params=[None, "postgres"])
def dialect(request):
    """Run each structural test under both the dialect-less (Flight) and the
    Postgres parse modes — the mapping must be identical."""
    return request.param


def _catalog() -> FacadeCatalog:
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


def _multi_schema_catalog() -> FacadeCatalog:
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


def test_probe_query_returns_probe_result(dialect) -> None:
    result = translate(sql="SELECT 1", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, ProbeResult)
    assert result.batch.rows == [{"1": 1}]


def test_info_schema_returns_info_schema_result(dialect) -> None:
    result = translate(
        sql="SELECT * FROM INFORMATION_SCHEMA.METRICS", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, InfoSchemaResult)
    assert len(result.batch.rows) > 0


@pytest.mark.parametrize(
    ("sql", "expected_tag"),
    [
        ("BEGIN", "BEGIN"),
        ("START TRANSACTION", "START TRANSACTION"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK", "ROLLBACK"),
        ("SET timezone = 'UTC'", "SET"),
        # pgjdbc setTransactionIsolation() — parses only as a Command fallback.
        (
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            "SET",
        ),
    ],
)
def test_no_op_statements_carry_command_tag(sql: str, expected_tag: str, dialect) -> None:
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.command_tag == expected_tag


def test_show_statement_is_noop_with_tag(dialect) -> None:
    result = translate(sql="SHOW search_path", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SHOW"


def test_command_fallback_warning_suppressed_during_translate(dialect, caplog) -> None:
    # sqlglot warns when a statement parses to the generic Command node; for
    # facade traffic that path is expected and handled, so translate() must
    # not leak one warning line per BI connection.
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        result = translate(
            sql="SHOW TRANSACTION ISOLATION LEVEL", catalog=_catalog(), dialect=dialect
        )
    assert isinstance(result, NoOpResult)
    assert not [r for r in caplog.records if "Falling back" in r.getMessage()]


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
def test_dml_ddl_rejected_read_only(sql: str, dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert READ_ONLY_MESSAGE in str(exc_info.value)


def test_select_star_on_table_rejected(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT * FROM orders", catalog=_catalog(), dialect=dialect)
    assert "SELECT *" in str(exc_info.value)
    assert "INFORMATION_SCHEMA.METRICS" in str(exc_info.value)


def test_parse_error_translates(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT FROM WHERE", catalog=_catalog(), dialect=dialect)
    assert "parse error" in str(exc_info.value).lower()


# --- table resolution --------------------------------------------------------


def test_schema_qualified_lookup(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum FROM jaffle.orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.facade_table.name == "orders"
    assert result.schema_name == "jaffle"


def test_catalog_qualified_lookup(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum FROM slayer.jaffle.orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_bare_name_unique_match(dialect) -> None:
    result = translate(
        sql="SELECT x FROM unique_a", catalog=_multi_schema_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.facade_table.name == "unique_a"
    assert result.schema_name == "dsA"


def test_bare_name_ambiguous_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT x FROM shared", catalog=_multi_schema_catalog(), dialect=dialect)
    assert "Ambiguous" in str(exc_info.value)
    assert "dsA.shared" in str(exc_info.value)
    assert "dsB.shared" in str(exc_info.value)


def test_bare_name_unknown_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT 1 FROM nope", catalog=_catalog(), dialect=dialect)
    assert "Unknown table" in str(exc_info.value)


def test_unknown_catalog_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT id FROM elsewhere.jaffle.orders", catalog=_catalog(), dialect=dialect)
    assert "Unknown catalog" in str(exc_info.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT revenue_sum FROM slayer.jaffle.orders",
        "SELECT revenue_sum FROM SLAYER.jaffle.orders",
        "SELECT revenue_sum FROM Slayer.jaffle.orders",
    ],
)
def test_catalog_qualifier_is_case_insensitive(sql: str, dialect) -> None:
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult), sql


# --- projection translation --------------------------------------------------


def test_simple_metric_and_dimension(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM jaffle.orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert result.query.measures is not None and len(result.query.measures) == 1
    assert result.query.measures[0].formula == "revenue:sum"
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["status"]
    mapping = dict(result.column_name_mapping)
    assert mapping == {
        "orders.revenue_sum": "revenue_sum",
        "orders.status": "status",
    }


def test_row_count_metric_maps_to_star_count(dialect) -> None:
    result = translate(sql="SELECT row_count FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "*:count"


def test_saved_measure_aov_maps_to_bare_name(dialect) -> None:
    result = translate(sql="SELECT aov, status FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    formulas = [m.formula for m in result.query.measures]
    assert "aov" in formulas


def test_cross_model_dotted_dimension(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, customers.region FROM orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["customers.region"]
    mapping = dict(result.column_name_mapping)
    assert mapping["orders.customers.region"] == "customers.region"


def test_unknown_projection_item_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT bogus FROM orders", catalog=_catalog(), dialect=dialect)
    assert "Unknown projection item" in str(exc_info.value)


def test_as_alias_renames_projected_column(dialect) -> None:
    result = translate(sql="SELECT revenue_sum AS rs FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.rs": "rs"}
    assert result.query.measures is not None
    assert result.query.measures[0].name == "rs"


# --- aggregate-SQL → metric mapping (DEV-1486 decision 21) -------------------


def test_sum_of_column_maps_to_measure(dialect) -> None:
    result = translate(sql="SELECT SUM(revenue) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "revenue:sum"
    # Default (unaliased) projected name mirrors the catalog metric name.
    assert dict(result.column_name_mapping) == {"orders.revenue_sum": "revenue_sum"}


def test_count_star_maps_to_star_count(dialect) -> None:
    result = translate(sql="SELECT COUNT(*) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "*:count"


def test_count_of_column_maps_to_count(dialect) -> None:
    result = translate(sql="SELECT COUNT(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "status:count"


def test_count_distinct_maps_to_count_distinct(dialect) -> None:
    result = translate(
        sql="SELECT COUNT(DISTINCT status) FROM orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "status:count_distinct"


def test_aggregate_over_joined_column_resolves_same_as_named_metric(dialect) -> None:
    # A joined-column aggregate resolves to the same cross-model metric a bare
    # named projection would (`customers.region_count`). Cross-model metric
    # *projection* is a pre-existing unsupported path (SlayerQuery measure names
    # can't contain dots — DEV-1448 territory), so both forms fail identically
    # at query construction. We assert the two are equivalent rather than that
    # they succeed, so the aggregate sugar is provably just an alias.
    agg_err = _raises_message("SELECT COUNT(customers.region) FROM orders", dialect)
    named_err = _raises_message("SELECT customers.region_count FROM orders", dialect)
    assert agg_err == named_err


def _raises_message(sql: str, dialect) -> str:
    try:
        translate(sql=sql, catalog=_catalog(), dialect=dialect)
    except Exception as exc:  # noqa: BLE001 — comparing failure parity
        return f"{type(exc).__name__}"
    return "OK"


@pytest.mark.parametrize("fn,agg", [("AVG", "avg"), ("MIN", "min"), ("MAX", "max")])
def test_avg_min_max_of_column_map(fn: str, agg: str, dialect) -> None:
    result = translate(sql=f"SELECT {fn}(revenue) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == f"revenue:{agg}"


def test_aggregate_alias_renames_projection(dialect) -> None:
    result = translate(sql="SELECT SUM(revenue) AS rev FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.rev": "rev"}
    assert result.query.measures is not None
    assert result.query.measures[0].name == "rev"
    assert result.query.measures[0].formula == "revenue:sum"


def test_aggregate_ineligible_for_column_errors(dialect) -> None:
    # SUM is not in TEXT's default aggregation set.
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT SUM(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert "status:sum" in str(exc_info.value)


def test_aggregate_over_saved_measure_errors_with_followup(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT SUM(aov) FROM orders", catalog=_catalog(), dialect=dialect)
    assert AGG_OVER_MEASURE_MESSAGE in str(exc_info.value)


def test_aggregate_over_expression_errors_with_followup(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT SUM(revenue + revenue) FROM orders", catalog=_catalog(), dialect=dialect)
    assert AGG_OVER_MEASURE_MESSAGE in str(exc_info.value)


def test_count_of_expression_is_not_row_count(dialect) -> None:
    # COUNT(<expression>) must NOT be mis-mapped to *:count (row count).
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT COUNT(CASE WHEN status = 'x' THEN 1 END) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert AGG_OVER_MEASURE_MESSAGE in str(exc_info.value)


def test_having_aggregate_maps_to_colon_filter(dialect) -> None:
    result = translate(
        sql="SELECT status, SUM(revenue) FROM orders GROUP BY status "
            "HAVING SUM(revenue) > 1000",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


def test_having_aggregate_literal_on_left_flips(dialect) -> None:
    result = translate(
        sql="SELECT status, SUM(revenue) FROM orders GROUP BY status "
            "HAVING 1000 < SUM(revenue)",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


def test_order_by_aggregate_expression_resolves(dialect) -> None:
    result = translate(
        sql="SELECT SUM(revenue) FROM orders ORDER BY SUM(revenue) DESC",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "revenue_sum"
    assert result.query.order[0].direction == "desc"


# --- time-grain wrapping -----------------------------------------------------


def test_month_wrapper_creates_time_dimension(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, month(ordered_at) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    td = result.query.time_dimensions[0]
    assert td.granularity == TimeGranularity.MONTH
    assert td.dimension.full_name == "ordered_at"


def test_date_trunc_creates_time_dimension(dialect) -> None:
    result = translate(
        sql="SELECT date_trunc('month', ordered_at), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_cast_wrapped_time_trunc_creates_time_dimension(dialect) -> None:
    """Live Metabase repro: when ``ordered_at`` is DATE-typed Metabase
    emits ``CAST(TIMESTAMP_TRUNC(ordered_at, MONTH) AS DATE)`` because
    the truncation function widens to TIMESTAMP. The outer CAST is
    semantically irrelevant; the translator must unwrap it and still
    recognise the inner time-grain shape."""
    result = translate(
        sql="SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    td = result.query.time_dimensions[0]
    assert td.granularity == TimeGranularity.MONTH
    assert td.dimension.full_name == "ordered_at"


def test_time_grain_on_non_time_column_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT month(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert "not a time column" in str(exc_info.value)


# --- dialect-only parse acceptance ------------------------------------------


def test_postgres_dialect_parses_cast_syntax() -> None:
    # `::text` cast in a WHERE predicate parses under the postgres dialect
    # (it would otherwise be a different parse). The predicate is emitted
    # verbatim into filters; engine-side Mode-B handling is out of scope here.
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status::text = 'x'",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)


def test_postgres_ilike_parses_and_emits_verbatim() -> None:
    # ILIKE parses under postgres and is emitted verbatim. The engine's Mode-B
    # DSL parser rejects ILIKE at execution time — a documented Phase-1 limit.
    # Here we only assert the translator does NOT special-case it.
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status ILIKE 'compl%'",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters is not None
    assert any("ILIKE" in f.upper() for f in result.query.filters)


# --- WHERE translation -------------------------------------------------------


def test_between_lifts_to_date_range(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2024-12-31"]
    assert not result.query.filters


def test_half_open_gte_lifts_to_date_range_lo(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", None]


def test_combined_half_open_gte_and_lte_set_both_bounds(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2025-01-01"]


def test_non_time_filter_passes_through_verbatim(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status = 'completed'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["status = 'completed'"]


def test_not_equal_rewrites_to_dsl_neq(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status != 'cancelled'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["status <> 'cancelled'"]


def test_metric_in_where_passes_through_for_having(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE revenue_sum > 1000",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue_sum > 1000"]


# --- GROUP BY / ORDER BY / LIMIT / OFFSET ------------------------------------


def test_group_by_matching_derived_set_passes(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders GROUP BY status",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_group_by_positional_is_ignored(dialect) -> None:
    result = translate(
        sql="SELECT status, SUM(revenue) FROM orders GROUP BY 1",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_group_by_omission_is_lenient(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status, customers.region FROM orders "
        "GROUP BY status",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_group_by_extra_item_errors_strict(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT revenue_sum, status FROM orders GROUP BY status, customers.region",
            catalog=_catalog(), dialect=dialect,
        )
    assert "customers.region" in str(exc_info.value)
    assert "not in the projection" in str(exc_info.value)


def test_order_by_by_projected_metric_name(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders ORDER BY revenue_sum DESC",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "revenue_sum"
    assert result.query.order[0].direction == "desc"


def test_order_by_unknown_column_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT revenue_sum, status FROM orders ORDER BY missing ASC",
            catalog=_catalog(), dialect=dialect,
        )
    assert "not in the projection" in str(exc_info.value)


def test_limit_and_offset_pass_through(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum FROM orders LIMIT 100 OFFSET 50",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.limit == 100
    assert result.query.offset == 50
