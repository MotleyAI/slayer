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
            # DEV-1566: DATE column for CAST(<date> AS TIMESTAMP) coverage.
            Column(name="delivered_at", type=DataType.DATE),
            # DEV-1566: BOOLEAN column for CAST allowlist coverage.
            Column(name="is_paid", type=DataType.BOOLEAN),
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


def test_metabase_aliased_cast_time_trunc_group_by_validates(dialect) -> None:
    """Round 20 follow-up: the SELECT aliases the time-truncated column
    back to its bare name (``AS "ordered_at"``) AND the GROUP BY repeats
    the same CAST/DATE_TRUNC expression unaliased. The projection's
    derived dim set must register both the user alias and the canonical
    ``month(ordered_at)`` form so the GROUP BY validator finds the match."""
    result = translate(
        sql=(
            'SELECT CAST(date_trunc(\'month\', "orders"."ordered_at") AS DATE) '
            'AS "ordered_at", "orders"."status", COUNT(*) AS "count" '
            'FROM "orders" '
            'GROUP BY CAST(date_trunc(\'month\', "orders"."ordered_at") AS DATE), '
            '"orders"."status" '
            'ORDER BY CAST(date_trunc(\'month\', "orders"."ordered_at") AS DATE) ASC, '
            '"orders"."status" ASC'
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_time_grain_on_non_time_column_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT month(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert "not a time column" in str(exc_info.value)


def test_metabase_sunday_week_wrapper_recognised(dialect) -> None:
    """DEV-1562 follow-up: when Metabase issues a week breakout on a DATE
    column, it wraps the truncation to shift Monday-based DATE_TRUNC to
    Sunday-based: ``CAST((CAST(DATE_TRUNC('week', col + INTERVAL '1 day')
    AS DATE) + INTERVAL '-1 day') AS DATE)``. The translator must peel the
    day-offset wrappers on both ends and end up at the WEEK grain over
    the bare column.
    """
    result = translate(
        sql=(
            'SELECT CAST((CAST(date_trunc(\'week\', '
            '("orders"."ordered_at" + INTERVAL \'1 day\')) AS DATE) '
            '+ INTERVAL \'-1 day\') AS DATE) AS "ordered_at", '
            'COUNT(*) AS "count" '
            'FROM "orders" '
            'GROUP BY CAST((CAST(date_trunc(\'week\', '
            '("orders"."ordered_at" + INTERVAL \'1 day\')) AS DATE) '
            '+ INTERVAL \'-1 day\') AS DATE)'
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    assert result.query.time_dimensions[0].granularity == TimeGranularity.WEEK
    assert result.query.time_dimensions[0].dimension.full_name == "ordered_at"


def test_one_day_offset_on_non_week_is_preserved(dialect) -> None:
    """The day-offset unwrap is scoped to WEEK only: a ``date_trunc('month',
    col + INTERVAL '1 day')`` query is NOT a Sunday-week wrapper, so the
    column-side offset must be treated as user intent (the column is not a
    bare ``ordered_at`` ref) and rejected with the existing translator error.
    """
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'month\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_partial_sunday_week_wrapper_is_rejected(dialect) -> None:
    """The Sunday-week unwrap requires BOTH the outer ``-1 day`` shift and
    the inner ``+1 day`` shift to be present together. Half a wrapper is
    user intent (a deliberately-shifted bucket) and must NOT silently
    collapse to plain ``WEEK(col)``.
    """
    # Inner +1 day alone — no outer wrapper. Not Sunday-week; reject.
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )
    # Inner -1 day alone — also not Sunday-week (wrong direction).
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'week\', '
                '("orders"."ordered_at" - INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_outer_week_day_offset_direction_aware(dialect) -> None:
    """Direction matters on the outer wrapper too: Metabase emits
    ``(date_trunc('week', col + INTERVAL '1 day') + INTERVAL '-1 day')``
    — outer net is ``-1 day``. The inverse shape with a ``+1 day`` outer
    offset is not Metabase's shape and must NOT collapse to a plain WEEK
    grain.
    """
    # Matching +1 outer offset on top of a Sunday-week inner is NOT the
    # Metabase shape; treat the whole thing as an unsupported projection.
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT (date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')) + INTERVAL \'1 day\'), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )


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


# --- CAST(<column> AS <type>) projection (DEV-1566) --------------------------
#
# The translator admits CAST around a bare/qualified Column reference, overrides
# the wire OID via projection_types, and leaves the engine SQL projecting the
# bare column. The strict allowlist mirrors the (source, target) pairs the wire
# encoders in slayer/pg_facade/types.py can losslessly handle.


def test_cast_column_projection_admits_date_to_timestamp(dialect) -> None:
    """Linear repro: CAST(<DATE col> AS TIMESTAMP) round-trips through the
    translator. Engine still projects the bare column; the wire layer learns
    the new OID via projection_types."""
    result = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    # Engine sees the bare column — no CAST pushed into SlayerQuery.
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["delivered_at"]
    # Wire schema reflects the casted type, not the column's declared DATE.
    assert result.projection_types == [DataType.TIMESTAMP]
    # column_name_mapping uses the inner column's dotted form as projected_name.
    assert dict(result.column_name_mapping) == {
        "orders.delivered_at": "delivered_at",
    }


def test_cast_column_with_alias_uses_alias_as_projected_name(dialect) -> None:
    """The engine still selects the BARE column; only the projected_name
    (user-facing label) carries the alias. So engine_alias stays
    ``orders.delivered_at`` — NOT ``orders.ts`` (which would be the
    aggregate-alias pattern; aggregates rename the SLayer-level measure)."""
    result = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.delivered_at": "ts"}
    assert result.projection_types == [DataType.TIMESTAMP]


def test_postgres_double_colon_cast_works() -> None:
    """``col::TYPE`` sugar parses to ``exp.Cast`` under the postgres dialect —
    same outcome as the keyword form."""
    result = translate(
        sql="SELECT delivered_at::TIMESTAMP FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TIMESTAMP]


def test_cast_joined_column_projection(dialect) -> None:
    """CAST around a joined dotted column ref resolves through the same
    cross-model dimension path as a bare projection."""
    result = translate(
        sql="SELECT CAST(customers.region AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["customers.region"]
    assert result.projection_types == [DataType.TEXT]


@pytest.mark.parametrize("type_name", ["UUID", "JSON", "ARRAY<INT>", "STRUCT<x INT>"])
def test_cast_unsupported_target_type_raises(type_name: str, dialect) -> None:
    """Cast target types not in the SLayer DataType mapping fall through to
    the existing 'Unsupported projection expression' error."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=f"SELECT CAST(revenue AS {type_name}) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert "Unsupported projection expression" in str(exc_info.value)


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("status", "INT"),         # TEXT → INT
        ("status", "BOOLEAN"),     # TEXT → BOOLEAN
        ("revenue", "BOOLEAN"),    # DOUBLE → BOOLEAN
        ("revenue", "INT"),        # DOUBLE → INT (lossy; dropped from allowlist)
        ("revenue", "DATE"),       # DOUBLE → DATE
        ("is_paid", "DATE"),       # BOOLEAN → DATE
        ("is_paid", "TIMESTAMP"),  # BOOLEAN → TIMESTAMP
        ("is_paid", "INT"),        # BOOLEAN → INT
        ("delivered_at", "INT"),   # DATE → INT
        ("delivered_at", "DOUBLE"),# DATE → DOUBLE
        ("ordered_at", "INT"),     # TIMESTAMP → INT
        ("ordered_at", "DOUBLE"),  # TIMESTAMP → DOUBLE
        ("id", "DATE"),            # INT → DATE
        ("id", "BOOLEAN"),         # INT → BOOLEAN
    ],
)
def test_cast_rejected_coercions_raise(col: str, target: str, dialect) -> None:
    """Pairs outside the §5 allowlist surface a strict, named error message."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=f"SELECT CAST({col} AS {target}) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert "Unsupported CAST" in str(exc_info.value)


def test_cast_rejected_error_message_pins_full_contract(dialect) -> None:
    """The rejected-coercion error must name the source DataType, target
    DataType, the offending SQL, and link the docs reference. Vague messages
    would regress agent-debuggability."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(status AS INT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "Unsupported CAST" in msg
    assert "TEXT" in msg              # source DataType
    assert "INT" in msg               # target DataType
    assert "CAST(status AS INT)" in msg  # offending SQL fragment
    assert "docs/interfaces/pg-facade.md" in msg  # docs pointer


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("status", "TEXT"),         # TEXT → TEXT
        ("revenue", "DOUBLE"),      # DOUBLE → DOUBLE
        ("id", "INT"),              # INT → INT
        ("delivered_at", "DATE"),   # DATE → DATE
        ("ordered_at", "TIMESTAMP"),# TIMESTAMP → TIMESTAMP
        ("is_paid", "BOOLEAN"),     # BOOLEAN → BOOLEAN
    ],
)
def test_cast_identity_pair_admitted_for_every_type(col: str, target: str, dialect) -> None:
    result = translate(
        sql=f"SELECT CAST({col} AS {target}) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType(target)]


@pytest.mark.parametrize(
    ("col", "target", "expected_type"),
    [
        # Date/time pair
        ("delivered_at", "TIMESTAMP", DataType.TIMESTAMP),
        ("ordered_at", "DATE", DataType.DATE),
        # Numeric pair (INT→DOUBLE only; DOUBLE→INT dropped)
        ("id", "DOUBLE", DataType.DOUBLE),
        # X → TEXT (always admitted)
        ("delivered_at", "TEXT", DataType.TEXT),
        ("ordered_at", "TEXT", DataType.TEXT),
        ("id", "TEXT", DataType.TEXT),
        ("revenue", "TEXT", DataType.TEXT),
        ("is_paid", "TEXT", DataType.TEXT),
        ("status", "TEXT", DataType.TEXT),
    ],
)
def test_cast_admitted_coercions_parametrised(
    col: str, target: str, expected_type: DataType, dialect,
) -> None:
    result = translate(
        sql=f"SELECT CAST({col} AS {target}) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [expected_type]


def test_cast_try_cast_rejected(dialect) -> None:
    """TRY_CAST parses to exp.TryCast, not exp.Cast, and is explicitly out
    of scope — Postgres has no native TRY_CAST."""
    with pytest.raises(TranslationError):
        translate(
            sql="SELECT TRY_CAST(status AS INT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_aggregate_inner_rejected(dialect) -> None:
    """CAST(<aggregate> AS T) is explicitly out of scope (Column only)."""
    with pytest.raises(TranslationError):
        translate(
            sql="SELECT CAST(SUM(revenue) AS DOUBLE) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_time_grain_compat_unchanged(dialect) -> None:
    """CAST(DATE_TRUNC(...) AS DATE) is the time-grain pattern — body.this is
    DateTrunc, not Column, so the new column-CAST branch returns None and the
    existing time-grain CAST-unwrap still handles it."""
    result = translate(
        sql="SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_cast_order_by_canonical_form_resolves(dialect) -> None:
    """An unaliased CAST projection ORDER-BY'd by the same CAST form resolves
    to the underlying engine column via the canonical-form registration."""
    result = translate(
        sql=(
            "SELECT CAST(delivered_at AS TIMESTAMP) FROM orders "
            "ORDER BY CAST(delivered_at AS TIMESTAMP) ASC"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "delivered_at"
    assert result.query.order[0].direction == "asc"


def test_cast_group_by_canonical_form_resolves(dialect) -> None:
    """A CAST in projection + the same CAST repeated in GROUP BY (dim-only
    dedupe shape) must validate, mirroring the time-grain GROUP BY canonical
    registration."""
    result = translate(
        sql=(
            "SELECT CAST(delivered_at AS TIMESTAMP) FROM orders "
            "GROUP BY CAST(delivered_at AS TIMESTAMP)"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TIMESTAMP]


def test_cast_unknown_source_datatype_admits_text_only(dialect) -> None:
    """Custom metrics with declared data_type=None admit ONLY CAST→TEXT;
    every other target is rejected so wire-encode-time crashes don't surface
    as opaque connection errors. Custom aggregations carry data_type=None
    when constructed without an explicit type — exercise that path."""
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        measures=[
            # No `type=` → ModelMeasure.type is None.
            ModelMeasure(name="custom_metric", formula="amount:sum"),
        ],
    )
    catalog = build_catalog(models_by_datasource={"jaffle": [orders]})

    # → TEXT admitted.
    result = translate(
        sql="SELECT CAST(custom_metric AS TEXT) FROM orders",
        catalog=catalog, dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]

    # → TIMESTAMP rejected with the strict-allowlist message.
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(custom_metric AS TIMESTAMP) FROM orders",
            catalog=catalog, dialect=dialect,
        )
    assert "Unsupported CAST" in str(exc_info.value)


@pytest.mark.parametrize(
    "col", ["delivered_at", "ordered_at", "id", "revenue", "is_paid", "status"],
)
def test_cast_text_target_admitted_from_every_source(col: str, dialect) -> None:
    """X → TEXT is always admitted (stringification is universal)."""
    result = translate(
        sql=f"SELECT CAST({col} AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]


@pytest.mark.parametrize(
    ("type_alias", "expected"),
    [
        # TEXT-family aliases.
        ("VARCHAR", DataType.TEXT),
        ("CHAR", DataType.TEXT),
        # INT-family aliases.
        ("INTEGER", DataType.INT),
        ("BIGINT", DataType.INT),
        ("SMALLINT", DataType.INT),
        # DOUBLE-family aliases (floating/decimal collapse to DOUBLE).
        ("FLOAT", DataType.DOUBLE),
        ("REAL", DataType.DOUBLE),
        ("DECIMAL", DataType.DOUBLE),
        ("NUMERIC", DataType.DOUBLE),
        # TIMESTAMP-family aliases.
        ("DATETIME", DataType.TIMESTAMP),
        ("TIMESTAMPTZ", DataType.TIMESTAMP),
    ],
)
def test_cast_sqlglot_type_aliases_map_to_slayer_datatype(
    type_alias: str, expected: DataType,
) -> None:
    """Each accepted sqlglot DataType.Type alias collapses onto the canonical
    SLayer DataType. Pinned under postgres dialect since some aliases
    (TIMESTAMPTZ) only parse cleanly there."""
    # Pick a source column whose declared type makes <source, expected> an
    # admitted pair: identity on the expected canonical type.
    source_col = {
        DataType.TEXT: "status",
        DataType.INT: "id",
        DataType.DOUBLE: "revenue",
        DataType.TIMESTAMP: "ordered_at",
    }[expected]
    result = translate(
        sql=f"SELECT CAST({source_col} AS {type_alias}) FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [expected]


def test_cast_parameterised_type_form_works() -> None:
    """sqlglot represents ``VARCHAR(255)`` / ``DECIMAL(10,2)`` etc. with their
    precision modifier on the SAME ``DataType.Type`` member, so the mapping
    collapses precision implicitly — SLayer wire types don't carry it."""
    result = translate(
        sql="SELECT CAST(status AS VARCHAR(255)) FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]


def test_cast_non_column_non_aggregate_inner_rejected(dialect) -> None:
    """The CAST detector REQUIRES body.this == exp.Column. Inner expressions
    that aren't bare columns (hygiene wrappers like SUBSTRING, function
    calls, arithmetic) must not accidentally route through the CAST branch
    — they must fall through to the existing fallback."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(SUBSTRING(status, 1, 1) AS TEXT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    # Specifically: NOT the strict-allowlist message.
    assert "Unsupported CAST" not in str(exc_info.value)


def test_cast_qualified_ref_order_by_canonical_resolves(dialect) -> None:
    """Canonical CAST form must work for qualified (joined) refs too.
    `cast(customers.region as text)` is the canonical key the registration
    pushes into item_by_projected_name."""
    result = translate(
        sql=(
            "SELECT CAST(customers.region AS TEXT) FROM orders "
            "ORDER BY CAST(customers.region AS TEXT) ASC"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.full_name == "customers.region"
    assert result.query.order[0].direction == "asc"


def test_cast_qualified_ref_group_by_canonical_resolves(dialect) -> None:
    """Same qualified-ref canonical-form coverage on the GROUP BY path."""
    result = translate(
        sql=(
            "SELECT CAST(customers.region AS TEXT) FROM orders "
            "GROUP BY CAST(customers.region AS TEXT)"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]


def test_cast_aliased_projection_order_by_canonical_resolves(dialect) -> None:
    """The whole point of registering the canonical form via setdefault is
    that ORDER BY by the CAST shape resolves even when the SELECT projection
    carries a different user alias. ``SELECT CAST(x AS T) AS y ... ORDER BY
    CAST(x AS T)`` must match the projection via the canonical key — not
    via the alias `y`."""
    result = translate(
        sql=(
            "SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders "
            "ORDER BY CAST(delivered_at AS TIMESTAMP) ASC"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "delivered_at"
    assert result.query.order[0].direction == "asc"


def test_cast_aliased_projection_group_by_canonical_resolves(dialect) -> None:
    """Same setdefault canonical-form coverage on the GROUP BY path with
    an aliased projection."""
    result = translate(
        sql=(
            "SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders "
            "GROUP BY CAST(delivered_at AS TIMESTAMP)"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.delivered_at": "ts"}


def test_cast_metric_projection_overrides_wire_type(dialect) -> None:
    """A CAST around a metric reference resolves through the metric path
    (`_record_metric`), and the cast target wins over the declared metric type."""
    result = translate(
        sql="SELECT CAST(revenue_sum AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "revenue:sum"
    # Declared metric type is DOUBLE; CAST(<metric> AS TEXT) overrides.
    assert result.projection_types == [DataType.TEXT]


# --- allow_column_cast gate (Codex round 1 — Flight regression guard) --------
#
# DEV-1566 admits CAST(<col> AS <type>) projection in the shared translator.
# The Flight facade materialises rows via pa.Table.from_pylist against a
# catalog-typed schema, which raises ArrowTypeError on values whose Python
# type doesn't match the declared Arrow type (date vs timestamp, bool vs
# utf8, etc.). The Flight shim passes allow_column_cast=False to reject the
# new projection shape at translate time; this test pins the gate.


def test_allow_column_cast_false_rejects_cast_projection() -> None:
    """With allow_column_cast=False, the CAST projection branch is skipped
    and the body falls through to the 'Unsupported projection expression'
    error (the existing terminal path)."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
            catalog=_catalog(), dialect=None, allow_column_cast=False,
        )
    assert "Unsupported projection expression" in str(exc_info.value)


def test_allow_column_cast_false_leaves_time_grain_cast_unwrap_working() -> None:
    """The gate must not regress the time-grain CAST-unwrap path — the
    Metabase fingerprint ``CAST(DATE_TRUNC(...) AS DATE)`` is detected by
    ``_detect_time_grain``, which runs BEFORE the column-CAST branch."""
    result = translate(
        sql=(
            "SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum "
            "FROM orders"
        ),
        catalog=_catalog(), dialect=None, allow_column_cast=False,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_allow_column_cast_default_true_unchanged(dialect) -> None:
    """Sanity: the default-True path is the same as not passing the kwarg
    (pg-facade behaviour). Pinned so the default never silently flips."""
    explicit = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect, allow_column_cast=True,
    )
    implicit = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(explicit, QueryResult)
    assert isinstance(implicit, QueryResult)
    assert explicit.projection_types == implicit.projection_types == [DataType.TIMESTAMP]
