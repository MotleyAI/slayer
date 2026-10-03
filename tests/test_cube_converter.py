"""Tests for the Cube → SLayer converter (slayer/cube/converter.py).

Projects are built in Python (parser is tested separately) so these
pin the mapping semantics directly.
"""

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.format import NumberFormatType
from slayer.core.models import Column, SlayerModel
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeJoin,
    CubeMeasure,
    CubeMeasureFilter,
    CubeProject,
    CubeSegment,
)
from slayer.cube.report import CubeIssueCategory
from slayer.engine.syntax import AggCall, DottedRef, Ref, parse_expr
from tests._cube_helpers import DS, column, convert, measure, meta


def _measure_column(model: SlayerModel, measure_name: str) -> Column:
    """Return the Column an `<agg>(<col>)` measure formula aggregates."""
    m = measure(model, measure_name)
    parsed = parse_expr(m.formula)
    assert isinstance(parsed, AggCall), m.formula
    source = parsed.source
    assert isinstance(source, (Ref, DottedRef)), m.formula
    col_ref = ".".join(source.parts) if isinstance(source, DottedRef) else source.name
    return column(model, col_ref)


# ── 4.1 cube → model ───────────────────────────────────────────────────────

def test_cube_becomes_table_backed_model():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", description="Customer orders",
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True)],
    )])
    models, _ = convert(project)
    orders = models["orders"]
    assert orders.sql_table == "public.orders"
    assert orders.data_source == DS
    assert orders.description == "Customer orders"
    assert column(orders, "id").primary_key is True
    assert column(orders, "id").type == DataType.DOUBLE


def test_public_false_dimension_is_hidden():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[
            CubeDimension(name="id", sql="{CUBE}.id", type="number"),
            CubeDimension(name="secret", sql="{CUBE}.secret", type="string", public=False),
        ],
    )])
    models, _ = convert(project)
    assert column(models["orders"], "secret").hidden is True
    assert column(models["orders"], "id").hidden is False


def test_public_false_cube_is_hidden_and_title_goes_to_meta():
    project = CubeProject(cubes=[CubeCube(
        name="internal", sql_table="public.internal", public=False, title="Internal",
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = convert(project)
    assert models["internal"].hidden is True
    assert meta(models["internal"])["cube_title"] == "Internal"


def test_per_cube_data_source_reported_and_stashed():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", data_source="warehouse_b",
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    # All models scoped under the single --datasource, NOT the per-cube one.
    assert models["orders"].data_source == DS
    assert meta(models["orders"])["cube_unmapped"]["data_source"] == "warehouse_b"
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


# ── 4.2 measures ───────────────────────────────────────────────────────────

def test_count_measure_no_sql_becomes_star_count():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="count", type="count")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = convert(project)
    assert measure(models["orders"], "count").formula == "count(*)"


def test_sum_measure_splits_column_and_modelmeasure_with_currency_format():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(
            name="total_revenue", type="sum", sql="{CUBE}.amount",
            title="Total Revenue", format="currency")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = convert(project)
    orders = models["orders"]
    m = measure(orders, "total_revenue")
    assert m.formula.startswith("sum(")
    assert m.label == "Total Revenue"
    col = _measure_column(orders, "total_revenue")
    assert col.type == DataType.DOUBLE
    assert col.format is not None
    assert col.format.type == NumberFormatType.CURRENCY


def test_filtered_measures_same_sql_get_distinct_columns():
    """Codex #4: two measures over the same `sql` but different `filters` must
    not collapse — otherwise the filter bleeds across both."""
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[
            CubeMeasure(name="total_revenue", type="sum", sql="{CUBE}.amount"),
            CubeMeasure(name="completed_revenue", type="sum", sql="{CUBE}.amount",
                        filters=[CubeMeasureFilter(sql="{CUBE}.status = 'completed'")]),
        ],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = convert(project)
    orders = models["orders"]
    unfiltered = _measure_column(orders, "total_revenue")
    filtered = _measure_column(orders, "completed_revenue")
    assert unfiltered.name != filtered.name
    assert unfiltered.filter is None
    assert filtered.filter == "status = 'completed'"


def test_count_distinct_approx_maps_to_count_distinct_with_lossy_report():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="uniq_users", type="count_distinct_approx",
                              sql="{CUBE}.user_id")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    assert measure(models["orders"], "uniq_users").formula.startswith("count_distinct(")
    assert any(i.category == CubeIssueCategory.LOSSY_MAPPING for i in report.issues)


def test_calculated_number_measure_becomes_dsl_formula():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[
            CubeMeasure(name="total_revenue", type="sum", sql="{CUBE}.amount"),
            CubeMeasure(name="count", type="count"),
            CubeMeasure(name="aov", type="number", sql="{total_revenue} / {count}"),
        ],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = convert(project)
    assert measure(models["orders"], "aov").formula == "total_revenue / count"


def test_calculated_measure_with_case_when_is_reported_not_emitted():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[
            CubeMeasure(name="count", type="count"),
            CubeMeasure(name="tier", type="string",
                        sql="CASE WHEN {count} > 100 THEN 'high' ELSE 'low' END"),
        ],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    assert models["orders"].get_measure("tier") is None
    assert any(i.category == CubeIssueCategory.COMPLEX_MEASURE for i in report.issues)


def test_finite_rolling_window_becomes_windowed_aggregation():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="revenue_30d", type="sum", sql="{CUBE}.amount",
                              rolling_window={"trailing": "30 day"})],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = convert(project)
    assert measure(models["orders"], "revenue_30d").formula == "sum(amount, window='30d')"


def test_unbounded_rolling_window_falls_back_and_reports():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="revenue_total", type="sum", sql="{CUBE}.amount",
                              rolling_window={"trailing": "unbounded"})],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    assert measure(models["orders"], "revenue_total").formula == "sum(amount)"
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_ROLLING_WINDOW
               for i in report.issues)


# ── 4.3 dimensions ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("cube_type,expected", [
    ("string", DataType.TEXT),
    ("number", DataType.DOUBLE),
    ("boolean", DataType.BOOLEAN),
    ("time", DataType.TIMESTAMP),
])
def test_dimension_type_mapping(cube_type, expected):
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="d", sql="{CUBE}.d", type=cube_type)],
    )])
    models, _ = convert(project)
    assert column(models["orders"], "d").type == expected


def test_dimension_sql_omitted_when_just_cube_dot_name():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="status", sql="{CUBE}.status", type="string")],
    )])
    models, _ = convert(project)
    assert column(models["orders"], "status").sql is None


def test_case_dimension_becomes_case_when_column():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="size_bucket", type="string", case={
            "when": [{"sql": "{CUBE}.size < 10", "label": "small"}],
            "else": {"label": "big"},
        })],
    )])
    models, _ = convert(project)
    sql = column(models["orders"], "size_bucket").sql
    assert sql is not None
    assert "CASE WHEN" in sql
    assert "'small'" in sql
    assert "'big'" in sql
    assert "{CUBE}" not in sql


def test_case_dimension_label_escapes_quotes():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="owner", type="string", case={
            "when": [{"sql": "{CUBE}.x = 1", "label": "Bob's"}],
            "else": {"label": "n/a"},
        })],
    )])
    models, _ = convert(project)
    sql = column(models["orders"], "owner").sql
    assert sql is not None
    assert "'Bob''s'" in sql  # apostrophe doubled, not a broken literal


def test_geo_dimension_reported_not_emitted():
    project = CubeProject(cubes=[CubeCube(
        name="stores", sql_table="public.stores",
        dimensions=[CubeDimension(name="location", type="geo",
                                  latitude={"sql": "{CUBE}.lat"},
                                  longitude={"sql": "{CUBE}.lng"})],
    )])
    models, report = convert(project)
    assert models["stores"].get_column("location") is None
    assert meta(models["stores"])["cube_unmapped"]["geo"]
    assert any(i.category == CubeIssueCategory.GEO_UNMAPPED for i in report.issues)


def test_subquery_dimension_reported_not_emitted():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="cust_ltv", type="number", sub_query=True,
                                  sql="{customers.lifetime_value}")],
    )])
    models, report = convert(project)
    assert models["orders"].get_column("cust_ltv") is None
    assert any(i.category == CubeIssueCategory.SUBQUERY_UNMAPPED for i in report.issues)


def test_custom_granularities_emit_base_column_and_report():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="created_at", sql="{CUBE}.created_at",
                                  type="time",
                                  granularities=[{"name": "fiscal_year",
                                                  "interval": "1 year",
                                                  "offset": "3 months"}])],
    )])
    models, report = convert(project)
    assert column(models["orders"], "created_at").type == DataType.TIMESTAMP
    assert any(i.category == CubeIssueCategory.GRANULARITY_UNMAPPED for i in report.issues)


# ── 4.4 joins ──────────────────────────────────────────────────────────────

def test_join_becomes_left_modeljoin_with_pairs():
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.customer_id = {customers.id}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                           primary_key=True)]),
    ])
    models, _ = convert(project)
    joins = models["orders"].joins
    assert len(joins) == 1
    assert joins[0].target_model == "customers"
    assert joins[0].join_pairs == [["customer_id", "id"]]
    assert joins[0].join_type == JoinType.LEFT


def test_join_to_missing_target_cube_reported():
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="ghost", relationship="many_to_one",
                                 sql="{CUBE}.ghost_id = {ghost.id}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
    ])
    models, report = convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)


def test_non_equi_join_reported_and_dropped():
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="windows", relationship="many_to_one",
                                 sql="{CUBE}.ts > {windows.start}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="windows", sql_table="public.windows",
                 dimensions=[CubeDimension(name="start", sql="{CUBE}.start", type="time")]),
    ])
    models, report = convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)


# ── 4.5 segments ───────────────────────────────────────────────────────────

def test_segment_becomes_boolean_column():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        segments=[CubeSegment(name="completed", sql="{CUBE}.status = 'completed'")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    col = column(models["orders"], "completed")
    assert col.type == DataType.BOOLEAN
    assert col.sql == "status = 'completed'"
    assert any(i.category == CubeIssueCategory.SEGMENT_AS_COLUMN for i in report.issues)


# ── 7. unmapped infra ──────────────────────────────────────────────────────

def test_pre_aggregations_reported_and_stashed_in_meta():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        pre_aggregations=[{"name": "main", "measures": ["CUBE.count"]}],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    assert meta(models["orders"])["cube_unmapped"]["pre_aggregations"]
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


def test_cube_with_no_source_is_dropped_and_reported():
    project = CubeProject(cubes=[CubeCube(name="bad")])
    models, report = convert(project)
    assert "bad" not in models
    assert any(i.category == CubeIssueCategory.NO_SOURCE for i in report.issues)


# ── 4.2 measures — more aggregation kinds ──────────────────────────────────

def _orders_with(measures):
    return CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", measures=measures,
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])


def test_count_with_sql_counts_column():
    models, _ = convert(_orders_with(
        [CubeMeasure(name="paid_count", type="count", sql="{CUBE}.paid_id")]))
    assert measure(models["orders"], "paid_count").formula.startswith("count(")
    assert measure(models["orders"], "paid_count").formula != "count(*)"


def test_count_distinct_exact():
    models, report = convert(_orders_with(
        [CubeMeasure(name="uniq", type="count_distinct", sql="{CUBE}.user_id")]))
    assert measure(models["orders"], "uniq").formula.startswith("count_distinct(")
    assert not any(i.category == CubeIssueCategory.LOSSY_MAPPING for i in report.issues)


@pytest.mark.parametrize("agg", ["avg", "min", "max"])
def test_simple_aggregation_passthrough(agg):
    models, _ = convert(_orders_with(
        [CubeMeasure(name=f"m_{agg}", type=agg, sql="{CUBE}.amount")]))
    assert measure(models["orders"], f"m_{agg}").formula.startswith(f"{agg}(")


@pytest.mark.parametrize("cube_type,expected", [
    ("string", DataType.TEXT),
    ("time", DataType.TIMESTAMP),
    ("boolean", DataType.BOOLEAN),
])
def test_calculated_measure_result_type_is_set(cube_type, expected):
    models, _ = convert(_orders_with([
        CubeMeasure(name="count", type="count"),
        CubeMeasure(name="derived", type=cube_type, sql="{count} + 1"),
    ]))
    m = measure(models["orders"], "derived")
    assert m.formula == "count + 1"
    assert m.type == expected


@pytest.mark.parametrize("rolling", [
    {"trailing": "30 day", "offset": "start"},
    {"leading": "1 month"},
])
def test_rolling_window_leading_or_offset_unsupported(rolling):
    models, report = convert(_orders_with(
        [CubeMeasure(name="r", type="sum", sql="{CUBE}.amount", rolling_window=rolling)]))
    assert measure(models["orders"], "r").formula == "sum(amount)"
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_ROLLING_WINDOW
               for i in report.issues)


def test_window_is_part_of_dedup_key():
    """Codex #4 window half: same sql + same (no) filter but different
    rolling_window → distinct columns, not a collapsed one."""
    models, _ = convert(_orders_with([
        CubeMeasure(name="rev", type="sum", sql="{CUBE}.amount"),
        CubeMeasure(name="rev_30d", type="sum", sql="{CUBE}.amount",
                    rolling_window={"trailing": "30 day"}),
    ]))
    orders = models["orders"]
    assert measure(orders, "rev").formula == "sum(amount)"
    assert measure(orders, "rev_30d").formula == "sum(amount, window='30d')"


# ── 4.4 joins — physical-column resolution (Codex #2) ──────────────────────

def test_join_member_resolves_to_physical_column():
    """`{customers.id}` keys the join by member name `id`; the physical `cust_pk`
    rides on the column's `sql`."""
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.customer_id = {customers.id}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.cust_pk",
                                           type="number", primary_key=True)]),
    ])
    models, _ = convert(project)
    assert models["orders"].joins[0].join_pairs == [["customer_id", "id"]]
    pk = column(models["customers"], "id")
    assert pk is not None
    assert pk.sql == "cust_pk"


def test_join_with_nontrivial_member_sql_is_unsupported():
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.email = {customers.email}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="email", sql="LOWER({CUBE}.email)",
                                           type="string")]),
    ])
    models, report = convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)


# ── 8. format mapping (Codex #8) ───────────────────────────────────────────

def test_percent_format_maps_to_percent():
    models, _ = convert(_orders_with(
        [CubeMeasure(name="rate", type="avg", sql="{CUBE}.rate", format="percent")]))
    col = _measure_column(models["orders"], "rate")
    assert col.format is not None
    assert col.format.type == NumberFormatType.PERCENT


@pytest.mark.parametrize("fmt", ["accounting", "abbr", "0.00%", "imageUrl"])
def test_unsupported_format_reported_and_dropped(fmt):
    models, report = convert(_orders_with(
        [CubeMeasure(name="m", type="sum", sql="{CUBE}.amount", format=fmt)]))
    # measure still emitted; format dropped (defaults to FLOAT).
    assert models["orders"].get_measure("m") is not None
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_FORMAT for i in report.issues)


def test_non_currency_format_never_carries_symbol():
    """Codex #8: a percent format with a stray symbol field must not pass
    `symbol` to NumberFormat (which would raise)."""
    models, _ = convert(_orders_with([CubeMeasure(
        name="rate", type="avg", sql="{CUBE}.rate",
        format={"type": "percent", "currency_symbol": "$"})]))
    col = _measure_column(models["orders"], "rate")
    assert col.format is not None
    assert col.format.type == NumberFormatType.PERCENT
    assert col.format.symbol is None


# ── 7. unmapped-infra meta stash (matrix) ──────────────────────────────────

@pytest.mark.parametrize("field,value", [
    ("refresh_key", {"every": "1 hour"}),
    ("calendar", True),
    ("hierarchies", [{"name": "geo", "levels": ["country"]}]),
    ("access_policy", [{"role": "admin"}]),
    ("sql_alias", "ord"),
])
def test_unmapped_cube_infra_stashed_and_reported(field, value):
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", **{field: value},
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    assert meta(models["orders"])["cube_unmapped"][field] is not None
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


def test_drill_members_reported():
    _, report = convert(_orders_with(
        [CubeMeasure(name="count", type="count", drill_members=["id", "status"])]))
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


# ── 9. Stage-2 / Tesseract deferral ────────────────────────────────────────

def test_switch_dimension_deferred():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="selector", type="switch"),
                    CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = convert(project)
    assert models["orders"].get_column("selector") is None
    assert any(i.category == CubeIssueCategory.DEFERRED_STAGE2 for i in report.issues)


def test_number_agg_measure_deferred():
    models, report = convert(_orders_with(
        [CubeMeasure(name="na", type="number_agg", sql="{CUBE}.amount")]))
    assert models["orders"].get_measure("na") is None
    assert any(i.category == CubeIssueCategory.DEFERRED_STAGE2 for i in report.issues)


def test_join_operand_naming_no_member_synthesises_hidden_column():
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.customer_id = {customers.legacy_id}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                           primary_key=True)]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                           primary_key=True)]),
    ])
    models, _ = convert(project)
    assert models["orders"].joins[0].join_pairs == [["customer_id", "legacy_id"]]
    for model, key in ((models["orders"], "customer_id"), (models["customers"], "legacy_id")):
        col = column(model, key)
        assert col is not None
        assert col.hidden is True
        assert col.is_base
