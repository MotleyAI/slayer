"""Tests for the Cube → SLayer converter (slayer/cube/converter.py).

DEV-1608 §4. Projects are built in Python (parser is tested separately) so these
pin the mapping semantics directly.
"""

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.format import NumberFormatType
from slayer.core.models import SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
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

DS = "test_ds"


def _convert(project: CubeProject) -> tuple[dict[str, SlayerModel], object]:
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}, result.report


def _measure_column(model: SlayerModel, measure_name: str):
    """Return the Column a `<col>:<agg>` measure formula references."""
    m = model.get_measure(measure_name)
    assert m is not None, f"measure {measure_name} missing on {model.name}"
    col_ref = m.formula.split(":")[0].strip()
    return model.get_column(col_ref)


# ── 4.1 cube → model ───────────────────────────────────────────────────────

def test_cube_becomes_table_backed_model():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", description="Customer orders",
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True)],
    )])
    models, _ = _convert(project)
    orders = models["orders"]
    assert orders.sql_table == "public.orders"
    assert orders.data_source == DS
    assert orders.description == "Customer orders"
    assert orders.get_column("id").primary_key is True
    assert orders.get_column("id").type == DataType.DOUBLE


def test_public_false_dimension_is_hidden():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[
            CubeDimension(name="id", sql="{CUBE}.id", type="number"),
            CubeDimension(name="secret", sql="{CUBE}.secret", type="string", public=False),
        ],
    )])
    models, _ = _convert(project)
    assert models["orders"].get_column("secret").hidden is True
    assert models["orders"].get_column("id").hidden is False


def test_public_false_cube_is_hidden_and_title_goes_to_meta():
    project = CubeProject(cubes=[CubeCube(
        name="internal", sql_table="public.internal", public=False, title="Internal",
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = _convert(project)
    assert models["internal"].hidden is True
    assert models["internal"].meta["cube_title"] == "Internal"


def test_per_cube_data_source_reported_and_stashed():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", data_source="warehouse_b",
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = _convert(project)
    # All models scoped under the single --datasource, NOT the per-cube one.
    assert models["orders"].data_source == DS
    assert models["orders"].meta["cube_unmapped"]["data_source"] == "warehouse_b"
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


# ── 4.2 measures ───────────────────────────────────────────────────────────

def test_count_measure_no_sql_becomes_star_count():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="count", type="count")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = _convert(project)
    assert models["orders"].get_measure("count").formula == "*:count"


def test_sum_measure_splits_column_and_modelmeasure_with_currency_format():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(
            name="total_revenue", type="sum", sql="{CUBE}.amount",
            title="Total Revenue", format="currency")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = _convert(project)
    orders = models["orders"]
    m = orders.get_measure("total_revenue")
    assert m.formula.endswith(":sum")
    assert m.label == "Total Revenue"
    col = _measure_column(orders, "total_revenue")
    assert col.type == DataType.DOUBLE
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
    models, _ = _convert(project)
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
    models, report = _convert(project)
    assert models["orders"].get_measure("uniq_users").formula.endswith(":count_distinct")
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
    models, _ = _convert(project)
    assert models["orders"].get_measure("aov").formula == "total_revenue / count"


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
    models, report = _convert(project)
    assert models["orders"].get_measure("tier") is None
    assert any(i.category == CubeIssueCategory.COMPLEX_MEASURE for i in report.issues)


def test_finite_rolling_window_becomes_windowed_aggregation():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="revenue_30d", type="sum", sql="{CUBE}.amount",
                              rolling_window={"trailing": "30 day"})],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, _ = _convert(project)
    assert models["orders"].get_measure("revenue_30d").formula == "amount:sum(window='30d')"


def test_unbounded_rolling_window_falls_back_and_reports():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        measures=[CubeMeasure(name="revenue_total", type="sum", sql="{CUBE}.amount",
                              rolling_window={"trailing": "unbounded"})],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = _convert(project)
    assert models["orders"].get_measure("revenue_total").formula == "amount:sum"
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
    models, _ = _convert(project)
    assert models["orders"].get_column("d").type == expected


def test_dimension_sql_omitted_when_just_cube_dot_name():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="status", sql="{CUBE}.status", type="string")],
    )])
    models, _ = _convert(project)
    assert models["orders"].get_column("status").sql is None


def test_case_dimension_becomes_case_when_column():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="size_bucket", type="string", case={
            "when": [{"sql": "{CUBE}.size < 10", "label": "small"}],
            "else": {"label": "big"},
        })],
    )])
    models, _ = _convert(project)
    sql = models["orders"].get_column("size_bucket").sql
    assert "CASE WHEN" in sql and "'small'" in sql and "'big'" in sql
    assert "{CUBE}" not in sql


def test_geo_dimension_reported_not_emitted():
    project = CubeProject(cubes=[CubeCube(
        name="stores", sql_table="public.stores",
        dimensions=[CubeDimension(name="location", type="geo",
                                  latitude={"sql": "{CUBE}.lat"},
                                  longitude={"sql": "{CUBE}.lng"})],
    )])
    models, report = _convert(project)
    assert models["stores"].get_column("location") is None
    assert models["stores"].meta["cube_unmapped"]["geo"]
    assert any(i.category == CubeIssueCategory.GEO_UNMAPPED for i in report.issues)


def test_subquery_dimension_reported_not_emitted():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="cust_ltv", type="number", sub_query=True,
                                  sql="{customers.lifetime_value}")],
    )])
    models, report = _convert(project)
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
    models, report = _convert(project)
    assert models["orders"].get_column("created_at").type == DataType.TIMESTAMP
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
    models, _ = _convert(project)
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
    models, report = _convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)


def test_case_dimension_label_escapes_quotes():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="owner", type="string", case={
            "when": [{"sql": "{CUBE}.x = 1", "label": "Bob's"}],
            "else": {"label": "n/a"},
        })],
    )])
    models, _ = _convert(project)
    sql = models["orders"].get_column("owner").sql
    assert "'Bob''s'" in sql  # apostrophe doubled, not a broken literal


def test_non_equi_join_reported_and_dropped():
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="windows", relationship="many_to_one",
                                 sql="{CUBE}.ts > {windows.start}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="windows", sql_table="public.windows",
                 dimensions=[CubeDimension(name="start", sql="{CUBE}.start", type="time")]),
    ])
    models, report = _convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)


# ── 4.5 segments ───────────────────────────────────────────────────────────

def test_segment_becomes_boolean_column():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        segments=[CubeSegment(name="completed", sql="{CUBE}.status = 'completed'")],
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = _convert(project)
    col = models["orders"].get_column("completed")
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
    models, report = _convert(project)
    assert models["orders"].meta["cube_unmapped"]["pre_aggregations"]
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


def test_cube_with_no_source_is_dropped_and_reported():
    project = CubeProject(cubes=[CubeCube(name="bad")])
    models, report = _convert(project)
    assert "bad" not in models
    assert any(i.category == CubeIssueCategory.NO_SOURCE for i in report.issues)


# ── 4.2 measures — more aggregation kinds ──────────────────────────────────

def _orders_with(measures):
    return CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders", measures=measures,
        dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])


def test_count_with_sql_counts_column():
    models, _ = _convert(_orders_with(
        [CubeMeasure(name="paid_count", type="count", sql="{CUBE}.paid_id")]))
    assert models["orders"].get_measure("paid_count").formula.endswith(":count")
    assert models["orders"].get_measure("paid_count").formula != "*:count"


def test_count_distinct_exact():
    models, report = _convert(_orders_with(
        [CubeMeasure(name="uniq", type="count_distinct", sql="{CUBE}.user_id")]))
    assert models["orders"].get_measure("uniq").formula.endswith(":count_distinct")
    assert not any(i.category == CubeIssueCategory.LOSSY_MAPPING for i in report.issues)


@pytest.mark.parametrize("agg", ["avg", "min", "max"])
def test_simple_aggregation_passthrough(agg):
    models, _ = _convert(_orders_with(
        [CubeMeasure(name=f"m_{agg}", type=agg, sql="{CUBE}.amount")]))
    assert models["orders"].get_measure(f"m_{agg}").formula.endswith(f":{agg}")


@pytest.mark.parametrize("cube_type,expected", [
    ("string", DataType.TEXT),
    ("time", DataType.TIMESTAMP),
    ("boolean", DataType.BOOLEAN),
])
def test_calculated_measure_result_type_is_set(cube_type, expected):
    models, _ = _convert(_orders_with([
        CubeMeasure(name="count", type="count"),
        CubeMeasure(name="derived", type=cube_type, sql="{count} + 1"),
    ]))
    m = models["orders"].get_measure("derived")
    assert m.formula == "count + 1"
    assert m.type == expected


@pytest.mark.parametrize("rolling", [
    {"trailing": "30 day", "offset": "start"},
    {"leading": "1 month"},
])
def test_rolling_window_leading_or_offset_unsupported(rolling):
    models, report = _convert(_orders_with(
        [CubeMeasure(name="r", type="sum", sql="{CUBE}.amount", rolling_window=rolling)]))
    assert models["orders"].get_measure("r").formula == "amount:sum"
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_ROLLING_WINDOW
               for i in report.issues)


def test_window_is_part_of_dedup_key():
    """Codex #4 window half: same sql + same (no) filter but different
    rolling_window → distinct columns, not a collapsed one."""
    models, _ = _convert(_orders_with([
        CubeMeasure(name="rev", type="sum", sql="{CUBE}.amount"),
        CubeMeasure(name="rev_30d", type="sum", sql="{CUBE}.amount",
                    rolling_window={"trailing": "30 day"}),
    ]))
    orders = models["orders"]
    assert orders.get_measure("rev").formula == "amount:sum"
    assert orders.get_measure("rev_30d").formula == "amount:sum(window='30d')"


# ── 4.4 joins — physical-column resolution (Codex #2) ──────────────────────

def test_join_member_resolves_to_physical_column():
    """`{customers.id}` where the `id` member's sql is `{CUBE}.cust_pk` must emit
    the physical column `cust_pk`, not the member name `id`."""
    project = CubeProject(cubes=[
        CubeCube(name="orders", sql_table="public.orders",
                 joins=[CubeJoin(name="customers", relationship="many_to_one",
                                 sql="{CUBE}.customer_id = {customers.id}")],
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        CubeCube(name="customers", sql_table="public.customers",
                 dimensions=[CubeDimension(name="id", sql="{CUBE}.cust_pk",
                                           type="number", primary_key=True)]),
    ])
    models, _ = _convert(project)
    assert models["orders"].joins[0].join_pairs == [["customer_id", "cust_pk"]]


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
    models, report = _convert(project)
    assert models["orders"].joins == []
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_JOIN for i in report.issues)


# ── 8. format mapping (Codex #8) ───────────────────────────────────────────

def test_percent_format_maps_to_percent():
    models, _ = _convert(_orders_with(
        [CubeMeasure(name="rate", type="avg", sql="{CUBE}.rate", format="percent")]))
    col = _measure_column(models["orders"], "rate")
    assert col.format.type == NumberFormatType.PERCENT


@pytest.mark.parametrize("fmt", ["accounting", "abbr", "0.00%", "imageUrl"])
def test_unsupported_format_reported_and_dropped(fmt):
    models, report = _convert(_orders_with(
        [CubeMeasure(name="m", type="sum", sql="{CUBE}.amount", format=fmt)]))
    # measure still emitted; format dropped (defaults to FLOAT).
    assert models["orders"].get_measure("m") is not None
    assert any(i.category == CubeIssueCategory.UNSUPPORTED_FORMAT for i in report.issues)


def test_non_currency_format_never_carries_symbol():
    """Codex #8: a percent format with a stray symbol field must not pass
    `symbol` to NumberFormat (which would raise)."""
    models, _ = _convert(_orders_with([CubeMeasure(
        name="rate", type="avg", sql="{CUBE}.rate",
        format={"type": "percent", "currency_symbol": "$"})]))
    col = _measure_column(models["orders"], "rate")
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
    models, report = _convert(project)
    assert models["orders"].meta["cube_unmapped"][field] is not None
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


def test_drill_members_reported():
    _, report = _convert(_orders_with(
        [CubeMeasure(name="count", type="count", drill_members=["id", "status"])]))
    assert any(i.category == CubeIssueCategory.UNMAPPED_INFRA for i in report.issues)


# ── 9. Stage-2 / Tesseract deferral ────────────────────────────────────────

def test_switch_dimension_deferred():
    project = CubeProject(cubes=[CubeCube(
        name="orders", sql_table="public.orders",
        dimensions=[CubeDimension(name="selector", type="switch"),
                    CubeDimension(name="id", sql="{CUBE}.id", type="number")],
    )])
    models, report = _convert(project)
    assert models["orders"].get_column("selector") is None
    assert any(i.category == CubeIssueCategory.DEFERRED_STAGE2 for i in report.issues)


def test_number_agg_measure_deferred():
    models, report = _convert(_orders_with(
        [CubeMeasure(name="na", type="number_agg", sql="{CUBE}.amount")]))
    assert models["orders"].get_measure("na") is None
    assert any(i.category == CubeIssueCategory.DEFERRED_STAGE2 for i in report.issues)
