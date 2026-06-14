"""Tests for slayer.facade.catalog — FacadeCatalog construction (DEV-1390 §5)."""

from __future__ import annotations

import logging
from typing import List

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.facade.catalog import (
    CATALOG_NAME,
    DEFAULT_BFS_DEPTH,
    FacadeCatalog,
    FacadeTable,
    build_catalog,
)


def _model(
    *,
    name: str,
    data_source: str = "ds1",
    columns: List[Column] | None = None,
    measures: List[ModelMeasure] | None = None,
    aggregations: List[Aggregation] | None = None,
    joins: List[ModelJoin] | None = None,
    hidden: bool = False,
    sql: str | None = None,
    description: str | None = None,
) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source=data_source,
        sql_table=None if sql else name,
        sql=sql,
        columns=columns or [],
        measures=measures or [],
        aggregations=aggregations or [],
        joins=joins or [],
        hidden=hidden,
        description=description,
    )


def _find_table(catalog: FacadeCatalog, *, schema: str, table: str) -> FacadeTable:
    schema_obj = next(s for s in catalog.schemas if s.name == schema)
    return next(t for t in schema_obj.tables if t.name == table)


def test_empty_catalog_round_trip() -> None:
    catalog = build_catalog(models_by_datasource={})
    assert catalog.catalog_name == CATALOG_NAME
    assert catalog.schemas == []


def test_single_table_basic_metrics_and_dimensions() -> None:
    model = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    # row_count synthetic.
    assert "row_count" in metric_names
    # PK clamp on id → only count/count_distinct.
    assert "id_count" in metric_names
    assert "id_count_distinct" in metric_names
    assert "id_sum" not in metric_names
    # Numeric DOUBLE column → full numeric agg suite minus parametrics.
    assert "revenue_sum" in metric_names
    assert "revenue_avg" in metric_names
    assert "revenue_max" in metric_names
    # Parametric built-ins skipped.
    assert "revenue_weighted_avg" not in metric_names
    assert "revenue_percentile" not in metric_names
    assert "revenue_corr" not in metric_names
    # TEXT column → count, min, max, first, last only.
    assert "status_count" in metric_names
    assert "status_min" in metric_names
    assert "status_sum" not in metric_names

    dim_names = {d.name for d in table.dimensions}
    assert dim_names == {"id", "revenue", "status", "ordered_at"}
    # is_time flag.
    by_name = {d.name: d for d in table.dimensions}
    assert by_name["ordered_at"].is_time is True
    assert by_name["status"].is_time is False
    # PK column is exposed as a dimension despite the metric-agg clamp.
    assert by_name["id"].dimension_ref == "id"


def test_hidden_model_excluded() -> None:
    visible = _model(name="orders", columns=[Column(name="x", type=DataType.INT)])
    hidden = _model(name="ghost", hidden=True, columns=[Column(name="x", type=DataType.INT)])
    cat = build_catalog(models_by_datasource={"ds1": [visible, hidden]})
    table_names = {t.name for t in cat.schemas[0].tables}
    assert table_names == {"orders"}


def test_hidden_column_excluded() -> None:
    model = _model(
        name="orders",
        columns=[
            Column(name="public", type=DataType.INT),
            Column(name="secret", type=DataType.INT, hidden=True),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name for d in table.dimensions}
    metric_names = {m.name for m in table.metrics}
    assert "public" in dim_names
    assert "secret" not in dim_names
    assert "secret_sum" not in metric_names
    assert "public_sum" in metric_names


def test_row_count_collision_renames_to_underscore(caplog) -> None:
    model = _model(
        name="orders",
        columns=[
            # User has a literal column named row_count — synthetic must rename.
            Column(name="row_count", type=DataType.INT),
        ],
    )
    with caplog.at_level(logging.WARNING):
        cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    assert "_row_count" in metric_names
    # The user's column-derived metrics still exist normally.
    assert "row_count_sum" in metric_names
    # The synthetic *:count is now named _row_count.
    synthetic = next(m for m in table.metrics if m.name == "_row_count")
    assert synthetic.measure_formula == "*:count"
    assert any("renaming the synthetic" in r.message for r in caplog.records)


def test_saved_model_measure_emitted_with_declared_type() -> None:
    measure = ModelMeasure(name="aov", formula="revenue:sum / *:count", type=DataType.DOUBLE,
                            label="AOV", description="Avg order value")
    model = _model(
        name="orders",
        columns=[Column(name="revenue", type=DataType.DOUBLE)],
        measures=[measure],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    aov = next(m for m in table.metrics if m.name == "aov")
    assert aov.measure_formula == "aov"
    assert aov.data_type == DataType.DOUBLE
    assert aov.label == "AOV"
    assert aov.description == "Avg order value"


def test_saved_model_measure_without_type_carries_none() -> None:
    measure = ModelMeasure(name="aov", formula="revenue:sum / *:count")
    model = _model(
        name="orders",
        columns=[Column(name="revenue", type=DataType.DOUBLE)],
        measures=[measure],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    aov = next(m for m in table.metrics if m.name == "aov")
    assert aov.data_type is None  # Wire schema from LIMIT 0 will fill in.


def test_custom_aggregation_with_params_skipped() -> None:
    # Custom agg with no params → eligible per rule 4.
    cheap_agg = Aggregation(name="my_count", formula="COUNT(DISTINCT {value})")
    # Custom agg with params → skipped per rule 4.
    parametric = Aggregation(
        name="my_weighted",
        formula="SUM({value} * {weight}) / SUM({weight})",
        params=[AggregationParam(name="weight", sql="weight_col")],
    )
    model = _model(
        name="orders",
        columns=[Column(name="revenue", type=DataType.DOUBLE)],
        aggregations=[cheap_agg, parametric],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    assert "revenue_my_count" in metric_names
    assert "revenue_my_weighted" not in metric_names


def test_explicit_allowed_aggregations_intersection() -> None:
    model = _model(
        name="orders",
        columns=[
            Column(
                name="revenue",
                type=DataType.DOUBLE,
                allowed_aggregations=["sum", "avg"],  # narrow whitelist
            ),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics if m.name.startswith("revenue_")}
    assert metric_names == {"revenue_sum", "revenue_avg"}


def test_single_hop_join_expansion() -> None:
    orders = _model(
        name="orders",
        columns=[Column(name="customer_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _model(
        name="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [orders, customers]})
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name: d.dimension_ref for d in table.dimensions}
    # Local dim plus single-hop joined dims (region + id).
    assert dim_names["customers.region"] == "customers.region"
    assert dim_names["customers.id"] == "customers.id"
    metric_names = {m.name: m.measure_formula for m in table.metrics}
    # Joined model row_count.
    assert metric_names["customers.row_count"] == "customers.*:count"
    # Joined column-agg pairing — region is TEXT, count is eligible.
    assert metric_names["customers.region_count"] == "customers.region:count"


def test_diamond_join_produces_two_distinct_paths() -> None:
    orders = _model(
        name="orders",
        columns=[
            Column(name="customer_id", type=DataType.INT),
            Column(name="warehouse_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
    )
    customers = _model(
        name="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    warehouses = _model(
        name="warehouses",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    regions = _model(
        name="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    cat = build_catalog(
        models_by_datasource={"ds1": [orders, customers, warehouses, regions]},
    )
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name for d in table.dimensions}
    # Both diamond paths produce distinct dimension entries.
    assert "customers.regions.name" in dim_names
    assert "warehouses.regions.name" in dim_names


def test_bfs_depth_limit_truncates() -> None:
    a = _model(name="a", columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                joins=[ModelJoin(target_model="b", join_pairs=[["id", "id"]])])
    b = _model(name="b", columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                joins=[ModelJoin(target_model="c", join_pairs=[["id", "id"]])])
    c = _model(name="c", columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                joins=[ModelJoin(target_model="d", join_pairs=[["id", "id"]])])
    d = _model(name="d", columns=[Column(name="leaf", type=DataType.TEXT)])
    cat = build_catalog(
        models_by_datasource={"ds1": [a, b, c, d]},
        bfs_depth=2,
    )
    table_a = _find_table(cat, schema="ds1", table="a")
    dim_names = {dim.name for dim in table_a.dimensions}
    # Depth 2 means we can reach b (1 hop) and c (2 hops) but not d (3 hops).
    assert any(name.startswith("b.") for name in dim_names)
    assert any(name.startswith("b.c.") for name in dim_names)
    assert not any(name.startswith("b.c.d.") for name in dim_names)


def test_table_type_view_for_sql_backed_model() -> None:
    view_model = SlayerModel(
        name="custom_view",
        data_source="ds1",
        sql="SELECT 1 AS id, 'a' AS label",
        columns=[
            Column(name="id", type=DataType.INT),
            Column(name="label", type=DataType.TEXT),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [view_model]})
    tbl = _find_table(cat, schema="ds1", table="custom_view")
    assert tbl.table_type == "VIEW"


def test_default_bfs_depth_constant_is_three() -> None:
    assert DEFAULT_BFS_DEPTH == 3


def test_multiple_datasources_keep_disjoint_schemas() -> None:
    m1 = _model(name="t", data_source="dsA", columns=[Column(name="x", type=DataType.INT)])
    m2 = _model(name="t", data_source="dsB", columns=[Column(name="x", type=DataType.INT)])
    cat = build_catalog(models_by_datasource={"dsA": [m1], "dsB": [m2]})
    schemas = {s.name: s for s in cat.schemas}
    assert set(schemas) == {"dsA", "dsB"}
    # Same model name, different schemas — both surface.
    assert {t.name for t in schemas["dsA"].tables} == {"t"}
    assert {t.name for t in schemas["dsB"].tables} == {"t"}


def test_no_time_dimension_drops_first_last_aggregations() -> None:
    """DEV-1558 live-Metabase repro: a model with no time dimension
    must NOT expose ``<col>_first`` / ``<col>_last`` aggregations
    because the engine can't resolve them — Metabase's flat fingerprint
    scan ``SELECT name_first FROM products`` would error with
    ``Aggregation 'first' requires a time column``."""
    no_time = _model(
        name="products",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="price", type=DataType.DOUBLE),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [no_time]})
    table = _find_table(cat, schema="ds1", table="products")
    metric_names = {m.name for m in table.metrics}
    assert "name_first" not in metric_names
    assert "name_last" not in metric_names
    assert "price_first" not in metric_names
    assert "price_last" not in metric_names
    # Non-time-dependent aggregations survive — sanity.
    assert "name_count" in metric_names
    assert "name_min" in metric_names
    assert "price_sum" in metric_names


def test_default_time_dimension_keeps_first_last_aggregations() -> None:
    """When ``default_time_dimension`` is set, the engine can resolve
    flat ``<col>:first`` calls, so the facade exposes them."""
    with_default = SlayerModel(
        name="orders", data_source="ds1", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        default_time_dimension="ordered_at",
    )
    cat = build_catalog(models_by_datasource={"ds1": [with_default]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    assert "name_first" in metric_names
    assert "name_last" in metric_names


def test_metric_data_type_for_aggregations_uses_coarse_inference() -> None:
    model = _model(
        name="orders",
        columns=[
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="flag", type=DataType.BOOLEAN),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    by_name = {m.name: m for m in table.metrics}
    # COUNT-family → INT regardless of column type.
    assert by_name["revenue_count"].data_type == DataType.INT
    assert by_name["status_count_distinct"].data_type == DataType.INT
    # SUM of DOUBLE → DOUBLE.
    assert by_name["revenue_sum"].data_type == DataType.DOUBLE
    # SUM of BOOLEAN → INT (boolean SUM is integer in every supported dialect).
    assert by_name["flag_sum"].data_type == DataType.INT
    # AVG of any numeric → DOUBLE.
    assert by_name["revenue_avg"].data_type == DataType.DOUBLE
    # MIN/MAX preserve column type.
    assert by_name["ordered_at_max"].data_type == DataType.TIMESTAMP
    assert by_name["status_min"].data_type == DataType.TEXT
