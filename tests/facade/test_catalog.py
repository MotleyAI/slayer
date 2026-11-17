"""Tests for slayer.facade.catalog — FacadeCatalog construction (DEV-1390 §5)."""

from __future__ import annotations

import logging

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.facade.catalog import (
    CATALOG_NAME,
    DEFAULT_BFS_DEPTH,
    FacadeCatalog,
    FacadeTable,
    build_catalog,
)


def _dataset(
    *,
    name: str,
    data_source: str = "ds1",
    columns: list[DatasetField] | None = None,
    metrics: list[DatasetMetric] | None = None,
    aggregations: list[Aggregation] | None = None,
    joins: list[DatasetRelationship] | None = None,
    hidden: bool = False,
    sql: str | None = None,
    description: str | None = None,
) -> SlayerDataset:
    return SlayerDataset(
        name=name,
        data_source=data_source,
        sql_table=None if sql else name,
        sql=sql,
        fields=columns or [],
        metrics=metrics or [],
        aggregations=aggregations or [],
        relationships=joins or [],
        hidden=hidden,
        description=description,
    )


def _find_table(catalog: FacadeCatalog, *, schema: str, table: str) -> FacadeTable:
    schema_obj = next(s for s in catalog.schemas if s.name == schema)
    return next(t for t in schema_obj.tables if t.name == table)


def test_empty_catalog_round_trip() -> None:
    catalog = build_catalog(datasets_by_datasource={})
    assert catalog.catalog_name == CATALOG_NAME
    assert catalog.schemas == []


def test_single_table_basic_metrics_and_dimensions() -> None:
    model = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="revenue", datatype=DataType.DOUBLE),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="ordered_at", datatype=DataType.TIMESTAMP),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
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
    visible = _dataset(name="orders", columns=[DatasetField(name="x", datatype=DataType.INT)])
    hidden = _dataset(name="ghost", hidden=True, columns=[DatasetField(name="x", datatype=DataType.INT)])
    cat = build_catalog(datasets_by_datasource={"ds1": [visible, hidden]})
    table_names = {t.name for t in cat.schemas[0].tables}
    assert table_names == {"orders"}


def test_hidden_column_excluded() -> None:
    model = _dataset(
        name="orders",
        columns=[
            DatasetField(name="public", datatype=DataType.INT),
            DatasetField(name="secret", datatype=DataType.INT, hidden=True),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name for d in table.dimensions}
    metric_names = {m.name for m in table.metrics}
    assert "public" in dim_names
    assert "secret" not in dim_names
    assert "secret_sum" not in metric_names
    assert "public_sum" in metric_names


def test_row_count_collision_renames_to_underscore(caplog) -> None:
    model = _dataset(
        name="orders",
        columns=[
            # User has a literal column named row_count — synthetic must rename.
            DatasetField(name="row_count", datatype=DataType.INT),
        ],
    )
    with caplog.at_level(logging.WARNING):
        cat = build_catalog(datasets_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    assert "_row_count" in metric_names
    # The user's column-derived metrics still exist normally.
    assert "row_count_sum" in metric_names
    # The synthetic count(*) is now named _row_count.
    synthetic = next(m for m in table.metrics if m.name == "_row_count")
    assert synthetic.metric_expression == "count(*)"
    assert any("renaming the synthetic" in r.message for r in caplog.records)


def test_row_count_collision_warning_has_no_colon_spelling(caplog) -> None:
    model = _dataset(name="orders", columns=[DatasetField(name="row_count", datatype=DataType.INT)])
    with caplog.at_level(logging.WARNING):
        build_catalog(datasets_by_datasource={"ds1": [model]})
    messages = [r.getMessage() for r in caplog.records if "renaming the synthetic" in r.getMessage()]
    assert messages
    assert not any(":count" in msg for msg in messages)


def test_saved_model_measure_emitted_with_declared_type() -> None:
    measure = DatasetMetric(name="aov", expression="sum(revenue) / count(*)", datatype=DataType.DOUBLE,
                            label="AOV", description="Avg order value")
    model = _dataset(
        name="orders",
        columns=[DatasetField(name="revenue", datatype=DataType.DOUBLE)],
        metrics=[measure],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    aov = next(m for m in table.metrics if m.name == "aov")
    assert aov.metric_expression == "aov"
    assert aov.data_type == DataType.DOUBLE
    assert aov.label == "AOV"
    assert aov.description == "Avg order value"


def test_saved_model_measure_without_type_carries_none() -> None:
    measure = DatasetMetric(name="aov", expression="sum(revenue) / count(*)")
    model = _dataset(
        name="orders",
        columns=[DatasetField(name="revenue", datatype=DataType.DOUBLE)],
        metrics=[measure],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    aov = next(m for m in table.metrics if m.name == "aov")
    assert aov.data_type is None  # Wire schema from LIMIT 0 will fill in.


def test_custom_aggregation_with_params_skipped() -> None:
    # Custom agg with no params → eligible per rule 4.
    cheap_agg = Aggregation(name="my_count", expression="COUNT(DISTINCT {value})")
    # Custom agg with params → skipped per rule 4.
    parametric = Aggregation(
        name="my_weighted",
        expression="SUM({value} * {weight}) / SUM({weight})",
        params=[AggregationParam(name="weight", sql="weight_col")],
    )
    model = _dataset(
        name="orders",
        columns=[DatasetField(name="revenue", datatype=DataType.DOUBLE)],
        aggregations=[cheap_agg, parametric],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    assert "revenue_my_count" in metric_names
    assert "revenue_my_weighted" not in metric_names


def test_explicit_allowed_aggregations_intersection() -> None:
    model = _dataset(
        name="orders",
        columns=[
            DatasetField(
                name="revenue",
                datatype=DataType.DOUBLE,
                allowed_aggregations=["sum", "avg"],  # narrow whitelist
            ),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics if m.name.startswith("revenue_")}
    assert metric_names == {"revenue_sum", "revenue_avg"}


def test_single_hop_join_expansion() -> None:
    orders = _dataset(
        name="orders",
        columns=[DatasetField(name="customer_id", datatype=DataType.INT)],
        joins=[DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _dataset(
        name="customers",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region", datatype=DataType.TEXT),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, customers]})
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name: d.dimension_ref for d in table.dimensions}
    # Local dim plus single-hop joined dims (region + id).
    assert dim_names["customers.region"] == "customers.region"
    assert dim_names["customers.id"] == "customers.id"
    metric_names = {m.name: m.metric_expression for m in table.metrics}
    # Joined model row_count.
    assert metric_names["customers.row_count"] == "count(customers.*)"
    # Joined column-agg pairing — region is TEXT, count is eligible.
    assert metric_names["customers.region_count"] == "count(customers.region)"


def test_diamond_join_produces_two_distinct_paths() -> None:
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="warehouse_id", datatype=DataType.INT),
        ],
        joins=[
            DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]]),
            DatasetRelationship(to="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
    )
    customers = _dataset(
        name="customers",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INT),
        ],
        joins=[DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]])],
    )
    warehouses = _dataset(
        name="warehouses",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INT),
        ],
        joins=[DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]])],
    )
    regions = _dataset(
        name="regions",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )
    cat = build_catalog(
        datasets_by_datasource={"ds1": [orders, customers, warehouses, regions]},
    )
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name for d in table.dimensions}
    # Both diamond paths produce distinct dimension entries.
    assert "customers.regions.name" in dim_names
    assert "warehouses.regions.name" in dim_names


def test_reverse_hop_dimensions_enumerated() -> None:
    # orders declares the only edge; the customers table still exposes the
    # reverse-path orders dims (DEV-1853).
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="status", datatype=DataType.TEXT),
        ],
        joins=[DatasetRelationship(
            to="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _dataset(
        name="customers",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, customers]})
    table = _find_table(cat, schema="ds1", table="customers")
    dim_names = {d.name for d in table.dimensions}
    assert "orders.status" in dim_names


def test_row_preserving_flags_by_direction_and_cardinality() -> None:
    # Forward m2o/1:1/unknown hops keep the root grain; a declared 1:N, the
    # inverted side of a m2o, an inverted unknown, and any path through a
    # fan-out hop do not (DEV-1853 browse-mode star scope).
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
        ],
        joins=[
            DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]],
                      cardinality=RelationshipCardinality.MANY_TO_ONE),
            DatasetRelationship(to="items", join_pairs=[["id", "order_id"]],
                      cardinality=RelationshipCardinality.ONE_TO_MANY),
            DatasetRelationship(to="receipts", join_pairs=[["id", "order_id"]],
                      cardinality=RelationshipCardinality.ONE_TO_ONE),
            DatasetRelationship(to="notes", join_pairs=[["id", "order_id"]]),
        ],
    )
    others = [
        _dataset(name=n, columns=[
            DatasetField(name="order_id", datatype=DataType.INT),
            DatasetField(name="tag", datatype=DataType.TEXT),
        ])
        for n in ("customers", "items", "receipts", "notes")
    ]
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, *others]})

    flags = {
        d.name: d.row_preserving
        for d in _find_table(cat, schema="ds1", table="orders").dimensions
    }
    assert flags["customers.tag"] is True    # forward m2o
    assert flags["items.tag"] is False       # declared 1:N
    assert flags["receipts.tag"] is True     # forward 1:1
    assert flags["notes.tag"] is True        # forward unknown, grandfathered

    cust_flags = {
        d.name: d.row_preserving
        for d in _find_table(cat, schema="ds1", table="customers").dimensions
    }
    assert cust_flags["orders.customer_id"] is False        # inverted m2o = 1:N
    assert cust_flags["orders.receipts.tag"] is False       # poisoned by 1st hop

    rec_flags = {
        d.name: d.row_preserving
        for d in _find_table(cat, schema="ds1", table="receipts").dimensions
    }
    assert rec_flags["orders.customer_id"] is True          # inverted 1:1

    note_flags = {
        d.name: d.row_preserving
        for d in _find_table(cat, schema="ds1", table="notes").dimensions
    }
    assert note_flags["orders.customer_id"] is False        # inverted unknown


def test_bfs_depth_limit_truncates() -> None:
    a = _dataset(name="a", columns=[DatasetField(name="id", datatype=DataType.INT, primary_key=True)],
                joins=[DatasetRelationship(to="b", join_pairs=[["id", "id"]])])
    b = _dataset(name="b", columns=[DatasetField(name="id", datatype=DataType.INT, primary_key=True)],
                joins=[DatasetRelationship(to="c", join_pairs=[["id", "id"]])])
    c = _dataset(name="c", columns=[DatasetField(name="id", datatype=DataType.INT, primary_key=True)],
                joins=[DatasetRelationship(to="d", join_pairs=[["id", "id"]])])
    d = _dataset(name="d", columns=[DatasetField(name="leaf", datatype=DataType.TEXT)])
    cat = build_catalog(
        datasets_by_datasource={"ds1": [a, b, c, d]},
        bfs_depth=2,
    )
    table_a = _find_table(cat, schema="ds1", table="a")
    dim_names = {dim.name for dim in table_a.dimensions}
    # Depth 2 means we can reach b (1 hop) and c (2 hops) but not d (3 hops).
    assert any(name.startswith("b.") for name in dim_names)
    assert any(name.startswith("b.c.") for name in dim_names)
    assert not any(name.startswith("b.c.d.") for name in dim_names)


def test_table_type_view_for_sql_backed_model() -> None:
    view_model = SlayerDataset(
        name="custom_view",
        data_source="ds1",
        sql="SELECT 1 AS id, 'a' AS label",
        fields=[
            DatasetField(name="id", datatype=DataType.INT),
            DatasetField(name="label", datatype=DataType.TEXT),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [view_model]})
    tbl = _find_table(cat, schema="ds1", table="custom_view")
    assert tbl.table_type == "VIEW"


def test_default_bfs_depth_constant_is_three() -> None:
    assert DEFAULT_BFS_DEPTH == 3


def test_multiple_datasources_keep_disjoint_schemas() -> None:
    m1 = _dataset(name="t", data_source="dsA", columns=[DatasetField(name="x", datatype=DataType.INT)])
    m2 = _dataset(name="t", data_source="dsB", columns=[DatasetField(name="x", datatype=DataType.INT)])
    cat = build_catalog(datasets_by_datasource={"dsA": [m1], "dsB": [m2]})
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
    no_time = _dataset(
        name="products",
        columns=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="price", datatype=DataType.DOUBLE),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [no_time]})
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
    with_default = SlayerDataset(
        name="orders", data_source="ds1", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="ordered_at", datatype=DataType.TIMESTAMP),
        ],
        default_time_dimension="ordered_at",
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [with_default]})
    table = _find_table(cat, schema="ds1", table="orders")
    metric_names = {m.name for m in table.metrics}
    assert "name_first" in metric_names
    assert "name_last" in metric_names


def test_metric_data_type_for_aggregations_uses_coarse_inference() -> None:
    model = _dataset(
        name="orders",
        columns=[
            DatasetField(name="revenue", datatype=DataType.DOUBLE),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="ordered_at", datatype=DataType.TIMESTAMP),
            DatasetField(name="flag", datatype=DataType.BOOLEAN),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [model]})
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
