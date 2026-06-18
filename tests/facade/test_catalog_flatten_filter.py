"""DEV-1567: cross-model metrics and dimensions must not surface through
the flat-column catalog probes BI tools introspect (pg_attribute,
INFORMATION_SCHEMA.COLUMNS). They must remain visible via the
catalog-shaped views (INFORMATION_SCHEMA.METRICS / .DIMENSIONS) and the
catalog fingerprint hash so cache invalidation still tracks them.

The leak (before this fix):

1. ``FacadeCatalog._metric_expansion`` and ``_dimension_expansion`` pre-
   expand cross-model entries with dotted names (``customers.row_count``,
   ``customers.regions.population_sum``).
2. INFORMATION_SCHEMA.COLUMNS (slayer/facade/info_schema.py:_serve_columns)
   and pg_catalog.pg_attribute (slayer/facade/catalog_sql.py:_column_specs)
   flatten ``tbl.dimensions + tbl.metrics`` into a single rowset.
3. Metabase reads the flat view, caches the dotted "columns", and emits
   ``SELECT "customers.row_count" FROM orders``-style fingerprint queries
   that lead to dotted ``SlayerQuery.measures[*].name`` Pydantic rejection.

Fix: keep ``tbl.metrics`` / ``tbl.dimensions`` raw, but filter cross-model
entries (``"." in name``) at the flat-view rendering step. The translator's
``metrics_by_name`` / ``metrics_by_formula`` / ``dims_by_name`` lookups
keep using the raw views so hand-written cross-model SQL still resolves.
"""

from __future__ import annotations

from typing import List

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.facade.catalog import (
    FacadeCatalog,
    FacadeTable,
    build_catalog,
    local_dimensions,
    local_metrics,
)


def _model(
    *,
    name: str,
    columns: List[Column],
    joins: List[ModelJoin] | None = None,
    measures: List[ModelMeasure] | None = None,
    aggregations: List[Aggregation] | None = None,
) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source="jaffle",
        sql_table=name,
        columns=columns,
        joins=joins or [],
        measures=measures or [],
        aggregations=aggregations or [],
    )


def _find_table(catalog: FacadeCatalog, *, table: str) -> FacadeTable:
    return next(
        t for t in catalog.schemas[0].tables if t.name == table
    )


def _orders_customers_catalog() -> FacadeCatalog:
    """Standard fixture: ``orders → customers`` single-hop join, both with
    a typed column set that produces a non-trivial cross-model fan-out."""
    orders = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="total", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _model(
        name="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="email", type=DataType.TEXT),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, customers]})


# --- local_metrics / local_dimensions helpers --------------------------------


def test_local_metrics_excludes_cross_model_entries() -> None:
    cat = _orders_customers_catalog()
    orders = _find_table(cat, table="orders")
    locals_ = local_metrics(orders)
    names = {m.name for m in locals_}
    # All cross-model expansion entries are excluded.
    assert not any("." in n for n in names)


def test_local_metrics_excludes_synthetic_row_count() -> None:
    """The rule-1 ``row_count`` metric (name='row_count',
    measure_formula='*:count') is catalog fan-out, not user-authored.
    BI tools see ``row_count`` as a queryable "column", emit
    ``SELECT row_count FROM orders`` fingerprint queries, and explode
    the wire response. Strip it from the flat-view path."""
    cat = _orders_customers_catalog()
    orders = _find_table(cat, table="orders")
    locals_ = local_metrics(orders)
    names = {m.name for m in locals_}
    assert "row_count" not in names


def test_local_metrics_excludes_same_model_column_builtin_agg_entries() -> None:
    """The rule-3 ``<col>_<agg>`` cartesian
    (e.g. ``total_sum`` / ``id_count`` / ``ordered_at_max``) is catalog
    fan-out so colon-form aggregate resolution finds the matching
    metric. Not user-authored, not in the flat view."""
    cat = _orders_customers_catalog()
    orders = _find_table(cat, table="orders")
    locals_ = local_metrics(orders)
    names = {m.name for m in locals_}
    assert "total_sum" not in names
    assert "id_count" not in names
    assert "ordered_at_max" not in names


def test_local_metrics_excludes_same_model_custom_aggregation_metrics() -> None:
    """The rule-4 ``<col>_<custom_agg>`` cartesian is also catalog
    fan-out — same rule as builtin column×agg."""
    orders = _model(
        name="orders",
        columns=[Column(name="amount", type=DataType.DOUBLE)],
        aggregations=[
            Aggregation(name="my_count", formula="COUNT(DISTINCT {value})"),
        ],
    )
    cat = build_catalog(models_by_datasource={"jaffle": [orders]})
    table = _find_table(cat, table="orders")
    locals_ = local_metrics(table)
    names = {m.name for m in locals_}
    assert "amount_my_count" not in names


def test_local_metrics_keeps_saved_model_measures() -> None:
    """The rule-2 saved ``ModelMeasure`` (name == measure_formula) IS
    user-authored — the user typed the name. It belongs in the flat
    view so BI tools can project / aggregate against it by name."""
    orders = _model(
        name="orders",
        columns=[Column(name="revenue", type=DataType.DOUBLE)],
        measures=[
            ModelMeasure(
                name="aov",
                formula="revenue:sum / *:count",
                type=DataType.DOUBLE,
            ),
        ],
    )
    cat = build_catalog(models_by_datasource={"jaffle": [orders]})
    table = _find_table(cat, table="orders")
    locals_ = local_metrics(table)
    names = {m.name for m in locals_}
    assert "aov" in names


def test_local_dimensions_excludes_cross_model_entries() -> None:
    cat = _orders_customers_catalog()
    orders = _find_table(cat, table="orders")
    locals_ = local_dimensions(orders)
    names = {d.name for d in locals_}
    assert not any("." in n for n in names)
    # Same-model dimensions survive.
    assert "id" in names
    assert "customer_id" in names
    assert "total" in names
    assert "ordered_at" in names


def test_local_dimensions_excludes_multi_hop_entries() -> None:
    """Multi-hop dimensions like ``customers.regions.name`` also carry a
    dot and must be filtered out."""
    orders = _model(
        name="orders",
        columns=[Column(name="customer_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _model(
        name="customers",
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
        models_by_datasource={"jaffle": [orders, customers, regions]},
    )
    table = _find_table(cat, table="orders")
    raw_names = {d.name for d in table.dimensions}
    local_names = {d.name for d in local_dimensions(table)}
    # Sanity: raw has both single-hop and multi-hop dotted entries.
    assert "customers.id" in raw_names
    assert "customers.regions.name" in raw_names
    # Filtered set has neither.
    assert "customers.id" not in local_names
    assert "customers.regions.name" not in local_names


def test_raw_metrics_still_carry_cross_model_entries() -> None:
    """Sanity: the catalog itself still has cross-model entries on the raw
    view. Only the flatten step filters them."""
    cat = _orders_customers_catalog()
    orders = _find_table(cat, table="orders")
    raw_names = {m.name for m in orders.metrics}
    assert "customers.row_count" in raw_names


# --- INFORMATION_SCHEMA.COLUMNS (canned) -------------------------------------


def test_info_schema_columns_excludes_dotted_entries() -> None:
    from slayer.facade.info_schema import _serve_columns

    cat = _orders_customers_catalog()
    batch = _serve_columns(catalog=cat)
    column_names = {row["column_name"] for row in batch.rows}
    assert not any("." in n for n in column_names)
    # Sanity: orders' base columns survive.
    assert "total" in column_names
    assert "ordered_at" in column_names


def test_info_schema_columns_row_count_matches_local_counts() -> None:
    from slayer.facade.info_schema import _serve_columns

    cat = _orders_customers_catalog()
    batch = _serve_columns(catalog=cat)
    expected = 0
    for sch in cat.schemas:
        for tbl in sch.tables:
            expected += len(local_dimensions(tbl)) + len(local_metrics(tbl))
    assert len(batch.rows) == expected


# --- pg_catalog.pg_attribute --------------------------------------------------


def test_pg_attribute_excludes_dotted_attnames() -> None:
    from slayer.facade.catalog_sql import _build_pg_attribute

    cat = _orders_customers_catalog()
    rel = _build_pg_attribute(cat)
    attnames = {row["attname"] for row in rel.rows}
    assert not any("." in n for n in attnames)


def test_pg_attribute_attnum_within_local_range() -> None:
    """Every attnum for a given table must be within
    ``len(local_dims) + len(local_metrics)``."""
    from slayer.facade.catalog_sql import _build_pg_attribute, _table_oid

    cat = _orders_customers_catalog()
    rel = _build_pg_attribute(cat)
    for sch in cat.schemas:
        for tbl in sch.tables:
            oid = _table_oid(sch.name, tbl)
            max_for_table = len(local_dimensions(tbl)) + len(local_metrics(tbl))
            attnums = [r["attnum"] for r in rel.rows if r["attrelid"] == oid]
            if attnums:
                assert max(attnums) <= max_for_table


# --- pg_catalog.pg_class.relnatts --------------------------------------------


def test_pg_class_relnatts_matches_local_count() -> None:
    """``relnatts`` counts the number of column rows pg_attribute will
    surface for that table; with the filter, it must equal the local
    count, not the raw catalog count."""
    from slayer.facade.catalog_sql import _build_pg_class, _table_oid

    cat = _orders_customers_catalog()
    rel = _build_pg_class(cat)
    by_oid = {row["oid"]: row for row in rel.rows}
    for sch in cat.schemas:
        for tbl in sch.tables:
            oid = _table_oid(sch.name, tbl)
            if oid not in by_oid:
                continue
            expected = len(local_dimensions(tbl)) + len(local_metrics(tbl))
            assert by_oid[oid]["relnatts"] == expected


# --- pg_catalog.pg_description ------------------------------------------------


def test_pg_description_attnum_within_local_range() -> None:
    """attnum on pg_description must align with the (filtered)
    pg_attribute attnum space — no orphan descriptions past the local
    column count."""
    from slayer.facade.catalog_sql import _build_pg_description, _table_oid

    orders = _model(
        name="orders",
        columns=[
            Column(
                name="id", type=DataType.INT, primary_key=True,
                description="order pk",
            ),
            Column(name="total", type=DataType.DOUBLE, description="ord total"),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _model(
        name="customers",
        columns=[
            Column(
                name="id", type=DataType.INT, primary_key=True,
                description="cust pk",
            ),
            Column(name="name", type=DataType.TEXT, description="cust name"),
        ],
    )
    # Add customer_id on orders so the join is satisfied.
    orders.columns.insert(
        1, Column(name="customer_id", type=DataType.INT, description="fk"),
    )
    cat = build_catalog(models_by_datasource={"jaffle": [orders, customers]})

    rel = _build_pg_description(cat)
    for sch in cat.schemas:
        for tbl in sch.tables:
            oid = _table_oid(sch.name, tbl)
            max_for_table = len(local_dimensions(tbl)) + len(local_metrics(tbl))
            attnums = [
                r["objsubid"] for r in rel.rows
                if r["objoid"] == oid and r["objsubid"] != 0  # NOSONAR(S125) — objsubid 0 marks the table-level description, skip it
            ]
            if attnums:
                assert max(attnums) <= max_for_table


# --- INFORMATION_SCHEMA.COLUMNS (Postgres-shaped, _build_is_columns) ---------


def test_is_columns_excludes_dotted_column_name() -> None:
    from slayer.facade.catalog_sql import _build_is_columns

    cat = _orders_customers_catalog()
    rel = _build_is_columns(cat, "jaffle")
    column_names = {row["column_name"] for row in rel.rows}
    assert not any("." in n for n in column_names)
    assert "total" in column_names


# --- INFORMATION_SCHEMA.METRICS / DIMENSIONS still include cross-model -------


def test_is_metrics_still_includes_cross_model_entries() -> None:
    """The catalog-namespaced metrics view must still expose cross-model
    metrics — that's the proper place for them and the regression guard
    against an over-eager filter."""
    from slayer.facade.catalog_sql import _build_is_metrics

    cat = _orders_customers_catalog()
    rel = _build_is_metrics(cat, "jaffle")
    metric_names = {row["metric_name"] for row in rel.rows}
    assert "customers.row_count" in metric_names


def test_is_dimensions_still_includes_cross_model_entries() -> None:
    from slayer.facade.catalog_sql import _build_is_dimensions

    cat = _orders_customers_catalog()
    rel = _build_is_dimensions(cat, "jaffle")
    dim_names = {row["dimension_name"] for row in rel.rows}
    assert "customers.name" in dim_names


# --- graph_fingerprint full-picture invariant --------------------------------


def test_graph_fingerprint_changes_when_cross_model_metric_changes() -> None:
    """Cache invalidation must still fire when a cross-model metric's
    underlying data shifts. The filter does NOT extend to the fingerprint
    hash — graph_fingerprint sees the raw view."""
    from slayer.facade.catalog_sql import _fingerprint as graph_fingerprint

    orders = _model(
        name="orders",
        columns=[
            Column(name="customer_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )

    def _customers_with_revenue_type(revenue_type: DataType) -> SlayerModel:
        return _model(
            name="customers",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="revenue", type=revenue_type),
            ],
        )

    cat_a = build_catalog(
        models_by_datasource={
            "jaffle": [orders, _customers_with_revenue_type(DataType.DOUBLE)],
        }
    )
    cat_b = build_catalog(
        models_by_datasource={
            "jaffle": [orders, _customers_with_revenue_type(DataType.INT)],
        }
    )
    # Both catalogs have a cross-model metric ``customers.revenue_sum``;
    # the type change is invisible on the flat-column probe (entry is
    # filtered out there) but must still shift the fingerprint.
    fp_a = graph_fingerprint(cat_a, "jaffle")
    fp_b = graph_fingerprint(cat_b, "jaffle")
    assert fp_a != fp_b
