"""Seed data and SLayer model builders for the Ossie export tests."""

from __future__ import annotations

from typing import Any

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Aggregation, Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery

DATASOURCE = "osids"

EXPORT_DDL = [
    "CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_totalprice DECIMAL(18,2), "
    "amount DOUBLE, quantity INTEGER, price DOUBLE, discount DOUBLE, status VARCHAR, region VARCHAR, "
    "is_paid BOOLEAN, ordered_on DATE, ordered_at TIMESTAMP, deleted_at TIMESTAMP, order_year INTEGER, "
    "note VARCHAR, \"LegalType\" VARCHAR)",
    "CREATE TABLE customers (c_custkey INTEGER PRIMARY KEY, segment VARCHAR, discount DOUBLE, rate DOUBLE, "
    "email VARCHAR)",
    "CREATE TABLE visits (visit_id INTEGER PRIMARY KEY, status VARCHAR, customer_id INTEGER)",
    "INSERT INTO orders VALUES "
    "(1, 101, 10.00, 10, 1, 10, 0, 'completed', 'US', TRUE, '2026-01-05', '2026-01-05 10:00:00', NULL, 2026, "
    "NULL, 'llc'), "
    "(2, 101, 5.00, 5, 2, 2.5, 1, 'open', 'EU', FALSE, '2026-01-06', '2026-01-06 11:00:00', NULL, 2026, "
    "NULL, 'gmbh'), "
    "(3, 102, 7.00, 7, 3, 3, 0.5, 'completed', 'US', TRUE, '2026-02-01', '2026-02-01 12:00:00', NULL, 2026, "
    "NULL, 'llc'), "
    "(4, 102, 4.00, 4, 1, 4, 0, 'test', 'US', NULL, '2026-02-03', '2026-02-03 09:00:00', NULL, 2025, "
    "NULL, NULL), "
    "(5, 101, 20.00, 20, 4, 5, 2, 'completed', 'EU', TRUE, '2026-03-02', '2026-03-02 08:00:00', "
    "'2026-03-05 00:00:00', 2026, NULL, 'llc')",
    "INSERT INTO customers VALUES (101, 'b2b', 1.0, 0.1, 'a@x.io'), (102, 'b2c', 0.5, 0.2, 'b@x.io'), "
    "(103, 'b2b', 2.0, 0.3, 'c@x.io')",
    "INSERT INTO visits VALUES (1, 'completed', 101), (2, 'completed', 101), (3, 'open', 102), "
    "(4, 'completed', 103)",
]


def col(name: str, type_: DataType = DataType.DOUBLE, sql: str | None = None, **extra: Any) -> Column:
    return Column(name=name, sql=sql, type=type_, **extra)


def measure(name: str, formula: str, **extra: Any) -> ModelMeasure:
    return ModelMeasure(name=name, formula=formula, **extra)


def to_customers(**extra: Any) -> ModelJoin:
    """``orders`` → ``customers`` on ``customer_id = id``, many-to-one unless overridden."""
    extra.setdefault("cardinality", JoinCardinality.MANY_TO_ONE)
    return ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]], **extra)


def orders_columns() -> list[Column]:
    return [
        col("id", DataType.INT, "o_orderkey", primary_key=True),
        col("customer_id", DataType.INT, "o_custkey"),
        col("total_amount", DataType.DOUBLE, "o_totalprice", db_type="DECIMAL(18,2)"),
        col("amount"),
        col("quantity", DataType.INT),
        col("price"),
        col("discount"),
        col("status", DataType.TEXT),
        col("region", DataType.TEXT),
        col("is_paid", DataType.BOOLEAN),
        col("ordered_on", DataType.DATE),
        col("ordered_at", DataType.TIMESTAMP),
        col("order_year", DataType.INT),
        col("note", DataType.TEXT),
    ]


def orders(*, columns: list[Column] | None = None, measures: list[ModelMeasure] | None = None,
           joins: list[ModelJoin] | None = None, **extra: Any) -> SlayerModel:
    """The ``orders`` table model; by default joined many-to-one to ``customers``."""
    return SlayerModel(
        name=extra.pop("name", "orders"), sql_table=extra.pop("sql_table", "orders"), data_source=DATASOURCE,
        columns=orders_columns() if columns is None else columns,
        measures=measures or [],
        joins=[to_customers()] if joins is None else joins,
        default_time_dimension=extra.pop("default_time_dimension", "ordered_on"),
        **extra,
    )


def customers(*, measures: list[ModelMeasure] | None = None, joins: list[ModelJoin] | None = None,
              **extra: Any) -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=DATASOURCE,
        columns=[
            col("id", DataType.INT, "c_custkey", primary_key=True),
            col("segment", DataType.TEXT),
            col("discount"),
            col("rate"),
            col("email", DataType.TEXT, unique=True),
        ],
        measures=measures or [],
        joins=joins or [],
        **extra,
    )


def visits(**extra: Any) -> SlayerModel:
    """A sql-mode model over ``visits``."""
    return SlayerModel(
        name="visits", sql="SELECT visit_id, status, customer_id FROM visits", data_source=DATASOURCE,
        columns=[
            col("visit_id", DataType.INT, primary_key=True),
            col("status", DataType.TEXT),
            col("customer_id", DataType.INT),
        ],
        **extra,
    )


def monthly_revenue() -> SlayerModel:
    """A query-backed model with a ``{region}`` variable defaulting to ``US``."""
    return SlayerModel(
        name="monthly_revenue", data_source=DATASOURCE,
        source_queries=[SlayerQuery.model_validate({
            "source_model": "orders",
            "time_dimensions": [{"dimension": "ordered_on", "granularity": "month"}],
            "measures": [{"formula": "sum(amount)", "name": "revenue"}],
            "filters": ["region = '{region}'"],
        })],
        query_variables={"region": "US"},
    )


BASIC_MEASURES = [
    measure("revenue", "sum(amount)"),
    measure("n", "count(*)"),
    measure("aov", "revenue / n"),
]


def rich_orders() -> SlayerModel:
    """``orders`` exercising every exportable measure family plus SLayer-only attributes."""
    columns = orders_columns()
    for c in columns:
        if c.name == "amount":
            c.format = NumberFormat(type=NumberFormatType.CURRENCY, precision=2)
            c.label = "Order amount"
            c.description = "Gross order value"
            c.meta = {"unit": "usd"}
        if c.name == "status":
            c.allowed_aggregations = ["count", "count_distinct"]
        if c.name == "ordered_on":
            c.granularity = "day"
        if c.name == "note":
            c.hidden = True
    columns += [
        col("gross", sql="price * quantity"),
        col("net", sql="gross - discount"),
        col("completed_amount", sql="amount", filter="status = 'completed'"),
        col("paid_flag", DataType.BOOLEAN, sql="is_paid", filter="status <> 'test'"),
        col("segment", DataType.TEXT, sql="customers.segment"),
    ]
    return orders(
        columns=columns,
        aggregations=[Aggregation(name="trimmed_sum", formula="SUM(CASE WHEN {value} < 15 THEN {value} END)")],
        measures=[
            *BASIC_MEASURES,
            measure("avg_amount", "avg(amount)", label="Average amount", type=DataType.DOUBLE,
                    meta={"owner": "finance"}),
            measure("min_amount", "min(amount)"),
            measure("max_amount", "max(amount)"),
            measure("n_amount", "count(amount)"),
            measure("n_customers", "count_distinct(customer_id)"),
            measure("p90", "percentile(amount, 0.9)"),
            measure("med", "median(amount)"),
            measure("sd", "stddev_samp(amount)"),
            measure("sdp", "stddev_pop(amount)"),
            measure("vs", "var_samp(amount)"),
            measure("vp", "var_pop(amount)"),
            measure("r", "corr(amount, other=quantity)"),
            measure("cvs", "covar_samp(amount, other=quantity)"),
            measure("cvp", "covar_pop(amount, other=quantity)"),
            measure("wavg", "weighted_avg(price, weight=quantity)"),
            measure("paid_count", "sum(is_paid)"),
            measure("paid_masked", "sum(paid_flag)"),
            measure("done_amount", "sum(completed_amount)"),
            measure("done_count", "count(completed_amount)"),
            measure("net_total", "sum(net)"),
            measure("notes", "count(note)"),
            measure("max_note", "max(note)"),
            measure("trimmed", "trimmed_sum(amount)"),
            measure("customer_discount", "sum(customers.discount)"),
            measure("aov_per_customer", "sum(amount) / count_distinct(customers.id)"),
            measure("discounted", "sum(amount * customers.rate)"),
            measure("revenue_growth", "change_pct(revenue)"),
            measure("running", "cumsum(revenue)"),
            measure("latest_amount", "last(amount)"),
            measure("rev_30d", "sum(amount, window='30d')"),
            measure("region_total", "sum(amount, partition_by=region)"),
        ],
    )

PG_DDL = [s.replace(" DOUBLE,", " DOUBLE PRECISION,").replace(" DOUBLE)", " DOUBLE PRECISION)") for s in EXPORT_DDL]
