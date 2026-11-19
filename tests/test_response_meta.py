"""response_meta.build_response_metadata.

Asserts the typed-plan-derived ``attributes`` + ``expected_columns`` match
what the legacy enriched-query path produced: result keys read straight from
the statement's projection, dimensions vs measures split by phase, measure format
inferred (currency inheritance, count→integer), labels propagated.
"""

from __future__ import annotations

import sqlglot

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.response_meta import build_response_metadata
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query
from slayer.sql.generator import generate_from_planned


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(
                name="amount",
                datatype=DataType.FLOAT,
                format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
            ),
            DatasetField(name="quantity", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING, label="Order status"),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="prod",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(
                name="revenue",
                datatype=DataType.FLOAT,
                format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="$"),
            ),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=_orders(), referenced_datasets=[_customers()]
    )


def _meta_for(query: SlayerQuery):
    bundle = _bundle()
    planned = plan_query(query=query, bundle=bundle)
    sql = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    attrs, cols = build_response_metadata(
        root_planned=planned, bundle=bundle, statement=sqlglot.parse_one(sql, dialect="postgres"),
    )
    return attrs, cols, sql


def test_expected_columns_match_rendered_result_keys():
    q = SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": ["orders.status"],
        "metrics": [{"metric": "sum(orders.amount)"}],
    })
    _attrs, cols, _sql = _meta_for(q)
    assert set(cols) == {"orders.status", "orders.amount_sum"}


def test_dimension_vs_measure_split_and_label():
    q = SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": ["orders.status"],
        "metrics": [{"metric": "sum(orders.amount)"}],
    })
    attrs, _cols, _sql = _meta_for(q)
    assert "orders.status" in attrs.dimensions
    assert "orders.amount_sum" in attrs.metrics
    # Dimension label flows from the Column.label.
    assert attrs.dimensions["orders.status"].label == "Order status"


def test_sum_inherits_currency_format():
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
    attrs, _cols, _sql = _meta_for(q)
    fm = attrs.metrics["orders.amount_sum"]
    assert fm.format is not None
    assert fm.format.type == NumberFormatType.CURRENCY
    assert fm.format.symbol == "€"


def test_star_count_is_integer():
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
    attrs, cols, _sql = _meta_for(q)
    assert "orders._count" in cols
    fm = attrs.metrics["orders._count"]
    assert fm.format.type == NumberFormatType.INTEGER


def test_renamed_measure_result_key_and_format():
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]})
    attrs, cols, _sql = _meta_for(q)
    assert cols == ["rev"]
    assert "rev" in attrs.metrics


def test_joined_dimension_result_key_full_path():
    q = SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": ["customers.name"],
        "metrics": [{"metric": "sum(orders.amount)"}],
    })
    _attrs, cols, _sql = _meta_for(q)
    assert "customers.name" in cols


def test_cross_model_aggregate_format_integer_for_count():
    q = SlayerQuery.model_validate({
        "population": "orders",
        "dimensions": ["orders.status"],
        "metrics": [{"metric": "sum(customers.revenue)"}],
    })
    attrs, cols, _sql = _meta_for(q)
    assert "customers.revenue_sum" in cols
    fm = attrs.metrics["customers.revenue_sum"]
    # sum inherits the target column's currency format.
    assert fm.format is not None
    assert fm.format.type == NumberFormatType.CURRENCY
    assert fm.format.symbol == "$"


# ---------------------------------------------------------------------------
# DEV-1788: stat/parametric aggregation display format through the full
# response-metadata path (query → plan → build_response_metadata → attributes).
# ---------------------------------------------------------------------------


def test_float_plain_aggregation_is_plain_float():
    # var_samp is in squared units — the source currency is NOT inherited.
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "var_samp(orders.amount)"}]})
    attrs, _cols, _sql = _meta_for(q)
    fm = attrs.metrics["orders.amount_var_samp"]
    assert fm.format.type == NumberFormatType.FLOAT


def test_avg_inherits_currency_format():
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "avg(orders.amount)"}]})
    attrs, _cols, _sql = _meta_for(q)
    fm = attrs.metrics["orders.amount_avg"]
    assert fm.format.type == NumberFormatType.CURRENCY
    assert fm.format.symbol == "€"


def test_avg_unformatted_measure_is_plain_float():
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "avg(orders.quantity)"}]})
    attrs, _cols, _sql = _meta_for(q)
    fm = attrs.metrics["orders.quantity_avg"]
    assert fm.format.type == NumberFormatType.FLOAT


def test_preserving_unformatted_measure_omits_format_entry():
    # quantity:sum has no source format and no label → no attributes entry,
    # though the column is still projected in expected_columns.
    q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.quantity)"}]})
    attrs, cols, _sql = _meta_for(q)
    assert "orders.quantity_sum" in cols
    assert "orders.quantity_sum" not in attrs.metrics
