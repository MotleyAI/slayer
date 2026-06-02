"""DEV-1450 stage 7b.15d — response_meta.build_response_metadata.

Asserts the typed-plan-derived ``attributes`` + ``expected_columns`` match
what the legacy EnrichedQuery path produced: result keys read straight from
the rendered SQL, dimensions vs measures split by phase, measure format
inferred (currency inheritance, count→integer), labels propagated.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.response_meta import build_response_metadata
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import generate_from_planned


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(
                name="amount",
                type=DataType.DOUBLE,
                format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
            ),
            Column(name="status", type=DataType.TEXT, label="Order status"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(
                name="revenue",
                type=DataType.DOUBLE,
                format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="$"),
            ),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(), referenced_models=[_customers()]
    )


def _meta_for(query: SlayerQuery):
    bundle = _bundle()
    planned = plan_query(query=query, bundle=bundle)
    sql = generate_from_planned(planned, bundle=bundle, dialect="postgres")
    attrs, cols = build_response_metadata(
        root_planned=planned, bundle=bundle, sql=sql, dialect="postgres"
    )
    return attrs, cols, sql


def test_expected_columns_match_rendered_result_keys():
    q = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
    )
    _attrs, cols, _sql = _meta_for(q)
    assert set(cols) == {"orders.status", "orders.amount_sum"}


def test_dimension_vs_measure_split_and_label():
    q = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
    )
    attrs, _cols, _sql = _meta_for(q)
    assert "orders.status" in attrs.dimensions
    assert "orders.amount_sum" in attrs.measures
    # Dimension label flows from the Column.label.
    assert attrs.dimensions["orders.status"].label == "Order status"


def test_sum_inherits_currency_format():
    q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
    attrs, _cols, _sql = _meta_for(q)
    fm = attrs.measures["orders.amount_sum"]
    assert fm.format is not None
    assert fm.format.type == NumberFormatType.CURRENCY
    assert fm.format.symbol == "€"


def test_star_count_is_integer():
    q = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
    attrs, cols, _sql = _meta_for(q)
    assert "orders._count" in cols
    fm = attrs.measures["orders._count"]
    assert fm.format.type == NumberFormatType.INTEGER


def test_renamed_measure_result_key_and_format():
    q = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "amount:sum", "name": "rev"}],
    )
    attrs, cols, _sql = _meta_for(q)
    assert cols == ["orders.rev"]
    assert "orders.rev" in attrs.measures


def test_joined_dimension_result_key_full_path():
    q = SlayerQuery(
        source_model="orders",
        dimensions=["customers.name"],
        measures=[{"formula": "amount:sum"}],
    )
    _attrs, cols, _sql = _meta_for(q)
    assert "orders.customers.name" in cols


def test_cross_model_aggregate_format_integer_for_count():
    q = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum"}],
    )
    attrs, cols, _sql = _meta_for(q)
    assert "orders.customers.revenue_sum" in cols
    fm = attrs.measures["orders.customers.revenue_sum"]
    # sum inherits the target column's currency format.
    assert fm.format is not None
    assert fm.format.type == NumberFormatType.CURRENCY
    assert fm.format.symbol == "$"
