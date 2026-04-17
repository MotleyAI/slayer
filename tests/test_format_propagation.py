"""Tests for number format propagation through the query engine metadata pipeline."""

import pytest

from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Dimension, Measure, SlayerModel
from slayer.engine.query_engine import FieldMetadata, _infer_aggregated_format


# ---------------------------------------------------------------------------
# _infer_aggregated_format
# ---------------------------------------------------------------------------


class TestInferAggregatedFormat:
    """Tests for _infer_aggregated_format resolving formats from source measures."""

    @pytest.fixture()
    def model(self):
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test_ds",
            dimensions=[
                Dimension(name="status", sql="status"),
            ],
            measures=[
                Measure(
                    name="revenue",
                    sql="amount",
                    format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
                ),
                Measure(
                    name="margin",
                    sql="margin",
                    format=NumberFormat(type=NumberFormatType.PERCENT),
                ),
                Measure(name="quantity", sql="quantity"),
            ],
        )

    def test_star_count_returns_integer(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="*", aggregation="count")
        assert fmt.type == NumberFormatType.INTEGER

    def test_count_returns_integer(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="revenue", aggregation="count")
        assert fmt.type == NumberFormatType.INTEGER

    def test_count_distinct_returns_integer(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="revenue", aggregation="count_distinct")
        assert fmt.type == NumberFormatType.INTEGER

    def test_avg_returns_float(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="revenue", aggregation="avg")
        assert fmt.type == NumberFormatType.FLOAT

    def test_sum_inherits_currency(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="revenue", aggregation="sum")
        assert fmt.type == NumberFormatType.CURRENCY
        assert fmt.symbol == "€"

    def test_min_inherits_percent(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="margin", aggregation="min")
        assert fmt.type == NumberFormatType.PERCENT

    def test_max_inherits_format(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="revenue", aggregation="max")
        assert fmt.type == NumberFormatType.CURRENCY

    def test_sum_no_format_returns_none(self, model):
        """Measure without format returns None for inheriting aggregations."""
        fmt = _infer_aggregated_format(model=model, measure_name="quantity", aggregation="sum")
        assert fmt is None

    def test_unknown_measure_returns_none(self, model):
        fmt = _infer_aggregated_format(model=model, measure_name="nonexistent", aggregation="sum")
        assert fmt is None


# ---------------------------------------------------------------------------
# FieldMetadata from enriched queries
# ---------------------------------------------------------------------------


class TestFieldMetadata:
    """Tests for FieldMetadata construction."""

    def test_metadata_with_format_no_label(self):
        fm = FieldMetadata(format=NumberFormat(type=NumberFormatType.CURRENCY))
        assert fm.label is None
        assert fm.format.type == NumberFormatType.CURRENCY

    def test_metadata_with_label_and_format(self):
        fm = FieldMetadata(label="Revenue", format=NumberFormat(type=NumberFormatType.FLOAT))
        assert fm.label == "Revenue"
        assert fm.format.type == NumberFormatType.FLOAT


# ---------------------------------------------------------------------------
# MCP format metadata output
# ---------------------------------------------------------------------------


class TestMcpFormatMeta:
    """Tests for _format_attributes in MCP server."""

    def test_format_meta_includes_precision_and_symbol(self):
        from slayer.engine.query_engine import ResponseAttributes
        from slayer.mcp.server import _format_attributes

        attrs = ResponseAttributes(
            measures={
                "orders.revenue_sum": FieldMetadata(
                    label="Revenue",
                    format=NumberFormat(type=NumberFormatType.CURRENCY, precision=2, symbol="€"),
                ),
            },
        )
        result = _format_attributes(attributes=attrs)
        assert "type=currency" in result
        assert "precision=2" in result
        assert "symbol=€" in result

    def test_format_meta_omits_none_fields(self):
        from slayer.engine.query_engine import ResponseAttributes
        from slayer.mcp.server import _format_attributes

        attrs = ResponseAttributes(
            measures={
                "orders.count": FieldMetadata(
                    format=NumberFormat(type=NumberFormatType.INTEGER),
                ),
            },
        )
        result = _format_attributes(attributes=attrs)
        assert "type=integer" in result
        assert "precision" not in result
        assert "symbol" not in result
