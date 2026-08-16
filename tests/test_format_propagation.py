"""Tests for number format propagation through the query engine metadata pipeline."""

import pytest

from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType, INTEGER_AGGREGATIONS
from slayer.core.models import Column, SlayerModel
from slayer.engine.query_engine import FieldMetadata

# DEV-1485 Stage D: imported through ``query_engine`` while the legacy
# ``_query_as_model`` re-exported it; now imported from its owning module.
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.engine.prebound import aggregated_type


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
            columns=[
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(
                    name="revenue",
                    sql="amount",
                    type=DataType.DOUBLE,
                    format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
                ),
                Column(
                    name="margin",
                    sql="margin",
                    type=DataType.DOUBLE,
                    format=NumberFormat(type=NumberFormatType.PERCENT),
                ),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
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


# ---------------------------------------------------------------------------
# Drift guard: ``aggregated_type`` (slot DataType) and ``_infer_aggregated_format``
# (display NumberFormat) both classify aggregations, and the integer bucket is
# the one classification they MUST agree on. Pinned here against the shared
# ``INTEGER_AGGREGATIONS`` constant so neither can drift on it. The float /
# inherit split for stat aggregations legitimately differs (type vs display)
# and is deliberately left as-is — reconciling it is tracked in DEV-1788.
# ---------------------------------------------------------------------------


class TestIntegerBucketSharedByTypeAndFormat:
    @pytest.fixture
    def model(self):
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test_ds",
            columns=[
                Column(
                    name="revenue",
                    sql="amount",
                    type=DataType.DOUBLE,
                    format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
                ),
            ],
        )

    def test_shared_constant_names_only_builtin_aggregations(self):
        assert INTEGER_AGGREGATIONS <= BUILTIN_AGGREGATIONS

    @pytest.mark.parametrize("aggregation", sorted(INTEGER_AGGREGATIONS))
    def test_integer_aggs_are_int_type_and_integer_format(self, model, aggregation):
        assert aggregated_type(
            model=model, measure_name="revenue", aggregation=aggregation,
        ) == DataType.INT
        assert _infer_aggregated_format(
            model=model, measure_name="revenue", aggregation=aggregation,
        ).type == NumberFormatType.INTEGER

    def test_star_count_is_int_type_and_integer_format(self, model):
        assert aggregated_type(
            model=model, measure_name="*", aggregation="count",
        ) == DataType.INT
        assert _infer_aggregated_format(
            model=model, measure_name="*", aggregation="count",
        ).type == NumberFormatType.INTEGER

    def test_stat_agg_type_format_divergence_is_pinned(self, model):
        # stddev is always a float, so the slot TYPE is DOUBLE — but the display
        # FORMAT still inherits the source column's format (currency here), not
        # integer/float. This intentional divergence is what DEV-1788 revisits.
        assert aggregated_type(
            model=model, measure_name="revenue", aggregation="stddev_samp",
        ) == DataType.DOUBLE
        assert _infer_aggregated_format(
            model=model, measure_name="revenue", aggregation="stddev_samp",
        ).type == NumberFormatType.CURRENCY
