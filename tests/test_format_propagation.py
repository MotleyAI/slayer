"""Tests for number format propagation through the query engine metadata pipeline."""

import pytest

from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.enums import (
    AggregationValueClass,
    BUILTIN_AGGREGATIONS,
    DataType,
    FLOAT_PLAIN_AGGREGATIONS,
    LIST_AGGREGATIONS,
    FLOAT_SOURCE_UNIT_AGGREGATIONS,
    INTEGER_AGGREGATIONS,
    PRESERVING_AGGREGATIONS,
    classify_aggregation,
)
from slayer.core.keys import AggregateKey, FieldKey, StarKey
from slayer.core.models import DatasetField, SlayerDataset
from slayer.engine.query_engine import FieldMetadata

from slayer.engine.response_meta import _infer_aggregated_format
from slayer.engine.query_engine import ResponseAttributes
from slayer.mcp.server import _format_attributes
from slayer.engine.key_metadata import (
    aggregated_type,
    metric_key_format_description,
    metric_key_type,
)


# ---------------------------------------------------------------------------
# _infer_aggregated_format
# ---------------------------------------------------------------------------


class TestInferAggregatedFormat:
    """Tests for _infer_aggregated_format resolving formats from source metrics."""

    @pytest.fixture()
    def dataset(self):
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="test_ds",
            fields=[
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(
                    name="revenue",
                    expression="amount",
                    datatype=DataType.FLOAT,
                    format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
                ),
                DatasetField(
                    name="margin",
                    expression="margin",
                    datatype=DataType.FLOAT,
                    format=NumberFormat(type=NumberFormatType.PERCENT),
                ),
                DatasetField(name="quantity", expression="quantity", datatype=DataType.FLOAT),
            ],
        )

    def test_star_count_returns_integer(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="*", aggregation="count")
        assert fmt.type == NumberFormatType.INTEGER

    def test_count_returns_integer(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation="count")
        assert fmt.type == NumberFormatType.INTEGER

    def test_count_distinct_returns_integer(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation="count_distinct")
        assert fmt.type == NumberFormatType.INTEGER

    def test_avg_inherits_currency(self, dataset):
        # avg-family now inherits the source field's units (was FLOAT).
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation="avg")
        assert fmt.type == NumberFormatType.CURRENCY
        assert fmt.symbol == "€"

    def test_avg_unformatted_falls_back_to_float(self, dataset):
        # No source units to inherit → FLOAT (result is fractional). Option 1.
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="quantity", aggregation="avg")
        assert fmt.type == NumberFormatType.FLOAT

    def test_sum_inherits_currency(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation="sum")
        assert fmt.type == NumberFormatType.CURRENCY
        assert fmt.symbol == "€"

    def test_min_inherits_percent(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="margin", aggregation="min")
        assert fmt.type == NumberFormatType.PERCENT

    def test_max_inherits_format(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation="max")
        assert fmt.type == NumberFormatType.CURRENCY

    def test_sum_no_format_returns_none(self, dataset):
        """Preserving aggregation over a metric without format returns None."""
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="quantity", aggregation="sum")
        assert fmt is None

    def test_unknown_measure_returns_none(self, dataset):
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="nonexistent", aggregation="sum")
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
        attrs = ResponseAttributes(
            metrics={
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
        attrs = ResponseAttributes(
            metrics={
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
# Shared classifier drift guard.
#
# ``aggregated_type`` (slot DataType) and ``_infer_aggregated_format`` (display
# NumberFormat) both read the single ``classify_aggregation`` classifier, so the
# type and format axes cannot drift apart again. These tests pin the full
# four-bucket table across formatted / unformatted / int-typed sources and
# through the public prebound callers.
# ---------------------------------------------------------------------------


class TestSharedClassifier:
    @pytest.fixture
    def dataset(self):
        return SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="test_ds",
            fields=[
                DatasetField(
                    name="revenue",
                    datatype=DataType.FLOAT,
                    format=NumberFormat(type=NumberFormatType.CURRENCY, symbol="€"),
                ),
                DatasetField(name="quantity", datatype=DataType.FLOAT),
                DatasetField(name="qty_int", datatype=DataType.INTEGER),
            ],
        )

    # --- classification table (hard-coded, independent of the frozensets) ---

    @pytest.mark.parametrize(
        "aggregation,expected",
        [
            ("count", AggregationValueClass.COUNT),
            ("count_distinct", AggregationValueClass.COUNT),
            ("count_distinct_approx", AggregationValueClass.COUNT),
            ("sum", AggregationValueClass.PRESERVING),
            ("min", AggregationValueClass.PRESERVING),
            ("max", AggregationValueClass.PRESERVING),
            ("first", AggregationValueClass.PRESERVING),
            ("last", AggregationValueClass.PRESERVING),
            ("avg", AggregationValueClass.FLOAT_SOURCE_UNITS),
            ("weighted_avg", AggregationValueClass.FLOAT_SOURCE_UNITS),
            ("median", AggregationValueClass.FLOAT_SOURCE_UNITS),
            ("percentile", AggregationValueClass.FLOAT_SOURCE_UNITS),
            ("stddev_samp", AggregationValueClass.FLOAT_SOURCE_UNITS),
            ("stddev_pop", AggregationValueClass.FLOAT_SOURCE_UNITS),
            ("corr", AggregationValueClass.FLOAT_PLAIN),
            ("var_samp", AggregationValueClass.FLOAT_PLAIN),
            ("var_pop", AggregationValueClass.FLOAT_PLAIN),
            ("covar_samp", AggregationValueClass.FLOAT_PLAIN),
            ("covar_pop", AggregationValueClass.FLOAT_PLAIN),
        ],
    )
    def test_classification_table(self, aggregation, expected):
        assert classify_aggregation(metric_name="revenue", aggregation=aggregation, source_type=None) == expected

    def test_star_classifies_as_count(self):
        assert (
            classify_aggregation(metric_name="*", aggregation="count", source_type=None)
            == AggregationValueClass.COUNT
        )

    def test_custom_aggregation_falls_back_to_preserving(self, dataset):
        # Dataset-defined aggregations (not builtin) inherit source type & format
        # through both consumers, not only the classifier.
        assert (
            classify_aggregation(metric_name="revenue", aggregation="my_custom", source_type=None)
            == AggregationValueClass.PRESERVING
        )
        assert aggregated_type(dataset=dataset, metric_name="revenue", aggregation="my_custom") == DataType.FLOAT
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation="my_custom")
        assert fmt.type == NumberFormatType.CURRENCY
        assert fmt.symbol == "€"

    # --- partition completeness: forces any new builtin agg to be classified ---

    def test_four_sets_partition_builtin_aggregations(self):
        sets = [
            INTEGER_AGGREGATIONS,
            PRESERVING_AGGREGATIONS,
            FLOAT_SOURCE_UNIT_AGGREGATIONS,
            FLOAT_PLAIN_AGGREGATIONS,
            LIST_AGGREGATIONS,
        ]
        union = set().union(*sets)
        assert union == BUILTIN_AGGREGATIONS
        assert sum(len(s) for s in sets) == len(union)  # pairwise disjoint

    # --- COUNT bucket: INT / INTEGER on every source ---

    @pytest.mark.parametrize("measure", ["revenue", "quantity", "qty_int"])
    @pytest.mark.parametrize("aggregation", sorted(INTEGER_AGGREGATIONS))
    def test_count_bucket(self, dataset, aggregation, measure):
        assert aggregated_type(dataset=dataset, metric_name=measure, aggregation=aggregation) == DataType.INTEGER
        assert (
            _infer_aggregated_format(dataset=dataset, metric_name=measure, aggregation=aggregation).type
            == NumberFormatType.INTEGER
        )

    def test_star_count_bucket(self, dataset):
        assert aggregated_type(dataset=dataset, metric_name="*", aggregation="count") == DataType.INTEGER
        assert (
            _infer_aggregated_format(dataset=dataset, metric_name="*", aggregation="count").type
            == NumberFormatType.INTEGER
        )

    # --- PRESERVING bucket: inherit source type & format ---

    @pytest.mark.parametrize("aggregation", sorted(PRESERVING_AGGREGATIONS))
    def test_preserving_currency_source(self, dataset, aggregation):
        assert aggregated_type(dataset=dataset, metric_name="revenue", aggregation=aggregation) == DataType.FLOAT
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation=aggregation)
        assert fmt.type == NumberFormatType.CURRENCY
        assert fmt.symbol == "€"

    @pytest.mark.parametrize("aggregation", sorted(PRESERVING_AGGREGATIONS))
    def test_preserving_unformatted_double_source(self, dataset, aggregation):
        assert aggregated_type(dataset=dataset, metric_name="quantity", aggregation=aggregation) == DataType.FLOAT
        assert _infer_aggregated_format(dataset=dataset, metric_name="quantity", aggregation=aggregation) is None

    @pytest.mark.parametrize("aggregation", sorted(PRESERVING_AGGREGATIONS))
    def test_preserving_int_source_inherits_int(self, dataset, aggregation):
        # The type axis: PRESERVING inherits the source's INT type ...
        assert aggregated_type(dataset=dataset, metric_name="qty_int", aggregation=aggregation) == DataType.INTEGER
        assert _infer_aggregated_format(dataset=dataset, metric_name="qty_int", aggregation=aggregation) is None

    # --- FLOAT_SOURCE_UNITS bucket: DOUBLE type always; format inherits, else FLOAT ---

    @pytest.mark.parametrize("aggregation", sorted(FLOAT_SOURCE_UNIT_AGGREGATIONS))
    def test_float_source_units_currency(self, dataset, aggregation):
        assert aggregated_type(dataset=dataset, metric_name="revenue", aggregation=aggregation) == DataType.FLOAT
        fmt = _infer_aggregated_format(dataset=dataset, metric_name="revenue", aggregation=aggregation)
        assert fmt.type == NumberFormatType.CURRENCY
        assert fmt.symbol == "€"

    @pytest.mark.parametrize("measure", ["quantity", "qty_int"])
    @pytest.mark.parametrize("aggregation", sorted(FLOAT_SOURCE_UNIT_AGGREGATIONS))
    def test_float_source_units_unformatted_falls_back_to_float(self, dataset, aggregation, measure):
        # ... whereas FLOAT_SOURCE_UNITS FORCES DOUBLE, even over an INT source.
        assert aggregated_type(dataset=dataset, metric_name=measure, aggregation=aggregation) == DataType.FLOAT
        assert (
            _infer_aggregated_format(dataset=dataset, metric_name=measure, aggregation=aggregation).type
            == NumberFormatType.FLOAT
        )

    # --- FLOAT_PLAIN bucket: DOUBLE type; plain FLOAT format on every source ---

    @pytest.mark.parametrize("measure", ["revenue", "quantity", "qty_int"])
    @pytest.mark.parametrize("aggregation", sorted(FLOAT_PLAIN_AGGREGATIONS))
    def test_float_plain_bucket(self, dataset, aggregation, measure):
        assert aggregated_type(dataset=dataset, metric_name=measure, aggregation=aggregation) == DataType.FLOAT
        assert (
            _infer_aggregated_format(dataset=dataset, metric_name=measure, aggregation=aggregation).type
            == NumberFormatType.FLOAT
        )

    # --- missing source field ---

    def test_preserving_missing_column(self, dataset):
        assert aggregated_type(dataset=dataset, metric_name="nope", aggregation="sum") is None
        assert _infer_aggregated_format(dataset=dataset, metric_name="nope", aggregation="sum") is None

    def test_float_source_units_missing_column_falls_back_to_float(self, dataset):
        assert aggregated_type(dataset=dataset, metric_name="nope", aggregation="avg") == DataType.FLOAT
        assert (
            _infer_aggregated_format(dataset=dataset, metric_name="nope", aggregation="avg").type
            == NumberFormatType.FLOAT
        )

    def test_float_plain_missing_column(self, dataset):
        assert aggregated_type(dataset=dataset, metric_name="nope", aggregation="corr") == DataType.FLOAT
        assert (
            _infer_aggregated_format(dataset=dataset, metric_name="nope", aggregation="corr").type
            == NumberFormatType.FLOAT
        )

    # --- drift guard through the public prebound callers ---

    @pytest.mark.parametrize(
        "aggregation,exp_type,exp_fmt",
        [
            ("count", DataType.INTEGER, NumberFormatType.INTEGER),
            ("sum", DataType.FLOAT, NumberFormatType.CURRENCY),  # inherit revenue's currency
            ("avg", DataType.FLOAT, NumberFormatType.CURRENCY),  # FLOAT_SOURCE_UNITS inherits currency
            ("var_samp", DataType.FLOAT, NumberFormatType.FLOAT),  # FLOAT_PLAIN drops currency
        ],
    )
    def test_public_callers_agree_per_bucket(self, dataset, aggregation, exp_type, exp_fmt):
        key = AggregateKey(source=FieldKey(leaf="revenue"), agg=aggregation)
        assert metric_key_type(dataset=dataset, key=key) == exp_type
        fmt, _desc = metric_key_format_description(dataset=dataset, key=key)
        assert fmt.type == exp_fmt

    def test_public_callers_star_count(self, dataset):
        key = AggregateKey(source=StarKey(), agg="count")
        assert metric_key_type(dataset=dataset, key=key) == DataType.INTEGER
        fmt, desc = metric_key_format_description(dataset=dataset, key=key)
        assert fmt.type == NumberFormatType.INTEGER
        assert desc is None
