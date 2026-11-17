"""Boolean aggregation result types and formats — one classifier, every surface agrees.

Spec: aggregations/boolean-inputs › "Boolean aggregation result types and formats",
"Boolean defaults include avg".
"""

from __future__ import annotations

from typing import Optional

import pytest

from slayer.core.enums import (
    DEFAULT_AGGREGATIONS_BY_TYPE,
    AggregationValueClass,
    DataType,
    classify_aggregation,
)
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.keys import AggregateKey, FieldKey, ValueKey
from slayer.core.models import Aggregation, DatasetField, SlayerDataset
from slayer.core.scope import DatasetScope, StageField, StageSchema
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import aggregated_type, metric_key_type, stage_metric_type
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.core.syntax import parse_expr
from slayer.facade.catalog import build_local_view
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev2046_fixtures import customers_model, orders_model

ORDERS = orders_model()
CUSTOMERS = customers_model()
BUNDLE = ResolvedSourceBundle(dialect="postgres", population=ORDERS, referenced_datasets=[CUSTOMERS])
INTEGER = NumberFormat(type=NumberFormatType.INTEGER)
PERCENT = NumberFormat(type=NumberFormatType.PERCENT)


def _bind(text: str) -> ValueKey:
    return bind_expr(
        parse_expr(text), scope=DatasetScope(population=ORDERS), bundle=BUNDLE, allow_metrics=True,
    ).value_key


class TestClassifier:
    @pytest.mark.parametrize("agg,expected", [
        ("sum", AggregationValueClass.COUNT),
        ("avg", AggregationValueClass.FLOAT_SOURCE_UNITS),
        ("min", AggregationValueClass.PRESERVING),
        ("max", AggregationValueClass.PRESERVING),
        ("count", AggregationValueClass.COUNT),
        ("count_distinct", AggregationValueClass.COUNT),
        ("first", AggregationValueClass.PRESERVING),
        ("last", AggregationValueClass.PRESERVING),
        ("my_custom", AggregationValueClass.PRESERVING),
    ])
    def test_boolean_source(self, agg: str, expected) -> None:
        assert classify_aggregation(
            metric_name="flag", aggregation=agg, source_type=DataType.BOOLEAN,
        ) is expected

    @pytest.mark.parametrize("source_type", [DataType.INT, DataType.DOUBLE, None])
    def test_non_boolean_sum_unchanged(self, source_type: Optional[DataType]) -> None:
        assert classify_aggregation(
            metric_name="amount", aggregation="sum", source_type=source_type,
        ) is AggregationValueClass.PRESERVING


class TestColumnTyping:
    def test_aggregated_type(self) -> None:
        types = {
            agg: aggregated_type(dataset=ORDERS, metric_name="flag", aggregation=agg)
            for agg in ("sum", "avg", "min", "max", "count", "first")
        }
        assert types == {
            "sum": DataType.INT, "avg": DataType.DOUBLE, "min": DataType.BOOLEAN,
            "max": DataType.BOOLEAN, "count": DataType.INT, "first": DataType.BOOLEAN,
        }
        assert aggregated_type(dataset=ORDERS, metric_name="big_order", aggregation="sum") is DataType.INT
        assert aggregated_type(dataset=ORDERS, metric_name="amount", aggregation="sum") is DataType.INT

    def test_sum_format_is_integer(self) -> None:
        assert _infer_aggregated_format(dataset=ORDERS, metric_name="flag", aggregation="sum") == INTEGER
        assert _infer_aggregated_format(dataset=ORDERS, metric_name="amount", aggregation="sum") is None

    def test_avg_format_percent_unless_column_declares_one(self) -> None:
        assert _infer_aggregated_format(dataset=ORDERS, metric_name="flag", aggregation="avg") == PERCENT
        fmt = NumberFormat(type=NumberFormatType.FLOAT, precision=3)
        model = orders_model(flag_format=fmt)
        assert _infer_aggregated_format(dataset=model, metric_name="flag", aggregation="avg") == fmt


class TestMeasureKeyType:
    @pytest.mark.parametrize("text,expected", [
        ("sum(flag)", DataType.INT),
        ("avg(flag)", DataType.DOUBLE),
        ("sum(big_order)", DataType.INT),
        ("sum(coalesce(flag, false))", DataType.INT),
        ("avg(coalesce(flag, false))", DataType.DOUBLE),
        ("max(coalesce(flag, false))", DataType.BOOLEAN),
        ("sum(amount > 15)", DataType.INT),
        ("avg(amount > 15)", DataType.DOUBLE),
        ("min(amount > 15)", DataType.BOOLEAN),
        ("count(amount > 15)", DataType.INT),
        ("sum(status in ('ok', 'hold'))", DataType.INT),
        ("median(flag)", DataType.DOUBLE),
        ("stddev_samp(amount > 15)", DataType.DOUBLE),
        ("sum(True)", DataType.INT),
    ])
    def test_boolean_sources(self, text: str, expected: DataType) -> None:
        assert metric_key_type(dataset=ORDERS, key=_bind(text), bundle=BUNDLE) is expected

    def test_mixed_coalesce_is_not_boolean_typed(self) -> None:
        """Scenario: mixed-type coalesce is not boolean — the boolean branch reads as its integer."""
        key = _bind("sum(coalesce(flag, amount))")
        assert metric_key_type(dataset=ORDERS, key=key, bundle=BUNDLE) is DataType.INT


class TestStageMeasureType:
    SCHEMA = StageSchema(relation_name="s1", fields=[
        StageField(name="fm", sql_alias="fm", datatype=DataType.BOOLEAN),
    ])

    def test_boolean_stage_column(self) -> None:
        types = {
            agg: stage_metric_type(AggregateKey(source=FieldKey(leaf="fm"), agg=agg), schema=self.SCHEMA)
            for agg in ("sum", "avg", "max", "count")
        }
        assert types == {
            "sum": DataType.INT, "avg": DataType.DOUBLE, "max": DataType.BOOLEAN, "count": DataType.INT,
        }


class TestBooleanDefaults:
    def test_boolean_default_set_is_the_numeric_set(self) -> None:
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.BOOLEAN] == DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INT]


def _facade_model() -> SlayerDataset:
    return SlayerDataset(
        name="facts", sql_table="facts", data_source="test", default_time_dimension="at",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="flag", datatype=DataType.BOOLEAN),
            DatasetField(name="qty", datatype=DataType.INT),
            DatasetField(name="price", datatype=DataType.DOUBLE),
            DatasetField(name="label", datatype=DataType.TEXT),
            DatasetField(name="at", datatype=DataType.TIMESTAMP),
        ],
        aggregations=[Aggregation(name="wsum", expression="SUM({value})")],
    )


class TestFacadeAgreement:
    """Scenario: facade agrees with the engine (custom aggregations included)."""

    def test_every_metric_type_matches_engine(self) -> None:
        model = _facade_model()
        _, metrics = build_local_view(model)
        by_name = {mt.name: mt for mt in metrics}
        aggs = set().union(*DEFAULT_AGGREGATIONS_BY_TYPE.values()) | {"wsum"}
        checked = 0
        for col in model.fields:
            for agg in sorted(aggs):
                metric = by_name.get(f"{col.name}_{agg}")
                if metric is None:
                    continue
                expected = aggregated_type(dataset=model, metric_name=col.name, aggregation=agg)
                assert metric.data_type == expected, (col.name, agg, metric.data_type, expected)
                checked += 1
        assert checked > 0
        assert "flag_avg" in by_name
        assert by_name["flag_sum"].data_type is DataType.INT
        assert by_name["flag_wsum"].data_type is None
