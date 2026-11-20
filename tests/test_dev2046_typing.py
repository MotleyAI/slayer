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
from slayer.core.query import SlayerQuery
from slayer.core.scope import DatasetScope, StageField, StageSchema
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import aggregated_type, metric_key_type
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.core.syntax import parse_expr
from slayer.facade.catalog import build_local_view
from slayer.ir.source_bundle import ResolvedSourceBundle, dataset_from_stage_schema

from tests._dev2046_fixtures import customers_dataset, orders_dataset

ORDERS = orders_dataset()
CUSTOMERS = customers_dataset()
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

    @pytest.mark.parametrize("source_type", [DataType.INTEGER, DataType.FLOAT, None])
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
            "sum": DataType.INTEGER, "avg": DataType.FLOAT, "min": DataType.BOOLEAN,
            "max": DataType.BOOLEAN, "count": DataType.INTEGER, "first": DataType.BOOLEAN,
        }
        assert aggregated_type(dataset=ORDERS, metric_name="big_order", aggregation="sum") is DataType.INTEGER
        assert aggregated_type(dataset=ORDERS, metric_name="amount", aggregation="sum") is DataType.INTEGER

    def test_sum_format_is_integer(self) -> None:
        assert _infer_aggregated_format(dataset=ORDERS, metric_name="flag", aggregation="sum") == INTEGER
        assert _infer_aggregated_format(dataset=ORDERS, metric_name="amount", aggregation="sum") is None

    def test_avg_format_percent_unless_column_declares_one(self) -> None:
        assert _infer_aggregated_format(dataset=ORDERS, metric_name="flag", aggregation="avg") == PERCENT
        fmt = NumberFormat(type=NumberFormatType.FLOAT, precision=3)
        model = orders_dataset(flag_format=fmt)
        assert _infer_aggregated_format(dataset=model, metric_name="flag", aggregation="avg") == fmt


class TestMeasureKeyType:
    @pytest.mark.parametrize("text,expected", [
        ("sum(flag)", DataType.INTEGER),
        ("avg(flag)", DataType.FLOAT),
        ("sum(big_order)", DataType.INTEGER),
        ("sum(coalesce(flag, false))", DataType.INTEGER),
        ("avg(coalesce(flag, false))", DataType.FLOAT),
        ("max(coalesce(flag, false))", DataType.BOOLEAN),
        ("sum(amount > 15)", DataType.INTEGER),
        ("avg(amount > 15)", DataType.FLOAT),
        ("min(amount > 15)", DataType.BOOLEAN),
        ("count(amount > 15)", DataType.INTEGER),
        ("sum(status in ('ok', 'hold'))", DataType.INTEGER),
        ("median(flag)", DataType.FLOAT),
        ("stddev_samp(amount > 15)", DataType.FLOAT),
        ("sum(True)", DataType.INTEGER),
    ])
    def test_boolean_sources(self, text: str, expected: DataType) -> None:
        assert metric_key_type(dataset=ORDERS, key=_bind(text), bundle=BUNDLE) is expected

    def test_mixed_coalesce_is_not_boolean_typed(self) -> None:
        """Scenario: mixed-type coalesce is not boolean — the boolean branch reads as its integer."""
        key = _bind("sum(coalesce(flag, amount))")
        assert metric_key_type(dataset=ORDERS, key=key, bundle=BUNDLE) is DataType.INTEGER


class TestStageMeasureType:
    SCHEMA = StageSchema(relation_name="s1", fields=[
        StageField(name="fm", sql_alias="fm", datatype=DataType.BOOLEAN),
    ])

    def test_boolean_stage_column(self) -> None:
        types = {
            agg: metric_key_type(
                dataset=dataset_from_stage_schema(name="s1", schema=self.SCHEMA, data_source="test"),
                key=AggregateKey(source=FieldKey(leaf="fm"), agg=agg))
            for agg in ("sum", "avg", "max", "count")
        }
        assert types == {
            "sum": DataType.INTEGER, "avg": DataType.FLOAT, "max": DataType.BOOLEAN, "count": DataType.INTEGER,
        }


class TestBooleanDefaults:
    def test_boolean_default_set_is_the_numeric_set(self) -> None:
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.BOOLEAN] == DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INTEGER]


def _facade_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="facts", sql_table="facts", data_source="test", default_time_dimension="at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="flag", datatype=DataType.BOOLEAN),
            DatasetField(name="qty", datatype=DataType.INTEGER),
            DatasetField(name="price", datatype=DataType.FLOAT),
            DatasetField(name="label", datatype=DataType.STRING),
            DatasetField(name="at", datatype=DataType.DATETIME),
        ],
        aggregations=[Aggregation(name="wsum", expression="SUM({value})")],
    )


def _plan_type(model: SlayerDataset, expression: str) -> Optional[DataType]:
    """The query plan's type of ``expression`` as the one metric of a query over ``model``."""
    query = SlayerQuery.model_validate({"population": model.name, "metrics": [{"metric": expression, "alias": "x"}]})
    prebound = bind_query_inputs(query=query, bundle=ResolvedSourceBundle(dialect="postgres", population=model))
    (declared,) = [dm for dm in prebound.declared_metrics if dm.public_name == "x"]
    return declared.datatype


class TestFacadeAgreement:
    """Scenario: facade agrees with the engine (custom aggregations included)."""

    def test_every_metric_type_matches_engine(self) -> None:
        model = _facade_dataset()
        _, metrics = build_local_view(model)
        by_name = {mt.name: mt for mt in metrics}
        aggs = set().union(*DEFAULT_AGGREGATIONS_BY_TYPE.values()) | {"wsum"}
        checked = 0
        for col in (c for c in model.fields if not c.primary_key):  # the plan refuses aggregating a key
            for agg in sorted(aggs):
                metric = by_name.get(f"{col.name}_{agg}")
                if metric is None:
                    continue
                expected = _plan_type(model, f"{agg}(facts.{col.name})")
                assert metric.data_type == expected, (col.name, agg, metric.data_type, expected)
                checked += 1
        assert checked > 0
        assert "flag_avg" in by_name
        assert by_name["flag_sum"].data_type is DataType.INTEGER
        assert by_name["flag_wsum"].data_type is DataType.INTEGER
