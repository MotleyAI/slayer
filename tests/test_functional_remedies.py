"""Error remedies name the functional aggregation spelling, never the legacy colon one."""

import re

import pytest
from pydantic import ValidationError

from slayer.core.errors import DerivedFieldFanningError
from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.engine.field_dependency import _unproven_arity_message
from slayer.core.syntax import split_entity_agg_ref
from slayer.sql.generator import AggRenderSpec, SQLGenerator
from slayer.core.query import QueryMetric

from tests._dev1832_fixtures import gen, orders_q
from tests._engine_helpers import orders_agg_sql

_COLON_AGG = re.compile(r"[\w*)\]>]:(?:<\w+>|[a-z_]+\b)")


def _assert_no_colon_agg(msg: str) -> None:
    assert not _COLON_AGG.search(msg), msg


def test_derived_column_fanning_remedy() -> None:
    msg = str(DerivedFieldFanningError(
        field="li_qty", dataset="orders", hop="line_items", reference="orders.line_items.qty",
    ))
    assert "<aggregation>(orders.line_items.qty)" in msg
    _assert_no_colon_agg(msg)


def test_unproven_arity_remedy() -> None:
    msg = _unproven_arity_message(field="li_qty", dataset="orders", hop="line_items")
    assert "(<aggregation>(line_items.<field>))" in msg
    _assert_no_colon_agg(msg)


async def test_cross_hop_source_remedy() -> None:
    query = orders_q(metrics=[
        QueryMetric(metric="sum(amount - customers.regions.bad_pop)", alias="m")])
    with pytest.raises(ValueError, match="Aggregate the target field directly") as ei:
        await gen(query)
    msg = str(ei.value)
    assert re.search(r"\(<aggregation>\([\w.]+\.<field>\)\)", msg), msg
    _assert_no_colon_agg(msg)


async def test_missing_parameter_remedy_at_bind() -> None:
    with pytest.raises(ValueError, match="requires parameter 'p'") as ei:
        await orders_agg_sql("percentile(amount)")
    msg = str(ei.value)
    assert "'percentile(metric, p=field)'" in msg
    _assert_no_colon_agg(msg)


def test_missing_parameter_remedy_in_generator() -> None:
    spec = AggRenderSpec(
        name="amount", sql="amount", dataset_name="orders", alias="amount_percentile",
        aggregation="percentile", agg_kwargs={},
    )
    generator = SQLGenerator(dialect="postgres")
    with pytest.raises(ValueError, match="requires parameter 'p'") as ei:
        generator._build_percentile(spec)
    msg = str(ei.value)
    assert "'percentile(metric, p=field)'" in msg
    _assert_no_colon_agg(msg)


def test_entity_ref_expression_remedy() -> None:
    with pytest.raises(ValueError) as ei:
        split_entity_agg_ref("sum(a - b)")
    msg = str(ei.value)
    assert "`agg(field)`" in msg
    assert "field:agg" not in msg


def test_colon_in_name_reason() -> None:
    with pytest.raises(ValidationError) as ei:
        DatasetField(name="a:b", sql="x")
    msg = str(ei.value)
    assert "legacy aggregation separator" in msg
    assert "revenue:sum" not in msg


@pytest.mark.parametrize("formula", ["customers.revenue", "cumsum(customers.revenue)"])
def test_cross_model_bare_measure_remedy(formula: str) -> None:
    model = SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="customer_id", datatype=DataType.INTEGER)],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        metrics=[DatasetMetric(name="m", expression=formula)],
    )
    conv = DbtToSlayerConverter(project=DbtProject(semantic_models=[], metrics=[]), data_source="test")
    conv._prune_dataset_metrics(model)
    [warning] = conv._unconverted
    msg = warning.message
    assert "'sum(customers.revenue)'" in msg
    _assert_no_colon_agg(msg)
