"""Error remedies name the functional aggregation spelling, never the legacy colon one."""

import re

import pytest
from pydantic import ValidationError

from slayer.core.errors import DerivedColumnFanningError
from slayer.core.formula import parse_formula
from slayer.core.models import Column, ModelMeasure
from slayer.engine.syntax import split_entity_agg_ref
from slayer.sql.generator import AggRenderSpec, SQLGenerator
from tests._dev1832_fixtures import gen, orders_q
from tests._engine_helpers import orders_agg_sql

_COLON_AGG = re.compile(r"[\w*)\]>]:(?:<\w+>|[a-z_]+\b)")


def _assert_no_colon_agg(msg: str) -> None:
    assert not _COLON_AGG.search(msg), msg


def test_derived_column_fanning_remedy() -> None:
    msg = str(DerivedColumnFanningError(
        column="li_qty", model="orders", hop="line_items", reference="orders.line_items.qty",
    ))
    assert "<aggregation>(orders.line_items.qty)" in msg
    _assert_no_colon_agg(msg)


async def test_cross_hop_source_remedy() -> None:
    query = orders_q(measures=[
        ModelMeasure(formula="sum(amount - customers.regions.bad_pop)", name="m")])
    with pytest.raises(ValueError, match="Aggregate the target column directly") as ei:
        await gen(query)
    msg = str(ei.value)
    assert re.search(r"\(<aggregation>\([\w.]+\.<column>\)\)", msg), msg
    _assert_no_colon_agg(msg)


async def test_missing_parameter_remedy_at_bind() -> None:
    with pytest.raises(ValueError, match="requires parameter 'p'") as ei:
        await orders_agg_sql("percentile(amount)")
    msg = str(ei.value)
    assert "'percentile(measure, p=column)'" in msg
    _assert_no_colon_agg(msg)


def test_missing_parameter_remedy_in_generator() -> None:
    spec = AggRenderSpec(
        name="amount", sql="amount", model_name="orders", alias="amount_percentile",
        aggregation="percentile", agg_kwargs={},
    )
    with pytest.raises(ValueError, match="requires parameter 'p'") as ei:
        SQLGenerator(dialect="postgres")._build_percentile(spec)
    msg = str(ei.value)
    assert "'percentile(measure, p=column)'" in msg
    _assert_no_colon_agg(msg)


def test_entity_ref_expression_remedy() -> None:
    with pytest.raises(ValueError) as ei:
        split_entity_agg_ref("sum(a - b)")
    msg = str(ei.value)
    assert "`agg(column)`" in msg
    assert "column:agg" not in msg


def test_colon_in_name_reason() -> None:
    with pytest.raises(ValidationError) as ei:
        Column(name="a:b", sql="x")
    msg = str(ei.value)
    assert "legacy aggregation separator" in msg
    assert "revenue:sum" not in msg


@pytest.mark.parametrize("formula", ["customers.revenue", "cumsum(customers.revenue)"])
def test_cross_model_bare_measure_remedy(formula: str) -> None:
    with pytest.raises(ValueError) as ei:
        parse_formula(formula)
    msg = str(ei.value)
    assert "'sum(customers.revenue)'" in msg
    _assert_no_colon_agg(msg)
