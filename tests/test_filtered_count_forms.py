"""Filtered-aggregate count semantics.

The MetricFlow filter push-down (importer Part 3.4) relies on SLayer's
``DatasetField.filter`` emitting the correct CASE-inside-aggregate forms for count
aggregations:

  col:count           -> COUNT(CASE WHEN f THEN col END)
  col:count_distinct  -> COUNT(DISTINCT CASE WHEN f THEN col END)
  col:sum             -> SUM(CASE WHEN f THEN col END)

These are regression guards: a future change to the filter wrapper that broke
count semantics would silently corrupt every filtered metric.

``region`` is referenced by the filters but not declared as a dataset field.
Since every Mode-A fragment enters through the one door, such a reference is
qualified against the scope root (``orders.region``) like any other — the old
path qualified only DECLARED fields and left this one bare, which bound it to
whatever table happened to be in scope once the filter was re-rendered inside a
rerooted CTE. The subject of these guards is the CASE-inside-aggregate shape,
which is unchanged.
"""

from __future__ import annotations

import re

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery

from tests._engine_helpers import _engine_generate


async def _gen(expression: str) -> str:
    model = SlayerDataset(
        name="orders", sql_table="orders", data_source="t",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amt", expression="amount", datatype=DataType.FLOAT, filter="region = 'US'"),
            DatasetField(name="cust", expression="customer_id", datatype=DataType.INTEGER, filter="region = 'US'"),
        ],
    )
    q = SlayerQuery(population="orders", metrics=[QueryMetric(metric=expression)])
    return await _engine_generate(query=q, dataset=model, dialect="postgres")


async def test_filtered_count_uses_case_inside_count() -> None:
    sql = (await _gen("count(orders.cust)")).upper().replace(" ", "")
    assert "COUNT(CASEWHENORDERS.REGION='US'THENORDERS.CUSTOMER_IDEND)" in sql


async def test_filtered_count_distinct_uses_case_inside_distinct() -> None:
    sql = (await _gen("count_distinct(orders.cust)")).upper().replace(" ", "")
    assert "COUNT(DISTINCTCASEWHENORDERS.REGION='US'THENORDERS.CUSTOMER_IDEND)" in sql


async def test_filtered_sum_uses_case_inside_sum() -> None:
    sql = (await _gen("sum(orders.amt)")).upper().replace(" ", "")
    assert "SUM(CASEWHENORDERS.REGION='US'THENORDERS.AMOUNTEND)" in sql


async def test_filtered_count_distinct_approx_wraps_case_in_exact_fallback() -> None:
    # count_distinct_approx routes through its own builder, which must
    # compose with the filter CASE wrapper. Postgres has no native approximate
    # distinct, so the exact COUNT(DISTINCT ...) fallback is emitted. The
    # filter rides the source field's expansion (no _wrap_filter parens), so
    # the fallback emits COUNT(DISTINCT CASE WHEN ... THEN col END).
    sql = re.sub(r"\s+", "", (await _gen("count_distinct_approx(orders.cust)")).upper())
    assert "COUNT(DISTINCTCASEWHENORDERS.REGION='US'THENORDERS.CUSTOMER_IDEND)" in sql
