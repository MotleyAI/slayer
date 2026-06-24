"""DEV-1595: filtered-aggregate count semantics.

The MetricFlow filter push-down (importer Part 3.4) relies on SLayer's
``Column.filter`` emitting the correct CASE-inside-aggregate forms for count
aggregations:

  col:count           -> COUNT(CASE WHEN f THEN col END)
  col:count_distinct  -> COUNT(DISTINCT CASE WHEN f THEN col END)
  col:sum             -> SUM(CASE WHEN f THEN col END)

These are regression guards: a future change to the filter wrapper that broke
count semantics would silently corrupt every filtered metric.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enrichment import enrich_query
from slayer.sql.generator import SQLGenerator


async def _noop(**k):
    return None


async def _gen(formula: str) -> str:
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="t",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="amt", sql="amount", type=DataType.DOUBLE, filter="region = 'US'"),
            Column(name="cust", sql="customer_id", type=DataType.INT, filter="region = 'US'"),
        ],
    )
    q = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula=formula)])
    enriched = await enrich_query(
        query=q, model=model, resolve_dimension_via_joins=_noop,
        resolve_cross_model_measure=_noop, resolve_join_target=_noop,
    )
    return SQLGenerator(dialect="postgres").generate(enriched=enriched)


async def test_filtered_count_uses_case_inside_count() -> None:
    sql = (await _gen("cust:count")).upper().replace(" ", "")
    assert "COUNT(CASEWHENREGION='US'THENORDERS.CUSTOMER_IDEND)" in sql


async def test_filtered_count_distinct_uses_case_inside_distinct() -> None:
    sql = (await _gen("cust:count_distinct")).upper().replace(" ", "")
    assert "COUNT(DISTINCTCASEWHENREGION='US'THENORDERS.CUSTOMER_IDEND)" in sql


async def test_filtered_sum_uses_case_inside_sum() -> None:
    sql = (await _gen("amt:sum")).upper().replace(" ", "")
    assert "SUM(CASEWHENREGION='US'THENORDERS.AMOUNTEND)" in sql
