"""Generator scope-render guards (design decision 15). The
one renderer (`_render_expression_source_sql` via `scope.resolve`) must register
a constituent's join exactly ONCE; the risk is a derived-field source that
crosses a relationship being rendered twice (private resolver + kwarg join-registration
pass). These lock the invariant across the §5.4 re-cut.

Spec: openspec …/specs/queries/partitioned-aggregates — "Mixed sources carry the
full expression-source surface" (cross-dataset constituents).
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from slayer.core.query import QueryDimension, QueryMetric

from tests._dev1847_fixtures import (
    DatasetField,
    DataType,
    SlayerQuery,
    corders_dataset,
    customers_dataset,
    gen,
    regions_dataset,
    sales_dataset,
)
from tests._engine_helpers import _engine_generate

_CROSS_MIXED = ("sum(customers.region_id * sum(corders.amount, partition_by=customers.region_id))")


def _join_count_to(sql: str, table: str) -> int:
    tree = sqlglot.parse_one(sql, read="postgres")
    return sum(1 for j in tree.find_all(exp.Join)
               if isinstance(j.this, exp.Table) and j.this.name == table)


def _derived_ref_datasets():
    """corders.cust_rid is a DERIVED field reading customers.rid1 — itself a
    DERIVED field — across the corders→customers relationship."""
    sales, regions, customers, corders = (
        sales_dataset(), regions_dataset(), customers_dataset(), corders_dataset())
    customers.fields.append(
        DatasetField(name="rid1", datatype=DataType.FLOAT, expression="region_id * 1"))
    corders.fields.append(
        DatasetField(name="cust_rid", datatype=DataType.FLOAT, expression="customers.rid1"))
    return [corders, customers, regions, sales]


class TestJoinEmittedOnce:
    async def test_cross_model_constituent_producer_emitted_once(self):
        sql = await gen(SlayerQuery(
            population="customers", dimensions=[QueryDimension(field="customers.region_id")],
            metrics=[QueryMetric(metric=_CROSS_MIXED, alias="xm")]))
        producers = [n for n in re.findall(r"(_cm_\w+) AS \(", sql)
                     if "amount" in n]
        assert len(producers) == 1, sql
        # and the crossing corders→customers join is emitted once inside it, not
        # re-rendered by a second resolver.
        assert _join_count_to(sql, "customers") == 1, sql
        assert_scope_closed(sql)

    async def test_derived_referencing_derived_across_join_once(self):
        """The join carrying the derived-of-derived constituent is emitted once
        with the allocator's alias — never re-rendered by a second resolver."""
        models = _derived_ref_datasets()
        sql = await _engine_generate(
            query=SlayerQuery(
                population="corders",
                metrics=[QueryMetric(
                    metric="sum(corders.amount * avg(corders.cust_rid, partition_by=corders.customer_id))",
                    alias="m")]),
            dataset=models[0], extra_datasets=models[1:],
            dialect="postgres", validate=False)
        assert _join_count_to(sql, "customers") == 1, sql
        assert_scope_closed(sql)
