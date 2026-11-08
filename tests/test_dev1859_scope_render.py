"""DEV-1859 task 4.6 — generator scope-render guards (design decision 15). The
one renderer (`_render_expression_source_sql` via `scope.resolve`) must register
a constituent's join exactly ONCE; the risk is a derived-column source that
crosses a join being rendered twice (private resolver + kwarg join-registration
pass). These lock the invariant across the §5.4 re-cut.

Spec: openspec …/specs/queries/partitioned-aggregates — "Mixed sources carry the
full expression-source surface" (cross-model constituents).
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1847_fixtures import (
    Column,
    ColumnRef,
    DataType,
    ModelMeasure,
    SlayerQuery,
    corders_model,
    customers_model,
    gen,
    regions_model,
    sales_model,
)
from tests._engine_helpers import _engine_generate

_CROSS_MIXED = ("sum(region_id * sum(corders.amount, partition_by=region_id))")


def _join_count_to(sql: str, table: str) -> int:
    tree = sqlglot.parse_one(sql, read="postgres")
    return sum(1 for j in tree.find_all(exp.Join)
               if isinstance(j.this, exp.Table) and j.this.name == table)


def _derived_ref_models():
    """corders.cust_rid is a DERIVED column reading customers.rid1 — itself a
    DERIVED column — across the corders→customers join."""
    sales, regions, customers, corders = (
        sales_model(), regions_model(), customers_model(), corders_model())
    customers.columns.append(
        Column(name="rid1", type=DataType.DOUBLE, sql="region_id * 1"))
    corders.columns.append(
        Column(name="cust_rid", type=DataType.DOUBLE, sql="customers.rid1"))
    return [corders, customers, regions, sales]


class TestJoinEmittedOnce:
    async def test_cross_model_constituent_producer_emitted_once(self):
        sql = await gen(SlayerQuery(
            source_model="customers", dimensions=[ColumnRef(name="region_id")],
            measures=[ModelMeasure(formula=_CROSS_MIXED, name="xm")]))
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
        models = _derived_ref_models()
        sql = await _engine_generate(
            query=SlayerQuery(
                source_model="corders",
                measures=[ModelMeasure(
                    formula="sum(amount * avg(cust_rid, partition_by=customer_id))",
                    name="m")]),
            model=models[0], extra_models=models[1:],
            dialect="postgres", validate=False)
        assert _join_count_to(sql, "customers") == 1, sql
        assert_scope_closed(sql)
