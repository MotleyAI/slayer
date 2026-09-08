"""DEV-1865 — order targets compile through the same typing pass as filters.

Covers the order-position gaps: opposite-direction MIN/MAX duplicates on one
column, NULLs-last on every backend, hidden order-only slots trimmed from the
result, and ordering by a cross-model partitioned aggregate. Run on
SQLite + DuckDB.
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import ModelMeasure, q

RK_REGION = "orders.customers.regions.name"
RK_TIER = "orders.customers.tier"


class TestDuplicateOppositeDirection:
    async def test_min_and_max_wrap_the_same_column(self, exec_engine) -> None:
        # ASC wraps per-group MIN, DESC per-group MAX — two hidden slots for one
        # column must coexist, and neither leaks into the projection.
        query = q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[
                {"column": "ordered_at", "direction": "asc"},
                {"column": "ordered_at", "direction": "desc"},
            ],
        )
        resp = await exec_engine.execute(query)
        assert resp.columns == ["orders.region", "orders.s"]
        sql = (await exec_engine.execute(query, dry_run=True)).sql
        assert sql is not None
        order_by = sql.upper().rsplit("ORDER BY", 1)[1]
        min_pos = order_by.index("ORDERED_AT_MIN")
        max_pos = order_by.index("ORDERED_AT_MAX")
        assert min_pos < max_pos
        assert "ASC" in order_by[min_pos:max_pos]
        assert "DESC" in order_by[max_pos:]


class TestNullsSortLast:
    @pytest.mark.parametrize("direction", ["asc", "desc"])
    async def test_null_dimension_values_sort_last(self, exec_engine, direction) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["city"],
            distinct_dimension_values=False,
            order=[{"column": "city", "direction": direction}],
        ))
        cities = [r["orders.city"] for r in resp.data]
        assert None in cities
        # Every NULL is at the tail, in both directions.
        first_null = cities.index(None)
        assert all(c is None for c in cities[first_null:])


class TestOrderOnlyTrimmed:
    async def test_order_by_undeclared_aggregate_adds_no_column(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": "amount:min", "direction": "asc"}],
        ))
        assert resp.columns == ["orders.region", "orders.s"]


class TestOrderByCrossModelPartitioned:
    async def test_sorts_by_partition_value_and_stays_hidden(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.regions.name", "customers.tier"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
            order=[{
                "column": "customers.spend:sum(partition_by=customers.regions.name)",
                "direction": "desc",
            }],
        ))
        assert RK_REGION in resp.columns
        assert "orders.sp" in resp.columns
        # The partitioned value is hidden, not projected.
        assert not any("partition" in c for c in resp.columns)
        # RegN partition total (300) sorts before RegS (50) under desc.
        regions = [r[RK_REGION] for r in resp.data]
        assert regions == sorted(regions, key=lambda r: {"RegN": 0, "RegS": 1}[r])
