"""DEV-1847 task 1.4/4.2 — nested producers compose to arbitrary depth (SQLite +
DuckDB). Depth-three executed values, one flat WITH with closed scopes and no
placeholder leak, and a producer consumed at two depths that renders once.
Fails until the depth-1 / strict-subset arms of ``_validate_nested_producer_plan``
are lifted.

Spec: openspec …/specs/queries/partitioned-aggregates — "Nested producers
compose to arbitrary depth".
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
import pytest

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1847_fixtures import (
    DEPTH3_MAX_AVG_BY_PRODUCT,
    INNER_CR,
    INNER_CRP,
    ModelMeasure,
    SHAPE_B_ACR,
    SHAPE_B_GROUP_SUM,
    SPEND_BAND_EXPR,
    gen,
    make_exec_engine,
    reagg,
    rows_by,
    sales_q,
)

DEPTH3 = f"max(avg({INNER_CRP}, partition_by=[region, product]))"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _grain_producer_count(sql: str, *, dialect: str, grain=("city", "region")) -> int:
    """Number of CTEs whose GROUP BY keys cover every token in ``grain``
    (substring match survives alias mangling like ``sales___city``)."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    count = 0
    for cte in tree.find_all(exp.CTE):
        select = cte.this
        group = select.args.get("group") if isinstance(select, exp.Select) else None
        if group is None:
            continue
        keys = " ".join(col.sql().lower() for col in group.expressions)
        if all(token in keys for token in grain):
            count += 1
    return count


class TestDepthThree:
    async def test_depth_three_executed_values(self, exec_engine):
        """Scenario: Depth-three re-aggregation — per product, the maximum over
        regions of the average over cities, by hand-computed values."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["product"],
            measures=[ModelMeasure(formula=DEPTH3, name="d3")]))
        by = rows_by(resp, "sales.product")
        for product, expected in DEPTH3_MAX_AVG_BY_PRODUCT.items():
            assert float(by[(product,)]["sales.d3"]) == pytest.approx(expected)

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb", "postgres"])
    async def test_depth_three_sql_is_flat_closed_and_leak_free(self, dialect):
        """One flat WITH, closed scopes, and no ``__regroup__`` placeholder leak
        at depth three, on each emission path. ``gen`` already asserts the SQL
        parses with no nested WITH (``_assert_valid_sql``)."""
        sql = await gen(sales_q(
            dimensions=["product"],
            measures=[ModelMeasure(formula=DEPTH3, name="d3")]), dialect=dialect)
        assert "__regroup__" not in sql, sql
        assert_scope_closed(sql, dialect=dialect)


class TestSharedProducerRendersOnce:
    async def test_one_producer_two_depths_renders_once(self, exec_engine):
        """Scenario: One producer consumed at two depths renders once — a
        computed dimension bands the city-region total AND a measure
        re-aggregates it; a single city-region producer serves both."""
        band = {"expression": SPEND_BAND_EXPR, "name": "spend_band"}
        both = sales_q(
            dimensions=["region", band],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      reagg("avg", INNER_CR, name="acr")])
        resp = await exec_engine.execute(both)
        by = rows_by(resp, "sales.region", "sales.spend_band")
        # BOTH consuming depths produce correct values: the row-attach band sum
        # (tot) and the re-aggregated avg of city totals (acr).
        for (region, label), total in SHAPE_B_GROUP_SUM.items():
            cell = by[(region, label)]["sales.tot"]
            assert (cell is None) if total is None else float(cell) == pytest.approx(total)
        for (region, label), acr in SHAPE_B_ACR.items():
            cell = by[(region, label)]["sales.acr"]
            assert (cell is None) if acr is None else float(cell) == pytest.approx(acr)

    async def test_producer_not_duplicated_by_second_consumer(self, exec_engine):
        """The re-aggregating measure reuses the banded dimension's producer —
        the city-region producer CTE count does not grow with the second
        consumer (interning, DEV-1838)."""
        band = {"expression": SPEND_BAND_EXPR, "name": "spend_band"}
        dialect = "postgres"
        single = sales_q(dimensions=["region", band],
                         measures=[ModelMeasure(formula="amount:sum", name="tot")])
        both = sales_q(dimensions=["region", band],
                       measures=[ModelMeasure(formula="amount:sum", name="tot"),
                                 reagg("avg", INNER_CR, name="acr")])
        s_sql = await gen(single, dialect=dialect)
        b_sql = await gen(both, dialect=dialect)
        s_count = _grain_producer_count(s_sql, dialect=dialect)
        # Interning: the re-aggregating measure reuses the banded dimension's
        # city-region producer, so the second consumer adds no new one. Relative
        # (b == s) rather than absolute, to survive any substring miscount.
        assert s_count >= 1
        assert _grain_producer_count(b_sql, dialect=dialect) == s_count
