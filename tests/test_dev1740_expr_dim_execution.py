"""DEV-1740 Part B1 — row-level expression dimensions (no aggregate inside).

These need no second pass: the expression goes straight into SELECT + GROUP BY.
Executed values on SQLite AND DuckDB, the no-second-pass SQL shape, name
references from order / filters, dedup under both ``distinct_dimension_values``
settings, and fail-closed name collision.
"""

from __future__ import annotations

import pytest

from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1740_fixtures import gen, make_exec_engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


BIG_SMALL = {"big": 22000.0, "small": 3000.0}
LOWER_CITY_TOTAL = {"paris": 7000.0, "berlin": 9000.0, "lyon": 1000.0,
                    "nyc": 2000.0, None: 6000.0}


class TestRowLevelValues:
    async def test_scalar_expression_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": "lower(city)", "name": "lc"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        got = {r["orders.lc"]: float(r["orders.rev"]) for r in resp.data}
        assert got == {k: pytest.approx(v) for k, v in LOWER_CITY_TOTAL.items()}

    async def test_case_band_over_plain_column(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[{
                "expression": "CASE WHEN amount > 2000 THEN 'big' ELSE 'small' END",
                "name": "sz",
            }],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        got = {r["orders.sz"]: float(r["orders.rev"]) for r in resp.data}
        assert got == {k: pytest.approx(v) for k, v in BIG_SMALL.items()}


class TestNoSecondPass:
    async def test_row_level_dim_emits_no_isolated_regroup_cte(self) -> None:
        sql = await gen(_q(
            dimensions=["region", {"expression": "lower(city)", "name": "lc"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        # ``_cm_`` is the isolated-aggregate (regroup) CTE prefix used by the B2
        # desugar and DEV-1739. A purely row-level expression dimension must not
        # trigger it — pinned so a needless extra scan is caught.
        assert "_cm_" not in sql
        assert "IS NOT DISTINCT FROM" not in sql.upper()
        assert "CASE" not in sql.upper()  # lower(city) is a plain scalar


class TestJoinedColumnInExpression:
    async def test_computed_dim_over_joined_column_pulls_the_join(self) -> None:
        # The expression crosses orders → customers; the base FROM must LEFT
        # JOIN customers (a throwaway-scope render dropped the registration).
        sql = await gen(_q(
            dimensions=[{"expression": "upper(customers.tier)", "name": "T"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        assert "JOIN" in sql.upper()
        assert "customers" in sql

    async def test_computed_dim_over_joined_column_executes(self, exec_engine) -> None:
        # gold = customers 1 & 3 → orders 1,2,5,6 = 8000; silver = customers
        # 2 & 4 → orders 3,4,7,8,9 = 17000.
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": "upper(customers.tier)", "name": "T"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        ))
        got = {r["orders.T"]: float(r["orders.rev"]) for r in resp.data}
        assert got == {"GOLD": pytest.approx(8000.0), "SILVER": pytest.approx(17000.0)}


class TestNameReferences:
    async def test_order_by_computed_name(self, exec_engine) -> None:
        # Order by the COMPUTED dimension's name (not a measure).
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": "lower(city)", "name": "lc"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            order=[{"column": "lc", "direction": "asc"}],
        ))
        lcs = [r["orders.lc"] for r in resp.data if r["orders.lc"] is not None]
        assert lcs == sorted(lcs)

    async def test_filter_by_computed_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": "lower(city)", "name": "lc"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            filters=["lc == 'paris'"],
        ))
        assert len(resp.data) == 1
        assert resp.data[0]["orders.lc"] == "paris"
        assert float(resp.data[0]["orders.rev"]) == pytest.approx(7000.0)


class TestDedupInteraction:
    async def test_dim_only_dedups_computed_values(self, exec_engine) -> None:
        # No measures: distinct computed values (5 cities incl. the NULL group).
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": "lower(city)", "name": "lc"}],
        ))
        assert len(resp.data) == 5

    async def test_ddv_false_projects_per_row(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=[{
                "expression": "CASE WHEN amount > 2000 THEN 'big' ELSE 'small' END",
                "name": "sz",
            }],
            distinct_dimension_values=False,
        ))
        assert len(resp.data) == 9  # one row per source order


class TestCollisionFailsClosed:
    async def test_name_colliding_with_a_column_raises(self) -> None:
        # ``channel`` is a real column (a legal plain dimension on its own, so
        # today this query plans fine — the collision guard is what must make it
        # fail). Naming a computed dimension ``channel`` shadows that column and
        # must be rejected fail-closed.
        q = _q(
            dimensions=["region", {"expression": "lower(city)", "name": "channel"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        )
        with pytest.raises(ValueError, match=r"(?i)collid|conflict|already|exists|shadow"):
            await gen(q)

    async def test_name_colliding_with_a_measure_raises(self) -> None:
        # A computed dimension named the same as a query measure must fail closed.
        q = _q(
            dimensions=["region", {"expression": "lower(city)", "name": "rev"}],
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
        )
        with pytest.raises(ValueError, match=r"(?i)collid|conflict|already|exists|shadow"):
            await gen(q)
