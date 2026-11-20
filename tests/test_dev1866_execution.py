"""A rootless query's rows and reported population against its explicit twins, on SQLite and DuckDB."""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1866_fixtures import (
    CHAIN_REVENUE_BY_REGION,
    make_chain_exec_engine,
    rows_by,
)
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite", "duckdb"])
async def chain_engine(request):
    async for engine in make_chain_exec_engine(request.param):
        yield engine


def _canon(rows: list[dict]) -> list[tuple]:
    # Row-content multiset; the twin queries have no ORDER BY, so row order may
    # differ between executions (notably on DuckDB) even for byte-identical SQL.
    return sorted((tuple(sorted(r.items())) for r in rows), key=repr)


async def _assert_twin(engine: SlayerQueryEngine, *, rootless: SlayerQuery, explicit: SlayerQuery,
                       population: str) -> None:
    r = await engine.execute(rootless)
    e = await engine.execute(explicit)
    assert r.sql == e.sql
    assert _canon(r.data) == _canon(e.data)
    assert r.columns == e.columns
    assert r.attributes == e.attributes
    assert r.warnings == e.warnings
    assert r.population == e.population == population
    assert r.population_inferred is True
    assert e.population_inferred is False


class TestExplicitTwinParity:
    async def test_canonical_customers_population(self, chain_engine) -> None:
        """The anchor is ``customers``, but West, which no order reaches, is not a row."""
        resp = await chain_engine.execute(SlayerQuery.model_validate({
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
        }))
        assert (resp.population, resp.population_inferred) == ("customers", True)
        assert {r["customers.region"] for r in resp.data} == {"North", "South"}

    async def test_field_filter_keeps_the_anchor(self, chain_engine) -> None:
        """An ``orders`` filter restricts the rows and never moves the anchor off ``customers``."""
        query = {
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "filter": ["orders.status = 'ok'"],
        }
        r = await chain_engine.execute(SlayerQuery.model_validate(query))
        e = await chain_engine.execute(SlayerQuery.model_validate({"population": "orders", **query}))
        assert _canon(r.data) == _canon(e.data)
        assert r.columns == e.columns
        assert (r.population, r.population_inferred) == ("customers", True)

    async def test_raw_row_mode(self, chain_engine) -> None:
        rootless = SlayerQuery(
            dimensions=["customers.region"],
            distinct_dimension_values=False,
        )
        await _assert_twin(
            chain_engine,
            rootless=rootless,
            explicit=SlayerQuery(
                population="customers",
                dimensions=["customers.region"],
                distinct_dimension_values=False,
            ),
            population="customers",
        )
        # One row per population (customers) row, not one per distinct region.
        resp = await chain_engine.execute(rootless)
        regions = [r["customers.region"] for r in resp.data]
        assert len(regions) == 4
        assert regions.count("North") == 2


class TestExplicitOverride:
    async def test_explicit_root_bypasses_inference(self, chain_engine) -> None:
        """Explicit source_model=orders is honored even though inference would
        pick customers — proven by West (a region with no orders) being absent."""
        resp = await chain_engine.execute(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
        }))
        assert resp.population == "orders"
        assert resp.population_inferred is False
        regions = {r["customers.region"] for r in resp.data}
        assert regions == {"North", "South"}  # rooted at orders ⇒ no West row

    async def test_explicit_bridge_owning_no_queried_field(self, chain_engine) -> None:
        """customers owns neither the queried dimension; it's a valid explicit
        bridge, and no inference runs (inference alone would pick regions)."""
        resp = await chain_engine.execute(SlayerQuery(
            population="customers",
            dimensions=["regions.name"],
        ))
        assert resp.population == "customers"
        assert resp.population_inferred is False
        assert {r["regions.name"] for r in resp.data} == {"North", "South", "West"}


class TestNullAttachment:
    async def test_region_without_orders_is_not_a_row(self, chain_engine) -> None:
        resp = await chain_engine.execute(SlayerQuery.model_validate({
            "dimensions": ["customers.region"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
        }))
        assert resp.population == "customers"
        assert resp.population_inferred is True
        got = rows_by(resp, key="customers.region", value="rev")
        assert got == {region: v for region, v in CHAIN_REVENUE_BY_REGION.items() if v is not None}
