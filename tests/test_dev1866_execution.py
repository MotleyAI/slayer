"""DEV-1866 — a rootless query executes identically to its explicit twin.

Runs on SQLite and DuckDB. The inferred-population query and the same query
with that population named explicitly must agree on SQL, data, columns,
attributes, and warnings; only the reported ``population`` / ``population_inferred``
metadata differs.
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1866_fixtures import (
    CHAIN_REVENUE_BY_REGION,
    make_chain_exec_engine,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def chain_engine(request):
    async for engine in make_chain_exec_engine(request.param):
        yield engine


def _canon(rows: list[dict]) -> list[tuple]:
    # Row-content multiset; the twin queries have no ORDER BY, so row order may
    # differ between executions (notably on DuckDB) even for byte-identical SQL.
    return sorted((tuple(sorted(r.items())) for r in rows), key=repr)


async def _assert_twin(engine, *, rootless: SlayerQuery, explicit: SlayerQuery,
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
        await _assert_twin(
            chain_engine,
            rootless=SlayerQuery(
                dimensions=["customers.region"],
                measures=[{"formula": "orders.amount:sum", "name": "rev"}],
            ),
            explicit=SlayerQuery(
                source_model="customers",
                dimensions=["customers.region"],
                measures=[{"formula": "orders.amount:sum", "name": "rev"}],
            ),
            population="customers",
        )

    async def test_field_filter_pulls_to_orders(self, chain_engine) -> None:
        await _assert_twin(
            chain_engine,
            rootless=SlayerQuery(
                dimensions=["customers.region"],
                measures=[{"formula": "amount:sum", "name": "rev"}],
                filters=["orders.status = 'ok'"],
            ),
            explicit=SlayerQuery(
                source_model="orders",
                dimensions=["customers.region"],
                measures=[{"formula": "amount:sum", "name": "rev"}],
                filters=["orders.status = 'ok'"],
            ),
            population="orders",
        )

    async def test_raw_row_mode(self, chain_engine) -> None:
        rootless = SlayerQuery(
            dimensions=["customers.region"],
            distinct_dimension_values=False,
        )
        await _assert_twin(
            chain_engine,
            rootless=rootless,
            explicit=SlayerQuery(
                source_model="customers",
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
        resp = await chain_engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
        ))
        assert resp.population == "orders"
        assert resp.population_inferred is False
        regions = {r["orders.customers.region"] for r in resp.data}
        assert regions == {"North", "South"}  # rooted at orders ⇒ no West row

    async def test_explicit_bridge_owning_no_queried_field(self, chain_engine) -> None:
        """customers owns neither the queried dimension; it's a valid explicit
        bridge, and no inference runs (inference alone would pick regions)."""
        resp = await chain_engine.execute(SlayerQuery(
            source_model="customers",
            dimensions=["regions.name"],
        ))
        assert resp.population == "customers"
        assert resp.population_inferred is False
        assert {r["customers.regions.name"] for r in resp.data} == {"North", "South", "West"}


class TestNullAttachment:
    async def test_region_without_orders_attaches_null(self, chain_engine) -> None:
        resp = await chain_engine.execute(SlayerQuery(
            dimensions=["customers.region"],
            measures=[{"formula": "orders.amount:sum", "name": "rev"}],
        ))
        assert resp.population == "customers"
        assert resp.population_inferred is True
        got = rows_by(resp, key="customers.region", value="customers.rev")
        assert got == CHAIN_REVENUE_BY_REGION
