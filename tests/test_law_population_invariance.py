"""LAW: a named population is invariant under metrics.

Axiom 12.1 (``architecture/semantics.arc42.md``): adding or removing a metric on
a query naming its population never changes the population or the dimension row
set (Law 7, anchored half). Runs on SQLite and DuckDB.
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1866_fixtures import make_chain_exec_engine
from slayer.engine.query_engine import SlayerQueryEngine

# (label, population, dimensions, filters)
_BASES = [
    ("customers_region", "customers", ["customers.region"], None),
    ("orders_scoped", "orders", ["customers.region"], ["orders.status = 'ok'"]),
]

# Metric sets layered onto each base; all bind under either population.
_MEASURE_SETS = [
    [],
    [{"metric": "sum(orders.amount)", "alias": "m1"}],
    [
        {"metric": "sum(orders.amount)", "alias": "m1"},
        {"metric": "avg(orders.amount)", "alias": "m2"},
    ],
]


@pytest.fixture(params=["sqlite", "duckdb"])
async def chain_engine(request):
    async for engine in make_chain_exec_engine(request.param):
        yield engine


def _law(condition: bool, *, detail: str) -> None:
    assert condition, f"LAW population-invariance violated — {detail}"


@pytest.mark.parametrize("label, population, dims, filters", _BASES)
async def test_measure_does_not_requantify(chain_engine: SlayerQueryEngine, label, population, dims,
                                           filters) -> None:
    base = await chain_engine.execute(SlayerQuery(population=population, dimensions=dims, filter=filters))
    dim_cols = list(base.columns)
    base_rows = {tuple(r[c] for c in dim_cols) for r in base.data}

    for measures in _MEASURE_SETS:
        resp = await chain_engine.execute(SlayerQuery(
            population=population, dimensions=dims, filter=filters, metrics=measures or None,
        ))
        _law(resp.population == population,
             detail=f"{label}: population moved to {resp.population!r} "
                    f"with measures={[m['alias'] for m in measures]}")
        _law(resp.population_inferred is False,
             detail=f"{label}: named population flagged inferred")
        rows = {tuple(r[c] for c in dim_cols) for r in resp.data}
        _law(rows == base_rows,
             detail=f"{label}: dimension row set changed with "
                    f"measures={[m['alias'] for m in measures]}: "
                    f"{rows ^ base_rows}")
