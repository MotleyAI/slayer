"""DEV-1866 — LAW: an inferred population is invariant under measures.

Axiom 12 (``architecture/semantics.arc42.md``): adding or removing a measure on
a rootless query must never change the inferred population or the dimension row
set. Compositionality — a measure attaches values, it never re-quantifies the
rows. Runs on SQLite and DuckDB.
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1866_fixtures import make_chain_exec_engine

# (label, dimensions, filters, expected inferred population)
_BASES = [
    ("customers_region", ["customers.region"], None, "customers"),
    ("orders_scoped", ["customers.region"], ["orders.status = 'ok'"], "orders"),
]

# Measure sets layered onto each base; all bind under either population.
_MEASURE_SETS = [
    [],
    [{"formula": "orders.amount:sum", "name": "m1"}],
    [
        {"formula": "orders.amount:sum", "name": "m1"},
        {"formula": "orders.amount:avg", "name": "m2"},
    ],
]


@pytest.fixture(params=["sqlite", "duckdb"])
async def chain_engine(request):
    async for engine in make_chain_exec_engine(request.param):
        yield engine


def _law(condition: bool, *, detail: str) -> None:
    assert condition, f"LAW population-invariance violated — {detail}"


@pytest.mark.parametrize("label, dims, filters, population", _BASES)
async def test_measure_does_not_requantify(chain_engine, label, dims, filters,
                                           population) -> None:
    base = await chain_engine.execute(SlayerQuery(dimensions=dims, filters=filters))
    _law(base.population == population,
         detail=f"{label}: base population {base.population!r} != {population!r}")
    dim_cols = list(base.columns)
    base_rows = {tuple(r[c] for c in dim_cols) for r in base.data}

    for measures in _MEASURE_SETS:
        resp = await chain_engine.execute(SlayerQuery(
            dimensions=dims, filters=filters, measures=measures or None,
        ))
        _law(resp.population == population,
             detail=f"{label}: population moved to {resp.population!r} "
                    f"with measures={[m['name'] for m in measures]}")
        _law(resp.population_inferred is True,
             detail=f"{label}: population not flagged inferred")
        rows = {tuple(r[c] for c in dim_cols) for r in resp.data}
        _law(rows == base_rows,
             detail=f"{label}: dimension row set changed with "
                    f"measures={[m['name'] for m in measures]}: "
                    f"{rows ^ base_rows}")
