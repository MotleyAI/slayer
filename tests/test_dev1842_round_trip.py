"""A dotted saved measure whose formula crosses back to a model on the host chain composes.

``customers.order_total`` (``orders.amount:sum``) from an ``orders``-rooted query reads a
second ``orders`` occurrence — the customer's orders — evaluated once per row of it.
Expected values come from raw-SQL oracles over the seeded DEV-1836 rows.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from tests._dev1836_fixtures import _seed_duckdb, _seed_sqlite
from tests._dev1842_fixtures import dev1842_models, q
from tests._engine_helpers import disposable_engine, seeded_exec_engine

# The second ``orders`` occurrence is a virtual model over every order (Axiom 6):
# its value never depends on the query root, so the orphan order counts, in the NULL tier.
_TOTAL_SQL = "SELECT SUM(amount) FROM orders"
_BY_TIER_SQL = (
    "SELECT c.tier, SUM(o.amount) FROM orders o LEFT JOIN customers c "
    "ON c.id = o.customer_id GROUP BY c.tier"
)
_TIER_CELLS_SQL = "SELECT DISTINCT c.tier FROM orders o LEFT JOIN customers c ON c.id = o.customer_id"


def _oracle(dialect: str, db_path: str, sql: str) -> list[tuple]:
    with disposable_engine(f"{dialect}:///{db_path}") as eng, eng.connect() as con:
        return [tuple(r) for r in con.execute(sa.text(sql)).fetchall()]


@pytest.fixture(params=["sqlite", "duckdb"])
async def seeded(request):
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
        pytest.importorskip("duckdb_engine")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev1842_models()) as (engine, db):
        yield engine, dialect, db


async def test_round_trip_composes_to_the_customers_orders_total(seeded) -> None:
    engine, dialect, db = seeded
    ((oracle,),) = _oracle(dialect, db, _TOTAL_SQL)
    assert float(oracle) == pytest.approx(117.0)
    resp = await engine.execute(q(measures=[{"formula": "customers.order_total", "name": "x"}]))
    assert [float(r["orders.x"]) for r in resp.data] == pytest.approx([117.0])


async def test_round_trip_is_attributed_per_host_tier(seeded) -> None:
    engine, dialect, db = seeded
    sums = dict(_oracle(dialect, db, _BY_TIER_SQL))
    oracle = {t: (None if sums.get(t) is None else float(sums[t]))
              for (t,) in _oracle(dialect, db, _TIER_CELLS_SQL)}
    assert oracle == {"gold": 40.0, "silver": 30.0, "bronze": 40.0, None: 7.0}
    resp = await engine.execute(q(
        dimensions=["customers.tier"],
        measures=[{"formula": "customers.order_total", "name": "x"}],
    ))
    got = {r["orders.customers.tier"]: (None if r["orders.x"] is None else float(r["orders.x"]))
           for r in resp.data}
    assert got == oracle
