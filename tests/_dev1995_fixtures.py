"""Aggregates are virtual models: fixtures on the DEV-1840 / DEV-1910 seeds.

Rows appended to the DEV-1840 seed (``make_vm_engine``):

* region 3 has a NULL name and owns customer 8 (orders 13, 14);
* order 12 names customer 99, which does not exist (dangling);
* order 15 (customer 2) has a partially-NULL store key ``('A', NULL)``;
* store ``('C', 1)`` (BOS) has no orders.

``make_app_null_engine``: the DEV-1910 null-status seed plus c5's NULL-status app order.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Optional

import pytest

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._dev1840_fixtures import (
    _seed_duckdb,
    _seed_sqlite,
    dev1840_models,
    rows_by,
)
from tests._dev1910_fixtures import _append_order, make_null_status_engine
from tests._engine_helpers import seeded_exec_engine

_EXTRA_ROWS = {
    "regions": [(3, None, 300.0)],
    "customers": [(8, 3, "p1", "gold", 70.0, "2024-04-22")],
    "stores": [("C", 1, "BOS", 50.0)],
    "orders": [
        (12, 99, "ok", "web", 9.0, "2024-02-05", "A", 1),
        (13, 8, "ok", "app", 11.0, "2024-04-25", "A", 2),
        (14, 8, "new", "web", 6.0, "2024-04-26", "B", 1),
        (15, 2, "ok", "web", 4.0, "2024-02-25", "A", None),
    ],
}

APP_NULL_STATUS_ORDER = (16, 5, None, "app", 2.0, "2024-04-05", "B", 1)


def _inserts():
    for table, rows in _EXTRA_ROWS.items():
        marks = ",".join("?" * len(rows[0]))
        yield f"INSERT INTO {table} VALUES ({marks})", rows


def _seed_vm_sqlite(db_path: str) -> None:
    _seed_sqlite(db_path)
    with transaction(db_path) as con:
        for sql, rows in _inserts():
            con.executemany(sql, rows)


def _seed_vm_duckdb(db_path: str) -> None:
    _seed_duckdb(db_path)
    con = pytest.importorskip("duckdb").connect(db_path)
    for sql, rows in _inserts():
        con.executemany(sql, [list(r) for r in rows])
    con.close()


async def make_vm_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """``params=["sqlite", "duckdb"]`` engine over the DEV-1840 seed plus the rows above."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_vm_duckdb if dialect == "duckdb" else _seed_vm_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=dev1840_models(),
    ) as (engine, _db):
        yield engine


async def make_app_null_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """The DEV-1910 null-status seed plus c5's NULL-status app order."""
    async for engine in make_null_status_engine(request):
        ds = await engine.storage.get_datasource("test")
        assert ds is not None
        assert ds.type is not None
        assert ds.database is not None
        _append_order(dialect=ds.type, db_path=ds.database, row=APP_NULL_STATUS_ORDER)
        yield engine


def q(payload: Dict[str, Any]) -> SlayerQuery:
    return SlayerQuery.model_validate(payload)


def cells(resp, dim: str, measure: str) -> Dict[Any, Any]:
    """``{dimension value: measure value}``; ``None`` keys the NULL cell."""
    return {k[0]: v[measure] for k, v in rows_by(resp, dim).items()}


async def materialise(engine: SlayerQueryEngine, *, name: str,
                      payload: Dict[str, Any]) -> Dict[str, Dict[Any, Any]]:
    """Save ``payload`` as a query-backed model; return ``{measure: {grain value: value}}``."""
    model = await engine.create_model_from_query(q(payload), name)
    grain = model.columns[0].name
    measures = [m["name"] for m in payload["measures"]]
    resp = await engine.execute(q({
        "source_model": name, "dimensions": [grain, *measures]}))
    return {m: cells(resp, f"{name}.{grain}", f"{name}.{m}") for m in measures}


def assert_parity(attached: Dict[Any, Any], oracle: Dict[Any, Any], *,
                  empty: Optional[Any]) -> None:
    """Every attached cell equals the oracle row for its grain value, else the empty value."""
    for key, value in attached.items():
        expected = oracle.get(key, empty)
        if expected is None:
            assert value is None, key
        else:
            assert float(value) == pytest.approx(expected), key


# Base DEV-1840 seed.
ORPHAN_NULL_REGION_SUM = 47.0
ORPHAN_NULL_REGION_COUNT = 2

# Virtual-model seed.
SUM_BY_REGION = {"North": 104.0, "South": 20.0, None: 73.0}
COUNT_BY_REGION = {"North": 7, "South": 2, None: 5}
SUM_BY_PLAN = {"p1": 55.0, "p2": 99.0, "p3": 15.0, None: 28.0}
COUNT_BY_PLAN = {"p1": 6, "p2": 4, "p3": 1, None: 3}
SUM_BY_CITY = {"NYC": 51.0, "LA": 83.0, "SF": 56.0, "DAL": 3.0, None: 4.0}
COUNT_BY_CITY = {"NYC": 4, "LA": 4, "SF": 4, "DAL": 1, None: 1}
STORE_SUM_BY_CITY = {"NYC": 51.0, "LA": 83.0, "SF": 56.0, "DAL": 3.0, "BOS": None}
STORE_COUNT_BY_CITY = {"NYC": 4, "LA": 4, "SF": 4, "DAL": 1, "BOS": 0}

# DEV-1910 null-status seed.
SPEND_BY_STATUS = {"ok": 420.0, "new": 290.0, None: 155.0}
ORDER_KEY_STATUS_ORDER = [None, "new", "ok"]

# App-filtered, with c5's NULL-status app order.
APP_SPEND_BY_STATUS = {"new": 250.0, "ok": 140.0, None: 80.0}
