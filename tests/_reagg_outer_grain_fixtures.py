"""Shared fixtures for re-aggregation outer-grain attribution against the operand dataset.

Graph (datasource ``test``): ``account_snapshots`` → ``customers`` many-to-one, so a query
rooted at ``customers`` reaches every ``account_snapshots`` column (and its month bucket)
only across the fanning ``customers → account_snapshots`` hop.

* Ann (1) holds accounts 10 and 11, Bob (2) account 20, Cy (3) has no snapshots, Dee (4)
  accounts 30 (both months) and 31 (February only) — association differs from broadcast.
* Every oracle is re-derived from the raw rows by ``test_reagg_outer_grain_fixtures_smoke.py``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

# (id, name)
CUSTOMERS_ROWS = [(1, "Ann"), (2, "Bob"), (3, "Cy"), (4, "Dee")]
# (id, account_id, customer_id, snapshot_date, balance)
SNAPSHOT_ROWS = [
    (1, 10, 1, "2024-01-05", 160.0),
    (2, 10, 1, "2024-01-20", 150.0),
    (3, 11, 1, "2024-01-10", 30.0),
    (4, 10, 1, "2024-02-03", 120.0),
    (5, 11, 1, "2024-02-15", 40.0),
    (6, 11, 1, "2024-02-25", 35.0),
    (7, 20, 2, "2024-01-07", 500.0),
    (8, 20, 2, "2024-02-07", 700.0),
    (9, 30, 4, "2024-01-12", 80.0),
    (10, 30, 4, "2024-02-12", 60.0),
    (11, 31, 4, "2024-02-18", 25.0),
]

JAN, FEB = "2024-01", "2024-02"

#: Inner grain with the joined month bucket.
P = "partition_by=[account_snapshots.account_id, id, account_snapshots.snapshot_date]"
#: Inner grain without it.
Q = "partition_by=[account_snapshots.account_id, id]"
MAX_Q = f"max(account_snapshots.balance, {Q})"

#: ``sum(<inner>(balance, P))`` by (name, month).
SUM_SUM_P = {
    ("Ann", JAN): 340.0, ("Ann", FEB): 195.0, ("Bob", JAN): 500.0, ("Bob", FEB): 700.0,
    ("Dee", JAN): 80.0, ("Dee", FEB): 85.0,
}
SUM_MAX_P = {
    ("Ann", JAN): 190.0, ("Ann", FEB): 160.0, ("Bob", JAN): 500.0, ("Bob", FEB): 700.0,
    ("Dee", JAN): 80.0, ("Dee", FEB): 85.0,
}
SUM_LAST_P = {
    ("Ann", JAN): 180.0, ("Ann", FEB): 155.0, ("Bob", JAN): 500.0, ("Bob", FEB): 700.0,
    ("Dee", JAN): 80.0, ("Dee", FEB): 85.0,
}
#: ``sum(sum(balance, window='60d', Q))`` by (name, month): trailing 60-day account totals.
SUM_WINDOWED_Q = {
    ("Ann", JAN): 340.0, ("Ann", FEB): 535.0, ("Bob", JAN): 500.0, ("Bob", FEB): 1200.0,
    ("Dee", JAN): 80.0, ("Dee", FEB): 165.0,
}
#: ``sum(max(balance, Q))`` by (name, account_id); Cy keeps its NULL row.
SUM_MAX_Q_BY_ACCOUNT: Dict[Tuple[str, Optional[int]], Optional[float]] = {
    ("Ann", 10): 160.0, ("Ann", 11): 40.0, ("Bob", 20): 700.0,
    ("Dee", 30): 80.0, ("Dee", 31): 25.0, ("Cy", None): None,
}
#: ``sum(sum(balance, P))`` by month only; Cy's NULL-month row carries NULL.
SUM_SUM_P_BY_MONTH: Dict[Optional[str], Optional[float]] = {JAN: 920.0, FEB: 980.0, None: None}
#: ``sum(max(balance, Q))`` by (name, month), associated: the accounts active that month.
ASSOCIATED_MAX_Q_BY_MONTH = {
    ("Ann", JAN): 200.0, ("Ann", FEB): 200.0, ("Bob", JAN): 700.0, ("Bob", FEB): 700.0,
    ("Dee", JAN): 80.0, ("Dee", FEB): 105.0,
}
#: The same, broadcast: the per-customer value in every month.
BROADCAST_MAX_Q_BY_MONTH = {**ASSOCIATED_MAX_Q_BY_MONTH, ("Dee", JAN): 105.0}
#: ``count(max(balance, Q))`` by account_id; Cy's NULL account counts 0.
COUNT_MAX_Q_BY_ACCOUNT: Dict[Optional[int], int] = {None: 0, 10: 1, 11: 1, 20: 1, 30: 1, 31: 1}
#: ``sum(max(balance, Q))`` by (name, band), band = hi iff the account max > 100.
BAND_THRESHOLD = 100
BAND_EXPR = f"CASE WHEN {MAX_Q} > {BAND_THRESHOLD} THEN 'hi' ELSE 'lo' END"
SUM_MAX_Q_BY_BAND = {
    ("Ann", "hi"): 160.0, ("Ann", "lo"): 40.0, ("Bob", "hi"): 700.0, ("Dee", "lo"): 105.0,
}
#: ``sum(max(balance, Q))`` with no dimensions.
KEYLESS_SUM_MAX_Q = 1005.0
#: Account totals ``sum(balance)`` per account.
ACCOUNT_TOTALS = {10: 430.0, 11: 105.0, 20: 1200.0, 30: 140.0, 31: 25.0}
#: Depth three: max over a customer's accounts of the account total.
DEPTH3_MAX_ACCOUNT_TOTAL_BY_NAME: Dict[str, Optional[float]] = {
    "Ann": 430.0, "Bob": 1200.0, "Dee": 140.0, "Cy": None,
}
#: ``rank`` (descending) of the per-account maxima.
RANK_OF_ACCOUNT_MAX = {("Bob", 20): 1, ("Ann", 10): 2, ("Dee", 30): 3, ("Ann", 11): 4, ("Dee", 31): 5}
#: ``weighted_avg(balance, weight=<the account's max>)`` per customer.
WAVG_BY_ACCOUNT_MAX = {
    "Ann": (430.0 * 160 + 105.0 * 40) / (3 * 160 + 3 * 40),
    "Bob": 600.0,
    "Dee": (140.0 * 80 + 25.0 * 25) / (2 * 80 + 25),
}

def query(**kw: Any) -> SlayerQuery:
    kw.setdefault("source_model", "customers")
    return SlayerQuery.model_validate(kw)


def m(formula: str, name: str = "v") -> Dict[str, str]:
    return {"formula": formula, "name": name}


MONTH_TD = {"dimension": "account_snapshots.snapshot_date", "granularity": "month"}


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def snapshots_model() -> SlayerModel:
    return SlayerModel(
        name="account_snapshots", data_source="test", sql_table="account_snapshots",
        default_time_dimension="snapshot_date",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="account_id", type=DataType.INT),
            Column(name="customer_id", type=DataType.INT),
            Column(name="snapshot_date", type=DataType.DATE),
            Column(name="balance", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                         cardinality=JoinCardinality.MANY_TO_ONE)],
    )


def outer_grain_models() -> List[SlayerModel]:
    return [customers_model(), snapshots_model()]


def bundle(*, source: str = "customers") -> ResolvedSourceBundle:
    models = outer_grain_models()
    root = next(x for x in models if x.name == source)
    return ResolvedSourceBundle(
        dialect="postgres", source_model=root,
        referenced_models=[x for x in models if x is not root])


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany("INSERT INTO customers VALUES (?,?)", CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE account_snapshots (id INTEGER PRIMARY KEY, account_id INTEGER, "
            "customer_id INTEGER, snapshot_date TEXT, balance REAL)")
        cur.executemany("INSERT INTO account_snapshots VALUES (?,?,?,?,?)", SNAPSHOT_ROWS)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
        con.executemany("INSERT INTO customers VALUES (?,?)", CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE account_snapshots (id INTEGER, account_id INTEGER, "
            "customer_id INTEGER, snapshot_date DATE, balance DOUBLE)")
        con.executemany("INSERT INTO account_snapshots VALUES (?,?,?,?,?)", SNAPSHOT_ROWS)
    finally:
        con.close()


@asynccontextmanager
async def outer_grain_engine(dialect: str) -> AsyncGenerator[SlayerQueryEngine]:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=outer_grain_models(),
    ) as (engine, _):
        yield engine


def col(columns: List[str], name: str) -> str:
    """The unique result column whose last segment is ``name``."""
    (hit,) = [c for c in columns if c == name or c.endswith(f".{name}")]
    return hit


def month(value: Any) -> Optional[str]:
    """``YYYY-MM`` of a time-bucket cell (string or date/datetime); NULL stays None."""
    return None if value is None else str(value)[:7]


def cells(resp, *, keys: Tuple[str, ...], value: str = "v") -> Dict[Tuple[Any, ...], Any]:
    """Result rows as ``{(key values…): value}``; a ``snapshot_date`` key reads as its month."""
    names = [col(resp.columns, k) for k in keys]
    v = col(resp.columns, value)
    out = {
        tuple(month(r[n]) if k == "snapshot_date" else r[n] for k, n in zip(keys, names)): r[v]
        for r in resp.data
    }
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def approx_cells(got: Dict[Any, Any], expected: Dict[Any, Any]) -> None:
    """``got`` equals ``expected`` key-for-key, numerics approximately, NULLs exactly."""
    assert set(got) == set(expected), (sorted(got, key=repr), sorted(expected, key=repr))
    for k, want in expected.items():
        if want is None:
            assert got[k] is None, (k, got[k])
        else:
            assert got[k] is not None, (k, got[k])
            assert float(got[k]) == pytest.approx(want), (k, got[k])


def warnings_of(resp, kind: str) -> list:
    return [w for w in (resp.warnings or []) if getattr(w, "kind", None) == kind]
