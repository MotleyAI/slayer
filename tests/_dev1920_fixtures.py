"""A small ``shop`` store (orders → customers → regions) for the colon-spelling removal tests.

Physical tables::

    regions   (id, name, population)
    customers (id, name, region_id, created_at)
    orders    (order_id, customer_id, status, region, note, created_at, updated_at, amount, price, balance)
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from tests._granularity_rename_fixtures import write_raw_store

DS = "shop"
BACKENDS = ["yaml", "sqlite"]
DIALECTS = ["sqlite", "duckdb"]
#: The versions written before the colon removal.
STORED_VERSIONS = {"SlayerModel": 18, "SlayerQuery": 9, "DatasourceConfig": 3, "Memory": 7}

REGION_ROWS = [(1, "North", 100.0), (2, "South", 250.0)]
CUSTOMER_ROWS = [
    (1, "Ann", 1, "2024-01-05 00:00:00"),
    (2, "Bob", 2, "2024-02-10 00:00:00"),
    (3, "Cid", 1, "2024-05-20 00:00:00"),
]
# (order_id, customer_id, status, region, note, created_at, updated_at, amount, price, balance)
ORDER_ROWS = [
    (1, 1, "paid", "North", "a:sum", "2024-01-10 00:00:00", "2024-01-11 00:00:00", 10.0, 1.0, 5.0),
    (2, 1, "new", "North", "x:count", "2024-01-20 00:00:00", "2024-01-25 00:00:00", 20.0, 2.0, 7.0),
    (3, 2, "paid", "South", "plain", "2024-02-15 00:00:00", "2024-02-16 00:00:00", 30.0, 3.0, 9.0),
    (4, 3, "paid", "North", "plain", "2024-04-01 00:00:00", "2024-04-03 00:00:00", 40.0, 4.0, 11.0),
    (5, 3, "new", "South", "a:sum", "2024-06-30 00:00:00", "2024-07-01 00:00:00", 50.0, 5.0, 13.0),
]

_DDL = (
    "CREATE TABLE regions (id INTEGER, name TEXT, population DOUBLE)",
    "CREATE TABLE customers (id INTEGER, name TEXT, region_id INTEGER, created_at TIMESTAMP)",
    "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, status TEXT, region TEXT, note TEXT, "
    "created_at TIMESTAMP, updated_at TIMESTAMP, amount DOUBLE, price DOUBLE, balance DOUBLE)",
)
_INSERTS = (
    ("INSERT INTO regions VALUES (?,?,?)", REGION_ROWS),
    ("INSERT INTO customers VALUES (?,?,?,?)", CUSTOMER_ROWS),
    ("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?)", ORDER_ROWS),
)

#: Three-month buckets starting on the 1st of Feb / May / Aug / Nov.
FISCAL_QUARTER = {"name": "fiscal_quarter", "base": "month", "multiple": 3, "origin": "2000-02-01T00:00:00"}


def seed(db_path: str, *, dialect: str) -> None:
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for stmt in _DDL:
                con.execute(stmt)
            for stmt, rows in _INSERTS:
                con.executemany(stmt, rows)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for stmt in _DDL:
            cur.execute(stmt)
        for stmt, rows in _INSERTS:
            cur.executemany(stmt, rows)


def datasource_doc(*, db_path: str, dialect: str, granularities: Sequence[dict] = ()) -> dict:
    doc: dict[str, Any] = {"version": STORED_VERSIONS["DatasourceConfig"], "name": DS, "type": dialect,
                           "database": db_path}
    if granularities:
        doc["granularities"] = list(granularities)
    return doc


_ORDER_COLUMNS = [
    {"name": "order_id", "type": "INT", "primary_key": True},
    {"name": "customer_id", "type": "INT"},
    {"name": "status", "type": "TEXT"},
    {"name": "region", "type": "TEXT"},
    {"name": "note", "type": "TEXT"},
    {"name": "created_at", "type": "TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP"},
    {"name": "amount", "type": "DOUBLE"},
    {"name": "price", "type": "DOUBLE"},
    {"name": "balance", "type": "DOUBLE"},
]


def orders_doc(*, version: int | None = STORED_VERSIONS["SlayerModel"], measures: Sequence[dict] = (),
               **extra: Any) -> dict:
    """``version=None`` is a fresh (unversioned) payload."""
    doc: dict[str, Any] = {
        "name": "orders", "sql_table": "orders", "data_source": DS, "columns": _ORDER_COLUMNS,
        "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"}],
        **extra,
    }
    if version is not None:
        doc["version"] = version
    if measures:
        doc["measures"] = list(measures)
    return doc


def customers_doc() -> dict:
    return {
        "version": STORED_VERSIONS["SlayerModel"], "name": "customers", "sql_table": "customers", "data_source": DS,
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "name", "type": "TEXT"},
            {"name": "region_id", "type": "INT"},
            {"name": "created_at", "type": "TIMESTAMP"},
        ],
        "joins": [{"target_model": "regions", "join_pairs": [["region_id", "id"]], "cardinality": "many_to_one"}],
    }


def regions_doc() -> dict:
    return {
        "version": STORED_VERSIONS["SlayerModel"], "name": "regions", "sql_table": "regions", "data_source": DS,
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "name", "type": "TEXT"},
            {"name": "population", "type": "DOUBLE"},
        ],
    }


def stored_query(**fields: Any) -> dict:
    """A query document as stored before the colon removal."""
    return {"version": STORED_VERSIONS["SlayerQuery"], "source_model": "orders", **fields}


def query_backed_doc(*, name: str, stage: dict) -> dict:
    return {"version": STORED_VERSIONS["SlayerModel"], "name": name, "data_source": DS, "source_queries": [stage]}


def memory_doc(*, memory_id: str, query: dict) -> dict:
    return {"version": STORED_VERSIONS["Memory"], "id": memory_id, "learning": f"learning {memory_id}",
            "entities": [], "query": query}


StoreFactory = Callable[..., Awaitable[StorageBackend]]


def store_base(tmp_path: Any, *, backend: str = "yaml", dialect: str = "sqlite", label: str = "store") -> str:
    """The directory ``make_store`` writes the store with these arguments into."""
    return str(tmp_path / f"{label}-{backend}-{dialect}")


@pytest.fixture
async def make_store(tmp_path: Any) -> AsyncIterator[StoreFactory]:
    """``await make_store(backend=, dialect=, models=, memories=, granularities=)``: a raw store over a seeded
    shop database; ``models`` defaults to orders, customers and regions as stored before the change."""
    datasources: list[DatasourceConfig] = []

    async def _make(*, backend: str = "yaml", dialect: str = "sqlite", models: Sequence[dict] | None = None,
                    memories: Sequence[dict] = (), granularities: Sequence[dict] = (),
                    label: str = "store") -> StorageBackend:
        base = store_base(tmp_path, backend=backend, dialect=dialect, label=label)
        os.makedirs(base, exist_ok=True)
        db_path = os.path.join(base, f"shop.{dialect}")
        seed(db_path, dialect=dialect)
        ds = datasource_doc(db_path=db_path, dialect=dialect, granularities=granularities)
        datasources.append(DatasourceConfig.model_validate(ds))
        docs = list(models) if models is not None else [orders_doc(), customers_doc(), regions_doc()]
        return await write_raw_store(backend=backend, base=base, datasources=[ds], models=docs, memories=memories)

    try:
        yield _make
    finally:
        for ds in datasources:
            engine_factory.invalidate_engine(ds)


async def run(storage: StorageBackend, query: SlayerQuery | dict) -> SlayerResponse:
    engine = SlayerQueryEngine(storage=storage)
    try:
        q = query if isinstance(query, SlayerQuery) else SlayerQuery.model_validate(query)
        return await engine.execute(q)
    finally:
        engine.close()


def rows(resp: SlayerResponse) -> list[dict]:
    """The result rows with float-normalised numbers, keys included."""
    return [{k: float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v for k, v in r.items()}
            for r in resp.data]
