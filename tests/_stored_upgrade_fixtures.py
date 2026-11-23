"""Raw 0.10.x-shaped stores over a small ``shop`` database (orders → customers → regions).

Physical tables::

    customers (id, name, region, signed_up_at)
    orders    (order_id, customer_id, status, ordered_at, amount)
    regions   (name, opened_on, label)       -- seed_regions only
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerResponse
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine

DS = "shop"
BACKENDS = ["yaml", "sqlite"]

CUSTOMERS_ROWS = [
    (1, "Ann", "North", "2023-06-01 00:00:00"),
    (2, "Bob", "South", "2024-03-01 00:00:00"),
    (3, "Cid", "North", "2024-08-01 00:00:00"),
]
# (order_id, customer_id, status, ordered_at, amount)
ORDERS_ROWS = [
    (1, 1, "ok", "2023-12-31 23:00:00", 10.0),
    (2, 1, "new", "2024-01-01 00:00:00", 20.0),
    (3, 2, "ok", "2024-01-01 01:30:00", 30.0),
    (4, 3, "ok", "2024-06-30 00:00:00", 40.0),
    (5, 3, "new", "2025-01-15 00:00:00", 50.0),
    (6, 2, "2024/01/01", "2024-07-01 00:00:00", 60.0),
]

# (name, opened_on, label)
REGIONS_ROWS = [
    ("North", "2023-01-15", "2024/01/01"),
    ("South", "2024-05-01", "s"),
]

TOTAL = 210.0
#: sum(amount) of orders whose customer's region opened on or after 2024-01-01 (South: orders 3 and 6).
AMOUNT_OPENED_SINCE_2024 = 90.0
#: sum(amount) by customers.region.
AMOUNT_BY_REGION = {"North": 120.0, "South": 90.0}
#: Highest sum(amount) per status ('ok' 80 > 'new' 70 > '2024/01/01' 60).
TOP_STATUS_DESC = "ok"


def seed_shop(db_path: str, *, dialect: str) -> None:
    """Create and fill the shop tables in a SQLite or DuckDB file."""
    ddl = (
        "CREATE TABLE customers (id INTEGER, name TEXT, region TEXT, signed_up_at TIMESTAMP)",
        "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, status TEXT, "
        "ordered_at TIMESTAMP, amount DOUBLE)",
    )
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for stmt in ddl:
                con.execute(stmt)
            con.executemany("INSERT INTO customers VALUES (?,?,?,?)", CUSTOMERS_ROWS)
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS_ROWS)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for stmt in ddl:
            cur.execute(stmt)
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?)", CUSTOMERS_ROWS)
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS_ROWS)


def seed_regions(db_path: str) -> None:
    """Add the ``regions`` table to a seeded DuckDB shop file."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE regions (name TEXT, opened_on DATE, label TEXT)")
        con.executemany("INSERT INTO regions VALUES (?,?,?)", REGIONS_ROWS)
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Raw documents in the shape SLayer 0.10.2 wrote them.
# --------------------------------------------------------------------------- #
def orders_v10(*, declare_fk: bool = True, version: int = 10, joins: list[dict] | None = None) -> dict:
    """``declare_fk=False`` is the 0.10.2 Cube-import shape (FK not a dimension)."""
    columns: list[dict] = [{"name": "order_id", "type": "INT", "primary_key": True}]
    if declare_fk:
        columns.append({"name": "customer_id", "type": "INT"})
    columns += [
        {"name": "status", "type": "TEXT"},
        {"name": "ordered_at", "type": "TIMESTAMP"},
        {"name": "amount", "type": "DOUBLE"},
    ]
    return {
        "version": version, "name": "orders", "sql_table": "orders", "data_source": DS,
        "columns": columns,
        "joins": joins if joins is not None else [
            {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"},
        ],
    }


def customers_v10(
    *, declare_pk: bool = True, version: int = 10, name: str = "customers", joins: list[dict] | None = None,
) -> dict:
    """``name`` other than ``customers`` reads the same table; ``joins`` are stored only when given."""
    columns: list[dict] = [{"name": "id", "type": "INT", "primary_key": True}] if declare_pk else []
    columns += [
        {"name": "name", "type": "TEXT"},
        {"name": "region", "type": "TEXT"},
        {"name": "signed_up_at", "type": "TIMESTAMP"},
    ]
    return {
        "version": version, "name": name, "sql_table": "customers", "data_source": DS,
        "columns": columns, **({"joins": joins} if joins is not None else {}),
    }


def region_join(target: str = "regions") -> dict:
    """A customers-side join to the regions model ``target`` on the region name."""
    return {"target_model": target, "join_pairs": [["region", "name"]], "cardinality": "many_to_one"}


#: The ``orders ↔ customers`` join declared on ``customers`` (orders then stores no joins).
CUSTOMERS_TO_ORDERS = {"target_model": "orders", "join_pairs": [["id", "customer_id"]], "cardinality": "one_to_many"}


def regions_v10(*, name: str = "regions", version: int = 10, date_column: str = "opened_on") -> dict:
    """``date_column`` names the DATE column reading ``opened_on``."""
    date_sql = {"sql": "opened_on"} if date_column != "opened_on" else {}
    return {
        "version": version, "name": name, "sql_table": "regions", "data_source": DS,
        "columns": [
            {"name": "name", "type": "TEXT", "primary_key": True},
            {"name": date_column, "type": "DATE", **date_sql},
            {"name": "label", "type": "TEXT"},
        ],
    }


def in_datasource(doc: dict, data_source: str) -> dict:
    return {**doc, "data_source": data_source}


def query_backed_v10(*, name: str, stages: list[dict], version: int = 10) -> dict:
    return {"version": version, "name": name, "data_source": DS, "source_queries": stages}


def rev_query(**extra: Any) -> dict:
    """A 0.10.2-shaped stored source query: ``sum(amount)`` as ``rev`` over orders."""
    return {"source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "rev"}], **extra}


def year_td(date_range: list | None) -> dict:
    td: dict = {"dimension": {"name": "ordered_at"}, "granularity": "year"}
    if date_range is not None:
        td["date_range"] = date_range
    return td


def funcstyle_rank_order(raw_formula: str) -> dict:
    """An expression order item as 0.10.2 persisted it."""
    return {"column": {"name": "_funcstyle_pending"}, "direction": "asc", "raw_formula": raw_formula}


def memory_v2(*, memory_id: str, query: dict, version: int = 2) -> dict:
    return {"version": version, "id": memory_id, "learning": f"learning {memory_id}", "entities": [], "query": query}


# --------------------------------------------------------------------------- #
# Stores.
# --------------------------------------------------------------------------- #
async def raw_store(
    *, backend: str, base: str, datasource: DatasourceConfig | None,
    models: Sequence[dict] = (), memories: Sequence[dict] = (), raw_rows: Sequence[tuple[str, str]] = (),
    extra_datasources: Sequence[DatasourceConfig] = (),
) -> StorageBackend:
    """A YAML or SQLite store whose datasets/memories are written verbatim (never validated).

    ``raw_rows`` are ``(name, text)`` dataset payloads written byte-for-byte (corrupt documents); a dataset is stored
    under its own ``data_source`` (default ``DS``).
    """
    os.makedirs(base, exist_ok=True)
    datasources = [*([datasource] if datasource is not None else []), *extra_datasources]
    if backend == "yaml":
        storage: StorageBackend = YAMLStorage(base_dir=base)
        for ds in datasources:
            await storage.save_datasource(ds)
        for doc in models:
            _write_text(path=yaml_model_path(base=base, name=doc["name"], data_source=doc.get("data_source", DS)),
                        text=yaml.safe_dump(doc, sort_keys=False))
        for name, text in raw_rows:
            _write_text(path=yaml_model_path(base=base, name=name), text=text)
        for mem in memories:
            front = {k: v for k, v in mem.items() if k not in ("id", "learning")}
            _write_text(
                path=storage._memory_md_path(mem["id"]),  # type: ignore[attr-defined]
                text=f"---\n{yaml.safe_dump(front)}---\n{mem['learning']}",
            )
        return storage
    db_path = os.path.join(base, "store.db")
    storage = SQLiteStorage(db_path=db_path)
    for ds in datasources:
        await storage.save_datasource(ds)
    with transaction(db_path) as conn:
        for doc in models:
            conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                         (doc.get("data_source", DS), doc["name"], json.dumps(doc)))
        for name, text in raw_rows:
            conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)", (DS, name, text))
        for mem in memories:
            conn.execute("INSERT INTO memories (id, data) VALUES (?, ?)", (mem["id"], json.dumps(mem)))
    return storage


def yaml_model_path(*, base: str, name: str, data_source: str = DS) -> str:
    return os.path.join(base, "models", data_source, f"{name}.yaml")


def _write_text(*, path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — test seed
        f.write(text)


def stored_bytes(*, backend: str, base: str, name: str) -> str:
    """The persisted dataset payload exactly as stored."""
    if backend == "yaml":
        with open(yaml_model_path(base=base, name=name), encoding="utf-8") as f:  # NOSONAR(S7493) — test read
            return f.read()
    with transaction(os.path.join(base, "store.db")) as conn:
        row = conn.execute("SELECT data FROM models WHERE data_source = ? AND name = ?", (DS, name)).fetchone()
    return row[0]


def field_of(dataset: SlayerDataset, name: str) -> DatasetField:
    by_name = {c.name: c for c in dataset.fields}
    assert name in by_name, sorted(by_name)
    return by_name[name]


async def raw_doc(storage: StorageBackend, name: str) -> dict:
    doc = await storage._load_raw_dataset_dict(name=name, data_source=DS)
    assert doc is not None
    return doc


# --------------------------------------------------------------------------- #
# Execution.
# --------------------------------------------------------------------------- #
async def run(storage: StorageBackend, query: SlayerQuery | dict, *, dry_run: bool = False) -> SlayerResponse:
    async with owned_query_engine(storage=storage) as engine:
        q = query if isinstance(query, SlayerQuery) else SlayerQuery.model_validate(query)
        return await engine.execute(q, dry_run=dry_run)


def metric_total(resp: SlayerResponse, *, metric: str) -> float:
    """Sum of the ``metric`` column (matched by suffix) over every row."""
    if not resp.data:
        return 0.0
    key = next(k for k in resp.data[0] if k == metric or k.endswith(f".{metric}") or k.endswith(f"_{metric}"))
    return sum(float(r[key]) for r in resp.data if r[key] is not None)


def by_dimension(resp: SlayerResponse, *, dim_suffix: str) -> dict:
    """``{dimension value: float(other column)}`` for a one-dimension, one-metric result."""
    row = resp.data[0]
    dim = next(k for k in row if k.endswith(dim_suffix))
    val = next(k for k in row if k != dim)
    return {r[dim]: float(r[val]) for r in resp.data}
