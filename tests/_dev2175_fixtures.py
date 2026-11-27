"""Shared seed, dataset builders and query helpers for the row-definition name-rule tests.

Table ``orders(id, status, state, amount, customer_id[, tags])`` with three rows; ``customers(id, name)``;
``lines(id, o_totalprice, amount)`` for the Ossie-meaning scenario.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.field_expression_migration import FIELD_EXPRESSION_VERSION
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import errors_in
from tests._engine_helpers import owned_query_engine

DS = "shop"
BACKENDS = ("sqlite", "duckdb")
#: A stored dataset version written before this change.
PREVIOUS = FIELD_EXPRESSION_VERSION - 1
TOKEN = {"sqlite": "SQLITE", "duckdb": "DUCKDB"}

ORDERS = [
    (1, "paid", "open", 10, 101),
    (2, "paid", "paid", 20, 101),
    (3, "open", "x", 5, 102),
]
CUSTOMERS = [(101, "Ann"), (102, "Bo")]
LINES = [(1, 100, 1), (2, 200, 2)]

_DDL = (
    "CREATE TABLE orders (id INTEGER, status VARCHAR, state VARCHAR, amount INTEGER, customer_id INTEGER{tags})",
    "CREATE TABLE customers (id INTEGER, name VARCHAR)",
    "CREATE TABLE lines (id INTEGER, o_totalprice INTEGER, amount INTEGER)",
)


def seed(db_path: str, *, dialect: str) -> None:
    """Create and fill the tables in a SQLite or DuckDB file (DuckDB adds ``orders.tags``)."""
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            con.execute(_DDL[0].format(tags=", tags VARCHAR[]"))
            con.execute(_DDL[1])
            con.execute(_DDL[2])
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)",
                            [(*r, t) for r, t in zip(ORDERS, [["a", "b"], ["b"], None], strict=True)])
            con.executemany("INSERT INTO customers VALUES (?,?)", CUSTOMERS)
            con.executemany("INSERT INTO lines VALUES (?,?,?)", LINES)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        cur.execute(_DDL[0].format(tags=""))
        cur.execute(_DDL[1])
        cur.execute(_DDL[2])
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS)
        cur.executemany("INSERT INTO customers VALUES (?,?)", CUSTOMERS)
        cur.executemany("INSERT INTO lines VALUES (?,?,?)", LINES)


def run_statements(db_path: str, *, dialect: str, statements: Sequence[str]) -> None:
    """Run ``statements`` against the SQLite or DuckDB file."""
    if not statements:
        return
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for statement in statements:
                con.execute(statement)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for statement in statements:
            cur.execute(statement)


def datasource(*, dialect: str, db_path: str) -> DatasourceConfig:
    return DatasourceConfig(name=DS, type=dialect, database=db_path)


# ── documents ─────────────────────────────────────────────────────────────

def orders_doc(*fields: dict[str, Any], **extra: Any) -> dict[str, Any]:
    """Dataset ``orders`` over table ``orders`` with ``id`` (primary key) plus ``fields``."""
    return {"name": "orders", "sql_table": "orders", "data_source": DS,
            "fields": [{"name": "id", "primary_key": True, "datatype": "Integer"}, *fields], **extra}


def customers_doc(*fields: dict[str, Any]) -> dict[str, Any]:
    return {"name": "customers", "sql_table": "customers", "data_source": DS,
            "fields": [{"name": "id", "primary_key": True, "datatype": "Integer"}, {"name": "name"}, *fields]}


def to_customers(**extra: Any) -> dict[str, Any]:
    """Relationship ``customers``: orders.customer_id → customers.id, many-to-one."""
    return {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"],
            "cardinality": "many_to_one", **extra}


def fld(name: str, expression: Any = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, **extra}
    if expression is not None:
        out["expression"] = expression
    return out


def entry(dialect: str, text: str) -> dict[str, str]:
    return {"dialect": dialect, "expression": text}


def dataset(doc: dict[str, Any]) -> SlayerDataset:
    return SlayerDataset.model_validate(doc)


def dumped_field(ds: SlayerDataset, name: str) -> dict[str, Any]:
    """The field ``name`` as the dataset dumps it."""
    for f in ds.model_dump(mode="json", exclude_none=True)["fields"]:
        if f["name"] == name:
            return f
    raise AssertionError(f"no field {name!r} on {ds.name}")


# ── engines ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def engine_with(
    *, dialect: str, docs: Sequence[dict[str, Any]] = (), validate: bool = True, extra: Sequence[str] = (),
) -> AsyncGenerator[tuple[SlayerQueryEngine, StorageBackend, str]]:
    """A seeded ``dialect`` datasource (plus ``extra`` statements) whose store holds ``docs``, saved through the engine
    when ``validate``."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, f"shop.{dialect}")
        seed(db_path, dialect=dialect)
        run_statements(db_path, dialect=dialect, statements=extra)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        async with owned_query_engine(storage=storage, datasources=[datasource(dialect=dialect, db_path=db_path)]) as eng:
            for doc in docs:
                if validate:
                    await eng.save_dataset(dataset(doc))
                else:
                    await storage.save_dataset(dataset(doc), _validate=False)
            yield eng, storage, db_path


async def rows(engine: SlayerQueryEngine, payload: dict[str, Any]) -> list[dict[str, Any]]:
    return (await engine.execute(payload)).data


async def by_id(engine: SlayerQueryEngine, *, field: str, ds: str = "orders") -> list[Any]:
    """``field`` per row of ``ds``, ordered by ``id``."""
    out = await rows(engine, {"population": ds, "dimensions": [f"{ds}.id", f"{ds}.{field}"],
                              "order": [{"field": f"{ds}.id"}]})
    return [r[f"{ds}.{field}"] for r in out]


async def counts_by(engine: SlayerQueryEngine, *, field: str, ds: str = "orders") -> dict[Any, int]:
    out = await rows(engine, {"population": ds, "dimensions": [f"{ds}.{field}"],
                              "metrics": [{"metric": f"count({ds}.*)", "alias": "n"}]})
    return {r[f"{ds}.{field}"]: r["n"] for r in out}


async def metric_value(engine: SlayerQueryEngine, *, metric: str, ds: str = "orders") -> Any:
    (row,) = await rows(engine, {"population": ds, "metrics": [{"metric": metric, "alias": "v"}]})
    return row["v"]


async def ids(engine: SlayerQueryEngine, ds: str = "orders") -> list[int]:
    out = await rows(engine, {"population": ds, "dimensions": [f"{ds}.id"], "order": [{"field": f"{ds}.id"}]})
    return [r[f"{ds}.id"] for r in out]


def assert_refused(exc: BaseException, *names: str, kind: type[BaseException] = SlayerError) -> None:
    """``exc`` (or an error a validation error wraps) is a ``kind`` whose message names every one of ``names``."""
    found = [e for e in errors_in(exc) if isinstance(e, kind)]
    assert found, f"expected a {kind.__name__}, got {exc!r}"
    assert any(all(n in str(e) for n in names) for e in found), f"{names} not all named in {found}"
