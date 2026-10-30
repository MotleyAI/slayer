"""Raw stores whose models define aggregations named after library functions, written as the previous release did.

Physical tables::

    t         (id, a, cust_id, s)    sum(a) = 7.0, max(a) = 4.0
    customers (id, name)
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator, Sequence
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql import engine_factory
from slayer.storage.sqlite_conn import transaction
from tests._stored_upgrade_fixtures import DS, raw_store

#: SlayerModel / SlayerQuery / Memory versions the previous release (v1.4.1) wrote.
PREVIOUS_MODEL_VERSION = 15
PREVIOUS_QUERY_VERSION = 6
PREVIOUS_MEMORY_VERSION = 4

T_ROWS = [(1, 1.0, 1, "x"), (2, 2.0, 1, "y"), (3, 4.0, 2, "z")]
CUSTOMER_ROWS = [(1, "abc"), (2, "xyz")]
SUM_A = 7.0
MAX_A = 4.0

_DDL = (
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a DOUBLE, cust_id INTEGER, s TEXT)",
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
)


def seed(db_path: str, *, dialect: str) -> None:
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        with contextlib.closing(duckdb.connect(db_path)) as con:
            for stmt in _DDL:
                con.execute(stmt)
            con.executemany("INSERT INTO t VALUES (?,?,?,?)", T_ROWS)
            con.executemany("INSERT INTO customers VALUES (?,?)", CUSTOMER_ROWS)
        return
    with transaction(db_path) as cur:
        for stmt in _DDL:
            cur.execute(stmt)
        cur.executemany("INSERT INTO t VALUES (?,?,?,?)", T_ROWS)
        cur.executemany("INSERT INTO customers VALUES (?,?)", CUSTOMER_ROWS)


def agg(name: str, formula: str = "SUM({value})", **params: str) -> dict:
    out: dict[str, Any] = {"name": name, "formula": formula}
    if params:
        out["params"] = [{"name": k, "sql": v} for k, v in params.items()]
    return out


def t_doc(
    *, name: str = "t", aggregations: Sequence[dict] = (), measures: Sequence[dict] = (),
    version: int = PREVIOUS_MODEL_VERSION,
) -> dict:
    """Stored model over ``t`` joining ``customers``."""
    doc: dict[str, Any] = {
        "version": version, "name": name, "sql_table": "t", "data_source": DS,
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "a", "type": "DOUBLE"},
            {"name": "cust_id", "type": "INT"},
            {"name": "s", "type": "TEXT"},
        ],
        "joins": [{"target_model": "customers", "join_pairs": [["cust_id", "id"]], "cardinality": "many_to_one"}],
    }
    if aggregations:
        doc["aggregations"] = list(aggregations)
    if measures:
        doc["measures"] = list(measures)
    return doc


def customers_doc(*, version: int = PREVIOUS_MODEL_VERSION) -> dict:
    return {
        "version": version, "name": "customers", "sql_table": "customers", "data_source": DS,
        "columns": [{"name": "id", "type": "INT", "primary_key": True}, {"name": "name", "type": "TEXT"}],
    }


def saved_query_doc(*, name: str, stage: dict, version: int = PREVIOUS_MODEL_VERSION) -> dict:
    """A query-backed model (run by name) with one stored stage."""
    return {"version": version, "name": name, "data_source": DS,
            "source_queries": [{"version": PREVIOUS_QUERY_VERSION, **stage}]}


def memory_doc(*, memory_id: str, query: dict) -> dict:
    return {"version": PREVIOUS_MEMORY_VERSION, "id": memory_id, "learning": f"learning {memory_id}",
            "entities": [], "query": {"version": PREVIOUS_QUERY_VERSION, **query}}


@contextlib.asynccontextmanager
async def stored_engine(
    *, models: Sequence[dict], memories: Sequence[dict] = (), dialect: str = "sqlite",
) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over a raw YAML store of ``models`` (plus ``customers``) and ``memories``."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, f"live.{'duckdb' if dialect == 'duckdb' else 'db'}")
        seed(db_path, dialect=dialect)
        ds = DatasourceConfig(name=DS, type=dialect, database=db_path)
        storage = await raw_store(backend="yaml", base=os.path.join(tmp, "store"), datasource=ds,
                                  models=[*models, customers_doc()], memories=memories)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine
        finally:
            engine.close()
            engine_factory.invalidate_engine(ds)


def measure(formula: str, name: str = "v") -> dict:
    return {"formula": formula, "name": name}


def value(resp: SlayerResponse, *, name: str = "v") -> Any:
    """The single-row value of the result column ending in ``name``."""
    assert len(resp.data) == 1, resp.data
    row = resp.data[0]
    key = next(k for k in row if k == name or k.endswith(f".{name}"))
    return row[key]
