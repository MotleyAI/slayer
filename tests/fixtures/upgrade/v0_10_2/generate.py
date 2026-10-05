"""Regenerate the 0.10.2 upgrade corpus; run under a venv with ``motley-slayer==0.10.2`` (not in CI).

    uv venv /tmp/v0102 && uv pip install --python /tmp/v0102 motley-slayer==0.10.2 duckdb==1.5.2
    /tmp/v0102/bin/python tests/fixtures/upgrade/v0_10_2/generate.py
"""

import asyncio
import json
import shutil
import subprocess
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path

import duckdb

import slayer

from slayer.core.models import Aggregation, AggregationParam, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.engine_factory import get_engine, invalidate_engine
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

OUT = Path(__file__).resolve().parent
DB = OUT / "data.duckdb"
LITE_DB = OUT / "data.sqlite"
YAML_STORE = OUT / "yaml_store"
SQLITE_STORE = OUT / "sqlite_store.db"
EXPECTED = OUT / "expected.json"
SHOP = "shop"
CUBE = "cube"
LITE = "lite"

_CUSTOMERS = [
    (1, "Ann", "North", "2023-06-01 00:00:00"),
    (2, "Bob", "North", "2024-02-01 00:00:00"),
    (3, "Cid", "South", "2024-05-01 00:00:00"),
    (4, "Dee", "East", "2022-01-01 00:00:00"),
]
_ORDERS = [
    (1, 1, "ok", "2023-12-31 23:00:00", 10.0),
    (2, 1, "new", "2024-01-01 00:00:00", 20.0),
    (3, 1, "ok", "2024-01-01 01:30:00", 5.0),
    (4, 2, "ok", "2024-02-15 12:00:00", 30.0),
    (5, 2, "2024/01/01", "2024-03-01 00:00:00", 7.0),
    (6, 3, "ok", "2024-06-30 00:00:00", 40.0),
    (7, 3, "new", "2024-06-30 03:00:00", 50.0),
    (8, 4, "ok", "2024-12-31 23:59:59", 60.0),
    (9, 4, "ok", "2025-01-01 00:00:00", 70.0),
]

_CUBE_ORDERS = """cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: customers
        relationship: many_to_one
        sql: "{CUBE}.customer_id = {customers}.id"
    dimensions:
      - {name: order_id, sql: order_id, type: number, primary_key: true}
      - {name: status, sql: status, type: string}
      - {name: ordered_at, sql: ordered_at, type: time}
    measures:
      - {name: amount, sql: amount, type: sum}
"""
_CUBE_CUSTOMERS = """cubes:
  - name: customers
    sql_table: customers
    dimensions:
      - {name: name, sql: name, type: string}
      - {name: region, sql: region, type: string}
"""

_REV = {"formula": "amount:sum", "name": "rev"}
_MONTH = {"dimension": "ordered_at", "granularity": "month"}


def _stage_model(*, name: str, stage: dict) -> dict:
    """A one-stage query-backed model over ``orders``."""
    return {"name": name, "data_source": SHOP, "source_queries": [{"source_model": "orders", **stage}]}


def _date_range_model(*, name: str, date_range: list) -> dict:
    return _stage_model(name=name, stage={
        "time_dimensions": [{**_MONTH, "date_range": date_range}], "measures": [_REV]})


def _filter_model(*, name: str, filters: list) -> dict:
    return _stage_model(name=name, stage={"dimensions": ["status"], "measures": [_REV], "filters": filters})


QUERY_BACKED = [
    {"name": "status_rank", "data_source": SHOP, "source_queries": [
        {"name": "by_status", "source_model": "orders", "dimensions": ["status"], "measures": [_REV]},
        {"source_model": "by_status", "dimensions": ["status"], "measures": [{"formula": "rev:sum", "name": "total"}],
         "order": [{"column": "rank(rev:sum)", "direction": "asc"}], "limit": 2},
    ]},
    _date_range_model(name="dr_offset_z", date_range=["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]),
    _date_range_model(name="dr_offset_plus2", date_range=["2024-01-01T02:00:00+02:00", "2024-06-30T02:00:00+02:00"]),
    _date_range_model(name="dr_slashed", date_range=["2024/01/01", "2024/06/29"]),
    _date_range_model(name="dr_empty", date_range=[]),
    _date_range_model(name="dr_one", date_range=["2024-01-01"]),
    _date_range_model(name="dr_three", date_range=["2024-01-01", "2024-06-30", "2024-12-31"]),
    _filter_model(name="flt_offset", filters=["ordered_at >= '2024-01-01T00:00:00Z'"]),
    _filter_model(name="flt_slashed", filters=["'2024/03/01' <= ordered_at"]),
    _filter_model(name="flt_range", filters=[
        "ordered_at >= '2024-01-01T00:00:00Z' and ordered_at <= '2024-06-30T00:00:00+02:00'"]),
    _filter_model(name="flt_in", filters=["ordered_at IN ('2024/01/01', '2024/03/01')"]),
    _filter_model(name="flt_joined", filters=["customers.signed_up_at >= '2024-01-01T00:00:00Z'"]),
    _filter_model(name="flt_text", filters=["status = '2024/01/01'"]),
]

MEMORIES = [
    {"id": "m_rank", "learning": "Top statuses by revenue.", "entities": [f"{SHOP}.orders"],
     "query": {"source_model": "status_rank", "dimensions": ["status"], "measures": [
         {"formula": "total:sum", "name": "total_sum"}]}},
    {"id": "m_rank_order", "learning": "Statuses ranked by revenue.", "entities": [f"{SHOP}.orders"],
     "query": {"source_model": "orders", "dimensions": ["status"], "measures": [_REV],
               "order": [{"column": "rank(amount:sum)", "direction": "asc"}], "limit": 2}},
    {"id": "m_offset", "learning": "Monthly revenue for the first half.", "entities": [f"{SHOP}.orders"],
     "query": {"source_model": "orders", "time_dimensions": [
         {**_MONTH, "date_range": ["2024-01-01T00:00:00+02:00", "2024-06-30T00:00:00+02:00"]}],
         "measures": [_REV]}},
]


def _over(model: str, *, dims: list, measures: list, **extra) -> dict:
    return {"source_model": model, "dimensions": dims, "measures": measures, **extra}


QUERIES = {
    "shop_by_region": _over("orders", dims=["customers.region"], measures=[{"formula": "sum(amount)", "name": "rev"}]),
    "shop_sum_n": _over("orders", dims=["status"], measures=[{"formula": "sum_n(amount)", "name": "rev2"}]),
    "cube_by_region": _over("orders", dims=["customers.region"], measures=[{"formula": "sum(amount_col)", "name": "rev"}]),
    "lite_sum_n": _over("sales", dims=["status"], measures=[{"formula": "sum_n(amount)", "name": "rev2"}]),
    "status_rank": _over("status_rank", dims=["status"], measures=[{"formula": "sum(total)", "name": "total_sum"}]),
    **{m["name"]: _over(m["name"], dims=[], measures=[{"formula": "sum(rev)", "name": "rev_sum"}])
       for m in QUERY_BACKED if m["name"].startswith(("dr_", "flt_"))},
}


def _seed_db() -> None:
    DB.unlink(missing_ok=True)
    con = duckdb.connect(str(DB))
    try:
        con.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, region VARCHAR, signed_up_at TIMESTAMP)")
        con.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS)
        con.execute(
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "status VARCHAR, ordered_at TIMESTAMP, amount DOUBLE)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS)
    finally:
        con.close()


def _seed_lite() -> None:
    LITE_DB.unlink(missing_ok=True)
    datasource = DatasourceConfig(name=LITE, type="sqlite", database=str(LITE_DB))
    try:
        with get_engine(datasource).begin() as conn:
            conn.exec_driver_sql("CREATE TABLE sales (id INTEGER PRIMARY KEY, status TEXT, amount REAL)")
            conn.exec_driver_sql("INSERT INTO sales VALUES (?,?,?)", [(o[0], o[2], o[4]) for o in _ORDERS])
    finally:
        invalidate_engine(datasource)


def _import_cube(*, storage_path: Path) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        model_dir = Path(tmp) / "model" / "cubes"
        model_dir.mkdir(parents=True)
        (model_dir / "orders.yml").write_text(_CUBE_ORDERS)
        (model_dir / "customers.yml").write_text(_CUBE_CUSTOMERS)
        slayer = Path(sys.executable).parent / "slayer"
        subprocess.run(
            [str(slayer), "import-cube", str(Path(tmp) / "model"), "--datasource", CUBE,
             "--storage", str(storage_path), "--report", str(Path(tmp) / "report.json")],
            check=True)


async def _populate(storage: StorageBackend) -> None:
    for name in (SHOP, CUBE):
        await storage.save_datasource(DatasourceConfig(name=name, type="duckdb", database=str(DB)))
    await storage.save_datasource(DatasourceConfig(name=LITE, type="sqlite", database=str(LITE_DB)))
    await storage.set_datasource_priority([SHOP, CUBE, LITE])
    shop = await storage.get_datasource(SHOP)
    assert shop is not None
    await ingest_datasource_idempotent(datasource=shop, storage=storage)
    orders = await storage.get_model("orders", data_source=SHOP)
    assert orders is not None
    orders.aggregations.append(Aggregation(
        name="sum_n", formula="SUM({value}) * CAST('{n}' AS DOUBLE)", params=[AggregationParam(name="n", sql="2")]))
    orders.joins.append(ModelJoin.model_validate(
        {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"}))
    await storage.save_model(orders)
    lite = await storage.get_datasource(LITE)
    assert lite is not None
    await ingest_datasource_idempotent(datasource=lite, storage=storage)
    sales = await storage.get_model("sales", data_source=LITE)
    assert sales is not None
    sales.aggregations.append(Aggregation(
        name="sum_n", formula="SUM({value}) * '{n}'", params=[AggregationParam(name="n", sql="2")]))
    await storage.save_model(sales)
    for raw in QUERY_BACKED:
        await storage.save_model(SlayerModel.model_validate(raw))
    for mem in MEMORIES:
        await storage.save_memory(
            id=mem["id"], learning=mem["learning"], entities=mem["entities"],
            query=SlayerQuery.model_validate(mem["query"]))


def _cell(value):
    return round(value, 6) if isinstance(value, float) else (None if value is None else str(value))


async def _record(storage: StorageBackend) -> dict:
    engine = SlayerQueryEngine(storage=storage)
    queries = {**QUERIES, **{f"memory:{m['id']}": m["query"] for m in MEMORIES}}
    out = {}
    for key, query in queries.items():
        ds = CUBE if key.startswith("cube_") else LITE if key.startswith("lite_") else SHOP
        resp = await engine.execute(SlayerQuery.model_validate(query), data_source=ds)
        ordered = "order" in query
        rows = [{k.rsplit(".", 1)[-1]: _cell(v) for k, v in row.items()} for row in resp.data]
        out[key] = {"query": query, "data_source": ds, "ordered": ordered,
                    "rows": rows if ordered else sorted(rows, key=lambda r: json.dumps(r, sort_keys=True))}
    return out


async def _relativise_datasources(storage: StorageBackend) -> None:
    """Re-save each datasource with its bare data file name; the test points it at a copy."""
    for name, kind, data in ((SHOP, "duckdb", DB), (CUBE, "duckdb", DB), (LITE, "sqlite", LITE_DB)):
        await storage.save_datasource(DatasourceConfig(name=name, type=kind, database=data.name))


async def main() -> None:
    assert version("motley-slayer") == "0.10.2" and "site-packages" in slayer.__file__, slayer.__file__
    _seed_db()
    _seed_lite()
    shutil.rmtree(YAML_STORE, ignore_errors=True)
    SQLITE_STORE.unlink(missing_ok=True)
    stores = ((YAMLStorage(base_dir=str(YAML_STORE)), YAML_STORE),
              (SQLiteStorage(db_path=str(SQLITE_STORE)), SQLITE_STORE))
    results = []
    for storage, path in stores:
        await _populate(storage)
        _import_cube(storage_path=path)
        results.append(await _record(storage))
    for key in results[0]:
        assert results[0][key] == results[1][key], (key, results[0][key], results[1][key])
    EXPECTED.write_text(json.dumps(results[0], indent=2, sort_keys=True) + "\n")
    for storage, _ in stores:
        await _relativise_datasources(storage)


if __name__ == "__main__":
    asyncio.run(main())
