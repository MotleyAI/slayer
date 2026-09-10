"""Shared fixtures for DEV-1847 — second-order aggregation (re-aggregation).

Axiom 6: an aggregate is a dataset, so it can be aggregated again. An outer
aggregation whose operand is an attached (partitioned) aggregate consumes the
inner producer's *grain rows*, never the broadcast query rows.

Two datasets, both datasource ``test`` (dual-engine: one seeded into SQLite AND
DuckDB via ``make_exec_engine``):

``sales`` — denormalized (region, city, product, amount); ``id`` is the PK.
  The city ``Alpha`` appears in BOTH North and South with different totals, so
  ``partition_by=[city, region]`` and ``partition_by=city`` diverge. Region
  ``Gap`` carries a NULL-city cell; region ``Void``'s only rows have NULL
  ``amount`` (sum → NULL, count → 0).

  sales rows (region, city, product, amount):
    North Alpha  P 10 | North Alpha P 10 | North Alpha Q 10   -> Alpha 30 (3 rows)
    North Beta   Q 60                                          -> Beta  60 (1 row)
    South Alpha  P 20 | South Alpha P 20                       -> Alpha 40 (2 rows)
    South Gamma  Q 100                                         -> Gamma 100 (1 row)
    East  Delta  P 50 | East Epsilon P 50 | East Zeta Q 80     -> 50/50/80
    Gap   NULL   P 7  | Gap Kappa    P 8                        -> NULL 7, Kappa 8
    Void  Xi     P NULL | Void Xi    P NULL                     -> NULL (all-null)

``regions``/``customers``/``corders`` — normalized chain for to-one attribution:
  regions(id,name): 1 North, 2 South
  customers(id,region_id): 1->1, 2->1, 3->2            (customers.id PK)
  corders(id,customer_id,amount): c1 {10,20}=30, c2 {40}=40, c3 {100}=100
  ``avg(sum(amount, partition_by=customer_id))`` by region: North 35, South 100.

Every oracle below is re-derived from the raw rows by the fixture smoke test
(``test_dev1847_fixtures_smoke.py``) so a hand-arithmetic slip fails loudly.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1836_fixtures import broadcast_warnings, rows_by
from tests._dev1841_fixtures import associated_warnings
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #
def sales_model() -> SlayerModel:
    return SlayerModel(
        name="sales", data_source="test", sql_table="sales",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="city", type=DataType.TEXT),
            Column(name="product", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            # Measure-local filtered column: amount restricted to product 'Q'.
            # Sparse by design — a (city, region) cell with no Q row has no value.
            Column(name="q_amount", type=DataType.DOUBLE, sql="amount",
                   filter="product = 'Q'"),
        ],
        # A model-defined custom aggregation (SUM) — exercises a custom OUTER
        # aggregation over the inner producer's grain rows.
        aggregations=[Aggregation(name="dsum", formula="SUM({value})")],
    )


def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                         cardinality=JoinCardinality.MANY_TO_ONE)],
    )


def corders_model() -> SlayerModel:
    return SlayerModel(
        name="corders", data_source="test", sql_table="corders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                         cardinality=JoinCardinality.MANY_TO_ONE)],
    )


def dev1847_models() -> List[SlayerModel]:
    """``[sales, regions, customers, corders]`` — sales first (engine host)."""
    return [sales_model(), regions_model(), customers_model(), corders_model()]


def source_queries_equiv_model() -> SlayerModel:
    """The manual two-stage encoding DEV-1847 replaces: stage 1 sums at the
    (region, city) grain, stage 2 averages per region — the same number the
    ``avg(sum(amount, partition_by=[city, region]))`` re-aggregation must match."""
    return SlayerModel(
        name="avg_city_total_by_region", data_source="test",
        source_queries=[
            SlayerQuery(
                name="city_region_total", source_model="sales",
                dimensions=["region", "city"],
                measures=[ModelMeasure(formula="amount:sum", name="city_total")],
            ),
            SlayerQuery(
                source_model="city_region_total",
                dimensions=["region"],
                measures=[ModelMeasure(formula="city_total:avg", name="acr")],
            ),
        ],
    )


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def sales_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    return SlayerQuery(**kw)


def chain_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "corders")
    return SlayerQuery(**kw)


async def gen(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    """Emit SQL for ``query`` against the sales graph at ``dialect`` (dry-run)."""
    models = dev1847_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=False)


# Inner partitioned-aggregate formulas (these parse today; the OUTER wrap is the
# new surface). Spelled functionally per the proposal; colon form is equivalent.
INNER_CR = "sum(amount, partition_by=[city, region])"  # [city, region] grain
INNER_CITY = "sum(amount, partition_by=city)"          # [city] grain (spans regions)
INNER_CRP = "sum(amount, partition_by=[city, region, product])"


def reagg(outer: str, inner: str, *, name: str, **kw) -> ModelMeasure:
    """An ``<outer>(<inner>)`` re-aggregation measure, e.g. reagg('avg', INNER_CR)."""
    extra = "".join(f", {k}={v}" for k, v in kw.items())
    return ModelMeasure(formula=f"{outer}({inner}{extra})", name=name)


# --------------------------------------------------------------------------- #
# Warning helpers.
# --------------------------------------------------------------------------- #
#: The warning kind DEV-1847 adds for an operand grain == outer grain.
DEGENERATE_KIND = "degenerate_reaggregation"


def degenerate_warnings(resp) -> list:
    """Response warnings announcing a degenerate (identity) re-aggregation."""
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == DEGENERATE_KIND]


def warning_kinds(resp) -> list:
    return [getattr(w, "kind", None) for w in (resp.warnings or [])]


# --------------------------------------------------------------------------- #
# Oracles (hand-computed; re-derived by the smoke test).
# --------------------------------------------------------------------------- #
#: avg(sum(amount, partition_by=[city, region])) by region — the headline oracle.
AVG_CITY_TOTAL_BY_REGION = {"North": 45.0, "South": 70.0, "East": 60.0, "Gap": 10.0}
#: the WRONG row-count-weighted value the naive broadcast would give.
ROW_WEIGHTED_WRONG = {"North": 37.5, "South": 60.0}
#: partition_by=city alone, re-aggregated globally (broadcast default) by region.
#: city totals [Alpha 70, Beta 60, Gamma 100, Delta 50, Epsilon 50, Zeta 80,
#: NULL 12, Kappa 8] -> 430 / 8 = 53.75.
BROADCAST_GLOBAL_AVG_CITY = 53.75
#: partition_by=city alone, re-aggregated per region under associate mode.
ASSOCIATE_AVG_CITY_BY_REGION = {"North": 65.0, "South": 85.0}
#: avg(sum(amount)) by region == per-region total (degenerate identity).
#: Also the oracle for the custom OUTER aggregation ``dsum`` (SUM of city totals).
DEGENERATE_SUM_BY_REGION = {"North": 90.0, "South": 140.0, "East": 180.0, "Gap": 20.0}
#: avg(sum(amount, partition_by=[])) — the grand total (keyless identity).
KEYLESS_GRAND_TOTAL = 430.0
#: count / min / max / median / count_distinct of East's city totals [50, 50, 80].
EAST_CITY_TOTALS = [50.0, 50.0, 80.0]
COUNT_CITY_CELLS_BY_REGION = {"North": 2, "South": 2, "East": 3}
#: depth-3: max over regions of (avg over cities of [city,region,product] totals).
DEPTH3_MAX_AVG_BY_PRODUCT = {"P": 50.0, "Q": 100.0}
#: Gap region: null-grain cell (7+5=12) and Kappa (8) -> avg(12, 8) = 10.
GAP_AVG = 10.0
#: Gap's NULL-city cell total — two NULL rows coalesce into one cell.
GAP_NULL_CELL_TOTAL = 12.0
#: to-one chain: avg(sum(amount, partition_by=customer_id)) by region name.
CHAIN_AVG_BY_REGION = {"North": 35.0, "South": 100.0}
#: composite operand ``q_amount:sum(pb=[city,region]) + amount:sum(pb=region)``
#: re-aggregated by region (a NULL constituent keeps its cell, not dropped).
COMPOSITE_AVG_BY_REGION = {"North": 125.0, "South": 240.0, "East": 260.0}
#: row-phase filter ``product='P'`` reaches the inner producer, shifting totals.
ROWPHASE_P_AVG_BY_REGION = {"North": 20.0, "South": 40.0, "East": 50.0, "Gap": 10.0}
#: shape B: spend_band = 'hi' when the [city,region] total > 45 else 'lo'.
#:   amount:sum(partition_by=spend_band) — the band total, broadcast.
SHAPE_B_BAND_TOTAL = {"hi": 340.0, "lo": 90.0}
#:   plain amount:sum grouped by (region, spend_band).
SHAPE_B_GROUP_SUM = {
    ("North", "lo"): 30.0, ("North", "hi"): 60.0,
    ("South", "lo"): 40.0, ("South", "hi"): 100.0,
    ("East", "hi"): 180.0, ("Gap", "lo"): 20.0, ("Void", "lo"): None,
}
#:   avg(sum(amount, partition_by=[city,region])) grouped by (region, spend_band)
#:   — spend_band is determined from the inner grain, so it partitions exactly.
SHAPE_B_ACR = {
    ("North", "lo"): 30.0, ("North", "hi"): 60.0,
    ("South", "lo"): 40.0, ("South", "hi"): 100.0,
    ("East", "hi"): 60.0, ("Gap", "lo"): 10.0, ("Void", "lo"): None,
}
SHAPE_B_BAND_THRESHOLD = 45.0
SPEND_BAND_EXPR = (
    f"CASE WHEN amount:sum(partition_by=[city, region]) > {SHAPE_B_BAND_THRESHOLD} "
    f"THEN 'hi' ELSE 'lo' END"
)


# --------------------------------------------------------------------------- #
# Execution dataset.
# --------------------------------------------------------------------------- #
_SALES_ROWS = [
    # (id, region, city, product, amount)
    (1, "North", "Alpha", "P", 10.0),
    (2, "North", "Alpha", "P", 10.0),
    (3, "North", "Alpha", "Q", 10.0),
    (4, "North", "Beta", "Q", 60.0),
    (5, "South", "Alpha", "P", 20.0),
    (6, "South", "Alpha", "P", 20.0),
    (7, "South", "Gamma", "Q", 100.0),
    (8, "East", "Delta", "P", 50.0),
    (9, "East", "Epsilon", "P", 50.0),
    (10, "East", "Zeta", "Q", 80.0),
    # Gap: two NULL-city rows (7 + 5) coalesce into ONE null-grain cell (12),
    # distinct from Kappa (8) — so Gap's avg of city totals is avg(12, 8) = 10.
    (11, "Gap", None, "P", 7.0),
    (12, "Gap", None, "P", 5.0),
    (13, "Gap", "Kappa", "P", 8.0),
    (14, "Void", "Xi", "P", None),
    (15, "Void", "Xi", "P", None),
]
_REGIONS_ROWS = [(1, "North"), (2, "South")]
_CUSTOMERS_ROWS = [(1, 1), (2, 1), (3, 2)]
_CORDERS_ROWS = [(1, 1, 10.0), (2, 1, 20.0), (3, 2, 40.0), (4, 3, 100.0)]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, "
                "city TEXT, product TEXT, amount REAL)")
    cur.executemany("INSERT INTO sales VALUES (?,?,?,?,?)", _SALES_ROWS)
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER)")
    cur.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMERS_ROWS)
    cur.execute("CREATE TABLE corders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
                "amount REAL)")
    cur.executemany("INSERT INTO corders VALUES (?,?,?)", _CORDERS_ROWS)
    con.commit()
    con.close()


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE sales (id INTEGER, region VARCHAR, city VARCHAR, "
                "product VARCHAR, amount DOUBLE)")
    con.executemany("INSERT INTO sales VALUES (?,?,?,?,?)", _SALES_ROWS)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR)")
    con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    con.execute("CREATE TABLE customers (id INTEGER, region_id INTEGER)")
    con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMERS_ROWS)
    con.execute("CREATE TABLE corders (id INTEGER, customer_id INTEGER, amount DOUBLE)")
    con.executemany("INSERT INTO corders VALUES (?,?,?)", _CORDERS_ROWS)
    con.close()


async def _engine_for(*, dialect: str, db_path: str,
                      models: List[SlayerModel]) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path))
    for model in models:
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_sqlite(db_path)
        else:
            _seed_duckdb(db_path)
        yield await _engine_for(
            dialect=dialect, db_path=db_path,
            models=models if models is not None else dev1847_models())


def region_key(resp) -> dict:
    """Rows keyed by the ``sales.region`` dimension cell."""
    return rows_by(resp, "sales.region")


__all__ = [
    "Column", "DataType", "ModelJoin", "ModelMeasure", "SlayerModel",
    "SlayerQuery", "JoinCardinality",
    "sales_model", "regions_model", "customers_model", "corders_model",
    "dev1847_models", "source_queries_equiv_model",
    "sales_q", "chain_q", "reagg", "region_key", "rows_by", "gen",
    "make_exec_engine",
    "broadcast_warnings", "associated_warnings", "degenerate_warnings",
    "warning_kinds", "DEGENERATE_KIND",
    "INNER_CR", "INNER_CITY", "INNER_CRP",
    "AVG_CITY_TOTAL_BY_REGION", "ROW_WEIGHTED_WRONG",
    "BROADCAST_GLOBAL_AVG_CITY", "ASSOCIATE_AVG_CITY_BY_REGION",
    "DEGENERATE_SUM_BY_REGION", "KEYLESS_GRAND_TOTAL",
    "EAST_CITY_TOTALS", "COUNT_CITY_CELLS_BY_REGION",
    "DEPTH3_MAX_AVG_BY_PRODUCT", "GAP_AVG", "GAP_NULL_CELL_TOTAL",
    "CHAIN_AVG_BY_REGION",
    "COMPOSITE_AVG_BY_REGION", "ROWPHASE_P_AVG_BY_REGION",
    "SHAPE_B_BAND_TOTAL", "SHAPE_B_GROUP_SUM", "SHAPE_B_ACR",
    "SHAPE_B_BAND_THRESHOLD", "SPEND_BAND_EXPR",
    "_SALES_ROWS", "_CORDERS_ROWS", "_CUSTOMERS_ROWS", "_REGIONS_ROWS",
]
