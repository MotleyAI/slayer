"""Shared fixtures for DEV-1832 — cross-model expression aggregation.

Three graphs in one seeded database:

* **Graph A** — the DEV-1840/1900 ``orders → customers → regions
  (→ region_events, 1:N fanning) / stores`` graph, extended with the columns
  DEV-1832 exercises: ``orders.cost/quantity/weight``, ``customers.discount``,
  the filtered ``customers.north_spend`` (``spend`` where ``regions.name =
  'North'``), and a customers-owned ``wsum`` custom aggregation. Used for the
  home-dataset, cross-model-expression and filtered-joined-leaf scenarios.
* **Graph B** — the DEV-1847 denormalized ``sales`` model (region, city,
  product, amount, quantity, unit_price, filtered ``q_amount``). Its
  re-aggregation / mixed-source oracles are re-used verbatim; DEV-1832 adds the
  filtered-operand and transform-constituent oracles over it.
* **monthly** — a tiny ``(region, ordered_at, amount)`` table so the grained-
  transform (cumsum) constituent oracle is a clean two-region running total.

Every absolute oracle below is re-derived from the raw rows by
``tests/test_dev1832_fixtures_smoke.py`` — the double-entry guard against
hand-arithmetic drift.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1840_fixtures import rows_by
from tests._dev1847_fixtures import (
    AVG_CITY_TOTAL_BY_REGION,
    MIXED_SUM_BY_REGION,
    sales_model,
)
from tests._dev1900_fixtures import (
    dev1900_models,
    unparseable_derived_models,
)
from tests._engine_helpers import _engine_generate

# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #
def monthly_model() -> SlayerModel:
    """One row per (region, month); a clean running-total substrate."""
    return SlayerModel(
        name="monthly", data_source="test", sql_table="monthly",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )


def monthly_multi_model() -> SlayerModel:
    """Like ``monthly`` but TWO rows per (region, month) — the substrate that
    separates a second-order aggregation's count over operand CELLS (Axiom 2.4,
    no row leaf → home is the operand dataset) from a naive count over base rows."""
    return SlayerModel(
        name="monthly_multi", data_source="test", sql_table="monthly_multi",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )


def _extend_graph_a(models: List[SlayerModel]) -> None:
    """Add DEV-1832's columns/aggregations to the DEV-1900 graph in place."""
    orders = next(m for m in models if m.name == "orders")
    orders.columns.extend([
        Column(name="cost", type=DataType.DOUBLE),
        Column(name="quantity", type=DataType.DOUBLE),
        Column(name="weight", type=DataType.DOUBLE),
    ])
    cust = next(m for m in models if m.name == "customers")
    cust.columns.extend([
        Column(name="discount", type=DataType.DOUBLE),
        # Filtered joined-model leaf: spend restricted to North customers.
        Column(name="north_spend", type=DataType.DOUBLE, sql="spend",
               filter="regions.name = 'North'"),
    ])
    # Customers-owned custom aggregation; default weight is customers-local, so
    # an explicit orders-local weight= widens the home (semantics home rule).
    cust.aggregations.append(Aggregation(
        name="wsum", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="spend")]))
    # Expression defaults: one naming a shallower (root-side) model, one local.
    cust.aggregations.append(Aggregation(
        name="wsum_expr", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="stores.rent * 2")]))
    cust.aggregations.append(Aggregation(
        name="wsum_local_expr", formula="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="spend * 2")]))


def dev1832_models() -> List[SlayerModel]:
    """Graph A (extended DEV-1900) + the ``sales``, ``monthly`` and
    ``monthly_multi`` graphs."""
    models = dev1900_models()
    _extend_graph_a(models)
    return [*models, sales_model(), monthly_model(), monthly_multi_model()]


def dev1832_unparseable_models() -> List[SlayerModel]:
    """Graph A with ``regions.unparseable`` (no dialect parses it) so an
    expression leaf naming it fails the analyzability check."""
    models = unparseable_derived_models()
    _extend_graph_a(models)
    return [*models, sales_model(), monthly_model(), monthly_multi_model()]


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def cust_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "customers")
    return SlayerQuery(**kw)


def sales_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    return SlayerQuery(**kw)


def monthly_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "monthly")
    return SlayerQuery(**kw)


def monthly_multi_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "monthly_multi")
    return SlayerQuery(**kw)


def month_td() -> List[TimeDimension]:
    return [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                          granularity=TimeGranularity.MONTH)]


def bundle(models: Optional[List[SlayerModel]] = None) -> ResolvedSourceBundle:
    """A ``ResolvedSourceBundle`` rooted at the first model (default: sales,
    the plan-structure root)."""
    models = models if models is not None else [sales_model(), *dev1832_models()]
    return ResolvedSourceBundle(
        source_model=models[0], referenced_models=models[1:])


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              models: Optional[List[SlayerModel]] = None) -> str:
    """Dry-run SQL for ``query`` (no seed; the fail-closed paths raise here)."""
    models = models if models is not None else dev1832_models()
    return await _engine_generate(
        query=query, model=next(m for m in models if m.name == query.source_model),
        extra_models=[m for m in models if m.name != query.source_model],
        dialect=dialect, validate=False)


def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB timestamp values."""
    return str(value)[:7]


# --------------------------------------------------------------------------- #
# Execution dataset (Graph A extended + sales + monthly).
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "North", 100.0), (2, "South", 200.0)]
_PLANS_ROWS = [("p1", "basic", 10.0), ("p2", "pro", 20.0), ("p3", "basic", 5.0)]
_REGION_EVENTS_ROWS = [(1, 1, 50.0), (2, 1, 50.0), (3, 2, 30.0)]
_STORES_ROWS = [("A", 1, "NYC", 500.0), ("A", 2, "LA", 300.0),
                ("B", 1, "SF", 200.0), ("B", 2, "DAL", 100.0)]
# (id, region_id, plan_code, tier, spend, signup_at, discount)
_CUSTOMERS_ROWS = [
    (1, 1, "p1", "gold", 100.0, "2024-01-05", 5.0),
    (2, 1, "p2", "silver", 150.0, "2024-02-10", 10.0),
    (3, 2, "p1", "gold", 60.0, "2024-03-15", 5.0),
    (4, None, "p2", "bronze", 40.0, "2024-03-20", 20.0),
    (5, 2, "p3", "silver", 80.0, "2024-04-15", 8.0),
    (6, 1, None, "gold", 30.0, "2024-04-18", 3.0),
    (7, 2, "p2", "gold", 55.0, "2024-04-20", 2.0),
]
# (id, customer_id, status, channel, amount, ordered_at, store_co, store_no,
#  cost, quantity, weight)
_ORDERS_ROWS = [
    (1, 1, "ok", "web", 10.0, "2024-01-10", "A", 1, 4.0, 2.0, 1.0),
    (2, 1, "new", "app", 20.0, "2024-01-20", "A", 2, 8.0, 1.0, 2.0),
    (3, 2, "ok", "web", 30.0, "2024-02-10", "B", 1, 10.0, 3.0, 1.0),
    (4, 2, "new", "app", 25.0, "2024-02-20", "A", 1, 5.0, 2.0, 2.0),
    (5, 3, "ok", "app", 5.0, "2024-03-05", "B", 1, 2.0, 1.0, 1.0),
    (6, 4, "new", "web", 40.0, "2024-03-10", "A", 2, 20.0, 4.0, 3.0),
    (7, 5, "ok", "app", 15.0, "2024-04-02", "B", 1, 5.0, 1.0, 1.0),
    (8, None, "ok", "web", 7.0, "2024-03-12", "A", 1, 3.0, 1.0, 1.0),
    (9, 6, "ok", "web", 12.0, "2024-04-10", "A", 2, 6.0, 2.0, 2.0),
    (10, 1, "ok", "web", 3.0, "2024-01-25", "B", 2, 1.0, 1.0, 1.0),
]
# (id, region, city, product, amount, quantity, unit_price) — DEV-1847 verbatim.
_SALES_ROWS = [
    (1, "North", "Alpha", "P", 10.0, 2.0, 4.0),
    (2, "North", "Alpha", "P", 10.0, 2.0, 8.0),
    (3, "North", "Alpha", "Q", 10.0, 3.0, 9.0),
    (4, "North", "Beta", "Q", 60.0, 1.0, 15.0),
    (5, "South", "Alpha", "P", 20.0, 4.0, 4.0),
    (6, "South", "Alpha", "P", 20.0, 2.0, 5.0),
    (7, "South", "Gamma", "Q", 100.0, 5.0, 12.0),
    (8, "East", "Delta", "P", 50.0, 3.0, 8.0),
    (9, "East", "Epsilon", "P", 50.0, 1.0, 1.0),
    (10, "East", "Zeta", "Q", 80.0, 2.0, 24.0),
    (11, "Gap", None, "P", 7.0, 2.0, 6.0),
    (12, "Gap", None, "P", 5.0, 1.0, 2.0),
    (13, "Gap", "Kappa", "P", 8.0, 3.0, 7.0),
    (14, "Void", "Xi", "P", None, 2.0, None),
    (15, "Void", "Xi", "P", None, 1.0, None),
]
# (id, region, ordered_at, amount). The West Feb cell has a NULL amount — SUM
# ignores it, so every absolute oracle is unchanged (smoke-test derivations skip
# None); it exercises the all-NULL collapse pick and the NULL dimension band.
_MONTHLY_ROWS = [
    (1, "North", "2024-01-10", 10.0),
    (2, "North", "2024-02-10", 20.0),
    (3, "North", "2024-03-10", 30.0),
    (4, "South", "2024-01-15", 5.0),
    (5, "South", "2024-02-15", 15.0),
    (6, "West", "2024-02-15", None),
]
# (id, region, ordered_at, amount) — TWO rows per (region, month), the months >90
# days apart so a trailing-90d window stays within one month. Per-(region,month)
# amount:sum cells: North Jan 10 / Jun 30, South Jan 5 / Jun 20. A second-order
# aggregation counts each of the four cells once (Axiom 2.4); a naive count over
# the eight base rows would double it.
_MONTHLY_MULTI_ROWS = [
    (1, "North", "2024-01-10", 6.0),
    (2, "North", "2024-01-20", 4.0),
    (3, "North", "2024-06-10", 18.0),
    (4, "North", "2024-06-20", 12.0),
    (5, "South", "2024-01-10", 3.0),
    (6, "South", "2024-01-20", 2.0),
    (7, "South", "2024-06-10", 12.0),
    (8, "South", "2024-06-20", 8.0),
]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    cur.execute("CREATE TABLE plans (code TEXT PRIMARY KEY, level TEXT, fee REAL)")
    cur.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "plan_code TEXT, tier TEXT, spend REAL, signup_at TEXT, discount REAL)")
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE stores (co TEXT, no INTEGER, city TEXT, rent REAL, "
        "PRIMARY KEY (co, no))")
    cur.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, channel TEXT, amount REAL, ordered_at TEXT, store_co TEXT, "
        "store_no INTEGER, cost REAL, quantity REAL, weight REAL)")
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    cur.execute(
        "CREATE TABLE region_events (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "value REAL)")
    cur.executemany("INSERT INTO region_events VALUES (?,?,?)", _REGION_EVENTS_ROWS)
    cur.execute(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, city TEXT, "
        "product TEXT, amount REAL, quantity REAL, unit_price REAL)")
    cur.executemany("INSERT INTO sales VALUES (?,?,?,?,?,?,?)", _SALES_ROWS)
    cur.execute(
        "CREATE TABLE monthly (id INTEGER PRIMARY KEY, region TEXT, "
        "ordered_at TEXT, amount REAL)")
    cur.executemany("INSERT INTO monthly VALUES (?,?,?,?)", _MONTHLY_ROWS)
    cur.execute(
        "CREATE TABLE monthly_multi (id INTEGER PRIMARY KEY, region TEXT, "
        "ordered_at TEXT, amount REAL)")
    cur.executemany("INSERT INTO monthly_multi VALUES (?,?,?,?)", _MONTHLY_MULTI_ROWS)
    con.commit()
    con.close()


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    con.execute("CREATE TABLE plans (code VARCHAR, level VARCHAR, fee DOUBLE)")
    con.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, plan_code VARCHAR, "
        "tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP, discount DOUBLE)")
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE stores (co VARCHAR, no INTEGER, city VARCHAR, rent DOUBLE)")
    con.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
        "channel VARCHAR, amount DOUBLE, ordered_at TIMESTAMP, store_co VARCHAR, "
        "store_no INTEGER, cost DOUBLE, quantity DOUBLE, weight DOUBLE)")
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.execute(
        "CREATE TABLE region_events (id INTEGER, region_id INTEGER, value DOUBLE)")
    con.executemany("INSERT INTO region_events VALUES (?,?,?)", _REGION_EVENTS_ROWS)
    con.execute(
        "CREATE TABLE sales (id INTEGER, region VARCHAR, city VARCHAR, "
        "product VARCHAR, amount DOUBLE, quantity DOUBLE, unit_price DOUBLE)")
    con.executemany("INSERT INTO sales VALUES (?,?,?,?,?,?,?)", _SALES_ROWS)
    con.execute(
        "CREATE TABLE monthly (id INTEGER, region VARCHAR, ordered_at TIMESTAMP, "
        "amount DOUBLE)")
    con.executemany("INSERT INTO monthly VALUES (?,?,?,?)", _MONTHLY_ROWS)
    con.execute(
        "CREATE TABLE monthly_multi (id INTEGER, region VARCHAR, "
        "ordered_at TIMESTAMP, amount DOUBLE)")
    con.executemany("INSERT INTO monthly_multi VALUES (?,?,?,?)", _MONTHLY_MULTI_ROWS)
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
            models=models if models is not None else dev1832_models())


# --------------------------------------------------------------------------- #
# Hand-computed oracles (every value re-derived by the smoke test).
# --------------------------------------------------------------------------- #
# Graph A --------------------------------------------------------------------
#: sum(amount - customers.discount) homed at orders, by orders.status. Each
#: order carries its own customer's discount; the orphan (no customer) is NULL.
HOST_DISCOUNT_BY_STATUS = {"ok": 39.0, "new": 50.0}
#: sum(amount - customers.north_spend) by status. north_spend is spend on North
#: customers, NULL elsewhere; the filter crosses customers → regions (to-one).
NORTH_SPEND_EXPR_BY_STATUS = {"ok": -325.0, "new": -205.0}
#: sum(customers.discount * avg(amount, partition_by=status)) by status — a
#: joined-model row leaf times a same-grain attached average, homed at orders.
JOINED_ROWLEAF_MIXED_BY_STATUS = {"ok": 2952.0 / 7.0, "new": 2975.0 / 3.0}

# Graph B (sales, q_amount) --------------------------------------------------
#: q_amount:sum single-column value (SUM(CASE WHEN product='Q' THEN amount END)).
QAMT_SUM = 250.0
#: weighted_avg(q_amount, weight=quantity): value masked, weight NOT — the new
#: pure-sugar meaning. The former filter-everything form gives WEIGHT_MASKED_WRONG.
WAVG_QAMT_WEIGHT_QTY = 750.0 / 34.0
WAVG_QAMT_WEIGHT_QTY_WRONG = 750.0 / 11.0
#: weighted_avg(amount, weight=q_amount): the filtered column used as a weight
#: IS masked (NULL on non-Q rows).
WAVG_AMOUNT_WEIGHT_QAMT = 80.4
#: group by q_amount: non-Q rows fall into the NULL group; count per cell.
COUNT_BY_QAMOUNT = {None: 11, 10.0: 1, 60.0: 1, 100.0: 1, 80.0: 1}
#: sum(q_amount - 1) over Q rows; count(q_amount - 1) counts the Q rows.
SUM_QAMT_MINUS_1 = 246.0
COUNT_QAMT_MINUS_1 = 4
#: sum(quantity * rank(avg(unit_price, partition_by=product))) by region.
#: rank() is DESC (Q's avg 15 → rank 1, P's avg 5 → rank 2).
MIXED_RANK_SUM_BY_REGION = {"North": 12.0, "South": 17.0, "East": 10.0,
                            "Gap": 12.0, "Void": 6.0}

# monthly (transforms) -------------------------------------------------------
#: amount:sum by (region, month): North Jan10/Feb20/Mar30, South Jan5/Feb15.
#: cumsum within region: North 10/30/60, South 5/20. Then sum over regions of
#: (running total - 1) per month.
GRAINED_CUMSUM_BY_MONTH = {"2024-01": 13.0, "2024-02": 48.0, "2024-03": 59.0}
#: ungrained sum(cumsum(amount:sum)) = cumsum at the query grain [month]
#: (identity + degenerate warning): amount:sum Jan15/Feb35/Mar30 → 15/50/80.
UNGRAINED_CUMSUM_BY_MONTH = {"2024-01": 15.0, "2024-02": 50.0, "2024-03": 80.0}

# Collapsing constituents (D4c): last/first reduce X along the month axis to one
# value per region (North last 30 / first 10, South last 15 / first 5, West NULL),
# summed over regions and broadcast across months.
LAST_SUM_BY_MONTH = {"2024-01": 45.0, "2024-02": 45.0, "2024-03": 45.0}
FIRST_SUM_BY_MONTH = {"2024-01": 15.0, "2024-02": 15.0, "2024-03": 15.0}
#: sum(cumsum(X) - last(X)) by month — cumsum preserves the axis, last collapses
#: it: North (10-30, 30-30, 60-30)=(−20,0,30), South (5-15, 20-15)=(−10,5).
CUMSUM_MINUS_LAST_BY_MONTH = {"2024-01": -30.0, "2024-02": 5.0, "2024-03": 30.0}

# Family coverage (D4a preserving transforms over X, summed by month; the first
# period of a within-region series is NULL for the difference/shift ops).
CHANGE_SUM_BY_MONTH = {"2024-02": 20.0, "2024-03": 10.0}
CHANGE_PCT_SUM_BY_MONTH = {"2024-02": 3.0, "2024-03": 0.5}
TIME_SHIFT_BACK_SUM_BY_MONTH = {"2024-02": 15.0, "2024-03": 20.0}
LAG_SUM_BY_MONTH = {"2024-02": 15.0, "2024-03": 20.0}
LEAD_SUM_BY_MONTH = {"2024-01": 35.0, "2024-02": 30.0}
CONSEC_SUM_BY_MONTH = {"2024-01": 0, "2024-02": 2, "2024-03": 2}

# DEV-1928: re-aggregation constituents in a mixed row source (X per region-month as
# above). last(X) collapses to the region's latest value (North 30 / South 15 / West
# NULL), min(X, partition_by=region) to its minimum (North 10 / South 5 / West NULL);
# each broadcasts onto the region's rows, times the row amount, summed per month.
COLLAPSE_MIXED_BY_MONTH = {"2024-01": 375.0, "2024-02": 825.0, "2024-03": 900.0}
HANDWRITTEN_MIN_MIXED_BY_MONTH = {"2024-01": 125.0, "2024-02": 275.0, "2024-03": 300.0}
#: sum(amount * last(amount:sum(partition_by=ordered_at))): the inner collapses the
#: ordered_at axis to an empty grain (one value = the latest month's cross-region total,
#: Mar 30), broadcast onto every row: amount * 30 summed per month.
EMPTY_GRAIN_MIXED_BY_MONTH = {"2024-01": 450.0, "2024-02": 1050.0, "2024-03": 900.0}
#: weighted_avg(amount, weight=min(X, partition_by=region)): SUM(amount*w)/SUM(w) per
#: month, w the region minimum broadcast onto its rows.
WAVG_MIN_MIXED_BY_MONTH = {"2024-01": 125.0 / 15.0, "2024-02": 275.0 / 15.0,
                           "2024-03": 30.0}
#: sum(amount * min(X, partition_by=region)) + amount:sum(partition_by=region) over
#: [region] + month: the mixed re-aggregation root plus the coarser region total
#: broadcast across months, keyed by (region, month); West's all-NULL cell drops out.
MIXED_COMBINED_BY_REGION_MONTH = {
    ("North", "2024-01"): 160.0, ("North", "2024-02"): 260.0,
    ("North", "2024-03"): 360.0, ("South", "2024-01"): 45.0,
    ("South", "2024-02"): 95.0}
#: sum(rank(amount:sum(window='90d', partition_by=region))) by month: the trailing-90d
#: per-region sum (North 10/30/60, South 5/20, West NULL), ranked DESC over the
#: (region, month) cells (NULLs last) → North 4/2/1, South 5/3, West 6; summed per month.
WINDOWED_INNER_BY_MONTH = {"2024-01": 9.0, "2024-02": 11.0, "2024-03": 1.0}
#: sum(cumsum(amount:sum(partition_by=[customers.tier, ordered_at]))) rooted at orders,
#: by month — the to-one cross-model partition key (customers.tier is determined from
#: orders): amount:sum per (tier, month), cumsum per tier over months, summed per month.
XMODEL_TIER_CUMSUM_BY_MONTH = {"2024-01": 33.0, "2024-02": 55.0, "2024-03": 85.0,
                               "2024-04": 120.0}

# DEV-1928 axiom compliance (monthly_multi, two rows per cell). A pure re-aggregation
# — no row leaf — is a second-order aggregation whose home is the operand dataset
# (Axiom 2.4), so it counts each (region, month) CELL once, never the two base rows.
#: sum(rank(amount:sum(window='90d', partition_by=region))): cells North 10/30,
#: South 5/20 → ranks DESC 3/1, 4/2 → summed per month over the four cells.
WINDOWED_MULTI_BY_MONTH = {"2024-01": 7.0, "2024-06": 3.0}
#: A ROW leaf flips the home back to the model rows: sum(amount * min(X, partition_by=
#: region)) sums over all eight rows — each amount times its region's minimum cell.
MIXED_MULTI_BY_MONTH = {"2024-01": 125.0, "2024-06": 400.0}

DEGENERATE_KIND = "degenerate_reaggregation"
BROADCAST_KIND = "broadcast"


def degenerate_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == DEGENERATE_KIND]


def broadcast_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == BROADCAST_KIND]


def status_key(resp, root: str = "orders") -> dict:
    return {k[0]: v for k, v in rows_by(resp, f"{root}.status").items()}


__all__ = [
    "Aggregation", "AggregationParam", "Column", "ColumnRef", "DataType",
    "ModelMeasure", "SlayerModel", "SlayerQuery", "TimeDimension",
    "TimeGranularity",
    "monthly_model", "monthly_multi_model",
    "dev1832_models", "dev1832_unparseable_models",
    "sales_model",
    "orders_q", "cust_q", "sales_q", "monthly_q", "monthly_multi_q",
    "month_td", "bundle", "gen",
    "make_exec_engine", "rows_by", "status_key", "month_key",
    "degenerate_warnings", "DEGENERATE_KIND",
    "AVG_CITY_TOTAL_BY_REGION", "MIXED_SUM_BY_REGION",
    "HOST_DISCOUNT_BY_STATUS", "NORTH_SPEND_EXPR_BY_STATUS",
    "JOINED_ROWLEAF_MIXED_BY_STATUS",
    "QAMT_SUM", "WAVG_QAMT_WEIGHT_QTY", "WAVG_QAMT_WEIGHT_QTY_WRONG",
    "WAVG_AMOUNT_WEIGHT_QAMT", "COUNT_BY_QAMOUNT", "SUM_QAMT_MINUS_1",
    "COUNT_QAMT_MINUS_1", "MIXED_RANK_SUM_BY_REGION",
    "GRAINED_CUMSUM_BY_MONTH", "UNGRAINED_CUMSUM_BY_MONTH",
    "LAST_SUM_BY_MONTH", "FIRST_SUM_BY_MONTH", "CUMSUM_MINUS_LAST_BY_MONTH",
    "CHANGE_SUM_BY_MONTH", "CHANGE_PCT_SUM_BY_MONTH", "TIME_SHIFT_BACK_SUM_BY_MONTH",
    "LAG_SUM_BY_MONTH", "LEAD_SUM_BY_MONTH", "CONSEC_SUM_BY_MONTH",
    "COLLAPSE_MIXED_BY_MONTH", "HANDWRITTEN_MIN_MIXED_BY_MONTH",
    "EMPTY_GRAIN_MIXED_BY_MONTH", "WAVG_MIN_MIXED_BY_MONTH",
    "MIXED_COMBINED_BY_REGION_MONTH", "WINDOWED_INNER_BY_MONTH",
    "XMODEL_TIER_CUMSUM_BY_MONTH",
    "WINDOWED_MULTI_BY_MONTH", "MIXED_MULTI_BY_MONTH",
    "broadcast_warnings", "BROADCAST_KIND",
    "_ORDERS_ROWS", "_CUSTOMERS_ROWS", "_REGIONS_ROWS", "_SALES_ROWS",
    "_MONTHLY_ROWS", "_MONTHLY_MULTI_ROWS",
]
