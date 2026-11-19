"""Shared fixtures for time_shift / consecutive_periods over a
cross-dataset or crossing-fragment aggregate.

The datasets mirror ``tests/test_dev1745_fragment_joins.py`` (orders → customers →
regions, ``wscaled_sum`` template crossing a relationship) so the shifted-CTE half reads
against the same shape the ``_cm_`` half was fixed against. Underscore-prefixed
so pytest skips collection here (like ``tests/_engine_helpers.py``).

The three time_shift inner-aggregate shapes distinguished here:

* **(a) local inner** — ``time_shift(sum(amount), -1)`` with a *sibling*
  cross-dataset metric. Guarded today only by association.
* **(b) host-rooted inner** — ``time_shift(wscaled_sum(amount), -1)`` whose
  default param crosses ``orders → customers → regions``. The crossing fragment
  isolates the aggregate host-rooted (``cte_root_model`` = host); the shifted CTE
  re-aggregates host-rooted with the fragment's join pulled in (Part 1).
* **(c) target-grain inner** — ``time_shift(sum(customers.spend), -1)``.
  Target-rooted (``cte_root_model`` is None); host-rooted re-aggregation would
  multiply target rows, so it stays behind the narrowed guard.
"""

from __future__ import annotations

import re

from slayer.storage.sqlite_conn import transaction
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import FieldRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from tests._engine_helpers import seeded_exec_engine

from tests._dev1746_fixtures import cte_names_in_order
from tests._engine_helpers import _engine_generate, _extract_cte_body


# --------------------------------------------------------------------------- #
# Datasets — orders (host) → customers → regions.
# --------------------------------------------------------------------------- #
def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="weight", datatype=DataType.FLOAT),
        ],
    )


def customers_dataset() -> SlayerDataset:
    """``customers`` carries ``spend`` (the target-grain metric for shape (c))
    and a ``signup_at`` a first/last explicit time arg can rank by."""
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="spend", datatype=DataType.FLOAT),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def line_items_dataset() -> SlayerDataset:
    """A 1:N child of ``orders`` (each order has several line items). The
    ``liscaled_sum`` fragment crosses this ONE-TO-MANY relationship, so a wrong host-base
    isolation would fan the join out and MULTIPLY sibling metrics — which is
    what the sibling-protection execution test detects (Codex F1)."""
    return SlayerDataset(
        name="line_items", data_source="test", sql_table="line_items",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INTEGER),
            DatasetField(name="factor", datatype=DataType.FLOAT),
        ],
    )


def orders_dataset() -> SlayerDataset:
    """Host dataset. ``wscaled_sum`` is declared HERE with a default param crossing
    two hops (``customers__regions.weight``), so ``wscaled_sum(amount)`` is the
    host-rooted crossing-fragment aggregate (shape (b)). ``liscaled_sum`` crosses
    the 1:N ``line_items`` relationship for the sibling-protection fan-out proof."""
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="line_items", from_columns=["id"], to_columns=["order_id"]),
        ],
        aggregations=[
            Aggregation(
                name="wscaled_sum", expression="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
            Aggregation(
                name="liscaled_sum", expression="SUM({value} * {f})",
                params=[AggregationParam(name="f", sql="line_items.factor")],
            ),
        ],
    )


def dev1750_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_dataset(), customers_dataset(), regions_dataset(), line_items_dataset()]


# --------------------------------------------------------------------------- #
# SQL-shape generation.
# --------------------------------------------------------------------------- #
async def gen(query: SlayerQuery, *, dialect: str = "duckdb") -> str:
    models = dev1750_datasets()
    return await _engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=False,
    )


def month_td(column: str = "ordered_at") -> List[TimeDimension]:
    return [TimeDimension(
        field=column,
        granularity=TimeGranularity.MONTH,
    )]


def shifted_relation(sql: str) -> str:
    """Name of the relation the sole ``sjoin_*`` CTE looks the shifted value up in —
    a ``shifted_*`` producer, or a base ``_cm_*`` producer it interns with."""
    [name] = re.findall(r"LEFT JOIN (\w+)", _extract_cte_body(sql, r"sjoin_\w+"))
    return name


def shifted_cte_body(sql: str) -> str:
    """Rendered body of the shifted relation (see :func:`shifted_relation`) — scope
    join-registration assertions belong here, not to whole-SQL substring checks a
    valid alias elsewhere could satisfy. Balanced-paren extraction so it finds the
    CTE even inside the transform chain's outer ``FROM ( … ) AS _outer`` wrap."""
    return _extract_cte_body(sql, re.escape(shifted_relation(sql)))


def base_cte_body(sql: str) -> str:
    """Rendered body of the host ``_base`` CTE (the local-slot spine — where a
    sibling ``sum(amount)`` lives and the crossing re-aggregation must NOT)."""
    return _extract_cte_body(sql, r"_base")


def cte_names(sql: str, *, dialect: str = "duckdb") -> List[str]:
    return cte_names_in_order(sql, dialect=dialect)


# --------------------------------------------------------------------------- #
# Execution dataset — hand-computable.
# --------------------------------------------------------------------------- #
# regions:   1 → weight 2.0,   2 → weight 3.0
# customers: 1 → region 1 (w=2),  2 → region 2 (w=3),  3 → region 1 (w=2)
#            (customer 3 has a NULL-region path only if region_id absent — here
#            every customer has a region so weights are well-defined)
# orders (id, customer_id, amount, status, ordered_at):
#   Jan: (1,1,10,'ok'), (2,2,5,'ok')          amount:sum=15   wscaled=10*2+5*3=35
#   Feb: (3,1,20,'ok')                         amount:sum=20   wscaled=20*2=40
#   Mar: (4,2,10,'hold')                       amount:sum=10   wscaled=10*3=30
#   plus a NULL-status row to prove null-safe partition survival:
#   Feb: (5,1,4,NULL)                          (own status group)
#   Mar: (6,1,8,NULL)
#
# customers.spend: 1→100, 2→200, 3→50
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, 2.0), (2, 3.0)]
_CUSTOMERS_ROWS = [
    # (id, region_id, spend, signup_at)
    (1, 1, 100.0, "2020-01-01"),
    (2, 2, 200.0, "2020-06-01"),
    (3, 1, 50.0, "2021-01-01"),
]
_ORDERS_ROWS = [
    # (id, customer_id, amount, status, ordered_at)
    (1, 1, 10.0, "ok", "2024-01-15"),
    (2, 2, 5.0, "ok", "2024-01-20"),
    (3, 1, 20.0, "ok", "2024-02-10"),
    (4, 2, 10.0, "hold", "2024-03-05"),
    (5, 1, 4.0, None, "2024-02-12"),
    (6, 1, 8.0, None, "2024-03-08"),
]
# line_items (id, order_id, factor) — order 1 fans out to TWO rows, so a leaked
# join would count order 1's amount twice in a sibling amount:sum.
#   liscaled_sum = SUM(amount * factor):
#     Jan  10*1 + 10*3 + 5*2 = 50
#     Feb  20*1 + 4*1        = 24
#     Mar  10*1 + 8*1        = 18
_LINE_ITEMS_ROWS = [
    (1, 1, 1.0), (2, 1, 3.0),   # order 1 → two line items
    (3, 2, 2.0),
    (4, 3, 1.0),
    (5, 4, 1.0),
    (6, 5, 1.0),
    (7, 6, 1.0),
]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, weight REAL)")
        cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "spend REAL, signup_at TEXT)"
        )
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "amount REAL, status TEXT, ordered_at TEXT)"
        )
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS_ROWS)
        cur.execute(
            "CREATE TABLE line_items (id INTEGER PRIMARY KEY, order_id INTEGER, "
            "factor REAL)"
        )
        cur.executemany("INSERT INTO line_items VALUES (?,?,?)", _LINE_ITEMS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, weight DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, spend DOUBLE, "
        "signup_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, "
        "status VARCHAR, ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS_ROWS)
    con.execute("CREATE TABLE line_items (id INTEGER, order_id INTEGER, factor DOUBLE)")
    con.executemany("INSERT INTO line_items VALUES (?,?,?)", _LINE_ITEMS_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture — the issue's required
    execution backends. A test module wraps this in ``@pytest.fixture`` so the
    fixture name lives where it is consumed (no cross-module import that would
    shadow the parameter). Each yields an engine over the hand-computed dataset.
    """
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=dev1750_datasets()) as (engine, _db):
        yield engine


def month_key(value) -> str:
    """First 7 chars of a DATE_TRUNC'd month value — a stable per-month key
    across SQLite (``2024-01-01`` text) and DuckDB (``2024-01-01 00:00:00``)."""
    return str(value)[:7]


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    return out


__all__ = [
    "orders_dataset", "customers_dataset", "regions_dataset", "dev1750_datasets",
    "gen", "month_td", "shifted_cte_body", "shifted_relation", "base_cte_body", "cte_names",
    "make_exec_engine", "month_key", "rows_by", "SlayerQuery", "DatasetMetric",
    "FieldRef", "TimeDimension", "TimeGranularity",
]
