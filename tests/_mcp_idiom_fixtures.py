"""The query idioms the MCP surface documents, and a seeded DuckDB engine to run them.

Underscore-prefixed so pytest skips collection. Joins: ``orders.customer_id →
customers.id``, ``customers.region_id → regions.id`` (reverse traversal is automatic).

regions:   1 North | 2 South | 3 East | 4 West
customers: 1 Alice R1 100 | 2 Bob R1 200 | 3 Carol R2 300 | 4 Dan R2 400 | 5 Eve R3 500
orders (id, customer, status, date, amount):
   1 Alice paid 2024-10-15  10 |  2 Bob   open 2024-11-10  20 |  3 Carol paid 2024-12-05  45
   4 Alice paid 2025-01-10 100 |  5 Bob   paid 2025-01-20  50 |  6 Carol open 2025-02-14  70
   7 Alice open 2025-03-03  30 |  8 Bob   paid 2025-04-08  60 |  9 Alice open 2025-06-01  25
  10 Carol paid 2025-06-15  75 | 11 Alice paid 2025-06-20   5

Monthly sum: 2024-10 10, -11 20, -12 45, 2025-01 150, -02 70, -03 30, -04 60, -06 105.
Per customer: Alice 170 (5), Bob 130 (3), Carol 190 (3); total 490. Status: paid 345 (7), open 145 (4);
every ordering customer has both statuses, so their credit (600) attributes to each status.
"""

from __future__ import annotations

from typing import Any, AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine

from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# Documented example texts — each must appear verbatim in its owning description
# --------------------------------------------------------------------------- #
SHARE_OF_GROUP = "sum(amount) / sum(amount, partition_by=customers.region_id)"
SHARE_OF_TOTAL = "sum(amount) / sum(amount, partition_by=[])"
TRAILING_WINDOW = "count_distinct(customer_id, window='90d')"
NESTED = (
    "avg(sum(amount, partition_by=[customers.region_id, customers.name]), "
    "partition_by=[customers.region_id])"
)
CHANGE = "change(sum(amount))"
CHANGE_PCT = "change_pct(sum(amount))"
TIME_SHIFT = "time_shift(sum(amount), -1)"
CUMSUM = "cumsum(sum(amount))"
CROSS_MODEL = "sum(customers.credit)"
RANK_FILTER = "rank(sum(amount), partition_by=customers.region_id, direction='desc') <= 1"
ANTI_JOIN = "orders.id is null"
ORDER_UNDISPLAYED = '{"column": "sum(amount)", "direction": "desc"}'
POPULATION_COUNT = "count(customers.orders.id)"
CONDITIONAL = "sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))"
STAGE_COLUMN = "customers__region_id"

#: Example text → (owning $defs model, field) in the query tool's input schema.
EXAMPLE_OWNERS: dict[str, tuple[str, str]] = {
    SHARE_OF_GROUP: ("SlayerQuery", "measures"),
    SHARE_OF_TOTAL: ("SlayerQuery", "measures"),
    TRAILING_WINDOW: ("SlayerQuery", "measures"),
    NESTED: ("SlayerQuery", "measures"),
    CHANGE: ("SlayerQuery", "measures"),
    CHANGE_PCT: ("SlayerQuery", "measures"),
    TIME_SHIFT: ("SlayerQuery", "measures"),
    CUMSUM: ("SlayerQuery", "measures"),
    CROSS_MODEL: ("SlayerQuery", "measures"),
    RANK_FILTER: ("SlayerQuery", "filters"),
    ANTI_JOIN: ("SlayerQuery", "filters"),
    ORDER_UNDISPLAYED: ("SlayerQuery", "order"),
    POPULATION_COUNT: ("SlayerQuery", "source_model"),
    CONDITIONAL: ("SlayerQuery", "source_model"),
    STAGE_COLUMN: ("SlayerQuery", "name"),
}


# --------------------------------------------------------------------------- #
# Models + seed
# --------------------------------------------------------------------------- #
def idiom_models(*, data_source: str = "test") -> list[SlayerModel]:
    return [
        SlayerModel(
            name="orders", sql_table="orders", data_source=data_source,
            default_time_dimension="order_date",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="status", type=DataType.TEXT),
                Column(name="order_date", type=DataType.DATE),
                Column(name="amount", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ),
        SlayerModel(
            name="customers", sql_table="customers", data_source=data_source,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
                Column(name="region_id", type=DataType.INT),
                Column(name="credit", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        ),
        SlayerModel(
            name="regions", sql_table="regions", data_source=data_source,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
            ],
        ),
    ]


_REGION_ROWS = [(1, "North"), (2, "South"), (3, "East"), (4, "West")]
_CUSTOMER_ROWS = [
    (1, "Alice", 1, 100), (2, "Bob", 1, 200), (3, "Carol", 2, 300),
    (4, "Dan", 2, 400), (5, "Eve", 3, 500),
]
_ORDER_ROWS = [
    (1, 1, "paid", "2024-10-15", 10), (2, 2, "open", "2024-11-10", 20),
    (3, 3, "paid", "2024-12-05", 45), (4, 1, "paid", "2025-01-10", 100),
    (5, 2, "paid", "2025-01-20", 50), (6, 3, "open", "2025-02-14", 70),
    (7, 1, "open", "2025-03-03", 30), (8, 2, "paid", "2025-04-08", 60),
    (9, 1, "open", "2025-06-01", 25), (10, 3, "paid", "2025-06-15", 75),
    (11, 1, "paid", "2025-06-20", 5),
]


def _values(rows: list[tuple]) -> str:
    return ", ".join(
        "(" + ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in row) + ")" for row in rows
    )


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    for stmt in (
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name VARCHAR)",
        f"INSERT INTO regions VALUES {_values(_REGION_ROWS)}",
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, region_id INTEGER, credit INTEGER)",
        f"INSERT INTO customers VALUES {_values(_CUSTOMER_ROWS)}",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status VARCHAR, "
        "order_date DATE, amount INTEGER)",
        f"INSERT INTO orders VALUES {_values(_ORDER_ROWS)}",
    ):
        con.execute(stmt)
    con.close()


async def make_idiom_engine() -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect="duckdb", seed=_seed_duckdb, models=idiom_models()) as (engine, _):
        yield engine


def rows_by(resp: Any, *, key: str) -> dict[Any, dict[str, Any]]:
    """Result rows keyed by the column whose name ends with ``key``."""
    col = next(c for c in resp.columns if c.endswith(key))
    out = {str(r[col])[:7] if "date" in col else r[col]: r for r in resp.data}
    assert len(out) == len(resp.data), f"duplicate result rows for {key}"
    return out


def value(row: dict[str, Any], suffix: str) -> Any:
    """The one value in ``row`` whose column name ends with ``suffix``."""
    hits = [v for k, v in row.items() if k.endswith(suffix)]
    assert len(hits) == 1, (suffix, list(row))
    return hits[0]
