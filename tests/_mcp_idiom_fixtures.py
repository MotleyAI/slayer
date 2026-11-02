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
``with_namesake`` adds customer 6 Alice R1 0 and order 12 (6, paid, 2025-07-01, 40).
``with_last_year`` adds orders 13 (Bob, paid, 2024-01-05, 8) and 14 (Carol, open, 2024-06-30, 12).

Child dataset (``make_child_engine``): ``orders`` and ``returns`` both join ``customers``.

regions:   1 North | 2 South
customers: 1 Alice R1 | 2 Bob R1 | 3 Carol R2 | 4 Dan (no region; omitted when ``with_regionless=False``)
orders (id, customer, amount): 1 Alice 10 | 2 Alice 20 | 3 Bob 35 | 4 Carol 40 | 5 Dan 50
   | 6 NULL 6 | 7 #99 (no such customer) 7
returns (id, customer, amount): 1 Alice 1 | 2 Alice 2 | 3 Alice 4 | 4 Carol 8
events (id, kind), joined to nothing: 1 a | 2 b | 3 a | 4 c | 5 a | 6 b

Orders: Alice 30, Bob 35, Carol 40, Dan 50, parentless 13; total 168. Returns: Alice 7, Carol 8; total 15.
By region: North 65, South 40, NULL 63 (Dan + parentless).

Items dataset (``make_items_engine``), one model ``items``:

items (id, code, flag, status, tags):
   1 A123  true  paid    [x, y] | 2 A12  false NULL    [x] | 3 B123 NULL paid [y]
   4 A1234 true  open    []     | 5 XA123 false shipped NULL
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine

from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# Documented example texts — each must appear verbatim in its owning description
# --------------------------------------------------------------------------- #
SHARE_OF_GROUP = "sum(amount) / sum(amount, partition_by=customers.regions.name)"
SHARE_OF_TOTAL = "sum(amount) / sum(amount, partition_by=[])"
TRAILING_WINDOW = "count_distinct(customer_id, window='90d')"
NESTED = (
    "avg(sum(amount, partition_by=[customers.regions.name, customers.id]), "
    "partition_by=[customers.regions.name])"
)
SPEND_BAND = "iif(sum(amount, partition_by=customers.id) > 150, 'high', 'low')"
CHANGE = "change(sum(amount))"
CHANGE_PCT = "change_pct(sum(amount))"
TIME_SHIFT = "time_shift(sum(amount), -1)"
CUMSUM = "cumsum(sum(amount))"
CROSS_MODEL = "sum(customers.credit)"
RANK_FILTER = "rank(sum(amount), partition_by=customers.regions.name, direction='desc') <= 1"
ANTI_JOIN = "orders.id is null"
ORDER_UNDISPLAYED = '{"column": "sum(amount)", "direction": "desc"}'
POPULATION_COUNT = "count(customers.orders.id)"
CONDITIONAL = "sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))"
STAGE_COLUMN = "customers__regions__name"
SPINE_TIME_DIMENSION = '{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": "2025"}'
SPINE_MEASURE = "sum(orders.amount)"
SECOND_CHILD = "sum(returns.amount)"
AGG_FILTER = "count(*) FILTER (WHERE status = 'paid')"
IN_LIST_VARIABLE = "status in ({statuses})"
CROSS_MODEL_RATIO = "sum(orders.amount) / count(*)"
#: Guidance marker for an aggregate over a model no join path reaches; owned by ``measures``.
UNRELATED_MODEL = "no join path reaches"
UNRELATED_RATIO = "count(events.id) / count(*)"

# Guidance markers
STAGE_NOT_NEEDED = "A STAGE IS NOT NEEDED"
STAGE_NEEDED = "a cross-row value must still see"
READABLE_LABEL = "never by an id alone"
ONE_QUERY = "ONE QUESTION = ONE QUERY"
FAN_OUT = "NEVER fans out or double counts"
PER_QUERY_EXPRESSION = "in the position that uses it"

#: Example text or schema-owned marker → (owning $defs model, field) in the query tool's input schema.
EXAMPLE_OWNERS: dict[str, tuple[str, str]] = {
    SHARE_OF_GROUP: ("SlayerQuery", "measures"),
    SHARE_OF_TOTAL: ("SlayerQuery", "measures"),
    TRAILING_WINDOW: ("SlayerQuery", "measures"),
    NESTED: ("SlayerQuery", "measures"),
    SPEND_BAND: ("SlayerQuery", "dimensions"),
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
    SPINE_TIME_DIMENSION: ("SlayerQuery", "time_dimensions"),
    SECOND_CHILD: ("SlayerQuery", "measures"),
    CROSS_MODEL_RATIO: ("SlayerQuery", "measures"),
    STAGE_NOT_NEEDED: ("SlayerQuery", "name"),
    STAGE_NEEDED: ("SlayerQuery", "name"),
    READABLE_LABEL: ("SlayerQuery", "dimensions"),
    AGG_FILTER: ("SlayerQuery", "measures"),
    IN_LIST_VARIABLE: ("SlayerQuery", "variables"),
    PER_QUERY_EXPRESSION: ("SlayerQuery", "source_model"),
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


_LAST_YEAR_ROWS = [(13, 2, "paid", "2024-01-05", 8), (14, 3, "open", "2024-06-30", 12)]


def _idiom_seed(*, with_namesake: bool, with_last_year: bool) -> Callable[[str], None]:
    customers = _CUSTOMER_ROWS + ([(6, "Alice", 1, 0)] if with_namesake else [])
    orders = (
        _ORDER_ROWS
        + ([(12, 6, "paid", "2025-07-01", 40)] if with_namesake else [])
        + (_LAST_YEAR_ROWS if with_last_year else [])
    )

    def seed(db_path: str) -> None:
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        for stmt in (
            "CREATE TABLE regions (id INTEGER PRIMARY KEY, name VARCHAR)",
            f"INSERT INTO regions VALUES {_values(_REGION_ROWS)}",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, region_id INTEGER, credit INTEGER)",
            f"INSERT INTO customers VALUES {_values(customers)}",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status VARCHAR, "
            "order_date DATE, amount INTEGER)",
            f"INSERT INTO orders VALUES {_values(orders)}",
        ):
            con.execute(stmt)
        con.close()

    return seed


async def make_idiom_engine(
    *, with_namesake: bool = False, with_last_year: bool = False,
) -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    async with seeded_exec_engine(
        dialect="duckdb", seed=_idiom_seed(with_namesake=with_namesake, with_last_year=with_last_year),
        models=idiom_models(),
    ) as (engine, _):
        yield engine


def _child_models(*, data_source: str = "test") -> list[SlayerModel]:
    def child(name: str) -> SlayerModel:
        return SlayerModel(
            name=name, sql_table=name, data_source=data_source,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

    return [
        child("orders"),
        child("returns"),
        SlayerModel(
            name="customers", sql_table="customers", data_source=data_source,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
                Column(name="region_id", type=DataType.INT),
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
        SlayerModel(
            name="events", sql_table="events", data_source=data_source,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="kind", type=DataType.TEXT),
            ],
        ),
    ]


def _child_seed(*, with_regionless: bool) -> Callable[[str], None]:
    customers = "(1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)" + (", (4, 'Dan', NULL)" if with_regionless else "")

    def seed(db_path: str) -> None:
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        for stmt in (
            "CREATE TABLE regions (id INTEGER PRIMARY KEY, name VARCHAR)",
            "INSERT INTO regions VALUES (1, 'North'), (2, 'South')",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR, region_id INTEGER)",
            f"INSERT INTO customers VALUES {customers}",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
            "INSERT INTO orders VALUES (1, 1, 10), (2, 1, 20), (3, 2, 35), (4, 3, 40), (5, 4, 50), "
            "(6, NULL, 6), (7, 99, 7)",
            "CREATE TABLE returns (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
            "INSERT INTO returns VALUES (1, 1, 1), (2, 1, 2), (3, 1, 4), (4, 3, 8)",
            "CREATE TABLE events (id INTEGER PRIMARY KEY, kind VARCHAR)",
            "INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'a'), (6, 'b')",
        ):
            con.execute(stmt)
        con.close()

    return seed


async def make_child_engine(*, with_regionless: bool = True) -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    async with seeded_exec_engine(
        dialect="duckdb", seed=_child_seed(with_regionless=with_regionless), models=_child_models(),
    ) as (engine, _):
        yield engine


def _items_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="items", sql_table="items", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="code", type=DataType.TEXT),
            Column(name="flag", type=DataType.BOOLEAN),
            Column(name="status", type=DataType.TEXT),
            Column(name="tags", type=DataType.ARRAY, element_type=DataType.TEXT),
        ],
    )


def _items_seed(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    for stmt in (
        "CREATE TABLE items (id INTEGER PRIMARY KEY, code VARCHAR, flag BOOLEAN, status VARCHAR, tags VARCHAR[])",
        "INSERT INTO items VALUES (1, 'A123', true, 'paid', ['x', 'y']), (2, 'A12', false, NULL, ['x']), "
        "(3, 'B123', NULL, 'paid', ['y']), (4, 'A1234', true, 'open', []), (5, 'XA123', false, 'shipped', NULL)",
    ):
        con.execute(stmt)
    con.close()


async def make_items_engine() -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect="duckdb", seed=_items_seed, models=[_items_model()]) as (engine, _):
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
