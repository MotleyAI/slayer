"""Shared fixtures for the Mode-B SQL-compatible grammar (spec: queries/expression-grammar and siblings).

Underscore-prefixed so pytest skips collection. ``orders.customer_id → customers.id`` is many-to-one.

orders (id, customer_id, region, city, status, amount, discount, price, quantity,
        created_at, order_date, created_txt, code, customer_name, note, path, "order total"):
   1  1  EU    Paris   paid     10    NULL  2  1  2024-01-05 10:00  2024-01-05  2024-12-25  '12'     O'Brien  we like cats  C:\\temp  100
   2  1  EU    Berlin  paid     20    5     3  2  2024-01-20 11:00  2024-01-20  2024-06-01  'abc'    Smith    we LIKE dogs  C:/temp   200
   3  2  US    NYC     Pending  30    1     4  3  2024-02-10 12:00  2024-02-10  2024-02-29  '12abc'  Jones    n/a           D:\\x     300
   4  2  US    LA      paid     40    NULL  5  2  2024-02-25 09:00  2024-02-25  2023-01-01  '7'      Brown    NULL          NULL      400
   5  3  NULL  Rome    void     NULL  2     1  4  2024-03-15 08:00  2024-03-15  NULL        NULL     Lee      x             y         NULL
customers (id, name, "Region", tier):  1 A North gold | 2 B South silver | 3 C NULL gold
balances (id, customer_id, balance, updated_at, status):
   1 1 100 2024-01-01 active | 2 1 150 2024-02-01 active | 3 1 999 2024-03-01 inactive
   4 2  50 2024-01-15 active | 5 2  70 2024-02-15 inactive | 6 2  80 2024-03-15 active

Paid rows (1, 2, 4): count by region EU 2 / US 1 / NULL 0; sum(amount) EU 30 / US 40 / NULL NULL.
weighted_avg(price, weight=quantity) over paid rows = (2 + 6 + 10) / (1 + 2 + 2) = 3.6.
Newest active balance: customer 1 → 150, customer 2 → 80.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, AggregationParam, Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import _engine_generate, seeded_exec_engine

#: Tier-1 dialects whose emission every SQL form must survive.
TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery"]
EXEC_BACKENDS = ["sqlite", "duckdb"]
ALL_IDS = frozenset({1, 2, 3, 4, 5})


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def orders_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="city", type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="discount", type=DataType.DOUBLE),
            Column(name="price", type=DataType.DOUBLE),
            Column(name="quantity", type=DataType.INT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="order_date", type=DataType.DATE),
            Column(name="created_txt", type=DataType.TEXT),
            Column(name="code", type=DataType.TEXT),
            Column(name="customer_name", type=DataType.TEXT),
            Column(name="note", type=DataType.TEXT),
            Column(name="path", type=DataType.TEXT),
            Column(name="order total", sql="order_total", type=DataType.DOUBLE),
            Column(name="payload", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        aggregations=[
            Aggregation(name="wsum", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight", sql="quantity")]),
        ],
    )


def customers_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="Region", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
        ],
    )


def balances_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="balances", sql_table="balances", data_source=data_source,
        default_time_dimension="updated_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="balance", type=DataType.DOUBLE),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="status", type=DataType.TEXT),
        ],
    )


def dev2101_models(*, data_source: str = "test") -> list[SlayerModel]:
    return [orders_model(data_source=data_source), customers_model(data_source=data_source),
            balances_model(data_source=data_source)]


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
ORDER_COLUMNS = (
    "id", "customer_id", "region", "city", "status", "amount", "discount", "price", "quantity",
    "created_at", "order_date", "created_txt", "code", "customer_name", "note", "path", "order_total", "payload",
)
ORDER_ROWS: list[tuple[Any, ...]] = [
    (1, 1, "EU", "Paris", "paid", 10.0, None, 2.0, 1, "2024-01-05 10:00:00", "2024-01-05", "2024-12-25",
     "12", "O'Brien", "we like cats", r"C:\temp", 100.0, '{"a": 1}'),
    (2, 1, "EU", "Berlin", "paid", 20.0, 5.0, 3.0, 2, "2024-01-20 11:00:00", "2024-01-20", "2024-06-01",
     "abc", "Smith", "we LIKE dogs", "C:/temp", 200.0, '{"a": 2}'),
    (3, 2, "US", "NYC", "Pending", 30.0, 1.0, 4.0, 3, "2024-02-10 12:00:00", "2024-02-10", "2024-02-29",
     "12abc", "Jones", "n/a", r"D:\x", 300.0, '{"a": 3}'),
    (4, 2, "US", "LA", "paid", 40.0, None, 5.0, 2, "2024-02-25 09:00:00", "2024-02-25", "2023-01-01",
     "7", "Brown", None, None, 400.0, None),
    (5, 3, None, "Rome", "void", None, 2.0, 1.0, 4, "2024-03-15 08:00:00", "2024-03-15", None,
     None, "Lee", "x", "y", None, None),
]
CUSTOMER_COLUMNS = ("id", "name", "Region", "tier")
CUSTOMER_ROWS: list[tuple[Any, ...]] = [(1, "A", "North", "gold"), (2, "B", "South", "silver"), (3, "C", None, "gold")]
BALANCE_COLUMNS = ("id", "customer_id", "balance", "updated_at", "status")
BALANCE_ROWS: list[tuple[Any, ...]] = [
    (1, 1, 100.0, "2024-01-01 00:00:00", "active"),
    (2, 1, 150.0, "2024-02-01 00:00:00", "active"),
    (3, 1, 999.0, "2024-03-01 00:00:00", "inactive"),
    (4, 2, 50.0, "2024-01-15 00:00:00", "active"),
    (5, 2, 70.0, "2024-02-15 00:00:00", "inactive"),
    (6, 2, 80.0, "2024-03-15 00:00:00", "active"),
]
TABLES = {
    "orders": (ORDER_COLUMNS, ORDER_ROWS),
    "customers": (CUSTOMER_COLUMNS, CUSTOMER_ROWS),
    "balances": (BALANCE_COLUMNS, BALANCE_ROWS),
}

#: Per-dialect (text, double, timestamp, identifier quote) spellings for the DDL.
_DDL_TYPES = {
    "sqlite": ("TEXT", "REAL", "TIMESTAMP", '"'),
    "duckdb": ("VARCHAR", "DOUBLE", "TIMESTAMP", '"'),
    "postgres": ("TEXT", "DOUBLE PRECISION", "TIMESTAMP", '"'),
    "tsql": ("NVARCHAR(100)", "FLOAT", "DATETIME2", '"'),
    "mysql": ("VARCHAR(100)", "DOUBLE", "DATETIME", "`"),
}


def ddl(dialect: str) -> list[str]:
    """CREATE TABLE statements for the three tables, spelled for ``dialect``."""
    text, double, ts, q = _DDL_TYPES[dialect]
    return [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, region {t}, city {t}, status {t}, "
        "amount {d}, discount {d}, price {d}, quantity INTEGER, created_at {ts}, order_date DATE, "
        "created_txt {t}, code {t}, customer_name {t}, note {t}, path {t}, order_total {d}, payload {t})".format(
            t=text, d=double, ts=ts),
        f"CREATE TABLE customers (id INTEGER PRIMARY KEY, name {text}, {q}Region{q} {text}, tier {text})",
        f"CREATE TABLE balances (id INTEGER PRIMARY KEY, customer_id INTEGER, balance {double}, "
        f"updated_at {ts}, status {text})",
    ]


def insert_sql(table: str, *, placeholder: str = "?", quote: str = '"') -> str:
    columns, _ = TABLES[table]
    cols = ", ".join(f"{quote}{c}{quote}" for c in columns)
    return f"INSERT INTO {table} ({cols}) VALUES ({', '.join(placeholder for _ in columns)})"


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        for stmt in ddl("sqlite"):
            con.execute(stmt)
        for table, (_, rows) in TABLES.items():
            con.executemany(insert_sql(table), rows)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        for stmt in ddl("duckdb"):
            con.execute(stmt)
        for table, (_, rows) in TABLES.items():
            con.executemany(insert_sql(table), rows)
    finally:
        con.close()


async def engine_for(dialect: str) -> AsyncIterator[SlayerQueryEngine]:
    """An executing engine over the seeded dataset on ``sqlite`` or ``duckdb`` (fixture body)."""
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev2101_models()) as (engine, _):
        yield engine


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=EXEC_BACKENDS`` fixture."""
    async for engine in engine_for(request.param):
        yield engine


# --------------------------------------------------------------------------- #
# Oracles: SQL forms in every position
# --------------------------------------------------------------------------- #
#: Row-level predicates and the order ids each keeps.
PREDICATES: list[tuple[str, frozenset[int]]] = [
    ("status = 'paid'", frozenset({1, 2, 4})),
    ("status <> 'paid'", frozenset({3, 5})),
    ("amount != 10", frozenset({2, 3, 4})),
    ("discount IS NULL", frozenset({1, 4})),
    ("discount IS NOT NULL", frozenset({2, 3, 5})),
    ("region is None", frozenset({5})),
    ("region IN ('EU', 'XX')", frozenset({1, 2})),
    ("region NOT IN ('EU')", frozenset({3, 4})),
    ("status = 'paid' AND amount > 15", frozenset({2, 4})),
    ("status = 'void' OR amount < 15", frozenset({1, 5})),
    ("NOT status = 'paid'", frozenset({3, 5})),
    ("note LIKE 'we%'", frozenset({1, 2})),
    ("note NOT LIKE 'we%'", frozenset({3, 5})),
    ("lower(trim(note)) LIKE 'we like%'", frozenset({1, 2})),
    ("note ILIKE 'WE LIKE%'", frozenset({1, 2})),
    ("note NOT ILIKE 'we like%'", frozenset({3, 5})),
    ("amount BETWEEN 10 AND 20", frozenset({1, 2})),
    ("amount NOT BETWEEN 10 AND 20", frozenset({3, 4})),
    ("region IS DISTINCT FROM 'EU'", frozenset({3, 4, 5})),
    ("region IS NOT DISTINCT FROM 'EU'", frozenset({1, 2})),
    ("region IS NOT DISTINCT FROM NULL", frozenset({5})),
    ("(amount > 15) IS TRUE", frozenset({2, 3, 4})),
    ("(amount > 15) IS NOT TRUE", frozenset({1, 5})),
    ("(amount > 15) IS FALSE", frozenset({1})),
    ("(amount > 15) IS NOT FALSE", frozenset({2, 3, 4, 5})),
    ("status || '-' || region = 'paid-EU'", frozenset({1, 2})),
    ("CAST(quantity AS TEXT) = '2'", frozenset({2, 4})),
    ("quantity::text = '2'", frozenset({2, 4})),
    ("EXTRACT(month FROM created_at) = 2", frozenset({3, 4})),
    ("created_at > TIMESTAMP '2024-02-01 00:00:00'", frozenset({3, 4, 5})),
    ("order_date >= DATE '2024-02-25'", frozenset({4, 5})),
    ("order_date + INTERVAL '7 days' > DATE '2024-03-01'", frozenset({4, 5})),
    ("order_date + INTERVAL '7' DAY > DATE '2024-03-01'", frozenset({4, 5})),
    ("order_date + INTERVAL 7 DAY > DATE '2024-03-01'", frozenset({4, 5})),
    ("order_date < CURRENT_DATE", ALL_IDS),
    ("created_at < CURRENT_TIMESTAMP", ALL_IDS),
    ("SUBSTRING(status FROM 1 FOR 3) = 'pai'", frozenset({1, 2, 4})),
    ("POSITION('n' IN status) > 0", frozenset({3})),
    ("customer_name = 'O''Brien'", frozenset({1})),
    (r"path = 'C:\temp'", frozenset({1})),
    ('"order total" >= 300', frozenset({3, 4})),
    ("-amount < -25", frozenset({3, 4})),
    ("quantity % 2 = 1", frozenset({1, 3})),
    ("amount % 20 = 10", frozenset({1, 3})),
    ("amount ** 2 > 500", frozenset({3, 4})),
    ("CASE WHEN status = 'paid' THEN amount ELSE 0 END > 15", frozenset({2, 4})),
    ("CASE status WHEN 'void' THEN 1 ELSE 0 END = 1", frozenset({5})),
]
PRED_IDS = [p for p, _ in PREDICATES]

#: Row-level value forms and each order's value.
VALUES: list[tuple[str, dict[int, Any]]] = [
    ("status || '-' || region", {1: "paid-EU", 2: "paid-EU", 3: "Pending-US", 4: "paid-US", 5: None}),
    ("CAST(quantity AS TEXT)", {1: "1", 2: "2", 3: "3", 4: "2", 5: "4"}),
    ("quantity::text", {1: "1", 2: "2", 3: "3", 4: "2", 5: "4"}),
    ("EXTRACT(month FROM created_at)", {1: 1, 2: 1, 3: 2, 4: 2, 5: 3}),
    ("SUBSTRING(status FROM 2 FOR 3)", {1: "aid", 2: "aid", 3: "end", 4: "aid", 5: "oid"}),
    ("POSITION('a' IN status)", {1: 2, 2: 2, 3: 0, 4: 2, 5: 0}),
    ("quantity % 3", {1: 1, 2: 2, 3: 0, 4: 2, 5: 1}),
    ("amount % 7", {1: 3, 2: 6, 3: 2, 4: 5, 5: None}),
    ("amount ** 2", {1: 100, 2: 400, 3: 900, 4: 1600, 5: None}),
    ("CASE WHEN amount > 15 THEN 'big' ELSE 'small' END", {1: "small", 2: "big", 3: "big", 4: "big", 5: "small"}),
    ("date_diff('day', DATE '2024-01-01', order_date)", {1: 4, 2: 19, 3: 40, 4: 55, 5: 74}),
]
VALUE_IDS = [v for v, _ in VALUES]
DATE_VALUES: list[tuple[str, dict[int, str]]] = [
    ("order_date + INTERVAL '1 month'", {1: "2024-02-05", 2: "2024-02-20", 3: "2024-03-10", 4: "2024-03-25",
                                         5: "2024-04-15"}),
    ("order_date - INTERVAL 2 DAY", {1: "2024-01-03", 2: "2024-01-18", 3: "2024-02-08", 4: "2024-02-23",
                                     5: "2024-03-13"}),
]


# --------------------------------------------------------------------------- #
# Query shorthands and readers
# --------------------------------------------------------------------------- #
def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def m(formula: str, name: str = "v") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def ids(resp: SlayerResponse, *, model: str = "orders") -> set[int]:
    return {int(r[f"{model}.id"]) for r in resp.data}


def ordered_ids(resp: SlayerResponse, *, model: str = "orders") -> list[int]:
    return [int(r[f"{model}.id"]) for r in resp.data]


def by_key(resp: SlayerResponse, key: str, value: str = "v", *, model: str = "orders") -> dict:
    out = {r[f"{model}.{key}"]: r[f"{model}.{value}"] for r in resp.data}
    assert len(out) == len(resp.data), f"duplicate result rows for one {key}"
    return out


def single(resp: SlayerResponse, name: str = "v", *, model: str = "orders") -> Any:
    assert len(resp.data) == 1, resp.data
    return resp.data[0][f"{model}.{name}"]


def num(value: Any) -> float | None:
    return None if value is None else float(value)


def day(value: Any) -> str | None:
    """A date / timestamp cell as ``YYYY-MM-DD`` across SQLite text and DuckDB values."""
    return None if value is None else str(value)[:10]


async def emit(query: SlayerQuery, *, dialect: str, models: Iterable[SlayerModel] | None = None,
               datasource_fields: dict | None = None) -> str:
    """Dry-run SQL for ``query`` over the dev2101 models on ``dialect`` (asserts it parses there)."""
    first, *rest = list(models) if models is not None else dev2101_models()
    return await _engine_generate(query=query, model=first, extra_models=rest, dialect=dialect,
                                  datasource_fields=datasource_fields)


# --------------------------------------------------------------------------- #
# NULL ordering (spec: queries/positions › Order items carry an explicit NULL placement)
# --------------------------------------------------------------------------- #
#: (direction, nulls) → raw-rows order of ids by ``discount`` (NULL on 1 and 4), ties by id.
DISCOUNT_ORDER = {
    ("desc", "first"): [1, 4, 2, 5, 3],
    ("desc", "last"): [2, 5, 3, 1, 4],
    ("asc", "first"): [1, 4, 3, 5, 2],
    ("asc", "last"): [3, 5, 2, 1, 4],
}
#: (direction, nulls) → cities ordered by the hidden ``max(discount)`` (NULL for Paris and LA), ties by city.
CITY_ORDER = {
    ("asc", "last"): ["NYC", "Rome", "Berlin", "LA", "Paris"],
    ("asc", "first"): ["LA", "Paris", "NYC", "Rome", "Berlin"],
    ("desc", "first"): ["LA", "Paris", "Berlin", "Rome", "NYC"],
    ("desc", "last"): ["Berlin", "Rome", "NYC", "LA", "Paris"],
}


def discount_rows_query(*, direction: str, nulls: str) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["id"], "distinct_dimension_values": False,
        "order": [{"column": "discount", "direction": direction, "nulls": nulls}, {"column": "id"}],
    })


def hidden_max_query(*, direction: str, nulls: str) -> SlayerQuery:
    return q(dimensions=["city"], order=[
        {"column": "max(discount)", "direction": direction, "nulls": nulls}, {"column": "city"},
    ])


async def check_null_ordering(engine: SlayerQueryEngine) -> None:
    """Every NULL-placement case on a live engine (shared by the server suites)."""
    for (direction, nulls), expected in DISCOUNT_ORDER.items():
        resp = await engine.execute(discount_rows_query(direction=direction, nulls=nulls))
        assert ordered_ids(resp) == expected, (direction, nulls, resp.sql)
    for (direction, nulls), expected in CITY_ORDER.items():
        resp = await engine.execute(hidden_max_query(direction=direction, nulls=nulls))
        assert [r["orders.city"] for r in resp.data] == expected, (direction, nulls, resp.sql)
        assert resp.columns == ["orders.city"]


# --------------------------------------------------------------------------- #
# Server suites (Postgres, MySQL, SQL Server)
# --------------------------------------------------------------------------- #
_SERVER_PARAMS = {"postgres": ("%s", '"'), "mysql": ("%s", "`"), "tsql": ("?", '"')}


def seed_server(cursor: Any, *, dialect: str) -> None:
    """Create and fill the three tables through a DB-API ``cursor`` on ``dialect``."""
    placeholder, quote = _SERVER_PARAMS[dialect]
    for stmt in ddl(dialect):
        cursor.execute(stmt)
    for table, (_, rows) in TABLES.items():
        cursor.executemany(insert_sql(table, placeholder=placeholder, quote=quote), rows)


async def check_server_forms(engine: SlayerQueryEngine) -> None:
    """Every SQL-form predicate as a filter, and the forms the spec pins on every Tier-1 dialect."""
    for pred, expected in PREDICATES:
        resp = await engine.execute(q(dimensions=["id"], filters=[pred]))
        assert ids(resp) == expected, (pred, resp.sql)
    for expr, values in VALUES:
        resp = await engine.execute(q(dimensions=["id", {"expression": expr, "name": "x"}]))
        got = {int(k): (v if v is None or isinstance(v, str) else float(v)) for k, v in by_key(resp, "id", "x").items()}
        want = {k: (v if v is None or isinstance(v, str) else float(v)) for k, v in values.items()}
        assert got == want, (expr, resp.sql)
    assert num(single(await engine.execute(q(measures=[m("min(-7 % 3)")])))) == -1


async def check_try_cast(engine: SlayerQueryEngine, *, nulls_invalid: bool = True) -> None:
    """``TRY_CAST(code AS INT)``: 12 / NULL / NULL / 7 / NULL; ``nulls_invalid=False`` expects the database's error."""
    query = q(dimensions=["id", {"expression": "TRY_CAST(code AS INT)", "name": "x"}])
    if not nulls_invalid:
        sql = (await engine.execute(query, dry_run=True)).sql or ""
        assert "PG_INPUT_IS_VALID" not in sql.upper(), sql
        with pytest.raises(Exception, match="(?i)invalid input syntax"):  # noqa: B017 — the database's own error
            await engine.execute(query)
        return
    resp = await engine.execute(query)
    got = {int(k): (None if v is None else int(v)) for k, v in by_key(resp, "id", "x").items()}
    assert got == {1: 12, 2: None, 3: None, 4: 7, 5: None}, resp.sql


async def check_filtered_aggregations(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(dimensions=["region"], measures=[m("count(*) FILTER (WHERE status = 'paid')")]))
    assert {k: num(v) for k, v in by_key(resp, "region").items()} == {"EU": 2, "US": 1, None: 0}, resp.sql
    resp = await engine.execute(q(measures=[m("weighted_avg(price, weight=quantity) FILTER (WHERE status = 'paid')")]))
    assert num(single(resp)) == pytest.approx(3.6), resp.sql
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "balances", "dimensions": ["customer_id"],
        "measures": [{"formula": "last(balance, updated_at) FILTER (WHERE status = 'active')", "name": "v"}],
    }))
    assert {int(k): num(v) for k, v in by_key(resp, "customer_id", model="balances").items()} == {1: 150.0, 2: 80.0}
