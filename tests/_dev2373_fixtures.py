"""A small shop for typed stage output fields and custom-aggregation result types.

Physical tables::

    orders    (id, customer_id, status, amount, quantity, rate, price, flag, order_date, ordered_at, region)
    customers (id, name, created_at, spend)

Stored edge: ``orders → customers`` (LEFT, many_to_one). ``orders`` defines the custom aggregations
``trues``, ``share``, ``all_true``, ``my_avg``, ``wsum`` (``weight`` defaults to ``1``), ``wdef`` (``weight``
defaults to ``rate``), ``custom_pick`` and ``total``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, Optional

import pytest
from sqlglot import exp

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.models import Aggregation, AggregationParam, DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.scope import StageField
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.plan import plan_stages
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import _engine_generate, seeded_exec_engine

DS = "test"
DIALECTS = ["sqlite", "duckdb"]

_INT, _TEXT, _DOUBLE, _DEC = DataType.INTEGER, DataType.STRING, DataType.FLOAT, DataType.DECIMAL
_BOOL, _DATE, _TS = DataType.BOOLEAN, DataType.DATE, DataType.DATETIME

_ORDERS_DDL = (
    "id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE, quantity INTEGER, rate DOUBLE, "
    "price DECIMAL(10,2), flag BOOLEAN, order_date DATE, ordered_at TIMESTAMP, region TEXT"
)
_ORDERS = [
    (1, 1, "paid", 10.0, 2, 1.5, Decimal("12.50"), True, "2024-01-10", "2024-01-10 09:00:00", "N"),
    (2, 1, "open", 20.0, 3, 2.0, Decimal("7.25"), False, "2024-01-20", "2024-01-20 10:00:00", "S"),
    (3, 2, "paid", 30.0, 1, 0.5, Decimal("3.00"), True, "2024-02-10", "2024-02-10 11:00:00", "N"),
    (4, 3, "paid", 40.0, 6, 1.0, Decimal("10.00"), False, "2024-02-15", "2024-02-15 12:00:00", "N"),
    (5, 2, "open", 50.0, 5, 2.5, Decimal("1.25"), True, "2024-03-05", "2024-03-05 13:00:00", "S"),
]
_CUSTOMERS_DDL = "id INTEGER, name TEXT, created_at TIMESTAMP, spend DECIMAL(10,2)"
_CUSTOMERS = [
    (1, "Ann", "2023-05-01 00:00:00", Decimal("100.50")),
    (2, "Bob", "2023-06-01 00:00:00", Decimal("200.25")),
    (3, "Cid", "2023-07-01 00:00:00", Decimal("50.00")),
]


def _seed(db_path: str, *, dialect: str) -> None:
    tables = {"orders": (_ORDERS_DDL, _ORDERS), "customers": (_CUSTOMERS_DDL, _CUSTOMERS)}
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for table, (ddl, rows) in tables.items():
                con.execute(f"CREATE TABLE {table} ({ddl})")
                con.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(rows[0]))})", rows)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for table, (ddl, rows) in tables.items():
            cur.execute(f"CREATE TABLE {table} ({ddl})")
            sqlite_rows = [tuple(float(v) if isinstance(v, Decimal) else v for v in r) for r in rows]
            cur.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(rows[0]))})", sqlite_rows)


def _fields(*spec: tuple[str, DataType], pk: Optional[str] = "id") -> list[DatasetField]:
    return [DatasetField(name=n, datatype=t, primary_key=n == pk) for n, t in spec]


def custom_aggregations(*, trues_datatype: Optional[str] = None) -> list[Aggregation]:
    trues: dict[str, Any] = {"name": "trues", "expression": "COUNT(CASE WHEN {value} THEN 1 END)"}
    if trues_datatype is not None:
        trues["datatype"] = trues_datatype
    return [
        Aggregation.model_validate(trues),
        Aggregation(name="share", expression="SUM({value}) * 1.0 / COUNT(*)"),
        Aggregation(name="all_true", expression="BOOL_AND({value})"),
        Aggregation(name="my_avg", expression="AVG({value})"),
        Aggregation(name="wsum", expression="SUM({value} * {weight})", params=[AggregationParam(name="weight", sql="1")]),
        Aggregation(name="wdef", expression="SUM({value} * {weight})", params=[AggregationParam(name="weight", sql="rate")]),
        Aggregation(name="custom_pick", expression="MY_UDF({value})"),
        Aggregation(name="total", expression="SUM({value})"),
    ]


def orders(*, aggregations: Optional[list[Aggregation]] = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders", default_time_dimension="ordered_at",
        fields=_fields(
            ("id", _INT), ("customer_id", _INT), ("status", _TEXT), ("amount", _DOUBLE), ("quantity", _INT),
            ("rate", _DOUBLE), ("price", _DEC), ("flag", _BOOL), ("order_date", _DATE), ("ordered_at", _TS),
            ("region", _TEXT),
        ),
        aggregations=custom_aggregations() if aggregations is None else aggregations,
        relationships=[DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"],
            join_type=JoinType.LEFT, cardinality=RelationshipCardinality.MANY_TO_ONE,
        )],
    )


def customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source=DS, sql_table="customers",
        fields=_fields(("id", _INT), ("name", _TEXT), ("created_at", _TS), ("spend", _DEC)),
    )


def shop_datasets(*, orders_dataset: Optional[SlayerDataset] = None) -> list[SlayerDataset]:
    return [orders_dataset or orders(), customers()]


def bundle(*, dialect: str = "duckdb", datasets: Optional[list[SlayerDataset]] = None) -> ResolvedSourceBundle:
    population, *rest = datasets or shop_datasets()
    return ResolvedSourceBundle(dialect=dialect, population=population, referenced_datasets=rest)


def q(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(fields)


def m(expr: str, alias: str = "x", **kw: Any) -> dict:
    return {"metric": expr, "alias": alias, **kw}


def d(expr: str, alias: str = "x") -> dict:
    return {"field": expr, "alias": alias}


def stage_columns(queries: list[SlayerQuery], *, stage: str, dialect: str = "duckdb",
                  datasets: Optional[list[SlayerDataset]] = None) -> dict[str, StageField]:
    """Every field stage ``stage`` emits, by name."""
    planned = plan_stages(queries=queries, bundle=bundle(dialect=dialect, datasets=datasets))
    (schema,) = [p.stage_schema for p in planned if p.stage_schema is not None and p.stage_schema.relation_name == stage]
    return {c.name: c for c in schema.fields}


def read_types(first: dict[str, Any], *, names: list[str], dialect: str = "duckdb",
               datasets: Optional[list[SlayerDataset]] = None) -> dict[str, Optional[DataType]]:
    """The types a later stage reads ``s``'s columns ``names`` with: each re-projected by stage ``t`` as itself."""
    s = q(name="s", population="orders", **first)
    t = q(name="t", population="s", dimensions=[d(f"s.{n}", n) for n in names])
    root = q(population="t", dimensions=[f"t.{n}" for n in names])
    columns = stage_columns([s, t, root], stage="t", dialect=dialect, datasets=datasets)
    return {n: columns[n].datatype for n in names}


def emitted_types(first: dict[str, Any], *, names: list[str], dialect: str = "duckdb",
                  datasets: Optional[list[SlayerDataset]] = None) -> dict[str, Optional[DataType]]:
    """The types stage ``s`` emits for its columns ``names``."""
    s = q(name="s", population="orders", **first)
    root = q(population="s", dimensions=[f"s.{n}" for n in names])
    columns = stage_columns([s, root], stage="s", dialect=dialect, datasets=datasets)
    return {n: columns[n].datatype for n in names}


def plan_type(item: dict[str, Any], *, name: str = "x", dialect: str = "duckdb",
              datasets: Optional[list[SlayerDataset]] = None, population: str = "orders") -> Optional[DataType]:
    """The query plan's type of the public metric ``name`` of a one-stage query over ``population``."""
    query = q(population=population, **item)
    prebound = bind_query_inputs(query=query, bundle=bundle(dialect=dialect, datasets=datasets))
    (found,) = [dm.datatype for dm in prebound.declared_metrics if dm.public_name == name]
    return found


async def postgres_sql(queries: list[SlayerQuery | dict], *, datasets: Optional[list[SlayerDataset]] = None) -> str:
    """The rendered Postgres SQL of ``queries`` (dry run)."""
    population, *rest = datasets or shop_datasets()
    return await _engine_generate(query=queries, dataset=population, extra_datasets=rest, dialect="postgres")


def cast_target(expression: exp.Expression) -> Optional[str]:
    """The Postgres type an outermost CAST converts to, else ``None``."""
    if isinstance(expression, exp.Cast):
        return expression.to.sql(dialect="postgres").upper()
    return None


@asynccontextmanager
async def shop(dialect: str, datasets: Optional[list[SlayerDataset]] = None) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=dialect, seed=lambda path: _seed(path, dialect=dialect),
        datasets=datasets if datasets is not None else shop_datasets(),
    ) as (engine, _db):
        yield engine


def value_of(row: dict[str, Any], name: str) -> Any:
    """``row``'s value under ``name`` or a key ending in ``.name``."""
    (key,) = [k for k in row if k == name or k.endswith(f".{name}")]
    return row[key]


def num(v: Any) -> Optional[float]:
    return None if v is None else float(v)
