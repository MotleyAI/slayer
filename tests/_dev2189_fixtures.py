"""Shared tenant schema, policies and query shapes for the policy-on-the-statement-tree suites."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Any, Optional

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.core.enums import DataType
from slayer.core.errors import ForcedFilterError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient, TypeProbe
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.session_policy import ScopedTable, apply_session_policy
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1739_fixtures import transaction

TENANT = "org"
TENANT_VALUE = "a"

EXECUTED_DIALECTS = ("sqlite", "duckdb", "postgres")
EMITTED_DIALECTS = ("bigquery", "tsql", "snowflake", "clickhouse", "mysql")
NO_LIST_DIALECTS = frozenset({"sqlite", "mysql", "tsql"})

#: Tables carrying the tenant column; ``regions`` is shared reference data without it.
TENANT_TABLES = frozenset({"customers", "orders", "items"})

REGIONS = [(1, "North"), (2, "South")]
CUSTOMERS = [(1, "Ann", 1, "a"), (2, "Bob", 2, "b"), (3, "Cid", 1, "a")]
ORDERS = [
    (1, 1, "paid", 10.0, "2025-01-05 10:00:00", "a", ["x", "y"]),
    (2, 1, "open", 20.0, "2025-02-10 10:00:00", "a", ["x"]),
    (3, 2, "paid", 30.0, "2025-01-20 10:00:00", "b", ["y"]),
    (4, 3, "paid", 40.0, "2025-03-01 10:00:00", "a", []),
    (5, 2, "open", 50.0, "2025-02-15 10:00:00", "b", None),
]
ITEMS = [(1, 1, 2.0, "a"), (2, 1, 1.0, "a"), (3, 3, 5.0, "b"), (4, 4, 1.0, "a")]

ORDER_TOTALS_SQL = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"


def rls_models(*, data_source: str = "test") -> list[SlayerModel]:
    """``regions`` ← ``customers`` ← ``orders`` ← ``items``, plus a sql-mode ``order_totals``."""
    return [
        SlayerModel(name="regions", sql_table="regions", data_source=data_source, columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ]),
        SlayerModel(name="customers", sql_table="customers", data_source=data_source, columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
            Column(name=TENANT, type=DataType.TEXT),
        ], joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
            ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]]),
        ]),
        SlayerModel(name="orders", sql_table="orders", data_source=data_source,
                    default_time_dimension="created_at", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name=TENANT, type=DataType.TEXT),
            Column(name="tags", type=DataType.ARRAY, element_type=DataType.TEXT),
        ]),
        SlayerModel(name="items", sql_table="items", data_source=data_source, columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="order_id", type=DataType.INT),
            Column(name="qty", type=DataType.DOUBLE),
            Column(name=TENANT, type=DataType.TEXT),
        ], joins=[ModelJoin(target_model="orders", join_pairs=[["order_id", "id"]])]),
        SlayerModel(name="order_totals", sql=ORDER_TOTALS_SQL, data_source=data_source, columns=[
            Column(name="customer_id", type=DataType.INT, primary_key=True),
            Column(name="total", type=DataType.DOUBLE),
        ]),
    ]


#: The query-backed model the ``query_backed`` shape reads (saved by the engine builders).
QUERY_BACKED = SlayerModel.model_validate({
    "name": "spend", "data_source": "test",
    "source_queries": [{"source_model": "orders", "dimensions": ["customer_id"], "measures": ["amount:sum"]}],
})


# --------------------------------------------------------------------------- #
# Seeding
# --------------------------------------------------------------------------- #
def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return repr(value)


def seed_statements(backend: str) -> list[str]:
    """DDL + inserts for ``backend`` (SQLite holds the list as JSON text)."""
    ts = {"sqlite": "TEXT", "duckdb": "TIMESTAMP", "postgres": "TIMESTAMP"}[backend]
    tags = {"sqlite": "TEXT", "duckdb": "VARCHAR[]", "postgres": "TEXT[]"}[backend]

    def tag_literal(value: Optional[list[str]]) -> str:
        if value is None:
            return "NULL"
        if backend == "sqlite":
            return _sql_literal(json.dumps(value))
        items = ", ".join(_sql_literal(v) for v in value)
        return f"ARRAY[{items}]::TEXT[]" if backend == "postgres" else f"[{items}]::VARCHAR[]"

    def rows(table: str, data: list[tuple], *, last: Callable[[Any], str] = _sql_literal) -> str:
        body = ", ".join(
            "(" + ", ".join([*(_sql_literal(v) for v in r[:-1]), last(r[-1])]) + ")" for r in data
        )
        return f"INSERT INTO {table} VALUES {body}"

    orders = [(*r[:4], r[4], r[5], r[6]) for r in ORDERS]
    return [
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)",
        f"CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region_id INTEGER, {TENANT} TEXT)",
        f"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, amount DOUBLE PRECISION, "
        f"created_at {ts}, {TENANT} TEXT, tags {tags})",
        f"CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, qty DOUBLE PRECISION, {TENANT} TEXT)",
        rows("regions", REGIONS),
        rows("customers", CUSTOMERS),
        rows("orders", orders, last=tag_literal),
        rows("items", ITEMS),
    ]


def _seed_file(backend: str, db_path: str) -> None:
    if backend == "sqlite":
        with transaction(db_path) as con:
            for stmt in seed_statements("sqlite"):
                con.execute(stmt.replace("DOUBLE PRECISION", "REAL"))
        return
    duckdb = pytest.importorskip("duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        for stmt in seed_statements("duckdb"):
            con.execute(stmt)


async def save_models(storage: YAMLStorage, *, data_source: str = "test") -> None:
    for model in rls_models(data_source=data_source):
        await storage.save_model(model)


async def save_query_backed(engine: SlayerQueryEngine) -> None:
    await engine.save_model(QUERY_BACKED)


@contextlib.asynccontextmanager
async def file_engine(backend: str) -> AsyncGenerator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    """A policy-free engine over a seeded SQLite / DuckDB file, with the query-backed model saved."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "seed.duckdb" if backend == "duckdb" else "seed.db")
        _seed_file(backend, db_path)
        ds = DatasourceConfig(name="test", type=backend, database=db_path)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(ds)
        await save_models(storage)
        engine = SlayerQueryEngine(storage=storage)
        try:
            await save_query_backed(engine)
            yield engine, ds
        finally:
            engine.close()
            engine_factory.invalidate_engine(ds)


@contextlib.asynccontextmanager
async def emission_engine(dialect: str) -> AsyncGenerator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    """A policy-free engine on a ``dialect``-typed datasource with no database behind it."""
    with tempfile.TemporaryDirectory() as tmp:
        ds = DatasourceConfig(name="test", type=dialect)
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_datasource(ds)
        await save_models(storage)
        # A query-backed model's save-time dry run never connects.
        await storage.save_model(QUERY_BACKED, _validate=False)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine, ds
        finally:
            engine.close()


async def correlating_profile(_self: Any) -> ServerProfile:
    """A ClickHouse server that runs correlated subqueries."""
    return ServerProfile(version=(25, 8), readonly=0, correlated_subqueries=True)


# --------------------------------------------------------------------------- #
# Policies
# --------------------------------------------------------------------------- #
ORDERS_RULE = JoinFilterRule(target_table="orders", join_path=("orders.customer_id = customers.id",))
ITEMS_RULE = JoinFilterRule(
    target_table="items", join_path=("items.order_id = orders.id", "orders.customer_id = customers.id"),
)


def _join(*, joins: list[JoinFilterRule], whitelist: list[str]) -> SessionPolicy:
    return SessionPolicy(ruleset=JoinFilterRuleset(
        table="customers", column=TENANT, value=TENANT_VALUE, joins=tuple(joins), whitelist=tuple(whitelist),
    ))


POLICIES: dict[str, SessionPolicy] = {
    "column_pass": SessionPolicy(ruleset=ColumnFilterRuleset(column=TENANT, value=TENANT_VALUE, on_unapplicable="pass")),
    "column_block": SessionPolicy(ruleset=ColumnFilterRuleset(column=TENANT, value=TENANT_VALUE)),
    "join_one_hop": _join(joins=[ORDERS_RULE], whitelist=["regions", "items"]),
    "join_multi_hop": _join(joins=[ORDERS_RULE, ITEMS_RULE], whitelist=["regions"]),
    "join_whitelist": _join(joins=[], whitelist=["orders", "items", "regions"]),
}


def stub_has_column(table: ScopedTable, column: str) -> Optional[bool]:
    """Column presence from the fixture schema (emission dialects have no database to probe)."""
    return table.name.casefold() in TENANT_TABLES if column == TENANT else None


async def stub_column_present(  # NOSONAR(S7503) — stands in for the async engine method
    *, datasource: DatasourceConfig, scoped_table: ScopedTable, column: str,
) -> Optional[bool]:
    return stub_has_column(scoped_table, column)


# --------------------------------------------------------------------------- #
# Query shapes
# --------------------------------------------------------------------------- #
def _q(body: dict[str, Any]) -> SlayerQuery:
    return SlayerQuery.model_validate(body)


SHAPES: dict[str, Any] = {
    "plain": _q({"source_model": "orders", "dimensions": ["status"], "measures": ["amount:sum"]}),
    "joined_dims": _q({"source_model": "orders", "dimensions": ["customers.name", "customers.regions.name"],
                       "measures": ["amount:sum"]}),
    "multi_stage": [
        {"name": "per_customer", "source_model": "orders", "dimensions": ["customer_id"], "measures": ["amount:sum"]},
        {"source_model": "per_customer", "measures": [{"formula": "amount_sum:max", "name": "top"}]},
    ],
    "query_backed": _q({"source_model": "spend", "measures": [{"formula": "amount_sum:sum", "name": "total"}]}),
    "sql_mode": _q({"source_model": "order_totals", "dimensions": ["customer_id"], "measures": ["total:sum"]}),
    "semi_join": _q({"source_model": "customers", "dimensions": ["name"], "filters": ["orders.amount > 15"],
                     "measures": [{"formula": "count(*)", "name": "n"}]}),
    "list_unnest": _q({"source_model": "orders", "dimensions": ["tags.value"],
                       "measures": [{"formula": "count(tags.*)", "name": "n"}]}),
    "time_spine": _q({"measures": [{"formula": "sum(orders.amount)", "name": "o"}],
                      "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month",
                                           "date_range": ["2025-01-01", "2025-03-31"]}]}),
    "cross_model": _q({"source_model": "orders", "dimensions": ["status"],
                       "measures": [{"formula": "amount:sum / customers.id:count", "name": "r"}]}),
    "paginated": _q({"source_model": "orders", "dimensions": ["status"], "measures": ["amount:sum"],
                     "order": [{"column": "amount_sum", "direction": "desc"}], "limit": 1, "offset": 1}),
}


def shape_supported(*, shape: str, dialect: str) -> bool:
    return not (shape == "list_unnest" and dialect in NO_LIST_DIALECTS)


# --------------------------------------------------------------------------- #
# The reference: the previous rewrite of the rendered, unscoped SQL
# --------------------------------------------------------------------------- #
def sqlglot_name(dialect: str) -> str:
    return get_dialect(dialect).sqlglot_name


def reference_scoped_sql(*, sql: str, dialect: str, policy: SessionPolicy, has_column) -> str:
    """Parse the unscoped render, apply ``policy`` to that tree, render it."""
    tree: Expr = sqlglot.parse_one(sql, read=dialect)
    scoped = apply_session_policy(tree, sql_dialect=get_dialect(dialect), policy=policy, has_column=has_column)
    return scoped.sql(dialect=dialect)


def parsed_query(sql: str, *, dialect: str) -> exp.Query:
    """``sql`` parsed; a rendered statement is always a query."""
    tree = sqlglot.parse_one(sql, read=dialect)
    assert isinstance(tree, exp.Query)
    return tree


def normalized(sql: str, *, dialect: str) -> str:
    return sqlglot.parse_one(sql, read=dialect).sql(dialect=dialect)


def refusal(exc: ForcedFilterError) -> tuple[Optional[str], Optional[str]]:
    return exc.table, exc.column


def row_values(rows: list[dict[str, Any]]) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


class ClientSpy:
    """Records every statement an ``SlayerSQLClient`` sends to the database."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.probed: list[str] = []

    @contextlib.contextmanager
    def installed(self, monkeypatch: pytest.MonkeyPatch) -> Generator[ClientSpy]:
        execute, probe = SlayerSQLClient.execute, SlayerSQLClient.get_result_column_types

        async def _execute(client: SlayerSQLClient, *args: Any, **kwargs: Any) -> Any:
            self.executed.append(str(kwargs.get("sql", args[0] if args else "")))
            return await execute(client, *args, **kwargs)

        async def _probe(client: SlayerSQLClient, type_probe: TypeProbe) -> Any:
            self.probed.append(type_probe.sql)
            return await probe(client, type_probe)

        monkeypatch.setattr(SlayerSQLClient, "execute", _execute)
        monkeypatch.setattr(SlayerSQLClient, "get_result_column_types", _probe)
        yield self
