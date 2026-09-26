"""Shared fixtures for DEV-1965 statement assembly.

orders (id, status, region, amount, qty, created_at):
   1  a  US  10  1  2024-01-05
   2  b  EU  20  2  2024-01-20
   3  a  EU  30  3  2024-02-10
   4  b  US  40  4  2024-02-25
   5  a  US  50  5  2024-03-05
   6  c  EU  60  6  2024-03-15
   7  b  US  70  7  2024-04-10

Monthly ``amount:sum``: Jan 30, Feb 70, Mar 110, Apr 70; cumulative 30, 100, 210, 280.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import _engine_generate, seeded_exec_engine

TIER1 = ["postgres", "sqlite", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]
OUTER_ALIAS = "_outer"

ORDERS_ROWS = [
    (1, "a", "US", 10.0, 1.0, "2024-01-05"),
    (2, "b", "EU", 20.0, 2.0, "2024-01-20"),
    (3, "a", "EU", 30.0, 3.0, "2024-02-10"),
    (4, "b", "US", 40.0, 4.0, "2024-02-25"),
    (5, "a", "US", 50.0, 5.0, "2024-03-05"),
    (6, "c", "EU", 60.0, 6.0, "2024-03-15"),
    (7, "b", "US", 70.0, 7.0, "2024-04-10"),
]
CUMSUM_BY_MONTH = {"2024-01": 30.0, "2024-02": 100.0, "2024-03": 210.0, "2024-04": 280.0}


def orders_model(*, filters: Optional[list[str]] = None) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at", filters=filters or [],
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="qty", sql="qty", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
    )


def q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery.model_validate(kw)


def month_td() -> list[dict]:
    return [{"dimension": "created_at", "granularity": "month"}]


def cumsum_chain(**kw) -> SlayerQuery:
    """``cumsum(amount:sum)`` by month (a one-step transform chain)."""
    kw.setdefault("time_dimensions", month_td())
    kw.setdefault("measures", [{"formula": "cumsum(amount:sum)", "name": "running"}])
    return q(**kw)


MODE_A_OR = "status = 'a' OR status = 'b'"


def where_having_query() -> SlayerQuery:
    """Same-phase WHERE / HAVING conjuncts, two of them ORs."""
    return q(dimensions=["region"], measures=["amount:sum"], filters=[
        "amount > 15",
        "region == 'US' or region == 'EU'",
        "amount:sum > 55 or amount:count > 2",
        "amount:max < 1000",
    ])


async def gen(query, *, dialect: str, model: Optional[SlayerModel] = None) -> str:
    return await _engine_generate(
        query=query, model=model or orders_model(), dialect=dialect, validate=False,
    )


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, region TEXT, "
            "amount REAL, qty REAL, created_at TEXT)"
        )
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE orders (id INTEGER, status VARCHAR, region VARCHAR, "
        "amount DOUBLE, qty DOUBLE, created_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", ORDERS_ROWS)
    con.close()


@asynccontextmanager
async def exec_engine(
    dialect: str, *, model: Optional[SlayerModel] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=[model or orders_model()],
    ) as (engine, _db):
        yield engine


def month_key(value) -> str:
    return str(value)[:7]


# --------------------------------------------------------------------------- #
# Statement-shape helpers.
# --------------------------------------------------------------------------- #
def parse(sql: str, dialect: str) -> exp.Select:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(tree, exp.Select), sql
    return tree


def outer_derived(top: exp.Select) -> exp.Select:
    """The chain's final select: the body of the ``_outer`` derived table."""
    frm = top.args.get("from_") or top.args.get("from")
    assert frm is not None, top.sql()
    assert isinstance(frm.this, exp.Subquery), top.sql()
    assert frm.this.alias == OUTER_ALIAS, top.sql()
    inner = frm.this.this
    assert isinstance(inner, exp.Select), top.sql()
    return inner


def cte_names(top: exp.Select) -> list[str]:
    with_ = top.args.get("with_")
    return [c.alias_or_name for c in with_.expressions] if with_ is not None else []


def assert_single_top_level_with(sql: str, dialect: str) -> exp.Select:
    """The statement opens with one ``WITH`` and no other ``WITH`` exists anywhere."""
    top = parse(sql, dialect)
    assert top.args.get("with_") is not None, f"no top-level WITH:\n{sql}"
    nested = [w for w in top.find_all(exp.With) if w.parent is not top]
    assert not nested, f"WITH nested below the statement top:\n{sql}"
    assert sql.lstrip().upper().startswith("WITH"), sql
    return top


def from_name(select: exp.Select) -> str:
    frm = select.args.get("from_") or select.args.get("from")
    assert frm is not None, select.sql()
    return frm.this.alias_or_name


def where_of(select: exp.Select) -> Optional[Expression]:
    where = select.args.get("where")
    return where.this if where is not None else None


def post_filter_where(sql: str, dialect: str = "postgres") -> str:
    """The rendered WHERE of the chain's final select, which must read the last chain step."""
    top = parse(sql, dialect)
    names = cte_names(top)
    assert names, f"no top-level WITH:\n{sql}"
    final = outer_derived(top)
    assert from_name(final) == names[-1], sql
    where = where_of(final)
    assert where is not None, f"no post-phase WHERE on the chain's final select:\n{sql}"
    return where.sql(dialect=dialect)


def outer_order(sql: str, dialect: str) -> list[tuple[str, str, bool]]:
    """``(column name, qualifier, descending)`` per outer ORDER BY term."""
    order = parse(sql, dialect).args.get("order")
    assert order is not None, sql
    out = []
    for ordered in order.expressions:
        assert isinstance(ordered.this, exp.Column), sql
        out.append((ordered.this.name, ordered.this.table, bool(ordered.args.get("desc"))))
    return out


def split_chain(sql: str) -> tuple[str, str]:
    """``(WITH block, outermost statement)`` of a pretty-printed hoisted statement."""
    idx = sql.rindex("\nSELECT")
    return sql[:idx], sql[idx:]


__all__ = [
    "CUMSUM_BY_MONTH", "MODE_A_OR", "ORDERS_ROWS", "OUTER_ALIAS", "TIER1",
    "assert_single_top_level_with", "cte_names", "cumsum_chain", "exec_engine",
    "from_name", "gen", "month_key", "month_td", "orders_model", "outer_derived",
    "outer_order", "parse", "post_filter_where", "q", "split_chain", "where_having_query", "where_of",
]
