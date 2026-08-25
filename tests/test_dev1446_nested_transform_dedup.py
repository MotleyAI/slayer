"""DEV-1450 stage 7b.15e — DEV-1446 acceptance via ``engine.execute``.

DEV-1446: a transform-wrapped aggregate ref of a renamed measure dedupes onto
the declared slot. With ``{"formula": "amount:sum", "name": "revenue"}`` and a
filter ``change(amount:sum) > 0``, the inner ``amount:sum`` and the declared
``revenue`` measure intern to ONE ``AggregateKey(amount, sum)`` slot (P2). The
base CTE computes ``SUM(amount)`` once; the ``change`` desugaring reuses that
slot's alias rather than minting a second ``amount_sum`` column.

End-to-end through the full cutover path against a seeded SQLite. The
planner-level dedup is pinned in the generator2 suites; this asserts the
actual filtered rows AND the single-slot structure in the emitted SQL.
"""

from __future__ import annotations

import os
import re
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def engine() -> AsyncIterator[Tuple[SlayerQueryEngine, str]]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount REAL, "
        "created_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, "paid", 10.0, "2024-01-15"),
            (2, "paid", 5.0, "2024-02-15"),
            (3, "open", 7.0, "2024-01-20"),
            (4, "open", 3.0, "2024-02-20"),
            (5, "paid", 9.0, "2024-03-10"),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
            ],
        )
    )
    yield SlayerQueryEngine(storage=storage), db_path


def _cte_body(sql: str, name: str) -> str:
    """Return the body of the named CTE (``<name> AS ( ... )``) with
    whitespace collapsed, by matching parentheses from the opening ``(``.
    """
    flat = re.sub(r"\s+", " ", sql)
    needle = f"{name} AS ("
    idx = flat.find(needle)
    assert idx >= 0, f"CTE {name!r} not found in SQL: {flat!r}"
    start = idx + len(needle)
    depth = 1
    i = start
    while i < len(flat) and depth > 0:
        if flat[i] == "(":
            depth += 1
        elif flat[i] == ")":
            depth -= 1
            if depth == 0:
                return flat[start:i]
        i += 1
    raise AssertionError(f"Unbalanced parens in CTE {name!r}")


def _query() -> SlayerQuery:
    # Monthly SUM(amount): Jan=17, Feb=8, Mar=9. change = current - prior:
    # Jan -> NULL, Feb -> -9, Mar -> +1. Only March passes ``change > 0``.
    return SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )
        ],
        measures=[{"formula": "amount:sum", "name": "revenue"}],
        filters=["change(amount:sum) > 0"],
    )


async def test_dev1446_change_filter_keeps_correct_rows(engine):
    eng, _ = engine
    resp = await eng.execute(_query())
    assert resp.columns == ["orders.created_at", "orders.revenue"]
    assert [dict(r) for r in resp.data] == [
        {"orders.created_at": "2024-03-01", "orders.revenue": 9.0}
    ]


async def test_dev1446_aggregate_interned_to_one_slot(engine):
    """The inner ``amount:sum`` inside ``change(...)`` dedupes onto the
    declared ``revenue`` slot — no separate ``amount_sum`` column is minted,
    and the ``change`` arithmetic in the filter is computed against the
    renamed slot's alias (``orders.revenue``).
    """
    eng, _ = engine
    dry = await eng.execute(_query(), dry_run=True)
    sql = dry.sql
    assert sql is not None
    # The row→aggregate phase lives in the ``base`` CTE. Scope the
    # single-aggregate assertion to that CTE body (the time-shifted
    # self-join CTE legitimately recomputes the aggregate at the shifted
    # grain and must not be counted as a duplicate slot).
    base = _cte_body(sql, "base").upper()
    assert base.count("SUM(ORDERS.AMOUNT)") == 1, base
    # Had the inner agg-ref minted its own slot, the base CTE would project a
    # second ``amount_sum`` aggregate alias alongside the declared ``revenue``
    # one; dedup means it does not.
    assert "AMOUNT_SUM" not in base, base
    # The change desugaring subtracts the shifted value from the renamed
    # slot's alias — proof the inner ref bound to the declared slot.
    assert '"orders.revenue" - "orders._time_shift_inner"' in sql
