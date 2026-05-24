"""DEV-1450 follow-up #4b — model filters referencing derived columns.

A ``SlayerModel.filters`` entry (Mode-A SQL, always-applied WHERE) that
references a non-trivial DERIVED column (``Column.sql`` set) now inlines
the column's expanded SQL instead of raising ``NotImplementedError``.
When the derived column's SQL crosses a join, the ``LEFT JOIN`` is pulled
into the FROM and the predicate is qualified to the join alias.

The base-only model-filter path (no derived columns) is unchanged, and
the two genuine rejects — a windowed ``Column.sql`` and a same-model
``ModelMeasure`` reference — still raise.

Each scenario is a distinct model over the SAME ``orders`` table so its
``filters`` list can differ. End-to-end through ``engine.execute`` with
``dry_run`` to inspect the emitted SQL.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)")
    cur.executemany(
        "INSERT INTO customers VALUES (?,?)",
        [(1, "NA"), (2, "NA"), (3, "EU")],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, amount REAL)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, 1, "paid", 10.0),
            (2, 1, "paid", 5.0),
            (3, 2, "open", 7.0),
            (4, 3, "open", 3.0),
            (5, 3, "paid", 9.0),
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
            name="customers",
            sql_table="customers",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
            ],
        )
    )

    def _base_cols():
        return [
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ]

    # Same-model derived column referenced by the model filter.
    await storage.save_model(
        SlayerModel(
            name="orders_big",
            sql_table="orders",
            data_source="prod",
            columns=[
                *_base_cols(),
                Column(name="big_order", sql="amount > 5", type=DataType.BOOLEAN),
            ],
            filters=["big_order"],
        )
    )
    # Derived column whose SQL crosses a join.
    await storage.save_model(
        SlayerModel(
            name="orders_eu",
            sql_table="orders",
            data_source="prod",
            columns=[
                *_base_cols(),
                Column(
                    name="eu_flag",
                    sql="customers.region = 'EU'",
                    type=DataType.BOOLEAN,
                ),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
            filters=["eu_flag"],
        )
    )
    # Base-only model filter (regression guard).
    await storage.save_model(
        SlayerModel(
            name="orders_base",
            sql_table="orders",
            data_source="prod",
            columns=_base_cols(),
            filters=["status = 'paid'"],
        )
    )
    # Windowed derived column — still rejected in a model filter.
    await storage.save_model(
        SlayerModel(
            name="orders_win",
            sql_table="orders",
            data_source="prod",
            columns=[
                *_base_cols(),
                Column(
                    name="ranked",
                    sql="rank() over (order by amount)",
                    type=DataType.INT,
                ),
            ],
            filters=["ranked > 1"],
        )
    )
    # Same-model measure reference — still rejected in a model filter.
    await storage.save_model(
        SlayerModel(
            name="orders_meas",
            sql_table="orders",
            data_source="prod",
            columns=_base_cols(),
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            filters=["tot > 100"],
        )
    )
    yield SlayerQueryEngine(storage=storage)


async def test_model_filter_inlines_same_model_derived(engine):
    """A model filter naming a same-model derived column inlines its SQL."""
    q = SlayerQuery(source_model="orders_big", measures=[{"formula": "*:count"}])
    dry = await engine.execute(q, dry_run=True)
    sql = dry.sql
    assert sql is not None
    # Expanded expression appears; the derived column NAME does not leak.
    assert "amount" in sql and "> 5" in sql
    assert "big_order" not in sql, sql
    # Always-applied WHERE keeps only amount > 5 (10, 7, 9).
    resp = await engine.execute(q)
    assert resp.data[0]["orders_big._count"] == 3


async def test_model_filter_derived_crosses_join(engine):
    """A derived column whose SQL crosses a join pulls the LEFT JOIN into the
    FROM and qualifies the predicate to the join alias."""
    q = SlayerQuery(source_model="orders_eu", measures=[{"formula": "*:count"}])
    dry = await engine.execute(q, dry_run=True)
    sql = dry.sql
    assert sql is not None
    assert "LEFT JOIN" in sql.upper(), sql
    assert "customers" in sql and "region" in sql and "'EU'" in sql
    assert "eu_flag" not in sql, sql
    # Only orders whose customer is EU (customer 3 -> orders 4, 5).
    resp = await engine.execute(q)
    assert resp.data[0]["orders_eu._count"] == 2


async def test_base_only_model_filter_unchanged(engine):
    """The base-column model-filter path is unaffected (regression guard)."""
    q = SlayerQuery(source_model="orders_base", measures=[{"formula": "*:count"}])
    dry = await engine.execute(q, dry_run=True)
    sql = dry.sql
    assert sql is not None
    assert "status" in sql and "'paid'" in sql
    resp = await engine.execute(q)
    assert resp.data[0]["orders_base._count"] == 3


async def test_windowed_derived_column_model_filter_raises(engine):
    """A model filter referencing a windowed ``Column.sql`` still raises."""
    q = SlayerQuery(source_model="orders_win", measures=[{"formula": "*:count"}])
    with pytest.raises(ValueError, match="window"):
        await engine.execute(q)


async def test_measure_ref_model_filter_raises(engine):
    """A model filter referencing a same-model ModelMeasure still raises."""
    q = SlayerQuery(source_model="orders_meas", measures=[{"formula": "*:count"}])
    with pytest.raises(ValueError, match="measure"):
        await engine.execute(q)
