"""DEV-1450 follow-up #5 — cross-model parametric aggregate result keys.

Cross-model parametric aggregates carry their kwarg-signature suffix in
the result key, so two variants on the same target column do NOT collide
(legacy dropped the suffix and the two columns clobbered each other).

This pins the (already-correct) behavior end-to-end:
* a single ``customers.revenue:percentile(p=0.5)`` surfaces
  ``orders.customers.revenue_percentile_p_0_5``;
* ``p=0.5`` and ``p=0.95`` together surface two DISTINCT keys.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, revenue REAL)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?)",
        [(1, "NA", 100.0), (2, "NA", 50.0), (3, "EU", 70.0)],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?)",
        [(1, 1, 10.0), (2, 1, 5.0), (3, 2, 7.0), (4, 3, 3.0), (5, 3, 9.0)],
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
                Column(name="revenue", type=DataType.DOUBLE),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        )
    )
    yield SlayerQueryEngine(storage=storage)


async def test_single_cross_model_parametric_key(engine):
    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:percentile(p=0.5)"}],
        )
    )
    assert "orders.customers.revenue_percentile_p_0_5" in resp.columns


async def test_two_parametric_variants_distinct_keys(engine):
    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[
                {"formula": "customers.revenue:percentile(p=0.5)"},
                {"formula": "customers.revenue:percentile(p=0.95)"},
            ],
        )
    )
    assert "orders.customers.revenue_percentile_p_0_5" in resp.columns
    assert "orders.customers.revenue_percentile_p_0_95" in resp.columns
    # Distinct keys — no collision.
    assert (
        "orders.customers.revenue_percentile_p_0_5"
        != "orders.customers.revenue_percentile_p_0_95"
    )
    assert len([c for c in resp.columns if "revenue_percentile" in c]) == 2
