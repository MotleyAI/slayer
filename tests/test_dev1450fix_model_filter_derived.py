"""Dataset filters referencing derived fields.

A ``SlayerDataset.filters`` entry (Mode-A SQL, always-applied WHERE) that
references a non-trivial DERIVED field (``DatasetField.sql`` set) now inlines
the field's expanded SQL instead of raising ``NotImplementedError``.
When the derived field's SQL crosses a relationship, the ``LEFT JOIN`` is pulled
into the FROM and the predicate is qualified to the join alias.

The base-only dataset-filter path (no derived fields) is unchanged, and
the two genuine rejects — a windowed ``DatasetField.sql`` and a same-dataset
``DatasetMetric`` reference — still raise.

Each scenario is a distinct dataset over the SAME ``orders`` table so its
``filters`` list can differ. End-to-end through ``engine.execute`` with
``dry_run`` to inspect the emitted SQL.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    with transaction(db_path) as con:
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

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_dataset(
        SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region", datatype=DataType.STRING),
            ],
        )
    )

    def _base_cols():
        return [
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ]

    # Same-dataset derived field referenced by the dataset filter.
    await storage.save_dataset(
        SlayerDataset(
            name="orders_big",
            sql_table="orders",
            data_source="prod",
            fields=[
                *_base_cols(),
                DatasetField(name="big_order", sql="amount > 5", datatype=DataType.BOOLEAN),
            ],
            filters=["orders_big.big_order"],
        )
    )
    # Derived field whose SQL crosses a relationship.
    await storage.save_dataset(
        SlayerDataset(
            name="orders_eu",
            sql_table="orders",
            data_source="prod",
            fields=[
                *_base_cols(),
                DatasetField(
                    name="eu_flag",
                    sql="customers.region = 'EU'",
                    datatype=DataType.BOOLEAN,
                ),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            ],
            filters=["orders_eu.eu_flag"],
        )
    )
    # Base-only dataset filter (regression guard).
    await storage.save_dataset(
        SlayerDataset(
            name="orders_base",
            sql_table="orders",
            data_source="prod",
            fields=_base_cols(),
            filters=["status = 'paid'"],
        )
    )
    # Windowed derived field — still rejected in a dataset filter.
    await storage.save_dataset(
        SlayerDataset(
            name="orders_win",
            sql_table="orders",
            data_source="prod",
            fields=[
                *_base_cols(),
                DatasetField(
                    name="ranked",
                    sql="rank() over (order by amount)",
                    datatype=DataType.INTEGER,
                ),
            ],
            filters=["orders_win.ranked > 1"],
        ),
        _validate=False,
    )
    # Same-dataset metric reference — still rejected in a dataset filter.
    await storage.save_dataset(
        SlayerDataset(
            name="orders_meas",
            sql_table="orders",
            data_source="prod",
            fields=_base_cols(),
            metrics=[DatasetMetric(expression="sum(amount)", name="tot")],
            filters=["orders_meas.tot > 100"],
        ),
        _validate=False,
    )
    yield SlayerQueryEngine(storage=storage)


async def test_model_filter_inlines_same_model_derived(engine):
    """A dataset filter naming a same-dataset derived field inlines its SQL."""
    q = SlayerQuery.model_validate({"population": "orders_big", "metrics": [{"metric": "count(orders_big.*)"}]})
    dry = await engine.execute(q, dry_run=True)
    sql = dry.sql
    assert sql is not None
    # Expanded expression appears; the derived field NAME does not leak.
    assert "amount" in sql
    assert "> 5" in sql
    assert "big_order" not in sql, sql
    # Always-applied WHERE keeps only amount > 5 (10, 7, 9).
    resp = await engine.execute(q)
    assert resp.data[0]["orders_big._count"] == 3


async def test_model_filter_derived_crosses_join(engine):
    """A derived field whose SQL crosses a relationship pulls the LEFT JOIN into the
    FROM and qualifies the predicate to the join alias."""
    q = SlayerQuery.model_validate({"population": "orders_eu", "metrics": [{"metric": "count(orders_eu.*)"}]})
    dry = await engine.execute(q, dry_run=True)
    sql = dry.sql
    assert sql is not None
    assert "LEFT JOIN" in sql.upper(), sql
    assert "customers" in sql
    assert "region" in sql
    assert "'EU'" in sql
    assert "eu_flag" not in sql, sql
    # Only orders whose customer is EU (customer 3 -> orders 4, 5).
    resp = await engine.execute(q)
    assert resp.data[0]["orders_eu._count"] == 2


async def test_base_only_model_filter_unchanged(engine):
    """The base-field dataset-filter path is unaffected (regression guard)."""
    q = SlayerQuery.model_validate({"population": "orders_base", "metrics": [{"metric": "count(orders_base.*)"}]})
    dry = await engine.execute(q, dry_run=True)
    sql = dry.sql
    assert sql is not None
    assert "status" in sql
    assert "'paid'" in sql
    resp = await engine.execute(q)
    assert resp.data[0]["orders_base._count"] == 3


async def test_windowed_derived_column_model_filter_raises(engine):
    """A dataset filter referencing a windowed ``DatasetField.sql`` still raises."""
    q = SlayerQuery.model_validate({"population": "orders_win", "metrics": [{"metric": "count(orders_win.*)"}]})
    with pytest.raises(ValueError, match="window"):
        await engine.execute(q)


async def test_measure_ref_model_filter_raises(engine):
    """A dataset filter referencing a same-dataset DatasetMetric still raises."""
    q = SlayerQuery.model_validate({"population": "orders_meas", "metrics": [{"metric": "count(orders_meas.*)"}]})
    with pytest.raises(ValueError, match="metric"):
        await engine.execute(q)
