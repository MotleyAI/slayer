"""Flat stage-schema references, end to end via ``engine.execute``.

A downstream stage sees its upstream as a FLAT ``StageSchema``, not
as a dataset with relationships (P5/P6). A multi-hop upstream dimension
(``customers.region``) surfaces downstream under its flat name
(``customers__region``); the dotted form names ``customers``, which no relationship path
reaches from the stage, so a row-level read of it raises ``UnrelatedDatasetError``.

End-to-end through ``engine.execute([stage1, root])``. The planner-level
behavior is pinned in ``tests/test_generator2_multistage.py``; this asserts
the rendered/executed result and the rejection of the dotted form.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnrelatedDatasetError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def engine() -> AsyncIterator[Tuple[SlayerQueryEngine, str]]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, revenue REAL)"
        )
        cur.executemany(
            "INSERT INTO customers VALUES (?,?,?)",
            [(1, "NA", 100.0), (2, "NA", 50.0), (3, "EU", 70.0)],
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
                DatasetField(name="revenue", datatype=DataType.FLOAT),
            ],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="prod",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            ],
        )
    )
    yield SlayerQueryEngine(storage=storage), db_path


def _stage1() -> SlayerQuery:
    return SlayerQuery.model_validate({
        "name": "stage1",
        "population": "orders",
        "dimensions": ["customers.region"],
        "metrics": [{"metric": "sum(orders.amount)"}],
    })


async def test_dev1449_flat_name_resolves(engine: Tuple[SlayerQueryEngine, str]) -> None:
    """The multi-hop upstream dimension is referenced downstream by its flat
    name ``customers__region``.
    """
    eng, _ = engine
    root = SlayerQuery.model_validate({
        "population": "stage1",
        "dimensions": ["stage1.customers__region"],
        "metrics": [{"metric": "sum(stage1.amount_sum)"}],
    })
    resp = await eng.execute([_stage1(), root])
    assert resp.columns == ["stage1.customers__region", "stage1.amount_sum_sum"]
    assert {
        tuple(sorted(r.items())) for r in resp.data
    } == {
        tuple(
            sorted(
                {"stage1.customers__region": "NA", "stage1.amount_sum_sum": 22.0}.items()
            )
        ),
        tuple(
            sorted(
                {"stage1.customers__region": "EU", "stage1.amount_sum_sum": 12.0}.items()
            )
        ),
    }


async def test_dev1449_dotted_form_raises(engine: Tuple[SlayerQueryEngine, str]) -> None:
    """The dotted form reads a dataset no relationship path reaches from the flat upstream stage."""
    eng, _ = engine
    root = SlayerQuery.model_validate({
        "population": "stage1",
        "dimensions": ["customers.region"],
        "metrics": [{"metric": "sum(stage1.amount_sum)"}],
    })
    stage1 = _stage1()
    with pytest.raises(UnrelatedDatasetError):
        await eng.execute([stage1, root])
