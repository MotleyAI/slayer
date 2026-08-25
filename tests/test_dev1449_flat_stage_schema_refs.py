"""DEV-1450 stage 7b.15e — DEV-1449 acceptance via ``engine.execute``.

DEV-1449: a downstream stage sees its upstream as a FLAT ``StageSchema``, not
as a model with joins (P5/P6). A multi-hop upstream dimension
(``customers.region``) surfaces downstream under its flat name
(``customers__region``); the dotted join form is illegal in stage scope and
raises ``IllegalScopeReferenceError``.

End-to-end through ``engine.execute([stage1, root])``. The planner-level
behavior is pinned in ``tests/test_generator2_multistage.py``; this asserts
the rendered/executed result and the rejection of the dotted form.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import IllegalScopeReferenceError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def engine() -> AsyncIterator[Tuple[SlayerQueryEngine, str]]:
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
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        )
    )
    yield SlayerQueryEngine(storage=storage), db_path


def _stage1() -> SlayerQuery:
    return SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "amount:sum"}],
    )


async def test_dev1449_flat_name_resolves(engine):
    """The multi-hop upstream dimension is referenced downstream by its flat
    name ``customers__region``.
    """
    eng, _ = engine
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["customers__region"],
        measures=[{"formula": "amount_sum:sum"}],
    )
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


async def test_dev1449_dotted_form_raises(engine):
    """The dotted join form is illegal against a flat upstream stage schema."""
    eng, _ = engine
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["customers.region"],
        measures=[{"formula": "amount_sum:sum"}],
    )
    with pytest.raises(IllegalScopeReferenceError):
        await eng.execute([_stage1(), root])
