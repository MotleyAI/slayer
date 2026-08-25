"""DEV-1450 stage 7b.15e — DEV-1448 acceptance via ``engine.execute``.

DEV-1448: a user ``name`` on a join-traversed measure governs the stage's
emitted column alias, so a downstream stage references it by that name. With
stage-1 ``{"formula": "customers.revenue:sum", "name": "rev"}``, the stage
schema exposes a flat column ``rev`` (NOT the canonical
``customers__revenue_sum``), and a root stage over it resolves ``rev:max``.

End-to-end through ``engine.execute([stage1, root])``. The planner-level
schema-column shape is pinned in ``tests/test_generator2_multistage.py``; this
asserts the result through the full multi-stage DAG render+execute, and that
the canonical (un-renamed) form is no longer a legal downstream reference.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnknownReferenceError
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


async def test_dev1448_downstream_resolves_user_name(engine):
    """The renamed join measure is referenced downstream by its user name."""
    eng, _ = engine
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
    )
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["status"],
        measures=[{"formula": "rev:max"}],
    )
    resp = await eng.execute([stage1, root])
    # Result-key contract: downstream stage prefix + canonical alias of the
    # downstream measure (``rev:max`` -> ``rev_max``).
    assert resp.columns == ["stage1.status", "stage1.rev_max"]
    assert {
        tuple(sorted(r.items())) for r in resp.data
    } == {
        tuple(sorted({"stage1.status": "open", "stage1.rev_max": 220.0}.items())),
        tuple(sorted({"stage1.status": "paid", "stage1.rev_max": 220.0}.items())),
    }


async def test_dev1448_canonical_form_not_a_downstream_name(engine):
    """Because the user ``name`` GOVERNS the stage alias, the canonical
    ``customers__revenue_sum`` is no longer exposed downstream — referencing
    it raises rather than silently resolving.
    """
    eng, _ = engine
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
    )
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["status"],
        measures=[{"formula": "customers__revenue_sum:max"}],
    )
    with pytest.raises(UnknownReferenceError):
        await eng.execute([stage1, root])
