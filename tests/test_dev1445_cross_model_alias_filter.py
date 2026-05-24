"""DEV-1450 stage 7b.15e — DEV-1445 acceptance via ``engine.execute``.

DEV-1445: a renamed cross-model measure
(``{"formula": "customers.revenue:sum", "name": "rev"}``) can be filtered by
EITHER the user alias (``rev``) OR the dotted colon form
(``customers.revenue:sum``). Both bind to ONE structural slot (P2/P4), so the
two filter forms produce bit-identical results.

The planner-level shape is already pinned in
``tests/test_generator2_cross_model.py`` (one ``_cm_`` CTE, one HAVING). This
file asserts the contract end-to-end through the full cutover path
(parse → bind → plan → render → execute) against a seeded SQLite, so the
result-key contract and the actual filtered rows are exercised, not just the
emitted SQL.
"""

from __future__ import annotations

import os
import re
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType
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


def _rowset(rows) -> set:
    return {tuple(sorted(r.items())) for r in rows}


_CTE_DEF_RE = re.compile(r"(?:WITH |,\s*)([A-Za-z_]\w*) AS \(")


def _cte_names(sql: str) -> list:
    return _CTE_DEF_RE.findall(sql)


async def test_dev1445_alias_filter_matches_dotted_filter(engine):
    """The user alias ``rev`` and the dotted colon form
    ``customers.revenue:sum`` bind to one slot — both filter forms produce
    identical results.

    Cross-model revenue grouped by the joined ``customers.region`` is
    ``NA = 150``, ``EU = 70``. A ``>= 80`` filter drops ``EU`` from the
    isolated ``_cm_`` CTE; the outer LEFT JOIN preserves the ``EU``
    dimension row with a NULL aggregate (HAVING-on-CTE semantics).
    """
    eng, _ = engine

    alias = await eng.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
            filters=["rev >= 80"],
        )
    )
    dotted = await eng.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
            filters=["customers.revenue:sum >= 80"],
        )
    )

    # Result-key contract: renamed cross-model measure surfaces as the
    # user name under the host prefix.
    assert alias.columns == ["orders.customers.region", "orders.rev"]
    assert dotted.columns == alias.columns
    # Both filter forms reference the SAME slot → identical results.
    assert _rowset(alias.data) == _rowset(dotted.data)
    # The surviving rows: NA keeps 150, EU's aggregate is filtered to NULL.
    assert _rowset(alias.data) == _rowset(
        [
            {"orders.customers.region": "NA", "orders.rev": 150.0},
            {"orders.customers.region": "EU", "orders.rev": None},
        ]
    )


async def test_dev1445_filter_actually_applies(engine):
    """Sanity that the HAVING is real: the filtered result differs from the
    unfiltered one (otherwise both-forms-equal would pass vacuously).
    """
    eng, _ = engine

    unfiltered = await eng.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
        )
    )
    filtered = await eng.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
            filters=["rev >= 80"],
        )
    )

    assert _rowset(unfiltered.data) == _rowset(
        [
            {"orders.customers.region": "NA", "orders.rev": 150.0},
            {"orders.customers.region": "EU", "orders.rev": 70.0},
        ]
    )
    assert _rowset(filtered.data) != _rowset(unfiltered.data)


async def test_dev1445_both_filter_forms_share_one_cte(engine):
    """Both filter forms in the SAME query intern to ONE slot — the rendered
    SQL has exactly one cross-model (``_cm_``) CTE with one HAVING predicate.

    This is the structural counterpart to the result-equality test: a
    regression that minted a duplicate cross-model slot for the dotted vs.
    alias spelling would still return the same rows (both slots compute the
    same value) but would emit two ``_cm_`` CTEs / two HAVING clauses.
    """
    eng, _ = engine
    query = SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
        filters=["rev >= 80", "customers.revenue:sum >= 80"],
    )
    # Idempotent: giving both filter forms is the same as giving one.
    both = await eng.execute(query)
    assert _rowset(both.data) == _rowset(
        [
            {"orders.customers.region": "NA", "orders.rev": 150.0},
            {"orders.customers.region": "EU", "orders.rev": None},
        ]
    )
    # Structural: one shared cross-model CTE, one HAVING.
    dry = await eng.execute(query, dry_run=True)
    sql = dry.sql
    assert sql is not None
    cm_ctes = [c for c in _cte_names(sql) if c.startswith("_cm_")]
    assert len(cm_ctes) == 1, f"expected one _cm_ CTE; got {_cte_names(sql)}"
    assert sql.upper().count("HAVING") == 1, sql
    assert sql.count(">= 80") == 1, sql
