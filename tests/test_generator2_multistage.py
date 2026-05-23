"""DEV-1450 stage 7b.15c — multi-stage DAG rendering for the typed pipeline.

``plan_stages`` returns a ``List[PlannedQuery]`` (root last). The legacy
engine renders a multi-stage DAG as nested rename-subqueries via
``_query_as_model``; the typed pipeline chains the stages as CTEs through
``generate_planned_stages``. SQL strings therefore differ from legacy, so
these tests are execution-based: render the new SQL, run it against the same
seeded SQLite the legacy ``engine.execute([...])`` ran against, and assert
identical result-key columns and row sets — plus structural shape (CTE per
non-root stage, flat downstream binding, the DEV-1448/1449 contracts).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import IllegalScopeReferenceError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import build_resolved_source_bundle
from slayer.engine.stage_planner import plan_stages
from slayer.sql.generator import generate_planned_stages
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Executable harness: a file-backed SQLite seeded with orders + customers,
# a YAMLStorage with the matching models, and an engine over both.
# ---------------------------------------------------------------------------


@pytest.fixture
async def harness() -> AsyncIterator[Tuple[SlayerQueryEngine, YAMLStorage, str]]:
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
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                )
            ],
        )
    )
    engine = SlayerQueryEngine(storage=storage)
    yield engine, storage, db_path


def _run_sqlite(db_path: str, sql: str) -> List[dict]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = [dict(r) for r in con.execute(sql).fetchall()]
    finally:
        con.close()
    return rows


def _rowset(rows: List[dict]) -> set:
    return {tuple(sorted(r.items())) for r in rows}


async def _new_sql(
    *,
    storage: YAMLStorage,
    stages: List[SlayerQuery],
    dialect: str = "sqlite",
) -> str:
    root = stages[-1]
    named = {q.name: q for q in stages[:-1] if q.name}
    bundle = await build_resolved_source_bundle(
        query=root, storage=storage, named_queries=named
    )
    planned = plan_stages(queries=stages, bundle=bundle)
    return generate_planned_stages(planned, bundle=bundle, dialect=dialect)


# ---------------------------------------------------------------------------
# Parity (execution) — new CTE chain == legacy nested-subquery output.
# ---------------------------------------------------------------------------


async def test_two_stage_local_aggregate_matches_legacy(harness):
    engine, storage, db_path = harness
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
    )
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["status"],
        measures=[{"formula": "amount_sum:max"}],
    )
    legacy = await engine.execute([stage1, root])
    new_sql = await _new_sql(storage=storage, stages=[stage1, root])
    new_rows = _run_sqlite(db_path, new_sql)

    # Same result-key columns and same row set as legacy.
    assert set(new_rows[0].keys()) == set(legacy.columns), new_sql
    assert _rowset(new_rows) == _rowset(legacy.data), new_sql
    # The non-root stage rendered as a CTE.
    assert "WITH" in new_sql.upper() and "STAGE1" in new_sql.upper()


async def test_three_stage_chain_matches_legacy(harness):
    engine, storage, db_path = harness
    s1 = SlayerQuery(
        name="s1",
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
    )
    s2 = SlayerQuery(
        name="s2",
        source_model="s1",
        dimensions=["status"],
        measures=[{"formula": "amount_sum:max"}],
    )
    root = SlayerQuery(
        source_model="s2",
        dimensions=["status"],
        measures=[{"formula": "amount_sum_max:min"}],
    )
    legacy = await engine.execute([s1, s2, root])
    new_sql = await _new_sql(storage=storage, stages=[s1, s2, root])
    new_rows = _run_sqlite(db_path, new_sql)
    assert set(new_rows[0].keys()) == set(legacy.columns), new_sql
    assert _rowset(new_rows) == _rowset(legacy.data), new_sql


async def test_cross_model_intermediate_stage_matches_legacy(harness):
    """An intermediate stage with a cross-model aggregate emits its own
    ``_cm_`` CTE — the chain must nest it inside the stage CTE and still
    execute / match legacy rows.
    """
    engine, storage, db_path = harness
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "customers.revenue:sum"}],
    )
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["status"],
        measures=[{"formula": "customers__revenue_sum:max"}],
    )
    legacy = await engine.execute([stage1, root])
    new_sql = await _new_sql(storage=storage, stages=[stage1, root])
    new_rows = _run_sqlite(db_path, new_sql)
    assert set(new_rows[0].keys()) == set(legacy.columns), new_sql
    assert _rowset(new_rows) == _rowset(legacy.data), new_sql


async def test_mixed_local_and_cross_model_intermediate_matches_legacy(harness):
    """Intermediate stage mixes a LOCAL aggregate with a cross-model one.

    The cross-model renderer emits base columns before cross-model columns,
    which diverges from ``public_projection`` order — a positional CTE
    column list would silently swap the downstream values. The by-name
    rename wrapper must map each column correctly; row-set parity with
    legacy is the oracle.
    """
    engine, storage, db_path = harness
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["status"],
        measures=[
            {"formula": "amount:sum"},
            {"formula": "customers.revenue:sum"},
        ],
    )
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["status"],
        measures=[
            {"formula": "amount_sum:max"},
            {"formula": "customers__revenue_sum:max"},
        ],
    )
    legacy = await engine.execute([stage1, root])
    new_sql = await _new_sql(storage=storage, stages=[stage1, root])
    new_rows = _run_sqlite(db_path, new_sql)
    assert set(new_rows[0].keys()) == set(legacy.columns), new_sql
    assert _rowset(new_rows) == _rowset(legacy.data), new_sql


# ---------------------------------------------------------------------------
# DEV-1448 — user ``name`` on a join-traversed measure governs the stage
# column alias and downstream references resolve it.
# ---------------------------------------------------------------------------


async def test_dev1448_named_join_measure_alias(harness):
    engine, storage, db_path = harness
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
    bundle = await build_resolved_source_bundle(
        query=root, storage=storage, named_queries={"stage1": stage1}
    )
    planned = plan_stages(queries=[stage1, root], bundle=bundle)
    # Stage-1 schema column carries the user name, not the canonical alias.
    s1_cols = [c.name for c in planned[0].stage_schema.columns]
    assert "rev" in s1_cols
    assert "customers__revenue_sum" not in s1_cols
    # End-to-end: renders, executes, downstream ``rev:max`` resolves.
    new_sql = generate_planned_stages(planned, bundle=bundle, dialect="sqlite")
    new_rows = _run_sqlite(db_path, new_sql)
    assert any("rev_max" in k for r in new_rows for k in r.keys()), new_sql


# ---------------------------------------------------------------------------
# DEV-1449 — downstream stage sees the upstream as a FLAT schema.
# ---------------------------------------------------------------------------


async def test_dev1449_flat_name_resolves(harness):
    engine, storage, db_path = harness
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "amount:sum"}],
    )
    # Downstream references the multi-hop dimension by its FLAT name.
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["customers__region"],
        measures=[{"formula": "amount_sum:sum"}],
    )
    bundle = await build_resolved_source_bundle(
        query=root, storage=storage, named_queries={"stage1": stage1}
    )
    planned = plan_stages(queries=[stage1, root], bundle=bundle)
    new_sql = generate_planned_stages(planned, bundle=bundle, dialect="sqlite")
    new_rows = _run_sqlite(db_path, new_sql)
    assert len(new_rows) > 0, new_sql


async def test_dev1449_dotted_form_raises(harness):
    engine, storage, db_path = harness
    stage1 = SlayerQuery(
        name="stage1",
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "amount:sum"}],
    )
    # Dotted form against a flat upstream schema is illegal.
    root = SlayerQuery(
        source_model="stage1",
        dimensions=["customers.region"],
        measures=[{"formula": "amount_sum:sum"}],
    )
    bundle = await build_resolved_source_bundle(
        query=root, storage=storage, named_queries={"stage1": stage1}
    )
    with pytest.raises(IllegalScopeReferenceError):
        plan_stages(queries=[stage1, root], bundle=bundle)
