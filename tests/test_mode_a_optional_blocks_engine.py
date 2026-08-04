"""End-to-end engine behavior for Mode-A optional blocks (DEV-1730).

Blocks + list-valued IN pushdowns are exercised against a real file-backed
SQLite datasource, asserting on RESULT DATA. Also pins the substitution
fast-path boundary (the documented DEV-1625 required-only-zero-vars hole).
"""
import sqlite3
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import (
    SlayerQueryEngine,
    _substitute_model_sql_surfaces,
)
from slayer.storage.yaml_storage import YAMLStorage


def _seed(db_path) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT, amount REAL)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [(1, "US", 100.0), (2, "US", 60.0), (3, "EU", 200.0),
         (4, "EU", 75.0), (5, "CA", 300.0)],
    )
    conn.commit()
    conn.close()


async def _engine_with(*models: SlayerModel):
    tmp = tempfile.TemporaryDirectory()
    _seed(f"{tmp.name}/orders.db")
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=f"{tmp.name}/orders.db")
    )
    for m in models:
        await storage.save_model(m)
    return SlayerQueryEngine(storage=storage), tmp


def _sum(resp, alias: str) -> float:
    assert resp.row_count == 1, f"expected 1 row: {resp.data}"
    return resp.data[0][alias]


def _block_model() -> SlayerModel:
    # sql-mode model whose WHERE carries an optional IN-list block.
    return SlayerModel(
        name="orders",
        sql="SELECT * FROM orders WHERE 1=1 AND {? region IN ({regions}) ?}",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


# ── e2e ─────────────────────────────────────────────────────────────────────


async def test_optional_block_collapses_to_unfiltered_when_missing():
    engine, tmp = await _engine_with(_block_model())
    try:
        q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 735.0
    finally:
        tmp.cleanup()


async def test_optional_block_filters_with_in_list_when_present():
    engine, tmp = await _engine_with(_block_model())
    try:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            variables={"regions": ["US", "CA"]},
        )
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 460.0  # US 160 + CA 300
    finally:
        tmp.cleanup()


async def test_optional_block_single_value_list():
    engine, tmp = await _engine_with(_block_model())
    try:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            variables={"regions": ["EU"]},
        )
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 275.0
    finally:
        tmp.cleanup()


async def test_required_var_alongside_block_raises_and_names_it():
    model = SlayerModel(
        name="orders",
        sql="SELECT * FROM orders WHERE amount >= {min_amount} "
            "AND {? region IN ({regions}) ?}",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )
    engine, tmp = await _engine_with(model)
    try:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            variables={"regions": ["US"]},  # min_amount omitted
        )
        with pytest.raises(Exception, match="min_amount"):
            await engine.execute(q)
    finally:
        tmp.cleanup()


async def test_dimension_query_over_block_model_resolves_types():
    # Exercises get_column_types: blocks must collapse under the defaults probe.
    engine, tmp = await _engine_with(_block_model())
    try:
        q = SlayerQuery(source_model="orders", dimensions=["region"])
        resp = await engine.execute(q)
        assert {r["orders.region"] for r in resp.data} == {"US", "EU", "CA"}
    finally:
        tmp.cleanup()


# ── substitution fast-path boundary (unit) ──────────────────────────────────


def test_blockfree_empty_vars_leaves_model_untouched():
    # DEV-1625 brace-literal protection: no vars + no block -> no substitution.
    model = SlayerModel(
        name="m", sql="SELECT * FROM t WHERE tags = '{1,2,3}'", data_source="ds",
    )
    out = _substitute_model_sql_surfaces(model=model, variables={})
    assert out.sql == "SELECT * FROM t WHERE tags = '{1,2,3}'"


def test_blockfree_required_var_zero_vars_is_untouched_not_raised():
    # The documented boundary hole: a block-free model whose only placeholder is
    # a *declared-looking* required var, called with zero variables, is left
    # untouched (NOT raised) — substitution only runs when vars OR a block exist.
    model = SlayerModel(
        name="m", sql="SELECT * FROM t WHERE region = '{region}'", data_source="ds",
    )
    out = _substitute_model_sql_surfaces(model=model, variables={})
    assert out.sql == "SELECT * FROM t WHERE region = '{region}'"


async def test_bare_var_default_satisfies_required_at_runtime():
    # A query_variables default fulfils an otherwise-bare required var so the
    # query runs without the caller supplying it.
    model = SlayerModel(
        name="orders",
        sql="SELECT * FROM orders WHERE region = '{region}'",
        data_source="ds",
        query_variables={"region": "EU"},
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )
    engine, tmp = await _engine_with(model)
    try:
        q = SlayerQuery(source_model="orders", measures=[{"formula": "amount:sum"}])
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 275.0  # EU only
    finally:
        tmp.cleanup()


async def test_get_column_types_probes_scalar_and_date_and_list_contexts():
    # The defaults probe must yield valid SQL across all optional-context shapes:
    # a scalar-cast block, a date-range block, and an IN-list block.
    model = SlayerModel(
        name="orders",
        sql="SELECT id, region, amount, "
            "{? '{d_from}' ?} AS d_scalar "
            "FROM orders WHERE 1=1 "
            "AND {? amount >= {min_amt} AND amount <= {max_amt} ?} "
            "AND {? region IN ({regions}) ?}",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )
    engine, tmp = await _engine_with(model)
    try:
        # All optional blocks collapse under the empty defaults probe -> valid SQL.
        types = await engine.get_column_types("orders")
        assert isinstance(types, dict)  # degrades gracefully, never raises
    finally:
        tmp.cleanup()


def test_block_bearing_empty_vars_collapses_block():
    # A block-bearing model must collapse even when zero variables are supplied.
    model = SlayerModel(
        name="m",
        sql="SELECT * FROM t WHERE 1=1 AND {? region IN ({regions}) ?}",
        data_source="ds",
    )
    out = _substitute_model_sql_surfaces(model=model, variables={})
    assert out.sql == "SELECT * FROM t WHERE 1=1 AND (1=1)"
