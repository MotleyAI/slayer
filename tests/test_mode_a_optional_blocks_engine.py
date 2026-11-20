"""End-to-end engine behavior for Mode-A optional blocks.

Blocks + list-valued IN pushdowns are exercised against a real file-backed
SQLite datasource, asserting on RESULT DATA. Also pins the substitution
fast-path boundary (the documented required-only-zero-vars hole).
"""
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import (
    SlayerQueryEngine,
    _substitute_dataset_sql_surfaces,
)
from slayer.sql.dialects import SqliteDialect
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage


def _seed(db_path) -> None:
    with transaction(str(db_path)) as conn:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT, amount REAL)"
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [(1, "US", 100.0), (2, "US", 60.0), (3, "EU", 200.0),
             (4, "EU", 75.0), (5, "CA", 300.0)],
        )


async def _engine_with(*datasets: SlayerDataset):
    tmp = tempfile.TemporaryDirectory()
    _seed(f"{tmp.name}/orders.db")
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=f"{tmp.name}/orders.db")
    )
    for m in datasets:
        await storage.save_dataset(m)
    return SlayerQueryEngine(storage=storage), tmp


def _sum(resp, alias: str) -> float:
    assert resp.row_count == 1, f"expected 1 row: {resp.data}"
    return resp.data[0][alias]


def _block_dataset() -> SlayerDataset:
    # sql-mode dataset whose WHERE carries an optional IN-list block.
    return SlayerDataset(
        name="orders",
        sql="SELECT * FROM orders WHERE 1=1 AND {? region IN ({regions}) ?}",
        data_source="ds",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region", expression="region", datatype=DataType.STRING),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
        ],
    )


# ── e2e ─────────────────────────────────────────────────────────────────────


async def test_optional_block_collapses_to_unfiltered_when_missing():
    engine, tmp = await _engine_with(_block_dataset())
    try:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 735.0
    finally:
        tmp.cleanup()


async def test_optional_block_filters_with_in_list_when_present():
    engine, tmp = await _engine_with(_block_dataset())
    try:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)"}],
            "variables": {"regions": ["US", "CA"]},
        })
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 460.0  # US 160 + CA 300
    finally:
        tmp.cleanup()


async def test_optional_block_single_value_list():
    engine, tmp = await _engine_with(_block_dataset())
    try:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)"}],
            "variables": {"regions": ["EU"]},
        })
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 275.0
    finally:
        tmp.cleanup()


async def test_required_var_alongside_block_raises_and_names_it():
    model = SlayerDataset(
        name="orders",
        sql="SELECT * FROM orders WHERE amount >= {min_amount} "
            "AND {? region IN ({regions}) ?}",
        data_source="ds",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region", expression="region", datatype=DataType.STRING),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
        ],
    )
    engine, tmp = await _engine_with(model)
    try:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(orders.amount)"}],
            "variables": {"regions": ["US"]},
        })
        with pytest.raises(Exception, match="min_amount"):
            await engine.execute(q)
    finally:
        tmp.cleanup()


async def test_dimension_query_over_block_model_resolves_types():
    # Exercises get_result_column_types: blocks must collapse under the defaults probe.
    engine, tmp = await _engine_with(_block_dataset())
    try:
        q = SlayerQuery(population="orders", dimensions=["orders.region"])
        resp = await engine.execute(q)
        assert {r["orders.region"] for r in resp.data} == {"US", "EU", "CA"}
    finally:
        tmp.cleanup()


# ── substitution fast-path boundary (unit) ──────────────────────────────────


def test_blockfree_empty_vars_leaves_model_untouched():
    # Brace-literal protection: no vars + no block -> no substitution.
    model = SlayerDataset(
        name="m", sql="SELECT * FROM t WHERE tags = '{1,2,3}'", data_source="ds",
    )
    out = _substitute_dataset_sql_surfaces(
        dataset=model, variables={}, dialect=SqliteDialect())
    assert out.sql == "SELECT * FROM t WHERE tags = '{1,2,3}'"


def test_blockfree_required_var_zero_vars_is_untouched_not_raised():
    # The documented boundary hole: a block-free dataset whose only placeholder is
    # a *declared-looking* required var, called with zero variables, is left
    # untouched (NOT raised) — substitution only runs when vars OR a block exist.
    model = SlayerDataset(
        name="m", sql="SELECT * FROM t WHERE region = '{region}'", data_source="ds",
    )
    out = _substitute_dataset_sql_surfaces(
        dataset=model, variables={}, dialect=SqliteDialect())
    assert out.sql == "SELECT * FROM t WHERE region = '{region}'"


async def test_bare_var_default_satisfies_required_at_runtime():
    # A query_variables default fulfils an otherwise-bare required var so the
    # query runs without the caller supplying it.
    model = SlayerDataset(
        name="orders",
        sql="SELECT * FROM orders WHERE region = '{region}'",
        data_source="ds",
        query_variables={"region": "EU"},
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region", expression="region", datatype=DataType.STRING),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
        ],
    )
    engine, tmp = await _engine_with(model)
    try:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)"}]})
        resp = await engine.execute(q)
        assert _sum(resp, "orders.amount_sum") == 275.0  # EU only
    finally:
        tmp.cleanup()


async def test_get_result_column_types_probes_scalar_and_date_and_list_contexts():
    # The defaults probe must yield valid SQL across all optional-context shapes:
    # a scalar-cast block, a date-range block, and an IN-list block.
    model = SlayerDataset(
        name="orders",
        sql="SELECT id, region, amount, "
            "{? '{d_from}' ?} AS d_scalar "
            "FROM orders WHERE 1=1 "
            "AND {? amount >= {min_amt} AND amount <= {max_amt} ?} "
            "AND {? region IN ({regions}) ?}",
        data_source="ds",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region", expression="region", datatype=DataType.STRING),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
        ],
    )
    engine, tmp = await _engine_with(model)
    try:
        # All optional blocks collapse under the empty defaults probe -> valid SQL.
        types = await engine.get_result_column_types("orders")
        assert isinstance(types, dict)  # degrades gracefully, never raises
    finally:
        tmp.cleanup()


def test_block_bearing_empty_vars_collapses_block():
    # A block-bearing dataset must collapse even when zero variables are supplied.
    model = SlayerDataset(
        name="m",
        sql="SELECT * FROM t WHERE 1=1 AND {? region IN ({regions}) ?}",
        data_source="ds",
    )
    out = _substitute_dataset_sql_surfaces(
        dataset=model, variables={}, dialect=SqliteDialect())
    assert out.sql == "SELECT * FROM t WHERE 1=1 AND (1=1)"
