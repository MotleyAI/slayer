"""DEV-1450 review fixes (group 1) — cross-model derived-column completeness.

Extends the #4a/#4b ``ColumnSqlKey`` support to the cross-model CTE path:

* Codex C1 — a cross-model aggregate target whose ``SlayerModel.filters``
  references a derived column expands it (not a bogus ``target.derived``).
* Codex C2 — a query filter on a joined derived column routed into the
  target CTE renders the expanded predicate.
* CR[7] — ``agg_kwarg_canonical_str`` canonicalizes a ``ColumnSqlKey``
  arg/kwarg instead of raising ``TypeError``.
* CR[9] — ``_local_agg_formula`` re-roots column-valued kwargs (strips the
  agg-source/target prefix) instead of dropping the path.
* CR[11] — aggregate metadata for a derived source resolves via ``src.model``.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.refs import agg_kwarg_canonical_str
from slayer.engine.cross_model_planner import _local_agg_formula
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Unit: CR[7] agg_kwarg_canonical_str handles ColumnSqlKey
# ---------------------------------------------------------------------------


def test_agg_kwarg_canonical_str_columnsqlkey_local():
    v = ColumnSqlKey(path=(), model="customers", column_name="net")
    assert agg_kwarg_canonical_str(v) == "net"


def test_agg_kwarg_canonical_str_columnsqlkey_joined():
    v = ColumnSqlKey(path=("customers",), model="customers", column_name="net")
    assert agg_kwarg_canonical_str(v) == "customers.net"


# ---------------------------------------------------------------------------
# Unit: CR[9] _local_agg_formula re-roots column kwargs (strips target prefix)
# ---------------------------------------------------------------------------


def test_local_agg_formula_reroots_column_kwarg_on_target():
    # corr(other=customers.region_id) with the agg rooted at ("customers",):
    # the kwarg is on the target, so it becomes target-local ``region_id``.
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="revenue"),
        agg="corr",
        kwargs=(("other", ColumnKey(path=("customers",), leaf="region_id")),),
    )
    assert _local_agg_formula(key) == "revenue:corr(other=region_id)"


def test_local_agg_formula_keeps_residual_path_for_deeper_kwarg():
    # A kwarg one hop past the target keeps its residual path.
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="revenue"),
        agg="corr",
        kwargs=(("other", ColumnKey(path=("customers", "regions"), leaf="code")),),
    )
    assert _local_agg_formula(key) == "revenue:corr(other=regions.code)"


# ---------------------------------------------------------------------------
# Engine fixture: orders -> customers, customers has derived columns
# ---------------------------------------------------------------------------


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, "
        "status TEXT, revenue REAL)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, "NA", "active", 100.0),
            (2, "NA", "inactive", 50.0),
            (3, "EU", "active", 70.0),
        ],
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
    # Customers with a derived model filter (is_active) AND a derived
    # column used in a query filter (big_spender).
    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
                Column(name="status", type=DataType.TEXT),
                Column(name="revenue", type=DataType.DOUBLE, label="Revenue"),
                Column(name="is_active", sql="status = 'active'", type=DataType.BOOLEAN),
                Column(name="big_spender", sql="revenue > 60", type=DataType.BOOLEAN),
            ],
            filters=["is_active"],
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


async def test_c1_target_derived_model_filter_expands(engine):
    """The cross-model CTE applies the target's derived model filter
    (is_active = status='active') — inactive customers are excluded."""
    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum"}],
        )
    )
    dry = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum"}],
        ),
        dry_run=True,
    )
    assert "is_active" not in dry.sql, dry.sql
    assert "status" in dry.sql and "'active'" in dry.sql
    by_region = {
        r["orders.customers.region"]: r["orders.customers.revenue_sum"]
        for r in resp.data
    }
    # NA: only active customer 1 (100); customer 2 (inactive, 50) excluded.
    # EU: active customer 3 (70).
    assert by_region == pytest.approx({"NA": 100.0, "EU": 70.0})


async def test_c2_routed_query_filter_on_joined_derived_column(engine):
    """A query filter on a joined derived column (customers.big_spender)
    routes into the target CTE and renders the expanded predicate."""
    q = SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.revenue:sum"}],
        filters=["customers.big_spender == 1"],
    )
    dry = await engine.execute(q, dry_run=True)
    assert "big_spender" not in dry.sql, dry.sql
    assert "revenue > 60" in dry.sql.replace('"', "")
    resp = await engine.execute(q)
    by_region = {
        r["orders.customers.region"]: r["orders.customers.revenue_sum"]
        for r in resp.data
    }
    # big_spender = revenue > 60 AND is_active (active): customer 1 (100, NA),
    # customer 3 (70, EU). Customer 2 (50) fails both.
    assert by_region == pytest.approx({"NA": 100.0, "EU": 70.0})


async def test_cr11_cross_model_agg_over_derived_column_metadata(engine):
    """An aggregate over a derived column resolves label/format via
    src.model (not a path walk that could miss it)."""
    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "customers.revenue:sum"}],
        )
    )
    # revenue carries label "Revenue"; the cross-model measure inherits it.
    meta = resp.attributes.measures.get("orders.customers.revenue_sum")
    assert meta is not None
    assert meta.label == "Revenue"
