"""DEV-1450 stage 7b.7 — smoke tests for the parity oracle helpers.

Asserts that the helpers in ``tests/parity_oracle.py`` are wired
correctly. Slice tests in 7b.8–7b.13 consume these helpers; if the
oracle itself is broken, every downstream parity test reports the
wrong root cause.

This file is deleted at the end of 7b.15 alongside the helper module.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests.parity_oracle import (
    assert_sql_equivalent,
    build_storage_with_models,
    legacy_sql_for,
    norm_sql,
)


# ---------------------------------------------------------------------------
# Models — minimal two-model setup for parity-oracle smoke tests.
# ---------------------------------------------------------------------------


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="test",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
        ],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="test",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


# ---------------------------------------------------------------------------
# norm_sql
# ---------------------------------------------------------------------------


def test_norm_sql_collapses_runs():
    assert norm_sql("SELECT   *   FROM   t") == "SELECT * FROM t"


def test_norm_sql_handles_newlines_and_tabs():
    assert norm_sql("SELECT\n  a,\n\tb\nFROM t") == "SELECT a, b FROM t"


def test_norm_sql_strips_leading_trailing():
    assert norm_sql("   SELECT 1   ") == "SELECT 1"


# ---------------------------------------------------------------------------
# assert_sql_equivalent
# ---------------------------------------------------------------------------


def test_assert_sql_equivalent_passes_on_whitespace_only_diff():
    assert_sql_equivalent("SELECT  a\nFROM t", "SELECT a FROM t")


def test_assert_sql_equivalent_raises_on_real_diff():
    with pytest.raises(AssertionError) as exc:
        assert_sql_equivalent("SELECT a FROM t", "SELECT b FROM t")
    msg = str(exc.value)
    assert "SQL parity failed" in msg
    assert "--- legacy ---" in msg
    assert "--- new ---" in msg
    assert "--- token diff ---" in msg


def test_assert_sql_equivalent_passes_when_identical():
    assert_sql_equivalent("SELECT 1", "SELECT 1")


# ---------------------------------------------------------------------------
# legacy_sql_for + build_storage_with_models — full async path
# ---------------------------------------------------------------------------


async def test_legacy_sql_for_returns_non_empty_sql(tmp_path):
    storage = await build_storage_with_models(tmp_path, _customers(), _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
    )
    sql = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    assert sql.strip(), "legacy_sql_for returned empty SQL"
    norm = norm_sql(sql).lower()
    assert "orders" in norm
    assert "sum" in norm


async def test_legacy_sql_for_is_deterministic(tmp_path):
    """Same query twice → same SQL (oracle baseline)."""
    storage = await build_storage_with_models(tmp_path, _customers(), _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum"}],
    )
    sql_a = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    sql_b = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    assert_sql_equivalent(sql_a, sql_b)


async def test_legacy_sql_for_renders_joined_dim(tmp_path):
    """Smoke: cross-model dim path resolves through ``_enrich``."""
    storage = await build_storage_with_models(tmp_path, _customers(), _orders())
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        dimensions=["customers.region_id"],
        measures=[{"formula": "amount:sum"}],
    )
    sql = await legacy_sql_for(engine=engine, model=_orders(), query=query)
    norm = norm_sql(sql).lower()
    assert "customers" in norm
