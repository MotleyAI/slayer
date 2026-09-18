"""SQLite suppresses declared DATE/TIMESTAMP casts (they collapse a text date to its leading year); every other dialect renders its cast unchanged."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column
from slayer.core.query import SlayerQuery
from tests._dev1471_fixtures import (
    BACKENDS,
    date_str,
    make_engine,
    orders_model,
    orders_table_spec,
)
from tests._engine_helpers import _engine_generate

# (id, customer_id, amount, region, created_at, shipped_at)
_ROWS = [
    (1, 100, 10.0, "W", "2025-01-15", None),
    (2, 100, 20.0, "W", "2025-03-20", "2025-03-22"),
    (3, 101, 30.0, "E", "2025-02-10", "2025-02-12"),
    (4, 101, 40.0, "E", "2025-02-25", None),
    (5, 102, 50.0, "N", "2025-05-05", "2025-05-06"),
]

_LAST_TOUCH = Column(name="last_touch", sql="coalesce(shipped_at, created_at)", type=DataType.TIMESTAMP)


def _norm(sql: str) -> str:
    return " ".join(sql.split())


async def _engine(backend: str, tmp: str, *, extra_columns=()):
    return await make_engine(
        backend,
        base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"),
        tables=[orders_table_spec(_ROWS)],
        models=[orders_model(extra_columns=extra_columns)],
    )


@pytest.mark.parametrize("backend", BACKENDS)
async def test_temporal_max_returns_full_date(backend: str) -> None:
    """created_at:max per customer is the full latest date on SQLite and DuckDB."""
    with tempfile.TemporaryDirectory() as tmp:
        eng = await _engine(backend, tmp)
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "created_at:max"}],
        }))
        got = {r["orders.customer_id"]: date_str(r["orders.created_at_max"]) for r in resp.data}
        assert got == {100: "2025-03-20", 101: "2025-02-25", 102: "2025-05-05"}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_partitioned_temporal_aggregate_returns_full_date(backend: str) -> None:
    """created_at:max(partition_by=customer_id) attaches the full per-customer latest date."""
    with tempfile.TemporaryDirectory() as tmp:
        eng = await _engine(backend, tmp)
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "created_at:max(partition_by=customer_id)"}],
        }))
        key = "orders.created_at_max_partition_by_customer_id"
        got = {r["orders.customer_id"]: date_str(r[key]) for r in resp.data}
        assert got == {100: "2025-03-20", 101: "2025-02-25", 102: "2025-05-05"}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_derived_temporal_column_returns_full_date(backend: str) -> None:
    """A non-identifier TIMESTAMP column (coalesce) projected and max-aggregated is a full date."""
    with tempfile.TemporaryDirectory() as tmp:
        eng = await _engine(backend, tmp, extra_columns=[_LAST_TOUCH])
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "last_touch:max"}],
        }))
        got = {r["orders.customer_id"]: date_str(r["orders.last_touch_max"]) for r in resp.data}
        assert got == {100: "2025-03-22", 101: "2025-02-25", 102: "2025-05-06"}


async def test_sqlite_sql_has_no_temporal_cast() -> None:
    """The SQLite aggregate is bare ``MAX(orders.created_at)`` — no ``AS TIMESTAMP`` wrapper."""
    sql = await _engine_generate(
        query=SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "created_at:max"}],
        }),
        model=orders_model(), dialect="sqlite",
    )
    normalized = _norm(sql)
    assert "MAX(orders.created_at)" in normalized, sql
    assert "AS TIMESTAMP" not in normalized, sql
    assert "AS DATE" not in normalized, sql


async def test_sqlite_derived_column_sql_has_no_temporal_cast() -> None:
    """The derived-column seam is suppressed on SQLite too (Codex's third seam)."""
    sql = await _engine_generate(
        query=SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "last_touch:max"}],
        }),
        model=orders_model(extra_columns=[_LAST_TOUCH]), dialect="sqlite",
    )
    assert "AS TIMESTAMP" not in _norm(sql), sql


# The rendered SQL for every non-SQLite dialect is byte-identical (whitespace-
# normalized) to what it rendered before this change — the cast survives there.
_EXPECTED_SQL = {
    "postgres": (
        'SELECT orders.customer_id AS "orders.customer_id", '
        'CAST(MAX(orders.created_at) AS TIMESTAMP) AS "orders.created_at_max" '
        "FROM orders AS orders GROUP BY orders.customer_id"
    ),
    "duckdb": (
        'SELECT orders.customer_id AS "orders.customer_id", '
        'CAST(MAX(orders.created_at) AS TIMESTAMP) AS "orders.created_at_max" '
        "FROM orders AS orders GROUP BY orders.customer_id"
    ),
    "tsql": (
        "SELECT orders.customer_id AS [orders___customer_id], "
        "CAST(MAX(orders.created_at) AS DATETIME2) AS [orders___created_at_max] "
        "FROM orders AS orders GROUP BY orders.customer_id"
    ),
    "bigquery": (
        "SELECT orders.customer_id AS `orders___customer_id`, "
        "CAST(MAX(orders.created_at) AS DATETIME) AS `orders___created_at_max` "
        "FROM orders AS orders GROUP BY orders.customer_id"
    ),
}


@pytest.mark.parametrize("dialect", sorted(_EXPECTED_SQL))
async def test_other_dialects_keep_their_casts(dialect: str) -> None:
    sql = await _engine_generate(
        query=SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "created_at:max"}],
        }),
        model=orders_model(), dialect=dialect,
    )
    assert _norm(sql) == _EXPECTED_SQL[dialect]
