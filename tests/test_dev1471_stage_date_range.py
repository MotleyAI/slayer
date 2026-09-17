"""A two-bound date_range on a stage time dimension filters the outer stage's rows before its aggregation, exactly as a model-scope range filters a model."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from tests._dev1471_fixtures import (
    BACKENDS,
    date_str,
    make_engine,
    orders_model,
    orders_table_spec,
)
from tests._engine_helpers import _where_text

TG = TimeGranularity

# Four monthly orders across 2025 (id, customer_id, amount, region, created_at, shipped_at).
_ROWS = [
    (1, 100, 100.0, "W", "2025-01-10", None),
    (2, 101, 200.0, "E", "2025-02-05", None),
    (3, 102, 300.0, "N", "2025-03-15", None),
    (4, 103, 400.0, "S", "2025-04-20", None),
]

_INNER = SlayerQuery(
    name="s1", source_model="orders",
    time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
    measures=[{"formula": "amount:sum", "name": "rev"}],
)


async def _exec(backend: str, tmp: str, outer: SlayerQuery):
    engine = await make_engine(
        backend, base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"),
        tables=[orders_table_spec(_ROWS)], models=[orders_model()],
    )
    return await engine.execute(query=[_INNER, outer])


@pytest.mark.parametrize("backend", BACKENDS)
async def test_stage_date_range_restricts_outer_stage(backend: str) -> None:
    outer = SlayerQuery(
        source_model="s1",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TG.MONTH,
            date_range=["2025-02-01", "2025-03-31"],
        )],
        measures=[{"formula": "rev:sum"}],
    )
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec(backend, tmp, outer)
    got = {date_str(r["s1.created_at"]): r["s1.rev_sum"] for r in resp.data}
    assert got == {"2025-02-01": 200.0, "2025-03-01": 300.0}
    # The range lands in the OUTER stage's WHERE on the stage column, not the inner CTE.
    where = _where_text(resp.sql or "", dialect=backend)
    assert "created_at" in where, resp.sql
    assert "2025-02-01" in where, resp.sql
    assert "2025-03-31" in where, resp.sql


@pytest.mark.parametrize("backend", BACKENDS)
async def test_stage_date_range_on_coarser_rebucketing(backend: str) -> None:
    outer = SlayerQuery(
        source_model="s1",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TG.YEAR,
            date_range=["2025-02-01", "2025-03-31"],
        )],
        measures=[{"formula": "rev:sum"}],
    )
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec(backend, tmp, outer)
    got = {date_str(r["s1.created_at"]): r["s1.rev_sum"] for r in resp.data}
    # Only Feb + Mar survive the range; the year bucket totals them.
    assert got == {"2025-01-01": 500.0}
