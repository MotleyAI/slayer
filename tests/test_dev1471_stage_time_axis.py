"""A stage's own time dimension is the transform time axis (shift/change/cumsum/last/windowed) and carries the functional surfaces (gran-order key, suffixed keys)."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from tests._dev1471_fixtures import (
    BACKENDS,
    date_str,
    make_engine,
    orders_model,
    orders_table_spec,
    region_chain_models,
    region_chain_tables,
)

TG = TimeGranularity

# One order per month, Jan–Apr 2025 (id, customer_id, amount, region, created_at, shipped_at).
_MONTHLY = [
    (1, 100, 100.0, "W", "2025-01-10", "2025-02-10"),
    (2, 101, 200.0, "E", "2025-02-05", "2025-03-05"),
    (3, 102, 300.0, "N", "2025-03-15", "2025-04-15"),
    (4, 103, 400.0, "S", "2025-04-20", "2025-05-20"),
]

_INNER = SlayerQuery.model_validate({
    "name": "s1", "source_model": "orders",
    "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
    "measures": [{"formula": "amount:sum", "name": "rev"}],
})


async def _exec_outer(backend: str, tmp: str, outer: SlayerQuery, *, tables=None, models=None, inner=None):
    engine = await make_engine(
        backend, base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"),
        tables=tables or [orders_table_spec(_MONTHLY)], models=models or [orders_model()],
    )
    return await engine.execute(query=[inner or _INNER, outer])


def _by_month(data: list[dict], value_key: str) -> dict:
    return {date_str(r["s1.created_at"]): r[value_key] for r in data}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_time_shift_over_stage_time_dimension(backend: str) -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
        "measures": [
            {"formula": "rev:sum"},
            {"formula": "time_shift(rev:sum, -1, 'month')", "name": "prev"},
        ],
    })
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec_outer(backend, tmp, outer)
    assert _by_month(resp.data, "s1.prev") == {
        "2025-01-01": None, "2025-02-01": 100.0, "2025-03-01": 200.0, "2025-04-01": 300.0,
    }


@pytest.mark.parametrize("backend", BACKENDS)
async def test_change_and_cumsum_over_stage_time_dimension(backend: str) -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
        "measures": [
            {"formula": "change(rev:sum)", "name": "chg"},
            {"formula": "cumsum(rev:sum)", "name": "cum"},
        ],
    })
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec_outer(backend, tmp, outer)
    assert _by_month(resp.data, "s1.chg") == {
        "2025-01-01": None, "2025-02-01": 100.0, "2025-03-01": 100.0, "2025-04-01": 100.0,
    }
    assert _by_month(resp.data, "s1.cum") == {
        "2025-01-01": 100.0, "2025-02-01": 300.0, "2025-03-01": 600.0, "2025-04-01": 1000.0,
    }


@pytest.mark.parametrize("backend", BACKENDS)
async def test_last_over_stage_time_dimension(backend: str) -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
        "measures": [{"formula": "last(rev:sum)", "name": "lst"}],
    })
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec_outer(backend, tmp, outer)
    assert set(_by_month(resp.data, "s1.lst").values()) == {400.0}


# A model holding exactly the inner stage's monthly rows — the spec's reference
# for "identical to the same shapes evaluated over a model-backed dataset".
def _monthly_model() -> SlayerModel:
    return SlayerModel(
        name="monthly", sql_table="monthly", data_source="ds",
        default_time_dimension="month_ts",
        columns=[
            Column(name="month_ts", sql="month_ts", type=DataType.TIMESTAMP),
            Column(name="rev", sql="rev", type=DataType.DOUBLE),
        ],
    )


_MONTHLY_MODEL_TABLE = {
    "name": "monthly",
    "columns": [("month_ts", "TIMESTAMP"), ("rev", "DOUBLE")],
    "rows": [("2025-01-01", 100.0), ("2025-02-01", 200.0), ("2025-03-01", 300.0), ("2025-04-01", 400.0)],
}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_windowed_aggregate_over_stage_time_dimension(backend: str) -> None:
    """rev:sum(window='60d') on the stage bucket equals the model-backed evaluation
    over a dataset holding the same monthly rows (transforms spec equivalence)."""
    windowed = [{"formula": "rev:sum(window='60d')", "name": "win"}]
    stage_outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
        "measures": windowed,
    })
    model_query = SlayerQuery.model_validate({
        "source_model": "monthly",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="month_ts"), granularity=TG.MONTH)],
        "measures": windowed,
    })
    with tempfile.TemporaryDirectory() as tmp:
        stage_resp = await _exec_outer(backend, tmp, stage_outer)
        model_engine = await make_engine(
            backend, base_dir=os.path.join(tmp, "model_store"),
            db_path=os.path.join(tmp, f"m.{backend}"),
            tables=[_MONTHLY_MODEL_TABLE], models=[_monthly_model()],
        )
        model_resp = await model_engine.execute(model_query)
    stage = _by_month(stage_resp.data, "s1.win")
    model = {date_str(r["monthly.month_ts"]): r["monthly.win"] for r in model_resp.data}
    assert stage == model
    assert model  # guard against an empty-vs-empty false pass


@pytest.mark.parametrize("backend", BACKENDS)
async def test_time_shift_over_multi_hop_flat_time_dimension(backend: str) -> None:
    tables = region_chain_tables(
        regions=[(10, "A", "2025-01-10"), (11, "B", "2025-02-10"), (12, "C", "2025-03-10")],
        customers=[(100, 10), (101, 11), (102, 12)],
        orders=[(1000, 100), (1001, 100), (1002, 101), (1003, 102), (1004, 102), (1005, 102)],
    )
    inner = SlayerQuery.model_validate({
        "name": "s1", "source_model": "orders",
        "time_dimensions": [TimeDimension(
            dimension=ColumnRef(name="customers.regions.last_activity_at"), granularity=TG.MONTH,
        )],
        "measures": [{"formula": "*:count", "name": "n"}],
    })
    col = "customers__regions__last_activity_at"
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name=col), granularity=TG.MONTH)],
        "measures": [
            {"formula": "n:sum"},
            {"formula": "time_shift(n:sum, -1, 'month')", "name": "prev"},
        ],
    })
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec_outer(backend, tmp, outer, tables=tables, models=region_chain_models(), inner=inner)
    prev = {date_str(r[f"s1.{col}"]): r["s1.prev"] for r in resp.data}
    assert prev == {"2025-01-01": None, "2025-02-01": 2, "2025-03-01": 1}


# --- two stage time dimensions need main_time_dimension ---
# NOTE: spec scenario names created_at month+year (one column); same-column
# selection is deferred (DEV-1925), so this uses two distinct columns.
# shipped_at is constant so ``change`` over the created_at axis is unambiguous
# and differs from the shipped_at axis (which would be all-NULL, one bucket).
_TWO_TD_ROWS = [
    (1, 100, 100.0, "W", "2025-01-15", "2025-06-01"),
    (2, 101, 250.0, "E", "2025-02-15", "2025-06-01"),
    (3, 102, 400.0, "N", "2025-03-15", "2025-06-01"),
]
_TWO_TD_INNER = SlayerQuery.model_validate({
    "name": "s1", "source_model": "orders",
    "dimensions": ["created_at", "shipped_at"],
    "measures": [{"formula": "amount:sum", "name": "rev"}],
})


async def _two_td_engine(backend: str, tmp: str):
    return await make_engine(
        backend, base_dir=os.path.join(tmp, "store"),
        db_path=os.path.join(tmp, f"t.{backend}"),
        tables=[orders_table_spec(_TWO_TD_ROWS)], models=[orders_model()],
    )


async def test_two_stage_time_dimensions_without_main_raise() -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [
            TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH),
            TimeDimension(dimension=ColumnRef(name="shipped_at"), granularity=TG.MONTH),
        ],
        "measures": [{"formula": "change(rev:sum)", "name": "chg"}],
    })
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _two_td_engine("sqlite", tmp)
        with pytest.raises(ValueError, match="unambiguous time dimension"):
            await engine.execute(query=[_TWO_TD_INNER, outer], dry_run=True)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_main_time_dimension_selects_stage_axis(backend: str) -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [
            TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH),
            TimeDimension(dimension=ColumnRef(name="shipped_at"), granularity=TG.MONTH),
        ],
        "main_time_dimension": "created_at",
        "measures": [{"formula": "change(rev:sum)", "name": "chg"}],
    })
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _two_td_engine(backend, tmp)
        resp = await engine.execute(query=[_TWO_TD_INNER, outer])
    # change over the created_at axis (shipped_at constant ⇒ shipped axis would be all NULL).
    chg = {date_str(r["s1.created_at"]): r["s1.chg"] for r in resp.data}
    assert chg == {"2025-01-01": None, "2025-02-01": 150.0, "2025-03-01": 150.0}


# --- functional surfaces on a stage time dimension ---
@pytest.mark.parametrize("backend", BACKENDS)
async def test_order_by_stage_bucket(backend: str) -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
        "measures": [{"formula": "rev:sum"}],
        "order": [OrderItem.model_validate({"column": "month(created_at)", "direction": "desc"})],
    })
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec_outer(backend, tmp, outer)
    buckets = [date_str(r["s1.created_at"]) for r in resp.data]
    assert buckets == ["2025-04-01", "2025-03-01", "2025-02-01", "2025-01-01"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_two_granularities_of_one_stage_column(backend: str) -> None:
    outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [
            TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH),
            TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.YEAR),
        ],
        "measures": [{"formula": "rev:sum"}],
    })
    with tempfile.TemporaryDirectory() as tmp:
        resp = await _exec_outer(backend, tmp, outer)
    assert resp.data, "expected rows"
    keys = set(resp.data[0].keys())
    assert "s1.created_at.month" in keys
    assert "s1.created_at.year" in keys
    years = {date_str(r["s1.created_at.year"]) for r in resp.data}
    assert years == {"2025-01-01"}
    months = {date_str(r["s1.created_at.month"]) for r in resp.data}
    assert months == {"2025-01-01", "2025-02-01", "2025-03-01", "2025-04-01"}
