"""Laws over spine populations: measure invariance, grain-union broadcast, NULL axes, DATE/TIMESTAMP axes, model filters."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType

from tests._dev2015_fixtures import (
    BACKENDS,
    JAN_MAR,
    ORDERS_ROWS,
    TWO_FACTS,
    bucket_key,
    by_bucket,
    m,
    spine_engine,
    spine_models,
    spine_query,
    spine_tables,
    value,
)

POOL = [
    *TWO_FACTS,
    m("avg(orders.amount)", "a"),
    m("count(returns.id)", "rc"),
    m("sum(orders.amount, window='2m')", "w"),
    m("cumsum(sum(returns.amount))", "cs"),
]
BASES: dict[str, dict[str, Any]] = {
    "spine-only": {},
    "per-group": {"dimensions": ["customers.region"], "date_range": JAN_MAR},
    "fact-filter": {"filters": ["orders.amount > 60"]},
    "p-filter": {"dimensions": ["customers.region"], "filters": ["customers.region != 'E'"]},
}
MEASURE_SETS = {"one": POOL[:1], "facts": POOL[:3], "all": POOL}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with spine_engine(request.param) as eng:
        yield eng


def _cells(resp, *, by: list[str]) -> dict[tuple, dict[str, Any]]:
    out: dict[tuple, dict[str, Any]] = {}
    for row in resp.data:
        cell = (*(value(row, d) for d in by), bucket_key(row["time_spine.timestamp"]))
        out[cell] = {k.rsplit(".", 1)[-1]: v for k, v in row.items() if k.rsplit(".", 1)[-1] in {x["name"] for x in POOL}}
    return out


@pytest.mark.parametrize("base", BASES)
async def test_measures_never_change_rows_or_siblings(engine, base) -> None:
    extra = dict(BASES[base])
    by = [d.rsplit(".", 1)[-1] for d in extra.get("dimensions", [])]
    results = {k: _cells(await engine.execute(spine_query(measures=ms, **extra)), by=by)
               for k, ms in MEASURE_SETS.items()}
    full = results["all"]
    for name, cells in results.items():
        assert set(cells) == set(full), name
        for cell, values in cells.items():
            for measure, v in values.items():
                assert v == full[cell][measure], (name, cell, measure)


async def test_grain_union_broadcast(engine) -> None:
    resp = await engine.execute(spine_query(
        measures=[
            m("sum(orders.amount) / sum(orders.amount, partition_by=customers.region)", "share"),
            m("count(orders.id) + count(returns.id)", "events"),
        ],
        dimensions=["customers.region"], date_range=JAN_MAR,
    ))
    got = by_bucket(resp, ["share", "events"], by=["region"])
    assert len(got) == 9
    assert got[("N", "2025-01")][0] == pytest.approx(100 / 170)
    assert got[("N", "2025-02")][0] == pytest.approx(70 / 170)
    assert got[("S", "2025-01")][0] == pytest.approx(1.0)
    assert got[("N", "2025-03")][0] is None
    assert {k: v[1] for k, v in got.items()} == {
        ("N", "2025-01"): 1.0, ("N", "2025-02"): 2.0, ("N", "2025-03"): 0.0,
        ("S", "2025-01"): 1.0, ("S", "2025-02"): 0.0, ("S", "2025-03"): 1.0,
        ("E", "2025-01"): 0.0, ("E", "2025-02"): 0.0, ("E", "2025-03"): 0.0,
    }


@pytest.mark.parametrize("backend", BACKENDS)
async def test_null_axis_rows_are_never_attributed(backend) -> None:
    rows = [*ORDERS_ROWS, (4, 3, None, 999.0)]
    async with spine_engine(backend, tables=spine_tables(orders_rows=rows)) as eng:
        resp = await eng.execute(spine_query(measures=TWO_FACTS))
        grid = await eng.execute(spine_query(measures=TWO_FACTS[:1], dimensions=["customers.region"],
                                             date_range=JAN_MAR))
    got = by_bucket(resp, ["o", "n"])
    assert sum(v[1] for v in got.values()) == 3.0
    assert got["2025-01"] == (150.0, 2.0)
    assert all(v == (None,) for k, v in by_bucket(grid, ["o"], by=["region"]).items() if k[0] == "E")


@pytest.mark.parametrize("backend", BACKENDS)
async def test_timestamp_axis_buckets_like_a_date_axis(backend) -> None:
    rows = [(1, 1, "2025-01-10 08:00:00", 100.0), (2, 2, "2025-01-31 23:59:59", 50.0),
            (3, 1, "2025-02-01 00:00:00", 70.0)]
    async with spine_engine(backend, tables=spine_tables(orders_rows=rows, date_type="TIMESTAMP"),
                            models=spine_models(date_type=DataType.TIMESTAMP)) as eng:
        resp = await eng.execute(spine_query(measures=TWO_FACTS))
        mid = await eng.execute(spine_query(measures=TWO_FACTS[:1], date_range=["2025-01-15", "2025-02-28"]))
    got = by_bucket(resp, ["o", "r", "n"])
    assert got["2025-01"] == (150.0, None, 2.0)
    assert got["2025-02"] == (70.0, 20.0, 1.0)
    assert by_bucket(mid, ["o"]) == {"2025-01": (50.0,), "2025-02": (70.0,)}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_model_filter_on_the_axis_column(backend) -> None:
    models = spine_models(orders_filters=["order_date >= '2025-02-01'"])
    async with spine_engine(backend, models=models) as eng:
        resp = await eng.execute(spine_query(measures=TWO_FACTS))
    got = by_bucket(resp, ["o", "r", "n"])
    assert got["2025-01"] == (None, None, 0.0)
    assert got["2025-02"] == (70.0, 20.0, 1.0)
    assert len(got) == 6
