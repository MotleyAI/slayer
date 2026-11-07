"""Ossie import: statistical and approximate-distinct aggregates; the explicit time role."""

import statistics
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from tests._osi_helpers import by_name, cols, convert, documents, ds, f, flat, m, scalar, seed_db, unconverted

_AMOUNTS = [10, 5, 7]
_QUANTITIES = [1, 2, 3]


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


def _orders(*metrics: dict[str, Any]) -> dict[str, Any]:
    return flat([ds("orders", fields=[f("order_id", "o_orderkey"), f("customer_id", "o_custkey"), f("amount"),
                                      f("quantity"), f("status")], metrics=list(metrics))])


def _covariance(xs: Sequence[float], ys: Sequence[float], *, sample: bool) -> float:
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (len(xs) - (1 if sample else 0))


@pytest.mark.parametrize(("sql", "aggregation", "expected"), [
    pytest.param("STDDEV_SAMP(amount)", "stddev_samp", statistics.stdev(_AMOUNTS), id="stddev-samp"),
    pytest.param("STDDEV_POP(amount)", "stddev_pop", statistics.pstdev(_AMOUNTS), id="stddev-pop"),
    pytest.param("VAR_SAMP(amount)", "var_samp", statistics.variance(_AMOUNTS), id="var-samp"),
    pytest.param("VAR_POP(amount)", "var_pop", statistics.pvariance(_AMOUNTS), id="var-pop"),
    pytest.param("CORR(amount, quantity)", "corr", statistics.correlation(_AMOUNTS, _QUANTITIES), id="corr"),
    pytest.param("COVAR_SAMP(amount, quantity)", "covar_samp", _covariance(_AMOUNTS, _QUANTITIES, sample=True),
                 id="covar-samp"),
    pytest.param("COVAR_POP(amount, quantity)", "covar_pop", _covariance(_AMOUNTS, _QUANTITIES, sample=False),
                 id="covar-pop"),
])
async def test_statistical_aggregate(db: Path, sql: str, aggregation: str, expected: float) -> None:
    result = convert(db=db, docs=documents(_orders(m("stat", sql))))
    assert not unconverted(result, "stat")
    stat = by_name(result)["orders"].get_measure("stat")
    assert stat is not None
    assert aggregation in stat.formula
    assert float(await scalar(result=result, db=db, model="orders", formula="stat")) == pytest.approx(expected)


async def test_filtered_correlation(db: Path) -> None:
    metric = m("r", "CORR(amount, quantity) FILTER (WHERE status = 'completed')")
    result = convert(db=db, docs=documents(_orders(metric)))
    assert not unconverted(result, "r")
    completed = statistics.correlation([10, 7], [1, 3])
    assert float(await scalar(result=result, db=db, model="orders", formula="r")) == pytest.approx(completed)


async def test_filtered_sample_stddev(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(m("sd", "STDDEV_SAMP(amount) FILTER (WHERE status <> 'x')"))))
    assert float(await scalar(result=result, db=db, model="orders", formula="sd")) == pytest.approx(
        statistics.stdev(_AMOUNTS))


async def test_approximate_distinct_count(db: Path) -> None:
    metric = {"name": "approx", "expression": {"dialects": [
        {"dialect": "DUCKDB", "expression": "APPROX_COUNT_DISTINCT(customer_id)"}]}}
    result = convert(db=db, docs=documents(_orders(metric)))
    assert not unconverted(result, "approx")
    approx = by_name(result)["orders"].get_measure("approx")
    assert approx is not None
    assert "count_distinct_approx" in approx.formula
    assert await scalar(result=result, db=db, model="orders", formula="approx") == 2


# ─────────────────────────────── time role ─────────────────────────────────

@pytest.mark.parametrize(("physical", "datatype", "is_time"), [
    pytest.param("ordered_at", "DateTime", False, id="temporal-opt-out"),
    pytest.param("order_year", "Integer", True, id="integer-year"),
])
def test_explicit_time_role_recorded(db: Path, physical: str, datatype: str, is_time: bool) -> None:
    doc = flat([ds("orders", fields=[f("order_id", "o_orderkey"),
                                     f("v", physical, datatype=datatype, dimension={"is_time": is_time})])])
    v = cols(by_name(convert(db=db, docs=documents(doc)))["orders"])["v"]
    assert (v.meta or {})["osi_is_time"] is is_time
