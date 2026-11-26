"""A transform over a windowed aggregate reads the aggregate's own cells: those of the same aggregate without
``window=`` (spec: queries/transforms › Transforms evaluate over their operand's domain). Seed and helpers:
``tests/test_dev2419_windowed_supply.py``."""

from __future__ import annotations

from typing import Any

import pytest

from tests._dev2212_fixtures import BACKENDS
from tests.test_dev2419_windowed_supply import (
    CAT_KEYS,
    CAT_ORDER_MONTH,
    SPINE,
    SPINE_KEYS,
    STATUS_SIGNUP_KEYS,
    STATUS_SIGNUP_MONTH,
    _cells,
    _engine,
    _key,
    _m,
    _run,
)

W = "count(customers.*, window='60d')"
WS = "sum(customers.credit, window='60d')"
PLAIN = "count(customers.*)"
TRANSFORMS = {"lag": "lag({})", "cumsum": "cumsum({})", "time_shift": "time_shift({}, -1)", "change": "change({})"}

CELLS = [("open", "2024-01"), ("open", "2024-03"), ("paid", "2024-01"), ("paid", "2024-02"),
         (None, "2024-04"), (None, "2024-05")]
W_VALUES = [1.0, 1.0, 1.0, 2.0, 1.0, 1.0]
#: (open, February) is no cell of the aggregate, so (open, March) has no previous bucket.
EXPECTED = {
    "lag": [None, 1.0, None, 1.0, None, 1.0],
    "cumsum": [1.0, 2.0, 1.0, 3.0, 1.0, 2.0],
    "time_shift": [None, None, None, 1.0, None, 1.0],
    "change": [None, None, None, 1.0, None, 0.0],
}
SIBLING = _m("sum(orders.amount)", "s")


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _expr(name: str, operand: str) -> str:
    return TRANSFORMS[name].format(operand)


async def _three_runs(backend: str, body: dict[str, Any], keys: tuple[str, ...], expr: str,
                      ) -> tuple[dict[tuple, Any], dict[tuple, Any], dict[tuple, Any]]:
    """The transform alone, beside an ``orders``-homed sibling, and under ``population: customers``."""
    async with _engine(backend) as eng:
        alone = await _run(eng, body, metrics=[_m(expr)])
        beside = await _run(eng, body, metrics=[_m(expr), SIBLING])
        named = await _run(eng, body, metrics=[_m(expr)], population="customers")
    return _cells(alone, keys), _cells(beside, keys), _cells(named, keys)


# --------------------------------------------------------------------------- #
# Spec scenarios
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name", list(TRANSFORMS))
async def test_a_transform_over_a_windowed_aggregate_reads_the_aggregates_own_cells(backend: str, name: str) -> None:
    # lag / cumsum pass before the fix (guards); time_shift / change raised an internal error
    async with _engine(backend) as eng:
        resp = await _run(eng, STATUS_SIGNUP_MONTH, metrics=[_m(W, "w"), _m(_expr(name, W))])
    assert _cells(resp, STATUS_SIGNUP_KEYS, "w") == dict(zip(CELLS, W_VALUES))
    assert _cells(resp, STATUS_SIGNUP_KEYS) == dict(zip(CELLS, EXPECTED[name]))


@pytest.mark.parametrize("name", list(TRANSFORMS))
async def test_a_sibling_or_a_named_population_leaves_a_windowed_transform_unchanged(backend: str, name: str) -> None:
    alone, beside, named = await _three_runs(backend, STATUS_SIGNUP_MONTH, STATUS_SIGNUP_KEYS, _expr(name, W))
    assert alone == dict(zip(CELLS, EXPECTED[name]))
    assert beside == alone
    assert named == alone


async def test_a_windowed_running_total_by_category_ignores_the_sibling(backend: str) -> None:
    alone, beside, named = await _three_runs(backend, CAT_ORDER_MONTH, CAT_KEYS, _expr("cumsum", W))
    expected = {("food", "2024-03"): 1.0, ("toys", "2024-01"): 1.0, ("toys", "2024-02"): 3.0,
                (None, "2024-04"): 1.0, (None, None): 0.0}
    assert alone == expected
    assert beside == expected
    assert named == expected


# --------------------------------------------------------------------------- #
# Further transforms and positions
# --------------------------------------------------------------------------- #
FURTHER = {"lead": f"lead({W})", "change_pct": f"change_pct({W})", "time_shift_sum": f"time_shift({WS}, -1)",
           "lead_sum": f"lead({WS})", "change_pct_sum": f"change_pct({WS})"}


@pytest.mark.parametrize("name", list(FURTHER))
async def test_further_windowed_transforms_ignore_siblings_and_population(backend: str, name: str) -> None:
    alone, beside, named = await _three_runs(backend, STATUS_SIGNUP_MONTH, STATUS_SIGNUP_KEYS, FURTHER[name])
    assert set(alone) == set(CELLS)
    assert beside == alone
    assert named == alone


async def test_a_nested_transform_steps_over_the_inner_transforms_cells(backend: str) -> None:
    alone, beside, named = await _three_runs(backend, STATUS_SIGNUP_MONTH, STATUS_SIGNUP_KEYS,
                                             f"lag({_expr('cumsum', W)})")
    assert alone == dict(zip(CELLS, [None, 1.0, None, 1.0, None, 1.0]))
    assert beside == alone
    assert named == alone


RANGED = {"dimensions": ["orders.status"], "time_dimensions": [
    {"field": "customers.signup_at", "granularity": "month", "date_range": ["2024-02-01", "2024-05-31"]}]}
RANGED_CELLS = [("open", "2024-03"), ("paid", "2024-02"), (None, "2024-04"), (None, "2024-05")]


@pytest.mark.parametrize(("name", "values"), [
    ("lag", [None, None, None, 1.0]),
    # calendar step: (paid, February) reads January, before the range
    ("time_shift", [None, 1.0, None, 1.0]),
])
async def test_a_date_range_bounds_the_windowed_transform_domain(backend: str, name: str, values: list) -> None:
    alone, beside, named = await _three_runs(backend, RANGED, STATUS_SIGNUP_KEYS, _expr(name, W))
    assert alone == dict(zip(RANGED_CELLS, values))
    assert beside == alone
    assert named == alone


MONTHS = [f"2024-0{i}" for i in range(1, 7)]
#: per status, January to June; time_shift and lag read the previous bucket's spine value (December reads 0)
SPINE_EXPECTED = {
    "time_shift": {"open": [0, 1, 1, 1, 0, 0], "paid": [0, 1, 2, 1, 0, 0], None: [0, 0, 0, 0, 1, 1]},
    "lag": {"open": [None, 1, 1, 1, 0, 0], "paid": [None, 1, 2, 1, 0, 0], None: [None, 0, 0, 0, 1, 1]},
    "cumsum": {"open": [1, 2, 3, 3, 3, 3], "paid": [1, 3, 4, 4, 4, 4], None: [0, 0, 0, 1, 2, 2]},
}


@pytest.mark.parametrize("name", list(SPINE_EXPECTED))
async def test_over_the_spine_a_windowed_transform_steps_over_its_homes_tuples(backend: str, name: str) -> None:
    # lag / cumsum are guards; time_shift read NULL in the NULL-status buckets
    alone, beside, named = await _three_runs(backend, SPINE, SPINE_KEYS, _expr(name, W))
    assert alone == {(s, mo): None if v is None else float(v)
                     for s, values in SPINE_EXPECTED[name].items() for mo, v in zip(MONTHS, values)}
    assert beside == alone
    assert named == alone


SUPPLIERS = [_m(PLAIN, "c"), SIBLING]


async def test_a_windowed_transform_in_a_filter_reads_its_metric_value(backend: str) -> None:
    expr = _expr("lag", W)
    async with _engine(backend) as eng:
        as_metric = await _run(eng, STATUS_SIGNUP_MONTH, metrics=[*SUPPLIERS, _m(expr)])
        filtered = await _run(eng, STATUS_SIGNUP_MONTH, metrics=SUPPLIERS, filter=[f"{expr} = 1"])
    values = _cells(as_metric, STATUS_SIGNUP_KEYS)
    assert values == dict(zip(CELLS, EXPECTED["lag"]))
    assert set(_cells(filtered, STATUS_SIGNUP_KEYS, "c")) == {k for k, v in values.items() if v == 1.0}


async def test_a_windowed_transform_as_an_order_key_reads_its_metric_value(backend: str) -> None:
    expr = _expr("cumsum", W)
    order = [{"field": expr, "direction": "desc"}, {"field": "customers.signup_at", "direction": "desc"},
             {"field": "orders.status", "direction": "asc"}]
    async with _engine(backend) as eng:
        as_metric = await _run(eng, STATUS_SIGNUP_MONTH, metrics=[*SUPPLIERS, _m(expr)], order=order)
        ordered = await _run(eng, STATUS_SIGNUP_MONTH, metrics=SUPPLIERS, order=order)
    assert _cells(as_metric, STATUS_SIGNUP_KEYS) == dict(zip(CELLS, EXPECTED["cumsum"]))
    rows = [tuple(_key(r[k]) for k in STATUS_SIGNUP_KEYS) for r in ordered.data]
    assert rows == [tuple(_key(r[k]) for k in STATUS_SIGNUP_KEYS) for r in as_metric.data]
    assert rows == [("paid", "2024-02"), (None, "2024-05"), ("open", "2024-03"), (None, "2024-04"),
                    ("open", "2024-01"), ("paid", "2024-01")]


# --------------------------------------------------------------------------- #
# Guard: the same transforms over the plain aggregate
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name", list(TRANSFORMS))
async def test_a_plain_transform_ignores_siblings_and_population(backend: str, name: str) -> None:
    alone, beside, named = await _three_runs(backend, STATUS_SIGNUP_MONTH, STATUS_SIGNUP_KEYS, _expr(name, PLAIN))
    assert set(alone) == set(CELLS)
    assert beside == alone
    assert named == alone
