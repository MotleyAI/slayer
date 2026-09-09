"""LAW grain union (semantics.arc42.md §3 law 1): grain(a ⊕ b) =
grain(a) ∨ grain(b). Population half: measures never move the group set.
Broadcast half: an attached operand is constant within its own semantic grain;
the direct half combines operands of different explicit grains at a finer
query grain and pins the union-grain cells."""

from __future__ import annotations

import pytest

from tests._law_harness import (
    ModelMeasure,
    OPERAND_GRAIN,
    canon,
    execute_shape,
    keyed_rows,
    law_assert,
    law_params,
    make_law_engine,
    month_key,
    month_td,
    q,
)

#: customers.spend grand total (grain-() broadcast value on filterless shapes).
CM_TOTAL = 350.0

#: Direct-half oracle: REGION_TOTAL[region] + city-alone total[city] per
#: union-grain cell (region, city) — hand-derived from the DEV-1739 rows.
MIXED_GRAIN_X = {
    ("North", "CityA"): 130.0, ("North", "CityB"): 140.0, ("North", None): 130.0,
    ("South", "CityC"): 100.0, (None, "CityD"): 120.0,
}


@pytest.fixture(params=law_params())
async def law_case(request):
    dialect, shape = request.param
    async for engine, _db_path in make_law_engine(dialect):
        yield engine, shape


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine, _db_path in make_law_engine(request.param):
        yield engine


def _grain_projection(key: tuple, *, shape, grain: tuple) -> tuple:
    parts = []
    if "region" in grain:
        parts.append(key[0])
    if "month" in grain:
        assert shape.with_month
        parts.append(key[-1])
    return tuple(parts)


async def test_population_and_operand_broadcast_constancy(law_case):
    engine, shape = law_case
    full = await execute_shape(engine, shape)
    if full is None:
        return
    full_rows = keyed_rows(full, shape=shape)

    dims_only = await execute_shape(engine, shape, dims_only=True)
    if dims_only is not None:
        dims_keys = set(keyed_rows(dims_only, shape=shape))
        law_assert(
            set(full_rows) == dims_keys,
            law="grain union",
            detail=f"measures moved the population: "
                   f"only-measured={sorted(map(str, set(full_rows) - dims_keys))}, "
                   f"only-dims={sorted(map(str, dims_keys - set(full_rows)))}",
            shape=shape,
        )

    for meas in shape.measure_keys:
        grain = OPERAND_GRAIN.get(meas)
        if grain is None:
            continue
        cells: dict[tuple, set] = {}
        for key, row in full_rows.items():
            cell = _grain_projection(key, shape=shape, grain=grain)
            cells.setdefault(cell, set()).add(canon(row[f"orders.m_{meas}"]))
        for cell, values in cells.items():
            law_assert(
                len(values) == 1,
                law="grain union",
                detail=f"operand {meas!r} not constant within its semantic "
                       f"grain cell {cell}: {sorted(map(str, values))}",
                shape=shape,
            )
        if meas == "cm" and shape.filter is None:
            (value,) = cells[()]
            law_assert(
                value == CM_TOTAL,
                law="grain union",
                detail=f"filterless cm broadcast is {value!r}, not {CM_TOTAL}",
                shape=shape,
            )


async def test_mixed_grain_formula_constant_within_union_grain_cells(exec_engine):
    """part(region) ⊕ part(city) queried at (region, city, month): x is typed
    by the union grain (region, city) and constant within each of its cells."""
    resp = await exec_engine.execute(q(
        dimensions=["region", "city"],
        time_dimensions=month_td(),
        measures=[ModelMeasure(
            formula="amount:sum(partition_by=region) + amount:sum(partition_by=city)",
            name="x",
        )],
    ))
    grain_keys = {
        (r["orders.region"], r["orders.city"], month_key(r["orders.ordered_at"]))
        for r in resp.data
    }
    assert len(grain_keys) == len(resp.data), "duplicate query-grain rows"
    cells: dict[tuple, set] = {}
    for row in resp.data:
        cell = (row["orders.region"], row["orders.city"])
        cells.setdefault(cell, set()).add(canon(row["orders.x"]))
    for cell, values in cells.items():
        assert len(values) == 1, (
            f"LAW grain union violated — mixed-grain formula not constant "
            f"within union-grain cell {cell}: {sorted(map(str, values))}"
        )
    got = {cell: next(iter(values)) for cell, values in cells.items()}
    assert got == MIXED_GRAIN_X, (
        f"LAW grain union violated — union-grain cell values moved: {got}"
    )
    months = {month_key(row["orders.ordered_at"]) for row in resp.data}
    assert len(months) > 1, "query grain did not include the month axis"
