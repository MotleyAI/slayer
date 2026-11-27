"""LAW measure monotonicity (semantics.arc42.md §3 Law 7): adding a measure to an inferred query never removes a
row or changes a value in an existing row; a new row carries the other measures' empty values."""

from __future__ import annotations

import pytest

from tests._dev2212_fixtures import seed_engine
from tests._law_harness import (
    ROOTLESS_EMPTY,
    law_assert,
    rootless_params,
    rootless_query,
    rootless_rows,
    values_equal,
)


@pytest.fixture(params=rootless_params())
async def case(request):
    dialect, shape = request.param
    async with seed_engine(dialect, orphan=True) as eng:
        yield eng, shape


async def test_adding_a_measure_only_adds_rows(case) -> None:
    eng, shape = case
    keys = shape.metric_keys if len(shape.metric_keys) > 1 else (*shape.metric_keys, "ret")
    full = rootless_rows(await eng.execute(rootless_query(shape, keys=keys)), shape=shape)
    for drop in keys:
        kept = [k for k in keys if k != drop]
        sub = rootless_rows(await eng.execute(rootless_query(shape, keys=kept)), shape=shape)
        law_assert(set(sub) <= set(full), law="measure monotonicity",
                   detail=f"adding {drop!r} removed rows {sorted(map(str, set(sub) - set(full)))}", shape=shape)
        for key, row in full.items():
            for k in kept:
                expected = sub[key][f"m_{k}"] if key in sub else ROOTLESS_EMPTY.get(k)
                law_assert(values_equal(row[f"m_{k}"], expected), law="measure monotonicity",
                           detail=f"cell {key} {k!r} is {row[f'm_{k}']!r}, alone {expected!r} (added {drop!r})",
                           shape=shape)
