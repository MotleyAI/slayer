"""LAW filter non-expansion (semantics.arc42.md §3 Law 9, Ossie S6): adding a field filter never adds a row or
changes a result key, with and without ``population``."""

from __future__ import annotations

import pytest

from tests._dev2212_fixtures import seed_engine
from tests._law_harness import (
    ROOTLESS_FILTERS,
    ROOTLESS_POPULATION,
    law_assert,
    rootless_params,
    rootless_query,
    rootless_rows,
)

FILTERS = [f for f in ROOTLESS_FILTERS.values() if f]


@pytest.fixture(params=rootless_params())
async def case(request):
    dialect, shape = request.param
    async with seed_engine(dialect, orphan=True) as eng:
        yield eng, shape


@pytest.mark.parametrize("anchored", [False, True], ids=["inferred", "anchored"])
async def test_a_filter_never_expands(case, anchored: bool) -> None:
    eng, shape = case
    population = ROOTLESS_POPULATION[shape.family] if anchored else None
    plain_resp = await eng.execute(rootless_query(shape, filter=None, population=population))
    plain = rootless_rows(plain_resp, shape=shape)
    for f in FILTERS:
        filtered_resp = await eng.execute(rootless_query(shape, filter=f, population=population))
        filtered = rootless_rows(filtered_resp, shape=shape)
        law_assert(filtered_resp.columns == plain_resp.columns, law="filter non-expansion",
                   detail=f"{f!r} changed keys {plain_resp.columns} -> {filtered_resp.columns}", shape=shape)
        if not shape.dims:
            continue
        law_assert(set(filtered) <= set(plain), law="filter non-expansion",
                   detail=f"{f!r} added rows {sorted(map(str, set(filtered) - set(plain)))}", shape=shape)
