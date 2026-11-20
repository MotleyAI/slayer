"""LAW unit (semantics.arc42.md §3 Law 10, Ossie §3.6): a query with measures and no dimensions returns exactly one
row, with and without ``population``, with and without a filter no row passes."""

from __future__ import annotations

import pytest

from tests._dev2212_fixtures import seed_engine
from tests._law_harness import ROOTLESS_POPULATION, law_assert, rootless_params, rootless_query


@pytest.fixture(params=rootless_params(lambda s: s.family == "unit"))
async def case(request):
    dialect, shape = request.param
    async with seed_engine(dialect, orphan=True) as eng:
        yield eng, shape


@pytest.mark.parametrize("population", [None, ROOTLESS_POPULATION["unit"]], ids=["inferred", "anchored"])
@pytest.mark.parametrize("filter", ["<shape>", "1 = 0"], ids=["shape-filter", "no-row-passes"])
async def test_a_dimensionless_query_returns_one_row(case, population, filter) -> None:
    eng, shape = case
    resp = await eng.execute(rootless_query(shape, filter=filter, population=population))
    law_assert(len(resp.data) == 1, law="unit", detail=f"{len(resp.data)} rows", shape=shape)
