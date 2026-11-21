"""LAW home-row preservation (semantics.arc42.md §3 Law 8, Ossie S1): in an inferred query every filtered home row of
a full-grain aggregate lands in exactly one row when its home determines the dimensions — so an additive measure
sums, over the result, to its total over the home's filtered rows (orphan home rows included)."""

from __future__ import annotations

from typing import Any, Optional

import pytest

from slayer.core.query import SlayerQuery

from tests._dev2212_fixtures import seed_engine
from tests._law_harness import law_assert, rootless_params, rootless_query, rootless_rows, values_equal

#: Additive measures homed on ``orders``, which determines every root-less dimension family.
ADDITIVE = {"rev": "sum(orders.amount)"}


def _total(values: list[Any]) -> Optional[float]:
    present = [float(v) for v in values if v is not None]
    return sum(present) if present else None


@pytest.fixture(params=rootless_params(lambda s: "rev" in s.metric_keys))
async def case(request):
    dialect, shape = request.param
    async with seed_engine(dialect, orphan=True) as eng:
        yield eng, shape


async def test_home_rows_are_preserved(case) -> None:
    eng, shape = case
    result = rootless_rows(await eng.execute(rootless_query(shape)), shape=shape)
    home = await eng.execute(SlayerQuery.model_validate({
        "population": "orders", "metrics": [{"metric": ADDITIVE["rev"], "alias": "t"}],
        **({"filter": [shape.filter]} if shape.filter else {}),
    }))
    over_rows = _total([row["m_rev"] for row in result.values()])
    law_assert(values_equal(over_rows, home.data[0]["t"]), law="home-row preservation",
               detail=f"result sums to {over_rows!r}, home rows to {home.data[0]['t']!r}", shape=shape)
