"""DEV-1824 fixture smoke — the shared fixtures import cleanly and their
already-supported oracles agree with executed values on both backends."""

from __future__ import annotations

import pytest

from tests._dev1824_fixtures import (
    CITY_TOTAL,
    GRAND_TOTAL,
    ModelMeasure,
    REGION_MONTH_TOTAL,
    REGION_TOTAL,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def test_oracles_are_mutually_consistent() -> None:
    assert sum(REGION_TOTAL.values()) == GRAND_TOTAL
    assert sum(CITY_TOTAL.values()) == GRAND_TOTAL
    assert sum(REGION_MONTH_TOTAL.values()) == GRAND_TOTAL
    for region, total in REGION_TOTAL.items():
        assert sum(v for (r, _), v in CITY_TOTAL.items() if r == region) == total
        assert sum(
            v for (r, _), v in REGION_MONTH_TOTAL.items() if r == region
        ) == total


class TestSeededDataset:
    async def test_city_totals(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert set(by) == set(CITY_TOTAL)
        for key, total in CITY_TOTAL.items():
            assert float(by[key]["orders.s"]) == pytest.approx(total)

    async def test_region_month_totals(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"])):
                float(r["orders.s"])
            for r in resp.data
        }
        assert got == pytest.approx(REGION_MONTH_TOTAL)
