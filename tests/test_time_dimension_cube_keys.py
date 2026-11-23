"""Cube's time-dimension keys: ``dateRange`` behaves as ``date_range``; ``compareDateRange`` is refused with SLayer's
period comparison.

Spec: queries/time-dimensions › Cube's time-dimension keys.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from slayer.core.errors import UnknownKeyError
from slayer.core.query import SlayerQuery, TimeDimension
from tests._engine_helpers import owned_query_engine
from tests._ossie_shapes_fixtures import shop_engine_store
from tests._ossie_vocabulary_fixtures import fresh
from tests._query_input_fixtures import RANGE, SAVED, TD, refusal, shop_with_saved

#: The issue's demo query over the shop store: monthly revenue and order count.
MONTHLY: dict[str, Any] = {"population": "orders", "metrics": ["sum(orders.amount)", "count(orders.*)"]}
COMPARE = [["2024-01-01", "2024-01-31"], ["2023-01-01", "2023-01-31"]]
#: The shop's order months in ``RANGE``; every month of history is five.
RANGE_MONTHS = ["2024-01-01 00:00:00", "2024-06-01 00:00:00"]


def months(response) -> list[str]:
    return sorted(str(r["orders.ordered_at"]) for r in response.data)


async def run(storage, query: Any, **kwargs: Any):
    async with owned_query_engine(storage=storage) as engine:
        return await engine.execute(fresh(query), **kwargs)


class TestDateRange:
    async def test_date_range_in_cube_spelling_filters_like_date_range(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        cube = await run(storage, {**MONTHLY, "time_dimensions": [{**TD, "dateRange": RANGE}]})
        snake = await run(storage, {**MONTHLY, "time_dimensions": [{**TD, "date_range": RANGE}]})
        assert months(cube) == RANGE_MONTHS
        assert cube.data == snake.data
        assert cube.sql == snake.sql

    def test_a_time_dimension_reads_date_range_in_cube_spelling(self) -> None:
        assert TimeDimension.model_validate({**TD, "dateRange": RANGE}).date_range == RANGE

    async def test_a_refinement_time_dimension_reads_it_too(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        response = await run(storage, SAVED, refine={"time_dimensions": [{**TD, "dateRange": RANGE}]})
        assert months(response) == RANGE_MONTHS


class TestCompareDateRange:
    @pytest.mark.parametrize(("build", "expected_loc"), [
        pytest.param(lambda: TimeDimension.model_validate({**TD, "compareDateRange": COMPARE}), (), id="entry"),
        pytest.param(lambda: SlayerQuery.model_validate({**MONTHLY, "time_dimensions": [{**TD, "compareDateRange": COMPARE}]}),
                     ("time_dimensions", 0), id="in_query"),
    ])
    def test_it_is_refused_pointing_to_period_comparison(self, build: Any, expected_loc: tuple) -> None:
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            build()
        err, loc = refusal(info.value)
        assert err.key == "compareDateRange"
        assert loc == expected_loc
        for word in ("compareDateRange", "change", "change_pct", "time_shift"):
            assert word in str(err), str(err)


class TestShorthand:
    async def test_string_shorthand_time_dimensions_still_run(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        response = await run(storage, {**MONTHLY, "time_dimensions": ["date_trunc('month', orders.ordered_at)"]})
        assert len(response.data) == 5
