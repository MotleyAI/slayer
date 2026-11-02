"""Boolean aggregation parameters read as 0/1, on SQLite and DuckDB (spec: aggregations/boolean-inputs)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2177_fixtures import (
    BOOLEAN_PARAMETER_CHECKS,
    check_custom_aggregation_receives_raw_boolean,
    dev2177_models,
)
from tests._mcp_idiom_fixtures import make_idiom_engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_idiom_engine(dialect=request.param, models=dev2177_models()):
        yield e


@pytest.mark.parametrize("check", BOOLEAN_PARAMETER_CHECKS, ids=[c.__name__ for c in BOOLEAN_PARAMETER_CHECKS])
async def test_boolean_parameter(engine: SlayerQueryEngine, check: Callable[[SlayerQueryEngine], Awaitable[None]]) -> None:
    await check(engine)


async def test_custom_aggregation_receives_raw_boolean(engine: SlayerQueryEngine) -> None:
    await check_custom_aggregation_receives_raw_boolean(engine)
