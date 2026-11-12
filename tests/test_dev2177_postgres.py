"""Boolean aggregation parameters and conditional branch types on a throwaway Postgres.

Spec: aggregations/boolean-inputs; queries/mixed-type-coercion.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2177_fixtures import (
    BOOLEAN_PARAMETER_CHECKS,
    check_concat_branches_rejected,
    check_custom_aggregation_receives_raw_boolean,
    dev2177_models,
)
from tests._mcp_idiom_fixtures import idiom_seed_statements
from tests._pg_helpers import client_datasource, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> AsyncIterator[SlayerQueryEngine]:
    cur = postgresql.cursor()
    for stmt in idiom_seed_statements():
        cur.execute(stmt)
    postgresql.commit()
    async with pg_query_engine(
        client_datasource(postgresql.info), base_dir=str(tmp_path), models=dev2177_models(data_source="pg"),
    ) as engine:
        yield engine


@pytest.mark.parametrize("check", BOOLEAN_PARAMETER_CHECKS, ids=[c.__name__ for c in BOOLEAN_PARAMETER_CHECKS])
async def test_boolean_parameter(pg_engine: SlayerQueryEngine,
                                 check: Callable[[SlayerQueryEngine], Awaitable[None]]) -> None:
    await check(pg_engine)


async def test_custom_aggregation_receives_raw_boolean(pg_engine: SlayerQueryEngine) -> None:
    await check_custom_aggregation_receives_raw_boolean(pg_engine)


async def test_concatenation_branch_rejected_at_plan_time(pg_engine: SlayerQueryEngine) -> None:
    await check_concat_branches_rejected(pg_engine)
