"""Mode-B date functions executed on a real Postgres: the oracle matrix plus spec scenarios."""

from collections.abc import AsyncIterator
from datetime import datetime, timezone

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev1737_fixtures import (
    DateCase,
    all_models,
    assert_case,
    check_server_scenarios,
    matrix_cases,
    matrix_query,
    server_seed_statements,
)
from tests._pg_helpers import client_datasource, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

CASES = matrix_cases()


@pytest.fixture
async def pg_dates(postgresql, tmp_path) -> AsyncIterator[SlayerQueryEngine]:
    cur = postgresql.cursor()
    for stmt in server_seed_statements("postgres", today=datetime.now(timezone.utc).date()):
        cur.execute(stmt)
    postgresql.commit()
    async with pg_query_engine(client_datasource(postgresql.info, name="testpg"), base_dir=str(tmp_path),
                               models=all_models(data_source="testpg")) as engine:
        yield engine


@pytest.mark.integration
@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
async def test_matrix(pg_dates: SlayerQueryEngine, case: DateCase) -> None:
    resp = await pg_dates.execute(matrix_query(case))
    assert_case(resp.data, case)


@pytest.mark.integration
async def test_scenarios(pg_dates: SlayerQueryEngine) -> None:
    await check_server_scenarios(pg_dates)
