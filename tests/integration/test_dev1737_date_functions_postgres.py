"""Mode-B date functions executed on a real Postgres: the oracle matrix plus spec scenarios."""

from datetime import datetime, timezone

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import (
    DateCase,
    all_models,
    assert_case,
    check_server_scenarios,
    matrix_cases,
    matrix_query,
    server_seed_statements,
)

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

CASES = matrix_cases()


@pytest.fixture
async def pg_dates(postgresql, tmp_path) -> SlayerQueryEngine:
    cur = postgresql.cursor()
    for stmt in server_seed_statements("postgres", today=datetime.now(timezone.utc).date()):
        cur.execute(stmt)
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="testpg", type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    ))
    for model in all_models(data_source="testpg"):
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
async def test_matrix(pg_dates: SlayerQueryEngine, case: DateCase) -> None:
    resp = await pg_dates.execute(matrix_query(case))
    assert_case(resp.data, case)


@pytest.mark.integration
async def test_scenarios(pg_dates: SlayerQueryEngine) -> None:
    await check_server_scenarios(pg_dates)
