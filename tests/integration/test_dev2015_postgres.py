"""The time spine and custom granularities on a real Postgres."""

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests.integration._dev2015_server import (
    check_all,
    check_quarter_hour_year,
    server_models,
    server_models_ts,
    server_statements,
    with_granularities,
)

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_spine(postgresql, tmp_path) -> SlayerQueryEngine:
    cur = postgresql.cursor()
    for stmt in server_statements("postgres"):
        cur.execute(stmt)
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    info = postgresql.info
    base = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port,
                            database=info.dbname, username=info.user, password="")
    for name, models in (("pg", server_models(data_source="pg")), ("pg_ts", server_models_ts(data_source="pg_ts"))):
        await storage.save_datasource(with_granularities(base, name=name))
        for model in models:
            await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_scenarios(pg_spine: SlayerQueryEngine) -> None:
    await check_all(pg_spine, data_source="pg", ts_data_source="pg_ts")


@pytest.mark.integration
async def test_quarter_hour_year(pg_spine: SlayerQueryEngine) -> None:
    await check_quarter_hour_year(pg_spine, data_source="pg")
