"""A FILTERed custom aggregation on a throwaway Postgres.

Spec: aggregations/functional-form › An aggregation accepts a FILTER clause.
"""

from __future__ import annotations

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2101_fixtures import check_filtered_custom_aggregations, dev2101_models, seed_server

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> SlayerQueryEngine:
    seed_server(postgresql.cursor(), dialect="postgres")
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="pg", type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    ))
    for model in dev2101_models(data_source="pg"):
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


async def test_filtered_custom_aggregations(pg_engine: SlayerQueryEngine) -> None:
    await check_filtered_custom_aggregations(pg_engine)
