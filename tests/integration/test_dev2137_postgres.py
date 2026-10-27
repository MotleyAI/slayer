"""List columns on a real Postgres: ingestion, list functions, element access and ``collect`` (spec:
models/list-columns, queries/list-functions, queries/list-children, aggregations/collect)."""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import contractors_model, contractors_statements
from tests._dev2137_fixtures import assignments_model, assignments_statements, check_lists

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in [*contractors_statements(dialect="postgres"), *assignments_statements(dialect="postgres")]:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    await storage.save_model(contractors_model(data_source="pg"))
    await storage.save_model(assignments_model(data_source="pg"))
    try:
        yield SlayerQueryEngine(storage=storage), ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.mark.integration
async def test_lists(pg) -> None:
    await check_lists(pg[0], dialect="postgres")


@pytest.mark.integration
async def test_ingested_list_columns(pg) -> None:
    (model,) = ingest_datasource(datasource=pg[1], include_tables=["contractors"])
    typed = {c.name: (c.type, c.element_type) for c in model.columns if c.name in ("skills", "scores")}
    assert typed == {"skills": (DataType.ARRAY, DataType.TEXT), "scores": (DataType.ARRAY, DataType.INT)}
