"""List columns on a real Postgres: ingestion, list functions, element access and ``collect`` (spec:
models/list-columns, queries/list-functions, queries/list-children, aggregations/collect)."""

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import contractors_model, contractors_statements
from tests._dev2137_fixtures import assignments_model, assignments_statements, check_lists
from tests._list_mixed_numbers import check_mixed_numbers, mixed_numbers_model, mixed_numbers_statements

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in [*contractors_statements(dialect="postgres"), *assignments_statements(dialect="postgres"),
                 *mixed_numbers_statements(dialect="postgres")]:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    for model in (contractors_model(data_source="pg"), assignments_model(data_source="pg"),
                  mixed_numbers_model(data_source="pg")):
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine, ds
    finally:
        await engine.aclose()
        engine.close()
        engine_factory.invalidate_engine(ds)


async def test_lists(pg) -> None:
    await check_lists(pg[0], dialect="postgres")


async def test_mixed_numeric_lists(pg) -> None:
    await check_mixed_numbers(pg[0])


async def test_ingested_list_columns(pg) -> None:
    engine, ds = pg
    (model,) = await ingest_datasource(ds, client=engine.client_for(ds), include_tables=["contractors"])
    typed = {c.name: (c.type, c.element_type) for c in model.columns if c.name in ("skills", "scores")}
    assert typed == {"skills": (DataType.ARRAY, DataType.TEXT), "scores": (DataType.ARRAY, DataType.INT)}
