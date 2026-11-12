"""List columns on a real Postgres: ingestion, list functions, element access and ``collect`` (spec:
models/list-columns, queries/list-functions, queries/list-children, aggregations/collect)."""

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.live_schema.ingestion import ingest_datasource_async
from tests._dev2120_fixtures import contractors_model, contractors_statements
from tests._dev2137_fixtures import assignments_model, assignments_statements, check_lists
from tests._list_mixed_numbers import check_mixed_numbers, mixed_numbers_model, mixed_numbers_statements
from tests._pg_helpers import client_datasource, pg_query_engine

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in [*contractors_statements(dialect="postgres"), *assignments_statements(dialect="postgres"),
                 *mixed_numbers_statements(dialect="postgres")]:
        cur.execute(stmt)
    postgresql.commit()
    ds = client_datasource(postgresql.info)
    models = [contractors_model(data_source="pg"), assignments_model(data_source="pg"),
              mixed_numbers_model(data_source="pg")]
    async with pg_query_engine(ds, base_dir=str(tmp_path), models=models) as engine:
        yield engine, ds


async def test_lists(pg) -> None:
    await check_lists(pg[0], dialect="postgres")


async def test_mixed_numeric_lists(pg) -> None:
    await check_mixed_numbers(pg[0])


async def test_ingested_list_columns(pg) -> None:
    engine, ds = pg
    (model,) = await ingest_datasource_async(ds, client=engine.client_for(ds), include_tables=["contractors"])
    typed = {c.name: (c.type, c.element_type) for c in model.columns if c.name in ("skills", "scores")}
    assert typed == {"skills": (DataType.ARRAY, DataType.TEXT), "scores": (DataType.ARRAY, DataType.INT)}
