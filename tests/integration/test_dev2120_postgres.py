"""The portable Mode-B library on a real Postgres (spec: queries/scalar-functions, regex, json, mixed-type-coercion)."""

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import (
    DOCS_ROWS,
    LIB,
    check_library,
    check_native_json,
    lib_model,
    lib_query,
    server_table_statements,
)

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in server_table_statements(LIB, dialect="postgres"):
        cur.execute(stmt)
    cur.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, payload JSONB)")
    cur.execute("CREATE TABLE docs_json (id INTEGER PRIMARY KEY, payload JSON)")
    for table in ("docs", "docs_json"):
        cur.executemany(f"INSERT INTO {table} VALUES (%s, %s)", DOCS_ROWS)
    cur.execute("SELECT current_setting('server_version_num')")
    version_num = int(cur.fetchone()[0])
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    await storage.save_model(lib_model(data_source="pg"))
    try:
        for model in ingest_datasource(datasource=ds, include_tables=["docs", "docs_json"]):
            await storage.save_model(model)
        yield SlayerQueryEngine(storage=storage), version_num
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.mark.integration
async def test_library(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("the NULL-on-error cases need Postgres 16+")
    await check_library(engine)


@pytest.mark.integration
@pytest.mark.parametrize("model", ["docs", "docs_json"])
async def test_native_json_columns(pg, model: str) -> None:
    await check_native_json(pg[0], model=model)


@pytest.mark.integration
async def test_author_cast_keeps_postgres_error(pg) -> None:
    with pytest.raises(Exception, match="(?i)invalid input syntax"):  # noqa: B017 — Postgres's own error
        await pg[0].execute(lib_query(filters=["CAST(num_txt AS INT) > 100"]))


@pytest.mark.integration
async def test_double_modulo_runs(pg) -> None:
    resp = await pg[0].execute(lib_query("x % 2", filters=["id = 7"]))
    assert float(resp.data[0]["lib.v"]) == -1.5
