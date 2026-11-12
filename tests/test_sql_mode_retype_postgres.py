"""An ``sql``-mode model's stale non-list column over a live Postgres list (spec: models/schema-drift)."""

import pytest

from slayer.core.enums import DataType
from tests._dev2120_fixtures import contractors_statements
from tests._pg_helpers import client_datasource, pg_query_engine
from tests._sql_mode_retype_fixtures import (
    check_query_carries_finding,
    check_reingest_retypes,
    check_validation_finding,
    sql_shapes,
    stale_sql_model,
)

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in contractors_statements(dialect="postgres"):
        cur.execute(stmt)
    # A list function sqlglot cannot type.
    cur.execute("CREATE FUNCTION same_list(a INTEGER[]) RETURNS INTEGER[] LANGUAGE SQL AS 'SELECT a'")
    postgresql.commit()
    ds = client_datasource(postgresql.info)
    async with pg_query_engine(ds, base_dir=str(tmp_path)) as engine:
        yield engine, ds


@pytest.mark.parametrize(("sql", "column", "element"), sql_shapes(table="contractors", column="scores",
                                                                  untypeable="same_list"))
async def test_sql_mode_list_retype(pg, sql: str, column: str, element: DataType) -> None:
    engine, ds = pg
    await engine.storage.save_model(stale_sql_model(sql=sql, column=column, data_source=ds.name))
    await check_validation_finding(engine, data_source=ds.name, column=column)
    await check_query_carries_finding(engine, column=column)
    await check_reingest_retypes(engine, datasource=ds, table="contractors", column=column, element=element)
