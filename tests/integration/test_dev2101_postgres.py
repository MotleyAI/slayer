"""The Mode-B SQL grammar on a real Postgres (spec: queries/expression-grammar, queries/casts, queries/positions)."""

from collections.abc import AsyncIterator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import (
    by_key,
    check_filtered_aggregations,
    check_null_ordering,
    check_server_forms,
    check_try_cast,
    dev2101_models,
    m,
    q,
    seed_server,
    single,
)
from tests._pg_helpers import client_datasource, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path) -> AsyncIterator[tuple[SlayerQueryEngine, int]]:
    cur = postgresql.cursor()
    seed_server(cur, dialect="postgres")
    cur.execute("SELECT current_setting('server_version_num')")
    version_num = int(cur.fetchone()[0])
    postgresql.commit()
    async with pg_query_engine(client_datasource(postgresql.info), base_dir=str(tmp_path),
                               models=dev2101_models(data_source="pg")) as engine:
        yield engine, version_num


@pytest.mark.integration
async def test_sql_forms(pg) -> None:
    await check_server_forms(pg[0])


@pytest.mark.integration
async def test_null_ordering(pg) -> None:
    await check_null_ordering(pg[0])


@pytest.mark.integration
async def test_filtered_aggregations(pg) -> None:
    await check_filtered_aggregations(pg[0])


@pytest.mark.integration
async def test_try_cast_follows_the_server_version(pg) -> None:
    engine, version_num = pg
    await check_try_cast(engine, nulls_invalid=version_num >= 160000)


@pytest.mark.integration
async def test_plain_cast_raises_on_invalid_input(pg) -> None:
    query = q(dimensions=["id", {"expression": "CAST(code AS INT)", "name": "x"}])
    with pytest.raises(Exception, match="(?i)invalid input syntax"):  # noqa: B017 — Postgres's own error
        await pg[0].execute(query)


@pytest.mark.integration
async def test_parameterised_cast_is_postgres_numeric(pg) -> None:
    resp = await pg[0].execute(q(measures=[m("sum(CAST(amount AS DECIMAL(10,2)))")]))
    assert "DECIMAL(10, 2)" in (resp.sql or "")
    assert single(resp) == 100


@pytest.mark.integration
async def test_cast_to_json_executes(pg) -> None:
    resp = await pg[0].execute(q(dimensions=["id", {"expression": "CAST(payload AS JSON)", "name": "x"}],
                                 filters=["id = 1"], distinct_dimension_values=False))
    assert by_key(resp, "id", "x")[1] in ({"a": 1}, '{"a": 1}')
