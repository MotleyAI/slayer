"""Cast type grammar, array casts and the TRY_CAST integer rule on a throwaway Postgres (spec: queries/casts)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._cast_fixtures import (
    casts_dataset,
    casts_statements,
    check_array_cast,
    check_author_casts,
    check_boolean_casts,
    check_casts_columns,
    check_try_cast_targets,
    tags_dataset,
    tags_statements,
)
from tests._dev2101_fixtures import by_key, dev2101_datasets, m, q, seed_server, single
from tests._pg_helpers import client_datasource, pg_query_engine

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path) -> AsyncIterator[tuple[SlayerQueryEngine, int]]:
    cur = postgresql.cursor()
    seed_server(cur, dialect="postgres")
    for stmt in [*tags_statements("postgres"), *casts_statements("postgres")]:
        cur.execute(stmt)
    cur.execute("SELECT current_setting('server_version_num')")
    version_num = int(cur.fetchone()[0])
    postgresql.commit()
    models = [*dev2101_datasets(data_source="pg"), tags_dataset(data_source="pg"), casts_dataset(data_source="pg")]
    async with pg_query_engine(client_datasource(postgresql.info), base_dir=str(tmp_path), datasets=models) as engine:
        yield engine, version_num


async def test_try_cast_integer_rule(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    await check_try_cast_targets(engine, dataset="orders")


async def test_try_cast_fractional_double_column(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(dimensions=["orders.id", {"field": "TRY_CAST(orders.price / 2 AS INT)", "alias": "x"}]))
    got = {int(k): (None if v is None else int(v)) for k, v in by_key(resp, "id", "x").items()}
    assert got == {1: 1, 2: 2, 3: 2, 4: 3, 5: 1}, resp.sql


@pytest.mark.parametrize(("expr", "expected"), [
    ("TRY_CAST(left('x', 1) AS INT)", None), ("TRY_CAST(left('7', 1) AS INT)", 7), ("TRY_CAST(lpad('1', 3, '0') AS INT)", 1),
    ("TRY_CAST(json_value('{{\"v\": \"x\"}}', '$.v') AS INT)", None), ("TRY_CAST(json_value('{{\"v\": \"7\"}}', '$.v') AS INT)", 7),
    ("json_value('{{\"v\": \"x\"}}', '$.v' RETURNING INT)", None),
    ("TRY_CAST(CAST(pi() AS TEXT) AS INT)", None), ("TRY_CAST(pi() AS INT)", 3),
])
async def test_try_cast_of_a_constant_never_fails_at_plan_time(pg, expr: str, expected) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]))
    assert set(by_key(resp, "id", "x").values()) == {expected}, resp.sql


async def test_try_cast_of_an_aggregate_reads_the_group(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(dimensions=["orders.region"], metrics=[
        {"metric": "TRY_CAST(count(orders.*) AS INT)", "alias": "n"},
        {"metric": "TRY_CAST(count(orders.*) + TRY_CAST('1' AS INT) AS INT)", "alias": "m"},
    ]))
    assert by_key(resp, "region", "n") == {"EU": 2, "US": 2, None: 1}, resp.sql
    assert by_key(resp, "region", "m") == {"EU": 3, "US": 3, None: 2}, resp.sql


async def test_author_casts_have_one_meaning(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    await check_author_casts(engine, dataset="orders")


async def test_boolean_casts_over_a_column(pg) -> None:
    await check_boolean_casts(pg[0], dataset="orders")


async def test_cast_columns(pg) -> None:
    await check_casts_columns(pg[0])


@pytest.mark.parametrize("text", ["NaN", "Infinity", "-Infinity"])
async def test_try_cast_of_a_non_finite_double_is_null(pg, text: str) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(metrics=[m(f"max(TRY_CAST(CAST('{text}' AS DOUBLE) AS INT))")]))
    assert single(resp) is None, resp.sql


async def test_array_cast_is_a_list(pg) -> None:
    await check_array_cast(pg[0])


async def test_zoned_timestamp_with_precision_executes(pg) -> None:
    resp = await pg[0].execute(q(
        dimensions=["orders.id", {"field": "CAST(orders.created_at AS TIMESTAMP(6) WITH TIME ZONE)", "alias": "x"}],
        filter=["orders.id = 1"], distinct_dimension_values=False,
    ))
    assert len(resp.data) == 1, resp.sql
    assert by_key(resp, "id", "x")[1] is not None, resp.sql
