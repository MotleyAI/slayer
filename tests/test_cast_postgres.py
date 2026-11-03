"""Cast type grammar, array casts and the TRY_CAST integer rule on a throwaway Postgres (spec: queries/casts)."""

from __future__ import annotations

import pytest

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._cast_fixtures import check_array_cast, check_try_cast_targets, tags_model, tags_statements
from tests._dev2101_fixtures import by_key, dev2101_models, q, seed_server

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
async def pg(postgresql, tmp_path) -> tuple[SlayerQueryEngine, int]:
    cur = postgresql.cursor()
    seed_server(cur, dialect="postgres")
    for stmt in tags_statements("postgres"):
        cur.execute(stmt)
    cur.execute("SELECT current_setting('server_version_num')")
    version_num = int(cur.fetchone()[0])
    postgresql.commit()
    storage = YAMLStorage(base_dir=str(tmp_path))
    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="pg", type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    ))
    for model in [*dev2101_models(data_source="pg"), tags_model(data_source="pg")]:
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage), version_num


async def test_try_cast_integer_rule(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    await check_try_cast_targets(engine, model="orders")


async def test_try_cast_fractional_double_column(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(dimensions=["id", {"expression": "TRY_CAST(price / 2 AS INT)", "name": "x"}]))
    got = {int(k): (None if v is None else int(v)) for k, v in by_key(resp, "id", "x").items()}
    assert got == {1: 1, 2: None, 3: 2, 4: None, 5: None}, resp.sql


@pytest.mark.parametrize(("expr", "expected"), [
    ("TRY_CAST(left('x', 1) AS INT)", None), ("TRY_CAST(left('7', 1) AS INT)", 7), ("TRY_CAST(lpad('1', 3, '0') AS INT)", 1),
    ("TRY_CAST(json_value('{{\"v\": \"x\"}}', '$.v') AS INT)", None), ("TRY_CAST(json_value('{{\"v\": \"7\"}}', '$.v') AS INT)", 7),
    ("json_value('{{\"v\": \"x\"}}', '$.v' RETURNING INT)", None),
])
async def test_try_cast_of_a_constant_never_fails_at_plan_time(pg, expr: str, expected) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(dimensions=["id", {"expression": expr, "name": "x"}]))
    assert set(by_key(resp, "id", "x").values()) == {expected}, resp.sql


async def test_try_cast_of_an_aggregate_reads_the_group(pg) -> None:
    engine, version_num = pg
    if version_num < 160000:
        pytest.skip("Postgres before 16 gets a plain CAST")
    resp = await engine.execute(q(dimensions=["region"], measures=[
        {"formula": "TRY_CAST(count(*) AS INT)", "name": "n"},
        {"formula": "TRY_CAST(count(*) + TRY_CAST('1' AS INT) AS INT)", "name": "m"},
    ]))
    assert by_key(resp, "region", "n") == {"EU": 2, "US": 2, None: 1}, resp.sql
    assert by_key(resp, "region", "m") == {"EU": 3, "US": 3, None: 2}, resp.sql


async def test_array_cast_is_a_list(pg) -> None:
    await check_array_cast(pg[0])


async def test_zoned_timestamp_with_precision_executes(pg) -> None:
    resp = await pg[0].execute(q(
        dimensions=["id", {"expression": "CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)", "name": "x"}],
        filters=["id = 1"], distinct_dimension_values=False,
    ))
    assert len(resp.data) == 1, resp.sql
    assert by_key(resp, "id", "x")[1] is not None, resp.sql
