"""Result values from a real Postgres: ``collect`` over a uuid column, ``jsonb`` objects, ``bytea``, intervals and
exact numerics on MCP json and over the Postgres wire (spec: results/value-encoding)."""

import uuid
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest

from slayer.core.models import DatasourceConfig, ModelMeasure
from slayer.engine.ingestion import ingest_datasource
from slayer.mcp.server import create_mcp_server
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2172_fixtures import FacadeStore, mcp_rows, mcp_text, pg_query, serve, strict_json

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

UID_1 = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
UID_2 = uuid.UUID("b1ffcd00-0d1c-4ef8-bb6d-6bb9bd380a22")
BIG = Decimal("12345678901234567890.12")
STATEMENTS = [
    "CREATE TABLE things (id INTEGER PRIMARY KEY, uid UUID, payload JSONB, blob BYTEA, span INTERVAL, "
    "price NUMERIC(22, 2))",
    f"INSERT INTO things VALUES (1, '{UID_1}', '{{\"a\": [1, \"x\"]}}', '\\xdeadbeef', '1 day 2 hours', {BIG}), "
    f"(2, '{UID_2}', '[1, 2]', '\\x00', '-90 minutes', 0.50), (3, NULL, NULL, NULL, NULL, NULL)",
]


@pytest.fixture
async def store(postgresql, tmp_path) -> AsyncIterator[FacadeStore]:
    cur = postgresql.cursor()
    for stmt in STATEMENTS:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    (model,) = ingest_datasource(datasource=ds, include_tables=["things"])
    await storage.save_model(model.model_copy(update={"measures": [
        ModelMeasure(name="uids", formula="collect(uid)"), ModelMeasure(name="prices", formula="collect(price)")]}))
    try:
        yield FacadeStore(storage=storage, datasource=ds)
    finally:
        engine_factory.invalidate_engine(ds)


async def _mcp_rows(store: FacadeStore, query: dict) -> tuple[str, list[dict]]:
    text = await mcp_text(create_mcp_server(storage=store.storage), query, fmt="json")
    return text, mcp_rows(text)


@pytest.mark.integration
async def test_collect_over_uuid_on_mcp_json(store: FacadeStore) -> None:
    _text, rows = await _mcp_rows(store, {"source_model": "things", "measures": ["uids"]})
    assert sorted(rows[0]["things.uids"]) == sorted([str(UID_1), str(UID_2)])


@pytest.mark.integration
async def test_collect_over_exact_numerics_keeps_every_digit(store: FacadeStore) -> None:
    text, rows = await _mcp_rows(store, {"source_model": "things", "measures": ["prices"]})
    assert str(BIG) in text
    assert f'"{BIG}"' not in text
    assert sorted(rows[0]["things.prices"]) == sorted([BIG, Decimal("0.50")])


@pytest.mark.integration
async def test_values_on_mcp_json(store: FacadeStore) -> None:
    _text, rows = await _mcp_rows(store, {"source_model": "things", "dimensions": ["id", "payload", "blob", "span"],
                                          "distinct_dimension_values": False})
    got = {r["things.id"]: (r["things.payload"], r["things.blob"], r["things.span"]) for r in rows}
    assert got == {1: ({"a": [1, "x"]}, "\\xdeadbeef", "P1DT2H"), 2: ([1, 2], "\\x00", "-PT1H30M"),
                   3: (None, None, None)}


@pytest.mark.integration
def test_values_over_the_postgres_wire(store: FacadeStore) -> None:
    with serve(store) as servers:
        rows = pg_query(servers.pg_port, "SELECT id, uid, payload, blob, span FROM things", dbname="pg").rows
    got = {r[0]: (r[1], None if r[2] is None else strict_json(r[2]), r[3], r[4]) for r in rows}
    assert got == {1: (str(UID_1), {"a": [1, "x"]}, "\\xdeadbeef", "P1DT2H"),
                   2: (str(UID_2), [1, 2], "\\x00", "-PT1H30M"), 3: (None, None, None, None)}


@pytest.mark.integration
def test_collect_over_uuid_over_the_postgres_wire(store: FacadeStore) -> None:
    with serve(store) as servers:
        ((uids,),) = pg_query(servers.pg_port, "SELECT uids FROM things", dbname="pg").rows
    assert sorted(strict_json(uids)) == sorted([str(UID_1), str(UID_2)])
