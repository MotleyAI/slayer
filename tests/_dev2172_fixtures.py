"""A DuckDB store with every value type and both join kinds, served over in-process Postgres-wire and Flight
servers, and the real clients that read them."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import threading
import uuid
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pyarrow as pa
import pyarrow.flight as fl
from google.protobuf.any_pb2 import Any as PbAny
from pydantic import BaseModel, ConfigDict

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig, ModelMeasure
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.flight import _flight_sql_pb2 as fsql_pb
from slayer.flight.handlers import FlightHandlers
from slayer.flight.server import build_server
from slayer.pg_facade.connection import PgConnection
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import seed_contractors

DS = "dev2172"
UID_1 = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
UID_3 = uuid.UUID("b1ffcd00-0d1c-4ef8-bb6d-6bb9bd380a22")
BIG = Decimal("12345678901234567890.12")

#: ``things`` row → its values, as the database holds them.
THINGS: tuple[dict[str, Any], ...] = (
    {"id": 1, "team_id": 1, "name": "a", "tags": ["x", "y"], "payload": {"a": [1, "x"]}, "uid": UID_1,
     "price": BIG, "blob": b"\xde\xad\xbe\xef", "span": dt.timedelta(days=1, hours=2),
     "ts": dt.datetime(2024, 1, 5, 10, 0)},
    {"id": 2, "team_id": 1, "name": "b", "tags": None, "payload": None, "uid": None, "price": None, "blob": None,
     "span": None, "ts": None},
    {"id": 3, "team_id": 2, "name": "c", "tags": [], "payload": [1, 2], "uid": UID_3, "price": Decimal("0.50"),
     "blob": b"\x00", "span": -dt.timedelta(minutes=90), "ts": dt.datetime(2024, 2, 7, 8, 0)},
)
#: ``parts``: thing 1 has two, thing 2 one, thing 3 none.
PARTS: tuple[tuple[int, int, int], ...] = ((1, 1, 5), (2, 1, 6), (3, 2, 7))
#: Ten orders, three statuses, two months; orders 1 and 2 share a timestamp.
ORDERS: tuple[tuple[int, str, float, str], ...] = (
    (1, "paid", 10.0, "2024-01-05 10:00:00"), (2, "paid", 20.0, "2024-01-05 10:00:00"),
    (3, "open", 30.0, "2024-01-06 09:00:00"), (4, "paid", 40.0, "2024-01-20 12:00:00"),
    (5, "void", 50.0, "2024-01-21 12:00:00"), (6, "open", 60.0, "2024-02-01 08:00:00"),
    (7, "paid", 70.0, "2024-02-02 08:00:00"), (8, "open", 80.0, "2024-02-03 08:00:00"),
    (9, "void", 90.0, "2024-02-10 08:00:00"), (10, "paid", 100.0, "2024-02-11 08:00:00"),
)
STATUSES = frozenset(s for _, s, _, _ in ORDERS)


def _blob(value: bytes | None) -> str:
    return "NULL" if value is None else "'" + "".join(f"\\x{b:02X}" for b in value) + "'::BLOB"


def _lit(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, list):
        return "[" + ", ".join(_lit(v) for v in value) + "]"
    if isinstance(value, dt.timedelta):
        return f"INTERVAL '{int(value.total_seconds())} seconds'"
    if isinstance(value, (int, Decimal)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _json(value: Any) -> str:
    return "NULL" if value is None else "'" + json.dumps(value) + "'"


def statements() -> list[str]:
    things = ", ".join(
        f"({t['id']}, {t['team_id']}, {_lit(t['name'])}, {_lit(t['tags'])}::VARCHAR[], {_json(t['payload'])}, "
        f"{_lit(t['uid'])}, {_lit(t['price'])}, {_blob(t['blob'])}, {_lit(t['span'])}, {_lit(t['ts'])})"
        for t in THINGS
    )
    return [
        "CREATE TABLE teams (id INTEGER PRIMARY KEY, label VARCHAR)",
        "INSERT INTO teams VALUES (1, 'red'), (2, 'blue')",
        "CREATE TABLE things (id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id), name VARCHAR, "
        "tags VARCHAR[], payload JSON, uid UUID, price DECIMAL(22, 2), blob BLOB, span INTERVAL, ts TIMESTAMP)",
        f"INSERT INTO things VALUES {things}",
        "CREATE TABLE parts (id INTEGER PRIMARY KEY, thing_id INTEGER REFERENCES things(id), qty INTEGER)",
        "INSERT INTO parts VALUES " + ", ".join(str(p) for p in PARTS),
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status VARCHAR, revenue DOUBLE, ordered_at TIMESTAMP)",
        "INSERT INTO orders VALUES " + ", ".join(f"({i}, '{s}', {r}, '{t}')" for i, s, r, t in ORDERS),
    ]


SAVED_MEASURES: dict[str, list[ModelMeasure]] = {
    "orders": [ModelMeasure(name="revenue_total", formula="sum(revenue)")],
    "things": [ModelMeasure(name="uids", formula="collect(uid)"), ModelMeasure(name="prices", formula="collect(price)")],
}


def strict_json(text: str) -> Any:
    """Strict JSON: no NaN / Infinity tokens; floats read as exact Decimals."""
    def refuse(token: str) -> Any:
        raise ValueError(f"invalid JSON constant {token}")
    return json.loads(text, parse_constant=refuse, parse_float=Decimal)


class FacadeStore(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    storage: YAMLStorage
    datasource: DatasourceConfig


async def build_store(base: Path) -> FacadeStore:
    """Seed DuckDB and ingest every table, plus ``SAVED_MEASURES``; ``things`` reaches ``parts`` through the
    reversed ``parts`` edge."""
    db = str(base / "dev2172.duckdb")
    seed_contractors("duckdb", db, extra=statements())
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    storage = YAMLStorage(base_dir=str(base / "store"))
    await storage.save_datasource(ds)
    for model in ingest_datasource(datasource=ds):
        if model.name in SAVED_MEASURES:
            model = model.model_copy(update={"measures": SAVED_MEASURES[model.name]})
        await storage.save_model(model)
    return FacadeStore(storage=storage, datasource=ds)


# --------------------------------------------------------------------------- #
# In-process servers
# --------------------------------------------------------------------------- #
class Servers(BaseModel):
    pg_port: int
    flight_port: int


@contextlib.contextmanager
def serve(store: FacadeStore) -> Generator[Servers]:
    """A Postgres-wire and a Flight server over ``store``, each with its own engine."""
    pg_engine = SlayerQueryEngine(storage=store.storage)
    flight_engine = SlayerQueryEngine(storage=store.storage)
    loop, thread, pg_port = _start_pg(engine=pg_engine, storage=store.storage)
    flight = build_server(host="127.0.0.1", port=0,
                          handlers=FlightHandlers(engine=flight_engine, storage=store.storage))
    flight_thread = threading.Thread(target=flight.serve, daemon=True)
    flight_thread.start()
    try:
        yield Servers(pg_port=pg_port, flight_port=flight.port)
    finally:
        flight.shutdown()
        flight.wait()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
        pg_engine.close()
        flight_engine.close()
        engine_factory.invalidate_engine(store.datasource)


def _start_pg(*, engine: SlayerQueryEngine, storage: YAMLStorage) -> tuple[asyncio.AbstractEventLoop, threading.Thread, int]:
    holder: dict[str, Any] = {}
    ready = threading.Event()

    def main() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        holder["loop"] = loop

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            conn = PgConnection(reader, writer, engine=engine, storage=storage, token=None, tls_ctx=None)
            try:
                await conn.run()
            finally:
                writer.close()

        async def start() -> asyncio.Server:
            server = await asyncio.start_server(handle, host="127.0.0.1", port=0)
            holder["port"] = server.sockets[0].getsockname()[1]
            ready.set()
            return server

        server = loop.run_until_complete(start())
        try:
            loop.run_forever()
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()

    thread = threading.Thread(target=main, daemon=True)
    thread.start()
    if not ready.wait(timeout=10):
        raise RuntimeError("pg facade failed to start")
    return holder["loop"], thread, holder["port"]


# --------------------------------------------------------------------------- #
# Clients
# --------------------------------------------------------------------------- #
class PgResult(BaseModel):
    columns: list[str]
    type_codes: list[int]
    rows: list[tuple[Any, ...]]


def pg_query(port: int, sql: str, *, dbname: str = DS) -> PgResult:
    with psycopg.connect(host="127.0.0.1", port=port, user="u", dbname=dbname, autocommit=True) as conn:
        cur = conn.execute(cast(LiteralString, sql))
        desc = cur.description or []
        return PgResult(columns=[d.name for d in desc], type_codes=[d.type_code for d in desc],
                        rows=list(cur.fetchall()))


def _descriptor(msg: Any, suffix: str) -> fl.FlightDescriptor:  # pyright: ignore[reportPrivateImportUsage]
    wrapped = PbAny()
    wrapped.type_url = f"type.googleapis.com/arrow.flight.protocol.sql.{suffix}"
    wrapped.value = msg.SerializeToString()
    return fl.FlightDescriptor.for_command(wrapped.SerializeToString())  # pyright: ignore[reportPrivateImportUsage]


def flight_query(port: int, sql: str) -> pa.Table:
    client = fl.FlightClient(f"grpc://127.0.0.1:{port}")  # pyright: ignore[reportPrivateImportUsage]
    try:
        cmd = fsql_pb.CommandStatementQuery()  # pyright: ignore[reportAttributeAccessIssue]
        cmd.query = sql
        info = client.get_flight_info(_descriptor(cmd, "CommandStatementQuery"))
        return client.do_get(info.endpoints[0].ticket).read_all()
    finally:
        client.close()


async def mcp_text(server: Any, query: Any, *, fmt: str) -> str:
    """One MCP ``query`` tool call's text output."""
    blocks, _ = await server.call_tool(name="query", arguments={"query": query, "format": fmt})
    return blocks[0].text


def mcp_rows(text: str) -> list[dict[str, Any]]:
    """The data rows of MCP json output, read strictly."""
    payload = strict_json(text)
    return payload["data"] if isinstance(payload, dict) else payload


def flight_table_schemas(port: int) -> dict[str, pa.Schema]:
    """``GetTables(include_schema=True)``: table name → its advertised schema (empty when none is sent)."""
    client = fl.FlightClient(f"grpc://127.0.0.1:{port}")  # pyright: ignore[reportPrivateImportUsage]
    try:
        cmd = fsql_pb.CommandGetTables()  # pyright: ignore[reportAttributeAccessIssue]
        cmd.include_schema = True
        info = client.get_flight_info(_descriptor(cmd, "CommandGetTables"))
        table = client.do_get(info.endpoints[0].ticket).read_all()
    finally:
        client.close()
    if "table_schema" not in table.column_names:
        return {}
    return {r["table_name"]: pa.ipc.read_schema(pa.py_buffer(r["table_schema"])) for r in table.to_pylist()}


def engine_query(store: FacadeStore, body: dict[str, Any]) -> Any:
    """One query through a fresh engine (closed afterwards)."""
    engine = SlayerQueryEngine(storage=store.storage)
    try:
        return run_sync(engine.execute(body))
    finally:
        engine.close()
