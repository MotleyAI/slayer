"""Each value type read back by real clients — psycopg over the Postgres wire, pyarrow over Arrow Flight — and
``collect`` over a UUID column (spec: results/value-encoding, facade/list-values)."""

from __future__ import annotations

import asyncio
import csv
import io
import datetime as dt
import json
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import Any, LiteralString, cast

import psycopg
import pytest

from slayer.mcp.server import create_mcp_server
from tests._dev2172_fixtures import (
    BIG,
    DS,
    THINGS,
    UID_1,
    UID_3,
    FacadeStore,
    Servers,
    build_store,
    flight_query,
    mcp_rows,
    mcp_text,
    pg_query,
    serve,
    strict_json,
)


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> FacadeStore:
    return asyncio.run(build_store(tmp_path_factory.mktemp("dev2172-wire-values")))


@pytest.fixture(scope="module")
def servers(store: FacadeStore) -> Iterator[Servers]:
    with serve(store) as running:
        yield running


def _json_text(text: str | None) -> Any:
    return None if text is None else strict_json(text)


#: Column → ``(read the cell back, the expected value per thing id)``.
TEXT_COLUMNS: dict[str, tuple[Callable[[Any], Any], dict[int, Any]]] = {
    "tags": (_json_text, {t["id"]: t["tags"] for t in THINGS}),
    "payload": (_json_text, {t["id"]: t["payload"] for t in THINGS}),
    "uid": (lambda v: v, {t["id"]: None if t["uid"] is None else str(t["uid"]) for t in THINGS}),
    "blob": (lambda v: v, {1: "\\xdeadbeef", 2: None, 3: "\\x00"}),
    "span": (lambda v: v, {1: "P1DT2H", 2: None, 3: "-PT1H30M"}),
}


def _pg_binary(port: int, sql: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(host="127.0.0.1", port=port, user="u", dbname=DS, autocommit=True) as conn:
        return list(conn.execute(cast(LiteralString, sql), binary=True).fetchall())


def _read(wire: str, servers: Servers, sql: str) -> list[tuple[Any, ...]]:
    if wire == "pg-text":
        return pg_query(servers.pg_port, sql).rows
    if wire == "pg-binary":
        return _pg_binary(servers.pg_port, sql)
    return [tuple(r.values()) for r in flight_query(servers.flight_port, sql).to_pylist()]


WIRES = ("pg-text", "pg-binary", "flight")


def _single_cell(text: str, *, fmt: str) -> str:
    """The one data cell of a one-column, one-row MCP csv / markdown output."""
    lines = [line for line in text.splitlines() if line and not line.startswith("#")]
    if fmt == "csv":
        return list(csv.reader(io.StringIO("\n".join(lines))))[1][0]
    row = lines[2]
    assert row.startswith("| ") and row.endswith(" |"), row
    return row[2:-2]
UIDS = {"source_model": "things", "measures": ["uids"]}


@pytest.mark.parametrize("wire", WIRES)
@pytest.mark.parametrize("column", TEXT_COLUMNS)
def test_text_valued_column_round_trip(servers: Servers, wire: str, column: str) -> None:
    read, expected = TEXT_COLUMNS[column]
    rows = _read(wire, servers, f'SELECT id, "{column}" FROM things')
    assert {r[0]: read(r[1]) for r in rows} == expected


@pytest.mark.parametrize("wire", WIRES)
def test_decimal_and_timestamp_columns_keep_their_native_types(servers: Servers, wire: str) -> None:
    rows = {r[0]: r[1:] for r in _read(wire, servers, "SELECT id, price, ts FROM things")}
    for thing in THINGS:
        price, ts = rows[thing["id"]]
        assert price == (None if thing["price"] is None else pytest.approx(float(thing["price"])))
        assert ts == thing["ts"]
        assert ts is None or isinstance(ts, dt.datetime)


#: ``collect``'s empty-list fallback is typed from the declared TEXT, not the column's uuid.
COLLECT_UUID_XFAIL = pytest.mark.xfail(strict=True, reason="DEV-2169 types collect's empty-list fallback from the column")


@COLLECT_UUID_XFAIL
class TestCollectOverUuid:
    @pytest.mark.parametrize("wire", WIRES)
    def test_over_the_wire_it_is_json_array_text(self, servers: Servers, wire: str) -> None:
        ((text,),) = _read(wire, servers, "SELECT uids FROM things")
        assert isinstance(text, str), text
        assert sorted(strict_json(text)) == sorted([str(UID_1), str(UID_3)])

    async def test_mcp_json_returns_an_array_of_uuid_strings(self, store: FacadeStore) -> None:
        text = await mcp_text(create_mcp_server(storage=store.storage), UIDS, fmt="json")
        assert sorted(mcp_rows(text)[0]["things.uids"]) == sorted([str(UID_1), str(UID_3)])

    @pytest.mark.parametrize("fmt", ["csv", "markdown"])
    async def test_mcp_text_formats_carry_the_json_text(self, store: FacadeStore, fmt: str) -> None:
        text = await mcp_text(create_mcp_server(storage=store.storage), UIDS, fmt=fmt)
        assert sorted(strict_json(_single_cell(text, fmt=fmt))) == sorted([str(UID_1), str(UID_3)])


class TestCollectOverExactDecimals:
    async def test_mcp_json_keeps_every_digit(self, store: FacadeStore) -> None:
        text = await mcp_text(create_mcp_server(storage=store.storage),
                              {"source_model": "things", "measures": ["prices"]}, fmt="json")
        assert str(BIG) in text and f'"{BIG}"' not in text
        assert sorted(mcp_rows(text)[0]["things.prices"]) == sorted([BIG, Decimal("0.50")])

    @pytest.mark.parametrize("wire", WIRES)
    def test_over_the_wire_the_list_text_keeps_every_digit(self, servers: Servers, wire: str) -> None:
        ((text,),) = _read(wire, servers, "SELECT prices FROM things")
        assert str(BIG) in text
        assert sorted(strict_json(text)) == sorted([BIG, Decimal("0.50")])


def test_json_object_column_is_never_a_python_repr(servers: Servers) -> None:
    rows = pg_query(servers.pg_port, "SELECT id, payload FROM things").rows
    text = dict(rows)[1]
    assert "'" not in text
    assert json.loads(text) == {"a": [1, "x"]}
