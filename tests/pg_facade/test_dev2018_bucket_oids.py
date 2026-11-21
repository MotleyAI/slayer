"""A facade bucket projection advertises the bucket's type, and its executed value encodes under that OID."""

from __future__ import annotations

import struct
from datetime import date, datetime

import pytest

from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.pg_facade import types as t
from slayer.pg_facade.protocol import OID_DATE, OID_TIMESTAMP

from tests._dev2018_fixtures import all_datasets, lt_engine

_PG_EPOCH = datetime(2000, 1, 1)


def _translate(sql: str) -> QueryResult:
    result = translate(sql=sql, catalog=build_catalog(datasets_by_datasource={"test": all_datasets()}))
    assert isinstance(result, QueryResult)
    return result


@pytest.mark.parametrize(("unit", "column", "oid"), [
    ("month", "d", OID_DATE),
    ("hour", "d", OID_TIMESTAMP),
    ("month", "ts", OID_TIMESTAMP),
    ("day", "ts", OID_TIMESTAMP),
])
def test_bucket_projection_oid(unit, column, oid) -> None:
    result = _translate(f"SELECT date_trunc('{unit}', {column}) AS b FROM bk")
    assert t.datatype_to_oid(result.projection_types[0]) == oid


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
@pytest.mark.parametrize(("unit", "oid", "text", "binary"), [
    ("month", OID_DATE, b"2024-06-01", struct.pack(">i", (date(2024, 6, 1) - _PG_EPOCH.date()).days)),
    ("hour", OID_TIMESTAMP, b"2024-06-15 00:00:00",
     struct.pack(">q", int((datetime(2024, 6, 15) - _PG_EPOCH).total_seconds()) * 1_000_000)),
], ids=["month", "hour"])
async def test_executed_bucket_encodes_under_its_oid(backend, unit, oid, text, binary) -> None:
    result = _translate(f"SELECT id, date_trunc('{unit}', d) AS b FROM bk WHERE id = 1")
    out_oid = t.datatype_to_oid(result.projection_types[1])
    assert out_oid == oid
    async with lt_engine(backend) as eng:
        resp = await eng.execute(result.query)
    [row] = resp.data
    value = next(v for k, v in row.items() if not k.endswith(".id"))
    assert t.value_to_text(value, out_oid) == text
    assert t.value_to_binary(value, out_oid) == binary
