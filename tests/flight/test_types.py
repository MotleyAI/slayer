"""Tests for slayer.flight.types — DataType ↔ Arrow ↔ JDBC round-trips."""

from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

from slayer.core.enums import DataType
from slayer.flight.types import (
    SUPPORTED_DATATYPES,
    arrow_to_datatype,
    datatype_to_arrow,
    datatype_to_jdbc,
)


@pytest.mark.parametrize(
    "dt,arrow_type,jdbc_name",
    [
        (DataType.TEXT, pa.utf8(), "VARCHAR"),
        (DataType.INT, pa.int64(), "BIGINT"),
        (DataType.DOUBLE, pa.float64(), "DOUBLE"),
        (DataType.BOOLEAN, pa.bool_(), "BOOLEAN"),
        (DataType.DATE, pa.date32(), "DATE"),
        (DataType.TIMESTAMP, pa.timestamp("us"), "TIMESTAMP"),
    ],
)
def test_datatype_forward_map(dt: DataType, arrow_type: pa.DataType, jdbc_name: str) -> None:
    assert datatype_to_arrow(dt) == arrow_type
    assert datatype_to_jdbc(dt) == jdbc_name


def test_supported_datatypes_covers_every_enum_value() -> None:
    assert set(SUPPORTED_DATATYPES) == set(DataType)


@pytest.mark.parametrize(
    "arrow_type,expected",
    [
        (pa.utf8(), DataType.TEXT),
        (pa.large_string(), DataType.TEXT),
        (pa.int8(), DataType.INT),
        (pa.int16(), DataType.INT),
        (pa.int32(), DataType.INT),
        (pa.int64(), DataType.INT),
        (pa.uint8(), DataType.INT),
        (pa.uint16(), DataType.INT),
        (pa.uint32(), DataType.INT),
        (pa.uint64(), DataType.INT),
        (pa.float16(), DataType.DOUBLE),
        (pa.float32(), DataType.DOUBLE),
        (pa.float64(), DataType.DOUBLE),
        (pa.decimal128(precision=18, scale=4), DataType.DOUBLE),
        (pa.bool_(), DataType.BOOLEAN),
        (pa.date32(), DataType.DATE),
        (pa.date64(), DataType.DATE),
        (pa.timestamp("s"), DataType.TIMESTAMP),
        (pa.timestamp("ms"), DataType.TIMESTAMP),
        (pa.timestamp("us"), DataType.TIMESTAMP),
        (pa.timestamp("ns"), DataType.TIMESTAMP),
    ],
)
def test_arrow_to_datatype_collapses_widths(arrow_type: pa.DataType, expected: DataType) -> None:
    assert arrow_to_datatype(arrow_type) == expected


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.list_(pa.int64()),
        pa.struct([("a", pa.int64())]),
        pa.binary(),
        pa.null(),
    ],
)
def test_arrow_to_datatype_returns_none_for_unmappable(arrow_type: pa.DataType) -> None:
    assert arrow_to_datatype(arrow_type) is None


def test_forward_then_reverse_round_trip() -> None:
    """For every SLayer DataType, forward-map to Arrow then reverse-map back."""
    for dt in DataType:
        round_tripped = arrow_to_datatype(datatype_to_arrow(dt))
        assert round_tripped == dt, f"{dt} did not round-trip cleanly"


def test_pa_table_from_pylist_with_explicit_schema_preserves_null_cells() -> None:
    """Per §6.4: pa.Table.from_pylist(data, schema=<explicit>) must keep None.

    Without an explicit schema, inferred-from-data would type a None-only
    column as null; with the explicit schema the column stays typed.
    """
    schema = pa.schema(
        [
            pa.field("name", datatype_to_arrow(DataType.TEXT)),
            pa.field("count", datatype_to_arrow(DataType.INT)),
            pa.field("price", datatype_to_arrow(DataType.DOUBLE)),
            pa.field("flag", datatype_to_arrow(DataType.BOOLEAN)),
            pa.field("d", datatype_to_arrow(DataType.DATE)),
            pa.field("ts", datatype_to_arrow(DataType.TIMESTAMP)),
        ]
    )
    rows = [
        {
            "name": "alpha",
            "count": 1,
            "price": 1.5,
            "flag": True,
            "d": datetime.date(2025, 1, 1),
            "ts": datetime.datetime(2025, 1, 1, 12, 0, 0),
        },
        {"name": None, "count": None, "price": None, "flag": None, "d": None, "ts": None},
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    assert table.schema == schema
    assert table.num_rows == 2
    # Column-by-column: the None cell must round-trip as null.
    for col in schema.names:
        values = table.column(col).to_pylist()
        assert values[1] is None, f"{col!r} second row should be None, got {values[1]!r}"


def test_pa_table_from_pylist_rejects_decimal_into_double() -> None:
    """pa.Table.from_pylist does **not** silently coerce Decimal into float64
    when the explicit schema asks for DOUBLE. Pins a contract the server
    handler must satisfy: when ``SlayerResponse.data`` carries Decimal cells
    (DuckDB / Postgres / SQLite native), the server pre-coerces to float
    before calling from_pylist (or uses ``pa.array(coerced_list, type=...)``
    column-by-column). Without that shim, the Arrow build raises
    ``ArrowInvalid``."""
    schema = pa.schema([pa.field("v", datatype_to_arrow(DataType.DOUBLE))])
    with pytest.raises(pa.ArrowInvalid):
        pa.Table.from_pylist([{"v": decimal.Decimal("3.14")}], schema=schema)
    # And the documented shim — pre-coerce Decimal → float — works:
    rows = [{"v": float(decimal.Decimal("3.14"))}]
    table = pa.Table.from_pylist(rows, schema=schema)
    assert table.column("v").to_pylist() == [3.14]
