"""ClickHouse-specific type-mapping tests for ingestion.

Skipped when the `clickhouse` extra is not installed (clickhouse_sqlalchemy
absent). Existing dialect-agnostic tests live in tests/test_ingestion.py and
remain unconditional.
"""

import logging
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.engine import ingestion
from slayer.engine.ingestion import (
    _introspect_query_columns_via_inspector,
    _sa_type_is_exact_numeric,
    _sa_type_is_float,
    _sa_type_to_data_type,
    _unwrap_clickhouse_wrappers,
)

ch_types = pytest.importorskip("clickhouse_sqlalchemy.types")


@pytest.fixture(autouse=True)
def _reset_warning_dedup():
    """Ensure each test sees a clean warning dedup set."""
    ingestion._logged_unmapped_sa_types.clear()
    yield
    ingestion._logged_unmapped_sa_types.clear()


class TestClickHouseIntTypes:
    @pytest.mark.parametrize(
        "sa_type_cls",
        [
            ch_types.Int8,
            ch_types.Int16,
            ch_types.Int32,
            ch_types.Int64,
            ch_types.Int128,
            ch_types.Int256,
            ch_types.UInt8,
            ch_types.UInt16,
            ch_types.UInt32,
            ch_types.UInt64,
            ch_types.UInt128,
            ch_types.UInt256,
        ],
    )
    def test_int_maps_to_number_not_float(self, sa_type_cls):
        sa_type = sa_type_cls()
        # DEV-1361: integer family now narrows to DataType.INT.
        assert _sa_type_to_data_type(sa_type) is DataType.INT
        assert _sa_type_is_float(sa_type) is False


class TestClickHouseFloatTypes:
    @pytest.mark.parametrize("sa_type_cls", [ch_types.Float32, ch_types.Float64])
    def test_float_maps_to_number_and_is_float(self, sa_type_cls):
        sa_type = sa_type_cls()
        assert _sa_type_to_data_type(sa_type) is DataType.DOUBLE
        assert _sa_type_is_float(sa_type) is True


class TestClickHouseDecimalScaleAware:
    @pytest.mark.parametrize(
        "scale,expect_float",
        [
            (2, True),
            (0, False),
        ],
    )
    def test_decimal_scale_decides_float(self, scale, expect_float):
        sa_type = ch_types.Decimal(10, scale)
        # DEV-1361: scale=0 → INT, scale>0 → DOUBLE.
        expected = DataType.DOUBLE if expect_float else DataType.INT
        assert _sa_type_to_data_type(sa_type) is expected
        assert _sa_type_is_float(sa_type) is expect_float


class TestClickHouseDateTimeTypes:
    def test_datetime_maps_to_timestamp(self):
        assert _sa_type_to_data_type(ch_types.DateTime()) is DataType.TIMESTAMP

    def test_datetime64_maps_to_timestamp(self):
        assert _sa_type_to_data_type(ch_types.DateTime64(3)) is DataType.TIMESTAMP

    def test_date_maps_to_date(self):
        assert _sa_type_to_data_type(ch_types.Date()) is DataType.DATE

    def test_date32_maps_to_date(self):
        assert _sa_type_to_data_type(ch_types.Date32()) is DataType.DATE


class TestClickHouseNullableUnwrap:
    def test_nullable_int(self):
        sa_type = ch_types.Nullable(ch_types.Int32())
        assert _sa_type_to_data_type(sa_type) is DataType.INT
        assert _sa_type_is_float(sa_type) is False

    def test_nullable_float(self):
        sa_type = ch_types.Nullable(ch_types.Float64())
        assert _sa_type_to_data_type(sa_type) is DataType.DOUBLE
        assert _sa_type_is_float(sa_type) is True

    def test_nullable_decimal_float(self):
        sa_type = ch_types.Nullable(ch_types.Decimal(10, 2))
        assert _sa_type_to_data_type(sa_type) is DataType.DOUBLE
        assert _sa_type_is_float(sa_type) is True

    def test_nullable_decimal_integer(self):
        sa_type = ch_types.Nullable(ch_types.Decimal(10, 0))
        # DEV-1361: scale=0 narrows to INT.
        assert _sa_type_to_data_type(sa_type) is DataType.INT
        assert _sa_type_is_float(sa_type) is False


class TestClickHouseLowCardinalityUnwrap:
    def test_low_cardinality_string(self):
        sa_type = ch_types.LowCardinality(ch_types.String())
        assert _sa_type_to_data_type(sa_type) is DataType.TEXT

    def test_low_cardinality_int(self):
        sa_type = ch_types.LowCardinality(ch_types.Int32())
        assert _sa_type_to_data_type(sa_type) is DataType.INT
        assert _sa_type_is_float(sa_type) is False


class TestClickHouseNestedWrappers:
    def test_low_cardinality_nullable_string(self):
        sa_type = ch_types.LowCardinality(ch_types.Nullable(ch_types.String()))
        assert _sa_type_to_data_type(sa_type) is DataType.TEXT

    def test_nullable_low_cardinality_int(self):
        sa_type = ch_types.Nullable(ch_types.LowCardinality(ch_types.Int32()))
        assert _sa_type_to_data_type(sa_type) is DataType.INT
        assert _sa_type_is_float(sa_type) is False


class TestUnwrapHelper:
    def test_unwrap_nullable_returns_inner(self):
        inner = ch_types.Int32()
        unwrapped = _unwrap_clickhouse_wrappers(ch_types.Nullable(inner))
        assert type(unwrapped).__name__ == "Int32"

    def test_unwrap_low_cardinality_returns_inner(self):
        inner = ch_types.Int32()
        unwrapped = _unwrap_clickhouse_wrappers(ch_types.LowCardinality(inner))
        assert type(unwrapped).__name__ == "Int32"

    def test_unwrap_nested_returns_innermost(self):
        unwrapped = _unwrap_clickhouse_wrappers(
            ch_types.LowCardinality(ch_types.Nullable(ch_types.String()))
        )
        assert type(unwrapped).__name__ == "String"

    def test_unwrap_non_wrapper_returns_self(self):
        sa_type = ch_types.Int32()
        assert _unwrap_clickhouse_wrappers(sa_type) is sa_type


class TestClickHouseExactNumericDetection:
    @pytest.mark.parametrize(
        "sa_type",
        [
            ch_types.Decimal(18, 2),
            ch_types.Nullable(ch_types.Decimal(18, 2)),
            ch_types.LowCardinality(ch_types.Nullable(ch_types.Decimal(10, 4))),
        ],
    )
    def test_wrapped_decimal_is_exact_numeric(self, sa_type):
        assert _sa_type_is_exact_numeric(sa_type) is True

    @pytest.mark.parametrize(
        "sa_type",
        [
            ch_types.Float64(),
            ch_types.Nullable(ch_types.Float64()),
            ch_types.Nullable(ch_types.Int32()),
        ],
    )
    def test_non_decimal_is_not_exact_numeric(self, sa_type):
        assert _sa_type_is_exact_numeric(sa_type) is False


def _introspect_columns(columns, referenced_tables=()):
    inspector = MagicMock()
    inspector.get_columns.return_value = columns
    inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    results = _introspect_query_columns_via_inspector(
        sa_engine=MagicMock(),
        inspector=inspector,
        table_name="t",
        ref=None,
        rollup_sql=None,
        referenced_tables=set(referenced_tables),
        fk_columns_by_table={},
    )
    return {c.name: c for c in results}


class TestWrappedDbTypeCapture:
    """Inspector-path db_type must store the bare inner type, no wrapper text."""

    def test_wrapped_decimal_stores_bare_inner_string(self):
        by_name = _introspect_columns([
            {"name": "amount", "type": ch_types.Nullable(ch_types.Decimal(18, 2))},
            {"name": "rate", "type": ch_types.LowCardinality(
                ch_types.Nullable(ch_types.Decimal(10, 4))
            )},
        ])
        assert by_name["amount"].type is DataType.DOUBLE
        assert by_name["amount"].db_type == "Decimal(18, 2)"
        assert by_name["rate"].type is DataType.DOUBLE
        assert by_name["rate"].db_type == "Decimal(10, 4)"

    def test_wrapped_opaque_stores_unwrapped_string(self):
        by_name = _introspect_columns([
            {"name": "payload", "type": ch_types.Nullable(sa.JSON())},
        ])
        assert by_name["payload"].type is DataType.UNKNOWN
        assert by_name["payload"].db_type == "JSON"

    def test_wrapped_decimal_on_referenced_table_stores_bare_inner_string(self):
        by_name = _introspect_columns(
            [{"name": "amount", "type": ch_types.Nullable(ch_types.Decimal(18, 2))}],
            referenced_tables={"refs"},
        )
        assert by_name["refs.amount"].type is DataType.DOUBLE
        assert by_name["refs.amount"].db_type == "Decimal(18, 2)"


class _FakeUnknownType(sa.types.TypeEngine):
    """Stand-in for an unrecognized SA type. Inherits TypeEngine so the
    function-signature contract is satisfied for static analysers without
    needing per-call type: ignore.
    """

    def __str__(self) -> str:
        return "FakeUnknownType()"


class _AnotherUnknownType(sa.types.TypeEngine):
    def __str__(self) -> str:
        return "AnotherUnknownType(123)"


class TestUnmappedTypeWarning:
    def test_warning_fires_once_per_type_name(self, caplog):
        with caplog.at_level(logging.WARNING, logger="slayer.engine.ingestion"):
            for _ in range(3):
                # Unrecognized types stay TEXT: most unmapped types are
                # comparable. Only the known non-comparable ones are opaque.
                result = _sa_type_to_data_type(_FakeUnknownType())
                assert result is DataType.TEXT

        relevant = [
            r for r in caplog.records if "FAKEUNKNOWNTYPE" in r.getMessage().upper()
        ]
        assert len(relevant) == 1

    def test_warning_message_contains_class_name_and_str(self, caplog):
        with caplog.at_level(logging.WARNING, logger="slayer.engine.ingestion"):
            _sa_type_to_data_type(_AnotherUnknownType())

        relevant = [
            r
            for r in caplog.records
            if "ANOTHERUNKNOWNTYPE" in r.getMessage().upper()
        ]
        assert len(relevant) == 1
        msg = relevant[0].getMessage()
        assert "ANOTHERUNKNOWNTYPE" in msg.upper()
        assert "AnotherUnknownType(123)" in msg

    def test_warning_dedup_isolated_between_distinct_types(self, caplog):
        with caplog.at_level(logging.WARNING, logger="slayer.engine.ingestion"):
            _sa_type_to_data_type(_FakeUnknownType())
            _sa_type_to_data_type(_AnotherUnknownType())

        msgs = [r.getMessage().upper() for r in caplog.records]
        assert any("FAKEUNKNOWNTYPE" in m for m in msgs)
        assert any("ANOTHERUNKNOWNTYPE" in m for m in msgs)
