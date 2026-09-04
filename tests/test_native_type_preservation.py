"""Unit tests for exact-numeric detection and native-type preservation."""

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, SlayerModel
from slayer.engine.introspect_utils import is_exact_numeric_db_type
from slayer.engine.prebound import measure_key_preserves_native_type


class TestIsExactNumericDbType:
    @pytest.mark.parametrize(
        "db_type",
        [
            "DECIMAL(18,2)",
            "DECIMAL",
            "NUMERIC",
            "NUMERIC(10, 0)",
            "Decimal64(4)",
            "Decimal(18, 2)",
            "BIGNUMERIC",
            "decimal(18,2)",
            "bignumeric",
        ],
    )
    def test_exact_numeric_types_match(self, db_type):
        assert is_exact_numeric_db_type(db_type) is True

    @pytest.mark.parametrize(
        "db_type",
        [
            None,
            "",
            "DOUBLE",
            "INT",
            "MONEY",
            "SMALLMONEY",
            "TEXT",
            # Wrapped strings deliberately do NOT match — unwrapping is the
            # caller's job; the helper stays a pure string predicate.
            "Nullable(Decimal(18, 2))",
            "LowCardinality(Nullable(Decimal(10, 4)))",
        ],
    )
    def test_non_exact_numeric_types_do_not_match(self, db_type):
        assert is_exact_numeric_db_type(db_type) is False


def _model_with_amount(db_type):
    return SlayerModel(
        name="orders",
        data_source="ds",
        sql_table="orders",
        columns=[Column(name="amount", type=DataType.DOUBLE, db_type=db_type)],
    )


_SUM_AMOUNT = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")


class TestMeasureKeyPreservesNativeType:
    @pytest.mark.parametrize(
        "db_type",
        ["DECIMAL(18,2)", "NUMERIC", "Decimal(18, 2)", "Decimal64(4)", "BIGNUMERIC"],
    )
    def test_exact_numeric_db_type_preserves(self, db_type):
        model = _model_with_amount(db_type)
        assert measure_key_preserves_native_type(model=model, key=_SUM_AMOUNT) is True

    @pytest.mark.parametrize(
        "db_type",
        [None, "Nullable(Decimal(18, 2))", "MONEY", "DOUBLE", "JSON"],
    )
    def test_other_db_types_do_not_preserve(self, db_type):
        model = _model_with_amount(db_type)
        assert measure_key_preserves_native_type(model=model, key=_SUM_AMOUNT) is False
