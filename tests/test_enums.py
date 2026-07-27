"""Tests for slayer.core.enums.DataType — sqlglot-aligned shape (DEV-1361)."""

import pytest
from sqlglot import exp

from slayer.core.enums import (
    DEFAULT_AGGREGATIONS_BY_TYPE,
    DataType,
)


class TestDataTypeShape:
    """The new enum has exactly TEXT, INT, DOUBLE, BOOLEAN, DATE, TIMESTAMP."""

    def test_exact_member_set(self) -> None:
        assert {m.name for m in DataType} == {
            "TEXT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP",
        }

    def test_no_legacy_string_member(self) -> None:
        assert not hasattr(DataType, "STRING")

    def test_no_legacy_number_member(self) -> None:
        assert not hasattr(DataType, "NUMBER")

    def test_no_legacy_integer_member(self) -> None:
        # Old name was INTEGER (we considered it briefly). Final canonical name is INT.
        assert not hasattr(DataType, "INTEGER")

    @pytest.mark.parametrize("legacy", ["COUNT", "COUNT_DISTINCT", "SUM", "AVERAGE", "MIN", "MAX", "LAST"])
    def test_aggregation_pseudo_types_dropped(self, legacy: str) -> None:
        assert not hasattr(DataType, legacy)

    def test_is_aggregation_helper_removed(self) -> None:
        # The helper had no callers outside the enum itself; gone in v5.
        assert not hasattr(DataType.TEXT, "is_aggregation")

    def test_python_type_helper_removed(self) -> None:
        # Was used only in a single test assertion; gone in v5.
        assert not hasattr(DataType.TEXT, "python_type")


class TestDataTypeValuesMatchSqlglot:
    """Every enum value is byte-equal to the matching sqlglot exp.DataType.Type value
    so _datatype_to_sqlglot collapses to identity."""

    @pytest.mark.parametrize(
        "name",
        ["TEXT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"],
    )
    def test_value_equals_sqlglot_name(self, name: str) -> None:
        assert getattr(DataType, name).value == name
        assert exp.DataType.Type(getattr(DataType, name).value).name == name


class TestDefaultAggregationsByType:
    def test_int_mirrors_double(self) -> None:
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INT] == DEFAULT_AGGREGATIONS_BY_TYPE[DataType.DOUBLE]

    def test_int_includes_sum_and_avg(self) -> None:
        assert "sum" in DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INT]
        assert "avg" in DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INT]

    def test_text_set_unchanged(self) -> None:
        # Mirror of the old STRING set, plus DEV-1595's count_distinct_approx
        # (eligible wherever count_distinct is — see DEFAULT_AGGREGATIONS_BY_TYPE).
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.TEXT] == frozenset({
            "count", "count_distinct", "count_distinct_approx",
            "first", "last", "min", "max",
        })

    def test_no_pseudo_type_keys(self) -> None:
        # No leftover entries keyed by the dropped pseudo-types.
        for key in DEFAULT_AGGREGATIONS_BY_TYPE.keys():
            assert isinstance(key, DataType)
            assert key.name in {"TEXT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"}
