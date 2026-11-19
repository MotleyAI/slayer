"""Tests for slayer.core.enums.DataType (Ossie's datatype names) and its engine ``TypeKind``."""

import pytest
from sqlglot import exp

from slayer.core.enums import (
    DEFAULT_AGGREGATIONS_BY_TYPE,
    DataType,
    TypeKind,
)

OSSIE_VALUES = {
    "STRING": "String", "INTEGER": "Integer", "DECIMAL": "Decimal", "FLOAT": "Float", "BOOLEAN": "Boolean",
    "DATE": "Date", "TIME": "Time", "DATETIME": "DateTime", "DATETIME_TZ": "DateTimeTz", "OPAQUE": "Opaque",
    "ARRAY": "Array",
}


class TestDataTypeShape:
    """The enum holds Ossie's ten datatype names plus ``Array``."""

    def test_exact_member_set(self) -> None:
        assert {m.name for m in DataType} == set(OSSIE_VALUES)

    @pytest.mark.parametrize("name", sorted(OSSIE_VALUES))
    def test_value_is_the_ossie_name(self, name: str) -> None:
        assert getattr(DataType, name).value == OSSIE_VALUES[name]

    def test_no_legacy_number_member(self) -> None:
        assert not hasattr(DataType, "NUMBER")

    @pytest.mark.parametrize("legacy", ["COUNT", "COUNT_DISTINCT", "SUM", "AVERAGE", "MIN", "MAX", "LAST"])
    def test_aggregation_pseudo_types_dropped(self, legacy: str) -> None:
        assert not hasattr(DataType, legacy)

    def test_is_aggregation_helper_removed(self) -> None:
        # The helper had no callers outside the enum itself; gone in v5.
        assert not hasattr(DataType.STRING, "is_aggregation")

    def test_python_type_helper_removed(self) -> None:
        # Was used only in a single test assertion; gone in v5.
        assert not hasattr(DataType.STRING, "python_type")


class TestTypeKindValuesMatchSqlglot:
    """Every engine kind's value is byte-equal to the matching sqlglot exp.DataType.Type value."""

    @pytest.mark.parametrize(
        "name",
        ["TEXT", "INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "UNKNOWN", "ARRAY"],
    )
    def test_value_equals_sqlglot_name(self, name: str) -> None:
        assert getattr(TypeKind, name).value == name
        assert exp.DataType.Type(getattr(TypeKind, name).value).name == name


class TestDataTypeIsOpaque:
    """``is_opaque`` is the single semantic hook for "stored + displayed, but
    not operable on" — consumers must not compare against UNKNOWN directly."""

    def test_unknown_is_opaque(self) -> None:
        assert DataType.OPAQUE.is_opaque is True

    @pytest.mark.parametrize(
        "dt",
        [m for m in DataType if m not in (DataType.OPAQUE, DataType.TIME)],
        ids=lambda m: m.name,
    )
    def test_every_other_member_is_not_opaque(self, dt: DataType) -> None:
        assert dt.is_opaque is False

    def test_unknown_has_no_default_aggregations(self) -> None:
        # An opaque column can't be aggregated at all, so it must not appear
        # in the type-default eligibility table.
        assert DataType.OPAQUE not in DEFAULT_AGGREGATIONS_BY_TYPE


class TestDefaultAggregationsByType:
    def test_int_mirrors_double(self) -> None:
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INTEGER] == DEFAULT_AGGREGATIONS_BY_TYPE[DataType.FLOAT]

    def test_int_includes_sum_and_avg(self) -> None:
        assert "sum" in DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INTEGER]
        assert "avg" in DEFAULT_AGGREGATIONS_BY_TYPE[DataType.INTEGER]

    def test_text_set_unchanged(self) -> None:
        # Mirror of the old STRING set, plus DEV-1595's count_distinct_approx
        # (eligible wherever count_distinct is — see DEFAULT_AGGREGATIONS_BY_TYPE).
        assert DEFAULT_AGGREGATIONS_BY_TYPE[DataType.STRING] == frozenset({
            "count", "count_distinct", "count_distinct_approx",
            "first", "last", "min", "max", "collect",
        })

    def test_no_pseudo_type_keys(self) -> None:
        # No leftover entries keyed by the dropped pseudo-types.
        for key in DEFAULT_AGGREGATIONS_BY_TYPE.keys():
            assert isinstance(key, DataType)
            assert key.name in {
                "STRING", "INTEGER", "DECIMAL", "FLOAT", "BOOLEAN", "DATE", "DATETIME", "DATETIME_TZ", "ARRAY"}
