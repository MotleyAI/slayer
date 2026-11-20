"""DEV-1883 — granularity values are reserved custom-aggregation names.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/models/aggregation-names.
"""
import pydantic
import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, DatasetField
from tests import _dev1883_fixtures as fx

GRANULARITIES = [g.value for g in TimeGranularity]


class TestGranularityNamesReserved:
    @pytest.mark.parametrize("name", ["month", "Month", "MONTH", "WEEK_SUNDAY", "year"])
    def test_aggregation_named_like_granularity_rejected(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError) as ei:
            Aggregation(name=name, expression="SUM({x})")
        msg = str(ei.value)
        for gran in GRANULARITIES:
            assert gran in msg, f"error must name reserved granularity {gran!r}: {msg}"

    def test_model_with_granularity_named_aggregation_rejected(self) -> None:
        amount = DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT)
        with pytest.raises(pydantic.ValidationError, match="week_sunday"):
            fx.dataset(
                name="orders", sql_table="orders", data_source="test",
                fields=[amount],
                aggregations=[{"name": "quarter", "expression": "SUM({x})"}],
            )


class TestExistingNamesUnaffected:
    def test_legal_custom_aggregation_still_accepted(self) -> None:
        agg = Aggregation(name="my_median", expression="MEDIAN({x})")
        assert agg.name == "my_median"

    def test_scalar_name_accepted(self) -> None:
        assert Aggregation(name="round", expression="ROUND({x})").name == "round"

    def test_transform_name_still_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="transform"):
            Aggregation(name="cumsum", expression="SUM({x})")
