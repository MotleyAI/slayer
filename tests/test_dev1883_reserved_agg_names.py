"""DEV-1883 — granularity values are reserved custom-aggregation names.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/models/aggregation-names.
"""
import pydantic
import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, Column
from tests import _dev1883_fixtures as fx

GRANULARITIES = [g.value for g in TimeGranularity]


class TestGranularityNamesReserved:
    @pytest.mark.parametrize("name", ["month", "Month", "MONTH", "WEEK_SUNDAY", "year"])
    def test_aggregation_named_like_granularity_rejected(self, name: str) -> None:
        with pytest.raises(pydantic.ValidationError) as ei:
            Aggregation(name=name, formula="SUM({x})")
        msg = str(ei.value)
        for gran in GRANULARITIES:
            assert gran in msg, f"error must name reserved granularity {gran!r}: {msg}"

    def test_model_with_granularity_named_aggregation_rejected(self) -> None:
        amount = Column(name="amount", sql="amount", type=DataType.DOUBLE)
        with pytest.raises(pydantic.ValidationError, match="week_sunday"):
            fx.model(
                name="orders", sql_table="orders", data_source="test",
                columns=[amount],
                aggregations=[{"name": "quarter", "formula": "SUM({x})"}],
            )


class TestExistingNamesUnaffected:
    def test_legal_custom_aggregation_still_accepted(self) -> None:
        agg = Aggregation(name="my_median", formula="MEDIAN({x})")
        assert agg.name == "my_median"

    def test_scalar_name_still_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="scalar"):
            Aggregation(name="round", formula="ROUND({x})")

    def test_transform_name_still_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="transform"):
            Aggregation(name="cumsum", formula="SUM({x})")
