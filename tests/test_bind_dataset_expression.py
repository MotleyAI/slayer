"""``bind_dataset_expression`` binds buckets of the custom granularities it is given."""

import pytest

from slayer.core.errors import UnknownGranularityError
from slayer.core.granularity import CustomGranularity, granularity_key
from slayer.core.keys import AggregateKey, TimeTruncKey
from slayer.engine.binding import bind_dataset_expression
from tests._osi_export_fixtures import customers, orders

_FORMULA = "count_distinct(date_trunc('fiscal_year', ordered_on))"
_FISCAL = CustomGranularity.model_validate({"name": "fiscal_year", "base": "month", "multiple": 12,
                                            "origin": "2024-04-01"})


def test_binds_a_custom_granularity_bucket() -> None:
    home = orders()
    key = bind_dataset_expression(_FORMULA, dataset=home, datasets=[home, customers()], dialect="duckdb",
                                  granularities={granularity_key(_FISCAL.name): _FISCAL})
    assert isinstance(key, AggregateKey)
    assert isinstance(key.source, TimeTruncKey)
    assert key.source.granularity == _FISCAL


def test_unknown_granularity_without_definitions() -> None:
    home = orders()
    datasets = [home, customers()]
    with pytest.raises(UnknownGranularityError):
        bind_dataset_expression(_FORMULA, dataset=home, datasets=datasets, dialect="duckdb")
