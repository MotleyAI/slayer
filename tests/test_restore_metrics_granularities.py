"""Load-time restoration of a ``SLAYER`` metric formula binds with the datasource's custom granularities."""

from slayer.core.granularity import CustomGranularity, granularity_key
from slayer.core.models import DatasetMetric
from slayer.storage.document_extensions import Restored, restore_metrics
from tests._osi_export_fixtures import customers, orders

_FORMULA = "count_distinct(date_trunc('fiscal_year', ordered_on))"
_FISCAL = CustomGranularity.model_validate({"name": "fiscal_year", "base": "month", "multiple": 12,
                                            "origin": "2024-04-01"})


def _restore(**granularities: CustomGranularity) -> list[DatasetMetric]:
    home = orders(metrics=[DatasetMetric(name="fiscal_years", expression=_FORMULA)])
    core = DatasetMetric(name="fiscal_years", expression="COUNT(DISTINCT ordered_on)")
    datasets = {home.name: home, "customers": customers()}
    restore_metrics(datasets, restored=[Restored(origin=home.name, home=home.name, name="fiscal_years", core=core)],
                    dialect="duckdb", granularities=granularities)
    return datasets[home.name].metrics


def test_restored_custom_bucket_formula_is_kept() -> None:
    [metric] = _restore(**{granularity_key(_FISCAL.name): _FISCAL})
    assert metric.text == _FORMULA


def test_without_the_definition_the_document_form_stands() -> None:
    [metric] = _restore()
    assert metric.text == "COUNT(DISTINCT ordered_on)"
