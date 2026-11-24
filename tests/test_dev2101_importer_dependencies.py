"""Importers check saved-metric dependencies over the one Mode-B parser (design D8)."""

from __future__ import annotations

import importlib

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, DatasetField, DatasetMetric, SlayerDataset
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.report import CubeConversionReport
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject

METRICS = {
    "a": "sum(amount)",
    "b": "a + missing",
    "c": "b * 2",
    "d": "e + 1",
    "e": "d + 1",
    "f": "a / count(*)",
    "g": "sum(amount) FILTER (WHERE status = 'paid')",
    "h": "a + CAST(sum(quantity) AS DOUBLE)",
    "k": "count(distinct status) + a",
}
SURVIVORS = {"a", "f", "g", "h", "k"}
DANGLING = {"b", "c"}
CYCLIC = {"d", "e"}


def _metrics(extra: dict[str, str] | None = None) -> list[DatasetMetric]:
    return [DatasetMetric(name=n, expression=f) for n, f in {**METRICS, **(extra or {})}.items()]


class TestDbtDependencyCheck:
    def _prune(self, metrics: list[DatasetMetric]) -> tuple[set[str], set[str]]:
        model = SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[DatasetField(name=c, datatype=DataType.FLOAT) for c in ("id", "amount", "quantity", "price")]
            + [DatasetField(name="status", datatype=DataType.STRING)],
            aggregations=[Aggregation(name="wsum", expression="SUM({value} * {weight})")],
            metrics=metrics,
        )
        conv = DbtToSlayerConverter(project=DbtProject(semantic_models=[], metrics=[]), data_source="test")
        conv._prune_dataset_metrics(model)
        dropped = {w.metric_name for w in conv._unconverted if w.category == "dangling_reference"}
        return {m.name for m in model.metrics if m.name}, {d for d in dropped if d}

    def test_survivors_and_drops(self) -> None:
        kept, dropped = self._prune(_metrics())
        assert kept == SURVIVORS
        assert dropped == DANGLING | CYCLIC

    def test_bare_column_outside_an_aggregation_is_dangling(self) -> None:
        kept, dropped = self._prune(_metrics({"i": "amount + a"}))
        assert "i" in dropped
        assert "i" not in kept

    def test_custom_aggregation_name_is_in_the_namespace(self) -> None:
        kept, _ = self._prune(_metrics({"j": "wsum(price, weight=quantity) + a"}))
        assert "j" in kept

    def test_self_reference_is_cyclic(self) -> None:
        _, dropped = self._prune(_metrics({"s": "s + 1"}))
        assert "s" in dropped


class TestCubeDependencyCheck:
    def _drop(self, metrics: list[DatasetMetric]) -> set[str]:
        conv = CubeToSlayerConverter(project=None, data_source="test")
        kept = conv._drop_unparseable_metrics("orders", metrics, CubeConversionReport())
        return {m.name for m in kept if m.name}

    def test_survivors(self) -> None:
        assert self._drop(_metrics()) == SURVIVORS

    def test_transitive_dangling_reference_dropped(self) -> None:
        assert not self._drop(_metrics()) & DANGLING

    def test_cyclic_measures_dropped(self) -> None:
        assert not self._drop(_metrics()) & CYCLIC


class TestOneParser:
    def test_legacy_parser_deleted(self) -> None:
        module = importlib.import_module("slayer.core.formula")
        with pytest.raises(ImportError, match="slayer.core.syntax"):
            getattr(module, "parse_expr")

    @pytest.mark.parametrize("module", ["slayer.dbt.converter", "slayer.cube.converter"])
    def test_importers_do_not_use_the_legacy_parser(self, module: str) -> None:
        assert not hasattr(importlib.import_module(module), "parse_formula")

    @pytest.mark.parametrize("name", ["ALL_TRANSFORMS", "TIME_TRANSFORMS", "TIMELESS_TRANSFORMS"])
    def test_transform_tables_live_in_enums(self, name: str) -> None:
        assert hasattr(importlib.import_module("slayer.core.enums"), name)
