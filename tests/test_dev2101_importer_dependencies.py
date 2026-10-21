"""Importers check saved-measure dependencies over the one Mode-B parser (design D8)."""

from __future__ import annotations

import importlib
import importlib.util

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, ModelMeasure, SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.report import CubeConversionReport
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject

MEASURES = {
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


def _measures(extra: dict[str, str] | None = None) -> list[ModelMeasure]:
    return [ModelMeasure(name=n, formula=f) for n, f in {**MEASURES, **(extra or {})}.items()]


class TestDbtDependencyCheck:
    def _prune(self, measures: list[ModelMeasure]) -> tuple[set[str], set[str]]:
        model = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[Column(name=c, type=DataType.DOUBLE) for c in ("id", "amount", "quantity", "price")]
            + [Column(name="status", type=DataType.TEXT)],
            aggregations=[Aggregation(name="wsum", formula="SUM({value} * {weight})")],
            measures=measures,
        )
        conv = DbtToSlayerConverter(project=DbtProject(semantic_models=[], metrics=[]), data_source="test")
        conv._prune_model_measures(model)
        dropped = {w.metric_name for w in conv._unconverted if w.category == "dangling_reference"}
        return {m.name for m in model.measures if m.name}, {d for d in dropped if d}

    def test_survivors_and_drops(self) -> None:
        kept, dropped = self._prune(_measures())
        assert kept == SURVIVORS
        assert dropped == DANGLING | CYCLIC

    def test_bare_column_outside_an_aggregation_is_dangling(self) -> None:
        kept, dropped = self._prune(_measures({"i": "amount + a"}))
        assert "i" in dropped and "i" not in kept

    def test_custom_aggregation_name_is_in_the_namespace(self) -> None:
        kept, _ = self._prune(_measures({"j": "wsum(price, weight=quantity) + a"}))
        assert "j" in kept

    def test_self_reference_is_cyclic(self) -> None:
        _, dropped = self._prune(_measures({"s": "s + 1"}))
        assert "s" in dropped


class TestCubeDependencyCheck:
    def _drop(self, measures: list[ModelMeasure]) -> set[str]:
        conv = CubeToSlayerConverter(project=None, data_source="test")
        kept = conv._drop_unparseable_measures("orders", measures, CubeConversionReport())
        return {m.name for m in kept if m.name}

    def test_survivors(self) -> None:
        assert self._drop(_measures()) == SURVIVORS

    def test_transitive_dangling_reference_dropped(self) -> None:
        assert not self._drop(_measures()) & DANGLING

    def test_cyclic_measures_dropped(self) -> None:
        assert not self._drop(_measures()) & CYCLIC


class TestOneParser:
    @pytest.mark.parametrize("name", [
        "parse_formula", "FieldSpec", "AggregatedMeasureRef", "ArithmeticField", "TransformField",
        "MixedArithmeticField", "_rewrite_funcstyle_aggregations", "_preprocess_agg_refs", "_expand_named_measures",
    ])
    def test_legacy_parser_deleted(self, name: str) -> None:
        if importlib.util.find_spec("slayer.core.formula") is not None:
            assert not hasattr(importlib.import_module("slayer.core.formula"), name)

    @pytest.mark.parametrize("module", ["slayer.dbt.converter", "slayer.cube.converter", "slayer.osi.converter"])
    def test_importers_do_not_use_the_legacy_parser(self, module: str) -> None:
        assert not hasattr(importlib.import_module(module), "parse_formula")

    @pytest.mark.parametrize("name", ["ALL_TRANSFORMS", "TIME_TRANSFORMS", "TIMELESS_TRANSFORMS"])
    def test_transform_tables_live_in_enums(self, name: str) -> None:
        assert hasattr(importlib.import_module("slayer.core.enums"), name)
