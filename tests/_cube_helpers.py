"""Shared helpers for the Cube converter tests."""

from typing import Any

from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.cube.report import CubeConversionReport

DS = "test_ds"


def convert(project: CubeProject) -> tuple[dict[str, SlayerModel], CubeConversionReport]:
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}, result.report


def measure(model: SlayerModel, name: str) -> ModelMeasure:
    m = model.get_measure(name)
    assert m is not None, f"measure {name} missing on {model.name}"
    return m


def column(model: SlayerModel, name: str) -> Column:
    c = model.get_column(name)
    assert c is not None, f"column {name} missing on {model.name}"
    return c


def meta(model: SlayerModel) -> dict[str, Any]:
    assert model.meta is not None, f"{model.name} has no meta"
    return model.meta
