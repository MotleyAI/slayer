"""Shared helpers for the Cube converter tests."""

from typing import Any

from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.cube.report import CubeConversionReport

DS = "test_ds"


def convert(project: CubeProject) -> tuple[dict[str, SlayerDataset], CubeConversionReport]:
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.datasets}, result.report


def measure(model: SlayerDataset, name: str) -> DatasetMetric:
    m = model.get_metric(name)
    assert m is not None, f"measure {name} missing on {model.name}"
    return m


def column(model: SlayerDataset, name: str) -> DatasetField:
    c = model.get_field(name)
    assert c is not None, f"column {name} missing on {model.name}"
    return c


def meta(model: SlayerDataset) -> dict[str, Any]:
    assert model.meta is not None, f"{model.name} has no meta"
    return model.meta
