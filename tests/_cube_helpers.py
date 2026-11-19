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


def metric(dataset: SlayerDataset, name: str) -> DatasetMetric:
    m = dataset.get_metric(name)
    assert m is not None, f"metric {name} missing on {dataset.name}"
    return m


def column(dataset: SlayerDataset, name: str) -> DatasetField:
    c = dataset.get_field(name)
    assert c is not None, f"field {name} missing on {dataset.name}"
    return c


def meta(dataset: SlayerDataset) -> dict[str, Any]:
    assert dataset.meta is not None, f"{dataset.name} has no meta"
    return dataset.meta
