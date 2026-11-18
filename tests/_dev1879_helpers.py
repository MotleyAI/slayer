"""Shared model fixtures for the unnamed-formula-measure naming tests."""

from __future__ import annotations

import tempfile
from collections.abc import Sequence

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def mart_dataset(
    *,
    extra_columns: Sequence[DatasetField] = (),
    metrics: Sequence[DatasetMetric] = (),
) -> SlayerDataset:
    return SlayerDataset(
        name="mart",
        data_source="test",
        sql_table="mart",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="target_id", datatype=DataType.INTEGER),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="logo_churn", datatype=DataType.FLOAT),
            DatasetField(name="logo_bop", datatype=DataType.FLOAT),
            DatasetField(name="cmrr_eop", datatype=DataType.FLOAT),
            *extra_columns,
            DatasetField(name="price", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
        ],
        relationships=[
            DatasetRelationship(to="targets", from_columns=["target_id"], to_columns=["id"]),
        ],
        metrics=list(metrics),
    )


def targets_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="targets",
        data_source="test",
        sql_table="targets",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="goal", datatype=DataType.FLOAT),
        ],
    )


async def dry_response(
    query: SlayerQuery,
    *,
    mart: SlayerDataset | None = None,
    dialect: str = "postgres",
):
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name="test", type=dialect))
        await storage.save_dataset(mart if mart is not None else mart_dataset())
        await storage.save_dataset(targets_dataset())
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(query, dry_run=True)
