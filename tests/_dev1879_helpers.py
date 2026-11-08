"""Shared model fixtures for the unnamed-formula-measure naming tests."""

from __future__ import annotations

import tempfile
from collections.abc import Sequence

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def mart_model(
    *,
    extra_columns: Sequence[Column] = (),
    measures: Sequence[ModelMeasure] = (),
) -> SlayerModel:
    return SlayerModel(
        name="mart",
        data_source="test",
        sql_table="mart",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="target_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="logo_churn", type=DataType.DOUBLE),
            Column(name="logo_bop", type=DataType.DOUBLE),
            Column(name="cmrr_eop", type=DataType.DOUBLE),
            *extra_columns,
            Column(name="price", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="targets", join_pairs=[["target_id", "id"]]),
        ],
        measures=list(measures),
    )


def targets_model() -> SlayerModel:
    return SlayerModel(
        name="targets",
        data_source="test",
        sql_table="targets",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="goal", type=DataType.DOUBLE),
        ],
    )


async def dry_response(
    query: SlayerQuery,
    *,
    mart: SlayerModel | None = None,
    dialect: str = "postgres",
):
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name="test", type=dialect))
        await storage.save_model(mart if mart is not None else mart_model())
        await storage.save_model(targets_model())
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(query, dry_run=True)
