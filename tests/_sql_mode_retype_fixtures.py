"""An ``sql``-mode model whose stored non-list column now selects a live list (spec: models/schema-drift).

Live table on every backend::

    <table> (id INTEGER, ..., <list column> <INTEGER list>, ...)
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine

MODEL = "stale_sql"


def sql_shapes(*, table: str, column: str, untypeable: str | None) -> list[Any]:
    """``(sql, model column, element type after re-ingest)`` per projection shape; ``untypeable`` wraps the list in a
    call sqlglot cannot type, so only the database's result type tells it is a list."""
    shapes = [
        pytest.param(f"SELECT id, {column} FROM {table}", column, DataType.INTEGER, id="bare"),
        pytest.param(f"SELECT t.id, t.{column} AS pts FROM {table} AS t", "stale_sql.pts", DataType.INTEGER, id="aliased"),
        pytest.param(f"SELECT * FROM {table}", column, DataType.INTEGER, id="star"),
        pytest.param(f"SELECT id, {column} FROM (SELECT * FROM {table}) AS s", column, DataType.INTEGER, id="subquery"),
        pytest.param(f"SELECT id, NULL AS {column} FROM {table} WHERE 1 = 0 UNION ALL SELECT id, {column} FROM {table}",
                     column, DataType.INTEGER, id="union-typed-by-a-later-branch"),
    ]
    if untypeable is not None:
        shapes.append(pytest.param(f"SELECT id, {untypeable}({column}) AS {column} FROM {table}", column,
                                   DataType.OPAQUE, id="untypeable"))
    return shapes


def stale_sql_dataset(*, sql: str, column: str, data_source: str) -> SlayerDataset:
    """The model as stored before list columns: the list typed ``DOUBLE``."""
    return SlayerDataset(name=MODEL, sql=sql, data_source=data_source, fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name=column, datatype=DataType.FLOAT, allowed_aggregations=["sum", "count"])])


async def check_validation_finding(engine: SlayerQueryEngine, *, data_source: str, column: str) -> None:
    entries = [e.model_dump(mode="json") for e in await engine.validate_datasets(data_source=data_source)]
    mine = [e for e in entries if e.get("dataset_name") == MODEL]
    deletes = [e for e in mine if e.get("tool") == "delete_dataset" or column in ((e.get("remove") or {}).get("fields") or [])]
    assert deletes == [], mine
    findings = [e for e in mine if e.get("finding") == "retype" and e.get("field") == column]
    assert len(findings) == 1, mine
    assert "ingest" in str(findings[0]).lower(), findings


async def check_query_carries_finding(engine: SlayerQueryEngine, *, column: str) -> None:
    with pytest.raises(SchemaDriftError) as info:
        await engine.execute({"population": MODEL, "metrics": [{"metric": f"sum({column})", "alias": "s"}]})
    assert column in str(info.value), str(info.value)
    assert "ingest" in str(info.value).lower(), str(info.value)


async def check_reingest_retypes(
    engine: SlayerQueryEngine, *, datasource: DatasourceConfig, table: str, column: str, element: DataType,
) -> None:
    result = await ingest_datasource_idempotent(datasource=datasource, storage=engine.storage,
                                                client=engine.client_for(datasource), include_tables=[table])
    assert any(a.dataset_name == MODEL and column in a.widened_fields for a in result.additions), result.additions
    loaded = await engine.storage.get_dataset(MODEL, data_source=datasource.name)
    assert loaded is not None
    col = loaded.get_field(column)
    assert col is not None
    assert (col.datatype, col.element_type, col.allowed_aggregations) == (DataType.ARRAY, element, ["count"])
