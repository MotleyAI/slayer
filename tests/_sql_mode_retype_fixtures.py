"""An ``sql``-mode model whose stored non-list column now selects a live list (spec: models/schema-drift).

Live table on every backend::

    <table> (id INTEGER, ..., <list column> <INTEGER list>, ...)
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine

MODEL = "stale_sql"


def sql_shapes(*, table: str, column: str, untypeable: str | None) -> list[Any]:
    """``(sql, model column, element type after re-ingest)`` per projection shape; ``untypeable`` wraps the list in a
    call sqlglot cannot type, so only the database's result type tells it is a list."""
    shapes = [
        pytest.param(f"SELECT id, {column} FROM {table}", column, DataType.INT, id="bare"),
        pytest.param(f"SELECT t.id, t.{column} AS pts FROM {table} AS t", "pts", DataType.INT, id="aliased"),
        pytest.param(f"SELECT * FROM {table}", column, DataType.INT, id="star"),
        pytest.param(f"SELECT id, {column} FROM (SELECT * FROM {table}) AS s", column, DataType.INT, id="subquery"),
        pytest.param(f"SELECT id, NULL AS {column} FROM {table} WHERE 1 = 0 UNION ALL SELECT id, {column} FROM {table}",
                     column, DataType.INT, id="union-typed-by-a-later-branch"),
    ]
    if untypeable is not None:
        shapes.append(pytest.param(f"SELECT id, {untypeable}({column}) AS {column} FROM {table}", column,
                                   DataType.UNKNOWN, id="untypeable"))
    return shapes


def stale_sql_model(*, sql: str, column: str, data_source: str) -> SlayerModel:
    """The model as stored before list columns: the list typed ``DOUBLE``."""
    return SlayerModel(name=MODEL, sql=sql, data_source=data_source, columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name=column, type=DataType.DOUBLE, allowed_aggregations=["sum", "count"])])


async def check_validation_finding(engine: SlayerQueryEngine, *, data_source: str, column: str) -> None:
    entries = [e.model_dump(mode="json") for e in await engine.validate_models(data_source=data_source)]
    mine = [e for e in entries if e.get("model_name") == MODEL]
    deletes = [e for e in mine if e.get("tool") == "delete_model" or column in ((e.get("remove") or {}).get("columns") or [])]
    assert deletes == [], mine
    findings = [e for e in mine if e.get("finding") == "list_retype" and e.get("column") == column]
    assert len(findings) == 1, mine
    assert "ingest" in str(findings[0]).lower(), findings


async def check_query_carries_finding(engine: SlayerQueryEngine, *, column: str) -> None:
    with pytest.raises(SchemaDriftError) as info:
        await engine.execute({"source_model": MODEL, "measures": [{"formula": f"sum({column})", "name": "s"}]})
    assert column in str(info.value), str(info.value)
    assert "ingest" in str(info.value).lower(), str(info.value)


async def check_reingest_retypes(
    engine: SlayerQueryEngine, *, datasource: DatasourceConfig, table: str, column: str, element: DataType,
) -> None:
    result = await ingest_datasource_idempotent(datasource=datasource, storage=engine.storage, include_tables=[table])
    assert any(a.model_name == MODEL and column in a.widened_columns for a in result.additions), result.additions
    loaded = await engine.storage.get_model(MODEL, data_source=datasource.name)
    assert loaded is not None
    col = loaded.get_column(column)
    assert col is not None
    assert (col.type, col.element_type, col.allowed_aggregations) == (DataType.ARRAY, element, ["count"])
