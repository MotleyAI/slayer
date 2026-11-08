"""A stored TEXT column over a live list is a re-type finding, never a deletion (spec: models/schema-drift)."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

LIVE = ("CREATE TABLE contractors (id INTEGER, skills VARCHAR[])",
        "INSERT INTO contractors VALUES (1, ['go', 'sql']), (2, [])")


@contextlib.asynccontextmanager
async def _env(*models: SlayerModel) -> AsyncGenerator[SlayerQueryEngine]:
    duckdb = pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "live.duckdb")
        with contextlib.closing(duckdb.connect(db_path)) as con:
            for stmt in LIVE:
                con.execute(stmt)
        ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(ds)
        for model in models:
            await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine
        finally:
            engine.close()
            engine_factory.invalidate_engine(ds)


def _contractors(skills: Column) -> SlayerModel:
    return SlayerModel(name="contractors", sql_table="contractors", data_source="ds",
                       columns=[Column(name="id", type=DataType.INT, primary_key=True), skills])


STALE = Column(name="skills", type=DataType.TEXT)


def _dumps(entries: list[Any]) -> list[dict[str, Any]]:
    return [e.model_dump(mode="json") for e in entries]


def _deletes_skills(entry: dict[str, Any]) -> bool:
    if entry.get("tool") == "delete_model":
        return entry.get("model_name") == "contractors"
    return "skills" in ((entry.get("remove") or {}).get("columns") or [])


def _retype_findings(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in entries if e.get("tool") not in ("edit_model", "delete_model")
            and "skills" in json.dumps(e) and "contractors" in json.dumps(e)]


class TestValidation:
    async def test_stale_text_list_column(self) -> None:
        async with _env(_contractors(STALE)) as engine:
            entries = _dumps(await engine.validate_models(data_source="ds"))
        assert not any(_deletes_skills(e) for e in entries), entries
        (finding,) = _retype_findings(entries)
        text = json.dumps(finding)
        assert "VARCHAR[]" in text, finding
        assert "ingest" in text.lower(), finding

    async def test_a_typed_list_column_is_not_drift(self) -> None:
        typed = Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT)
        async with _env(_contractors(typed)) as engine:
            entries = _dumps(await engine.validate_models(data_source="ds"))
        assert entries == [], entries

    async def test_a_declared_list_in_an_sql_mode_model(self) -> None:
        model = SlayerModel(name="crew", sql="SELECT id, skills FROM contractors", data_source="ds", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="skills", type=DataType.ARRAY, element_type=DataType.TEXT)])
        async with _env(model) as engine:
            entries = _dumps(await engine.validate_models(data_source="ds"))
        assert entries == [], entries


class TestQueryTime:
    async def test_a_refused_query_explained(self) -> None:
        async with _env(_contractors(STALE)) as engine:
            with pytest.raises(SchemaDriftError) as info:
                await engine.execute({"source_model": "contractors", "measures": [{"formula": "count(*)", "name": "n"}],
                                      "filters": ["skills = 'go'"]})
        err = info.value
        assert "contractors" in err.models
        assert not any(_deletes_skills(e) for e in _dumps(err.to_delete)), err.to_delete
        assert "skills" in str(err), str(err)
        assert "ingest" in str(err).lower(), str(err)
