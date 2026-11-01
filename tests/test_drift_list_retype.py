"""Any non-list column over a live list is a report-only re-type finding that re-ingest applies
(spec: models/schema-drift)."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine import ingestion
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._sql_mode_retype_fixtures import (
    MODEL,
    check_query_carries_finding,
    check_reingest_retypes,
    check_validation_finding,
    sql_shapes,
    stale_sql_model,
)

LIVE = ("CREATE TABLE players (id INTEGER, team VARCHAR, scores INTEGER[])",
        "INSERT INTO players VALUES (1, 'A', [1, 2]), (2, 'B', [])")


@contextlib.asynccontextmanager
async def _env(model: SlayerModel) -> AsyncGenerator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    duckdb = pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "live.duckdb")
        with contextlib.closing(duckdb.connect(db_path)) as con:
            for stmt in LIVE:
                con.execute(stmt)
        ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(ds)
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine, ds
        finally:
            engine.close()
            engine_factory.invalidate_engine(ds)


def _players(scores: Column) -> SlayerModel:
    return SlayerModel(name="players", sql_table="players", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="team", type=DataType.TEXT),
        Column(name="bad", sql="no_such_fn(id)", type=DataType.DOUBLE),
        scores])


def _deletes_scores(entry: dict[str, Any]) -> bool:
    if entry.get("tool") == "delete_model":
        return entry.get("model_name") == "players"
    return "scores" in ((entry.get("remove") or {}).get("columns") or [])


def _findings(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in entries if e.get("finding") == "list_retype" and e.get("column") == "scores"]


STALE_TYPES = [t for t in DataType if t is not DataType.ARRAY]
assert DataType.UNKNOWN in STALE_TYPES


class TestValidation:
    @pytest.mark.parametrize("stored", STALE_TYPES, ids=lambda t: t.value)
    async def test_a_non_list_column_over_a_live_list(self, stored: DataType) -> None:
        async with _env(_players(Column(name="scores", type=stored))) as (engine, _):
            entries = [e.model_dump(mode="json") for e in await engine.validate_models(data_source="ds")]
        assert not any(_deletes_scores(e) for e in entries), entries
        (finding,) = _findings(entries)
        text = json.dumps(finding)
        assert "INTEGER[]" in text, finding
        assert "ingest" in text.lower(), finding


class TestQueryTime:
    async def test_a_refused_query_carries_the_finding(self) -> None:
        async with _env(_players(Column(name="scores", type=DataType.DOUBLE))) as (engine, _):
            with pytest.raises(SchemaDriftError) as info:
                await engine.execute({"source_model": "players", "measures": [{"formula": "sum(scores)", "name": "s"}]})
        err = info.value
        assert not any(_deletes_scores(e.model_dump(mode="json")) for e in err.to_delete), err.to_delete
        assert "scores" in str(err), str(err)
        assert "ingest" in str(err).lower(), str(err)

    async def test_an_unread_stale_column_leaves_the_error(self) -> None:
        async with _env(_players(Column(name="scores", type=DataType.DOUBLE))) as (engine, _):
            with pytest.raises(SQLAlchemyError) as info:
                await engine.execute({"source_model": "players", "measures": [{"formula": "sum(bad)"}]})
        assert not isinstance(info.value, SchemaDriftError), info.value


class TestReingest:
    @pytest.mark.parametrize("stored", STALE_TYPES, ids=lambda t: t.value)
    async def test_re_ingest_re_types_in_place(self, stored: DataType) -> None:
        mine = Column(name="scores", type=stored, description="Points per game", label="Scores")
        async with _env(_players(mine)) as (engine, ds):
            await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, include_tables=["players"])
            loaded = await engine.storage.get_model("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.INT)
        assert (scores.description, scores.label) == ("Points per game", "Scores")

    @pytest.mark.parametrize("mine", [
        Column(name="scores", type=DataType.TIMESTAMP, granularity="month"),
        Column(name="scores", type=DataType.DOUBLE, format=NumberFormat(type=NumberFormatType.FLOAT)),
    ], ids=["granularity", "format"])
    async def test_re_ingest_drops_scalar_only_fields(self, mine: Column) -> None:
        async with _env(_players(mine)) as (engine, ds):
            await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, include_tables=["players"])
            loaded = await engine.storage.get_model("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (scores.type, scores.granularity, scores.format) == (DataType.ARRAY, None, None)

    async def test_re_ingest_keeps_only_count(self) -> None:
        mine = Column(name="scores", type=DataType.DOUBLE, allowed_aggregations=["sum", "avg", "count"])
        async with _env(_players(mine)) as (engine, ds):
            await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, include_tables=["players"])
            loaded = await engine.storage.get_model("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (scores.type, scores.allowed_aggregations) == (DataType.ARRAY, ["count"])

    async def test_a_derived_column_named_like_a_live_list_untouched(self) -> None:
        derived = Column(name="scores", sql="CAST(id AS DOUBLE)", type=DataType.DOUBLE)
        async with _env(_players(derived)) as (engine, ds):
            await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, include_tables=["players"])
            loaded = await engine.storage.get_model("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_column("scores")
        assert scores is not None
        assert scores.type is DataType.DOUBLE


_SQL_SHAPES = sql_shapes(table="players", column="scores", untypeable="list_reverse")


class TestSqlMode:
    @pytest.mark.parametrize(("sql", "column", "element"), _SQL_SHAPES)
    async def test_validation_finding(self, sql: str, column: str, element: DataType) -> None:
        async with _env(stale_sql_model(sql=sql, column=column, data_source="ds")) as (engine, _):
            await check_validation_finding(engine, data_source="ds", column=column)

    @pytest.mark.parametrize(("sql", "column", "element"), _SQL_SHAPES)
    async def test_a_refused_query_carries_the_finding(self, sql: str, column: str, element: DataType) -> None:
        async with _env(stale_sql_model(sql=sql, column=column, data_source="ds")) as (engine, _):
            await check_query_carries_finding(engine, column=column)

    @pytest.mark.parametrize(("sql", "column", "element"), _SQL_SHAPES)
    async def test_re_ingest_re_types_in_place(self, sql: str, column: str, element: DataType) -> None:
        async with _env(stale_sql_model(sql=sql, column=column, data_source="ds")) as (engine, ds):
            await check_reingest_retypes(engine, datasource=ds, table="players", column=column, element=element)

    async def test_a_by_name_union_aligns_branches_by_name(self) -> None:
        sql = "SELECT id, scores FROM players UNION ALL BY NAME SELECT scores, id FROM players"
        async with _env(stale_sql_model(sql=sql, column="scores", data_source="ds")) as (engine, ds):
            await check_reingest_retypes(engine, datasource=ds, table="players", column="scores", element=DataType.INT)
            loaded = await engine.storage.get_model(MODEL, data_source="ds")
        assert loaded is not None
        key = loaded.get_column("id")
        assert key is not None
        assert key.type is DataType.INT

    async def test_a_by_name_union_types_a_column_only_a_later_branch_has(self) -> None:
        sql = "SELECT id FROM players UNION ALL BY NAME SELECT id, scores FROM players"
        async with _env(stale_sql_model(sql=sql, column="scores", data_source="ds")) as (engine, ds):
            await check_reingest_retypes(engine, datasource=ds, table="players", column="scores", element=DataType.INT)

    async def test_re_ingest_skips_a_parameterized_model(self) -> None:
        sql = "SELECT id, scores FROM players WHERE id > {min_id}"
        async with _env(stale_sql_model(sql=sql, column="scores", data_source="ds")) as (engine, ds):
            result = await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, include_tables=["players"])
        assert [e for e in result.errors if e.model_name == MODEL] == []

    async def test_re_ingest_keeps_an_edit_made_meanwhile(self) -> None:
        model = stale_sql_model(sql="SELECT id, scores FROM players", column="scores", data_source="ds")
        async with _env(model) as (engine, ds):
            real = ingestion.sql_result_types

            async def edited_meanwhile(**kwargs: Any) -> Any:
                result = await real(**kwargs)
                stored = await engine.storage.get_model(MODEL, data_source="ds")
                assert stored is not None
                await engine.storage.save_model(stored.model_copy(update={"description": "edited meanwhile"}))
                return result

            with patch.object(ingestion, "sql_result_types", side_effect=edited_meanwhile):
                await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, include_tables=["players"])
            loaded = await engine.storage.get_model(MODEL, data_source="ds")
        assert loaded is not None
        scores = loaded.get_column("scores")
        assert scores is not None
        assert (loaded.description, scores.type) == ("edited meanwhile", DataType.ARRAY)
