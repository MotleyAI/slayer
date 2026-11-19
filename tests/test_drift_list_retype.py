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
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.live_schema import ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._sql_mode_retype_fixtures import (
    MODEL,
    check_query_carries_finding,
    check_reingest_retypes,
    check_validation_finding,
    sql_shapes,
    stale_sql_dataset,
)

LIVE = ("CREATE TABLE players (id INTEGER, team VARCHAR, scores INTEGER[])",
        "INSERT INTO players VALUES (1, 'A', [1, 2]), (2, 'B', [])",
        "CREATE TABLE events (id INTEGER, seen TIMESTAMP[])",
        "INSERT INTO events VALUES (1, ['2024-01-05 10:00:00'::TIMESTAMP]), (2, [])")


@contextlib.asynccontextmanager
async def _env(dataset: SlayerDataset) -> AsyncGenerator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    duckdb = pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "live.duckdb")
        with contextlib.closing(duckdb.connect(db_path)) as con:
            for stmt in LIVE:
                con.execute(stmt)
        ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        await storage.save_datasource(ds)
        await storage.save_dataset(dataset)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine, ds
        finally:
            await engine.aclose()
            engine.close()
            engine_factory.invalidate_engine(ds)


def _players(scores: DatasetField) -> SlayerDataset:
    return SlayerDataset(name="players", sql_table="players", data_source="ds", fields=[
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="team", datatype=DataType.TEXT),
        DatasetField(name="bad", sql="no_such_fn(id)", datatype=DataType.DOUBLE),
        scores])


def _deletes_scores(entry: dict[str, Any]) -> bool:
    if entry.get("tool") == "delete_dataset":
        return entry.get("dataset_name") == "players"
    return "scores" in ((entry.get("remove") or {}).get("fields") or [])


def _findings(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in entries if e.get("finding") == "retype" and e.get("field") == "scores"]


STALE_TYPES = [t for t in DataType if t is not DataType.ARRAY]
assert DataType.UNKNOWN in STALE_TYPES


class TestValidation:
    @pytest.mark.parametrize("stored", STALE_TYPES, ids=lambda t: t.value)
    async def test_a_non_list_column_over_a_live_list(self, stored: DataType) -> None:
        async with _env(_players(DatasetField(name="scores", datatype=stored))) as (engine, _):
            entries = [e.model_dump(mode="json") for e in await engine.validate_datasets(data_source="ds")]
        assert not any(_deletes_scores(e) for e in entries), entries
        (finding,) = _findings(entries)
        text = json.dumps(finding)
        assert "INTEGER[]" in text, finding
        assert "ingest" in text.lower(), finding


class TestQueryTime:
    async def test_a_refused_query_carries_the_finding(self) -> None:
        async with _env(_players(DatasetField(name="scores", datatype=DataType.DOUBLE))) as (engine, _):
            with pytest.raises(SchemaDriftError) as info:
                await engine.execute({"population": "players", "metrics": [{"expression": "sum(scores)", "name": "s"}]})
        err = info.value
        assert not any(_deletes_scores(e.model_dump(mode="json")) for e in err.to_delete), err.to_delete
        assert "scores" in str(err), str(err)
        assert "ingest" in str(err).lower(), str(err)

    async def test_an_unread_stale_column_leaves_the_error(self) -> None:
        async with _env(_players(DatasetField(name="scores", datatype=DataType.DOUBLE))) as (engine, _):
            with pytest.raises(SQLAlchemyError) as info:
                await engine.execute({"population": "players", "metrics": [{"expression": "sum(bad)"}]})
        assert not isinstance(info.value, SchemaDriftError), info.value


class TestReingest:
    @pytest.mark.parametrize("stored", STALE_TYPES, ids=lambda t: t.value)
    async def test_re_ingest_re_types_in_place(self, stored: DataType) -> None:
        mine = DatasetField(name="scores", datatype=stored, description="Points per game", label="Scores")
        async with _env(_players(mine)) as (engine, ds):
            await engine.ingest_idempotent(ds, include_tables=["players"])
            loaded = await engine.storage.get_dataset("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_field("scores")
        assert scores is not None
        assert (scores.datatype, scores.element_type) == (DataType.ARRAY, DataType.INT)
        assert (scores.description, scores.label) == ("Points per game", "Scores")

    @pytest.mark.parametrize("mine", [
        DatasetField(name="scores", datatype=DataType.TIMESTAMP, granularity="month"),
        DatasetField(name="scores", datatype=DataType.DOUBLE, format=NumberFormat(type=NumberFormatType.FLOAT)),
    ], ids=["granularity", "format"])
    async def test_re_ingest_drops_scalar_only_fields(self, mine: DatasetField) -> None:
        async with _env(_players(mine)) as (engine, ds):
            await engine.ingest_idempotent(ds, include_tables=["players"])
            loaded = await engine.storage.get_dataset("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_field("scores")
        assert scores is not None
        assert (scores.datatype, scores.granularity, scores.format) == (DataType.ARRAY, None, None)

    async def test_re_ingest_keeps_only_count(self) -> None:
        mine = DatasetField(name="scores", datatype=DataType.DOUBLE, allowed_aggregations=["sum", "avg", "count"])
        async with _env(_players(mine)) as (engine, ds):
            await engine.ingest_idempotent(ds, include_tables=["players"])
            loaded = await engine.storage.get_dataset("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_field("scores")
        assert scores is not None
        assert (scores.datatype, scores.allowed_aggregations) == (DataType.ARRAY, ["count"])

    async def test_a_derived_column_named_like_a_live_list_untouched(self) -> None:
        derived = DatasetField(name="scores", sql="CAST(id AS DOUBLE)", datatype=DataType.DOUBLE)
        async with _env(_players(derived)) as (engine, ds):
            await engine.ingest_idempotent(ds, include_tables=["players"])
            loaded = await engine.storage.get_dataset("players", data_source="ds")
        assert loaded is not None
        scores = loaded.get_field("scores")
        assert scores is not None
        assert scores.datatype is DataType.DOUBLE

    async def test_re_typing_field_effects_converge(self) -> None:
        """Twice re-ingested: kept fields survive, scalar-only fields drop, the second run changes nothing."""
        seen = DatasetField(name="seen", datatype=DataType.TIMESTAMP, granularity="month",
                      format=NumberFormat(type=NumberFormatType.INTEGER), description="Sightings",
                      label="Seen", meta={"owner": "ops"}, hidden=True, sql="seen", unique=True,
                      allowed_aggregations=["min", "max", "count"])
        events = SlayerDataset(name="events", sql_table="events", data_source="ds",
                             fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True), seen])
        async with _env(events) as (engine, ds):
            await engine.ingest_idempotent(ds, include_tables=["events"])
            first = await engine.storage.get_dataset("events", data_source="ds")
            await engine.ingest_idempotent(ds, include_tables=["events"])
            second = await engine.storage.get_dataset("events", data_source="ds")
        assert first is not None
        assert second is not None
        first_seen, key = first.get_field("seen"), first.get_field("id")
        assert first_seen is not None
        assert key is not None
        assert (first_seen.datatype, first_seen.element_type) == (DataType.ARRAY, DataType.TIMESTAMP)
        assert (first_seen.granularity, first_seen.format) == (None, None)
        assert first_seen.allowed_aggregations == ["count"]
        assert (first_seen.description, first_seen.label, first_seen.meta, first_seen.hidden) == (
            "Sightings", "Seen", {"owner": "ops"}, True)
        assert (first_seen.sql, first_seen.primary_key, first_seen.unique) == ("seen", False, True)
        assert key.primary_key is True
        second_seen = second.get_field("seen")
        assert second_seen is not None
        assert second_seen.model_dump() == first_seen.model_dump()


_SQL_SHAPES = sql_shapes(table="players", column="scores", untypeable="list_reverse")


class TestSqlMode:
    @pytest.mark.parametrize(("sql", "column", "element"), _SQL_SHAPES)
    async def test_validation_finding(self, sql: str, column: str, element: DataType) -> None:
        async with _env(stale_sql_dataset(sql=sql, column=column, data_source="ds")) as (engine, _):
            await check_validation_finding(engine, data_source="ds", column=column)

    @pytest.mark.parametrize(("sql", "column", "element"), _SQL_SHAPES)
    async def test_a_refused_query_carries_the_finding(self, sql: str, column: str, element: DataType) -> None:
        async with _env(stale_sql_dataset(sql=sql, column=column, data_source="ds")) as (engine, _):
            await check_query_carries_finding(engine, column=column)

    @pytest.mark.parametrize(("sql", "column", "element"), _SQL_SHAPES)
    async def test_re_ingest_re_types_in_place(self, sql: str, column: str, element: DataType) -> None:
        async with _env(stale_sql_dataset(sql=sql, column=column, data_source="ds")) as (engine, ds):
            await check_reingest_retypes(engine, datasource=ds, table="players", column=column, element=element)

    async def test_a_by_name_union_aligns_branches_by_name(self) -> None:
        sql = "SELECT id, scores FROM players UNION ALL BY NAME SELECT scores, id FROM players"
        async with _env(stale_sql_dataset(sql=sql, column="scores", data_source="ds")) as (engine, ds):
            await check_reingest_retypes(engine, datasource=ds, table="players", column="scores", element=DataType.INT)
            loaded = await engine.storage.get_dataset(MODEL, data_source="ds")
        assert loaded is not None
        key = loaded.get_field("id")
        assert key is not None
        assert key.datatype is DataType.INT

    async def test_a_by_name_union_types_a_column_only_a_later_branch_has(self) -> None:
        sql = "SELECT id FROM players UNION ALL BY NAME SELECT id, scores FROM players"
        async with _env(stale_sql_dataset(sql=sql, column="scores", data_source="ds")) as (engine, ds):
            await check_reingest_retypes(engine, datasource=ds, table="players", column="scores", element=DataType.INT)

    async def test_re_ingest_skips_a_parameterized_model(self) -> None:
        sql = "SELECT id, scores FROM players WHERE id > {min_id}"
        async with _env(stale_sql_dataset(sql=sql, column="scores", data_source="ds")) as (engine, ds):
            result = await engine.ingest_idempotent(ds, include_tables=["players"])
        assert [e for e in result.errors if e.dataset_name == MODEL] == []

    async def test_re_ingest_keeps_an_edit_made_meanwhile(self) -> None:
        model = stale_sql_dataset(sql="SELECT id, scores FROM players", column="scores", data_source="ds")
        async with _env(model) as (engine, ds):
            real = ingestion.sql_result_types

            async def edited_meanwhile(**kwargs: Any) -> Any:
                result = await real(**kwargs)
                stored = await engine.storage.get_dataset(MODEL, data_source="ds")
                assert stored is not None
                await engine.storage.save_dataset(stored.model_copy(update={"description": "edited meanwhile"}))
                return result

            with patch.object(ingestion, "sql_result_types", side_effect=edited_meanwhile):
                await engine.ingest_idempotent(ds, include_tables=["players"])
            loaded = await engine.storage.get_dataset(MODEL, data_source="ds")
        assert loaded is not None
        scores = loaded.get_field("scores")
        assert scores is not None
        assert (loaded.description, scores.datatype) == ("edited meanwhile", DataType.ARRAY)
