"""Any non-list column over a live list is a report-only re-type finding that re-ingest applies
(spec: models/schema-drift, models/list-columns)."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

LIVE = ("CREATE TABLE players (id INTEGER, team VARCHAR, scores INTEGER[])",
        "INSERT INTO players VALUES (1, 'A', [1, 2]), (2, 'B', [])",
        "CREATE TABLE events (id INTEGER, seen TIMESTAMP[], tags VARCHAR[])",
        "INSERT INTO events VALUES (1, ['2024-01-05 10:00:00'::TIMESTAMP], ['go']), (2, [], [])")


class Env:
    """An engine over the live DuckDB file, its datasource and the raw store."""

    def __init__(self, *, engine: SlayerQueryEngine, ds: DatasourceConfig, storage: YAMLStorage) -> None:
        self.engine = engine
        self.ds = ds
        self.storage = storage

    async def reingest(self, *tables: str) -> Any:
        return await ingest_datasource_idempotent(datasource=self.ds, storage=self.storage,
                                                  client=self.engine.client_for(self.ds), include_tables=list(tables))

    async def column(self, model: str, name: str) -> Column:
        loaded = await self.storage.get_model(model, data_source="ds")
        assert loaded is not None
        column = loaded.get_column(name)
        assert column is not None
        return column


@contextlib.asynccontextmanager
async def _env(*models: SlayerModel) -> AsyncGenerator[Env]:
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
            yield Env(engine=engine, ds=ds, storage=storage)
        finally:
            engine.close()
            engine_factory.invalidate_engine(ds)


def _players(scores: Column) -> SlayerModel:
    return SlayerModel(name="players", sql_table="players", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="team", type=DataType.TEXT),
        Column(name="bad", sql="no_such_fn(id)", type=DataType.DOUBLE),
        scores])


def _events(*columns: Column) -> SlayerModel:
    return SlayerModel(name="events", sql_table="events", data_source="ds",
                       columns=[Column(name="id", type=DataType.INT, primary_key=True), *columns])


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
        async with _env(_players(Column(name="scores", type=stored))) as env:
            entries = [e.model_dump(mode="json") for e in await env.engine.validate_models(data_source="ds")]
        assert not any(_deletes_scores(e) for e in entries), entries
        (finding,) = _findings(entries)
        text = json.dumps(finding)
        assert "INTEGER[]" in text, finding
        assert "ingest" in text.lower(), finding


class TestQueryTime:
    async def test_a_refused_query_carries_the_finding(self) -> None:
        async with _env(_players(Column(name="scores", type=DataType.DOUBLE))) as env:
            with pytest.raises(SchemaDriftError) as info:
                await env.engine.execute({"source_model": "players",
                                          "measures": [{"formula": "sum(scores)", "name": "s"}]})
        err = info.value
        assert not any(_deletes_scores(e.model_dump(mode="json")) for e in err.to_delete), err.to_delete
        assert "scores" in str(err), str(err)
        assert "ingest" in str(err).lower(), str(err)

    async def test_an_unread_stale_column_leaves_the_error(self) -> None:
        async with _env(_players(Column(name="scores", type=DataType.DOUBLE))) as env:
            with pytest.raises(SQLAlchemyError) as info:
                await env.engine.execute({"source_model": "players", "measures": [{"formula": "sum(bad)"}]})
        assert not isinstance(info.value, SchemaDriftError), info.value


class TestReingest:
    @pytest.mark.parametrize("stored", STALE_TYPES, ids=lambda t: t.value)
    async def test_re_ingest_re_types_in_place(self, stored: DataType) -> None:
        mine = Column(name="scores", type=stored, description="Points per game", label="Scores")
        async with _env(_players(mine)) as env:
            await env.reingest("players")
            scores = await env.column("players", "scores")
        assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.INT)
        assert (scores.description, scores.label) == ("Points per game", "Scores")

    @pytest.mark.parametrize("mine", [
        Column(name="scores", type=DataType.TIMESTAMP, granularity="month"),
        Column(name="scores", type=DataType.DOUBLE, format=NumberFormat(type=NumberFormatType.FLOAT)),
    ], ids=["granularity", "format"])
    async def test_re_ingest_drops_scalar_only_fields(self, mine: Column) -> None:
        async with _env(_players(mine)) as env:
            await env.reingest("players")
            scores = await env.column("players", "scores")
        assert (scores.type, scores.granularity, scores.format) == (DataType.ARRAY, None, None)

    async def test_re_ingest_keeps_only_count(self) -> None:
        mine = Column(name="scores", type=DataType.DOUBLE, allowed_aggregations=["sum", "avg", "count"])
        async with _env(_players(mine)) as env:
            await env.reingest("players")
            scores = await env.column("players", "scores")
        assert (scores.type, scores.allowed_aggregations) == (DataType.ARRAY, ["count"])

    async def test_a_derived_column_named_like_a_live_list_untouched(self) -> None:
        derived = Column(name="scores", sql="CAST(id AS DOUBLE)", type=DataType.DOUBLE)
        async with _env(_players(derived)) as env:
            await env.reingest("players")
            scores = await env.column("players", "scores")
        assert scores.type is DataType.DOUBLE

    async def test_re_typing_field_effects_converge(self) -> None:
        """Twice re-ingested: kept fields survive, scalar-only fields drop, the second run changes nothing."""
        seen = Column(name="seen", type=DataType.TIMESTAMP, granularity="month",
                      format=NumberFormat(type=NumberFormatType.INTEGER), description="Sightings",
                      label="Seen", meta={"owner": "ops"}, hidden=True, sql="seen", unique=True,
                      allowed_aggregations=["min", "max", "count"])
        async with _env(_events(seen)) as env:
            await env.reingest("events")
            first = await env.column("events", "seen")
            key = await env.column("events", "id")
            await env.reingest("events")
            second = await env.column("events", "seen")
        assert (first.type, first.element_type) == (DataType.ARRAY, DataType.TIMESTAMP)
        assert (first.granularity, first.format) == (None, None)
        assert first.allowed_aggregations == ["count"]
        assert (first.description, first.label, first.meta, first.hidden) == ("Sightings", "Seen", {"owner": "ops"},
                                                                              True)
        assert (first.sql, first.primary_key, first.unique) == ("seen", False, True)
        assert key.primary_key is True
        assert second.model_dump() == first.model_dump()


class TestListColumnRetype:
    """Spec models/list-columns › Re-ingest re-types TEXT list columns."""

    async def test_re_typing_keeps_metadata(self) -> None:
        async with _env(_events(Column(name="tags", type=DataType.TEXT, description="Skill tags"))) as env:
            await env.reingest("events")
            tags = await env.column("events", "tags")
        assert (tags.type, tags.element_type, tags.description) == (DataType.ARRAY, DataType.TEXT, "Skill tags")

    async def test_re_typing_keeps_only_the_count_aggregation(self) -> None:
        stale = Column(name="tags", type=DataType.TEXT, allowed_aggregations=["count", "count_distinct"])
        async with _env(_events(stale)) as env:
            await env.reingest("events")
            tags = await env.column("events", "tags")
        assert (tags.type, tags.allowed_aggregations) == (DataType.ARRAY, ["count"])

    async def test_a_users_element_type_survives(self) -> None:
        mine = Column(name="scores", type=DataType.ARRAY, element_type=DataType.TEXT)
        async with _env(_players(mine)) as env:
            await env.reingest("players")
            scores = await env.column("players", "scores")
        assert (scores.type, scores.element_type) == (DataType.ARRAY, DataType.TEXT)
