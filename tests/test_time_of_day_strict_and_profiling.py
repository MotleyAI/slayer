"""A time of day under the strict Ossie binding, as a ``Time`` field of an Ossie document, and in sample profiling
(spec: queries/time-of-day › Time of day in strict Ossie mode, Time-of-day fields are profiled as a range;
compatibility/ossie-vocabulary › Datatype values are Ossie's names)."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from datetime import time
from pathlib import Path
from typing import Any

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.dataset_render import render_dataset_inspection
from tests._dev2217_fixtures import DS, sql_engine, strict
from tests._dev2243_fixtures import dataset, document_text, field, metric
from tests._time_of_day_fixtures import BACKENDS, is_time, shifts_engine

SQL = ("CREATE TABLE t (id VARCHAR, tod TIME, ts TIMESTAMP);"
       "INSERT INTO t VALUES ('A', '09:30:00', '2026-01-01 08:00:00'), ('B', '09:30:00', '2026-01-01 10:15:00'),"
       "('C', '17:00:00', '2026-01-02 23:00:00');")
DOC = document_text(name="m", datasets=[dataset("t", primary_key=["id"], metrics=[metric("n", "COUNT(*)")], fields=[
    field("id"), field("tod", datatype="Time"), field("ts", datatype="DateTime"),
    field("lit", "TIME '10:30:00'", datatype="Time"), field("made", "CAST('10:30:00' AS TIME)", datatype="Time"),
    field("clock", "CURRENT_TIME", datatype="Time"), field("clock_call", "CURRENT_TIME()", datatype="Time"),
    field("ts_time", "CAST(ts AS TIME)", datatype="Time")])])
#: A strict filter using the constructs and the rows it keeps.
FILTERS = {
    "t.tod > TIME '12:00:00'": 1,
    "t.tod = TIME '09:30:00'": 2,
    "CAST(t.ts AS TIME) < CAST('10:00:00' AS TIME)": 1,
    "t.tod <= CURRENT_TIME OR t.tod > CURRENT_TIME": 3,
    "t.tod <= CURRENT_TIME() OR t.tod > CURRENT_TIME()": 3,
    "CAST('10:30:00' AS TIME) = TIME '10:30:00'": 3,
    "t.lit = t.made AND t.ts_time >= TIME '10:15:00'": 2,
}


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with sql_engine(tmp_path, sql=SQL, documents=[DOC]) as eng:
        yield eng


def _cells(response: Any) -> list[list[Any]]:
    return [[row[c] for c in response.columns] for row in response.data]


class TestStrict:
    async def test_the_three_constructs_as_dimensions(self, engine: SlayerQueryEngine) -> None:
        response = await strict(engine, {"dimensions": ["t.clock", "t.clock_call", "t.lit", "t.made"]})
        [[clock, clock_call, lit, made]] = _cells(response)
        assert type(clock) is time, clock
        assert type(clock_call) is time, clock_call
        assert is_time(lit, time(10, 30)), lit
        assert is_time(made, time(10, 30)), made

    @pytest.mark.parametrize("filter_text", sorted(FILTERS))
    async def test_the_constructs_in_a_filter(self, engine: SlayerQueryEngine, filter_text: str) -> None:
        query = {"metrics": ["t.n"], "filter": filter_text}
        assert _cells(await strict(engine, query)) == [[FILTERS[filter_text]]]
        assert _cells(await engine.execute({"population": "t", **query})) == [[FILTERS[filter_text]]]

    async def test_a_time_field_as_a_dimension(self, engine: SlayerQueryEngine) -> None:
        response = await strict(engine, {"dimensions": ["t.tod"], "metrics": ["t.n"]})
        assert sorted(_cells(response)) == [[time(9, 30), 2], [time(17, 0), 1]]


class TestOssieDocumentField:
    async def test_grouped_and_aggregated(self, engine: SlayerQueryEngine) -> None:
        grouped = await engine.execute({"population": "t", "dimensions": ["t.tod"],
                                        "metrics": [{"metric": "count(t.*)", "alias": "n"}]})
        ends = await engine.execute({"population": "t", "metrics": [{"metric": "min(t.tod)", "alias": "lo"},
                                                                     {"metric": "max(t.tod)", "alias": "hi"}]})
        assert sorted((r["t.tod"], r["n"]) for r in grouped.data) == [(time(9, 30), 2), (time(17, 0), 1)]
        assert ends.data == [{"lo": time(9, 30), "hi": time(17, 0)}]

    async def test_the_stored_type_is_kept(self, engine: SlayerQueryEngine) -> None:
        stored = await engine.storage.get_dataset("t", data_source=DS)
        assert stored is not None
        types = {f.name: str(f.datatype) for f in stored.fields}
        assert {types[n] for n in ("tod", "lit", "made", "clock", "ts_time")} == {"Time"}


class TestProfiling:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_time_field_is_profiled_as_a_range(self, backend: str) -> None:
        async with shifts_engine(backend) as eng:
            stored = await eng.storage.get_dataset("shifts", data_source="test")
            assert stored is not None
            out = await render_dataset_inspection(dataset=stored, storage=eng.storage, engine=eng, format="json",
                                                  sections=["fields"])
        rendered = {f["name"]: f["sampled"] for f in json.loads(out)["fields"]}
        assert rendered["starts_at"] == "09:30:00 .. 17:00:00"
