"""A time-of-day result cell is a time of day, whatever the driver returns (spec: queries/time-of-day › A
time-of-day result is a time of day)."""

from __future__ import annotations

import json
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import time, timedelta
from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.format import encode_json, encode_text
from slayer.core.models import DatasourceConfig
from slayer.database.client import ExecutionResult, SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import patch_profile
from tests._engine_helpers import owned_query_engine
from tests._time_of_day_fixtures import BACKENDS, ROWS, d, is_time, m, q, shifts_columns, shifts_dataset, shifts_engine

START = "shifts.starts_at"
#: Databases whose drivers return a time of day as text or as a duration.
SERVERS = ["mysql", "clickhouse", "postgres", "sqlite"]
#: A driver's value for a time-of-day column and the time of day it is.
DRIVER_FORMS: list[tuple[Any, time | None]] = [
    ("10:30:00", time(10, 30)),
    ("10:30:00.000000", time(10, 30)),
    ("10:30:00.123456", time(10, 30, 0, 123456)),
    ("00:00:00", time(0, 0)),
    ("23:59:59.999999", time(23, 59, 59, 999999)),
    (timedelta(hours=10, minutes=30), time(10, 30)),
    (timedelta(0), time(0, 0)),
    (timedelta(hours=23, minutes=59, seconds=59, microseconds=999999), time(23, 59, 59, 999999)),
    (time(10, 30), time(10, 30)),
    (time(10, 30, 0, 500000), time(10, 30, 0, 500000)),
    (None, None),
]
#: A driver's value that is no time of day, and the text the error names it by.
NOT_A_TIME: list[tuple[Any, str]] = [
    (timedelta(hours=25), "25:00:00"),
    (timedelta(hours=24), "24:00:00"),
    (-timedelta(hours=1, minutes=30), "-01:30:00"),
    ("25:00:00", "25:00:00"),
    ("24:00:00", "24:00:00"),
    ("-01:30:00", "-01:30:00"),
    ("noon", "noon"),
    ("2024-01-15 10:30:00", "2024-01-15 10:30:00"),
]


@asynccontextmanager
async def served(monkeypatch: pytest.MonkeyPatch, ds_type: str, row: dict[str, Any]) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine on a ``ds_type`` datasource whose database answers every statement with ``row``."""

    async def answer(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
        return ExecutionResult(rows=[dict(row)])

    monkeypatch.setattr(SlayerSQLClient, "execute", answer)
    patch_profile(monkeypatch, version=(99, 0, 0))
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        ds = DatasourceConfig.model_validate({"name": "test", "type": ds_type})
        async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
            await storage.save_dataset(shifts_dataset(), _validate=False)
            yield engine


def _id(value: Any) -> str:
    return repr(value)


class TestDriverForms:
    @pytest.mark.parametrize("ds_type", SERVERS)
    @pytest.mark.parametrize(("driver", "expected"), DRIVER_FORMS, ids=[_id(v) for v, _ in DRIVER_FORMS])
    async def test_a_field_cell(self, monkeypatch: pytest.MonkeyPatch, ds_type: str, driver: Any,
                                expected: time | None) -> None:
        async with served(monkeypatch, ds_type, {START: driver}) as engine:
            resp = await engine.execute(q(dimensions=[START]))
        [row] = resp.data
        assert row[START] is None if expected is None else is_time(row[START], expected), row

    @pytest.mark.parametrize("ds_type", SERVERS)
    @pytest.mark.parametrize("entry", [
        {"dimensions": [d("TIME '10:30:00'")]}, {"dimensions": [d("CAST(shifts.label AS TIME)")]},
        {"dimensions": [d(f"coalesce(shifts.ends_at, {START})")]}, {"metrics": [m(f"min({START})")]},
        {"metrics": [m(f"last({START}, shifts.created_at)")]},
    ], ids=["literal", "cast", "conditional", "min", "last"])
    @pytest.mark.parametrize("driver", ["10:30:00", timedelta(hours=10, minutes=30)], ids=["text", "duration"])
    async def test_a_computed_cell(self, monkeypatch: pytest.MonkeyPatch, ds_type: str, entry: dict[str, Any],
                                   driver: Any) -> None:
        async with served(monkeypatch, ds_type, {"v": driver}) as engine:
            resp = await engine.execute(q(**entry))
        assert is_time(resp.data[0]["v"], time(10, 30)), resp.data
        assert json.loads(encode_json(resp.data)) == [{"v": "10:30:00"}]

    @pytest.mark.parametrize("ds_type", SERVERS)
    @pytest.mark.parametrize(("driver", "named"), NOT_A_TIME, ids=[_id(v) for v, _ in NOT_A_TIME])
    async def test_a_value_that_is_no_time_of_day_fails_the_query(self, monkeypatch: pytest.MonkeyPatch, ds_type: str,
                                                                  driver: Any, named: str) -> None:
        query = q(dimensions=[START])
        async with served(monkeypatch, ds_type, {START: driver}) as engine:
            with pytest.raises(SlayerError) as info:
                await engine.execute(query)
        msg = str(info.value)
        assert START in msg, msg
        assert named in msg, msg

    @pytest.mark.parametrize("ds_type", SERVERS)
    async def test_the_error_names_a_computed_column(self, monkeypatch: pytest.MonkeyPatch, ds_type: str) -> None:
        query = q(metrics=[m(f"max({START})", "opens")])
        async with served(monkeypatch, ds_type, {"opens": timedelta(hours=30)}) as engine:
            with pytest.raises(SlayerError) as info:
                await engine.execute(query)
        assert "opens" in str(info.value)
        assert "30:00:00" in str(info.value)

    @pytest.mark.parametrize("ds_type", SERVERS)
    async def test_other_columns_are_left_alone(self, monkeypatch: pytest.MonkeyPatch, ds_type: str) -> None:
        row = {"shifts.label": "10:30:00", "span": "25:00:00", START: "09:30:00"}
        async with served(monkeypatch, ds_type, row) as engine:
            resp = await engine.execute(q(dimensions=["shifts.label", d("shifts.label || ''", "span"), START]))
        [got] = resp.data
        assert (got["shifts.label"], got["span"]) == ("10:30:00", "25:00:00")
        assert is_time(got[START], time(9, 30)), got

    @pytest.mark.parametrize("ds_type", ["clickhouse", "postgres"])
    async def test_a_list_of_times_cell(self, monkeypatch: pytest.MonkeyPatch, ds_type: str) -> None:
        async with served(monkeypatch, ds_type, {"v": ["09:30:00", timedelta(hours=17)]}) as engine:
            resp = await engine.execute(q(metrics=[m(f"collect({START})")]))
        assert resp.data[0]["v"] == [time(9, 30), time(17, 0)]


def text_engine(*times: str | None):
    """A SQLite engine over shifts whose ``starts_at`` text column holds ``times``."""
    rows = [(i, t, None, *ROWS[0][3:]) for i, t in enumerate(times, start=1)]
    return shifts_engine("sqlite", rows=rows, columns=shifts_columns("sqlite"))


class TestSqliteText:

    async def test_stored_text_is_read_as_a_time_of_day(self) -> None:
        async with text_engine("09:30:00", "17:00:00.250", "09:30", None) as engine:
            resp = await engine.execute(q(dimensions=["shifts.id", START]))
            low = await engine.execute(q(metrics=[m(f"min({START})", "lo"), m(f"max({START})", "hi")]))
            kept = await engine.execute(q(dimensions=["shifts.id"], filter=[f"{START} > TIME '09:30:00'"]))
        assert {int(r["shifts.id"]): r[START] for r in resp.data} == {
            1: time(9, 30), 2: time(17, 0, 0, 250000), 3: time(9, 30), 4: None}
        assert is_time(low.data[0]["lo"], time(9, 30)), low.data
        assert is_time(low.data[0]["hi"], time(17, 0, 0, 250000)), low.data
        assert {int(r["shifts.id"]) for r in kept.data} == {2}

    async def test_one_group_for_one_time_in_two_spellings(self) -> None:
        async with text_engine("09:30:00", "09:30", "09:30:00.000") as engine:
            resp = await engine.execute(q(dimensions=[START], metrics=[m("count(shifts.*)", "n")]))
        assert [(r[START], int(r["n"])) for r in resp.data] == [(time(9, 30), 3)]

    @pytest.mark.parametrize("stored", ["noon", "25:00:00", "24:00:00", "2024-01-15", "9h30"])
    async def test_text_that_names_no_time_fails_the_query(self, stored: str) -> None:
        query = q(dimensions=["shifts.id", START])
        async with text_engine("09:30:00", stored) as engine:
            with pytest.raises(SlayerError) as info:
                await engine.execute(query)
        msg = str(info.value)
        assert START in msg, msg
        assert stored in msg, msg


class TestStages:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_stage_column_stays_a_time_of_day(self, backend: str) -> None:
        stage = {"name": "s1", "population": "shifts", "dimensions": ["shifts.id", START, d("TIME '10:30'", "lit")],
                 "metrics": [m("max(shifts.ends_at)", "hi")]}
        final = {"population": "s1", "dimensions": ["s1.id", "s1.starts_at", "s1.lit", "s1.hi"],
                 "filter": ["s1.starts_at < '12:00'"]}
        async with shifts_engine(backend) as engine:
            resp = await engine.execute([stage, final])
        rows = {int(r["s1.id"]): (r["s1.starts_at"], r["s1.lit"], r["s1.hi"]) for r in resp.data}
        assert rows == {1: (time(9, 30), time(10, 30), time(17, 0)), 2: (time(9, 30), time(10, 30), None)}
        assert all(v is None or type(v) is time for row in rows.values() for v in row), rows


class TestCodec:
    @pytest.mark.parametrize(("value", "form"), [
        (time(10, 30), "10:30:00"), (time(0, 0), "00:00:00"), (time(9, 5, 7), "09:05:07"),
        (time(10, 30, 0, 123456), "10:30:00.123456"),
    ])
    def test_json_and_text_forms(self, value: time, form: str) -> None:
        assert json.loads(encode_json(value)) == form
        assert encode_text(value) == form

    def test_a_list_of_times(self) -> None:
        assert json.loads(encode_json([time(9, 30), None, time(17, 0)])) == ["09:30:00", None, "17:00:00"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_response_serialises_a_time_cell(self, backend: str) -> None:
        async with shifts_engine(backend) as engine:
            resp = await engine.execute(q(dimensions=[START], filter=["shifts.id = 3"]))
        assert json.loads(encode_json(resp.data)) == [{START: "17:00:00"}]
        assert "17:00:00" in resp.to_markdown()
