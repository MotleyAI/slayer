"""ClickHouse answers a time of day from server 25.6: as written from 25.12, with the time-type setting sent on
25.6–25.11, and refused before any SQL on an older or unknown server (spec: queries/time-of-day › ClickHouse answers
from server 25.6)."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

import pytest
import sqlglot

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.database.client import ExecutionResult, SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._ch_fake_http import CORRELATED_SETTING, TIME_SETTING, FakeClickHouse, fake_ch_engine, route_engine
from tests._time_of_day_fixtures import d, m, q, shifts_dataset

CAST = q(dimensions=[d("CAST('10:30:00' AS TIME)")])
#: Statements that compute or read a time of day.
TIME_QUERIES: dict[str, SlayerQuery] = {
    "cast": CAST,
    "double-colon": q(dimensions=[d("shifts.label::TIME")]),
    "try-cast": q(dimensions=[d("TRY_CAST(shifts.label AS TIME)")]),
    "literal": q(dimensions=[d("TIME '10:30:00'")]),
    "clock": q(dimensions=[d("current_time()")]),
    "keyword": q(dimensions=["shifts.id"], filter=["CAST(shifts.created_at AS TIME) < CURRENT_TIME"]),
    "field": q(dimensions=["shifts.starts_at"]),
    "filter": q(dimensions=["shifts.id"], filter=["shifts.starts_at >= '09:00'"]),
    "aggregate": q(metrics=[m("min(shifts.starts_at)")]),
    "part": q(dimensions=[d("date_part('hour', shifts.starts_at)")]),
    "order": q(dimensions=["shifts.id"], order=[{"field": "shifts.ends_at"}]),
    "to-text": q(dimensions=[d("CAST(shifts.starts_at AS VARCHAR)")]),
}
NO_TIME = q(dimensions=["shifts.label"], metrics=[m("sum(shifts.amount)")], filter=["shifts.created_at >= '2024-01-16'"])
MODES = ["dry_run", "explain", "execute"]


class ChServer:
    """A fake ClickHouse answering the profile probes; user statements are recorded, never sent."""

    def __init__(self, *, monkeypatch: pytest.MonkeyPatch, stack: contextlib.ExitStack) -> None:
        self._monkeypatch = monkeypatch
        self._stack = stack
        self.executed: list[str] = []

    def start(self, **kw: Any) -> FakeClickHouse:
        fake = FakeClickHouse(**kw)
        route_engine(self._monkeypatch, self._stack.enter_context(fake_ch_engine(fake)))
        return fake


@pytest.fixture
def ch_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[ChServer]:
    with contextlib.ExitStack() as stack:
        server = ChServer(monkeypatch=monkeypatch, stack=stack)

        async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
            server.executed.append(sql)
            return ExecutionResult(rows=[])

        monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
        yield server


@pytest.fixture
async def engine(tmp_path) -> Any:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="clickhouse"))
    await storage.save_dataset(shifts_dataset(), _validate=False)
    eng = SlayerQueryEngine(storage=storage)
    try:
        yield eng
    finally:
        eng.close()


async def _sql(engine: SlayerQueryEngine, query: SlayerQuery) -> str:
    sql = (await engine.execute(query, dry_run=True)).sql
    assert sql is not None
    return sql


def _settings(sql: str) -> list[str]:
    return [s.sql() for s in sqlglot.parse_one(sql, dialect="clickhouse").args.get("settings") or []]


async def _run(engine: SlayerQueryEngine, query: SlayerQuery, mode: str) -> Any:
    return await engine.execute(query, dry_run=mode == "dry_run", explain=mode == "explain")


class TestDefaultOnServer:
    @pytest.mark.parametrize("version", ["25.12.1.1", "26.1.2.3"])
    @pytest.mark.parametrize("name", sorted(TIME_QUERIES))
    async def test_runs_as_written(self, ch_server: ChServer, engine: SlayerQueryEngine, version: str,
                                   name: str) -> None:
        ch_server.start(version=version)
        sql = await _sql(engine, TIME_QUERIES[name])
        assert TIME_SETTING not in sql, sql

    async def test_a_readonly_user_needs_nothing(self, ch_server: ChServer, engine: SlayerQueryEngine) -> None:
        ch_server.start(version="25.12.1.1", readonly=1)
        await engine.execute(CAST)
        assert len(ch_server.executed) == 1
        assert TIME_SETTING not in ch_server.executed[0]


class TestSettingSent:
    @pytest.mark.parametrize("version", ["25.6.1.1", "25.8.3.1", "25.11.9.9"])
    @pytest.mark.parametrize("name", sorted(TIME_QUERIES))
    async def test_the_statement_enables_the_setting(self, ch_server: ChServer, engine: SlayerQueryEngine,
                                                     version: str, name: str) -> None:
        ch_server.start(version=version)
        sql = await _sql(engine, TIME_QUERIES[name])
        assert sql.count("SETTINGS") == 1, sql
        assert any(TIME_SETTING in s and s.replace(" ", "").endswith("=1") for s in _settings(sql)), sql
        assert CORRELATED_SETTING not in sql, sql

    @pytest.mark.parametrize("mode", MODES)
    async def test_sent_in_every_mode(self, ch_server: ChServer, engine: SlayerQueryEngine, mode: str) -> None:
        ch_server.start(version="25.8.3.1")
        resp = await _run(engine, CAST, mode)
        sent = ch_server.executed if mode != "dry_run" else [resp.sql or ""]
        assert sent, mode
        assert all(TIME_SETTING in s for s in sent), sent

    @pytest.mark.parametrize(("readonly", "already_on"), [(1, True), (2, False), (0, False)])
    async def test_a_user_who_may_run_it_runs_it(self, ch_server: ChServer, engine: SlayerQueryEngine,
                                                 readonly: int, already_on: bool) -> None:
        ch_server.start(version="25.8.3.1", readonly=readonly, time_types=already_on)
        await engine.execute(CAST)
        assert len(ch_server.executed) == 1
        assert TIME_SETTING in ch_server.executed[0]


class TestOldServer:
    @pytest.mark.parametrize("version", ["25.4.1.1", "25.5.9.9", "24.3.1.1"])
    @pytest.mark.parametrize("mode", MODES)
    async def test_refused_before_any_sql(self, ch_server: ChServer, engine: SlayerQueryEngine, version: str,
                                          mode: str) -> None:
        fake = ch_server.start(version=version)
        with pytest.raises(SlayerError) as info:
            await _run(engine, CAST, mode)
        msg = str(info.value)
        assert "25.6" in msg, msg
        assert "time" in msg.lower(), msg
        assert ".".join(version.split(".")[:2]) in msg, msg
        assert ch_server.executed == []
        assert fake.user_statements() == []

    @pytest.mark.parametrize("name", sorted(TIME_QUERIES))
    async def test_every_time_of_day_statement_is_refused(self, ch_server: ChServer, engine: SlayerQueryEngine,
                                                          name: str) -> None:
        ch_server.start(version="25.4.1.1")
        with pytest.raises(SlayerError, match=r"25\.6"):
            await _sql(engine, TIME_QUERIES[name])

    @pytest.mark.parametrize("mode", MODES)
    async def test_an_unknown_version_is_refused(self, ch_server: ChServer, engine: SlayerQueryEngine,
                                                 mode: str) -> None:
        fake = ch_server.start(fail_permission=True)
        with pytest.raises(SlayerError) as info:
            await _run(engine, CAST, mode)
        assert "25.6" in str(info.value), str(info.value)
        assert ch_server.executed == []
        assert fake.user_statements() == []

    async def test_a_failed_probe_is_rechecked_by_the_next_query(self, ch_server: ChServer,
                                                                 engine: SlayerQueryEngine) -> None:
        fake = ch_server.start(fail_permission=True)
        for _ in range(2):
            with pytest.raises(SlayerError):
                await _sql(engine, CAST)
        assert fake.permission_checks() == 2


class TestSettingForbidden:
    @pytest.mark.parametrize("mode", MODES)
    async def test_refused_naming_the_setting(self, ch_server: ChServer, engine: SlayerQueryEngine, mode: str) -> None:
        fake = ch_server.start(version="25.8.3.1", readonly=1, time_types=False)
        with pytest.raises(SlayerError) as info:
            await _run(engine, CAST, mode)
        msg = str(info.value)
        assert TIME_SETTING in msg, msg
        assert "readonly" in msg, msg
        assert ch_server.executed == []
        assert fake.user_statements() == []


class TestNoTimeOfDay:
    @pytest.mark.parametrize("version", ["24.3.1.1", "25.8.3.1", "25.12.1.1"])
    async def test_a_query_without_one_is_unaffected(self, ch_server: ChServer, engine: SlayerQueryEngine,
                                                     version: str) -> None:
        fake = ch_server.start(version=version)
        sql = await _sql(engine, NO_TIME)
        assert TIME_SETTING not in sql, sql
        assert "SETTINGS" not in sql, sql
        assert fake.permission_checks() == 0

    async def test_it_runs_for_a_readonly_user_on_an_old_server(self, ch_server: ChServer,
                                                                engine: SlayerQueryEngine) -> None:
        ch_server.start(version="24.3.1.1", readonly=1)
        await engine.execute(NO_TIME)
        assert len(ch_server.executed) == 1


class TestRenderedType:
    @pytest.mark.parametrize("name", ["cast", "double-colon", "try-cast", "literal", "clock"])
    async def test_a_computed_time_of_day_is_time64_6(self, ch_server: ChServer, engine: SlayerQueryEngine,
                                                      name: str) -> None:
        ch_server.start(version="25.12.1.1")
        sql = await _sql(engine, TIME_QUERIES[name])
        assert "Time64(6)" in sql, sql

    async def test_a_reflected_column_is_read_as_it_is(self, ch_server: ChServer, engine: SlayerQueryEngine) -> None:
        ch_server.start(version="25.12.1.1")
        sql = await _sql(engine, TIME_QUERIES["field"])
        assert "Time64" not in sql, sql

    async def test_the_parts_are_not_the_timestamp_functions(self, ch_server: ChServer,
                                                             engine: SlayerQueryEngine) -> None:
        ch_server.start(version="25.12.1.1")
        sql = await _sql(engine, TIME_QUERIES["part"])
        assert "toHour(" not in sql, sql
        assert "EXTRACT(" not in sql.upper(), sql
