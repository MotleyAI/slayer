"""ClickHouse statement timeout as a per-request setting on the real HTTP driver (fake server)."""

from __future__ import annotations

import asyncio
import threading
import warnings

import pytest

from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning, StatementTimeoutSkippedWarning
from slayer.sql.client import SlayerSQLClient, _TYPE_PROBE_TIMEOUT_SECONDS
from tests._ch_fake_http import (
    CH_TIMEOUT_KEY,
    PERMISSION_SQL,
    FakeClickHouse,
    ch_datasource,
    fake_ch_engine,
    pooled_ch_settings,
    route_engine,
)

# ClickHouse-specific spellings a sqlglot round-trip would rewrite.
VERBATIM_SQL = "SELECT toStartOfMonth(d) AS m, x::Int32 AS y FROM t -- trailing comment"


def _skipped(record: list[warnings.WarningMessage]) -> list[StatementTimeoutSkippedWarning]:
    return [w.message.payload for w in record if isinstance(w.message, SlayerStatementTimeoutSkippedWarning)]


def _readonly_payload(timeout: int) -> StatementTimeoutSkippedWarning:
    return StatementTimeoutSkippedWarning(datasource="fake_ch", timeout_seconds=timeout, reason="readonly_user")


@pytest.fixture(params=["sync", "async"])
def execute(request: pytest.FixtureRequest):
    def _sync(client: SlayerSQLClient, sql: str, timeout: int):
        return client.execute_sync(sql, timeout_seconds=timeout)

    def _async(client: SlayerSQLClient, sql: str, timeout: int):
        return asyncio.run(client.execute(sql, timeout_seconds=timeout))

    return _sync if request.param == "sync" else _async


class TestTimeoutTravelsBesideTheStatement:
    def test_statement_bytes_unchanged_and_setting_in_request(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse()
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            result = execute(SlayerSQLClient(datasource=ch_datasource()), VERBATIM_SQL, 30)
        assert result.rows == [{"x": 1}]
        (params,) = fake.params_for(VERBATIM_SQL)
        assert params[CH_TIMEOUT_KEY] == 30
        assert set(fake.statements()) == {PERMISSION_SQL, VERBATIM_SQL}

    def test_sql_own_setting_sent_unchanged(self, monkeypatch, execute) -> None:
        sql = "SELECT sleep(2) SETTINGS max_execution_time = 5"
        fake = FakeClickHouse()
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            execute(SlayerSQLClient(datasource=ch_datasource()), sql, 1)
        (params,) = fake.params_for(sql)
        assert params[CH_TIMEOUT_KEY] == 1

    def test_setting_removed_after_call(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse()
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            execute(SlayerSQLClient(datasource=ch_datasource()), "SELECT 1", 30)
            assert CH_TIMEOUT_KEY not in pooled_ch_settings(engine)

    def test_prior_value_restored_after_call(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse()
        with fake_ch_engine(fake, ch_settings={CH_TIMEOUT_KEY: 77}) as engine:
            route_engine(monkeypatch, engine)
            execute(SlayerSQLClient(datasource=ch_datasource()), "SELECT 1", 30)
            assert pooled_ch_settings(engine)[CH_TIMEOUT_KEY] == 77
        assert fake.params_for("SELECT 1")[0][CH_TIMEOUT_KEY] == 30

    def test_prior_value_restored_when_statement_raises(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse(fail_marker="boom_marker")
        with fake_ch_engine(fake, ch_settings={CH_TIMEOUT_KEY: 77}) as engine:
            route_engine(monkeypatch, engine)
            client = SlayerSQLClient(datasource=ch_datasource())
            with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
                execute(client, "SELECT 'boom_marker'", 30)
            assert pooled_ch_settings(engine)[CH_TIMEOUT_KEY] == 77

    def test_absence_restored_when_statement_raises(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse(fail_marker="boom_marker")
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = SlayerSQLClient(datasource=ch_datasource())
            with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
                execute(client, "SELECT 'boom_marker'", 30)
            assert CH_TIMEOUT_KEY not in pooled_ch_settings(engine)


class TestReadonlyUsers:
    def test_readonly_1_runs_without_setting_and_warns(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse(readonly=1)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = SlayerSQLClient(datasource=ch_datasource())
            for _ in range(2):
                with warnings.catch_warnings(record=True) as record:
                    warnings.simplefilter("always")
                    result = execute(client, "SELECT 1", 30)
                assert result.rows == [{"x": 1}]
                assert result.warnings == [_readonly_payload(30)]
                assert _skipped(record) == [_readonly_payload(30)]
        assert fake.permission_checks() == 1
        assert all(CH_TIMEOUT_KEY not in p for p in fake.params_for("SELECT 1"))

    def test_readonly_2_gets_timeout_without_warning(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse(readonly=2)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            with warnings.catch_warnings(record=True) as record:
                warnings.simplefilter("always")
                result = execute(SlayerSQLClient(datasource=ch_datasource()), "SELECT 1", 30)
        assert result.warnings == []
        assert _skipped(record) == []
        assert fake.params_for("SELECT 1")[0][CH_TIMEOUT_KEY] == 30

    def test_permission_checked_before_setting(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse()
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            execute(SlayerSQLClient(datasource=ch_datasource()), "SELECT 1", 30)
        assert fake.statements() == [PERMISSION_SQL, "SELECT 1"]
        assert CH_TIMEOUT_KEY not in fake.params_for(PERMISSION_SQL)[0]

    def test_checked_once_per_engine_across_clients_and_paths(self, monkeypatch) -> None:
        fake = FakeClickHouse(readonly=1)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            SlayerSQLClient(datasource=ch_datasource()).execute_sync("SELECT 1")
            asyncio.run(SlayerSQLClient(datasource=ch_datasource()).execute("SELECT 1"))
            asyncio.run(SlayerSQLClient(datasource=ch_datasource()).get_column_types("SELECT 1"))
        assert fake.permission_checks() == 1

    def test_each_engine_checks_on_its_own(self, monkeypatch) -> None:
        fake = FakeClickHouse(readonly=1)
        for _ in range(2):
            with fake_ch_engine(fake) as engine:
                route_engine(monkeypatch, engine)
                SlayerSQLClient(datasource=ch_datasource()).execute_sync("SELECT 1")
        assert fake.permission_checks() == 2

    def test_checked_once_under_concurrent_threads(self, monkeypatch) -> None:
        fake = FakeClickHouse(readonly=1, permission_delay=0.05)
        errors: list[BaseException] = []
        barrier = threading.Barrier(8, timeout=10)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = SlayerSQLClient(datasource=ch_datasource())

            def _worker() -> None:
                try:
                    barrier.wait()
                    client.execute_sync("SELECT 1")
                except BaseException as exc:  # noqa: BLE001 — surfaced below
                    errors.append(exc)

            threads = [threading.Thread(target=_worker) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
            assert not any(t.is_alive() for t in threads)
        assert errors == []
        assert fake.permission_checks() == 1

    def test_failing_permission_check_propagates(self, monkeypatch, execute) -> None:
        fake = FakeClickHouse(fail_permission=True)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = SlayerSQLClient(datasource=ch_datasource())
            with pytest.raises(Exception, match="permission probe failed"):
                execute(client, "SELECT 1", 30)
            with pytest.raises(Exception, match="permission probe failed"):
                execute(client, "SELECT 1", 30)
        assert "SELECT 1" not in fake.statements()
        assert fake.permission_checks() == 2


class TestTypeProbe:
    def test_probe_carries_timeout_setting(self, monkeypatch) -> None:
        fake = FakeClickHouse()
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            types = asyncio.run(SlayerSQLClient(datasource=ch_datasource()).get_column_types("SELECT 1 AS x"))
            assert CH_TIMEOUT_KEY not in pooled_ch_settings(engine)
        assert types == {"x": "number"}
        (probe,) = [s for s in fake.statements() if s != PERMISSION_SQL]
        assert "SELECT 1 AS x" in probe
        assert fake.params_for(probe)[0][CH_TIMEOUT_KEY] == _TYPE_PROBE_TIMEOUT_SECONDS

    def test_readonly_probe_runs_without_setting_and_warns(self, monkeypatch) -> None:
        fake = FakeClickHouse(readonly=1)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            with warnings.catch_warnings(record=True) as record:
                warnings.simplefilter("always")
                types = asyncio.run(SlayerSQLClient(datasource=ch_datasource()).get_column_types("SELECT 1 AS x"))
        assert types == {"x": "number"}
        (probe,) = [s for s in fake.statements() if s != PERMISSION_SQL]
        assert CH_TIMEOUT_KEY not in fake.params_for(probe)[0]
        assert _skipped(record) == [_readonly_payload(_TYPE_PROBE_TIMEOUT_SECONDS)]
