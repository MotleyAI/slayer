"""The client's per-engine ClickHouse server profile, on the real HTTP driver (fake server)."""

from __future__ import annotations

import asyncio
import threading
import warnings
from collections.abc import Callable

import pytest

from slayer.core.models import DatasourceConfig
from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects.base import ServerProfile
from tests._ch_fake_http import (
    CH_TIMEOUT_KEY,
    CORRELATED_SQL,
    PROFILE_SQL,
    FakeClickHouse,
    ch_datasource,
    fake_ch_engine,
    route_engine,
)


def _client() -> SlayerSQLClient:
    return SlayerSQLClient(datasource=ch_datasource())


def _profile(client: SlayerSQLClient) -> ServerProfile:
    return asyncio.run(client.server_profile())


def _within(seconds: float, fn: Callable[[], object]) -> None:
    """Run ``fn`` on a daemon thread; fail on a hang (deadlock) instead of blocking the suite."""
    errors: list[BaseException] = []

    def _run() -> None:
        try:
            fn()
        except BaseException as exc:  # noqa: BLE001 — re-raised below
            errors.append(exc)

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout=seconds)
    assert not worker.is_alive(), "call hung (deadlock?)"
    if errors:
        raise errors[0]


class TestProfileContents:
    def test_full_profile_on_25_8(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=1, correlated=True)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            profile = _profile(_client())
        assert profile == ServerProfile(version=(25, 8), readonly=1, correlated_subqueries=True)
        assert fake.statements() == [PROFILE_SQL, CORRELATED_SQL]

    @pytest.mark.parametrize("version", ["24.8.14.10459", "25.3.1.1"])
    def test_no_correlated_statement_below_25_4(self, monkeypatch, version: str) -> None:
        fake = FakeClickHouse(version=version, readonly=1)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            profile = _profile(_client())
        assert profile.correlated_subqueries is None
        assert profile.readonly == 1
        assert fake.statements() == [PROFILE_SQL]

    @pytest.mark.parametrize(("version", "parsed"), [("25.4.1.1", (25, 4)), ("26.1.2.3", (26, 1))])
    def test_correlated_statement_from_25_4(self, monkeypatch, version: str, parsed: tuple[int, int]) -> None:
        fake = FakeClickHouse(version=version, readonly=1, correlated=False)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            profile = _profile(_client())
        assert profile == ServerProfile(version=parsed, readonly=1, correlated_subqueries=False)
        assert fake.correlated_checks() == 1

    def test_profile_probes_carry_no_timeout_setting(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1")
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            _client().execute_sync("SELECT 1", timeout_seconds=30)
        assert CH_TIMEOUT_KEY not in fake.params_for(PROFILE_SQL)[0]
        assert CH_TIMEOUT_KEY not in fake.params_for(CORRELATED_SQL)[0]
        assert fake.params_for("SELECT 1")[0][CH_TIMEOUT_KEY] == 30

    def test_non_clickhouse_has_empty_profile(self) -> None:
        client = SlayerSQLClient(datasource=DatasourceConfig(name="mem", type="sqlite", database=":memory:"))
        try:
            assert _profile(client) == ServerProfile()
        finally:
            client.close()


class TestCorrelatedProbeFailure:
    def test_field_unknown_and_timeout_still_applies(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=0, fail_correlated=True)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = _client()
            with warnings.catch_warnings():
                warnings.simplefilter("error", SlayerStatementTimeoutSkippedWarning)
                result = client.execute_sync("SELECT 1", timeout_seconds=30)
            profile = _profile(client)
        assert result.warnings == []
        assert fake.params_for("SELECT 1")[0][CH_TIMEOUT_KEY] == 30
        assert profile.version == (25, 8)
        assert profile.readonly == 0
        assert profile.correlated_subqueries is None


class TestBaseProbeFailure:
    def test_propagates_and_is_not_cached(self, monkeypatch) -> None:
        fake = FakeClickHouse(fail_permission=True)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = _client()
            for _ in range(2):
                with pytest.raises(Exception, match="permission probe failed"):
                    _profile(client)
        assert fake.permission_checks() == 2


class TestOneFillPerEngine:
    def test_execution_fill_reused_by_server_profile(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=1, correlated=True)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            _client().execute_sync("SELECT 1")
            profile = _profile(_client())
        assert profile == ServerProfile(version=(25, 8), readonly=1, correlated_subqueries=True)
        assert (fake.permission_checks(), fake.correlated_checks()) == (1, 1)

    def test_server_profile_fill_reused_by_execution(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=1, correlated=True)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            _profile(_client())
            with warnings.catch_warnings(record=True) as record:
                warnings.simplefilter("always")
                result = _client().execute_sync("SELECT 1", timeout_seconds=30)
        assert [w.reason for w in result.warnings] == ["readonly_user"]
        assert any(isinstance(w.message, SlayerStatementTimeoutSkippedWarning) for w in record)
        assert CH_TIMEOUT_KEY not in fake.params_for("SELECT 1")[0]
        assert (fake.permission_checks(), fake.correlated_checks()) == (1, 1)

    def test_concurrent_threads_fill_once(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=1, correlated=True, permission_delay=0.05)
        errors: list[BaseException] = []
        barrier = threading.Barrier(8, timeout=10)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = _client()

            def _worker(index: int) -> None:
                try:
                    barrier.wait()
                    if index % 2:
                        client.execute_sync("SELECT 1")
                    else:
                        _profile(client)
                except BaseException as exc:  # noqa: BLE001 — surfaced below
                    errors.append(exc)

            threads = [threading.Thread(target=_worker, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
            assert not any(t.is_alive() for t in threads)
        assert errors == []
        assert (fake.permission_checks(), fake.correlated_checks()) == (1, 1)


class TestNoReentry:
    def test_sync_execution_fill_does_not_deadlock(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=0, correlated=False)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = _client()
            _within(10, lambda: client.execute_sync("SELECT 1", timeout_seconds=30))
        assert fake.statements() == [PROFILE_SQL, CORRELATED_SQL, "SELECT 1"]

    def test_async_execution_fill_does_not_deadlock(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=0, correlated=False)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = _client()
            _within(10, lambda: asyncio.run(client.execute("SELECT 1", timeout_seconds=30)))
        assert fake.statements() == [PROFILE_SQL, CORRELATED_SQL, "SELECT 1"]

    def test_server_profile_fill_does_not_deadlock(self, monkeypatch) -> None:
        fake = FakeClickHouse(version="25.8.3.1", readonly=0, correlated=False)
        with fake_ch_engine(fake) as engine:
            route_engine(monkeypatch, engine)
            client = _client()
            _within(10, lambda: _profile(client))
        assert fake.statements() == [PROFILE_SQL, CORRELATED_SQL]
