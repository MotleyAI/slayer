"""A fake ClickHouse HTTP server behind the real ``clickhouse-sqlalchemy`` driver.

The real DBAPI connection and its ``transport.ch_settings`` stay in play; only the
``requests`` session is replaced, so each request's body and URL params are recorded.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
import sqlalchemy as sa

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from tests._engine_helpers import disposable_engine

pytest.importorskip("clickhouse_sqlalchemy")

CH_TIMEOUT_KEY = "max_execution_time"
CORRELATED_SETTING = "allow_experimental_correlated_subqueries"
PROFILE_SQL = "SELECT version(), getSetting('readonly')"
CORRELATED_SQL = f"SELECT getSetting('{CORRELATED_SETTING}')"


class FakeResponse:
    def __init__(self, *, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.text = body

    def iter_lines(self) -> Iterator[bytes]:
        return iter(self.text.encode("utf-8").split(b"\n"))


def _tsv(names: list[str], types: list[str], rows: list[list[str]]) -> str:
    return "\n".join("\t".join(line) for line in [names, types, *rows])


class FakeClickHouse:
    """Answers every request; records ``(sql, params)`` per request in order."""

    def __init__(self, *, readonly: int = 0, fail_marker: str | None = None,
                 fail_permission: bool = False, permission_delay: float = 0.0,
                 version: str = "24.3.1.1", correlated: bool = False,
                 fail_correlated: bool = False) -> None:
        self.readonly = readonly
        self.fail_marker = fail_marker
        self.fail_permission = fail_permission
        self.permission_delay = permission_delay
        self.version = version
        self.correlated = correlated
        self.fail_correlated = fail_correlated
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self._lock = threading.Lock()

    def post(self, url: str, *, data: bytes, params: dict[str, Any], **_kw: Any) -> FakeResponse:
        sql = data.decode("utf-8")
        with self._lock:
            self.requests.append((sql, dict(params)))
        if sql == PROFILE_SQL:
            time.sleep(self.permission_delay)
            if self.fail_permission:
                return FakeResponse(status_code=500, body="Code: 999. permission probe failed")
            return FakeResponse(status_code=200, body=_tsv(
                ["version()", "getSetting('readonly')"], ["String", "UInt64"], [[self.version, str(self.readonly)]],
            ))
        if sql == CORRELATED_SQL:
            if self.fail_correlated:
                return FakeResponse(status_code=500, body="Code: 999. correlated probe failed")
            return FakeResponse(status_code=200, body=_tsv(
                [f"getSetting('{CORRELATED_SETTING}')"], ["Bool"], [["true" if self.correlated else "false"]],
            ))
        if self.readonly == 1 and CH_TIMEOUT_KEY in params:
            return FakeResponse(status_code=500, body="Code: 164. DB::Exception: Cannot modify setting in readonly mode. (READONLY)")
        if self.readonly == 1 and not self.correlated and f"{CORRELATED_SETTING} = 1" in sql:
            return FakeResponse(status_code=500, body="Code: 164. DB::Exception: Cannot modify setting in readonly mode. (READONLY)")
        if self.fail_marker is not None and self.fail_marker in sql:
            return FakeResponse(status_code=500, body="Code: 159. DB::Exception: boom. (TIMEOUT_EXCEEDED)")
        if sql.lower() == "select version()":
            return FakeResponse(status_code=200, body=_tsv(["v"], ["String"], [[self.version]]))
        if sql.lower() == "select currentdatabase()":
            return FakeResponse(status_code=200, body=_tsv(["d"], ["String"], [["default"]]))
        return FakeResponse(status_code=200, body=_tsv(["x"], ["UInt8"], [["1"]]))

    def statements(self) -> list[str]:
        """User-issued statements (driver bootstrap queries excluded)."""
        boot = {"select version()", "select currentdatabase()"}
        return [sql for sql, _ in self.requests if sql.lower() not in boot]

    def params_for(self, sql: str) -> list[dict[str, Any]]:
        return [params for s, params in self.requests if s == sql]

    def permission_checks(self) -> int:
        return len(self.params_for(PROFILE_SQL))

    def correlated_checks(self) -> int:
        return len(self.params_for(CORRELATED_SQL))

    def user_statements(self) -> list[str]:
        """Statements other than the server-profile probes."""
        return [sql for sql in self.statements() if sql not in (PROFILE_SQL, CORRELATED_SQL)]


def ch_datasource(name: str = "fake_ch") -> DatasourceConfig:
    return DatasourceConfig(
        name=name, type="clickhouse", host="ch.invalid", port=8123,
        database="default", username="u", password="p",
    )


@contextmanager
def fake_ch_engine(
    fake: FakeClickHouse, *, ch_settings: dict[str, Any] | None = None,
) -> Generator[sa.Engine]:
    connect_args: dict[str, Any] = {"http_session": fake}
    if ch_settings is not None:
        connect_args["ch_settings"] = ch_settings
    with disposable_engine("clickhouse+http://u:p@ch.invalid:8123/default", connect_args=connect_args) as engine:
        yield engine


def pooled_ch_settings(engine: sa.Engine) -> dict[str, Any]:
    """The ``ch_settings`` of the (single) pooled DBAPI connection."""
    with engine.connect() as conn:
        return dict(conn.connection.dbapi_connection.transport.ch_settings)  # pyright: ignore[reportOptionalMemberAccess]


def route_engine(monkeypatch: pytest.MonkeyPatch, engine: sa.Engine) -> None:
    """Make the engine factory hand ``engine`` to every client."""
    monkeypatch.setattr(engine_factory, "get_engine", lambda _ds: engine)
