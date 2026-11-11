"""Harness law (python.database.arc42.md §3.3): every async engine a client hands out is disposed on its own loop.

A pytest plugin. It records each async engine ``SlayerSQLClient`` hands out (with its creating loop, datasource
and building test), each completed ``dispose()`` on that loop, and each abandonment of a closed loop's engine.
Once an engine's loop has closed, the teardown that follows judges it: neither disposed there nor (under
``abandons_async_engines``) abandoned fails that teardown, naming the test that built it.
"""

from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import Callable, Generator
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncEngine

from slayer.database import client as sql_client

MARKER = "abandons_async_engines"


class _Record(BaseModel):
    """One handed-out engine, held strongly until its loop is judged."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    engine: AsyncEngine
    loop: asyncio.AbstractEventLoop
    datasource: str
    test: str
    may_abandon: bool
    disposed: bool = False
    abandoned: bool = False


_lock = threading.Lock()
_records: dict[int, _Record] = {}
_current: pytest.Item | None = None
_originals: dict[str, Any] = {}


def _hand_out(original: Callable[[sql_client.SlayerSQLClient], AsyncEngine]) -> Callable:
    @functools.wraps(original)
    def get_async_engine(self: sql_client.SlayerSQLClient) -> AsyncEngine:
        engine = original(self)
        if type(engine) is AsyncEngine:
            item = _current
            with _lock:
                _records.setdefault(id(engine), _Record(
                    engine=engine, loop=asyncio.get_running_loop(), datasource=self.datasource.name,
                    test=item.nodeid if item is not None else "<outside a test>",
                    may_abandon=item is not None and item.get_closest_marker(MARKER) is not None,
                ))
        return engine
    return get_async_engine


def _dispose(original: Callable) -> Callable:
    @functools.wraps(original)
    async def dispose(self: AsyncEngine, close: bool = True) -> None:
        await original(self, close)
        with _lock:
            record = _records.get(id(self))
            if record is not None and close and record.loop is asyncio.get_running_loop():
                record.disposed = True
    return dispose


def _abandon(original: Callable[[list[AsyncEngine]], None]) -> Callable:
    @functools.wraps(original)
    def abandon(engines: list[AsyncEngine]) -> None:
        with _lock:
            for engine in engines:
                record = _records.get(id(engine))
                if record is not None:
                    record.abandoned = True
        original(engines)
    return abandon


def _install() -> None:
    _originals["get"] = sql_client.SlayerSQLClient._get_async_engine
    _originals["dispose"] = AsyncEngine.dispose
    _originals["abandon"] = sql_client._abandon
    sql_client.SlayerSQLClient._get_async_engine = _hand_out(_originals["get"])  # type: ignore[method-assign]
    AsyncEngine.dispose = _dispose(_originals["dispose"])  # type: ignore[method-assign]
    sql_client._abandon = _abandon(_originals["abandon"])


def _uninstall() -> None:
    sql_client.SlayerSQLClient._get_async_engine = _originals.pop("get")  # type: ignore[method-assign]
    AsyncEngine.dispose = _originals.pop("dispose")  # type: ignore[method-assign]
    sql_client._abandon = _originals.pop("abandon")


def _judge() -> list[str]:
    """Violations among the engines whose loop has closed; judged records are released."""
    with _lock:
        judged = [key for key, record in _records.items() if record.loop.is_closed()]
        records = [_records.pop(key) for key in judged]
    return [
        f"{r.test} built an async engine for datasource {r.datasource!r} that was never disposed on its "
        f"event loop before the loop closed; release it with `await client.aclose()` (or the query engine's "
        f"`aclose()`) on that loop"
        for r in records
        if not (r.disposed or (r.abandoned and r.may_abandon))
    ]


def pytest_configure(config: pytest.Config) -> None:
    if not _originals:
        _install()
        config.add_cleanup(_uninstall)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> Generator[None, object, object]:
    global _current
    previous, _current = _current, item
    try:
        return (yield)
    finally:
        _current = previous


@pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> Generator[None, None, None]:
    yield
    violations = _judge()
    if violations:
        pytest.fail("\n".join(violations), pytrace=False)
