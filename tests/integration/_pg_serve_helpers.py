"""Shared bootstrap for in-process Postgres-facade servers used by tests.

The body matches ``_start_pg_demo_server`` previously inlined at
``tests/integration/test_integration_pg_facade.py``; lifted here so the
existing asyncpg suite and the new live-Metabase suite can share a single
implementation. Adds an optional ``log_records`` capture knob for the
Metabase suite's hygiene-noise assertion (DEV-1562, A.6).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import tempfile
import threading
import time

import pytest

from slayer.cli import _prepare_demo, _resolve_storage
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.pg_facade.auth import validate_bind_address
from slayer.pg_facade.connection import PgConnection

DEMO_DATASOURCE = "jaffle_shop"


def start_pg_demo_server(
    *,
    token: str | None,
    log_records: list[logging.LogRecord] | None = None,
    storage_sink: list | None = None,
    bind_host: str = "127.0.0.1",
) -> tuple[asyncio.AbstractEventLoop, threading.Thread, str, int]:
    """Boot a Postgres-facade server backed by the Jaffle Shop demo.

    Returns ``(loop, thread, host, port)``. Caller stops the server via
    ``loop.call_soon_threadsafe(loop.stop)`` followed by ``thread.join()``.

    When ``log_records`` is supplied, a memory handler is attached to the
    ``slayer.pg_facade`` and ``slayer.facade`` loggers and every record
    emitted at ``DEBUG`` or above is appended to the list. Callers wanting
    the bug-4 WARN-volume check filter the list themselves. The handler is
    not removed automatically — tests own the list lifetime.

    When ``storage_sink`` is supplied, the engine's ``storage`` reference is
    appended to it after boot so tests can mutate stored ``SlayerModel``s
    (e.g. flip ``Column.hidden`` for the B.3 isolation test) and trigger
    re-sync.

    ``bind_host`` controls which interface the asyncio server listens on.
    Default ``"127.0.0.1"`` matches the legacy behaviour of the asyncpg
    integration suite (everything in the same host network namespace). The
    Metabase e2e suite passes ``"0.0.0.0"`` so a Metabase container reaching
    in via ``host.docker.internal`` (resolved by ``--add-host
    host.docker.internal:host-gateway`` on Linux runners) finds the socket
    accepting on the host's external interface. The returned tuple always
    reports ``"127.0.0.1"`` as the host string so loopback-based callers
    (asyncpg, psycopg in-process) keep using the cleaner address.

    The helper enforces ``pg_facade.auth.validate_bind_address`` — binding
    to a non-loopback address without a configured ``token`` raises, so a
    caller can't accidentally expose unauthenticated query access on a
    network-facing interface (CLI startup applies the same guard).
    """
    validate_bind_address(host=bind_host, token=token)
    args = argparse.Namespace(
        storage=tempfile.mkdtemp(prefix="slayer-pg-it-"),
        models_dir=None,
        datasource=None,
        force=False,
    )
    storage = _resolve_storage(args)
    try:
        _prepare_demo(args, storage)
    except Exception as exc:  # pragma: no cover - demo deps missing
        pytest.skip(f"Jaffle Shop demo unavailable: {exc}")
    engine = SlayerQueryEngine(storage=storage)

    if log_records is not None:
        handler = _ListHandler(log_records)
        handler.setLevel(logging.DEBUG)
        for logger_name in ("slayer.pg_facade", "slayer.facade"):
            log = logging.getLogger(logger_name)
            log.addHandler(handler)
            if log.level > logging.DEBUG or log.level == logging.NOTSET:
                log.setLevel(logging.DEBUG)

    if storage_sink is not None:
        storage_sink.append(storage)

    holder: dict = {}
    ready = threading.Event()

    def _thread_main() -> None:
        loop = asyncio.new_event_loop()
        holder["loop"] = loop
        asyncio.set_event_loop(loop)

        async def handle(reader, writer) -> None:
            conn = PgConnection(
                reader, writer, engine=engine, storage=storage, token=token, tls_ctx=None,
            )
            try:
                await conn.run()
            finally:
                writer.close()

        async def _setup():
            server = await asyncio.start_server(handle, host=bind_host, port=0)
            holder["port"] = server.sockets[0].getsockname()[1]
            holder["server"] = server
            ready.set()
            return server

        server = loop.run_until_complete(_setup())
        try:
            loop.run_forever()
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()

    thread = threading.Thread(target=_thread_main, daemon=True)
    thread.start()
    if not ready.wait(timeout=10) or "port" not in holder:
        raise RuntimeError("pg facade demo server failed to start within 10s")
    time.sleep(0.1)
    return holder["loop"], thread, "127.0.0.1", holder["port"]


class _ListHandler(logging.Handler):
    def __init__(self, sink: list[logging.LogRecord]) -> None:
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        self._sink.append(record)
