"""``serve`` shutdown: every accepted connection is closed, even one whose handler never ran."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest

from slayer.pg_facade.server import serve


async def test_stop_before_a_handler_runs_still_closes_its_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    real_start_server = asyncio.start_server
    listening: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    serving: list[asyncio.Task[None]] = []

    async def start_server(client_connected_cb: Any, **kwargs: Any) -> asyncio.Server:
        def stop_on_accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Any:
            handler = client_connected_cb(reader, writer)
            serving[0].cancel()  # lands before the handler task is even created
            return handler

        server = await real_start_server(stop_on_accept, **kwargs)
        listening.set_result(server.sockets[0].getsockname()[1])
        return server

    monkeypatch.setattr(asyncio, "start_server", start_server)
    serving.append(asyncio.create_task(serve(host="127.0.0.1", port=0)))
    reader, client = await asyncio.open_connection("127.0.0.1", await listening)
    try:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(serving[0], timeout=5)
        assert await asyncio.wait_for(reader.read(), timeout=5) == b""  # the server closed its end
    finally:
        client.close()
