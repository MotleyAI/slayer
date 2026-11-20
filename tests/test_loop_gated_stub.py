"""Smoke checks for the loop-gated blocking stub."""

from __future__ import annotations

import asyncio

from tests._loop_gated_stub import gated


async def test_times_out_when_called_on_the_loop_thread() -> None:
    stub = gated(timeout=0.2)
    assert stub() is False
    assert (stub.calls, stub.timeouts) == (1, 1)
    assert stub.handles[0].cancelled()


async def test_returns_promptly_when_called_off_the_loop() -> None:
    stub = gated(fn=lambda x: x * 2)
    assert await asyncio.to_thread(stub, 21) == 42
    stub.assert_loop_stayed_free()
