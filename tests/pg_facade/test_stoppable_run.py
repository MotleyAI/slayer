"""``_StoppableRun``: a coroutine in its own event loop, stoppable from any thread."""

from __future__ import annotations

import asyncio
import threading

from slayer.pg_facade.server import _StoppableRun


def test_stop_before_run_skips_the_coroutine() -> None:
    started: list[bool] = []

    async def coroutine() -> None:
        started.append(True)

    runner = _StoppableRun(coroutine)
    runner.stop()
    runner.run()
    assert started == []


def test_stop_from_another_thread_cancels_and_returns() -> None:
    running = threading.Event()
    cleaned: list[bool] = []

    async def coroutine() -> None:
        running.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.append(True)

    runner = _StoppableRun(coroutine)
    worker = threading.Thread(target=runner.run, daemon=True)
    worker.start()
    assert running.wait(10)
    runner.stop()
    worker.join(10)
    assert not worker.is_alive()
    assert cleaned == [True]


def test_stop_after_run_finished_is_a_no_op() -> None:
    async def coroutine() -> None:
        return None

    runner = _StoppableRun(coroutine)
    runner.run()
    runner.stop()
