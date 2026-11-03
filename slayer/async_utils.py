"""Async/sync bridge utilities.

SLayer is async-first internally. These helpers let sync callers
(notebooks, CLI, scripts) use the async API without managing event loops.
"""

import asyncio
import concurrent.futures
import contextvars
import inspect
import queue
import sys
from typing import Any, TypeVar
from collections.abc import Callable, Coroutine

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

_CallerCall = tuple[Callable[[], Any], "concurrent.futures.Future[Any]"]
_caller_calls: contextvars.ContextVar["queue.SimpleQueue[_CallerCall | None] | None"] = contextvars.ContextVar(
    "_caller_calls", default=None,
)


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine synchronously.

    Handles three scenarios:
    1. No event loop running → use asyncio.run()
    2. Inside Jupyter/existing loop → run in a thread to avoid nesting
    3. General case → create a new loop
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop running — simplest case
        return asyncio.run(coro)

    # Loop already running (Jupyter, async framework, etc.)
    # Run in a thread to avoid "cannot run nested event loop" error
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def marked_coroutine_function(func: F) -> F:
    """Mark a plain function returning a coroutine as a coroutine function (Python 3.12+)."""
    if sys.version_info >= (3, 12):
        return inspect.markcoroutinefunction(func)
    return func


async def on_caller_thread(func: Callable[[], T]) -> T:
    """Run blocking ``func`` on the thread that called ``run_sync_on_caller_thread``; inline outside one."""
    calls = _caller_calls.get()
    if calls is None:
        return func()
    done: concurrent.futures.Future[T] = concurrent.futures.Future()
    calls.put((func, done))
    return await asyncio.wrap_future(done)


def run_sync_on_caller_thread(coro: Coroutine[Any, Any, T]) -> T:
    """``run_sync`` whose ``on_caller_thread`` work stays on this thread, for thread-bound engines.

    Under a running loop the coroutine runs on a helper thread while this thread serves its calls.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    calls: queue.SimpleQueue[_CallerCall | None] = queue.SimpleQueue()

    async def _serve_from_caller() -> T:
        _caller_calls.set(calls)
        return await coro

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(asyncio.run, _serve_from_caller())
        result.add_done_callback(lambda _: calls.put(None))
        while (call := calls.get()) is not None:
            func, done = call
            if done.set_running_or_notify_cancel():
                try:
                    done.set_result(func())
                except BaseException as exc:  # noqa: BLE001 — delivered to the awaiting coroutine
                    done.set_exception(exc)
        return result.result()
