"""Async/sync bridge utilities.

SLayer is async-first internally. These helpers let sync callers
(notebooks, CLI, scripts) use the async API without managing event loops:
every synchronous call runs on one long-lived bridge loop per process, so
connections opened by one call serve the next.
"""

import asyncio
import atexit
import concurrent.futures
import contextvars
import inspect
import logging
import os
import queue
import sys
import threading
from typing import Any, TypeVar
from collections.abc import Awaitable, Callable, Coroutine

logger = logging.getLogger(__name__)

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

_CallerCall = tuple[Callable[[], Any], "concurrent.futures.Future[Any]"]
_caller_calls: contextvars.ContextVar["queue.SimpleQueue[_CallerCall | None] | None"] = contextvars.ContextVar(
    "_caller_calls", default=None,
)

# Seconds an interrupted caller waits for its cancelled workflow's cleanup.
_CLEANUP_SECONDS = 10.0

ShutdownCallback = Callable[[], Awaitable[None]]


class _Bridge:
    """The process's bridge loop, its thread, and the resources disposed on its shutdown."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.closing = False
        self.on_shutdown: dict[int, ShutdownCallback] = {}
        self.next_token = 0


_bridge = _Bridge()


def _reset_after_fork() -> None:
    """In a forked child: forget the parent's loop, thread and resources without touching them."""
    global _bridge
    _bridge = _Bridge()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def _serve_forever(loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
    asyncio.set_event_loop(loop)
    loop.call_soon(ready.set)
    loop.run_forever()


def _bridge_loop() -> asyncio.AbstractEventLoop:
    """The running bridge loop, started on first use."""
    bridge = _bridge
    with bridge.lock:
        if bridge.closing:
            raise RuntimeError("The SLayer bridge event loop is shutting down.")
        if bridge.loop is None or bridge.thread is None or not bridge.thread.is_alive():
            loop = asyncio.new_event_loop()
            ready = threading.Event()
            thread = threading.Thread(
                target=_serve_forever, args=(loop, ready), name="slayer-bridge", daemon=True,
            )
            thread.start()
            ready.wait()
            bridge.loop, bridge.thread = loop, thread
        return bridge.loop


def on_bridge_thread() -> bool:
    """Whether the calling thread is the bridge loop's."""
    return _bridge.thread is threading.current_thread()


def is_bridge_loop(loop: asyncio.AbstractEventLoop) -> bool:
    return loop is _bridge.loop


def register_shutdown(callback: ShutdownCallback) -> int:
    """Run ``callback`` on the bridge loop at bridge shutdown; returns a token for ``unregister_shutdown``."""
    bridge = _bridge
    with bridge.lock:
        bridge.next_token += 1
        bridge.on_shutdown[bridge.next_token] = callback
        return bridge.next_token


def unregister_shutdown(token: int) -> bool:
    """Drop a shutdown callback; ``False`` when shutdown already ran it (or it was never registered)."""
    with _bridge.lock:
        return _bridge.on_shutdown.pop(token, None) is not None


def call_on_bridge(work: Callable[[], Coroutine[Any, Any, None]]) -> None:
    """Run ``work()`` on the bridge loop: awaited off the bridge thread, scheduled on it; a no-op without a loop."""
    loop = _bridge.loop
    if loop is None or loop.is_closed():
        return
    if on_bridge_thread():
        loop.create_task(work())
        return
    try:
        asyncio.run_coroutine_threadsafe(work(), loop).result(timeout=_CLEANUP_SECONDS)
    except Exception:
        logger.warning("A bridge-loop release failed.", exc_info=True)


async def _contained(coro: Coroutine[Any, Any, T]) -> tuple[bool, Any]:
    """``coro``'s result, or the ``SystemExit`` / ``KeyboardInterrupt`` it raised, which would otherwise stop the loop."""
    try:
        return True, await coro
    except (SystemExit, KeyboardInterrupt) as exc:
        return False, exc


def _submit(coro: Coroutine[Any, Any, T], context: contextvars.Context) -> tuple["concurrent.futures.Future[T]", Callable[[], None]]:
    """Start ``coro`` on the bridge loop in ``context``; returns its outcome and a thread-safe cancel."""
    loop = _bridge_loop()
    done: concurrent.futures.Future[T] = concurrent.futures.Future()
    started: list[asyncio.Task[tuple[bool, Any]]] = []
    cancelled = threading.Event()

    def _transfer(task: asyncio.Task[tuple[bool, Any]]) -> None:
        try:
            returned, value = task.result()
        except BaseException as exc:  # noqa: BLE001  # NOSONAR(S5754) — the outcome, cancellation included, goes to the caller
            done.set_exception(exc)
            return
        if returned:
            done.set_result(value)
        else:
            done.set_exception(value)

    def _start() -> None:
        task = loop.create_task(_contained(coro), context=context)
        started.append(task)
        task.add_done_callback(_transfer)
        if cancelled.is_set():
            task.cancel()

    def _cancel() -> None:
        cancelled.set()
        if started:
            loop.call_soon_threadsafe(started[0].cancel)

    loop.call_soon_threadsafe(_start)
    return done, _cancel


def _await_interruptibly(done: "concurrent.futures.Future[T]", cancel: Callable[[], None], *, serve: "queue.SimpleQueue[_CallerCall | None] | None" = None) -> T:
    """``done``'s result; an interruption cancels the work, waits bounded for its cleanup, then re-raises."""
    try:
        if serve is not None:
            _serve_caller_calls(serve)
        return done.result()
    except BaseException:
        if not done.done():
            cancel()
            if serve is not None:
                _serve_caller_calls(serve, until=done)
            else:
                concurrent.futures.wait([done], timeout=_CLEANUP_SECONDS)
        raise


def _serve_caller_calls(
    calls: "queue.SimpleQueue[_CallerCall | None]", *, until: "concurrent.futures.Future[Any] | None" = None,
) -> None:
    """Run queued ``on_caller_thread`` calls here until the workflow ends (bounded when ``until`` is given)."""
    while True:
        if until is not None:
            try:
                call = calls.get(timeout=_CLEANUP_SECONDS)
            except queue.Empty:
                return
        else:
            call = calls.get()
        if call is None:
            return
        func, result = call
        if result.set_running_or_notify_cancel():
            try:
                result.set_result(func())
            except BaseException as exc:  # noqa: BLE001  # NOSONAR(S5754) — delivered to the awaiting coroutine; narrower would hang shutdown
                result.set_exception(exc)


def _run_private(coro: Coroutine[Any, Any, T], calls: "queue.SimpleQueue[_CallerCall | None]") -> T:
    """``coro`` on a private loop in a helper thread, ``calls`` served here: for a call from the bridge thread."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="slayer-nested") as pool:
        result = pool.submit(contextvars.copy_context().run, asyncio.run, coro)
        result.add_done_callback(lambda _: calls.put(None))
        _serve_caller_calls(calls)
        return result.result()


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine synchronously on the bridge loop, in the caller's context.

    Its ``on_caller_thread`` work runs on this thread, which serves it while the coroutine runs.
    """
    calls: queue.SimpleQueue[_CallerCall | None] = queue.SimpleQueue()

    async def _serve_from_caller() -> T:
        _caller_calls.set(calls)
        return await coro

    if on_bridge_thread():
        return _run_private(_serve_from_caller(), calls)
    done, cancel = _submit(_serve_from_caller(), contextvars.copy_context())
    done.add_done_callback(lambda _: calls.put(None))
    return _await_interruptibly(done, cancel, serve=calls)


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
    """``run_sync``, named for callers whose engines are bound to this thread."""
    return run_sync(coro)


async def _run_shutdown_callbacks(callbacks: list[ShutdownCallback]) -> None:
    for callback in callbacks:
        try:
            await callback()
        except Exception:
            logger.warning("A bridge shutdown callback failed.", exc_info=True)


def shutdown_bridge() -> None:
    """Dispose the bridge's resources on its loop, then stop and join it (idempotent)."""
    bridge = _bridge
    with bridge.lock:
        if bridge.loop is None or bridge.closing:
            return
        bridge.closing = True
        loop, thread = bridge.loop, bridge.thread
        callbacks = list(bridge.on_shutdown.values())
        bridge.on_shutdown.clear()
    try:
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            try:
                asyncio.run_coroutine_threadsafe(_run_shutdown_callbacks(callbacks), loop).result(timeout=_CLEANUP_SECONDS)
            except Exception:
                logger.warning("Bridge shutdown did not finish its releases.", exc_info=True)
            loop.call_soon_threadsafe(loop.stop)
            if thread is not None:
                thread.join(timeout=_CLEANUP_SECONDS)
            if not loop.is_running():
                loop.close()
    finally:
        with bridge.lock:
            bridge.loop, bridge.thread, bridge.closing = None, None, False


atexit.register(shutdown_bridge)
