"""Async/sync bridge utilities.

SLayer is async-first internally. These helpers let sync callers
(notebooks, CLI, scripts) use the async API without managing event loops:
every synchronous call runs on one long-lived bridge loop per process, so
connections opened by one call serve the next.
"""

import asyncio
import atexit
import concurrent.futures
import contextlib
import contextvars
import functools
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
        # Outcomes of the workflows submitted and not yet finished; shutdown fails any it leaves behind.
        self.in_flight: set[concurrent.futures.Future[Any]] = set()


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


def _bridge_loop_locked(bridge: _Bridge) -> asyncio.AbstractEventLoop:
    """The running bridge loop, started on first use (callers hold ``bridge.lock``)."""
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


def register_shutdown(callback: ShutdownCallback) -> int | None:
    """Run ``callback`` on the bridge loop at bridge shutdown; returns a token for ``unregister_shutdown``,
    or ``None`` while shutdown runs (the caller releases the resource itself)."""
    bridge = _bridge
    with bridge.lock:
        if bridge.closing:
            return None
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


def _settled(bridge: _Bridge, done: "concurrent.futures.Future[Any]") -> None:
    with bridge.lock:
        bridge.in_flight.discard(done)


async def _contained(coro: Coroutine[Any, Any, T]) -> tuple[bool, Any]:
    """``coro``'s result, or the ``SystemExit`` / ``KeyboardInterrupt`` it raised, which would otherwise stop the loop."""
    try:
        return True, await coro
    except (SystemExit, KeyboardInterrupt) as exc:
        return False, exc


class _Workflow:
    """A coroutine bound for the bridge loop in ``context``: its outcome, and a thread-safe cancel valid from creation."""

    def __init__(self, coro: Coroutine[Any, Any, Any], context: contextvars.Context) -> None:
        self.coro, self.context = coro, context
        self.done: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.task: asyncio.Task[tuple[bool, Any]] | None = None
        self.cancelled = threading.Event()

    def start(self) -> None:
        bridge = _bridge
        with bridge.lock:  # scheduled before any shutdown starts, which then cancels it
            if bridge.closing:
                raise RuntimeError("The SLayer bridge event loop is shutting down.")
            self.loop = _bridge_loop_locked(bridge)
            self.done.add_done_callback(functools.partial(_settled, bridge))
            bridge.in_flight.add(self.done)
            self.loop.call_soon_threadsafe(self._run, self.loop)

    def _run(self, loop: asyncio.AbstractEventLoop) -> None:
        if not self.done.set_running_or_notify_cancel():
            return  # cancelled before it ran; the canceller closed the coroutine
        self.task = loop.create_task(_contained(self.coro), context=self.context)
        self.task.add_done_callback(self._transfer)
        if self.cancelled.is_set():
            self.task.cancel()

    def _transfer(self, task: asyncio.Task[tuple[bool, Any]]) -> None:
        try:
            returned, value = task.result()
        except BaseException as exc:  # noqa: BLE001  # NOSONAR(S5754) — the outcome, cancellation included, goes to the caller
            self.done.set_exception(exc)
            return
        if returned:
            self.done.set_result(value)
        else:
            self.done.set_exception(value)

    def cancel(self) -> bool:
        """Cancel the workflow; returns whether it may still be running (its cleanup is worth waiting for)."""
        self.cancelled.set()
        if self.done.cancel():
            self.coro.close()
            return False
        if self.done.done():
            return False
        if self.task is not None and self.loop is not None:
            with contextlib.suppress(RuntimeError):  # the loop closed under a concurrent shutdown
                self.loop.call_soon_threadsafe(self.task.cancel)
        return True


def _await_interruptibly(workflow: _Workflow, *, serve: "queue.SimpleQueue[_CallerCall | None]") -> Any:
    """Start ``workflow`` and return its result; an interruption cancels it, waits bounded for its cleanup, then
    re-raises."""
    try:
        workflow.done.add_done_callback(lambda _: serve.put(None))
        workflow.start()
        _serve_caller_calls(serve)
        return workflow.done.result()
    except BaseException:
        if workflow.cancel():
            _serve_caller_calls(serve, until=workflow.done)
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


def _run_private(coro: Coroutine[Any, Any, T], context: contextvars.Context, calls: "queue.SimpleQueue[_CallerCall | None]") -> T:
    """``coro`` on a private loop in a helper thread, ``calls`` served here: for a call from the bridge thread."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="slayer-nested") as pool:
        result = pool.submit(context.run, asyncio.run, coro)
        result.add_done_callback(lambda _: calls.put(None))
        _serve_caller_calls(calls)
        return result.result()


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine synchronously on the bridge loop, in the caller's context.

    Its ``on_caller_thread`` work runs on this thread, which serves it while the coroutine runs.
    """
    calls: queue.SimpleQueue[_CallerCall | None] = queue.SimpleQueue()
    context = contextvars.copy_context()
    context.run(_caller_calls.set, calls)
    if on_bridge_thread():
        return _run_private(coro, context, calls)
    return _await_interruptibly(_Workflow(coro, context), serve=calls)


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


async def _wind_down(callbacks: list[ShutdownCallback]) -> None:
    """Cancel the workflows still running, then run the shutdown callbacks."""
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.wait(pending, timeout=_CLEANUP_SECONDS)
    for callback in callbacks:
        try:
            await callback()
        except Exception:
            logger.warning("A bridge shutdown callback failed.", exc_info=True)


def shutdown_bridge() -> None:
    """Cancel the bridge's workflows, dispose its resources on its loop, then stop and join it (idempotent;
    a no-op on the bridge thread itself)."""
    bridge = _bridge
    with bridge.lock:
        if bridge.loop is None or bridge.closing or bridge.thread is threading.current_thread():
            return
        bridge.closing = True
        loop, thread = bridge.loop, bridge.thread
        callbacks = list(bridge.on_shutdown.values())
        bridge.on_shutdown.clear()
        in_flight = list(bridge.in_flight)
    try:
        if thread is not None and thread.is_alive():
            try:
                asyncio.run_coroutine_threadsafe(_wind_down(callbacks), loop).result(timeout=2 * _CLEANUP_SECONDS)
            except Exception:
                logger.warning("Bridge shutdown did not finish its releases.", exc_info=True)
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=_CLEANUP_SECONDS)
            if not loop.is_running():
                loop.close()
    finally:
        for done in in_flight:  # a workflow its cancellation did not end in time: its caller must not wait on
            with contextlib.suppress(concurrent.futures.InvalidStateError):
                done.set_exception(asyncio.CancelledError("the SLayer bridge event loop shut down"))
        with bridge.lock:
            bridge.loop, bridge.thread, bridge.closing = None, None, False


atexit.register(shutdown_bridge)
