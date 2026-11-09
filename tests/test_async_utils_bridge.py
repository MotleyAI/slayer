"""The long-lived bridge loop behind the synchronous entry points (spec: sql/execution)."""

from __future__ import annotations

import asyncio
import contextvars
import os
import select
import signal
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
import sqlalchemy.event as sa_event

from slayer import async_utils
from slayer.async_utils import on_caller_thread, run_sync, run_sync_on_caller_thread
from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from tests import conftest
from tests._engine_helpers import disposable_engine

REPO = Path(__file__).resolve().parent.parent

# Seconds before a wait counts as hung.
_BOUND = 15

_caller_value: contextvars.ContextVar[str] = contextvars.ContextVar("_caller_value", default="unset")

_POSIX_MAIN_THREAD = pytest.mark.skipif(
    not hasattr(os, "fork") or threading.current_thread() is not threading.main_thread()
    or signal.getsignal(signal.SIGINT) is not signal.default_int_handler,
    reason="needs POSIX signals on the main thread with the default SIGINT handler",
)
_FORK_WARNING = pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")


async def _running_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_running_loop()


async def _thread_ident() -> int:
    return threading.get_ident()


async def _value(value: Any) -> Any:
    return value


async def _read_caller_value() -> str:
    return _caller_value.get()


def _bounded(fn: Callable[[], Any]) -> Any:
    """``fn()`` on a daemon thread; fail instead of hanging."""
    box: dict[str, Any] = {}

    def _run() -> None:
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 — re-raised below
            box["error"] = exc

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=_BOUND)
    assert not thread.is_alive(), "call hung"
    if "error" in box:
        raise box["error"]
    return box.get("value")


# ---------------------------------------------------------------------------
# One loop, one thread
# ---------------------------------------------------------------------------


def test_run_sync_reuses_one_loop() -> None:
    first = run_sync(_running_loop())
    second = run_sync(_running_loop())
    assert first is second
    assert not first.is_closed()


def test_run_sync_runs_on_one_bridge_thread() -> None:
    idents = {run_sync(_thread_ident()) for _ in range(3)}
    assert len(idents) == 1
    assert threading.get_ident() not in idents


def test_run_sync_inside_a_running_loop_uses_the_bridge_loop() -> None:
    outside = run_sync(_running_loop())

    async def _inside() -> tuple[asyncio.AbstractEventLoop, asyncio.AbstractEventLoop]:
        return asyncio.get_running_loop(), run_sync(_running_loop())

    caller_loop, bridged = asyncio.run(_inside())
    assert bridged is outside
    assert bridged is not caller_loop


def test_nested_run_sync_from_the_bridge_loop_completes() -> None:
    async def _outer() -> int:
        return run_sync(_value(41)) + 1

    assert _bounded(lambda: run_sync(_outer())) == 42


async def _exit() -> None:
    raise SystemExit(3)


def test_system_exit_reaches_the_caller_and_the_bridge_survives() -> None:
    loop = run_sync(_running_loop())

    def _call() -> None:
        run_sync(_exit())

    with pytest.raises(SystemExit):
        _bounded(_call)
    assert _bounded(lambda: run_sync(_running_loop())) is loop


# ---------------------------------------------------------------------------
# Context variables
# ---------------------------------------------------------------------------


def test_caller_context_reaches_the_workflow() -> None:
    token = _caller_value.set("caller")
    try:
        assert run_sync(_read_caller_value()) == "caller"
    finally:
        _caller_value.reset(token)


def test_caller_context_reaches_the_workflow_from_inside_a_running_loop() -> None:
    async def _inside() -> str:
        _caller_value.set("jupyter")
        return run_sync(_read_caller_value())

    assert asyncio.run(_inside()) == "jupyter"


# ---------------------------------------------------------------------------
# Interruption
# ---------------------------------------------------------------------------


@_POSIX_MAIN_THREAD
def test_interrupt_cancels_the_workflow_and_waits_for_its_cleanup() -> None:
    started = threading.Event()
    cancelled = threading.Event()
    cleaned = threading.Event()

    async def _wait() -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            await asyncio.sleep(0.2)
            cleaned.set()

    def _interrupt() -> None:
        if started.wait(_BOUND):
            os.kill(os.getpid(), signal.SIGINT)

    timer = threading.Thread(target=_interrupt, daemon=True)
    timer.start()
    begun = time.monotonic()
    waiting = _wait()
    with pytest.raises(KeyboardInterrupt):
        run_sync(waiting)
    timer.join(_BOUND)
    assert time.monotonic() - begun < _BOUND
    assert cancelled.is_set()
    assert cleaned.is_set()
    assert run_sync(_value(1)) == 1


def _interrupt_scheduling(monkeypatch: pytest.MonkeyPatch, *, after: threading.Event | None) -> None:
    """The caller's first bridge-loop scheduling is interrupted: after ``after`` is set, or before it schedules."""
    loop = run_sync(_running_loop())
    real, fired = loop.call_soon_threadsafe, threading.Event()

    def _interrupted(callback: Any, *args: Any, **kwargs: Any) -> Any:
        if async_utils.on_bridge_thread() or fired.is_set():
            return real(callback, *args, **kwargs)
        fired.set()
        if after is None:
            raise KeyboardInterrupt
        real(callback, *args, **kwargs)
        assert after.wait(_BOUND)
        raise KeyboardInterrupt

    monkeypatch.setattr(loop, "call_soon_threadsafe", _interrupted)


def test_an_interrupt_just_after_submission_cancels_the_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    started, cancelled = threading.Event(), threading.Event()

    async def _wait() -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    waiting = _wait()

    def _call() -> None:
        run_sync(waiting)

    _interrupt_scheduling(monkeypatch, after=started)
    with pytest.raises(KeyboardInterrupt):
        _bounded(_call)
    assert cancelled.is_set()
    assert _bounded(lambda: run_sync(_value(2))) == 2


def test_an_interrupt_before_submission_closes_the_workflow_unstarted(monkeypatch: pytest.MonkeyPatch) -> None:
    ran = threading.Event()

    async def _never() -> None:
        ran.set()

    workflow = _never()

    def _call() -> None:
        run_sync(workflow)

    _interrupt_scheduling(monkeypatch, after=None)
    begun = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        _bounded(_call)
    assert time.monotonic() - begun < 5
    assert workflow.cr_frame is None
    assert not ran.is_set()
    assert _bounded(lambda: run_sync(_value(5))) == 5


# ---------------------------------------------------------------------------
# Fork
# ---------------------------------------------------------------------------


def _read_child(fd: int, pid: int) -> str:
    """The child's report, or a failure if it hung."""
    ready, _, _ = select.select([fd], [], [], _BOUND)
    if not ready:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        pytest.fail("forked child hung")
    out = os.read(fd, 4096).decode()
    os.waitpid(pid, 0)
    return out


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork")
@_FORK_WARNING
def test_fork_while_another_thread_submits() -> None:
    parent_loop = run_sync(_running_loop())
    stop = threading.Event()

    def _submit() -> None:
        while not stop.is_set():
            run_sync(_value(1))

    submitter = threading.Thread(target=_submit, daemon=True)
    submitter.start()
    time.sleep(0.05)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child
        try:
            os.close(read_fd)
            child_loop = run_sync(_running_loop())
            ok = run_sync(_value(7)) == 7 and child_loop is not parent_loop
            os.write(write_fd, b"ok" if ok else b"wrong result")
        except BaseException as exc:  # noqa: BLE001 — reported to the parent
            os.write(write_fd, repr(exc).encode()[:1000])
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        assert _read_child(read_fd, pid) == "ok"
    finally:
        os.close(read_fd)
        stop.set()
        submitter.join(_BOUND)
    assert run_sync(_running_loop()) is parent_loop


# ---------------------------------------------------------------------------
# Caller-thread calls
# ---------------------------------------------------------------------------


def _caller_engine_client(engine: sa.Engine, path: Path) -> SlayerSQLClient:
    return SlayerSQLClient(DatasourceConfig(name="c", type="sqlite", database=str(path)), sa_engine=engine)


def test_caller_thread_calls_stay_on_the_caller_thread(tmp_path: Path) -> None:
    path = tmp_path / "caller.db"
    with disposable_engine(f"sqlite:///{path}") as engine:
        checkouts: list[int] = []
        sa_event.listen(engine, "checkout", lambda *_a: checkouts.append(threading.get_ident()))
        client = _caller_engine_client(engine, path)

        async def _workflow() -> tuple[int, list[Any]]:
            rows = [(await client.execute("SELECT 1 AS x")).rows for _ in range(2)]
            return threading.get_ident(), rows

        workflow_thread, rows = run_sync_on_caller_thread(_workflow())
    assert rows == [[{"x": 1}]] * 2
    assert checkouts
    assert set(checkouts) == {threading.get_ident()}
    assert workflow_thread != threading.get_ident()
    assert workflow_thread == run_sync(_thread_ident())


def test_caller_thread_calls_from_inside_a_running_loop(tmp_path: Path) -> None:
    path = tmp_path / "caller.db"
    with disposable_engine(f"sqlite:///{path}") as engine:
        checkouts: list[int] = []
        sa_event.listen(engine, "checkout", lambda *_a: checkouts.append(threading.get_ident()))
        client = _caller_engine_client(engine, path)

        async def _inside() -> list[Any]:
            return run_sync_on_caller_thread(client.execute("SELECT 1 AS x")).rows

        assert asyncio.run(_inside()) == [{"x": 1}]
    assert checkouts
    assert set(checkouts) == {threading.get_ident()}


def test_a_raising_caller_thread_call_propagates() -> None:
    def _boom() -> None:
        raise ValueError("boom")

    async def _workflow() -> None:
        await on_caller_thread(_boom)

    def _run_workflow() -> None:
        run_sync_on_caller_thread(_workflow())

    with pytest.raises(ValueError, match="boom"):
        _bounded(_run_workflow)
    assert _bounded(lambda: run_sync_on_caller_thread(_value(3))) == 3


@_POSIX_MAIN_THREAD
def test_interrupting_a_caller_thread_workflow_cleans_up() -> None:
    """The caller is serving the queue when interrupted; the workflow's cleanup still runs."""
    served = threading.Event()
    cleaned = threading.Event()

    async def _workflow() -> None:
        await on_caller_thread(served.set)
        try:
            await asyncio.sleep(30)
        finally:
            await asyncio.sleep(0.2)
            cleaned.set()

    def _interrupt() -> None:
        if served.wait(_BOUND):
            time.sleep(0.1)
            os.kill(os.getpid(), signal.SIGINT)

    timer = threading.Thread(target=_interrupt, daemon=True)
    timer.start()
    begun = time.monotonic()
    workflow = _workflow()
    with pytest.raises(KeyboardInterrupt):
        run_sync_on_caller_thread(workflow)
    timer.join(_BOUND)
    assert time.monotonic() - begun < _BOUND
    assert cleaned.is_set()
    assert run_sync_on_caller_thread(_value(4)) == 4


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


_SHUTDOWN_SCRIPT = textwrap.dedent("""
    import threading
    from slayer import async_utils

    async def _thread():
        return threading.current_thread()

    before = set(threading.enumerate())
    bridge = async_utils.run_sync(_thread())
    assert bridge is not threading.main_thread(), "workflow ran on the caller thread"
    async_utils.shutdown_bridge()
    async_utils.shutdown_bridge()
    bridge.join(5)
    assert not bridge.is_alive(), "bridge thread still alive"
    assert set(threading.enumerate()) <= before, threading.enumerate()
    print("ok")
""")


def test_shutdown_is_idempotent_and_stops_the_bridge() -> None:
    proc = subprocess.run(
        [sys.executable, "-W", "error::ResourceWarning", "-c", _SHUTDOWN_SCRIPT],
        cwd=str(REPO), capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"
    assert "ResourceWarning" not in proc.stderr


_SHUTDOWN_CANCELS_SCRIPT = textwrap.dedent("""
    import asyncio, threading
    from slayer import async_utils

    started, outcome = threading.Event(), []

    async def _wait():
        started.set()
        await asyncio.sleep(60)

    def _call():
        try:
            async_utils.run_sync(_wait())
        except BaseException as exc:
            outcome.append(type(exc).__name__)

    caller = threading.Thread(target=_call, daemon=True)
    caller.start()
    assert started.wait(10)
    async_utils.shutdown_bridge()
    caller.join(10)
    assert not caller.is_alive(), "the caller still waits on a stopped loop"
    print(outcome)
""")


def test_shutdown_cancels_a_running_workflow() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _SHUTDOWN_CANCELS_SCRIPT], cwd=str(REPO), capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "['CancelledError']"


_SHUTDOWN_OUTLASTED_SCRIPT = textwrap.dedent("""
    import asyncio, threading
    from slayer import async_utils

    async_utils._CLEANUP_SECONDS = 0.5
    started, outcome = threading.Event(), []

    async def _stubborn():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await asyncio.sleep(60)

    def _call():
        try:
            async_utils.run_sync(_stubborn())
        except BaseException as exc:
            outcome.append(type(exc).__name__)

    caller = threading.Thread(target=_call, daemon=True)
    caller.start()
    assert started.wait(10)
    async_utils.shutdown_bridge()
    caller.join(10)
    assert not caller.is_alive(), "the caller waits on a workflow shutdown abandoned"
    print(outcome)
""")


def test_shutdown_releases_a_caller_whose_workflow_outlasts_it() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _SHUTDOWN_OUTLASTED_SCRIPT], cwd=str(REPO), capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "['CancelledError']"


def test_no_shutdown_callback_is_taken_while_shutdown_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _release() -> None:
        return None

    monkeypatch.setattr(async_utils._bridge, "closing", True)
    assert async_utils.register_shutdown(_release) is None


def test_shutdown_from_the_bridge_thread_is_a_no_op() -> None:
    loop = run_sync(_running_loop())

    async def _shut_down_from_inside() -> None:
        async_utils.shutdown_bridge()

    run_sync(_shut_down_from_inside())
    assert run_sync(_running_loop()) is loop
    assert loop.is_running()


def test_session_leak_gate_shuts_the_bridge_down(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(async_utils, "shutdown_bridge", lambda: calls.append("bridge"), raising=False)
    monkeypatch.setattr(engine_factory, "reset_cache", lambda: calls.append("cache"))
    fixture = conftest._dispose_engines_at_session_end
    teardown = fixture._get_wrapped_function()()
    next(teardown)
    next(teardown, None)
    assert calls.count("bridge") == 1
