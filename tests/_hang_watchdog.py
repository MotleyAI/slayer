"""Test-suite hang watchdog for xdist runs; inert unless one of its options is given.

``--stall-report-after S`` prints in-flight tests running for ``S`` seconds or more and
live workers silent that long (again every further ``S``). ``--stack-dump-after S``
dumps every process's thread stacks into ``--hang-report-dir`` ``S`` seconds after the
session starts. ``--hard-timeout-grace G`` ends a worker whose test outlives its
pytest-timeout (signal method) by ``G`` seconds, after dumping its stacks. A worker
enforces both with faulthandler's one timer, a C thread that runs without the GIL.
"""

from __future__ import annotations

import argparse
import faulthandler
import math
import os
import sys
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import IO, Any

import pytest

_TAG = "[hang-watchdog]"
_DEADLINE_KEY = "hang_watchdog_deadline"


def _seconds(value: str) -> float:
    seconds = float(value)
    if not (math.isfinite(seconds) and seconds > 0):
        raise argparse.ArgumentTypeError(f"must be a positive number of seconds, got {value!r}")
    return seconds


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("hang watchdog")
    group.addoption(
        "--stall-report-after", type=_seconds, metavar="S",
        help="xdist: report tests in flight and workers silent for S seconds or more",
    )
    group.addoption(
        "--stack-dump-after", type=_seconds, metavar="S",
        help="dump every process's thread stacks into --hang-report-dir S seconds after session start",
    )
    group.addoption("--hang-report-dir", metavar="DIR", help="directory for the watchdog's stack dumps")
    group.addoption(
        "--hard-timeout-grace", type=_seconds, metavar="G",
        help="xdist: end a worker whose test outlives its signal-method timeout by G seconds",
    )


def _report_dir(config: pytest.Config) -> Path:
    directory = config.getoption("--hang-report-dir")
    if not directory:
        raise pytest.UsageError("--stack-dump-after and --hard-timeout-grace need --hang-report-dir")
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return path


def pytest_configure(config: pytest.Config) -> None:
    stall_after = config.getoption("--stall-report-after")
    dump_after = config.getoption("--stack-dump-after")
    grace = config.getoption("--hard-timeout-grace")
    workerinput: dict[str, Any] | None = getattr(config, "workerinput", None)
    if (dump_after is not None or grace is not None) and float(config.getini("faulthandler_timeout") or 0) > 0:
        raise pytest.UsageError("--stack-dump-after and --hard-timeout-grace cannot be combined with faulthandler_timeout")
    if workerinput is None:
        if stall_after is not None or dump_after is not None:
            deadline = None if dump_after is None else time.time() + dump_after
            directory = None if dump_after is None else _report_dir(config)
            controller = _Controller(stall_after=stall_after, deadline=deadline, directory=directory)
            config.pluginmanager.register(controller, "hang-watchdog-controller")
        return
    deadline = workerinput.get(_DEADLINE_KEY)
    if deadline is None and grace is None:
        return
    directory = _report_dir(config)
    name = f"{workerinput['workerid']}-{os.getpid()}.txt"
    timer = _WorkerTimer(
        deadline=deadline, dump_path=directory / name, grace=grace, hard_path=directory / f"hard-timeout-{name}",
    )
    config.pluginmanager.register(timer, "hang-watchdog-worker-timer")


def _emit(line: str) -> None:
    sys.stderr.write(f"\n{_TAG} {line}\n")
    sys.stderr.flush()


class _Controller:
    """xdist controller: in-flight tests, worker activity, stall reports and the deadline dump."""

    def __init__(self, *, stall_after: float | None, deadline: float | None, directory: Path | None) -> None:
        self._lock = threading.Lock()
        self._in_flight: dict[str, list[float]] = {}  # nodeid → [start, next report at]
        self._workers: dict[str, list[float]] = {}  # worker id → [last activity, next report at]
        self._stall_after = stall_after
        self._stop = threading.Event()
        if stall_after is not None:
            threading.Thread(target=self._report_stalls, name="hang-watchdog-stalls", daemon=True).start()
        self._deadline = deadline
        self._dump_path = Path()
        self._timer: threading.Timer | None = None
        if deadline is not None and directory is not None:
            self._dump_path = directory / f"controller-{os.getpid()}.txt"
            self._timer = threading.Timer(max(deadline - time.time(), 0.0), self._dump)
            self._timer.name = "hang-watchdog-dump"
            self._timer.daemon = True
            self._timer.start()

    @pytest.hookimpl(optionalhook=True)
    def pytest_configure_node(self, node: Any) -> None:
        node.workerinput[_DEADLINE_KEY] = self._deadline

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodeready(self, node: Any) -> None:
        with self._lock:
            self._workers[node.gateway.id] = self._fresh()

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node: Any, error: object) -> None:
        with self._lock:
            self._workers.pop(node.gateway.id, None)

    def pytest_runtest_logstart(self, nodeid: str) -> None:
        with self._lock:
            self._in_flight[nodeid] = self._fresh()

    def pytest_runtest_logfinish(self, nodeid: str) -> None:
        with self._lock:
            self._in_flight.pop(nodeid, None)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        node = getattr(report, "node", None)
        with self._lock:
            if report.when not in ("setup", "call", "teardown"):  # xdist's crashed-item report
                self._in_flight.pop(report.nodeid, None)
            if node is not None and node.gateway.id in self._workers:
                self._workers[node.gateway.id] = self._fresh()

    def pytest_unconfigure(self) -> None:
        self._stop.set()
        if self._timer is not None:
            self._timer.cancel()

    def _fresh(self) -> list[float]:
        return [time.monotonic(), self._stall_after or 0.0]

    def _due(self, entries: dict[str, list[float]], now: float) -> list[tuple[str, float]]:
        due = []
        for key, entry in entries.items():
            elapsed = now - entry[0]
            if elapsed >= entry[1]:
                due.append((key, elapsed))
                entry[1] += self._stall_after or 0.0
        return due

    def _report_stalls(self) -> None:
        assert self._stall_after is not None
        while not self._stop.wait(min(self._stall_after / 4, 1.0)):
            now = time.monotonic()
            with self._lock:
                tests = self._due(self._in_flight, now)
                workers = self._due(self._workers, now)
            for nodeid, elapsed in tests:
                _emit(f"test running for {elapsed:.1f}s: {nodeid}")
            for worker_id, elapsed in workers:
                _emit(f"worker {worker_id} silent for {elapsed:.1f}s")

    def _dump(self) -> None:
        now = time.monotonic()
        with self._lock:
            in_flight = sorted((nodeid, now - entry[0]) for nodeid, entry in self._in_flight.items())
        with self._dump_path.open("w") as out:
            out.write("In flight:\n")
            out.writelines(f"  {nodeid} ({elapsed:.1f}s)\n" for nodeid, elapsed in in_flight)
            out.write("\n")
            out.flush()
            faulthandler.dump_traceback(file=out, all_threads=True)
        _emit(f"stacks dumped to {self._dump_path.parent}")


class _WorkerTimer:
    """Worker: faulthandler's one timer, armed for the earlier of the session deadline (dump) and the hard bound (dump, exit)."""

    def __init__(self, *, deadline: float | None, dump_path: Path, grace: float | None, hard_path: Path) -> None:
        self._grace = grace
        self._session_deadline = deadline
        self._test_deadline: float | None = None
        self._interacting = False
        self._files: dict[Path, IO[str]] = {}
        if deadline is not None:
            self._dump_file = self._files[dump_path] = dump_path.open("w")
        if grace is not None:
            self._hard_file = self._files[hard_path] = hard_path.open("w")
        self._rearm()

    @pytest.hookimpl(tryfirst=True, optionalhook=True)
    def pytest_timeout_set_timer(self, item: pytest.Item, settings: Any) -> None:
        if self._grace is None or settings.method != "signal":
            return
        self._hard_file.seek(0)
        self._hard_file.truncate()
        self._hard_file.write(f"Hard timeout: {item.nodeid}\n\n")
        self._hard_file.flush()
        self._test_deadline = time.time() + settings.timeout + self._grace
        self._rearm()

    @pytest.hookimpl(tryfirst=True, optionalhook=True)
    def pytest_timeout_cancel_timer(self, item: pytest.Item) -> None:
        if self._interacting or self._test_deadline is None:  # pytest-timeout cancels on every test failure
            return
        self._test_deadline = None
        self._hard_file.seek(0)
        self._hard_file.truncate()
        self._rearm()

    @pytest.hookimpl(wrapper=True)
    def pytest_exception_interact(self) -> Generator[None, object, object]:
        self._interacting = True
        try:
            return (yield)
        finally:
            self._interacting = False
            self._rearm()  # pytest's faulthandler plugin cancels the timer here

    @pytest.hookimpl(trylast=True)
    def pytest_unconfigure(self) -> None:
        faulthandler.cancel_dump_traceback_later()
        for path, file in self._files.items():
            file.close()
            if path.stat().st_size == 0:
                path.unlink()
        self._files.clear()

    def _rearm(self) -> None:
        now = time.time()
        if self._session_deadline is not None and self._session_deadline <= now:
            if os.fstat(self._dump_file.fileno()).st_size == 0:
                faulthandler.dump_traceback(file=self._dump_file, all_threads=True)
            self._session_deadline = None
        targets = []
        if self._session_deadline is not None:
            targets.append((self._session_deadline, self._dump_file, False))
        if self._test_deadline is not None:
            targets.append((self._test_deadline, self._hard_file, True))
        if not targets:
            faulthandler.cancel_dump_traceback_later()
            return
        when, file, exit_ = min(targets, key=lambda target: target[0])
        faulthandler.dump_traceback_later(max(when - now, 0.001), file=file, exit=exit_)
