"""Test-suite hang watchdog for xdist runs; inert unless one of its options is given.

``--stall-report-after S`` prints in-flight tests running for ``S`` seconds or more and
live workers silent that long (again every further ``S``). ``--stack-dump-after S``
dumps every process's thread stacks into ``--hang-report-dir`` ``S`` seconds after the
session starts. ``--hard-timeout-grace G`` ends a worker whose test outlives its
pytest-timeout (signal method) by ``G`` seconds, after dumping its stacks.
"""

from __future__ import annotations

import faulthandler
import os
import sys
import threading
import time
from pathlib import Path
from typing import IO, Any

import pytest

_TAG = "[hang-watchdog]"
_DEADLINE_KEY = "hang_watchdog_deadline"


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("hang watchdog")
    group.addoption(
        "--stall-report-after", type=float, metavar="S",
        help="xdist: report tests in flight and workers silent for S seconds or more",
    )
    group.addoption(
        "--stack-dump-after", type=float, metavar="S",
        help="dump every process's thread stacks into --hang-report-dir S seconds after session start",
    )
    group.addoption("--hang-report-dir", metavar="DIR", help="directory for the watchdog's stack dumps")
    group.addoption(
        "--hard-timeout-grace", type=float, metavar="G",
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
    if dump_after is not None and float(config.getini("faulthandler_timeout") or 0) > 0:
        raise pytest.UsageError("--stack-dump-after cannot be combined with faulthandler_timeout")
    if workerinput is None:
        if stall_after is not None or dump_after is not None:
            deadline = None if dump_after is None else time.time() + dump_after
            directory = None if dump_after is None else _report_dir(config)
            controller = _Controller(stall_after=stall_after, deadline=deadline, directory=directory)
            config.pluginmanager.register(controller, "hang-watchdog-controller")
        return
    worker_id = workerinput["workerid"]
    dump = None
    if workerinput.get(_DEADLINE_KEY) is not None:
        dump = _DeadlineDump(path=_report_dir(config) / f"{worker_id}-{os.getpid()}.txt")
        dump.arm(deadline=workerinput[_DEADLINE_KEY])
        config.pluginmanager.register(dump, "hang-watchdog-dump")
    if grace is not None:
        path = _report_dir(config) / f"hard-timeout-{worker_id}-{os.getpid()}.txt"
        config.pluginmanager.register(_HardBound(grace=grace, path=path, dump=dump), "hang-watchdog-hard-bound")


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


class _DeadlineDump:
    """Worker: faulthandler dumps every thread's stack at the controller's deadline."""

    def __init__(self, *, path: Path) -> None:
        self.path = path
        self._file: IO[str] | None = None

    def arm(self, *, deadline: float) -> None:
        self._file = self.path.open("w")
        remaining = deadline - time.time()
        if remaining > 0:
            faulthandler.dump_traceback_later(remaining, file=self._file)
        else:
            faulthandler.dump_traceback(file=self._file, all_threads=True)
            self.close()

    def close(self) -> None:
        if self._file is None:
            return
        faulthandler.cancel_dump_traceback_later()
        self._file.close()
        self._file = None
        if self.path.stat().st_size == 0:
            self.path.unlink()

    @pytest.hookimpl(trylast=True)
    def pytest_unconfigure(self) -> None:
        self.close()


class _HardBound:
    """Worker: ends the process when a signal-method test timeout outlives its grace."""

    def __init__(self, *, grace: float, path: Path, dump: _DeadlineDump | None) -> None:
        self._grace = grace
        self._path = path
        self._dump = dump
        self._cond = threading.Condition()
        self._deadline: float | None = None
        self._nodeid = ""
        self._thread: threading.Thread | None = None

    @pytest.hookimpl(tryfirst=True, optionalhook=True)
    def pytest_timeout_set_timer(self, item: pytest.Item, settings: Any) -> None:
        if settings.method != "signal" or threading.current_thread() is not threading.main_thread():
            return
        with self._cond:
            self._nodeid = item.nodeid
            self._deadline = time.monotonic() + settings.timeout + self._grace
            if self._thread is None:
                self._thread = threading.Thread(target=self._watch, name="hang-watchdog-hard-bound", daemon=True)
                self._thread.start()
            self._cond.notify()

    @pytest.hookimpl(tryfirst=True, optionalhook=True)
    def pytest_timeout_cancel_timer(self, item: pytest.Item) -> None:
        with self._cond:
            self._deadline = None
            self._cond.notify()

    def _watch(self) -> None:
        with self._cond:
            while True:
                if self._deadline is None:
                    self._cond.wait()
                    continue
                remaining = self._deadline - time.monotonic()
                if remaining > 0:
                    self._cond.wait(remaining)
                    continue
                self._fire()

    def _fire(self) -> None:
        with self._path.open("w") as out:
            out.write(f"Hard timeout: {self._nodeid}\n\n")
            out.flush()
            faulthandler.dump_traceback(file=out, all_threads=True)
        if self._dump is not None:
            self._dump.close()
        os._exit(1)
