"""``slayer serve`` with ``_run_serve`` replaced by a scripted ``_serve_until_terminated`` scenario.

Run as ``python -m tests._server_shutdown_script <scenario> [options]``; markers go to stdout, one per line, flushed.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

from slayer import cli, usage_reporting
from slayer.usage_reporting import recorder
from slayer.usage_reporting.recorder import NOTICE

_forever = threading.Event()
_real_run_serve = cli._run_serve


def mark(text: str) -> None:
    print(text, flush=True)


def _blocking_until_stopped() -> tuple[Callable[[], None], Callable[[], None]]:
    stopped = threading.Event()

    def run() -> None:
        mark("RUNNING")
        stopped.wait()

    def stop() -> None:
        mark("STOP")
        stopped.set()

    return run, stop


def _startup(args) -> None:
    os.kill(os.getpid(), args.sig)
    _forever.wait(10)
    mark("NOT_INTERRUPTED")
    cli._serve_until_terminated(run=lambda: mark("RUNNING"), stop=lambda: mark("STOP"))


def _join(_args) -> None:
    run, stop = _blocking_until_stopped()
    cli._serve_until_terminated(run=run, stop=stop)


def _early(args) -> None:
    run, stop = _blocking_until_stopped()

    def signalling_run() -> None:
        os.kill(os.getpid(), args.sig)
        run()

    cli._serve_until_terminated(run=signalling_run, stop=stop)


def _stuck_stop(_args) -> None:
    def run() -> None:
        usage_reporting.record(surface="mcp", token="query")
        mark("RUNNING")
        _forever.wait()

    def stop() -> None:
        mark("STOPPING")
        _forever.wait()

    cli._serve_until_terminated(run=run, stop=stop)


def _slow_unwind(_args) -> None:
    run, stop = _blocking_until_stopped()

    def unwinding_run() -> None:
        run()
        time.sleep(0.5)
        mark("UNWOUND")

    cli._serve_until_terminated(run=unwinding_run, stop=stop)


def _failing_stop(error: BaseException) -> Callable[[argparse.Namespace], None]:
    def scenario(_args) -> None:
        def run() -> None:
            usage_reporting.record(surface="mcp", token="query")
            mark("RUNNING")
            _forever.wait()

        def stop() -> None:
            raise error

        cli._serve_until_terminated(run=run, stop=stop)

    return scenario


def _signal_inside_worker_start(args) -> None:
    real_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        if thread.name == "slayer-server":
            signal.raise_signal(args.sig)
        real_start(thread)

    threading.Thread.start = start  # type: ignore[method-assign]
    cli._serve_until_terminated(run=lambda: mark("RUNNING"), stop=lambda: mark("STOP"))


def _main_thread_sleeping() -> bool:
    stat = Path(f"/proc/self/task/{threading.main_thread().native_id}/stat").read_text()
    return stat.rsplit(")", 1)[1].split()[0] == "S"


def _signal_once_main_sleeps(thread_id: int, sig: int) -> None:
    """Deliver ``sig`` to the non-main thread ``thread_id`` once the main thread is blocked."""

    def deliver() -> None:
        streak = 0
        while streak < 5:
            streak = streak + 1 if _main_thread_sleeping() else 0
            time.sleep(0.02)
        signal.pthread_kill(thread_id, sig)

    threading.Thread(target=deliver, daemon=True).start()


def _worker_thread_signalled(args) -> None:
    run, stop = _blocking_until_stopped()

    def signalling_run() -> None:
        _signal_once_main_sleeps(threading.get_ident(), args.sig)
        run()

    cli._serve_until_terminated(run=signalling_run, stop=stop)


def _stop_thread_signalled(args) -> None:
    worker: list[int] = []

    def run() -> None:
        worker.append(threading.get_ident())
        usage_reporting.record(surface="mcp", token="query")
        mark("RUNNING")
        _forever.wait()

    def stop() -> None:
        mark("STOPPING")
        _signal_once_main_sleeps(worker[0], args.sig)
        _forever.wait()

    cli._serve_until_terminated(run=run, stop=stop)


def _uvicorn_stuck_stop(args) -> None:
    class Server:
        def __init__(self, config: Any) -> None:
            pass

        def run(self, sockets: Any = None) -> None:
            usage_reporting.record(surface="mcp", token="query")
            mark("RUNNING")
            _forever.wait()

        @property
        def should_exit(self) -> bool:
            return False

        @should_exit.setter
        def should_exit(self, value: bool) -> None:
            if value:
                mark("STOPPING")
                _forever.wait()

    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.Config = lambda app, **kwargs: app  # type: ignore[attr-defined]
    fake_uvicorn.Server = Server  # type: ignore[attr-defined]
    fake_api = types.ModuleType("slayer.api.server")
    fake_api.create_app = lambda **kwargs: "APP"  # type: ignore[attr-defined]
    sys.modules.update({"uvicorn": fake_uvicorn, "slayer.api.server": fake_api})
    cli._resolve_storage = lambda _cli_args: None
    _real_run_serve(args.cli)


def _returned(args) -> None:
    cli._serve_until_terminated(run=lambda: mark("RAN"), stop=lambda: mark("STOP"))
    os.kill(os.getpid(), args.sig)


def _signal_beats_failure(_args) -> None:
    stopped = threading.Event()

    def run() -> None:
        mark("RUNNING")
        stopped.wait()
        raise RuntimeError("worker failed after stop")

    def stop() -> None:
        mark("STOP")
        stopped.set()

    cli._serve_until_terminated(run=run, stop=stop)


def _raising(error: BaseException) -> Callable[[argparse.Namespace], None]:
    def scenario(_args) -> None:
        def run() -> None:
            raise error

        cli._serve_until_terminated(run=run, stop=lambda: mark("STOP"))

    return scenario


def _disposition(_args) -> None:
    run, stop = _blocking_until_stopped()

    def reporting_run() -> None:
        ignored = signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
        mark(f"SIGTERM_IGNORED={ignored}")
        run()

    cli._serve_until_terminated(run=reporting_run, stop=stop)


SCENARIOS: dict[str, Callable[[argparse.Namespace], None]] = {
    "startup": _startup,
    "join": _join,
    "early": _early,
    "stuck_stop": _stuck_stop,
    "failing_stop": _failing_stop(RuntimeError("stop-boom")),
    "failing_stop_exit": _failing_stop(SystemExit(3)),
    "signal_inside_worker_start": _signal_inside_worker_start,
    "slow_unwind": _slow_unwind,
    "worker_thread_signalled": _worker_thread_signalled,
    "stop_thread_signalled": _stop_thread_signalled,
    "uvicorn_stuck_stop": _uvicorn_stuck_stop,
    "served": lambda _args: mark("SERVED"),
    "returned": _returned,
    "signal_beats_failure": _signal_beats_failure,
    "worker_exception": _raising(RuntimeError("worker-boom")),
    "worker_exit": _raising(SystemExit(7)),
    "worker_interrupt": _raising(KeyboardInterrupt()),
    "disposition": _disposition,
}


def _install_prior(kind: str) -> None:
    def marks(signum, frame) -> None:
        mark("PRIOR")

    def raises(signum, frame) -> None:
        mark("PRIOR")
        raise RuntimeError("prior handler failed")

    handlers = {"marks": marks, "raises": raises, "ignore": signal.SIG_IGN}
    if kind in handlers:
        signal.signal(signal.SIGTERM, handlers[kind])


class _SignallingStderr:
    """Sends ``count`` SIGTERMs to this process from inside the first write of the first-run notice."""

    def __init__(self, real: Any, *, count: int) -> None:
        self._real = real
        self._pending = count

    def write(self, text: str) -> int:
        if NOTICE[:40] in text:
            while self._pending:
                self._pending -= 1
                os.kill(os.getpid(), signal.SIGTERM)
        return self._real.write(text)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _signal_on_cli_record() -> None:
    real = recorder.record

    def record(**kwargs: Any) -> None:
        if kwargs.get("surface") == "cli":
            os.kill(os.getpid(), signal.SIGTERM)
        real(**kwargs)

    recorder.record = record


def _stuck_shutdown() -> None:
    mark("SHUTTING_DOWN")
    _forever.wait()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--prior", default="none", choices=["none", "marks", "raises", "ignore"])
    parser.add_argument("--sig", type=int, default=int(signal.SIGTERM))
    parser.add_argument("--signals-on-notice", type=int, default=0)
    parser.add_argument("--signal-on-cli-record", action="store_true")
    parser.add_argument("--stuck-shutdown", action="store_true")
    args = parser.parse_args()
    _install_prior(args.prior)
    if args.signals_on_notice:
        sys.stderr = _SignallingStderr(sys.stderr, count=args.signals_on_notice)
    if args.signal_on_cli_record:
        _signal_on_cli_record()
    if args.stuck_shutdown:
        usage_reporting.shutdown = _stuck_shutdown

    def run_scenario(cli_args: argparse.Namespace) -> None:
        args.cli = cli_args
        SCENARIOS[args.scenario](args)

    cli._run_serve = run_scenario
    sys.argv = ["slayer", "serve"]
    cli.main()


if __name__ == "__main__":
    main()
