"""How server commands react to SIGTERM / SIGINT: exit status, no traceback, graceful and forced stop."""

from __future__ import annotations

import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import IO, Literal, NoReturn

import psycopg2
import pytest

from slayer import cli as slayer_cli
from slayer.usage_reporting.recorder import NOTICE
from tests._usage_reporting_helpers import (
    build_storage,
    cli,
    free_port,
    is_posix,
    quiet_config,
    show,
    total,
    usage,
    wait_for_port,
    wait_until,
    write_config,
)

pytestmark = pytest.mark.skipif(not is_posix(), reason="POSIX signals")
_linux_only = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc")

_REPO = Path(__file__).resolve().parents[1]
_SIGNALS = [signal.SIGTERM, signal.SIGINT]
_CLEAN = {"ok": 1, "errors": {}}


class _Watched:
    """A subprocess whose stdout / stderr lines are collected as they arrive."""

    def __init__(self, argv: list[str], *, env: dict[str, str], keep_stdin: bool = False) -> None:
        self.proc = subprocess.Popen(
            argv, env=env, cwd=_REPO, text=True,
            stdin=subprocess.PIPE if keep_stdin else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.out: list[str] = []
        self.err: list[str] = []
        self._pumps = [
            threading.Thread(target=self._pump, args=(self.proc.stdout, self.out), daemon=True),
            threading.Thread(target=self._pump, args=(self.proc.stderr, self.err), daemon=True),
        ]
        for pump in self._pumps:
            pump.start()

    @staticmethod
    def _pump(stream: IO[str] | None, lines: list[str]) -> None:
        assert stream is not None
        for line in stream:
            lines.append(line.rstrip("\n"))

    @property
    def stderr(self) -> str:
        return "\n".join(self.err)

    def expect(self, text: str, *, on_stderr: bool = False, timeout: float = 60.0) -> None:
        lines = self.err if on_stderr else self.out
        assert wait_until(lambda: any(text in line for line in lines), timeout=timeout), (
            f"never saw {text!r} (exit {self.proc.poll()}); stdout={self.out!r} stderr={self.stderr!r}"
        )

    def send(self, sig: signal.Signals) -> None:
        self.proc.send_signal(sig)

    def finish(self, *, timeout: float = 30.0) -> int:
        try:
            returncode = self.proc.wait(timeout=timeout)
        finally:
            if self.proc.poll() is None:
                self.proc.kill()
                self.proc.wait(timeout=10)
            for pump in self._pumps:
                pump.join(timeout=10)
        return returncode


@pytest.fixture
def quiet(usage_reporting_env) -> dict[str, str]:
    quiet_config()
    return usage_reporting_env.subprocess_env()


@pytest.fixture
def store(usage_reporting_env) -> str:
    quiet_config()
    return build_storage(usage_reporting_env.root)


def _scenario(
    env: dict[str, str], name: str, *, sig: int = signal.SIGTERM, prior: str = "none", flags: tuple[str, ...] = (),
) -> _Watched:
    return _Watched(
        [sys.executable, "-m", "tests._server_shutdown_script", name, "--sig", str(int(sig)), "--prior", prior, *flags],
        env=env,
    )


def _assert_signal_stop(proc: _Watched, returncode: int, sig: int) -> None:
    assert returncode == 128 + sig, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    assert usage(show(), "cli:serve") == _CLEAN


# --- the shared shutdown path (_serve_until_terminated) ---------------------------------------------


@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_during_startup_stops_before_serving(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "startup", sig=sig)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, sig)
    assert "NOT_INTERRUPTED" not in proc.out
    assert "RUNNING" not in proc.out


@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_during_startup_raises_nothing_into_startup(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "startup_raised_into", sig=sig)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, sig)
    assert "RAISED_INTO_STARTUP" not in proc.out


@pytest.mark.parametrize("sig", _SIGNALS)
def test_no_server_starts_after_a_signal(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "serve_after_signal", sig=sig)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, sig)
    assert not {"RAISED_INTO_STARTUP", "RUNNING", "STOP"} & set(proc.out)


@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_while_serving_stops_once(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "join")
    proc.expect("RUNNING")
    proc.send(sig)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, sig)
    assert proc.out.count("STOP") == 1


@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_as_run_starts_stops_once(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "early", sig=sig)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, sig)
    assert proc.out.count("STOP") == 1


@pytest.mark.parametrize("second", _SIGNALS)
def test_second_signal_forces_exit_from_stuck_stop(quiet: dict[str, str], second: signal.Signals) -> None:
    proc = _scenario(quiet, "stuck_stop")
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    proc.expect("STOPPING")
    proc.send(second)
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, second)
    assert proc.out.count("STOPPING") == 1
    assert usage(show(), "mcp:query")["ok"] == 1


@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_waits_for_server_to_unwind(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "slow_unwind")
    proc.expect("RUNNING")
    proc.send(sig)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, sig)
    assert proc.out[-1] == "UNWOUND"


@pytest.mark.parametrize(("scenario", "error"), [
    ("failing_stop", "RuntimeError('stop-boom')"),
    ("failing_stop_exit", "SystemExit(3)"),
])
@pytest.mark.parametrize("sig", _SIGNALS)
def test_failing_stop_still_exits_on_first_signal(
    quiet: dict[str, str], sig: signal.Signals, scenario: str, error: str,
) -> None:
    proc = _scenario(quiet, scenario)
    proc.expect("RUNNING")
    proc.send(sig)
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, sig)
    assert f"server stop failed: {error}" in proc.stderr
    assert usage(show(), "mcp:query")["ok"] == 1


@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_inside_command_worker_start_never_serves(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "join", sig=sig, flags=("--signal-inside-command-start",))
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, sig)
    assert not {"RUNNING", "STOP"} & set(proc.out)


@_linux_only
@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_taken_by_worker_thread_stops_once(quiet: dict[str, str], sig: signal.Signals) -> None:
    proc = _scenario(quiet, "worker_thread_signalled", sig=sig)
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, sig)
    assert proc.out.count("STOP") == 1


@_linux_only
@pytest.mark.parametrize("second", _SIGNALS)
def test_second_signal_taken_by_stop_thread_forces_exit(quiet: dict[str, str], second: signal.Signals) -> None:
    proc = _scenario(quiet, "stop_thread_signalled", sig=second)
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, second)
    assert proc.out.count("STOPPING") == 1
    assert usage(show(), "mcp:query")["ok"] == 1


@pytest.mark.parametrize("second", _SIGNALS)
def test_second_signal_forces_exit_from_stuck_uvicorn_stop(quiet: dict[str, str], second: signal.Signals) -> None:
    proc = _scenario(quiet, "uvicorn_stuck_stop")
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    proc.expect("STOPPING")
    proc.send(second)
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, second)
    assert proc.out.count("STOPPING") == 1
    assert usage(show(), "mcp:query")["ok"] == 1


@pytest.mark.parametrize("third", _SIGNALS)
def test_signal_during_hard_exit_exits_at_once(quiet: dict[str, str], third: signal.Signals) -> None:
    proc = _scenario(quiet, "stuck_stop", flags=("--stuck-shutdown",))
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    proc.expect("STOPPING")
    proc.send(signal.SIGTERM)
    proc.expect("SHUTTING_DOWN")
    proc.send(third)
    returncode = proc.finish(timeout=10)
    assert returncode == 128 + third, proc.stderr


def test_signal_inside_usage_reporting_start_is_counted(usage_reporting_env) -> None:
    quiet_config()
    write_config(notice_shown_schema=None)
    proc = _scenario(usage_reporting_env.subprocess_env(), "served", flags=("--signals-on-notice", "1"))
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, signal.SIGTERM)
    assert NOTICE[:40] in proc.stderr
    assert "SERVED" not in proc.out


def test_second_signal_before_recording_starts_exits_uncounted(usage_reporting_env) -> None:
    quiet_config()
    write_config(notice_shown_schema=None)
    proc = _scenario(usage_reporting_env.subprocess_env(), "served", flags=("--signals-on-notice", "2"))
    returncode = proc.finish(timeout=10)
    assert returncode == 128 + signal.SIGTERM, proc.stderr
    assert "SERVED" not in proc.out
    assert total(usage(show(), "cli:serve")) == 0


def test_signal_while_closing_turns_normal_end_into_signal_exit(quiet: dict[str, str]) -> None:
    proc = _scenario(quiet, "served", flags=("--signal-on-cli-record",))
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, signal.SIGTERM)
    assert proc.out == ["SERVED"]


def test_signal_while_closing_keeps_committed_failure(quiet: dict[str, str]) -> None:
    proc = _scenario(quiet, "worker_exception", flags=("--signal-on-cli-record",))
    returncode = proc.finish()
    assert returncode == 1, proc.stderr
    assert "RuntimeError: worker-boom" in proc.stderr
    entry = usage(show(), "cli:serve")
    assert entry["ok"] == 0
    assert total(entry) == 1


def test_signal_after_run_returned(quiet: dict[str, str]) -> None:
    proc = _scenario(quiet, "returned")
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, signal.SIGTERM)
    assert proc.out == ["RAN"]


def test_signal_wins_over_worker_failure(quiet: dict[str, str]) -> None:
    proc = _scenario(quiet, "signal_beats_failure")
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, signal.SIGTERM)
    assert proc.out.count("STOP") == 1


@pytest.mark.parametrize(("scenario", "expected", "needle"), [
    ("worker_exception", 1, "Error: RuntimeError: worker-boom (rerun with --debug for the traceback)"),
    ("worker_exit", 7, None),
    ("worker_interrupt", -signal.SIGINT, "KeyboardInterrupt"),
])
def test_worker_failure_without_signal_is_reraised(
    quiet: dict[str, str], scenario: str, expected: int, needle: str | None,
) -> None:
    proc = _scenario(quiet, scenario)
    returncode = proc.finish()
    assert returncode == expected, proc.stderr
    if needle is not None:
        assert ("Traceback" in proc.stderr) == (scenario == "worker_interrupt")
        assert needle in proc.stderr
    assert "STOP" not in proc.out
    entry = usage(show(), "cli:serve")
    assert entry["ok"] == 0
    assert sum(entry["errors"].values()) == 1


@pytest.mark.parametrize("prior", ["marks", "raises"])
def test_prior_handler_called_once(quiet: dict[str, str], prior: str) -> None:
    proc = _scenario(quiet, "join", prior=prior)
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, signal.SIGTERM)
    assert proc.out.count("PRIOR") == 1
    assert proc.out.count("STOP") == 1


def test_prior_handler_not_called_by_forcing_signal(quiet: dict[str, str]) -> None:
    proc = _scenario(quiet, "stuck_stop", prior="marks")
    proc.expect("RUNNING")
    proc.send(signal.SIGTERM)
    proc.expect("STOPPING")
    proc.send(signal.SIGTERM)
    returncode = proc.finish(timeout=10)
    _assert_signal_stop(proc, returncode, signal.SIGTERM)
    assert proc.out.count("PRIOR") == 1


def test_inherited_ignore_stays_ignored(quiet: dict[str, str]) -> None:
    proc = _scenario(quiet, "disposition", prior="ignore")
    proc.expect("RUNNING")
    assert "SIGTERM_IGNORED=True" in proc.out
    proc.send(signal.SIGTERM)
    proc.send(signal.SIGINT)
    returncode = proc.finish()
    _assert_signal_stop(proc, returncode, signal.SIGINT)


def _serve_stopped_by_own_signal(_args) -> None:
    stopped = threading.Event()

    def run() -> None:
        signal.raise_signal(signal.SIGTERM)
        stopped.wait()

    slayer_cli._serve_until_terminated(run=run, stop=stopped.set)


@pytest.mark.parametrize(("run_serve", "slayer_now", "expected"), [
    (lambda _args: None, None, 0),
    (lambda _args: None, "not-a-time", 1),
    (_serve_stopped_by_own_signal, None, 128 + signal.SIGTERM),
], ids=["returned", "startup-error", "signal-stop"])
def test_main_restores_signal_handlers(
    quiet: dict[str, str], monkeypatch: pytest.MonkeyPatch, run_serve, slayer_now: str | None, expected: int,
) -> None:
    monkeypatch.setattr(slayer_cli, "_run_serve", run_serve)
    if slayer_now is not None:
        monkeypatch.setenv("SLAYER_NOW", slayer_now)
    before = {sig: signal.getsignal(sig) for sig in _SIGNALS}
    assert cli(["serve"]).returncode == expected
    assert {sig: signal.getsignal(sig) for sig in _SIGNALS} == before


def test_signal_just_before_handler_restore_is_not_lost(quiet: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(slayer_cli, "_run_serve", lambda _args: None)
    real_restore = slayer_cli._TerminationGuard.restore
    raised: list[bool] = []

    def restore(guard: slayer_cli._TerminationGuard) -> None:
        if not raised:
            raised.append(True)
            signal.raise_signal(signal.SIGTERM)
        real_restore(guard)

    monkeypatch.setattr(slayer_cli._TerminationGuard, "restore", restore)
    assert cli(["serve"]).returncode == 128 + signal.SIGTERM


def test_guard_stays_installed_after_signal_races_restore(monkeypatch: pytest.MonkeyPatch) -> None:
    before = {sig: signal.getsignal(sig) for sig in _SIGNALS}
    guard = slayer_cli._TerminationGuard(token=None)
    real_restore = slayer_cli._TerminationGuard.restore

    def restore() -> None:
        real_restore(guard)
        guard.signum = signal.SIGTERM

    monkeypatch.setattr(guard, "restore", restore)
    try:
        with pytest.raises(SystemExit) as exc:
            guard.finish()
        assert exc.value.code == 128 + signal.SIGTERM
        assert signal.getsignal(signal.SIGTERM) == guard._handle
    finally:
        real_restore(guard)
    assert {sig: signal.getsignal(sig) for sig in _SIGNALS} == before


@pytest.mark.parametrize("abandoned", [True, False])
def test_finish_exits_hard_only_for_an_abandoned_worker(monkeypatch: pytest.MonkeyPatch, abandoned: bool) -> None:
    class _ExitedNow(Exception):
        pass

    def exit_now(code: int) -> NoReturn:
        raise _ExitedNow(code)

    before = {sig: signal.getsignal(sig) for sig in _SIGNALS}
    monkeypatch.setattr(slayer_cli, "_exit_now", exit_now)
    guard = slayer_cli._TerminationGuard(token=None)
    try:
        guard.signum = signal.SIGTERM
        guard.abandoned = abandoned
        with pytest.raises(_ExitedNow if abandoned else SystemExit) as exc:
            guard.finish()
    finally:
        guard.restore()
    assert exc.value.args[0] == 128 + signal.SIGTERM
    assert {sig: signal.getsignal(sig) for sig in _SIGNALS} == before


@pytest.mark.parametrize("second", _SIGNALS)
@pytest.mark.parametrize("first", _SIGNALS)
@pytest.mark.parametrize("phase", ["before", "running", "closing", "after"])
def test_guard_handler_never_raises(
    monkeypatch: pytest.MonkeyPatch, phase: Literal["before", "running", "closing", "after"],
    first: signal.Signals, second: signal.Signals,
) -> None:
    before = {sig: signal.getsignal(sig) for sig in _SIGNALS}
    prior_calls: list[int] = []
    forced: list[int] = []
    recorded: list[dict[str, object]] = []

    def prior(signum: int, _frame: object) -> None:
        prior_calls.append(signum)
        raise RuntimeError("prior handler failed")

    monkeypatch.setattr(slayer_cli, "_exit_now", forced.append)
    monkeypatch.setattr(slayer_cli.usage_reporting, "record", lambda **kwargs: recorded.append(kwargs))
    for sig in _SIGNALS:
        signal.signal(sig, prior)
    guard = None
    try:
        guard = slayer_cli._TerminationGuard(token="serve")
        guard.phase = phase
        guard._handle(first, None)
        assert (guard.signum, guard.signalled.is_set(), forced, recorded) == (first, True, [], [])
        guard._handle(second, None)
    finally:
        if guard is not None:
            guard.restore()
        for sig, handler in before.items():
            signal.signal(sig, handler)
    assert prior_calls == [first]
    assert forced == ([] if phase == "closing" else [128 + second])
    assert recorded == ([{"surface": "cli", "token": "serve"}] if phase in ("before", "running") else [])
    assert {sig: signal.getsignal(sig) for sig in _SIGNALS} == before


# --- the real server commands ------------------------------------------------------------------------------


def _server_argv(command: str, store: str, port: int) -> list[str]:
    argv = [sys.executable, "-m", "slayer", command, "--storage", store]
    return argv if command == "mcp" else [*argv, "--host", "127.0.0.1", "--port", str(port)]


@pytest.mark.parametrize("command", ["serve", "mcp", "pg-serve", "flight-serve"])
@pytest.mark.parametrize("sig", _SIGNALS)
def test_signal_during_startup_stops_and_spools(
    store: str, usage_reporting_env, command: str, sig: signal.Signals,
) -> None:
    write_config(notice_shown_schema=None)
    proc = _Watched(
        _server_argv(command, store, free_port()), env=usage_reporting_env.subprocess_env(),
        keep_stdin=command == "mcp",
    )
    proc.expect(NOTICE[:40], on_stderr=True)
    proc.send(sig)
    returncode = proc.finish()
    assert returncode == 128 + sig, proc.stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    assert usage(show(), f"cli:{command}") == _CLEAN


def test_pg_serve_idle_client_does_not_block_stop(store: str, usage_reporting_env) -> None:
    port = free_port()
    proc = _Watched(_server_argv("pg-serve", store, port), env=usage_reporting_env.subprocess_env())
    assert wait_for_port(port), proc.stderr
    client = psycopg2.connect(host="127.0.0.1", port=port, user="slayer", dbname="slayer", connect_timeout=30)
    try:
        proc.send(signal.SIGTERM)
        returncode = proc.finish()
    finally:
        client.close()
    assert returncode == 128 + signal.SIGTERM, proc.stderr
    assert usage(show(), "cli:pg-serve") == _CLEAN
