"""python.usage_reporting.arc42.md principle 1: fail-silent — usage reporting never affects the host process."""

from __future__ import annotations

import datetime
import json
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from slayer import usage_reporting
from slayer.usage_reporting import sender, settings
from tests._usage_reporting_capture import CaptureServer, RefusedEndpoint
from tests._usage_reporting_helpers import (
    SENT,
    SUBPROCESS_TIMEOUT,
    assert_no_sentinel,
    build_storage,
    cli,
    end_process,
    make_due,
    now_utc,
    quiet_config,
    run_slayer,
    show,
    spool_bytes,
    usage,
)

_QUERY = {"source_model": "orders", "measures": ["sum(amount)"]}


@pytest.fixture
def store(usage_reporting_env) -> str:
    quiet_config()
    return build_storage(usage_reporting_env.root)


def _commands(store: str) -> list[list[str]]:
    return [
        ["models", "--storage", store, "list"],
        ["query", json.dumps(_QUERY), "--storage", store],
        ["query", json.dumps({"source_model": "no_such_model", "measures": ["count(*)"]}), "--storage", store],
        ["models", "--storage", store, "show", "no_such_model"],
    ]


def _outcomes(commands: list[list[str]]) -> list[tuple[int, str, str]]:
    results = []
    for args in commands:
        result = cli(args)
        results.append((result.returncode, result.stdout, result.stderr))
    return results


def _baseline(store: str, monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, str, str]]:
    with monkeypatch.context() as mp:
        mp.setenv("SLAYER_USAGE_REPORTING", "off")
        return _outcomes(_commands(store))


def _refused(env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_USAGE_REPORTING_ENDPOINT", RefusedEndpoint().url)


def _timeout(env, monkeypatch: pytest.MonkeyPatch) -> None:
    env.capture.behaviour.delay = 30


def _server_error(env, monkeypatch: pytest.MonkeyPatch) -> None:
    env.capture.behaviour.status = 500


def _corrupt_spool(env, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = settings.spool_dir()
    directory.mkdir(parents=True, exist_ok=True)
    usage_reporting.start()
    usage_reporting.record(surface="mcp", token="query")
    end_process()
    for path in directory.iterdir():
        path.write_bytes(b"\x00{not json")


def _spool_dir_is_a_file(env, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = settings.spool_dir()
    directory.parent.mkdir(parents=True, exist_ok=True)
    directory.write_text("not a directory")


def _clock_raises(env, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken() -> datetime.datetime:
        raise RuntimeError("clock broken")

    monkeypatch.setattr("slayer.usage_reporting.clock.now", broken)


_FAILURES: dict[str, Callable] = {
    "endpoint_refused": _refused,
    "endpoint_timeout": _timeout,
    "endpoint_500": _server_error,
    "corrupt_spool": _corrupt_spool,
    "spool_dir_is_a_file": _spool_dir_is_a_file,
    "clock_raises": _clock_raises,
}


@pytest.mark.parametrize("failure", sorted(_FAILURES))
def test_failures_leave_output_and_exit_code_unchanged(
    store: str, usage_reporting_env, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    expected = _baseline(store, monkeypatch)
    make_due()
    _FAILURES[failure](usage_reporting_env, monkeypatch)
    assert _outcomes(_commands(store)) == expected


@pytest.mark.parametrize(("error", "printed"), [
    (BrokenPipeError(32, "Broken pipe"), False),
    (ConnectionResetError(104, "Connection reset by peer"), False),
    (RuntimeError("handler bug"), True),
])
def test_capture_server_is_silent_only_about_a_client_that_went_away(
    capsys: pytest.CaptureFixture[str], error: Exception, printed: bool,
) -> None:
    server = CaptureServer()
    try:
        with socket.socket() as request:
            try:
                raise error
            except Exception:
                server._server.handle_error(request, ("127.0.0.1", 0))
    finally:
        server._server.server_close()
    assert ("Traceback" in capsys.readouterr().err) is printed


def test_corrupt_config_leaves_stdout_and_exit_code_unchanged(
    store: str, usage_reporting_env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = [(code, out) for code, out, _ in _baseline(store, monkeypatch)]
    settings.config_path().write_text('{"install_id": 42, "last_sent": "yesterday-ish", "setting": [1]}')
    assert [(code, out) for code, out, _ in _outcomes(_commands(store))] == expected


class _EvilError(Exception):
    def __str__(self) -> str:
        raise RuntimeError(SENT)

    def __repr__(self) -> str:
        raise RuntimeError(SENT)


def test_record_never_raises_on_hostile_input(usage_reporting_env) -> None:
    quiet_config()
    usage_reporting.start()
    usage_reporting.record(surface="mcp", token="query", error=_EvilError())
    usage_reporting.record(surface="no-such-surface", token=SENT)
    usage_reporting.record(surface="mcp", token=None)  # type: ignore[arg-type]
    usage_reporting.record(surface="mcp", token=SENT * 1000, error=KeyboardInterrupt())
    end_process()
    report = show()
    assert usage(report, "mcp:query")["errors"] == {"other": 1}
    assert_no_sentinel(json.dumps(report))
    assert_no_sentinel(spool_bytes())


def test_flush_and_shutdown_never_raise_when_storage_vanishes(usage_reporting_env) -> None:
    quiet_config()
    usage_reporting.start()
    usage_reporting.record(surface="mcp", token="query")
    shutil.rmtree(settings.config_dir())
    usage_reporting.shutdown()
    end_process()


def test_send_never_raises_when_config_dir_vanishes(tmp_path) -> None:
    gone = tmp_path / "gone"
    paths = sender.Paths(config=gone / "usage-reporting.json", spool=gone / "spool", lease=gone / "send.lease")
    sender.send(paths, now=datetime.datetime.now(datetime.timezone.utc), url="http://127.0.0.1:9/")


_EXIT_WAIT_WRAPPER = """
import atexit, runpy, time
from pathlib import Path
atexit.register(lambda: Path({exit_stamp!r}).write_text(repr(time.monotonic())))
try:
    runpy.run_module("slayer", run_name="__main__", alter_sys=True)
finally:
    Path({work_done_stamp!r}).write_text(repr(time.monotonic()))
"""


def _read_stamp(path: Path, result: subprocess.CompletedProcess[str]) -> float:
    try:
        return float(path.read_text())
    except (OSError, ValueError):
        raise AssertionError(f"{path.name} stamp missing or unparseable; stderr:\n{result.stderr}") from None


def _run_slayer_timing_exit_wait(
    args: list[str], *, env: dict[str, str], stamp_dir: Path,
) -> tuple[subprocess.CompletedProcess[str], float]:
    """Run the CLI; also return seconds from the command's return to the last exit hook."""
    work_done, exit_stamp = stamp_dir / "work_done", stamp_dir / "exit"
    wrapper = _EXIT_WAIT_WRAPPER.format(exit_stamp=str(exit_stamp), work_done_stamp=str(work_done))
    result = subprocess.run(
        [sys.executable, "-c", wrapper, *args],
        env=env, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    return result, _read_stamp(exit_stamp, result) - _read_stamp(work_done, result)


def test_stalled_send_delays_exit_by_at_most_about_a_second(
    store: str, usage_reporting_env, tmp_path: Path,
) -> None:
    args = ["models", "--storage", store, "list"]
    run_slayer(args, env=usage_reporting_env.subprocess_env())
    make_due()
    usage_reporting_env.capture.behaviour.delay = 10
    result, exit_wait = _run_slayer_timing_exit_wait(
        args, env=usage_reporting_env.subprocess_env(), stamp_dir=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert usage_reporting_env.capture.arrivals == 1
    assert exit_wait < 2.0

    usage_reporting_env.set_now(now_utc() + datetime.timedelta(days=1))
    assert usage(show(), "cli:models.list")["ok"] == 2


def test_first_run_notice_and_send_never_touch_stdout(store: str, usage_reporting_env) -> None:
    settings.config_path().unlink()
    args = ["models", "--storage", store, "list"]
    first_run = run_slayer(args, env=usage_reporting_env.subprocess_env())
    off = run_slayer(args, env=usage_reporting_env.subprocess_env(SLAYER_USAGE_REPORTING="off"))
    assert (first_run.returncode, first_run.stdout) == (off.returncode, off.stdout)
