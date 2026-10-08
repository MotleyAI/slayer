"""Local spooling, the 24 h send gate, delivery bookkeeping, destination and install identity."""

from __future__ import annotations

import datetime
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from slayer import telemetry
from slayer.telemetry import sender, settings
from tests._telemetry_helpers import (
    McpStdio,
    build_storage,
    cli,
    end_process,
    is_posix,
    make_due,
    now_utc,
    quiet_config,
    config,
    run_python,
    run_slayer,
    show,
    spool_files,
    total,
    usage,
    wait_until,
    write_config,
)

posix_only = pytest.mark.skipif(not is_posix(), reason="POSIX signals / file modes")


@pytest.fixture
def store(telemetry_env) -> str:
    quiet_config()
    return build_storage(telemetry_env.root)


def _spool_one(token: str = "query") -> None:
    telemetry.start()
    telemetry.record(surface="mcp", token=token)
    end_process()


# --- spooling at process end ---------------------------------------------------------------------


def test_normal_exit_spools_without_a_request(store: str, telemetry_env) -> None:
    result = run_slayer(["models", "--storage", store, "list"], env=telemetry_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert len(spool_files()) == 1
    assert telemetry_env.capture.arrivals == 0


def test_concurrent_processes_each_write_their_own_spool_file(store: str, telemetry_env) -> None:
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "slayer", "models", "--storage", store, "list"],
            env=telemetry_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(5)
    ]
    assert [p.wait(timeout=120) for p in procs] == [0] * 5
    assert len(spool_files()) == 5
    assert usage(show(), "cli:models.list")["ok"] == 5


def _mcp_with_one_call(store: str, env: dict[str, str], *, argv: list[str] | None = None) -> McpStdio:
    session = McpStdio(args=["--storage", store], env=env, argv=argv)
    session.initialize()
    response = session.call_tool("list_datasources", {})
    assert "result" in response, response
    return session


@posix_only
def test_stdio_mcp_sigterm_spools(store: str, telemetry_env) -> None:
    session = _mcp_with_one_call(store, telemetry_env.subprocess_env())
    try:
        session.proc.send_signal(signal.SIGTERM)
        session.wait()
    finally:
        session.kill()
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


_CHAINED_HANDLER_SCRIPT = """
import pathlib, signal, sys
def handler(signum, frame):
    pathlib.Path({marker!r}).write_text("called")
signal.signal(signal.SIGTERM, handler)
sys.argv = ["slayer", "mcp", "--storage", {store!r}]
from slayer.cli import main
main()
"""


@posix_only
def test_stdio_mcp_sigterm_chains_existing_handler(store: str, telemetry_env, tmp_path: Path) -> None:
    marker = tmp_path / "marker"
    argv = [sys.executable, "-c", _CHAINED_HANDLER_SCRIPT.format(marker=str(marker), store=store)]
    session = _mcp_with_one_call(store, telemetry_env.subprocess_env(), argv=argv)
    try:
        session.proc.send_signal(signal.SIGTERM)
        session.wait()
    finally:
        session.kill()
    assert marker.read_text() == "called"
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


@posix_only
def test_stdio_mcp_sigint_spools(store: str, telemetry_env) -> None:
    session = _mcp_with_one_call(store, telemetry_env.subprocess_env())
    try:
        session.proc.send_signal(signal.SIGINT)
        session.wait()
    finally:
        session.kill()
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


def test_stdio_mcp_end_of_input_spools(store: str, telemetry_env) -> None:
    session = _mcp_with_one_call(store, telemetry_env.subprocess_env())
    try:
        session.close_stdin()
        session.wait()
    finally:
        session.kill()
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


@posix_only
def test_stdio_mcp_broken_pipe_spools(store: str, telemetry_env) -> None:
    session = _mcp_with_one_call(store, telemetry_env.subprocess_env())
    try:
        assert session.proc.stdout is not None
        session.proc.stdout.close()
        session.send({"jsonrpc": "2.0", "id": 99, "method": "tools/call",
                      "params": {"name": "list_datasources", "arguments": {}}})
        session.close_stdin()
        session.wait()
    finally:
        session.kill()
    assert total(usage(show(), "mcp:list_datasources")) >= 1


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, *, timeout: float = 60.0) -> bool:
    def listening() -> bool:
        with socket.socket() as sock:
            return sock.connect_ex(("127.0.0.1", port)) == 0
    return wait_until(listening, timeout=timeout)


@posix_only
@pytest.mark.parametrize(("command", "sig"), [
    ("serve", signal.SIGTERM),
    ("serve", signal.SIGINT),
    ("pg-serve", signal.SIGINT),
    ("flight-serve", signal.SIGINT),
])
def test_server_shutdown_spools(store: str, telemetry_env, command: str, sig: signal.Signals) -> None:
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "slayer", command, "--host", "127.0.0.1", "--port", str(port), "--storage", store],
        env=telemetry_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_for_port(port), f"{command} did not start (exit {proc.poll()})"
        proc.send_signal(sig)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    cli_key = f"cli:{command}"
    assert total(usage(show(), cli_key)) == 1


@posix_only
def test_serve_sigterm_spools_rest_counts(store: str, telemetry_env) -> None:
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "slayer", "serve", "--host", "127.0.0.1", "--port", str(port), "--storage", store],
        env=telemetry_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_for_port(port)
        result = run_python(
            f"import urllib.request; urllib.request.urlopen('http://127.0.0.1:{port}/health').read()",
            env=telemetry_env.subprocess_env(),
        )
        assert result.returncode == 0, result.stderr
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    assert usage(show(), "rest:/health")["ok"] == 1


_FORK_SCRIPT = """
import os, sys
from slayer import telemetry
telemetry.start()
telemetry.record(surface="mcp", token="query")
pid = os.fork()
if pid == 0:
    telemetry.flush()
    os._exit(0)
os.waitpid(pid, 0)
"""


@posix_only
def test_forked_child_does_not_repeat_parent_counts(telemetry_env) -> None:
    quiet_config()
    result = run_python(_FORK_SCRIPT, env=telemetry_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert usage(show(), "mcp:query")["ok"] == 1


# --- spool files ----------------------------------------------------------------------------------------


@posix_only
def test_spool_files_are_owner_only(telemetry_env) -> None:
    quiet_config()
    _spool_one()
    files = spool_files()
    assert files
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in files)


@posix_only
def test_non_regular_spool_entries_ignored(telemetry_env) -> None:
    quiet_config()
    _spool_one()
    (real,) = spool_files()
    directory = settings.spool_dir()
    (directory / f"link-{real.name}").symlink_to(real)
    (directory / f"dir-{real.name}").mkdir()
    os.mkfifo(directory / f"fifo-{real.name}")
    assert usage(show(), "mcp:query")["ok"] == 1


def test_corrupt_spool_file_skipped(telemetry_env) -> None:
    quiet_config()
    _spool_one()
    (corrupt,) = spool_files()
    corrupt.write_text("{not json")
    _spool_one()
    assert usage(show(), "mcp:query")["ok"] == 1


def test_spool_is_capped_dropping_oldest(telemetry_env) -> None:
    quiet_config()
    for _ in range(140):
        _spool_one("query")
    for _ in range(10):
        _spool_one("search")
    files = spool_files()
    assert len(files) <= 100
    assert sum(p.stat().st_size for p in files) <= 1_000_000
    report = show()
    assert usage(report, "mcp:search")["ok"] == 10
    assert usage(report, "mcp:query")["ok"] <= 90


# --- the 24 h gate (injected clock) ------------------------------------------------------------------------


_T0 = datetime.datetime(2026, 5, 4, 12, 0, tzinfo=datetime.timezone.utc)


def test_not_yet_due_sends_nothing_and_spools(telemetry_env) -> None:
    quiet_config(last_sent=_T0)
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=3))
    _spool_one()
    time.sleep(0.5)
    assert telemetry_env.capture.arrivals == 0
    assert usage(show(), "mcp:query")["ok"] == 1


def test_idle_server_sends_nothing(telemetry_env) -> None:
    quiet_config(last_sent=_T0)
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=1))
    telemetry.start()
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=49))
    time.sleep(0.5)
    end_process()
    assert telemetry_env.capture.arrivals == 0


def test_due_on_activity_sends_once_merging_spool_and_live(telemetry_env) -> None:
    quiet_config(last_sent=_T0)
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=1))
    _spool_one("search")
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=25))
    telemetry.record(surface="mcp", token="query")
    requests = telemetry_env.capture.wait_for(count=1)
    telemetry.record(surface="mcp", token="query")
    time.sleep(0.3)
    end_process()
    assert len(telemetry_env.capture.requests) == 1
    sent = requests[0].json()["properties"]["usage"]
    assert sent["mcp:search"]["ok"] == 1
    assert sent["mcp:query"]["ok"] == 2
    assert wait_until(lambda: config().last_sent == _T0 + datetime.timedelta(hours=25))
    remaining = show()
    assert usage(remaining, "mcp:search")["ok"] == 0
    assert usage(remaining, "mcp:query")["ok"] == 1


def test_due_at_start_sends_spool(telemetry_env) -> None:
    quiet_config(last_sent=_T0)
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=1))
    _spool_one()
    telemetry_env.set_now(_T0 + datetime.timedelta(hours=26))
    telemetry.start()
    requests = telemetry_env.capture.wait_for(count=1)
    end_process()
    assert len(requests) == 1
    assert requests[0].json()["properties"]["usage"]["mcp:query"]["ok"] == 1


def test_last_send_in_the_future_counts_as_due(telemetry_env) -> None:
    quiet_config(last_sent=_T0 + datetime.timedelta(days=30))
    telemetry_env.set_now(_T0)
    _spool_one()
    telemetry.start()
    assert len(telemetry_env.capture.wait_for(count=1)) == 1
    end_process()


def test_clock_jump_forward_is_due(telemetry_env) -> None:
    quiet_config(last_sent=_T0)
    telemetry_env.set_now(_T0)
    _spool_one()
    telemetry_env.set_now(_T0 + datetime.timedelta(days=400))
    telemetry.start()
    assert len(telemetry_env.capture.wait_for(count=1)) == 1
    end_process()


# --- bookkeeping -----------------------------------------------------------------------------------------------


def test_failed_send_keeps_spool_and_last_send(telemetry_env) -> None:
    telemetry_env.capture.behaviour.status = 500
    quiet_config()
    _spool_one()
    before = make_due().last_sent
    telemetry.start()
    assert telemetry_env.capture.wait_for(count=1)
    telemetry.shutdown()
    end_process()
    assert config().last_sent == before
    assert usage(show(), "mcp:query")["ok"] == 1


def test_timed_out_send_keeps_spool(telemetry_env) -> None:
    telemetry_env.capture.behaviour.delay = 30
    quiet_config()
    _spool_one()
    before = make_due().last_sent
    telemetry.start()
    assert telemetry_env.capture.wait_for_arrival()
    started = time.monotonic()
    telemetry.shutdown()
    assert time.monotonic() - started < 1.5
    end_process()
    assert config().last_sent == before
    telemetry_env.set_now(now_utc() + datetime.timedelta(days=1))
    assert usage(show(), "mcp:query")["ok"] == 1


def test_no_wait_at_exit_without_send_in_flight(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    started = time.monotonic()
    telemetry.shutdown()
    assert time.monotonic() - started < 0.1
    end_process()


def test_two_simultaneous_senders_send_one_report(store: str, telemetry_env) -> None:
    telemetry_env.capture.behaviour.delay = 0.5
    run_slayer(["models", "--storage", store, "list"], env=telemetry_env.subprocess_env())
    make_due()
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "slayer", "datasources", "--storage", store, "list"],
            env=telemetry_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(2)
    ]
    assert [p.wait(timeout=120) for p in procs] == [0, 0]
    time.sleep(1.5)
    assert telemetry_env.capture.arrivals == 1
    assert len(telemetry_env.capture.requests) == 1


def test_sender_crash_recovered_by_a_later_send(store: str, telemetry_env) -> None:
    telemetry_env.capture.behaviour.stall_first = 60
    result = run_slayer(["models", "--storage", store, "list"], env=telemetry_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    make_due()
    session = McpStdio(args=["--storage", store], env=telemetry_env.subprocess_env())
    try:
        assert telemetry_env.capture.wait_for_arrival(timeout=60)
    finally:
        session.kill()
    telemetry_env.set_now(now_utc() + datetime.timedelta(days=2))
    telemetry.start()
    requests = telemetry_env.capture.wait_for(count=1, timeout=20)
    end_process()
    assert len(requests) == 1
    assert requests[0].json()["properties"]["usage"]["cli:models.list"]["ok"] == 1


def test_disable_racing_a_send_leaves_nothing_behind(store: str, telemetry_env) -> None:
    telemetry_env.capture.behaviour.delay = 3.0
    quiet_config()
    _spool_one()
    make_due()
    telemetry.start()
    assert telemetry_env.capture.wait_for_arrival()
    disabled = run_slayer(["telemetry", "disable"], env=telemetry_env.subprocess_env())
    assert disabled.returncode == 0, disabled.stderr
    assert not telemetry_env.capture.requests
    assert telemetry_env.capture.wait_for(count=1, timeout=15)
    telemetry.shutdown()
    end_process()
    current = config()
    assert current.setting == "disabled"
    assert current.install_id is None
    assert spool_files() == []


# --- destination and request shape ----------------------------------------------------------------------------


def test_default_endpoint_is_posthog_eu() -> None:
    assert sender.DEFAULT_ENDPOINT == "https://eu.i.posthog.com/i/v0/e/"


def test_request_shape(telemetry_env) -> None:
    quiet_config()
    _spool_one()
    make_due()
    telemetry.start()
    (request,) = telemetry_env.capture.wait_for(count=1)
    end_process()
    assert request.path == "/i/v0/e/"
    assert request.headers.get("Content-Type", "").startswith("application/json")
    body = request.json()
    properties = body["properties"]
    assert body["event"] == "slayer_usage"
    assert body["distinct_id"] == properties["install_id"]
    assert body["uuid"] == properties["batch_id"]
    assert properties["$geoip_disable"] is True
    assert properties["$process_person_profile"] is False
    assert "api_key" in body
    assert "timestamp" not in body


# --- install identity -------------------------------------------------------------------------------------------------


def test_install_id_created_on_first_use(telemetry_env) -> None:
    quiet_config()
    _spool_one()
    current = config()
    assert isinstance(current.install_id, uuid.UUID)
    assert current.install_id_created == now_utc().date()


@pytest.mark.parametrize(("age_days", "rotated"), [(14 * 31, True), (12 * 30, False)])
def test_install_id_rotation(telemetry_env, age_days: int, rotated: bool) -> None:
    old = uuid.uuid4()
    write_config(install_id=old, install_id_created=_T0.date())
    quiet_config(last_sent=_T0)
    telemetry_env.set_now(_T0 + datetime.timedelta(days=age_days))
    _spool_one()
    current = config()
    assert (current.install_id != old) is rotated
    if rotated:
        assert current.install_id_created == (_T0 + datetime.timedelta(days=age_days)).date()
        assert show()["install_id"] == str(current.install_id)


def test_install_id_stored_in_config_dir_not_model_storage(store: str, telemetry_env) -> None:
    cli(["models", "--storage", store, "list"])
    install_id = str(config().install_id)
    for path in Path(store).rglob("*"):
        if path.is_file():
            assert install_id not in path.read_text(errors="ignore")
    assert settings.config_path().is_relative_to(settings.config_dir())
    assert json.loads(settings.config_path().read_text())
