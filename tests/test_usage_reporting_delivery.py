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
import threading
import time
import uuid
from pathlib import Path

import pytest

from slayer import usage_reporting
from slayer.usage_reporting import control, sender, settings, spool
from tests._usage_reporting_helpers import (
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
def store(usage_reporting_env) -> str:
    quiet_config()
    return build_storage(usage_reporting_env.root)


def _spool_one(token: str = "query") -> None:
    usage_reporting.start()
    usage_reporting.record(surface="mcp", token=token)
    end_process()


# --- spooling at process end ---------------------------------------------------------------------


def test_normal_exit_spools_without_a_request(store: str, usage_reporting_env) -> None:
    result = run_slayer(["models", "--storage", store, "list"], env=usage_reporting_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert len(spool_files()) == 1
    assert usage_reporting_env.capture.arrivals == 0


def test_concurrent_processes_each_write_their_own_spool_file(store: str, usage_reporting_env) -> None:
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "slayer", "models", "--storage", store, "list"],
            env=usage_reporting_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
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
def test_stdio_mcp_sigterm_spools(store: str, usage_reporting_env) -> None:
    session = _mcp_with_one_call(store, usage_reporting_env.subprocess_env())
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
def test_stdio_mcp_sigterm_chains_existing_handler(store: str, usage_reporting_env, tmp_path: Path) -> None:
    marker = tmp_path / "marker"
    argv = [sys.executable, "-c", _CHAINED_HANDLER_SCRIPT.format(marker=str(marker), store=store)]
    session = _mcp_with_one_call(store, usage_reporting_env.subprocess_env(), argv=argv)
    try:
        session.proc.send_signal(signal.SIGTERM)
        session.wait()
    finally:
        session.kill()
    assert marker.read_text() == "called"
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


@posix_only
def test_stdio_mcp_sigint_spools(store: str, usage_reporting_env) -> None:
    session = _mcp_with_one_call(store, usage_reporting_env.subprocess_env())
    try:
        session.proc.send_signal(signal.SIGINT)
        session.wait()
    finally:
        session.kill()
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


def test_stdio_mcp_end_of_input_spools(store: str, usage_reporting_env) -> None:
    session = _mcp_with_one_call(store, usage_reporting_env.subprocess_env())
    try:
        session.close_stdin()
        session.wait()
    finally:
        session.kill()
    assert usage(show(), "mcp:list_datasources")["ok"] == 1


@posix_only
def test_stdio_mcp_broken_pipe_spools(store: str, usage_reporting_env) -> None:
    proc = subprocess.Popen(
        [sys.executable, "-m", "slayer", "mcp", "--storage", store],
        env=usage_reporting_env.subprocess_env(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    stdin, stdout = proc.stdin, proc.stdout
    assert stdin is not None
    assert stdout is not None

    def send(message: dict) -> None:
        stdin.write((json.dumps(message) + "\n").encode())
        stdin.flush()

    try:
        send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "claude-code", "version": "2.3.1"}}})
        assert json.loads(stdout.readline())["id"] == 1
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "list_datasources", "arguments": {}}})
        assert json.loads(stdout.readline())["id"] == 2
        stdout.close()
        send({"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": {"name": "list_datasources", "arguments": {}}})
        stdin.close()
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
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
def test_server_shutdown_spools(store: str, usage_reporting_env, command: str, sig: signal.Signals) -> None:
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "slayer", command, "--host", "127.0.0.1", "--port", str(port), "--storage", store],
        env=usage_reporting_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
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
def test_serve_sigterm_spools_rest_counts(store: str, usage_reporting_env) -> None:
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "slayer", "serve", "--host", "127.0.0.1", "--port", str(port), "--storage", store],
        env=usage_reporting_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_for_port(port)
        result = run_python(
            f"import urllib.request; urllib.request.urlopen('http://127.0.0.1:{port}/health').read()",
            env=usage_reporting_env.subprocess_env(),
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
from slayer import usage_reporting
usage_reporting.start()
usage_reporting.record(surface="mcp", token="query")
pid = os.fork()
if pid == 0:
    usage_reporting.flush()
    os._exit(0)
os.waitpid(pid, 0)
"""


@posix_only
def test_forked_child_does_not_repeat_parent_counts(usage_reporting_env) -> None:
    quiet_config()
    result = run_python(_FORK_SCRIPT, env=usage_reporting_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert usage(show(), "mcp:query")["ok"] == 1


# --- spool files ----------------------------------------------------------------------------------------


@posix_only
def test_spool_files_are_owner_only(usage_reporting_env) -> None:
    quiet_config()
    _spool_one()
    files = spool_files()
    assert files
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in files)


@posix_only
def test_non_regular_spool_entries_ignored(usage_reporting_env) -> None:
    quiet_config()
    _spool_one()
    (real,) = spool_files()
    directory = settings.spool_dir()
    (directory / f"link-{real.name}").symlink_to(real)
    (directory / f"dir-{real.name}").mkdir()
    os.mkfifo(directory / f"fifo-{real.name}")
    assert usage(show(), "mcp:query")["ok"] == 1


def test_corrupt_spool_file_skipped(usage_reporting_env) -> None:
    quiet_config()
    _spool_one()
    (corrupt,) = spool_files()
    corrupt.write_text("{not json")
    _spool_one()
    assert usage(show(), "mcp:query")["ok"] == 1


def test_spool_is_capped_dropping_oldest(usage_reporting_env) -> None:
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


def test_not_yet_due_sends_nothing_and_spools(usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=3))
    _spool_one()
    time.sleep(0.5)
    assert usage_reporting_env.capture.arrivals == 0
    assert usage(show(), "mcp:query")["ok"] == 1


def test_idle_server_sends_nothing(usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=1))
    usage_reporting.start()
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=49))
    time.sleep(0.5)
    end_process()
    assert usage_reporting_env.capture.arrivals == 0


def test_due_on_activity_sends_once_merging_spool_and_live(usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=1))
    _spool_one("search")
    usage_reporting.start()
    usage_reporting.record(surface="mcp", token="query")
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=25))
    usage_reporting.record(surface="mcp", token="query")
    requests = usage_reporting_env.capture.wait_for(count=1)
    usage_reporting.record(surface="mcp", token="query")
    time.sleep(0.3)
    end_process()
    assert len(usage_reporting_env.capture.requests) == 1
    sent = requests[0].json()["properties"]["usage"]
    assert sent["mcp:search"]["ok"] == 1
    assert sent["mcp:query"]["ok"] == 2
    assert wait_until(lambda: config().last_sent == _T0 + datetime.timedelta(hours=25))
    remaining = show()
    assert usage(remaining, "mcp:search")["ok"] == 0
    assert usage(remaining, "mcp:query")["ok"] == 1


def test_due_at_start_sends_spool(usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=1))
    _spool_one()
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=26))
    usage_reporting.start()
    requests = usage_reporting_env.capture.wait_for(count=1)
    end_process()
    assert len(requests) == 1
    assert requests[0].json()["properties"]["usage"]["mcp:query"]["ok"] == 1


def test_last_send_in_the_future_counts_as_due(usage_reporting_env) -> None:
    quiet_config(last_sent=_T0 + datetime.timedelta(days=30))
    usage_reporting_env.set_now(_T0)
    _spool_one()
    usage_reporting.start()
    assert len(usage_reporting_env.capture.wait_for(count=1)) == 1
    end_process()


def test_clock_jump_forward_is_due(usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0)
    _spool_one()
    usage_reporting_env.set_now(_T0 + datetime.timedelta(days=400))
    usage_reporting.start()
    assert len(usage_reporting_env.capture.wait_for(count=1)) == 1
    end_process()


# --- bookkeeping -----------------------------------------------------------------------------------------------


def test_failed_send_keeps_spool_and_last_send(usage_reporting_env) -> None:
    usage_reporting_env.capture.behaviour.status = 500
    quiet_config()
    _spool_one()
    before = make_due().last_sent
    usage_reporting.start()
    assert usage_reporting_env.capture.wait_for(count=1)
    usage_reporting.shutdown()
    end_process()
    assert config().last_sent == before
    assert usage(show(), "mcp:query")["ok"] == 1


def test_timed_out_send_keeps_spool(usage_reporting_env) -> None:
    usage_reporting_env.capture.behaviour.delay = 30
    quiet_config()
    _spool_one()
    before = make_due().last_sent
    usage_reporting.start()
    assert usage_reporting_env.capture.wait_for_arrival()
    started = time.monotonic()
    usage_reporting.shutdown()
    assert time.monotonic() - started < 1.5
    end_process()
    assert config().last_sent == before
    usage_reporting_env.set_now(now_utc() + datetime.timedelta(days=1))
    assert usage(show(), "mcp:query")["ok"] == 1


def test_no_wait_at_exit_without_send_in_flight(usage_reporting_env) -> None:
    quiet_config()
    usage_reporting.start()
    started = time.monotonic()
    usage_reporting.shutdown()
    assert time.monotonic() - started < 0.1
    end_process()


def test_two_simultaneous_senders_send_one_report(store: str, usage_reporting_env) -> None:
    usage_reporting_env.capture.behaviour.delay = 0.5
    run_slayer(["models", "--storage", store, "list"], env=usage_reporting_env.subprocess_env())
    make_due()
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "slayer", "datasources", "--storage", store, "list"],
            env=usage_reporting_env.subprocess_env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(2)
    ]
    assert [p.wait(timeout=120) for p in procs] == [0, 0]
    time.sleep(1.5)
    assert usage_reporting_env.capture.arrivals == 1
    assert len(usage_reporting_env.capture.requests) == 1


def test_stale_lease_taken_over_by_one_sender_only(tmp_path: Path, monkeypatch) -> None:
    lease = tmp_path / "send.lease"
    now = now_utc()
    assert spool.acquire_lease(lease, now=now - datetime.timedelta(hours=1)) is not None
    real_expired = spool._lease_expired
    rival: list[str | None] = []

    def rival_checks_meanwhile(path: Path, *, now: datetime.datetime) -> bool:
        expired = real_expired(path, now=now)
        if not rival:
            rival.append(None)
            rival[0] = spool.acquire_lease(path, now=now)
        return expired

    monkeypatch.setattr(spool, "_lease_expired", rival_checks_meanwhile)
    mine = spool.acquire_lease(lease, now=now)
    assert sum(token is not None for token in (mine, *rival)) == 1


def test_sender_crash_recovered_by_a_later_send(store: str, usage_reporting_env) -> None:
    usage_reporting_env.capture.behaviour.stall_first = 60
    result = run_slayer(["models", "--storage", store, "list"], env=usage_reporting_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    make_due()
    session = McpStdio(args=["--storage", store], env=usage_reporting_env.subprocess_env())
    try:
        assert usage_reporting_env.capture.wait_for_arrival(timeout=60)
    finally:
        session.kill()
    usage_reporting_env.set_now(now_utc() + datetime.timedelta(days=2))
    usage_reporting.start()
    requests = usage_reporting_env.capture.wait_for(count=1, timeout=20)
    end_process()
    assert len(requests) == 1
    assert requests[0].json()["properties"]["usage"]["cli:models.list"]["ok"] == 1


def test_disable_racing_a_send_leaves_nothing_behind(store: str, usage_reporting_env) -> None:
    usage_reporting_env.capture.hold()
    usage_reporting_env.monkeypatch.setattr(sender, "HTTP_TIMEOUT", 120.0)  # outlasts the hold
    quiet_config()
    _spool_one()
    make_due()
    usage_reporting.start()
    assert usage_reporting_env.capture.wait_for_arrival()
    disabled = run_slayer(["usage-reporting", "disable"], env=usage_reporting_env.subprocess_env())
    assert disabled.returncode == 0, disabled.stderr
    assert not usage_reporting_env.capture.requests
    usage_reporting_env.capture.release()
    assert usage_reporting_env.capture.wait_for(count=1, timeout=15)
    usage_reporting.shutdown()
    end_process()
    current = config()
    assert current.setting == "disabled"
    assert current.install_id is None
    assert spool_files() == []


# --- destination and request shape ----------------------------------------------------------------------------


def test_default_endpoint_is_posthog_eu() -> None:
    assert sender.DEFAULT_ENDPOINT == "https://eu.i.posthog.com/i/v0/e/"


def test_request_shape(usage_reporting_env) -> None:
    quiet_config()
    _spool_one()
    make_due()
    usage_reporting.start()
    (request,) = usage_reporting_env.capture.wait_for(count=1)
    end_process()
    assert request.path == "/i/v0/e/"
    assert request.headers.get("Content-Type", "").startswith("application/json")
    body = request.json()
    properties = body["properties"]
    assert body["event"] == "slayer_usage"
    assert body["distinct_id"] == properties["install_id"]
    assert body["uuid"] == properties["batch_id"]
    assert properties["$geoip_disable"] is True
    assert "$process_person_profile" not in properties
    assert "api_key" in body
    assert "timestamp" not in body


# --- install identity -------------------------------------------------------------------------------------------------


def test_install_id_created_on_first_use(usage_reporting_env) -> None:
    quiet_config()
    _spool_one()
    current = config()
    assert isinstance(current.install_id, uuid.UUID)
    assert current.install_id_created == now_utc().date()


@pytest.mark.parametrize(("age_days", "rotated"), [(11 * 31, True), (10 * 30, False)])
def test_install_id_rotation(usage_reporting_env, age_days: int, rotated: bool) -> None:
    old = uuid.uuid4()
    write_config(install_id=old, install_id_created=_T0.date())
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(days=age_days))
    _spool_one()
    current = config()
    assert (current.install_id != old) is rotated
    if rotated:
        assert current.install_id_created == (_T0 + datetime.timedelta(days=age_days)).date()
        assert show()["install_id"] == str(current.install_id)


def test_running_process_rotates_install_id_before_sending(usage_reporting_env) -> None:
    old = uuid.uuid4()
    write_config(install_id=old, install_id_created=(_T0 - datetime.timedelta(days=300)).date())
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=1))
    usage_reporting.start()
    usage_reporting_env.set_now(_T0 + datetime.timedelta(days=60))
    usage_reporting.record(surface="mcp", token="query")
    (request,) = usage_reporting_env.capture.wait_for(count=1)
    end_process()
    assert request.json()["distinct_id"] != str(old)
    assert request.json()["distinct_id"] == str(config().install_id)


def _disable_during_rotation(monkeypatch: pytest.MonkeyPatch) -> threading.Thread:
    """Run ``slayer usage-reporting disable`` (as if from another process) while the install ID rotates."""
    real = settings.with_install_id
    racer = threading.Thread(target=control.disable)

    def rotate_while_disabling(config: settings.UsageReportingConfig, *, today: datetime.date):
        racer.start()
        racer.join(timeout=0.5)
        return real(config, today=today)

    monkeypatch.setattr(settings, "with_install_id", rotate_while_disabling)
    monkeypatch.setattr(sender, "with_install_id", rotate_while_disabling, raising=False)
    return racer


def _assert_stays_disabled(racer: threading.Thread) -> None:
    racer.join(timeout=10)
    assert not racer.is_alive()
    final = config()
    assert final.setting == "disabled"
    assert final.install_id is None


def test_disable_during_send_rotation_is_never_undone(usage_reporting_env, monkeypatch) -> None:
    write_config(install_id=uuid.uuid4(), install_id_created=(_T0 - datetime.timedelta(days=400)).date(),
                 last_sent=_T0 - datetime.timedelta(days=2))
    racer = _disable_during_rotation(monkeypatch)
    paths = sender.Paths(config=settings.config_path(), spool=settings.spool_dir(), lease=settings.lease_path())
    sender.send(paths, now=_T0, url=sender.endpoint())
    _assert_stays_disabled(racer)


def test_disable_during_start_is_never_undone(usage_reporting_env, monkeypatch) -> None:
    quiet_config()
    racer = _disable_during_rotation(monkeypatch)
    usage_reporting.start()
    _assert_stays_disabled(racer)
    end_process()
    assert not spool_files()


def test_install_id_stored_in_config_dir_not_model_storage(store: str, usage_reporting_env) -> None:
    cli(["models", "--storage", store, "list"])
    install_id = str(config().install_id)
    for path in Path(store).rglob("*"):
        if path.is_file():
            assert install_id not in path.read_text(errors="ignore")
    assert settings.config_path().is_relative_to(settings.config_dir())
    assert json.loads(settings.config_path().read_text())
