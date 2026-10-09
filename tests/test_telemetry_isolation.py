"""telemetry.arc42.md principle 1: fail-silent — telemetry never affects the host process."""

from __future__ import annotations

import datetime
import json
import shutil
import time
from collections.abc import Callable

import pytest

from slayer import telemetry
from slayer.telemetry import settings
from tests._telemetry_capture import RefusedEndpoint
from tests._telemetry_helpers import (
    SENT,
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
def store(telemetry_env) -> str:
    quiet_config()
    return build_storage(telemetry_env.root)


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
        mp.setenv("SLAYER_TELEMETRY", "off")
        return _outcomes(_commands(store))


def _refused(env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_TELEMETRY_ENDPOINT", RefusedEndpoint().url)


def _timeout(env, monkeypatch: pytest.MonkeyPatch) -> None:
    env.capture.behaviour.delay = 30


def _server_error(env, monkeypatch: pytest.MonkeyPatch) -> None:
    env.capture.behaviour.status = 500


def _corrupt_spool(env, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = settings.spool_dir()
    directory.mkdir(parents=True, exist_ok=True)
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
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

    monkeypatch.setattr("slayer.telemetry.clock.now", broken)


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
    store: str, telemetry_env, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    expected = _baseline(store, monkeypatch)
    make_due()
    _FAILURES[failure](telemetry_env, monkeypatch)
    assert _outcomes(_commands(store)) == expected


def test_corrupt_config_leaves_stdout_and_exit_code_unchanged(
    store: str, telemetry_env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = [(code, out) for code, out, _ in _baseline(store, monkeypatch)]
    settings.config_path().write_text('{"install_id": 42, "last_sent": "yesterday-ish", "setting": [1]}')
    assert [(code, out) for code, out, _ in _outcomes(_commands(store))] == expected


class _EvilError(Exception):
    def __str__(self) -> str:
        raise RuntimeError(SENT)

    def __repr__(self) -> str:
        raise RuntimeError(SENT)


def test_record_never_raises_on_hostile_input(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query", error=_EvilError())
    telemetry.record(surface="no-such-surface", token=SENT)
    telemetry.record(surface="mcp", token=None)  # type: ignore[arg-type]
    telemetry.record(surface="mcp", token=SENT * 1000, error=KeyboardInterrupt())
    end_process()
    report = show()
    assert usage(report, "mcp:query")["errors"] == {"other": 1}
    assert_no_sentinel(json.dumps(report))
    assert_no_sentinel(spool_bytes())


def test_flush_and_shutdown_never_raise_when_storage_vanishes(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    shutil.rmtree(settings.config_dir())
    telemetry.shutdown()
    end_process()


def test_stalled_send_delays_exit_by_at_most_about_a_second(store: str, telemetry_env) -> None:
    args = ["models", "--storage", store, "list"]
    off_env = telemetry_env.subprocess_env(SLAYER_TELEMETRY="off")
    started = time.monotonic()
    assert run_slayer(args, env=off_env).returncode == 0
    off_duration = time.monotonic() - started

    run_slayer(args, env=telemetry_env.subprocess_env())
    make_due()
    telemetry_env.capture.behaviour.delay = 10
    started = time.monotonic()
    result = run_slayer(args, env=telemetry_env.subprocess_env())
    on_duration = time.monotonic() - started
    assert result.returncode == 0, result.stderr
    assert telemetry_env.capture.arrivals == 1
    assert on_duration - off_duration < 2.0

    telemetry_env.set_now(now_utc() + datetime.timedelta(days=1))
    assert usage(show(), "cli:models.list")["ok"] == 2


def test_first_run_notice_and_send_never_touch_stdout(store: str, telemetry_env) -> None:
    settings.config_path().unlink()
    args = ["models", "--storage", store, "list"]
    first_run = run_slayer(args, env=telemetry_env.subprocess_env())
    off = run_slayer(args, env=telemetry_env.subprocess_env(SLAYER_TELEMETRY="off"))
    assert (first_run.returncode, first_run.stdout) == (off.returncode, off.stdout)
