"""Opt-out precedence, the ``slayer telemetry`` command and the one-time notice."""

from __future__ import annotations

import json
import time
import uuid
from importlib.metadata import PathDistribution
from pathlib import Path

import pytest

from slayer import telemetry
from slayer.telemetry import settings
from slayer.telemetry.payload import SCHEMA_VERSION
from slayer.telemetry.settings import DecidingRule
from tests._telemetry_helpers import (
    McpStdio,
    build_storage,
    cli,
    end_process,
    make_due,
    quiet_config,
    config,
    show,
    spool_files,
    usage,
    write_config,
)

_NOTICE_URL = "docs.motley.ai/slayer/reference/telemetry"
_ENV_VARS = ("DO_NOT_TRACK", "SLAYER_TELEMETRY", "CI")


@pytest.fixture
def clean_env(telemetry_env, monkeypatch: pytest.MonkeyPatch):
    """No control variable set, not an editable install, writable config dir."""
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(settings, "is_editable_install", lambda *args, **kwargs: False)
    return telemetry_env


def _make_unwritable(env, monkeypatch: pytest.MonkeyPatch) -> None:
    blocker = env.root / "blocker"
    blocker.write_text("a file, not a directory")
    for var in ("XDG_CONFIG_HOME", "HOME", "APPDATA"):
        monkeypatch.setenv(var, str(blocker / "below"))


# --- precedence ----------------------------------------------------------------------


_MATRIX = [
    # env, editable, unwritable, persisted -> enabled, rule
    ({"DO_NOT_TRACK": "1", "SLAYER_TELEMETRY": "on"}, False, False, None, False, DecidingRule.DO_NOT_TRACK),
    ({"DO_NOT_TRACK": "true"}, False, False, "enabled", False, DecidingRule.DO_NOT_TRACK),
    ({"DO_NOT_TRACK": "0"}, False, False, None, True, DecidingRule.DEFAULT),
    ({"SLAYER_TELEMETRY": "on", "CI": "true"}, False, False, None, True, DecidingRule.ENV),
    ({"SLAYER_TELEMETRY": "on"}, True, False, "disabled", True, DecidingRule.ENV),
    ({"SLAYER_TELEMETRY": "off"}, False, False, "enabled", False, DecidingRule.ENV),
    ({"SLAYER_TELEMETRY": "maybe"}, False, False, None, True, DecidingRule.DEFAULT),
    ({"CI": "true"}, False, False, None, False, DecidingRule.CI),
    ({"CI": "true"}, False, False, "enabled", False, DecidingRule.CI),
    ({}, True, False, "enabled", False, DecidingRule.EDITABLE),
    ({}, False, True, None, False, DecidingRule.UNWRITABLE),
    ({}, False, False, "disabled", False, DecidingRule.PERSISTED),
    ({}, False, False, "enabled", True, DecidingRule.PERSISTED),
    ({}, False, False, None, True, DecidingRule.DEFAULT),
]


@pytest.mark.parametrize(("env", "editable", "unwritable", "persisted", "enabled", "rule"), _MATRIX)
def test_precedence(
    clean_env, monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str], editable: bool, unwritable: bool, persisted: str | None, enabled: bool, rule: DecidingRule,
) -> None:
    if persisted is not None:
        write_config(setting=persisted)
    if unwritable:
        _make_unwritable(clean_env, monkeypatch)
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    monkeypatch.setattr(settings, "is_editable_install", lambda *args, **kwargs: editable)
    state = settings.resolve_state()
    assert (state.enabled, state.rule) == (enabled, rule)


def _dist(tmp_path: Path, direct_url: str | None) -> PathDistribution:
    info = tmp_path / "motley_slayer-9.9.9.dist-info"
    info.mkdir()
    (info / "METADATA").write_text("Metadata-Version: 2.1\nName: motley-slayer\nVersion: 9.9.9\n")
    if direct_url is not None:
        (info / "direct_url.json").write_text(direct_url)
    return PathDistribution(info)


@pytest.mark.parametrize(("direct_url", "editable"), [
    ('{"url": "file:///src", "dir_info": {"editable": true}}', True),
    ('{"url": "file:///src", "dir_info": {"editable": false}}', False),
    ('{"url": "https://files.example/x.whl", "archive_info": {}}', False),
    (None, False),
    ("{not json", False),
])
def test_editable_install_detection(tmp_path: Path, direct_url: str | None, editable: bool) -> None:
    assert settings.is_editable_install(_dist(tmp_path, direct_url)) is editable


def test_config_dir_follows_platform_convention(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLAYER_STORAGE", str(tmp_path / "storage"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setattr("sys.platform", "linux")
    assert settings.config_dir() == tmp_path / "xdg" / "slayer"
    monkeypatch.delenv("XDG_CONFIG_HOME")
    assert settings.config_dir() == tmp_path / "home" / ".config" / "slayer"
    monkeypatch.setattr("sys.platform", "darwin")
    assert settings.config_dir() == tmp_path / "home" / "Library" / "Application Support" / "slayer"
    monkeypatch.setattr("sys.platform", "win32")
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    assert settings.config_dir() == tmp_path / "appdata" / "slayer"


# --- off means nothing ------------------------------------------------------------------


@pytest.fixture
def store(telemetry_env) -> str:
    return build_storage(telemetry_env.root)


@pytest.mark.parametrize("env", [{"SLAYER_TELEMETRY": "off"}, {"DO_NOT_TRACK": "1"}, {"CI": "true"}])
def test_off_records_writes_prints_and_sends_nothing(
    clean_env, store: str, monkeypatch: pytest.MonkeyPatch, env: dict[str, str],
) -> None:
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    result = cli(["models", "--storage", store, "list"])
    assert result.returncode == 0, result.stderr
    assert _NOTICE_URL not in result.stderr
    assert not settings.config_dir().exists()
    time.sleep(0.3)
    assert clean_env.capture.arrivals == 0


def test_unwritable_config_dir_is_silent(clean_env, store: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _make_unwritable(clean_env, monkeypatch)
    result = cli(["models", "--storage", store, "list"])
    assert result.returncode == 0, result.stderr
    assert _NOTICE_URL not in result.stderr
    time.sleep(0.3)
    assert clean_env.capture.arrivals == 0


# --- slayer telemetry status|enable|disable|show ----------------------------------------------


def test_status_names_ci_as_the_deciding_rule(clean_env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CI", "true")
    result = cli(["telemetry", "status"])
    assert result.returncode == 0, result.stderr
    assert "off" in result.stdout.lower()
    assert "CI" in result.stdout


def test_status_reports_default_on_and_install_id(clean_env) -> None:
    install_id = uuid.uuid4()
    write_config(install_id=install_id)
    result = cli(["telemetry", "status"])
    assert result.returncode == 0, result.stderr
    assert "on" in result.stdout.lower()
    assert str(install_id) in result.stdout


def test_enable_and_disable_persist(clean_env) -> None:
    assert cli(["telemetry", "disable"]).returncode == 0
    assert config().setting == "disabled"
    assert (settings.resolve_state().enabled, settings.resolve_state().rule) == (False, DecidingRule.PERSISTED)
    assert cli(["telemetry", "enable"]).returncode == 0
    assert config().setting == "enabled"
    assert settings.resolve_state().enabled


def test_disable_removes_spool_and_install_id_and_stops_sending(clean_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    end_process()
    assert spool_files()
    assert config().install_id is not None

    assert cli(["telemetry", "disable"]).returncode == 0
    assert spool_files() == []
    assert config().install_id is None

    make_due()
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    end_process()
    time.sleep(0.3)
    assert clean_env.capture.arrivals == 0
    assert spool_files() == []


def test_show_prints_pending_report_and_does_not_send(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    end_process()
    make_due()
    result = cli(["telemetry", "show"])
    assert result.returncode == 0, result.stderr
    assert usage(json.loads(result.stdout), "mcp:query")["ok"] == 1
    time.sleep(0.5)
    assert telemetry_env.capture.arrivals == 0


def test_show_matches_the_next_send(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    telemetry.record(surface="mcp", token="search", error=ValueError("x"))
    end_process()
    shown = show()
    make_due()
    telemetry.start()
    body = telemetry_env.capture.wait_for(count=1)[0].json()
    end_process()
    for key in ("mcp:query", "mcp:search"):
        assert body["properties"]["usage"][key] == shown["usage"][key]
    assert body["properties"]["install_id"] == shown["install_id"]


# --- the notice --------------------------------------------------------------------------------


def _assert_notice(stderr: str) -> None:
    assert _NOTICE_URL in stderr
    assert "anonymous" in stderr.lower()
    assert "usage" in stderr.lower()
    assert "SLAYER_TELEMETRY=off" in stderr or "slayer telemetry disable" in stderr


def test_notice_once_on_stderr(telemetry_env, store: str) -> None:
    write_config(last_sent=None)
    first = cli(["models", "--storage", store, "list"])
    second = cli(["models", "--storage", store, "list"])
    _assert_notice(first.stderr)
    assert _NOTICE_URL not in first.stdout
    assert _NOTICE_URL not in second.stderr
    assert config().notice_shown_schema == SCHEMA_VERSION


def test_notice_again_after_schema_major_bump(telemetry_env, store: str) -> None:
    quiet_config()
    write_config(notice_shown_schema=SCHEMA_VERSION - 1)
    first = cli(["models", "--storage", store, "list"])
    second = cli(["models", "--storage", store, "list"])
    _assert_notice(first.stderr)
    assert _NOTICE_URL not in second.stderr


def test_stdio_mcp_notice_goes_to_stderr_only(telemetry_env, store: str) -> None:
    session = McpStdio(args=["--storage", store], env=telemetry_env.subprocess_env())
    try:
        session.initialize()
        session.request("tools/list", {})
        session.close_stdin()
        session.wait()
    finally:
        session.kill()
    for line in session.stdout_lines:
        assert json.loads(line)["jsonrpc"] == "2.0"
    _assert_notice(session.stderr.decode())


def _mcp_transcript(*, env: dict[str, str], store: str) -> list[bytes]:
    session = McpStdio(args=["--storage", store], env=env)
    try:
        session.initialize()
        session.request("tools/list", {})
        session.call_tool("list_datasources", {})
        session.close_stdin()
        session.wait()
    finally:
        session.kill()
    return session.stdout_lines


def test_stdio_mcp_stdout_identical_with_telemetry_on_and_off(telemetry_env, store: str) -> None:
    on = _mcp_transcript(env=telemetry_env.subprocess_env(), store=store)
    off = _mcp_transcript(env=telemetry_env.subprocess_env(SLAYER_TELEMETRY="off"), store=store)
    assert on == off
