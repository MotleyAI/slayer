"""OTel switches: the ``SLAYER_OTEL`` precedence, the standard ``OTEL_*`` variables, and ``slayer otel``."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from slayer.otel import control, settings
from slayer.otel.settings import DecidingRule
from tests._usage_reporting_capture import CaptureServer, RefusedEndpoint
from tests._usage_reporting_helpers import cli

_SECRET = "Basic%20c2VjcmV0dG9rZW4="


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for var in (
        "SLAYER_OTEL", "SLAYER_OTEL_CAPTURE", "OTEL_SDK_DISABLED", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER", "OTEL_LOGS_EXPORTER", "OTEL_PYTHON_LOG_LEVEL",
        *(f"OTEL_EXPORTER_OTLP_{signal.upper()}_{part}" for signal in settings.SIGNALS
          for part in ("ENDPOINT", "HEADERS", "PROTOCOL")),
    ):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def backend(clean_env: pytest.MonkeyPatch) -> Iterator[CaptureServer]:
    server = CaptureServer().start()
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.url.removesuffix("/i/v0/e/"))
    try:
        yield server
    finally:
        server.stop()


# --- precedence ----------------------------------------------------------------------


_MATRIX = [
    # env -> enabled, rule
    ({}, False, DecidingRule.DEFAULT),
    ({"SLAYER_OTEL": "on"}, True, DecidingRule.ENV),
    ({"SLAYER_OTEL": "TRUE"}, True, DecidingRule.ENV),
    ({"SLAYER_OTEL": "1"}, True, DecidingRule.ENV),
    ({"SLAYER_OTEL": "off"}, False, DecidingRule.ENV),
    ({"SLAYER_OTEL": "maybe"}, False, DecidingRule.ENV),
    ({"SLAYER_OTEL": "  "}, False, DecidingRule.DEFAULT),
    ({"SLAYER_OTEL": "on", "OTEL_SDK_DISABLED": "true"}, False, DecidingRule.SDK_DISABLED),
    ({"SLAYER_OTEL": "on", "OTEL_SDK_DISABLED": "True "}, False, DecidingRule.SDK_DISABLED),
    ({"SLAYER_OTEL": "on", "OTEL_SDK_DISABLED": "false"}, True, DecidingRule.ENV),
    ({"SLAYER_OTEL": "on", "OTEL_SDK_DISABLED": "1"}, True, DecidingRule.ENV),
]


@pytest.mark.parametrize(("env", "enabled", "rule"), _MATRIX)
def test_precedence(clean_env: pytest.MonkeyPatch, env: dict[str, str], enabled: bool, rule: DecidingRule) -> None:
    for name, value in env.items():
        clean_env.setenv(name, value)
    state = settings.resolve_state()
    assert (state.enabled, state.rule) == (enabled, rule)


# --- standard variables --------------------------------------------------------------


_CAPTURE = [
    ("", frozenset()),
    ("sql", frozenset({"sql"})),
    (" SQL , exceptions ", frozenset({"sql", "exceptions"})),
    ("sql,params,everything", frozenset({"sql"})),
]


@pytest.mark.parametrize(("raw", "expected"), _CAPTURE)
def test_capture_keeps_only_known_values(clean_env: pytest.MonkeyPatch, raw: str, expected: frozenset[str]) -> None:
    clean_env.setenv("SLAYER_OTEL_CAPTURE", raw)
    assert settings.capture() == expected


_ENDPOINTS = [
    ({}, "traces", "http://localhost:4318/v1/traces"),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.example:4318/"}, "metrics", "https://otel.example:4318/v1/metrics"),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.example/api/default"}, "logs",
     "https://otel.example/api/default/v1/logs"),
    ({"OTEL_EXPORTER_OTLP_ENDPOINT": "https://a", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://b/custom"},
     "traces", "https://b/custom"),
]


@pytest.mark.parametrize(("env", "signal", "expected"), _ENDPOINTS)
def test_endpoint_follows_the_otlp_rules(
    clean_env: pytest.MonkeyPatch, env: dict[str, str], signal: str, expected: str,
) -> None:
    for name, value in env.items():
        clean_env.setenv(name, value)
    assert settings.endpoint(signal) == expected


def test_signal_headers_replace_the_general_headers(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=general,x-org=1")
    clean_env.setenv("OTEL_EXPORTER_OTLP_LOGS_HEADERS", f"Authorization={_SECRET}")
    assert (settings.headers("traces"), settings.headers("logs")) == (
        {"authorization": "general", "x-org": "1"}, {"Authorization": "Basic c2VjcmV0dG9rZW4="},
    )


_EXPORTERS = [
    (None, True),
    ("otlp", True),
    ("none", False),
    ("console", False),
    ("console,otlp", True),
]


@pytest.mark.parametrize(("raw", "exported"), _EXPORTERS)
def test_only_otlp_is_exported(clean_env: pytest.MonkeyPatch, raw: str | None, exported: bool) -> None:
    if raw is not None:
        clean_env.setenv("OTEL_METRICS_EXPORTER", raw)
    assert settings.exports("metrics") is exported


# --- status --------------------------------------------------------------------------


def test_status_names_the_deciding_rule(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("SLAYER_OTEL", "on")
    clean_env.setenv("OTEL_SDK_DISABLED", "true")
    assert control.status().splitlines()[0] == "OTel is off (decided by: OTEL_SDK_DISABLED)."


def test_status_shows_header_names_and_hides_values(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_HEADERS", f"Authorization={_SECRET}")
    report = control.status()
    assert ("Headers: Authorization (values hidden)" in report, "c2VjcmV0" in report) == (True, False)


def test_status_reports_unsupported_values(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_LOGS_EXPORTER", "console")
    clean_env.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    report = control.status()
    assert ("unsupported exporter values ignored: console" in report, "grpc not supported" in report) == (True, True)


_SERVICE = [
    ({}, "slayer"),
    ({"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=prod,service.name=bi"}, "bi"),
    ({"OTEL_RESOURCE_ATTRIBUTES": "service.name=bi", "OTEL_SERVICE_NAME": "api"}, "api"),
]


@pytest.mark.parametrize(("env", "expected"), _SERVICE)
def test_status_names_the_service(clean_env: pytest.MonkeyPatch, env: dict[str, str], expected: str) -> None:
    for name, value in env.items():
        clean_env.setenv(name, value)
    assert control.status().splitlines()[2].startswith(f"Service: {expected} ")


def test_status_command_prints_the_report(clean_env: pytest.MonkeyPatch) -> None:
    result = cli(["otel", "status"])
    assert (result.returncode, result.stdout.startswith("OTel is off (decided by: default).")) == (0, True)


# --- test ----------------------------------------------------------------------------


def test_test_probes_every_signal_and_sends_one_span(backend: CaptureServer) -> None:
    report, code = control.test()
    paths = sorted(request.path for request in backend.wait_for(count=4))
    assert (code, paths, control.TEST_SPAN in report) == (
        0, ["/v1/logs", "/v1/metrics", "/v1/traces", "/v1/traces"], True,
    )


def test_test_sends_the_configured_headers(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-token=abc")
    control.test()
    sent = [{name.lower(): value for name, value in request.headers.items()} for request in backend.wait_for(count=4)]
    assert {headers.get("x-token") for headers in sent} == {"abc"}


_REJECTIONS = [
    (401, "check OTEL_EXPORTER_OTLP_HEADERS"),
    (404, "/v1/<signal> is added"),
    (413, "request size"),
]


@pytest.mark.parametrize(("status", "hint"), _REJECTIONS)
def test_test_explains_a_rejection(backend: CaptureServer, status: int, hint: str) -> None:
    backend.behaviour.status = status
    report, code = control.test()
    assert (code, hint in report, control.TEST_SPAN in report) == (1, True, False)


def test_test_reports_a_refused_connection(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", RefusedEndpoint().url.removesuffix("/i/v0/e/"))
    report, code = control.test()
    assert (code, report.count("cannot connect"), report.splitlines()[-1]) == (1, 3, "3 endpoint(s) failed.")


def test_test_skips_a_signal_that_is_not_exported(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_LOGS_EXPORTER", "none")
    report, code = control.test()
    assert (code, "logs:    skipped, not exported" in report) == (0, True)


def test_test_command_exits_with_one_on_failure(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", RefusedEndpoint().url.removesuffix("/i/v0/e/"))
    result = cli(["otel", "test"])
    assert (result.returncode, "endpoint(s) failed" in result.stdout) == (1, True)
