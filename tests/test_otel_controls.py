"""OTel switches: the ``SLAYER_OTEL`` precedence, the standard ``OTEL_*`` variables, and ``slayer otel``."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from slayer.otel import control, settings
from slayer.otel.settings import DecidingRule
from tests._usage_reporting_capture import CaptureServer, RefusedEndpoint
from tests._usage_reporting_helpers import cli, run_python

_SECRET = "Basic%20c2VjcmV0dG9rZW4="


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for var in (
        "SLAYER_OTEL", "SLAYER_OTEL_CAPTURE_CONTENT", "OTEL_SDK_DISABLED", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER", "OTEL_LOGS_EXPORTER", "OTEL_PYTHON_LOG_LEVEL",
        "OTEL_TRACES_SAMPLER", "OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_CLIENT_KEY",
        *(f"OTEL_EXPORTER_OTLP_{signal.upper()}_{part}" for signal in settings.SIGNALS
          for part in ("ENDPOINT", "HEADERS", "PROTOCOL", "CERTIFICATE", "CLIENT_CERTIFICATE", "CLIENT_KEY")),
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


_CAPTURE = [("", False), ("on", True), (" TRUE ", True), ("1", True), ("yes", True), ("off", False), ("sql", False)]


@pytest.mark.parametrize(("raw", "expected"), _CAPTURE)
def test_capture_content_is_one_switch(clean_env: pytest.MonkeyPatch, raw: str, expected: bool) -> None:
    clean_env.setenv("SLAYER_OTEL_CAPTURE_CONTENT", raw)
    assert settings.capture_content() is expected


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
    ({"OTEL_RESOURCE_ATTRIBUTES": "service.name=Sales%20API"}, "Sales API"),
    ({"OTEL_SERVICE_NAME": "unknown_service-canary"}, "unknown_service-canary"),
]


@pytest.mark.parametrize(("env", "expected"), _SERVICE)
def test_status_names_the_service(clean_env: pytest.MonkeyPatch, env: dict[str, str], expected: str) -> None:
    for name, value in env.items():
        clean_env.setenv(name, value)
    assert control.status().splitlines()[2].startswith(f"Service: {expected} ")


def test_status_command_prints_the_report(clean_env: pytest.MonkeyPatch) -> None:
    result = cli(["otel", "status"])
    assert (result.returncode, result.stdout.startswith("OTel is off (decided by: default).")) == (0, True)


_STATUS_WITHOUT_EXTRA = (
    "import importlib.abc, sys\n"
    "class Block(importlib.abc.MetaPathFinder):\n"
    "    def find_spec(self, name, path=None, target=None):\n"
    "        if name.split('.')[0] == 'opentelemetry':\n"
    "            raise ModuleNotFoundError(name)\n"
    "sys.meta_path.insert(0, Block())\n"
    "for module in [name for name in sys.modules if name.split('.')[0] == 'opentelemetry']:\n"
    "    del sys.modules[module]\n"
    "import importlib.metadata as md\n"
    "real = md.distribution\n"
    "def hidden(name):\n"
    "    if name.startswith('opentelemetry'):\n"
    "        raise md.PackageNotFoundError(name)\n"
    "    return real(name)\n"
    "md.distribution = hidden\n"
    "from slayer.otel import control\n"
    "print(control.status())\n"
)

_STATUS_IMPORTS = (
    "import sys\n"
    "from slayer.otel import control\n"
    "control.status()\n"
    "print(sorted(name for name in sys.modules if name.startswith('opentelemetry.sdk')))\n"
)


def test_status_imports_no_otel_package(clean_env: pytest.MonkeyPatch) -> None:
    result = run_python(_STATUS_IMPORTS, env=dict(os.environ))
    assert (result.returncode, result.stdout.strip()) == (0, "[]"), result.stderr


def test_service_name_keeps_the_last_value_as_the_sdk_does(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=api,service.name=worker")
    assert settings.service_name() == "worker"


def test_status_works_without_the_extra(clean_env: pytest.MonkeyPatch) -> None:
    result = run_python(_STATUS_WITHOUT_EXTRA, env=dict(os.environ))
    assert (result.returncode, "otel extra: not installed" in result.stdout) == (0, True), result.stderr


_URLS = [
    ("http://user:pw@collector:4318/v1/traces", "http://user:***@collector:4318/v1/traces"),
    ("https://collector/v1/traces?api_key=s3cret&region=eu", "https://collector/v1/traces?api_key=***&region=***"),
    ("http://collector:4318/v1/traces", "http://collector:4318/v1/traces"),
]


@pytest.mark.parametrize(("url", "shown"), _URLS)
def test_endpoints_are_shown_without_secrets(url: str, shown: str) -> None:
    assert control._shown(url) == shown


def test_status_masks_a_password_in_the_endpoint(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://user:s3cret@collector:4318")
    assert ("s3cret" in control.status(), "user:***@collector" in control.status()) == (False, True)


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
    received = backend.wait_for(count=4)
    sent = [{name.lower(): value for name, value in request.headers.items()} for request in received]
    assert ([request.path for request in received].count("/v1/traces"), {headers.get("x-token") for headers in sent}) == (
        2, {"abc"},
    )


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
    assert (code, report.count("cannot connect"), report.splitlines()[-1]) == (1, 3, "3 check(s) failed.")


def test_test_skips_a_signal_that_is_not_exported(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_LOGS_EXPORTER", "none")
    report, code = control.test()
    assert (code, "logs:    skipped, not exported" in report) == (0, True)


def test_test_fails_when_the_backend_rejects_the_span(
    backend: CaptureServer, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(control, "_send_test_span", lambda: (False, "Test span: the backend rejected it."))
    report, code = control.test()
    assert (code, report.splitlines()[-1]) == (1, "1 check(s) failed.")


def test_test_sends_the_span_whatever_the_sampler(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_TRACES_SAMPLER", "always_off")
    report, code = control.test()
    assert (code, "Test span: sent" in report) == (0, True), report


def test_test_names_the_decoded_service(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=Sales%20API")
    report, _ = control.test()
    assert "from service Sales API." in report


def test_test_sends_nothing_with_the_sdk_disabled(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_SDK_DISABLED", "true")
    report, code = control.test()
    assert (code, report.startswith("Not tested"), backend.wait_for(count=1, timeout=0.3)) == (0, True, [])


def test_test_sends_no_span_when_traces_are_not_exported(
    backend: CaptureServer, clean_env: pytest.MonkeyPatch,
) -> None:
    clean_env.setenv("OTEL_TRACES_EXPORTER", "none")
    report, code = control.test()
    paths = sorted(request.path for request in backend.wait_for(count=2))
    assert (code, paths, control.TEST_SPAN in report) == (0, ["/v1/logs", "/v1/metrics"], False)


class _Redirecting(BaseHTTPRequestHandler):
    seen: list[tuple[str, str, str | None]] = []

    def do_POST(self) -> None:  # noqa: N802 — http.server naming
        self.seen.append(("POST", self.path, self.headers.get("x-token")))
        self.send_response(302)
        self.send_header("Location", "/elsewhere")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 — http.server naming
        self.seen.append(("GET", self.path, self.headers.get("x-token")))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 — http.server signature
        return None


def test_test_does_not_follow_a_redirect(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setattr(_Redirecting, "seen", [])
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Redirecting)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        host, port = server.server_address[:2]
        clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://{host}:{port}")
        clean_env.setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-token=s3cret")
        report, code = control.test()
    finally:
        server.shutdown()
        server.server_close()
    methods = {method for method, _, _ in _Redirecting.seen}
    assert (code, methods, "redirects" in report) == (1, {"POST"}, True)


def test_test_uses_the_configured_certificate(clean_env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:9")
    clean_env.setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", str(tmp_path / "missing-ca.pem"))
    report, code = control.test()
    assert (code, report.count("cannot connect"), "missing-ca.pem" in report or "No such file" in report) == (
        1, 3, True,
    )


def test_test_ignores_tls_files_for_an_http_endpoint(
    backend: CaptureServer, clean_env: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", str(tmp_path / "stale-ca.pem"))
    report, code = control.test()
    assert (code, "cannot connect" in report) == (0, False), report


def test_test_reports_an_endpoint_without_a_scheme(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector")
    report, code = control.test()
    assert (code, report.count("not a valid URL")) == (1, 3)


def test_test_sends_endpoint_credentials_as_basic_auth(backend: CaptureServer, clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", backend.url.removesuffix("/i/v0/e/").replace("://", "://ops:pw@"))
    report, code = control.test()
    sent = {request.headers.get("Authorization") for request in backend.wait_for(count=4)}
    assert (code, sent) == (0, {"Basic b3BzOnB3"}), report


def test_test_reports_an_unsendable_header_without_its_value(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", RefusedEndpoint().url.removesuffix("/i/v0/e/"))
    clean_env.setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer%20hidden%0Avalue")
    report, code = control.test()
    assert (code, report.count("not sendable"), "hidden" in report) == (1, 3, False), report


def test_test_command_exits_with_one_on_failure(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", RefusedEndpoint().url.removesuffix("/i/v0/e/"))
    result = cli(["otel", "test"])
    assert (result.returncode, "check(s) failed" in result.stdout) == (1, True)
