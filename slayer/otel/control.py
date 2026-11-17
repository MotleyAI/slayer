"""``slayer otel status`` and ``slayer otel test``: inspect and check the OTel setup.

Both read the same settings that ``start()`` uses. ``status`` makes no network call and imports
no OTel package. ``test`` sends one empty OTLP request per signal, then one test span.
There is no ``enable``, ``disable`` or ``show``: the standard environment variables are the only
switch, so no saved setting can contradict them, and telemetry has no single report to show.
"""

from __future__ import annotations

import importlib.util
import os
import urllib.error
import urllib.request

from slayer import __version__
from slayer.otel import settings

TEST_SPAN = "slayer.otel.test"
TEST_TIMEOUT_SECONDS = 5

_HINTS = {
    401: "the backend rejected the credentials; check OTEL_EXPORTER_OTLP_HEADERS",
    403: "the backend rejected the credentials; check OTEL_EXPORTER_OTLP_HEADERS",
    404: "no OTLP receiver at this path; OTEL_EXPORTER_OTLP_ENDPOINT is a base URL, /v1/<signal> is added",
    405: "this URL does not accept OTLP; check the endpoint",
    413: "the backend refused the request size",
}


def status() -> str:
    state = settings.resolve_state()
    lines = [
        f"OTel is {'on' if state.enabled else 'off'} (decided by: {state.rule.value}).",
        f"otel extra: {'installed' if _extra_installed() else 'not installed; pip install motley-slayer[otel]'}",
        f"Service: {_service_name()} {__version__}",
    ]
    for signal in settings.SIGNALS:
        requested = settings.exporters_requested(signal)
        target = settings.endpoint(signal) if settings.exports(signal) else "not exported"
        lines.append(f"{signal + ':':<9}{','.join(sorted(requested))} -> {target}")
        unsupported = requested - settings.SUPPORTED_EXPORTERS
        if unsupported:
            lines.append(f"         unsupported exporter values ignored: {', '.join(sorted(unsupported))}")
    names = sorted({name for signal in settings.SIGNALS for name in settings.headers(signal)})
    lines += [
        f"Headers: {', '.join(names) + ' (values hidden)' if names else 'none'}",
        f"Protocol: {_protocols()}",
        f"Sampler: {os.environ.get('OTEL_TRACES_SAMPLER') or 'parentbased_always_on (default)'}",
        f"Capture: {', '.join(sorted(settings.capture())) or 'none (SLAYER_OTEL_CAPTURE)'}",
        f"Log level: {os.environ.get('OTEL_PYTHON_LOG_LEVEL') or 'Python logging (OTEL_PYTHON_LOG_LEVEL not set)'}",
    ]
    return "\n".join(lines)


def test() -> tuple[str, int]:
    """Probe each exported signal's endpoint, then send one test span; return the report and an exit code."""
    lines: list[str] = []
    failures = 0
    for signal in settings.SIGNALS:
        if not settings.exports(signal):
            lines.append(f"{signal + ':':<9}skipped, not exported")
            continue
        url = settings.endpoint(signal)
        ok, detail = _probe(url=url, headers=settings.headers(signal))
        failures += 0 if ok else 1
        lines.append(f"{signal + ':':<9}{detail}  {url}")
    if failures == 0 and settings.exports("traces"):
        lines.append(_send_test_span())
    lines.append("All endpoints accepted OTLP." if failures == 0 else f"{failures} endpoint(s) failed.")
    return "\n".join(lines), 0 if failures == 0 else 1


def _probe(*, url: str, headers: dict[str, str]) -> tuple[bool, str]:
    """POST an empty OTLP request: valid protobuf, so it checks reachability, credentials and path only."""
    request = urllib.request.Request(
        url, data=b"", method="POST", headers={**headers, "Content-Type": "application/x-protobuf"},
    )
    try:
        with urllib.request.urlopen(request, timeout=TEST_TIMEOUT_SECONDS) as response:
            return True, f"ok, HTTP {response.status}"
    except urllib.error.HTTPError as error:
        return False, f"HTTP {error.code}: {_HINTS.get(error.code, error.reason)}"
    except (urllib.error.URLError, OSError) as error:
        reason = getattr(error, "reason", error)
        return False, f"cannot connect: {reason}"


def _send_test_span() -> str:
    if not _extra_installed():
        return "Test span: not sent, the otel extra is not installed."
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # ALLOW(import-not-top): optional extra
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.trace import TracerProvider  # ALLOW(import-not-top): optional extra
    from opentelemetry.sdk.trace.export import SpanExportResult  # ALLOW(import-not-top): optional extra

    from slayer.otel.exporters import build_resource  # ALLOW(import-not-top): optional extra

    provider = TracerProvider(resource=build_resource(), shutdown_on_exit=False)
    span = provider.get_tracer("slayer", __version__).start_span(TEST_SPAN)
    span.end()
    exporter = OTLPSpanExporter(timeout=TEST_TIMEOUT_SECONDS)
    try:
        result = exporter.export([span])  # pyright: ignore[reportArgumentType]
    finally:
        exporter.shutdown()
    if result is SpanExportResult.SUCCESS:
        return f"Test span: sent; search your backend for the span {TEST_SPAN} from service {_service_name()}."
    return "Test span: the exporter reported a failure; see the error above."


def _extra_installed() -> bool:
    return all(
        importlib.util.find_spec(name) is not None
        for name in ("opentelemetry.sdk", "opentelemetry.exporter.otlp.proto.http")
    )


def _service_name() -> str:
    if os.environ.get("OTEL_SERVICE_NAME"):
        return os.environ["OTEL_SERVICE_NAME"]
    for item in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
        key, _, value = item.partition("=")
        if key.strip() == "service.name" and value.strip():
            return value.strip()
    return "slayer"


def _protocols() -> str:
    values = {settings.protocol(signal) for signal in settings.SIGNALS}
    unsupported = sorted(values - {settings.SUPPORTED_PROTOCOL})
    note = f"; {', '.join(unsupported)} not supported, http/protobuf is used" if unsupported else ""
    return f"{settings.SUPPORTED_PROTOCOL}{note}"
