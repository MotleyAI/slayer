"""``slayer otel status`` and ``slayer otel test``: inspect and check the OTel setup.

Both read the same settings that ``start()`` uses. ``status`` makes no network call and imports
no OTel package. ``test`` sends one empty OTLP request per signal, then one test span. It runs
while ``SLAYER_OTEL`` is off, so an operator can test a backend first, but not with
``OTEL_SDK_DISABLED=true``. Both print endpoints with passwords and query values masked.
There is no ``enable``, ``disable`` or ``show``: the standard environment variables are the only
switch, so no saved setting can contradict them, and telemetry has no single report to show.
"""

from __future__ import annotations

import base64
import importlib.metadata
import os
import ssl
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

from slayer import __version__
from slayer.otel import settings

TEST_SPAN = "slayer.otel.test"
TEST_TIMEOUT_SECONDS = 5

_REDIRECT_HINT = "the endpoint redirects; set the URL it redirects to"
_HINTS = {
    401: "the backend rejected the credentials; check OTEL_EXPORTER_OTLP_HEADERS",
    403: "the backend rejected the credentials; check OTEL_EXPORTER_OTLP_HEADERS",
    404: "no OTLP receiver at this path; OTEL_EXPORTER_OTLP_ENDPOINT is a base URL, /v1/<signal> is added",
    405: "this URL does not accept OTLP; check the endpoint",
    413: "the backend refused the request size",
    **dict.fromkeys((301, 302, 307, 308), _REDIRECT_HINT),
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
        target = _shown(settings.endpoint(signal)) if settings.exports(signal) else "not exported"
        lines.append(f"{signal + ':':<9}{','.join(sorted(requested))} -> {target}")
        unsupported = requested - settings.SUPPORTED_EXPORTERS
        if unsupported:
            lines.append(f"         unsupported exporter values ignored: {', '.join(sorted(unsupported))}")
    names = sorted({name for signal in settings.SIGNALS for name in settings.headers(signal)})
    lines += [
        f"Headers: {', '.join(names) + ' (values hidden)' if names else 'none'}",
        f"Protocol: {_protocols()}",
        f"Sampler: {os.environ.get('OTEL_TRACES_SAMPLER') or 'parentbased_always_on (default)'}",
        f"Content capture: {'on' if settings.capture_content() else 'off'} ({settings.CAPTURE_CONTENT_ENV})",
        f"Log level: {os.environ.get('OTEL_PYTHON_LOG_LEVEL') or 'Python logging (OTEL_PYTHON_LOG_LEVEL not set)'}",
    ]
    return "\n".join(lines)


def test() -> tuple[str, int]:
    """Probe each exported signal's endpoint, then send one test span; return the report and an exit code."""
    if settings.resolve_state().rule is settings.DecidingRule.SDK_DISABLED:
        return "Not tested: OTEL_SDK_DISABLED=true turns OTel off, so no request was sent.", 0
    lines: list[str] = []
    failures = 0
    for signal in settings.SIGNALS:
        if not settings.exports(signal):
            lines.append(f"{signal + ':':<9}skipped, not exported")
            continue
        url = settings.endpoint(signal)
        ok, detail = _probe(url=url, headers=settings.headers(signal), tls=settings.tls(signal))
        failures += 0 if ok else 1
        lines.append(f"{signal + ':':<9}{detail}  {_shown(url)}")
    if failures == 0 and settings.exports("traces"):
        sent, line = _send_test_span()
        failures += 0 if sent is not False else 1
        lines.append(line)
    lines.append("All endpoints accepted OTLP." if failures == 0 else f"{failures} check(s) failed.")
    return "\n".join(lines), 0 if failures == 0 else 1


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect would resend the credential headers to another URL, and turn the POST into a GET."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, PLR0913
        return None


def _probe(*, url: str, headers: dict[str, str], tls: settings.TlsFiles) -> tuple[bool, str]:
    """POST an empty OTLP request: valid protobuf, so it checks reachability, credentials and path only."""
    if _scheme(url) not in ("http", "https"):
        return False, "not a valid URL: the endpoint needs http:// or https://"
    url, headers = _with_url_credentials(url=url, headers=headers)
    try:
        request = urllib.request.Request(
            url, data=b"", method="POST", headers={**headers, "Content-Type": "application/x-protobuf"},
        )
        # The TLS files apply to HTTPS only, as in the exporter: a stale CA path must not fail an HTTP probe.
        handlers: list[urllib.request.BaseHandler] = [_NoRedirect()]
        if request.type == "https":
            handlers.append(urllib.request.HTTPSHandler(context=_tls_context(tls)))
        opener = urllib.request.build_opener(*handlers)
        with opener.open(request, timeout=TEST_TIMEOUT_SECONDS) as response:
            return True, f"ok, HTTP {response.status}"
    except urllib.error.HTTPError as error:
        # Not error.reason: the server chooses that text, and it can echo the URL with its query credentials.
        return False, f"HTTP {error.code}: {_HINTS.get(error.code, 'the backend rejected the request')}"
    except OSError as error:
        reason = getattr(error, "reason", error)
        return False, f"cannot connect: {reason}"
    except ValueError:
        # Not the error text: it quotes the offending URL or header value.
        return False, "not sendable: the endpoint or a header value is malformed"


def _with_url_credentials(*, url: str, headers: dict[str, str]) -> tuple[str, dict[str, str]]:
    """As the exporter's HTTP client does: a URL's user info becomes Basic auth, replacing any Authorization header."""
    parts = urlsplit(url)
    if parts.username is None and parts.password is None:
        return url, headers
    login = f"{unquote(parts.username or '')}:{unquote(parts.password or '')}"
    credentials = {name: value for name, value in headers.items() if name.lower() != "authorization"}
    credentials["Authorization"] = "Basic " + base64.b64encode(login.encode("latin-1")).decode("ascii")
    host = parts.netloc.rpartition("@")[2]
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment)), credentials


def _tls_context(tls: settings.TlsFiles) -> ssl.SSLContext:
    """The CA and client certificate files the OTLP exporter uses; the system defaults otherwise."""
    context = ssl.create_default_context(cafile=tls.certificate)
    if tls.client_certificate:
        context.load_cert_chain(certfile=tls.client_certificate, keyfile=tls.client_key)
    return context


def _scheme(url: str) -> str | None:
    try:
        return urlsplit(url).scheme
    except ValueError:
        return None


def _shown(url: str) -> str:
    """The URL for display: a password in it and every query value become ``***``."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<malformed URL>"
    netloc = parts.netloc
    if parts.password is not None:
        userinfo, _, host = netloc.rpartition("@")
        netloc = f"{userinfo.partition(':')[0]}:***@{host}"
    query = "&".join(
        f"{item.partition('=')[0]}=***" if "=" in item else item for item in parts.query.split("&") if item
    )
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def _send_test_span() -> tuple[bool | None, str]:
    """Send one span; the first value is ``None`` when the span was not sent on purpose."""
    if not _extra_installed():
        return None, "Test span: not sent, the otel extra is not installed."
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # ALLOW(import-not-top): optional extra
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.trace import TracerProvider  # ALLOW(import-not-top): optional extra
    from opentelemetry.sdk.trace.export import SpanExportResult  # ALLOW(import-not-top): optional extra
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON  # ALLOW(import-not-top): optional extra
    from requests import Session  # ALLOW(import-not-top): optional extra, the exporter's HTTP client

    from slayer.otel import logs  # ALLOW(import-not-top): optional extra
    from slayer.otel.exporters import build_resource  # ALLOW(import-not-top): optional extra

    # ALWAYS_ON: an explicit test sends its span whatever OTEL_TRACES_SAMPLER says.
    provider = TracerProvider(resource=build_resource(), sampler=ALWAYS_ON, shutdown_on_exit=False)
    span = provider.get_tracer("slayer", __version__).start_span(TEST_SPAN)
    span.end()
    # The exporter counts a redirect as delivered without following it; the test calls it a failure.
    session = Session()
    session.hooks["response"].append(_refuse_redirect)
    exporter = OTLPSpanExporter(timeout=TEST_TIMEOUT_SECONDS, session=session)
    logs.hide_export_text()
    try:
        result = exporter.export([span])  # pyright: ignore[reportArgumentType]
    finally:
        exporter.shutdown()
        logs.show_export_text()
    if result is SpanExportResult.SUCCESS:
        service = provider.resource.attributes.get("service.name", _service_name())
        return True, f"Test span: sent; search your backend for the span {TEST_SPAN} from service {service}."
    return False, "Test span: the backend rejected it; see the error above."


class _Redirected(Exception):
    """Not a connection error, so the exporter fails at once instead of retrying."""


def _refuse_redirect(response: Any, *args: object, **kwargs: object) -> None:
    if 300 <= response.status_code < 400:
        raise _Redirected(f"HTTP {response.status_code}: {_REDIRECT_HINT}")


def _extra_installed() -> bool:
    return all(_distribution_exists(name) for name in ("opentelemetry-sdk", "opentelemetry-exporter-otlp-proto-http"))


def _distribution_exists(name: str) -> bool:
    # Package metadata, not find_spec: find_spec imports the parent package, which status must not do.
    try:
        importlib.metadata.distribution(name)
    except importlib.metadata.PackageNotFoundError:
        return False
    return True


def _service_name() -> str:
    return settings.service_name() or settings.DEFAULT_SERVICE_NAME


def _protocols() -> str:
    values = {settings.protocol(signal) for signal in settings.SIGNALS}
    unsupported = sorted(values - {settings.SUPPORTED_PROTOCOL})
    note = f"; {', '.join(unsupported)} not supported, http/protobuf is used" if unsupported else ""
    return f"{settings.SUPPORTED_PROTOCOL}{note}"
