"""SLayer's OTel switches and the standard ``OTEL_*`` variables, read without importing OTel.

``start()``, ``exporters.configure()`` and ``slayer otel status`` all read the environment here,
so the status command reports exactly what a start would do.
"""

from __future__ import annotations

import logging
import os
from enum import StrEnum
from urllib.parse import unquote

from pydantic import BaseModel

logger = logging.getLogger(__name__)

_ON = frozenset({"1", "true", "yes", "on"})
CAPTURE_CONTENT_ENV = "SLAYER_OTEL_CAPTURE_CONTENT"
SIGNALS = ("traces", "metrics", "logs")
DEFAULT_ENDPOINT = "http://localhost:4318"
SUPPORTED_EXPORTERS = frozenset({"otlp", "none"})
SUPPORTED_PROTOCOL = "http/protobuf"
DEFAULT_SERVICE_NAME = "slayer"


class DecidingRule(StrEnum):
    SDK_DISABLED = "OTEL_SDK_DISABLED"
    ENV = "SLAYER_OTEL"
    DEFAULT = "default"


class OtelState(BaseModel):
    """Whether OTel is on for a CLI-started process, and the first rule that decided it."""

    enabled: bool
    rule: DecidingRule


def resolve_state() -> OtelState:
    """First match wins: ``OTEL_SDK_DISABLED=true``, then ``SLAYER_OTEL``, then off."""
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return OtelState(enabled=False, rule=DecidingRule.SDK_DISABLED)
    requested = os.environ.get("SLAYER_OTEL", "").strip().lower()
    if requested:
        return OtelState(enabled=requested in _ON, rule=DecidingRule.ENV)
    return OtelState(enabled=False, rule=DecidingRule.DEFAULT)


def enabled() -> bool:
    state = resolve_state()
    if state.rule is DecidingRule.SDK_DISABLED:
        logger.debug("otel: OTEL_SDK_DISABLED=true keeps OTel off")
    return state.enabled


def capture_content() -> bool:
    """Whether ``SLAYER_OTEL_CAPTURE_CONTENT`` opts into content: SQL text, exception text and full log records."""
    return os.environ.get(CAPTURE_CONTENT_ENV, "").strip().lower() in _ON


def exporters_requested(signal: str) -> frozenset[str]:
    """The values of ``OTEL_{SIGNAL}_EXPORTER``; ``otlp`` is the standard default."""
    raw = os.environ.get(f"OTEL_{signal.upper()}_EXPORTER") or "otlp"
    return frozenset(item.strip().lower() for item in raw.split(",") if item.strip())


def exports(signal: str) -> bool:
    return "otlp" in exporters_requested(signal)


def endpoint(signal: str) -> str:
    """The URL a signal is sent to: the per-signal variable as given, else the base URL plus ``/v1/<signal>``."""
    specific = os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT", "").strip()
    if specific:
        return specific
    base = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip() or DEFAULT_ENDPOINT
    return f"{base.rstrip('/')}/v1/{signal}"


def headers(signal: str) -> dict[str, str]:
    """The OTLP headers for a signal; the per-signal variable replaces the general one."""
    raw = (os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_HEADERS")
           or os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", ""))
    pairs = (item.split("=", 1) for item in raw.split(",") if "=" in item)
    return {unquote(name).strip(): unquote(value).strip() for name, value in pairs}


def protocol(signal: str) -> str:
    return (os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL")
            or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
            or SUPPORTED_PROTOCOL).strip().lower()


def service_name() -> str | None:
    """The service name the operator set, decoded as the SDK decodes it; ``None`` if none is set."""
    explicit = os.environ.get("OTEL_SERVICE_NAME", "").strip()
    if explicit:
        return explicit
    found = None
    for item in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
        key, _, value = item.partition("=")
        if key.strip() == "service.name" and value.strip():
            found = unquote(value.strip())  # the SDK keeps the last value
    return found


class TlsFiles(BaseModel):
    """The certificate files the OTLP exporter uses for a signal; ``None`` means the default."""

    certificate: str | None = None
    client_certificate: str | None = None
    client_key: str | None = None


def tls(signal: str) -> TlsFiles:
    """The per-signal ``*_CERTIFICATE``, ``*_CLIENT_CERTIFICATE`` and ``*_CLIENT_KEY``, else the general ones."""

    def read(suffix: str) -> str | None:
        return (os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_{suffix}")
                or os.environ.get(f"OTEL_EXPORTER_OTLP_{suffix}") or None)

    return TlsFiles(
        certificate=read("CERTIFICATE"),
        client_certificate=read("CLIENT_CERTIFICATE"),
        client_key=read("CLIENT_KEY"),
    )
