"""Opt-in OpenTelemetry for SLayer: spans and metrics from ``slayer.core.stages``."""

from slayer.otel.middleware import RequestMiddleware
from slayer.otel.recorder import (
    instrument,
    is_active,
    request,
    reset,
    shutdown,
    start,
    uninstrument,
)

__all__ = [
    "RequestMiddleware",
    "instrument",
    "is_active",
    "request",
    "reset",
    "shutdown",
    "start",
    "uninstrument",
]
