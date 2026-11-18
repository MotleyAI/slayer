"""OTel providers for processes started by the ``slayer`` CLI, from the standard ``OTEL_*`` variables.

Supported: OTLP over HTTP/protobuf, or ``none``, per signal. The SDK and the OTLP exporters
read the endpoint, headers, sampler, metric interval and resource variables themselves.
"""

from __future__ import annotations

import logging
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import BaseModel, ConfigDict, Field

from slayer import __version__
from slayer.otel import recorder, settings

logger = logging.getLogger(__name__)

class Configured(BaseModel):
    """The providers created here, all private to SLayer; ``owned`` lists them for shutdown."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    owned: list[Any] = Field(default_factory=list)
    tracer_provider: TracerProvider | None = None
    meter_provider: MeterProvider | None = None
    logger_provider: LoggerProvider | None = None


def configure() -> Configured:
    """Create SLayer's providers, unless the host set a global tracer provider; then create none.

    The providers stay private, never global. A library that traces through the global provider
    (newer FastAPI versions do) then exports nothing through SLayer, so its URL paths and
    query values never leave the process. If a step fails, the providers created so far shut down.
    """
    if not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider):
        return Configured()
    _note_unsupported_values()
    configured = Configured()
    try:
        _create_providers(configured)
    except Exception:
        shutdown_all(configured.owned)
        raise
    return configured


def _create_providers(configured: Configured) -> None:
    resource = build_resource()
    # shutdown_on_exit=False: the SDK's own exit hook would flush without a time limit;
    # recorder.shutdown() flushes these providers with a bounded wait instead.
    tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=False)
    configured.owned.append(tracer_provider)
    configured.tracer_provider = tracer_provider
    if settings.exports("traces"):
        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    readers = [PeriodicExportingMetricReader(OTLPMetricExporter())] if settings.exports("metrics") else []
    meter_provider = MeterProvider(resource=resource, metric_readers=readers, shutdown_on_exit=False)
    configured.owned.append(meter_provider)
    configured.meter_provider = meter_provider
    if settings.exports("logs"):
        logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
        configured.owned.append(logger_provider)
        configured.logger_provider = logger_provider
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))


def shutdown_all(providers: list[Any]) -> None:
    """Close ``providers`` within the same 1 s bound as the exit flush."""
    recorder.close_providers(providers)


def _note_unsupported_values() -> None:
    """``console`` exporters and the ``grpc`` protocol are not supported; say so at DEBUG."""
    for signal in settings.SIGNALS:
        unsupported = settings.exporters_requested(signal) - settings.SUPPORTED_EXPORTERS
        if unsupported:
            logger.debug(
                "otel: %s exporter values %s are not supported; only otlp and none", signal, sorted(unsupported),
            )
        if settings.protocol(signal) != settings.SUPPORTED_PROTOCOL:
            logger.debug(
                "otel: %s protocol %s is not supported; using http/protobuf", signal, settings.protocol(signal),
            )


def build_resource() -> Resource:
    """The SDK's resource from the environment, plus SLayer's name and version where the operator set none."""
    detected = Resource.create()
    defaults: dict[str, str] = {}
    if settings.service_name() is None:
        defaults["service.name"] = settings.DEFAULT_SERVICE_NAME
    if "service.version" not in detected.attributes:
        defaults["service.version"] = __version__
    return detected.merge(Resource(defaults)) if defaults else detected
