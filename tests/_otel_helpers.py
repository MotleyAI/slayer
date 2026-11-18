"""In-memory OTel providers for the ``otel_env`` fixture, plus readers for what they collected."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from pydantic import BaseModel, ConfigDict

from slayer import otel


class MetricPoint(BaseModel):
    name: str
    unit: str
    attributes: dict[str, Any]
    value: float
    count: int = 0
    buckets: tuple[float, ...] = ()


class OtelCapture(BaseModel):
    """Private providers with in-memory exporters; nothing touches OTel's global providers."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    spans_exporter: InMemorySpanExporter
    metric_reader: InMemoryMetricReader
    tracer_provider: TracerProvider
    meter_provider: MeterProvider

    @classmethod
    def build(cls) -> OtelCapture:
        exporter = InMemorySpanExporter()
        reader = InMemoryMetricReader()
        # ALWAYS_ON and an enabled SDK: an inherited OTEL_TRACES_SAMPLER=always_off or
        # OTEL_SDK_DISABLED=true would otherwise leave nothing to check.
        with sdk_enabled():
            tracer_provider = TracerProvider(sampler=ALWAYS_ON, shutdown_on_exit=False)
            meter_provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        return cls(
            spans_exporter=exporter, metric_reader=reader,
            tracer_provider=tracer_provider, meter_provider=meter_provider,
        )

    def instrument(self, *, capture: tuple[str, ...] = ()) -> None:
        otel.instrument(tracer_provider=self.tracer_provider, meter_provider=self.meter_provider, capture=capture)

    def close(self) -> None:
        otel.reset()
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()

    def spans(self) -> list[ReadableSpan]:
        return list(self.spans_exporter.get_finished_spans())

    def span_names(self) -> list[str]:
        return [span.name for span in self.spans()]

    def span(self, name: str) -> ReadableSpan:
        matches = [span for span in self.spans() if span.name == name]
        assert matches, f"no span {name!r}; got {self.span_names()}"
        return matches[-1]

    def attributes(self, name: str) -> dict[str, Any]:
        return dict(self.span(name).attributes or {})

    def parent_name(self, name: str) -> str | None:
        child = self.span(name)
        if child.parent is None:
            return None
        by_id = {span.context.span_id: span.name for span in self.spans() if span.context is not None}
        return by_id.get(child.parent.span_id)

    def metrics(self) -> list[MetricPoint]:
        data = self.metric_reader.get_metrics_data()
        points: list[MetricPoint] = []
        if data is None:
            return points
        for resource in data.resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    for point in metric.data.data_points:
                        points.append(_point(name=metric.name, unit=metric.unit or "", point=point))
        return points

    def metric(self, name: str) -> list[MetricPoint]:
        return [point for point in self.metrics() if point.name == name]

    def everything(self) -> str:
        """Every exported span, event and metric as one text blob, for leak checks."""
        parts = [span.to_json() for span in self.spans()]
        parts += [point.model_dump_json() for point in self.metrics()]
        return "\n".join(parts)


@contextmanager
def sdk_enabled() -> Iterator[None]:
    """Clear OTEL_SDK_DISABLED while providers are built: the SDK reads it at construction."""
    saved = os.environ.pop("OTEL_SDK_DISABLED", None)
    try:
        yield
    finally:
        if saved is not None:
            os.environ["OTEL_SDK_DISABLED"] = saved


def _point(*, name: str, unit: str, point: Any) -> MetricPoint:
    attributes = dict(point.attributes or {})
    if hasattr(point, "explicit_bounds"):
        return MetricPoint(
            name=name, unit=unit, attributes=attributes, value=point.sum, count=point.count,
            buckets=tuple(point.explicit_bounds),
        )
    return MetricPoint(name=name, unit=unit, attributes=attributes, value=point.value)
