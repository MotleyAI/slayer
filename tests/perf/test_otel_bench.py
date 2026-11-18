"""The cost of a ``@staged`` function: with no listener (OTel off), and with the OTel listener on.

Run with: poetry run pytest tests/perf/test_otel_bench.py --benchmark-only
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from slayer import otel
from slayer.core import stages
from tests._otel_helpers import sdk_enabled


def _plain(value: int, *, name: str) -> int:
    return value


@stages.staged(name="db.execute", params={"datasource.name": "name"}, result=lambda value: {"rows": value})
def _decorated(value: int, *, name: str) -> int:
    return value


class _Discard(SpanExporter):
    """Keeps no span, so a long run measures the instrumentation and not a growing list."""

    def export(self, spans) -> SpanExportResult:  # noqa: ANN001
        return SpanExportResult.SUCCESS


@pytest.fixture
def listening() -> Iterator[None]:
    # Record every span, whatever the shell's sampler or SDK switch says.
    with sdk_enabled():
        tracer_provider = TracerProvider(sampler=ALWAYS_ON, shutdown_on_exit=False)
        meter_provider = MeterProvider(metric_readers=[InMemoryMetricReader()], shutdown_on_exit=False)
    tracer_provider.add_span_processor(SimpleSpanProcessor(_Discard()))
    otel.instrument(tracer_provider=tracer_provider, meter_provider=meter_provider)
    try:
        yield
    finally:
        otel.reset()
        tracer_provider.shutdown()
        meter_provider.shutdown()


_CALLS: dict[str, Callable[..., int]] = {"plain": _plain, "staged": _decorated}


@pytest.mark.benchmark(group="otel-off")
@pytest.mark.parametrize("call", list(_CALLS))
def test_otel_off(benchmark, call: str) -> None:
    assert benchmark(_CALLS[call], 3, name="shop") == 3


@pytest.mark.benchmark(group="otel-on")
def test_otel_on(benchmark, listening: None) -> None:
    assert benchmark(_decorated, 3, name="shop") == 3
