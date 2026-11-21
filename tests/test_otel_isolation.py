"""A real ``slayer`` process with OTel on: the output is unchanged, a down or slow backend never delays exit."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests._usage_reporting_capture import CaptureServer, RefusedEndpoint
from tests._usage_reporting_helpers import build_storage, run_python, run_slayer, wait_until

_QUERY = {"population": "orders", "metrics": ["sum(orders.amount)"], "dimensions": ["orders.region"]}
_SLACK_SECONDS = 5.0

_HOST_PROVIDER = (
    "from opentelemetry import trace\n"
    "from opentelemetry.sdk.trace import TracerProvider\n"
    "host = TracerProvider()\n"
    "trace.set_tracer_provider(host)\n"
    "from slayer import otel\n"
    "otel.start()\n"
    "print(otel.is_active(), trace.get_tracer_provider() is host)\n"
    "otel.shutdown()\n"
)


def _env(**overrides: str) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith(("OTEL_", "SLAYER_OTEL"))}
    env.update({"SLAYER_USAGE_REPORTING": "off", "OTEL_BSP_SCHEDULE_DELAY": "100"}, **overrides)
    return env


def _base(url: str) -> str:
    return url.removesuffix("/i/v0/e/")


@pytest.fixture
def store(tmp_path: Path) -> str:
    return build_storage(tmp_path)


@pytest.fixture
def backend() -> Iterator[CaptureServer]:
    server = CaptureServer().start()
    try:
        yield server
    finally:
        server.stop()


def _query(store: str, **env: str) -> tuple[str, int, float]:
    began = time.monotonic()
    result = run_slayer(["query", json.dumps(_QUERY), "--storage", store], env=_env(**env))
    return result.stdout, result.returncode, time.monotonic() - began


# --- the host is unchanged -----------------------------------------------------------


def test_output_is_the_same_with_otel_on(store: str, backend: CaptureServer) -> None:
    off = _query(store)
    on = _query(store, SLAYER_OTEL="on", OTEL_EXPORTER_OTLP_ENDPOINT=_base(backend.url))
    assert (on[0], on[1]) == (off[0], off[1])


def test_spans_reach_the_backend_as_otlp_protobuf(store: str, backend: CaptureServer) -> None:
    _query(store, SLAYER_OTEL="on", OTEL_EXPORTER_OTLP_ENDPOINT=_base(backend.url))
    wait_until(lambda: any(request.path == "/v1/traces" for request in backend.requests))
    traces = [request for request in backend.requests if request.path == "/v1/traces"]
    content_types = {value for name, value in traces[0].headers.items() if name.lower() == "content-type"}
    assert (content_types, b"slayer.query" in b"".join(request.body for request in traces)) == (
        {"application/x-protobuf"}, True,
    )


def test_a_host_tracer_provider_is_kept() -> None:
    result = run_python(_HOST_PROVIDER, env=_env(SLAYER_OTEL="on", OTEL_EXPORTER_OTLP_ENDPOINT=RefusedEndpoint().url))
    assert (result.returncode, result.stdout.strip()) == (0, "True True")


# --- a broken backend never delays exit ----------------------------------------------


def test_a_refused_backend_adds_at_most_the_shutdown_bound(store: str) -> None:
    baseline = _query(store)[2]
    stdout, code, elapsed = _query(store, SLAYER_OTEL="on", OTEL_EXPORTER_OTLP_ENDPOINT=_base(RefusedEndpoint().url))
    assert (code, bool(stdout), elapsed - baseline < _SLACK_SECONDS) == (0, True, True)


# Only traces export, so a request at the backend proves that trace export ran.
_TRACES_ONLY = {"OTEL_METRICS_EXPORTER": "none", "OTEL_LOGS_EXPORTER": "none"}


def test_a_stalled_backend_adds_at_most_the_shutdown_bound(store: str, backend: CaptureServer) -> None:
    baseline = _query(store)[2]
    backend.behaviour.delay = 30
    _, code, elapsed = _query(store, SLAYER_OTEL="on", OTEL_EXPORTER_OTLP_ENDPOINT=_base(backend.url), **_TRACES_ONLY)
    assert (code, elapsed - baseline < _SLACK_SECONDS, backend.wait_for_arrival()) == (0, True, True)


def test_a_rejecting_backend_leaves_stderr_clean(store: str, backend: CaptureServer) -> None:
    backend.behaviour.status = 401
    result = run_slayer(
        ["query", json.dumps(_QUERY), "--storage", store],
        env=_env(SLAYER_OTEL="on", OTEL_EXPORTER_OTLP_ENDPOINT=_base(backend.url), **_TRACES_ONLY),
    )
    received = backend.wait_for(count=1)
    assert (result.returncode, "Traceback" in result.stderr, [r.path for r in received][:1]) == (0, False, ["/v1/traces"])
