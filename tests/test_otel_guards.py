"""OTel is inert until switched on, and never fails the host: every activation path is fail-silent."""

from __future__ import annotations

import logging
import os
import threading
import time

import pytest

from slayer import otel
from slayer.core import probes
from slayer.otel import exporters, logs, recorder
from tests._otel_helpers import OtelCapture
from tests._usage_reporting_helpers import run_python

# Watches the importer, not sys.modules: a dependency such as FastAPI can import OTel on its own.
_LOAD_MODULES = (
    "import builtins\n"
    "seen = set()\n"
    "original = builtins.__import__\n"
    "def watch(name, globals=None, *args, **kwargs):\n"
    "    importer = (globals or {}).get('__name__', '')\n"
    "    if name.split('.')[0] == 'opentelemetry' and importer.split('.')[0] == 'slayer':\n"
    "        seen.add(importer)\n"
    "    return original(name, globals, *args, **kwargs)\n"
    "builtins.__import__ = watch\n"
    "import slayer.cli, slayer.engine.query_engine, slayer.api.server, slayer.mcp.server, slayer.otel\n"
    "print(sorted(seen))\n"
)

_WITHOUT_EXTRA = (
    "import importlib.abc, sys\n"
    "class Block(importlib.abc.MetaPathFinder):\n"
    "    def find_spec(self, name, path=None, target=None):\n"
    "        if name.split('.')[0] == 'opentelemetry':\n"
    "            raise ImportError(name)\n"
    "sys.meta_path.insert(0, Block())\n"
    "import importlib.metadata as md\n"
    "real = md.distribution\n"
    "def hidden(name):\n"
    "    if name.startswith('opentelemetry'):\n"
    "        raise md.PackageNotFoundError(name)\n"
    "    return real(name)\n"
    "md.distribution = hidden\n"
    "from slayer import otel\n"
    "from slayer.otel import control\n"
    "otel.start()\n"
    "print(otel.is_active(), control._extra_installed())\n"
    "otel.shutdown()\n"
)

# SLayer's providers are private: the global tracer provider stays the proxy, and a second start works.
_PRIVATE_AND_RESTARTABLE = (
    "from opentelemetry import trace\n"
    "from slayer import otel\n"
    "from slayer.otel import recorder\n"
    "otel.start()\n"
    "first = list(recorder._owned)\n"
    "otel.shutdown()\n"
    "otel.start()\n"
    "second = list(recorder._owned)\n"
    "print(isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider), otel.is_active(),\n"
    "      len(second) == len(first) > 0, not set(map(id, first)) & set(map(id, second)))\n"
    "otel.shutdown()\n"
)


def _env(**overrides: str) -> dict[str, str]:
    # An inherited OTEL_SDK_DISABLED=true would stop start() before the code under test runs.
    inherited = {key: value for key, value in os.environ.items() if key != "OTEL_SDK_DISABLED"}
    return {**inherited, "SLAYER_USAGE_REPORTING": "off", **overrides}


class _SlowProvider:
    def shutdown(self) -> None:
        time.sleep(3)


class _FastProvider:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def shutdown(self) -> None:
        self.closed.set()


# --- inert by default ----------------------------------------------------------------


def test_the_suite_inherits_no_otel_setting_from_the_shell() -> None:
    assert [name for name in os.environ if name.startswith("OTEL_")] == []


def test_no_slayer_module_imports_otel_when_off() -> None:
    result = run_python(_LOAD_MODULES, env=_env())
    assert (result.returncode, result.stdout.strip()) == (0, "[]")


def test_start_does_nothing_when_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "off")
    otel.start()
    assert (otel.is_active(), probes._listeners) == (False, ())


def test_sdk_disabled_wins_over_slayer_otel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    otel.start()
    assert otel.is_active() is False


# --- fail-silent ---------------------------------------------------------------------


def test_start_without_the_extra_stays_off_and_silent() -> None:
    result = run_python(_WITHOUT_EXTRA, env=_env(SLAYER_OTEL="on"))
    assert (result.returncode, result.stdout.strip(), result.stderr) == (0, "False False", "")


def test_a_failing_configuration_stays_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    reached: list[bool] = []

    def broken() -> exporters.Configured:
        reached.append(True)
        raise RuntimeError("exporter bug")

    monkeypatch.setattr(exporters, "configure", broken)
    otel.start()
    assert (otel.is_active(), reached) == (False, [True])


def test_a_late_setup_failure_closes_the_providers_created_so_far(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    for signal in ("TRACES", "METRICS", "LOGS"):
        monkeypatch.setenv(f"OTEL_{signal}_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
    closed: list[object] = []
    original = exporters.shutdown_all

    def broken_log_exporter() -> None:
        raise RuntimeError("log exporter bug")

    def observed(providers: list[object]) -> None:
        closed.extend(providers)
        original(providers)

    monkeypatch.setattr(exporters, "OTLPLogExporter", broken_log_exporter)
    monkeypatch.setattr(exporters, "shutdown_all", observed)
    otel.start()
    workers = [t for t in threading.enumerate() if t.name.startswith(("OtelBatchSpanRecordProcessor", "OtelPeriodic"))]
    assert (otel.is_active(), recorder._owned, len(closed), workers) == (False, [], 3, [])


def test_a_setup_rollback_waits_at_most_the_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")

    def slow_then_broken(configured: exporters.Configured) -> None:
        configured.owned.append(_SlowProvider())
        raise RuntimeError("exporter bug")

    monkeypatch.setattr(exporters, "_create_providers", slow_then_broken)
    began = time.monotonic()
    otel.start()
    assert (otel.is_active(), time.monotonic() - began < recorder.SHUTDOWN_WAIT_SECONDS + 1) == (False, True)


def test_exporter_log_text_is_hidden_while_slayer_owns_the_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
    exporter_logger = logging.getLogger(logs.EXPORTER_LOGGERS[1])
    otel.start()
    try:
        hidden = any(isinstance(f, logs._ExportTextFilter) for f in exporter_logger.filters)
    finally:
        otel.reset()
    shown = not any(isinstance(f, logs._ExportTextFilter) for f in exporter_logger.filters)
    assert (hidden, shown) == (True, True)


def test_providers_stay_private_and_a_second_start_gets_new_ones() -> None:
    env = _env(SLAYER_OTEL="on", OTEL_TRACES_EXPORTER="none", OTEL_METRICS_EXPORTER="none", OTEL_LOGS_EXPORTER="none")
    result = run_python(_PRIVATE_AND_RESTARTABLE, env=env)
    assert (result.returncode, result.stdout.strip()) == (0, "True True True True"), result.stderr


def test_shutdown_waits_at_most_the_bound_in_a_daemon_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(recorder, "_owned", [_SlowProvider()])
    began = time.monotonic()
    otel.shutdown()
    elapsed = time.monotonic() - began
    workers = [thread for thread in threading.enumerate() if thread.name == "slayer-otel-shutdown"]
    assert (elapsed < recorder.SHUTDOWN_WAIT_SECONDS + 1, bool(workers), all(t.daemon for t in workers)) == (
        True, True, True,
    )


def test_shutdown_joins_with_one_shared_one_second_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    timeouts: list[float] = []
    original = threading.Thread.join

    def spy(self: threading.Thread, timeout: float | None = None) -> None:
        if self.name == "slayer-otel-shutdown":
            timeouts.append(timeout if timeout is not None else float("inf"))
        original(self, timeout)

    monkeypatch.setattr(threading.Thread, "join", spy)
    monkeypatch.setattr(recorder, "_owned", [_SlowProvider(), _SlowProvider()])
    otel.shutdown()
    assert (len(timeouts), max(timeouts) <= recorder.SHUTDOWN_WAIT_SECONDS, timeouts[-1] < 0.1) == (2, True, True)


def test_a_slow_provider_does_not_hold_back_the_others(monkeypatch: pytest.MonkeyPatch) -> None:
    fast = _FastProvider()
    monkeypatch.setattr(recorder, "_owned", [_SlowProvider(), fast])
    otel.shutdown()
    assert fast.closed.is_set()


def test_reset_closes_the_owned_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    fast = _FastProvider()
    monkeypatch.setattr(recorder, "_owned", [fast])
    otel.reset()
    assert (fast.closed.is_set(), recorder._owned) == (True, [])


@pytest.mark.parametrize("call", [otel.shutdown, otel.reset, otel.uninstrument])
def test_ending_before_starting_is_harmless(call) -> None:
    call()
    assert otel.is_active() is False


# --- library use ---------------------------------------------------------------------


def test_instrument_twice_installs_one_listener() -> None:
    capture = OtelCapture.build()
    try:
        capture.instrument()
        capture.instrument()
        assert len(probes._listeners) == 1
    finally:
        capture.close()


def test_uninstrument_removes_the_listener() -> None:
    capture = OtelCapture.build()
    try:
        capture.instrument()
        otel.uninstrument()
        assert (otel.is_active(), probes._listeners) == (False, ())
    finally:
        capture.close()

