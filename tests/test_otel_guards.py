"""OTel is inert until switched on, and never fails the host: every activation path is fail-silent."""

from __future__ import annotations

import os
import threading
import time

import pytest

from slayer import otel
from slayer.core import stages
from slayer.otel import exporters, recorder
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
    "from slayer import otel\n"
    "otel.start()\n"
    "print(otel.is_active())\n"
    "otel.shutdown()\n"
)


def _env(**overrides: str) -> dict[str, str]:
    return {**os.environ, "SLAYER_USAGE_REPORTING": "off", **overrides}


class _SlowProvider:
    def shutdown(self) -> None:
        time.sleep(3)


# --- inert by default ----------------------------------------------------------------


def test_no_slayer_module_imports_otel_when_off() -> None:
    result = run_python(_LOAD_MODULES, env=_env())
    assert (result.returncode, result.stdout.strip()) == (0, "[]")


def test_start_does_nothing_when_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "off")
    otel.start()
    assert (otel.is_active(), stages._listeners) == (False, ())


def test_sdk_disabled_wins_over_slayer_otel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    otel.start()
    assert otel.is_active() is False


# --- fail-silent ---------------------------------------------------------------------


def test_start_without_the_extra_stays_off_and_silent() -> None:
    result = run_python(_WITHOUT_EXTRA, env=_env(SLAYER_OTEL="on"))
    assert (result.returncode, result.stdout.strip(), result.stderr) == (0, "False", "")


def test_a_failing_configuration_stays_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_OTEL", "on")

    def broken() -> exporters.Configured:
        raise RuntimeError("exporter bug")

    monkeypatch.setattr(exporters, "configure", broken)
    otel.start()
    assert otel.is_active() is False


def test_shutdown_waits_at_most_the_bound_in_a_daemon_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(recorder, "_owned", [_SlowProvider()])
    began = time.monotonic()
    otel.shutdown()
    elapsed = time.monotonic() - began
    workers = [thread for thread in threading.enumerate() if thread.name == "slayer-otel-shutdown"]
    assert (elapsed < recorder.SHUTDOWN_WAIT_SECONDS + 1, bool(workers), all(t.daemon for t in workers)) == (
        True, True, True,
    )


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
        assert len(stages._listeners) == 1
    finally:
        capture.close()


def test_uninstrument_removes_the_listener() -> None:
    capture = OtelCapture.build()
    try:
        capture.instrument()
        otel.uninstrument()
        assert (otel.is_active(), stages._listeners) == (False, ())
    finally:
        capture.close()

