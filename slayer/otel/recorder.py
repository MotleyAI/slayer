"""Process state: inert until :func:`start` (CLI) or :func:`instrument` (library) installs the listener."""

from __future__ import annotations

import contextlib
import functools
import inspect
import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import slayer.core.probes as probes
from slayer.otel import settings

if TYPE_CHECKING:
    from slayer.otel.adapter import OtelProbeListener

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])

# The longest the host process waits at shutdown for a final export, as usage reporting does.
SHUTDOWN_WAIT_SECONDS = 1.0

_listener: OtelProbeListener | None = None
_owned: list[Any] = []
_log_handlers: list[logging.Handler] = []
_lingering: list[threading.Thread] = []  # shutdowns that outlived their bound
# One lifecycle change at a time, so the listener, providers and handlers stay in step; re-entrant for start().
_lifecycle = threading.RLock()


def _locked(fn: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with _lifecycle:
            return fn(*args, **kwargs)

    return wrapper


def _silent(fn: Callable[P, R]) -> Callable[P, R | None]:
    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
        try:
            return fn(*args, **kwargs)
        except Exception:
            logger.debug("otel: %s failed", fn.__name__, exc_info=True)
            return None

    return wrapper


def is_active() -> bool:
    return _listener is not None


@_silent
@_locked
def start() -> None:
    """Activate in a process started by the ``slayer`` CLI, if ``SLAYER_OTEL=on``."""
    global _owned, _log_handlers
    if _listener is not None or not settings.enabled():
        return
    try:
        from slayer.otel import exporters, logs  # ALLOW(import-not-top): optional extra
    except ImportError:
        logger.debug("otel: SLAYER_OTEL is on but the otel extra is missing; pip install 'motley-slayer[otel]'")
        return
    content = settings.capture_content()
    configured = exporters.configure()
    if configured.owned:
        logs.hide_export_text()  # our exporters' failure logs can quote the endpoint URL
    try:
        if configured.logger_provider is not None:
            _log_handlers = logs.attach(logger_provider=configured.logger_provider, full=content)
        # SLayer's own providers are private and passed here; with a host provider, both are None.
        instrument(
            tracer_provider=configured.tracer_provider, meter_provider=configured.meter_provider,
            capture_content=content,
        )
    except Exception:
        _detach_logs()
        if exporters.shutdown_all(configured.owned):
            logs.show_export_text()
        raise
    _owned = configured.owned


@_locked
def instrument(*, tracer_provider: Any = None, meter_provider: Any = None, capture_content: bool = False) -> None:
    """Install the probe listener; the providers default to the global ones. Never configures providers."""
    global _listener
    if _listener is not None:
        return
    from slayer.otel.adapter import OtelProbeListener  # ALLOW(import-not-top): optional extra

    _listener = OtelProbeListener(
        tracer_provider=tracer_provider, meter_provider=meter_provider, capture_content=capture_content,
    )
    probes.add_listener(_listener)


@_locked
def uninstrument() -> None:
    global _listener
    if _listener is not None:
        probes.remove_listener(_listener)
        _listener = None


@_silent
def shutdown() -> None:
    """Remove the listener and log handler; flush the providers :func:`start` created, waiting at most 1 s."""
    reset()


@_locked
def reset() -> None:
    """Like :func:`shutdown`, but errors propagate: remove the listener, then close the owned providers."""
    global _owned
    uninstrument()
    _detach_logs()
    providers, _owned = _owned, []
    if close_providers(providers) and providers:  # a shutdown still running may still log the URL
        from slayer.otel import logs  # ALLOW(import-not-top): optional extra

        logs.show_export_text()


def close_providers(providers: list[Any]) -> bool:
    """Shut each provider down in its own thread, so a slow backend delays no other signal's final batch.

    Returns whether no shutdown, of this call or an earlier one, is still running.
    """
    workers = [
        threading.Thread(target=_shutdown_one, kwargs={"provider": provider}, name="slayer-otel-shutdown", daemon=True)
        for provider in providers
    ]
    for worker in workers:
        worker.start()
    deadline = time.monotonic() + SHUTDOWN_WAIT_SECONDS
    for worker in workers:
        worker.join(timeout=max(0.0, deadline - time.monotonic()))
    _lingering[:] = [worker for worker in (*_lingering, *workers) if worker.is_alive()]
    return not _lingering


def _shutdown_one(*, provider: Any) -> None:
    with contextlib.suppress(Exception):
        provider.shutdown()


def _detach_logs() -> None:
    global _log_handlers
    if _log_handlers:
        from slayer.otel import logs  # ALLOW(import-not-top): optional extra

        logs.detach(_log_handlers)
        _log_handlers = []


def request(
    *,
    surface: str,
    token: str | None = None,
    params: Mapping[str, str] | None = None,
    extract: Callable[[inspect.BoundArguments], probes.Attributes] | None = None,
) -> Callable[[F], F]:
    """Mark a surface's handler as one ``request`` probe, as ``usage_reporting.counting`` counts it."""
    attributes: dict[str, probes.AttributeValue] = {"surface": surface, "operation": token or "other"}
    return probes.probe(name="request", attributes=attributes, params=params, extract=extract)
