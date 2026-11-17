"""Process state: inert until :func:`start` (CLI) or :func:`instrument` (library) installs the listener."""

from __future__ import annotations

import contextlib
import functools
import inspect
import logging
import threading
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from slayer.core import stages
from slayer.otel import settings

if TYPE_CHECKING:
    from slayer.otel.adapter import OtelStageListener

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])

# The longest the host process waits at shutdown for a final export, as usage reporting does.
SHUTDOWN_WAIT_SECONDS = 1.0

_listener: OtelStageListener | None = None
_owned: list[Any] = []
_log_handlers: list[logging.Handler] = []


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
    capture = settings.capture()
    configured = exporters.configure()
    _owned = configured.owned
    if configured.logger_provider is not None:
        _log_handlers = logs.attach(logger_provider=configured.logger_provider, full="exceptions" in capture)
    instrument(capture=capture)


def instrument(*, tracer_provider: Any = None, meter_provider: Any = None, capture: Iterable[str] = ()) -> None:
    """Install the stage listener; the providers default to the global ones. Never configures providers."""
    global _listener
    if _listener is not None:
        return
    from slayer.otel.adapter import OtelStageListener  # ALLOW(import-not-top): optional extra

    _listener = OtelStageListener(
        tracer_provider=tracer_provider, meter_provider=meter_provider, capture=frozenset(capture),
    )
    stages.add_listener(_listener)


def uninstrument() -> None:
    global _listener
    if _listener is not None:
        stages.remove_listener(_listener)
        _listener = None


@_silent
def shutdown() -> None:
    """Remove the listener and log handler; flush the providers :func:`start` created, waiting at most 1 s."""
    global _owned
    uninstrument()
    _detach_logs()
    providers, _owned = _owned, []
    if providers:
        worker = threading.Thread(
            target=_shutdown_providers, kwargs={"providers": providers}, name="slayer-otel-shutdown", daemon=True,
        )
        worker.start()
        worker.join(timeout=SHUTDOWN_WAIT_SECONDS)


def reset() -> None:
    global _owned
    uninstrument()
    _detach_logs()
    _owned = []


def _shutdown_providers(*, providers: list[Any]) -> None:
    for provider in providers:
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
    extract: Callable[[inspect.BoundArguments], stages.Attributes] | None = None,
) -> Callable[[F], F]:
    """Mark a surface's handler as one ``request`` stage, as ``usage_reporting.counting`` counts it."""
    attributes: dict[str, stages.AttributeValue] = {"surface": surface, "operation": token or "other"}
    return stages.staged(name="request", attributes=attributes, params=params, extract=extract)
