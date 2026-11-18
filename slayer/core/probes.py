"""Probes: named orchestration steps that listeners can observe.

Mark a function with :func:`probe`, or a block with :func:`scope`; :func:`current`
reaches the open probe. Names are short (``ingest.scan.schema``); a listener adds
any namespace. With no listener, every entry point takes the fast path: one tuple
test, no timestamp, no allocation.
"""

from __future__ import annotations

import contextlib
import functools
import inspect
import logging
import time
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token
from typing import Any, Protocol, TypeVar, cast

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

AttributeValue = str | int | float | bool
Attributes = Mapping[str, AttributeValue]
F = TypeVar("F", bound=Callable[..., Any])


class ProbeEvent(BaseModel):
    name: str
    time_ns: int
    attributes: dict[str, AttributeValue] = Field(default_factory=dict)


class Probe(BaseModel):
    name: str
    parent: Probe | None = None
    attributes: dict[str, AttributeValue] = Field(default_factory=dict)
    events: list[ProbeEvent] = Field(default_factory=list)
    start_ns: int = 0
    started: float = 0.0
    duration_s: float = 0.0
    active: bool = True

    def set(self, attributes: Attributes) -> None:
        if self.active:
            self.attributes.update(_clean(attributes))

    def add_event(self, *, name: str, attributes: Attributes | None = None) -> None:
        if self.active:
            self.events.append(ProbeEvent(name=name, time_ns=time.time_ns(), attributes=_clean(attributes or {})))


class ProbeListener(Protocol):
    def on_start(self, probe: Probe) -> None: ...

    def on_end(self, probe: Probe, *, error: BaseException | None) -> None: ...


_INACTIVE = Probe(name="inactive", active=False)
_listeners: tuple[ProbeListener, ...] = ()
_current: ContextVar[Probe | None] = ContextVar("slayer_probe_current", default=None)


def add_listener(listener: ProbeListener) -> None:
    global _listeners
    if listener not in _listeners:
        _listeners = (*_listeners, listener)


def remove_listener(listener: ProbeListener) -> None:
    global _listeners
    _listeners = tuple(item for item in _listeners if item is not listener)


def current() -> Probe:
    """The innermost open probe, or an inactive probe that ignores every write."""
    return _current.get() or _INACTIVE


class _InactiveScope:
    __slots__ = ()

    def __enter__(self) -> Probe:
        return _INACTIVE

    def __exit__(self, *exc_info: object) -> bool:
        return False


_INACTIVE_SCOPE = _InactiveScope()


class _ProbeScope:
    __slots__ = ("_listeners", "_probe", "_token")

    def __init__(self, opened: Probe, *, listeners: tuple[ProbeListener, ...]) -> None:
        self._probe = opened
        self._listeners = listeners  # the same listeners see the start and the end
        self._token: Token[Probe | None] | None = None

    def __enter__(self) -> Probe:
        self._probe.parent = _current.get()
        self._probe.start_ns, self._probe.started = time.time_ns(), time.perf_counter()
        self._token = _current.set(self._probe)
        _notify_start(self._probe, listeners=self._listeners)
        return self._probe

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> bool:
        if self._token is not None:
            _current.reset(self._token)
        _finish(self._probe, error=exc, listeners=self._listeners)
        return False


def scope(*, name: str, attributes: Attributes | None = None) -> _ProbeScope | _InactiveScope:
    """A context manager for one probe around a block."""
    listeners = _listeners
    if not listeners:
        return _INACTIVE_SCOPE
    return _ProbeScope(Probe(name=name, attributes=_clean(attributes or {})), listeners=listeners)


def probe(
    *,
    name: str,
    attributes: Attributes | None = None,
    params: Mapping[str, str] | None = None,
    extract: Callable[[inspect.BoundArguments], Attributes] | None = None,
    result: Callable[[Any], Attributes] | None = None,
) -> Callable[[F], F]:
    """Mark a whole function as one probe; ``params`` maps attribute keys to parameter names."""
    fixed = dict(attributes or {})
    mapping = dict(params or {})

    def decorate(fn: F) -> F:
        signature = _checked_signature(fn, name=name, params=mapping)

        def opened(*, args: tuple[Any, ...], kwargs: dict[str, Any]) -> _ProbeScope | _InactiveScope:
            bound = _bound_attributes(signature, args=args, kwargs=kwargs, params=mapping, extract=extract)
            return scope(name=name, attributes={**fixed, **bound})

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if not _listeners:
                    return await fn(*args, **kwargs)
                with opened(args=args, kwargs=kwargs) as open_probe:
                    value = await fn(*args, **kwargs)
                    _apply_result(open_probe, result=result, value=value)
                    return value

            return cast(F, async_wrapper)

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _listeners:
                return fn(*args, **kwargs)
            with opened(args=args, kwargs=kwargs) as open_probe:
                value = fn(*args, **kwargs)
                _apply_result(open_probe, result=result, value=value)
                return value

        return cast(F, sync_wrapper)

    return decorate


def _checked_signature(fn: Callable[..., Any], *, name: str, params: Mapping[str, str]) -> inspect.Signature:
    """Refuse a generator and an unknown parameter name at decoration time."""
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        raise TypeError(f"@probe cannot wrap generator {fn.__qualname__}")
    signature = inspect.signature(fn)
    missing = sorted(set(params.values()) - set(signature.parameters))
    if missing:
        raise TypeError(f"@probe(name={name!r}) on {fn.__qualname__}: no parameter {missing}")
    return signature


def _finish(finished: Probe, *, error: BaseException | None, listeners: tuple[ProbeListener, ...]) -> None:
    finished.duration_s = time.perf_counter() - finished.started
    for listener in listeners:
        try:
            listener.on_end(finished, error=error)
        except Exception:
            _debug("probe listener failed on end of %s", finished.name)
    finished.active = False  # a task that outlives the probe can no longer write to it


def _notify_start(started: Probe, *, listeners: tuple[ProbeListener, ...]) -> None:
    for listener in listeners:
        try:
            listener.on_start(started)
        except Exception:
            _debug("probe listener failed on start of %s", started.name)


def _bound_attributes(
    signature: inspect.Signature,
    *,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    params: Mapping[str, str],
    extract: Callable[[inspect.BoundArguments], Attributes] | None,
) -> dict[str, AttributeValue]:
    if not params and extract is None:
        return {}
    try:
        bound = signature.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        attributes = _clean({key: bound.arguments.get(param) for key, param in params.items()})
        if extract is not None:
            attributes.update(_clean(extract(bound)))
        return attributes
    except Exception:
        _debug("probe attributes failed")
        return {}


def _apply_result(target: Probe, *, result: Callable[[Any], Attributes] | None, value: Any) -> None:
    if result is None or not target.active:
        return
    try:
        target.set(result(value))
    except Exception:
        _debug("probe result attributes failed for %s", target.name)


def _debug(message: str, *args: object) -> None:
    """Log a probe failure; a failing handler must not reach the host either."""
    with contextlib.suppress(Exception):
        logger.debug(message, *args, exc_info=True)


def _clean(attributes: Mapping[str, Any]) -> dict[str, AttributeValue]:
    return {key: value for key, value in attributes.items() if isinstance(value, (str, int, float, bool))}
