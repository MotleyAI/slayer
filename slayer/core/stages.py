"""Pipeline stages: named orchestration steps that listeners can observe.

Mark a function with :func:`staged`, or a block with :func:`stage`; :func:`current`
reaches the open stage. Names are short (``ingest.scan.schema``); a listener adds
any namespace. With no listener, every entry point takes the fast path: one tuple
test, no timestamp, no allocation.
"""

from __future__ import annotations

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


class StageEvent(BaseModel):
    name: str
    time_ns: int
    attributes: dict[str, AttributeValue] = Field(default_factory=dict)


class Stage(BaseModel):
    name: str
    parent: Stage | None = None
    attributes: dict[str, AttributeValue] = Field(default_factory=dict)
    events: list[StageEvent] = Field(default_factory=list)
    start_ns: int = 0
    started: float = 0.0
    duration_s: float = 0.0
    active: bool = True

    def set(self, attributes: Attributes) -> None:
        if self.active:
            self.attributes.update(_clean(attributes))

    def add_event(self, *, name: str, attributes: Attributes | None = None) -> None:
        if self.active:
            self.events.append(StageEvent(name=name, time_ns=time.time_ns(), attributes=_clean(attributes or {})))


class StageListener(Protocol):
    def on_start(self, stage: Stage) -> None: ...

    def on_end(self, stage: Stage, *, error: BaseException | None) -> None: ...


_INACTIVE = Stage(name="inactive", active=False)
_listeners: tuple[StageListener, ...] = ()
_current: ContextVar[Stage | None] = ContextVar("slayer_stage_current", default=None)


def add_listener(listener: StageListener) -> None:
    global _listeners
    if listener not in _listeners:
        _listeners = (*_listeners, listener)


def remove_listener(listener: StageListener) -> None:
    global _listeners
    _listeners = tuple(item for item in _listeners if item is not listener)


def current() -> Stage:
    """The innermost open stage, or an inactive stage that ignores every write."""
    return _current.get() or _INACTIVE


class _InactiveScope:
    __slots__ = ()

    def __enter__(self) -> Stage:
        return _INACTIVE

    def __exit__(self, *exc_info: object) -> bool:
        return False


_INACTIVE_SCOPE = _InactiveScope()


class _StageScope:
    __slots__ = ("_stage", "_token")

    def __init__(self, opened: Stage) -> None:
        self._stage = opened
        self._token: Token[Stage | None] | None = None

    def __enter__(self) -> Stage:
        self._token = _current.set(self._stage)
        _notify_start(self._stage)
        return self._stage

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> bool:
        if self._token is not None:
            _current.reset(self._token)
        _finish(self._stage, error=exc)
        return False


def stage(*, name: str, attributes: Attributes | None = None) -> _StageScope | _InactiveScope:
    """A context manager for one stage around a block."""
    if not _listeners:
        return _INACTIVE_SCOPE
    return _StageScope(
        Stage(
            name=name, parent=_current.get(), attributes=_clean(attributes or {}),
            start_ns=time.time_ns(), started=time.perf_counter(),
        )
    )


def staged(
    *,
    name: str,
    attributes: Attributes | None = None,
    params: Mapping[str, str] | None = None,
    extract: Callable[[inspect.BoundArguments], Attributes] | None = None,
    result: Callable[[Any], Attributes] | None = None,
) -> Callable[[F], F]:
    """Mark a whole function as one stage; ``params`` maps attribute keys to parameter names."""
    fixed = dict(attributes or {})
    mapping = dict(params or {})

    def decorate(fn: F) -> F:
        if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
            raise TypeError(f"@staged cannot wrap generator {fn.__qualname__}")
        signature = inspect.signature(fn)
        missing = sorted(set(mapping.values()) - set(signature.parameters))
        if missing:
            raise TypeError(f"@staged(name={name!r}) on {fn.__qualname__}: no parameter {missing}")

        def opened(args: tuple[Any, ...], kwargs: dict[str, Any]) -> _StageScope | _InactiveScope:
            bound = _bound_attributes(signature, args=args, kwargs=kwargs, params=mapping, extract=extract)
            return stage(name=name, attributes={**fixed, **bound})

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if not _listeners:
                    return await fn(*args, **kwargs)
                with opened(args, kwargs) as open_stage:
                    value = await fn(*args, **kwargs)
                    _apply_result(open_stage, result=result, value=value)
                    return value

            return cast(F, async_wrapper)

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _listeners:
                return fn(*args, **kwargs)
            with opened(args, kwargs) as open_stage:
                value = fn(*args, **kwargs)
                _apply_result(open_stage, result=result, value=value)
                return value

        return cast(F, sync_wrapper)

    return decorate


def _finish(finished: Stage, *, error: BaseException | None) -> None:
    finished.duration_s = time.perf_counter() - finished.started
    for listener in _listeners:
        try:
            listener.on_end(finished, error=error)
        except Exception:
            logger.debug("stage listener failed on end of %s", finished.name, exc_info=True)


def _notify_start(started: Stage) -> None:
    for listener in _listeners:
        try:
            listener.on_start(started)
        except Exception:
            logger.debug("stage listener failed on start of %s", started.name, exc_info=True)


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
        logger.debug("stage attributes failed", exc_info=True)
        return {}


def _apply_result(target: Stage, *, result: Callable[[Any], Attributes] | None, value: Any) -> None:
    if result is None or not target.active:
        return
    try:
        target.set(result(value))
    except Exception:
        logger.debug("stage result attributes failed for %s", target.name, exc_info=True)


def _clean(attributes: Mapping[str, Any]) -> dict[str, AttributeValue]:
    return {key: value for key, value in attributes.items() if isinstance(value, (str, int, float, bool))}
