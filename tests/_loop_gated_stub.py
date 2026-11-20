"""A blocking stub that returns only once the event loop has run a callback it scheduled."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

GATE_TIMEOUT_S = 3.0


def _false(*_args: Any, **_kwargs: Any) -> bool:
    return False


class LoopGatedStub:
    """Each call blocks until ``loop`` runs a callback; called on the loop thread it times out instead."""

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        fn: Callable[..., Any] = _false,
        timeout: float = GATE_TIMEOUT_S,
    ) -> None:
        self._loop = loop
        self._fn = fn
        self._timeout = timeout
        self._lock = threading.Lock()
        self.calls = 0
        self.timeouts = 0
        self.threads: list[int] = []
        self.handles: list[asyncio.Handle] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        released = threading.Event()
        handle = self._loop.call_soon_threadsafe(released.set)
        with self._lock:
            self.calls += 1
            self.threads.append(threading.get_ident())
            self.handles.append(handle)
        if not released.wait(self._timeout):
            handle.cancel()
            with self._lock:
                self.timeouts += 1
        return self._fn(*args, **kwargs)

    def assert_loop_stayed_free(self) -> None:
        assert self.calls >= 1, "the stubbed check was never reached"
        assert self.timeouts == 0, f"{self.timeouts} of {self.calls} call(s) blocked the event loop"


def gated(*, fn: Callable[..., Any] = _false, timeout: float = GATE_TIMEOUT_S) -> LoopGatedStub:
    """A stub gated on the running loop."""
    return LoopGatedStub(loop=asyncio.get_running_loop(), fn=fn, timeout=timeout)
