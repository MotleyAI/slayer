"""Opt-in per-stage timing for the query hot path (facade perf debugging).

Set ``SLAYER_PROFILE_TIMING`` to a truthy value (``1`` / ``true`` / ``yes`` /
``on``) to enable. Disabled by default so production logs stay quiet.

When enabled, wrap a request in :func:`open_query_profile` and bracket
sub-stages with :func:`start` / :func:`record`; one summary line is logged at
INFO on scope exit, e.g.::

    slayer.timing query: resolve_model=2.1 resolve_datasource=1.4 enrich=0.8 \
        generate_sql=3.2 execute=774.1 connect=254.6 set_timeout=251.1 \
        query=268.4 total=782.9 ms

Overhead when disabled: one env-var read when a scope would open, and one
``ContextVar`` read per ``start`` / ``record`` — no timestamps are taken and
nothing is logged.
"""

from __future__ import annotations

import contextlib
import logging
import os
import time
from contextvars import ContextVar

logger = logging.getLogger(__name__)

_TRUTHY = frozenset({"1", "true", "yes", "on"})

# Active collector: ordered ``[(stage_name, elapsed_ms), ...]``. ``None`` when
# no profile scope is open in the current context — the common (and disabled)
# case, which every ``start`` / ``record`` short-circuits on.
_collector: ContextVar[list[tuple[str, float]] | None] = ContextVar(
    "slayer_timing_collector", default=None
)


def timing_enabled() -> bool:
    """True when ``SLAYER_PROFILE_TIMING`` is set to a truthy value."""
    return os.environ.get("SLAYER_PROFILE_TIMING", "").strip().lower() in _TRUTHY


@contextlib.contextmanager
def open_query_profile(label: str = "query"):
    """Open a timing scope; emit one summary line on exit. No-op when disabled.

    Safe to wrap an ``await`` — the ``ContextVar`` propagates to coroutines
    awaited within the same task, so nested ``record`` calls in the engine and
    SQL client land in this scope's collector.
    """
    if not timing_enabled():
        yield
        return
    token = _collector.set([])
    started = time.perf_counter()
    try:
        yield
    finally:
        total_ms = (time.perf_counter() - started) * 1000.0
        stages = _collector.get() or []
        _collector.reset(token)
        rendered = " ".join(f"{name}={ms:.1f}" for name, ms in stages)
        logger.info("slayer.timing %s: %s total=%.1f ms", label, rendered, total_ms)


def start() -> float | None:
    """Return a start timestamp when a profile is open, else ``None``.

    The ``None`` fast-path keeps ``start`` / ``record`` free when profiling is
    off or the call site runs outside any :func:`open_query_profile` scope.
    """
    return time.perf_counter() if _collector.get() is not None else None


def record(name: str, started: float | None) -> None:
    """Record elapsed ms for ``name`` since ``started`` into the open profile.

    No-op when ``started`` is ``None`` (profiling off / no scope open).
    """
    if started is None:
        return
    collector = _collector.get()
    if collector is not None:
        collector.append((name, (time.perf_counter() - started) * 1000.0))
