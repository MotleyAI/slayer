"""Per-engine server profile cache, filled once per engine over whichever connection asks first."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import weakref
from collections.abc import Awaitable, Callable
from typing import Any

from slayer.sql.dialects.base import ServerProfile, SqlDialect

logger = logging.getLogger(__name__)

# The probe's first row, or ``None`` when it returned none.
FirstRow = tuple[Any, ...] | None

# Per-engine server profile, and the lock guarding its fill. A failed fill is never cached.
_server_profiles: "weakref.WeakKeyDictionary[Any, ServerProfile]" = weakref.WeakKeyDictionary()
_profile_locks: "weakref.WeakKeyDictionary[Any, Any]" = weakref.WeakKeyDictionary()
_profile_locks_guard = threading.Lock()


def _fresh_profile_locks() -> None:
    """In a forked child: the parent's profile locks may be held, so start over."""
    global _profile_locks_guard
    _profile_locks_guard = threading.Lock()
    _profile_locks.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_fresh_profile_locks)


def _profile_lock(engine: Any, factory: Callable[[], Any]) -> Any:
    with _profile_locks_guard:
        lock = _profile_locks.get(engine)
        if lock is None:
            lock = _profile_locks[engine] = factory()
        return lock


def _correlated_probe_failed(exc: Exception) -> None:
    logger.warning("Correlated-subquery setting probe failed; treating it as unknown: %s", exc)


def cached_profile(
    *, engine: Any, dialect: SqlDialect, first_row: Callable[[str], FirstRow],
    on_base: Callable[[ServerProfile], None] | None = None,
) -> ServerProfile:
    """``engine``'s cached profile, probed through ``first_row`` on first use; ``on_base`` sees the base profile
    before any follow-up probe runs."""
    profile_sql = dialect.server_profile_sql()
    if profile_sql is None:
        return ServerProfile()
    with _profile_lock(engine, threading.Lock):
        cached = _server_profiles.get(engine)
        if cached is not None:
            return cached
        base_row = first_row(profile_sql)
        profile = dialect.parse_server_profile(base_row=base_row)
        if on_base is not None:
            on_base(profile)
        correlated = dialect.correlated_setting_probe(profile)
        if correlated is not None:
            try:
                correlated_row = first_row(correlated.sql(dialect=dialect.sqlglot_name))
            except Exception as exc:  # the driver's own error types need not be DBAPI errors
                _correlated_probe_failed(exc)
                return profile
            profile = dialect.parse_server_profile(base_row=base_row, correlated_row=correlated_row)
        _server_profiles[engine] = profile
        return profile


async def cached_profile_async(
    *, engine: Any, dialect: SqlDialect, first_row: Callable[[str], Awaitable[FirstRow]],
) -> ServerProfile:
    """Async sibling of ``cached_profile``."""
    profile_sql = dialect.server_profile_sql()
    if profile_sql is None:
        return ServerProfile()
    async with _profile_lock(engine, asyncio.Lock):
        cached = _server_profiles.get(engine)
        if cached is not None:
            return cached
        base_row = await first_row(profile_sql)
        profile = dialect.parse_server_profile(base_row=base_row)
        correlated = dialect.correlated_setting_probe(profile)
        if correlated is not None:
            try:
                correlated_row = await first_row(correlated.sql(dialect=dialect.sqlglot_name))
            except Exception as exc:  # the driver's own error types need not be DBAPI errors
                _correlated_probe_failed(exc)
                return profile
            profile = dialect.parse_server_profile(base_row=base_row, correlated_row=correlated_row)
        _server_profiles[engine] = profile
        return profile
