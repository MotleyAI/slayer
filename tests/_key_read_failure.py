"""Make one table's key read fail inside the real best-effort read, as CPython's ``sqlite3`` did on 3.14."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

import pytest

from slayer.sql.dialects import reflection

LOST = "<sqlite3.Connection object at 0x0> returned NULL without setting an exception"


def fail_key_read(
    monkeypatch: pytest.MonkeyPatch, *, table: str, reads: tuple[str, ...] = ("primary key",),
) -> None:
    """Every module's ``best_effort`` raises ``LOST`` for ``reads`` of ``table`` (keyed on its ``table`` / ``what``)."""
    real = reflection.best_effort

    def _lost() -> Any:
        raise SystemError(LOST)

    def _best_effort(fn: Callable[[], Any], *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("table") == table and kwargs.get("what") in reads:
            fn = _lost
        return real(fn, *args, **kwargs)

    for name, module in list(sys.modules.items()):
        if name.startswith("slayer.") and getattr(module, "best_effort", None) is real:
            monkeypatch.setattr(module, "best_effort", _best_effort)
