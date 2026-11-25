"""Whether a model validator rewrote stored input while validating, so the upgrade counts it as a change."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

from pydantic import BaseModel


class CollectedRewrites(BaseModel):
    """What validation inside ``collecting_rewrites`` rewrote."""

    rewritten: bool = False


_COLLECTED: ContextVar[CollectedRewrites | None] = ContextVar("_COLLECTED", default=None)


def stored_input_rewritten() -> None:
    """Mark that a validator rewrote its input (a legacy spelling, not a filled default)."""
    collected = _COLLECTED.get()
    if collected is not None:
        collected.rewritten = True


@contextmanager
def collecting_rewrites() -> Generator[CollectedRewrites]:
    """Collect the rewrites marked by validation inside the block."""
    collected = CollectedRewrites()
    token = _COLLECTED.set(collected)
    try:
        yield collected
    finally:
        _COLLECTED.reset(token)
