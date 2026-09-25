"""Compact duration parsing for windowed measures (``window='90d'``).

Shared by the plan-time windowed guard (engine layer) and the SQL generator's
per-unit interval emission (sql layer). Lives in ``slayer.core`` so the engine
planner can validate a window duration at plan time WITHOUT importing the SQL layer.

Compact syntax only — an integer immediately followed by a unit, repeated with
no separators: ``1y2m3w5d6h7min8s``. Units: ``y`` year, ``m`` month, ``w`` week,
``d`` day, ``h`` hour, ``min`` minute, ``s`` second.
"""

from __future__ import annotations

import re

from slayer.core.errors import WindowDurationError

# ``min`` must precede the single-char alternation so ``7min`` parses the whole
# ``min`` unit rather than a bare ``m`` followed by a stray ``in``.
_WINDOW_DURATION_RE = re.compile(r"(?P<num>\d+)(?P<unit>min|[ymwdhs])")


def parse_window_duration(value: str) -> list[tuple[int, str]]:
    """Parse a compact duration like ``1y2m3w5d6h7min8s`` into ``(amount, unit)``
    parts, in written order; raises ``WindowDurationError`` on any malformed input."""
    if not isinstance(value, str):
        raise WindowDurationError(
            summary=f"Window duration must be a compact duration string like '90d', got {value!r}.",
            suggestion="Use syntax like '1y2m3w5d6h7min8s'.",
        )
    if not value:
        raise WindowDurationError(summary="Window duration cannot be empty.")
    pos = 0
    parts: list[tuple[int, str]] = []
    for match in _WINDOW_DURATION_RE.finditer(value):
        if match.start() != pos:
            break
        amount = int(match.group("num"))
        unit = match.group("unit")
        if amount <= 0:
            raise WindowDurationError(summary=f"Window duration parts must be positive in '{value}'.")
        parts.append((amount, unit))
        pos = match.end()
    if pos != len(value) or not parts:
        raise WindowDurationError(
            summary=f"Invalid window duration '{value}'.", suggestion="Use syntax like '1y2m3w5d6h7min8s'.",
        )
    return parts
