"""Compact duration parsing for windowed measures (``window='90d'``).

Shared by the plan-time windowed guard (engine layer, DEV-1714) and the SQL
generator's per-unit interval emission (sql layer). Lives in ``slayer.core`` and
is dependency-free so the engine planner can validate a window duration at plan
time WITHOUT importing the SQL layer.

Compact syntax only — an integer immediately followed by a unit, repeated with
no separators: ``1y2m3w5d6h7min8s``. Units: ``y`` year, ``m`` month, ``w`` week,
``d`` day, ``h`` hour, ``min`` minute, ``s`` second.
"""

from __future__ import annotations

import re

# ``min`` must precede the single-char alternation so ``7min`` parses the whole
# ``min`` unit rather than a bare ``m`` followed by a stray ``in``.
_WINDOW_DURATION_RE = re.compile(r"(?P<num>\d+)(?P<unit>min|[ymwdhs])")


def parse_window_duration(value: str) -> list[tuple[int, str]]:
    """Parse a compact duration like ``1y2m3w5d6h7min8s`` into ``(amount, unit)``
    parts, in written order.

    Raises ``ValueError`` on an empty string, a non-positive amount, or any
    malformed / gapped input (e.g. ``'90x'``, ``'d90'``). The error messages are
    a stable contract — the plan-time guard and its tests match on them.
    """
    if not value:
        raise ValueError("Window duration cannot be empty")
    pos = 0
    parts: list[tuple[int, str]] = []
    for match in _WINDOW_DURATION_RE.finditer(value):
        if match.start() != pos:
            raise ValueError(
                f"Invalid window duration '{value}'. Use syntax like '1y2m3w5d6h7min8s'."
            )
        amount = int(match.group("num"))
        unit = match.group("unit")
        if amount <= 0:
            raise ValueError(f"Window duration parts must be positive in '{value}'")
        parts.append((amount, unit))
        pos = match.end()
    if pos != len(value) or not parts:
        raise ValueError(
            f"Invalid window duration '{value}'. Use syntax like '1y2m3w5d6h7min8s'."
        )
    return parts
