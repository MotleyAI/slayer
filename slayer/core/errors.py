"""Public error types raised by the SLayer core/engine/storage layers.

Kept in ``slayer.core`` so callers can catch them without importing engine or
storage internals. Each class is defined with a stable name and signature; the
message format is decided by the layer that raises it.
"""

from __future__ import annotations

from typing import List


class SlayerError(Exception):
    """Base class for SLayer-specific errors.

    Catching ``SlayerError`` distinguishes our intentional failure modes from
    unexpected ``Exception`` paths (driver errors, IO errors, etc.).
    """


class AmbiguousModelError(SlayerError):
    """Raised when a bare model name resolves to ≥2 datasources and the
    datasource priority list (``set_datasource_priority``) does not pick a
    unique winner.

    The constructor stores the ambiguous name and the list of candidate
    datasources so callers (MCP, REST, CLI) can render a consistent message.
    """

    def __init__(self, name: str, candidates: List[str]) -> None:
        self.name = name
        self.candidates = list(candidates)
        super().__init__(
            f"Model '{name}' exists in multiple datasources: "
            f"{sorted(self.candidates)}. Pass an explicit data_source=... "
            f"or call set_datasource_priority([...]) to disambiguate."
        )
