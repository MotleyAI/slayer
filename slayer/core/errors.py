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
    datasource priority list does not pick a unique winner.

    The constructor stores the ambiguous name and the list of candidate
    datasources. The default message is intentionally surface-neutral: it
    states the fact and a generic remediation, but does not reference any
    Python-, REST-, MCP-, or CLI-specific invocation form. Each surface
    appends its own concrete remediation when it catches the error
    (``data_source=...`` query param for REST, the
    ``set_datasource_priority`` MCP tool, the ``slayer datasources
    priority`` CLI subcommand, etc.).
    """

    def __init__(self, name: str, candidates: List[str]) -> None:
        self.name = name
        self.candidates = list(candidates)
        super().__init__(
            f"Model '{name}' exists in multiple datasources: "
            f"{sorted(self.candidates)}. Specify a data_source or set a "
            f"datasource priority to disambiguate."
        )
