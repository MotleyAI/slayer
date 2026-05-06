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


class EntityResolutionError(SlayerError):
    """Raised when an entity reference cannot be resolved to a canonical
    ``<datasource>.<model>[.<leaf>]`` form (DEV-1357).

    Wraps the spec's resolution-failure cases: unknown segment, ambiguous
    bare column matching multiple models in the priority-winner
    datasource, ``*:count`` invoked outside a query context, and similar.
    Distinct from ``AmbiguousModelError`` (which fires for the model leg
    of bare-name resolution and is reused by the resolver verbatim).
    """


class LearningOrQueryNotFoundError(SlayerError):
    """Raised when a learning or saved-query ID does not exist in storage.

    Used by both the learning APIs (``L<int>`` IDs) and the saved-query APIs
    (``Q<int>`` IDs); the unified ``delete_learning_or_query`` MCP tool also
    surfaces this error.
    """

    def __init__(self, identifier: str) -> None:
        self.identifier = identifier
        super().__init__(
            f"No learning or saved query with id '{identifier}'."
        )
