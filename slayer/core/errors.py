"""Public error types raised by the SLayer core/engine/storage layers.

Kept in ``slayer.core`` so callers can catch them without importing engine or
storage internals. Each class is defined with a stable name and signature; the
message format is decided by the layer that raises it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

if TYPE_CHECKING:
    from slayer.engine.schema_drift import ToDeleteEntry  # noqa: F401


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


class SchemaDriftError(SlayerError):
    """Raised by ``SlayerQueryEngine.execute()`` when a query fails and the
    failure was attributed to schema drift via ``validate_models``.

    Carries the touched model names, the structured ``to_delete`` payload
    (filtered to those models), and the original DBAPI exception (set as
    ``__cause__`` for tracebacks).
    """

    def __init__(
        self,
        models: List[str],
        to_delete: List[Any],
        original: BaseException,
    ) -> None:
        self.models = list(models)
        self.to_delete = list(to_delete)
        super().__init__(
            f"Schema drift detected on models {sorted(self.models)}. "
            f"Run validate_models to inspect the {len(self.to_delete)} "
            f"pending delete(s)."
        )
        self.__cause__ = original
