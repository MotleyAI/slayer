"""The per-document load boundary and the per-operation record of documents that failed to load."""

import contextlib
import warnings
from collections.abc import Generator
from typing import Literal, TypeVar

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.warnings import SlayerUnloadableDocumentWarning, UnloadableDocumentWarning

T = TypeVar("T")
Loaded = tuple[list[T], list[StoredDocumentLoadError]]


@contextlib.contextmanager
def stored_document_boundary(
    *, kind: Literal["model", "memory"], name: str, data_source: str | None = None,
) -> Generator[None]:
    """Re-raise any failure inside one stored document's load as a ``StoredDocumentLoadError`` naming it."""
    try:
        yield
    except StoredDocumentLoadError:
        raise
    except Exception as exc:
        raise StoredDocumentLoadError(kind=kind, name=name, data_source=data_source, cause=exc) from exc


class DocumentLoadFailures:
    """Unloadable documents met by one operation; each is warned once."""

    def __init__(self) -> None:
        self._errors: dict[str, StoredDocumentLoadError] = {}
        self.warnings: list[UnloadableDocumentWarning] = []

    @property
    def errors(self) -> list[StoredDocumentLoadError]:
        return list(self._errors.values())

    def record(self, error: StoredDocumentLoadError) -> None:
        if error.document in self._errors:
            return
        self._errors[error.document] = error
        payload = UnloadableDocumentWarning(
            document_kind="memory" if error.data_source is None else "model",
            data_source=error.data_source, name=error.name, cause=error.cause_text,
        )
        self.warnings.append(payload)
        warnings.warn(SlayerUnloadableDocumentWarning(payload), stacklevel=3)

    def skip(self, loaded: Loaded[T]) -> list[T]:
        """The loaded documents; each failure is recorded and skipped."""
        documents, errors = loaded
        for error in errors:
            self.record(error)
        return documents


def all_or_raise(loaded: Loaded[T]) -> list[T]:
    """The loaded documents; the first failure is raised (fail closed)."""
    documents, errors = loaded
    if errors:
        raise errors[0]
    return documents
