"""Stored ontology and mapping documents: what an explicit change would break, and database-driven changes."""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Generator
from contextvars import ContextVar
from typing import Any

from pydantic import BaseModel

from slayer.core.deprecation import outside_stacklevel
from slayer.core.document_text import parse_document_text
from slayer.core.errors import BrokenMapping, OntologyMappingBreakError, UnreadableDocumentError
from slayer.ontology.scope import MappingScope, check_mapping

ONTOLOGY_NAME = "ontology name"
MAPPING_NAME = "mapping name"

_DATABASE_DRIVEN: ContextVar[bool] = ContextVar("_DATABASE_DRIVEN", default=False)


class StoredMapping(BaseModel):
    """A stored mapping that resolves."""

    name: str
    data_source: str
    text: str


class OntologyMappingUnresolvedWarning(UserWarning):
    """A database-driven change left stored mappings unresolved."""


@contextlib.contextmanager
def database_driven() -> Generator[None]:
    """Changes made inside follow the database: they apply, warning about each stored mapping they break."""
    token = _DATABASE_DRIVEN.set(True)
    try:
        yield
    finally:
        _DATABASE_DRIVEN.reset(token)


def stored_data(text: str | None) -> Any:
    """A stored document's text as data; ``None`` when absent or unreadable."""
    if text is None:
        return None
    try:
        return parse_document_text(text)
    except UnreadableDocumentError:
        return None


def broken_mappings(mappings: list[tuple[str, dict[str, Any]]], *, before: MappingScope,
                    after: MappingScope) -> list[BrokenMapping]:
    """The ``(name, data)`` mappings of one datasource that resolve worse in ``after`` than in ``before``."""
    out: list[BrokenMapping] = []
    for name, data in mappings:
        held = {str(p) for p in check_mapping(data, scope=before).problems}
        new = [p for p in check_mapping(data, scope=after).problems if str(p) not in held]
        if new:
            out.append(BrokenMapping(data_source=before.data_source, name=name, problems=[str(p) for p in new],
                                     entries=[p.entry for p in new]))
    return out


def report_broken(*, change: str, broken: list[BrokenMapping], own: list[str] | None = None) -> None:
    """Refuse ``change`` for breaking ``broken`` (listing the changed document's ``own`` problems too); a
    database-driven change applies and warns instead."""
    if not broken:
        return
    error = OntologyMappingBreakError(change=change, broken=broken, own=own or [])
    if not _DATABASE_DRIVEN.get():
        raise error
    names = ", ".join(f"{b.data_source}.{b.name}" for b in broken)
    warnings.warn(f"{change} leaves stored mapping(s) {names} unresolved:\n" + "\n".join(f"  - {r}" for r in
                                                                                       error.reasons),
                  OntologyMappingUnresolvedWarning, stacklevel=outside_stacklevel())
