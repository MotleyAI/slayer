"""Stored ontology and mapping documents: what an explicit change would break, and database-driven changes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from slayer.core.document_text import parse_document_text
from slayer.core.errors import BrokenDependent, UnreadableDocumentError
from slayer.ontology.scope import check_mapping
from slayer.core.datasource_scope import DatasourceScope

ONTOLOGY_NAME = "ontology name"
MAPPING_NAME = "mapping name"

class StoredMapping(BaseModel):
    """A stored mapping that resolves."""

    name: str
    data_source: str
    text: str


def stored_data(text: str | None) -> Any:
    """A stored document's text as data; ``None`` when absent or unreadable."""
    if text is None:
        return None
    try:
        return parse_document_text(text)
    except UnreadableDocumentError:
        return None


def broken_mappings(mappings: list[tuple[str, dict[str, Any]]], *, before: DatasourceScope,
                    after: DatasourceScope) -> list[BrokenDependent]:
    """The ``(name, data)`` mappings of one datasource that resolve worse in ``after`` than in ``before``."""
    out: list[BrokenDependent] = []
    for name, data in mappings:
        held = {str(p) for p in check_mapping(data, scope=before).problems}
        new = [p for p in check_mapping(data, scope=after).problems if str(p) not in held]
        if new:
            out.append(BrokenDependent(data_source=before.data_source, name=name, problems=[str(p) for p in new],
                                     entries=[p.entry for p in new]))
    return out


